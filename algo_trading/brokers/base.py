"""
algo_trading/brokers/base.py
─────────────────────────────
Abstract base class for all broker WebSocket feeds.

Every broker feed must subclass ``BaseFeed`` and implement:
    • ``connect_and_stream``  — the protocol-specific connection loop
    • ``normalize_tick``      — converts raw broker data → unified frame

The base class provides:
    • Auto-reconnect with exponential backoff
    • Per-broker rate limiting
    • Periodic token refresh (re-reads instrument tokens from DB)
    • Unified ``run()`` entrypoint that the engine calls
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
import time
from typing import Any

logger = logging.getLogger("algo_trading.brokers")


class BaseFeed(abc.ABC):
    """Abstract base for a single broker's WebSocket feed."""

    # ── Subclasses MUST set this ──────────────────────────────────
    BROKER_NAME: str = ""

    def __init__(
        self,
        *,
        account_id: str = "",
        broker_name: str = "",
        api_provider: str = "",
        api_key: str = "",
        access_token: str = "",
        api_secret: str = "",
        api_endpoint: str = "",
        instrument_tokens: list,
        rate_limit_interval: float = 0.1,
        reconnect_base_delay: float = 2.0,
        reconnect_max_delay: float = 60.0,
        token_refresh_interval: float = 30.0,
    ) -> None:
        self.account_id = account_id or broker_name
        self.broker_name = broker_name
        self.api_provider = api_provider
        self.api_key = api_key
        self.access_token = access_token
        self.api_secret = api_secret
        self.api_endpoint = api_endpoint
        self.instrument_tokens = list(instrument_tokens)

        # Timing controls
        self.rate_limit_interval = rate_limit_interval
        self.reconnect_base_delay = reconnect_base_delay
        self.reconnect_max_delay = reconnect_max_delay
        self.token_refresh_interval = token_refresh_interval

        # Runtime state (set by ``run()``)
        self._tick_queue: asyncio.Queue | None = None
        self._log_queue: asyncio.Queue | None = None
        self._last_tick_times: dict[str, float] = {}
        self._running: bool = False

    # ──────────────────────────────────────────────────────────────
    # Public API — called by the engine
    # ──────────────────────────────────────────────────────────────

    async def run(
        self,
        tick_queue: asyncio.Queue,
        log_queue: asyncio.Queue,
    ) -> None:
        """
        Main entrypoint.  Runs forever:
            1. connect_and_stream (broker-specific)
            2. on disconnect → exponential backoff → retry
        Also spawns a background task for periodic token refresh.
        """
        self._tick_queue = tick_queue
        self._log_queue = log_queue
        self._running = True

        # Spawn token-refresh background loop
        refresh_task = asyncio.create_task(self._token_refresh_loop())

        delay = self.reconnect_base_delay
        try:
            while self._running:
                try:
                    self.log("INFO", "Starting connection…")
                    await self.connect_and_stream()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.log("WARNING", f"Connection lost: {exc}")

                # Exponential backoff
                self.log("INFO", f"Reconnecting in {delay:.1f}s…")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.reconnect_max_delay)
        except asyncio.CancelledError:
            self.log("INFO", "Feed cancelled — shutting down.")
        finally:
            self._running = False
            refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await refresh_task

    # ──────────────────────────────────────────────────────────────
    # Abstract — subclasses MUST implement
    # ──────────────────────────────────────────────────────────────

    @abc.abstractmethod
    async def connect_and_stream(self) -> None:
        """
        Open the broker-specific WebSocket / Socket.IO connection,
        read messages in a loop, and call ``self.enqueue_tick(raw_data)``
        for each message.

        Must NOT catch ``asyncio.CancelledError`` — let it propagate so
        the ``run()`` loop can perform a clean shutdown.

        When the connection drops (EOF, network error, etc.) this method
        should simply return so ``run()`` can trigger the backoff-retry.
        """

    @abc.abstractmethod
    def normalize_tick(self, raw_data: Any) -> dict:
        """
        Convert a broker-specific payload into the unified frame format::
            {
                "timestamp": <int, epoch nanoseconds>,
                "broker": "<broker_name>",
                "data": <dict, the original payload>,
            }
        """

    # ──────────────────────────────────────────────────────────────
    # Helpers — available to subclasses
    # ──────────────────────────────────────────────────────────────

    async def enqueue_tick(self, raw_data: Any) -> None:
        """
        Rate-limit, normalise, and push a tick onto this broker's
        dedicated tick queue. Throttling is performed per-symbol so that
        high-frequency instruments cannot starve other contracts.
        """
        frame = self.normalize_tick(raw_data)
        if not frame:
            return

        data = frame.get("data") or {}
        sym_key = ""
        if isinstance(data, dict):
            sym_key = str(
                data.get("symbol")
                or data.get("tradingsymbol")
                or data.get("instrument_token")
                or data.get("tk")
                or data.get("s")
                or ""
            ).strip()
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            sym_key = str(
                data[0].get("symbol")
                or data[0].get("tradingsymbol")
                or data[0].get("instrument_token")
                or data[0].get("tk")
                or data[0].get("s")
                or ""
            ).strip()

        now = time.time()
        sym_slot = sym_key if sym_key else "__global__"
        last_t = self._last_tick_times.get(sym_slot, 0.0)
        if (now - last_t) < self.rate_limit_interval:
            return  # throttled for this specific symbol
        self._last_tick_times[sym_slot] = now

        if self._tick_queue is not None:
            await self._tick_queue.put(frame)

    def log(self, level: str, message: str, context: str | None = None) -> None:
        """
        Push a structured log entry into the shared log queue for database persistence and standard logging.
        """
        from algo_trading.brokers.consumers import log_event
        ctx = context or (self.account_id or self.broker_name).upper()
        log_event(level, message, context=ctx, log_queue=self._log_queue)

    def make_frame(self, data: dict) -> dict:
        """Convenience: build a unified tick frame."""
        return {
            "timestamp": time.time_ns(),
            "account_id": self.account_id,
            "broker": self.broker_name,
            "api_provider": self.api_provider,
            "data": data,
        }

    # ──────────────────────────────────────────────────────────────
    # Token refresh
    # ──────────────────────────────────────────────────────────────

    async def _token_refresh_loop(self) -> None:
        """
        Periodically re-read instrument tokens from the DB
        (via Django ORM in a thread) and call ``on_tokens_changed``
        if they differ from the current set.
        """
        while self._running:
            await asyncio.sleep(self.token_refresh_interval)
            try:
                new_tokens = await asyncio.to_thread(self._read_tokens_from_db)
                if set(new_tokens) != set(self.instrument_tokens):
                    old = self.instrument_tokens
                    self.instrument_tokens = list(new_tokens)
                    self.log(
                        "INFO",
                        f"Instrument tokens changed: {len(old)} → {len(new_tokens)}",
                    )
                    await self.on_tokens_changed(old, self.instrument_tokens)
            except Exception as exc:
                self.log("ERROR", f"Token refresh failed: {exc}")

    DEFAULT_INSTRUMENT_TOKENS: list = []

    def _read_tokens_from_db(self) -> list:
        """
        Synchronous DB read (runs inside ``asyncio.to_thread``).
        Reads tokens strictly bound to this account instance.

        Explicitly closes the thread-local Django DB connection on exit so
        that each short-lived ``asyncio.to_thread`` worker does not leave an
        idle psycopg connection open, which would exhaust the pool when many
        feeds run their token-refresh loops concurrently.
        """
        try:
            from kalai.models import Broker

            broker_obj = Broker.resolve(self.account_id)
            if not broker_obj:
                return list(self.DEFAULT_INSTRUMENT_TOKENS)

            tokens = broker_obj.get_subscribed_tokens()
            return tokens if tokens else list(self.DEFAULT_INSTRUMENT_TOKENS)
        finally:
            try:
                from django.db import connection as _django_conn
                _django_conn.close()
            except Exception:
                pass

    async def on_tokens_changed(
        self, old_tokens: list, new_tokens: list
    ) -> None:
        """
        Hook called when instrument tokens change.
        Subclasses can override to re-subscribe on the live connection.
        Default: no-op (the next reconnect will use the updated tokens).
        """

    # ──────────────────────────────────────────────────────────────
    # Repr
    # ──────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} broker={self.broker_name!r} "
            f"tokens={len(self.instrument_tokens)}>"
        )

