"""
algo_trading/brokers/feeds/zerodha.py
──────────────────────────────────────
Zerodha WebSocket feed using the ``kiteconnect.KiteTicker`` library.

KiteTicker is synchronous (runs its own internal event loop on a background
thread). We wrap it so that tick callbacks push data into the async engine's
per-broker ``tick_queue`` via ``loop.call_soon_threadsafe``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from kiteconnect import KiteTicker

from algo_trading.brokers.base import BaseFeed

logger = logging.getLogger("algo_trading.brokers")


class ZerodhaFeed(BaseFeed):
    """WebSocket feed for Zerodha (Kite Connect)."""

    BROKER_NAME = "zerodha"
    ALIASES = ["kite", "zerodha_kite"]

    WS_URL = "wss://ws.kite.trade"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._ticker = None  # KiteTicker instance
        self._ticker_thread_started = False
        self._loop: asyncio.AbstractEventLoop | None = None

    # ──────────────────────────────────────────────────────────────
    # BaseFeed abstract implementation
    # ──────────────────────────────────────────────────────────────

    async def connect_and_stream(self) -> None:
        """
        Start KiteTicker on a background thread.
        The method blocks (via an asyncio.Event) until the ticker
        disconnects or is cancelled.
        """
        self._loop = asyncio.get_running_loop()
        disconnect_event = asyncio.Event()

        # Create a fresh KiteTicker instance
        self._ticker = KiteTicker(self.api_key, self.access_token)

        # ── Wire up callbacks ─────────────────────────────────
        def on_connect(ws, response):
            self.log("INFO", f"KiteTicker connected. Subscribing to {len(self.instrument_tokens)} tokens.")
            if self.instrument_tokens:
                ws.subscribe(self.instrument_tokens)
                ws.set_mode(ws.MODE_FULL, self.instrument_tokens)

        def on_ticks(ws, ticks):
            for tick in ticks:
                # Push into the async tick queue from the sync callback thread
                if self._loop and self._tick_queue is not None:
                    self._loop.call_soon_threadsafe(
                        self._tick_queue.put_nowait,
                        self.normalize_tick(tick),
                    )

        def on_close(ws, code, reason):
            self.log("WARNING", f"KiteTicker closed: code={code}, reason={reason} - Auto-reconnecting...")
            # We do NOT set disconnect_event here because kiteconnect automatically reconnects in the background.
            # Returning from connect_and_stream would attempt to create a NEW KiteTicker, which crashes the Twisted reactor.

        def on_noreconnect(ws):
            self.log("ERROR", "KiteTicker max reconnects reached. Restarting engine...")
            import os
            os._exit(1)

        def on_error(ws, code, reason):
            self.log("ERROR", f"KiteTicker error: code={code}, reason={reason}")

        self._ticker.on_connect = on_connect
        self._ticker.on_ticks = on_ticks
        self._ticker.on_close = on_close
        self._ticker.on_error = on_error
        self._ticker.on_noreconnect = on_noreconnect

        # ── Start KiteTicker in its own daemon thread ─────────
        self.log("INFO", "Starting KiteTicker thread…")
        await asyncio.to_thread(self._ticker.connect, threaded=True)

        # Wait for disconnect or cancellation
        await disconnect_event.wait()

        # Cleanup
        self._stop_ticker()

    def normalize_tick(self, raw_data: Any) -> dict:
        """Wrap a Kite tick dict into the unified frame format."""
        return self.make_frame(raw_data)

    # ──────────────────────────────────────────────────────────────
    # Token refresh — re-subscribe on the live connection
    # ──────────────────────────────────────────────────────────────

    async def on_tokens_changed(self, old_tokens: list, new_tokens: list) -> None:
        """Called by BaseFeed when instrument tokens change in the DB."""
        ticker = self._ticker
        if ticker is None:
            return

        self.log(
            "INFO",
            f"Re-subscribing: {len(old_tokens)} → {len(new_tokens)} tokens",
        )

        def _resubscribe():
            try:
                if old_tokens:
                    ticker.unsubscribe(old_tokens)
                if new_tokens:
                    ticker.subscribe(new_tokens)
                    ticker.set_mode(ticker.MODE_FULL, new_tokens)
            except Exception as exc:
                logger.error("Re-subscribe failed: %s", exc)

        await asyncio.to_thread(_resubscribe)

    # ──────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────

    def _stop_ticker(self) -> None:
        """Safely close the KiteTicker connection."""
        if self._ticker is not None:
            try:
                self._ticker.close()
            except Exception:
                pass
            self._ticker = None
