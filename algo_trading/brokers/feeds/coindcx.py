"""
algo_trading/brokers/feeds/coindcx.py
──────────────────────────────────────
CoinDCX WebSocket feed using ``python-socketio`` async client.

CoinDCX uses Socket.IO (not raw WebSocket), so we use
``socketio.AsyncClient`` which runs natively inside the asyncio loop.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from typing import Any

import orjson
import socketio

from algo_trading.brokers.base import BaseFeed

logger = logging.getLogger("algo_trading.brokers")


class CoinDCXFeed(BaseFeed):
    """WebSocket feed for CoinDCX (Socket.IO)."""

    BROKER_NAME = "coindcx"
    ALIASES = ["coindcx_pro", "coindcx_futures", "coindcx_api"]

    SOCKET_URL = "https://stream.coindcx.com"
    HEARTBEAT_INTERVAL = 25  # seconds
    DEFAULT_INSTRUMENT_TOKENS = ["B-BTC_USDT@orderbook@20", "B-BTC_USDT@trade"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sio: socketio.AsyncClient | None = None

    # ──────────────────────────────────────────────────────────────
    # BaseFeed abstract implementation
    # ──────────────────────────────────────────────────────────────

    async def connect_and_stream(self) -> None:
        """
        Create an AsyncClient, register event handlers, connect, and
        wait until disconnection.
        """
        disconnect_event = asyncio.Event()

        self._sio = socketio.AsyncClient(
            reconnection=False,  # We handle reconnection in BaseFeed.run()
            logger=False,
            engineio_logger=False,
        )

        # ── Register event handlers ───────────────────────────
        @self._sio.event
        async def connect():
            self.log("INFO", "Socket.IO connected. Joining channels…")
            try:
                await self._subscribe()
            except Exception as exc:
                self.log("ERROR", f"Error in _subscribe: {exc}")
            # Start heartbeat
            asyncio.create_task(self._heartbeat_loop())

        @self._sio.event
        async def disconnect():
            self.log("WARNING", "Socket.IO disconnected.")
            disconnect_event.set()

        @self._sio.on("depth-update")
        async def on_depth(data):
            await self._handle_market_event(data)

        @self._sio.on("depth-update-20")
        async def on_depth_20(data):
            await self._handle_market_event(data)

        @self._sio.on("new-trade")
        async def on_trade(data):
            await self._handle_market_event(data)

        @self._sio.on("balance-update")
        async def on_balance(data):
            self.log("INFO", f"Balance update: {data}")

        @self._sio.on("order-update")
        async def on_order(data):
            self.log("INFO", f"Order update: {data}")

        @self._sio.on("error")
        async def on_error(data):
            self.log("ERROR", f"Stream error: {data}")

        # ── Connect ───────────────────────────────────────────
        self.log("INFO", f"Connecting to {self.SOCKET_URL}…")
        await self._sio.connect(self.SOCKET_URL, transports=["websocket"])

        # Block until disconnected
        await disconnect_event.wait()

        # Cleanup
        await self._disconnect()

    def normalize_tick(self, raw_data: Any) -> dict:
        """Wrap a CoinDCX market event into the unified frame format."""
        return self.make_frame(raw_data)

    # ──────────────────────────────────────────────────────────────
    # Token refresh — re-subscribe on the live connection
    # ──────────────────────────────────────────────────────────────

    async def on_tokens_changed(self, old_tokens: list, new_tokens: list) -> None:
        """Re-join channels when instrument tokens change."""
        if self._sio is None or not self._sio.connected:
            return

        self.log("INFO", f"Re-subscribing channels: {len(old_tokens)} → {len(new_tokens)}")
        # Socket.IO doesn't have an "unsubscribe" — we just join the new ones.
        # Old channels will naturally stop receiving data if not re-joined on reconnect.
        for symbol in new_tokens:
            if symbol not in old_tokens:
                await self._sio.emit("join", {"channelName": str(symbol)})
                self.log("INFO", f"Joined new channel: {symbol}")

    # ──────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────

    async def _subscribe(self) -> None:
        """Join the private user channel + all public market channels."""
        self.log("INFO", "In _subscribe starting...")
        if self._sio is None:
            self.log("INFO", "Socket is None in _subscribe")
            return

        # ─ Private channel (auth required) ────────────────────
        self.log("INFO", "Signing private channel...")
        body = {"channel": "coindcx"}
        signature = self._sign(body)
        self.log("INFO", f"Signature generated: {signature[:10]}...")
        await self._sio.emit("join", {
            "channelName": "coindcx",
            "authSignature": signature,
            "apiKey": self.api_key,
        })
        self.log("INFO", "Joined private user channel 'coindcx'.")

        # ─ Public market data channels ────────────────────────
        for symbol in self.instrument_tokens:
            self.log("INFO", f"Submitting join query for symbol: {symbol}")
            await self._sio.emit("join", {"channelName": str(symbol)})
            self.log("INFO", f"Join query submitted for symbol: {symbol}")

    def _sign(self, body: dict) -> str:
        """
        CoinDCX HMAC-SHA256 signature over the JSON-serialized body.
        """
        json_body = orjson.dumps(body)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            json_body,
            hashlib.sha256,
        ).hexdigest()

    async def _handle_market_event(self, raw: Any) -> None:
        """
        CoinDCX wraps every event as ``{"event": ..., "data": "<json>"}``.
        The inner ``data`` is a JSON string that must be parsed separately.
        """
        parsed = self._unwrap(raw)
        if parsed is not None:
            await self.enqueue_tick(parsed)

    @staticmethod
    def _unwrap(raw: Any) -> dict | None:
        """Extract and parse the inner JSON payload."""
        try:
            inner = raw.get("data") if isinstance(raw, dict) else raw
            if isinstance(inner, str):
                return orjson.loads(inner)
            if isinstance(inner, dict):
                return inner
            return raw if isinstance(raw, dict) else None
        except Exception as exc:
            logger.warning("Failed to unwrap CoinDCX payload: %s (%s)", raw, exc)
            return None

    async def _heartbeat_loop(self) -> None:
        """Send application-level pings to keep the connection alive."""
        try:
            while self._sio is not None and self._sio.connected:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                if self._sio is not None and self._sio.connected:
                    await self._sio.emit("ping", {"data": "Ping message"})
                    self.log("DEBUG", "Sent app-level heartbeat ping.")
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.log("ERROR", f"Heartbeat failed: {exc}")

    async def _disconnect(self) -> None:
        """Gracefully disconnect the Socket.IO client."""
        if self._sio is not None:
            try:
                await self._sio.disconnect()
            except Exception:
                pass
            self._sio = None
