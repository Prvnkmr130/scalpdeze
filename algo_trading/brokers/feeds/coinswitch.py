"""
algo_trading/brokers/feeds/coinswitch.py
────────────────────────────────────────
CoinSwitch PRO WebSocket feed using ``python-socketio`` async client.

CoinSwitch Pro uses Socket.IO v4 namespaces (e.g. ``/coinswitchx``, ``/c2c1``)
and symbol channel subscriptions (format: ``BASE,QUOTE`` like ``BTC,INR``, ``BTC,USDT``).
"""

from __future__ import annotations

import asyncio
import logging
import zlib
from datetime import datetime, timezone
from typing import Any

import orjson
import socketio

from algo_trading.brokers.base import BaseFeed

logger = logging.getLogger("algo_trading.brokers")


def extract_coinswitch_pair(token: Any) -> str:
    """Normalize token or option contract string to CoinSwitch 'BASE,QUOTE' pair."""
    s = str(token).strip()
    if "/" in s:
        return s.replace("/", ",")
    if "_" in s:
        return s.replace("_", ",")
    if "-" in s:
        # Option / derivative format: 'BTC-25SEP26-80000-P-USDT'
        parts = s.split("-")
        base = parts[0]
        quote = parts[-1] if len(parts) > 1 and parts[-1] in ("USDT", "INR", "USDC") else "USDT"
        return f"{base},{quote}"
    return s


def str_to_token(s: Any) -> int:
    """Deterministic 32-bit positive integer token generator from string symbol across all processes."""
    if s is None:
        return 0
    raw_str = str(s).strip()
    if '/' in raw_str:
        raw_str = raw_str.replace('/', '').strip()
    return int(zlib.crc32(raw_str.encode('utf-8')) % (10**9))


class CoinSwitchFeed(BaseFeed):
    """WebSocket feed for CoinSwitch PRO (Socket.IO v4)."""

    BROKER_NAME = "coinswitch"
    ALIASES = ["coinswitch_pro", "coinswitchx"]

    SOCKET_URL = "wss://ws.coinswitch.co/"
    DEFAULT_SOCKETIO_PATH = "/pro/realtime-rates-socket/spot"
    DEFAULT_NAMESPACE = "/coinswitchx"
    HEARTBEAT_INTERVAL = 25  # seconds
    DEFAULT_INSTRUMENT_TOKENS = ["BTC,INR", "BTC,USDT", "ETH,INR", "ETH,USDT"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sio: socketio.AsyncClient | None = None
        self._active_namespaces: set[str] = {self.DEFAULT_NAMESPACE, "/c2c1", "/c2c2"}

    # ──────────────────────────────────────────────────────────────
    # BaseFeed Abstract Implementation
    # ──────────────────────────────────────────────────────────────

    async def connect_and_stream(self) -> None:
        """
        Create an AsyncClient, register event handlers, connect to CoinSwitch
        Socket.IO namespaces, and wait until disconnection.
        """
        disconnect_event = asyncio.Event()

        self._sio = socketio.AsyncClient(
            reconnection=False,  # Reconnection handled in BaseFeed.run()
            logger=False,
            engineio_logger=False,
        )

        namespaces = [self.DEFAULT_NAMESPACE, "/c2c1", "/c2c2"]

        # ── Register Event Handlers ───────────────────────────
        for ns in namespaces:
            self._register_namespace_handlers(ns, disconnect_event)

        # ── Connect ───────────────────────────────────────────
        auth_payload = {"apiKey": self.api_key} if self.api_key else None
        self.log("INFO", f"Connecting to {self.SOCKET_URL} ({self.DEFAULT_SOCKETIO_PATH})…")

        try:
            await self._sio.connect(
                self.SOCKET_URL,
                namespaces=namespaces,
                transports=["websocket"],
                socketio_path=self.DEFAULT_SOCKETIO_PATH,
                auth=auth_payload,
            )
        except Exception as exc:
            # Fallback to spot/coinswitchx path if custom socketio_path fails
            fallback_path = "/pro/realtime-rates-socket/spot/coinswitchx"
            self.log("WARNING", f"Connection with socketio_path failed: {exc}. Retrying fallback path {fallback_path}...")
            await self._sio.connect(
                self.SOCKET_URL,
                namespaces=namespaces,
                transports=["websocket"],
                socketio_path=fallback_path,
                auth=auth_payload,
            )

        # Block until disconnected
        await disconnect_event.wait()

    def _register_namespace_handlers(self, ns: str, disconnect_event: asyncio.Event) -> None:
        """Register Socket.IO event listeners for a specific namespace."""
        if not self._sio:
            return

        @self._sio.on("connect", namespace=ns)
        async def on_connect():
            self.log("INFO", f"Socket.IO connected on namespace {ns}. Joining channels…")
            try:
                await self._subscribe(namespace=ns)
            except Exception as exc:
                self.log("ERROR", f"Error subscribing on {ns}: {exc}")
            asyncio.create_task(self._heartbeat_loop())

        @self._sio.on("disconnect", namespace=ns)
        async def on_disconnect():
            self.log("WARNING", f"Socket.IO disconnected on namespace {ns}.")
            disconnect_event.set()

        @self._sio.on("FETCH_ORDER_BOOK_CS_PRO", namespace=ns)
        async def on_fetch_order_book(data):
            await self._handle_market_event(data, "FETCH_ORDER_BOOK_CS_PRO", ns)

        @self._sio.on("FETCH_TRADES_CS_PRO", namespace=ns)
        async def on_fetch_trades(data):
            await self._handle_market_event(data, "FETCH_TRADES_CS_PRO", ns)

        @self._sio.on("FETCH_TICKER_CS_PRO", namespace=ns)
        async def on_fetch_ticker(data):
            await self._handle_market_event(data, "FETCH_TICKER_CS_PRO", ns)

        @self._sio.on("orderbook", namespace=ns)
        async def on_orderbook(data):
            await self._handle_market_event(data, "orderbook", ns)

        @self._sio.on("depth-update", namespace=ns)
        async def on_depth_update(data):
            await self._handle_market_event(data, "depth-update", ns)

        @self._sio.on("trades", namespace=ns)
        async def on_trades(data):
            await self._handle_market_event(data, "trades", ns)

        @self._sio.on("new-trade", namespace=ns)
        async def on_new_trade(data):
            await self._handle_market_event(data, "new-trade", ns)

        @self._sio.on("candlestick", namespace=ns)
        async def on_candlestick(data):
            await self._handle_market_event(data, "candlestick", ns)

        @self._sio.on("balance-update", namespace=ns)
        async def on_balance(data):
            self.log("INFO", f"Balance update on {ns}: {data}")

        @self._sio.on("order-update", namespace=ns)
        async def on_order(data):
            self.log("INFO", f"Order update on {ns}: {data}")

        @self._sio.on("error", namespace=ns)
        async def on_error(data):
            self.log("ERROR", f"Stream error on {ns}: {data}")

    def normalize_tick(self, raw_data: Any, event_type: str = "trade") -> dict[str, Any]:
        """
        Normalize raw CoinSwitch WebSocket payloads into DeltaZero26 standard tick dictionary.
        """
        if isinstance(raw_data, (bytes, bytearray)):
            raw_data = orjson.loads(raw_data)
        elif isinstance(raw_data, str):
            raw_data = orjson.loads(raw_data)

        if not isinstance(raw_data, dict):
            raw_data = {"data": raw_data}

        # Extract symbol
        symbol = (
            raw_data.get("symbol")
            or raw_data.get("s")
            or raw_data.get("pair")
            or raw_data.get("channel", "")
        )
        if "," in symbol:
            symbol = symbol.replace(",", "/")

        token = symbol
        ltp = float(
            raw_data.get("price")
            or raw_data.get("p")
            or raw_data.get("last_price")
            or raw_data.get("close")
            or raw_data.get("c")
            or 0.0
        )
        volume = float(
            raw_data.get("quantity")
            or raw_data.get("q")
            or raw_data.get("volume")
            or raw_data.get("v")
            or 0.0
        )

        bids = raw_data.get("bids") or raw_data.get("b") or []
        asks = raw_data.get("asks") or raw_data.get("a") or []

        buy_demand = float(bids[0][1]) if bids and len(bids[0]) > 1 else 0.0
        sell_demand = float(asks[0][1]) if asks and len(asks[0]) > 1 else 0.0

        if ltp == 0.0 and bids and len(bids[0]) > 0:
            ltp = float(bids[0][0])

        depth_dict = {"buy": bids[:5] if bids else [], "sell": asks[:5] if asks else []}

        # Extract timestamp
        ts = raw_data.get("timestamp") or raw_data.get("E") or raw_data.get("t") or raw_data.get("time")
        if isinstance(ts, (int, float)) and ts > 0:
            dt = datetime.fromtimestamp(ts / 1000.0 if ts > 1e11 else ts, tz=timezone.utc).isoformat()
        else:
            dt = datetime.now(timezone.utc).isoformat()

        int_token = str_to_token(symbol) if isinstance(token, str) and not token.isdigit() else (int(token) if token is not None and str(token).isdigit() else str_to_token(symbol))

        return {
            "instrument_token": int_token,
            "tradingsymbol": symbol,
            "last_price": ltp,
            "open": float(raw_data.get("open") or raw_data.get("o") or ltp),
            "high": float(raw_data.get("high") or raw_data.get("h") or ltp),
            "low": float(raw_data.get("low") or raw_data.get("l") or ltp),
            "close": ltp,
            "volume": volume,
            "buy_demand": buy_demand,
            "sell_demand": sell_demand,
            "depth": depth_dict,
            "date_time": dt,
        }

    # ──────────────────────────────────────────────────────────────
    # Internal Helpers
    # ──────────────────────────────────────────────────────────────

    async def _handle_market_event(self, data: Any, event_type: str, namespace: str) -> None:
        """Parse incoming Socket.IO packet and forward normalized tick to queue."""
        if not data:
            return

        try:
            if isinstance(data, list):
                for item in data:
                    tick = self.normalize_tick(item, event_type)
                    if self._tick_queue and tick.get("last_price", 0) > 0:
                        await self._tick_queue.put(self.make_frame(tick))
            else:
                tick = self.normalize_tick(data, event_type)
                if self._tick_queue and tick.get("last_price", 0) > 0:
                    await self._tick_queue.put(self.make_frame(tick))
        except Exception as exc:
            self.log("ERROR", f"Failed to handle market event ({event_type}): {exc}")

    async def _subscribe(self, namespace: str = DEFAULT_NAMESPACE) -> None:
        """Emit subscription requests for all configured instrument tokens."""
        if not self._sio:
            return

        tokens = self.instrument_tokens or self.DEFAULT_INSTRUMENT_TOKENS
        pairs = list(dict.fromkeys(extract_coinswitch_pair(t) for t in tokens if t))
        if not pairs:
            return

        self.log("INFO", f"Subscribing to {len(pairs)} CoinSwitch pairs on namespace {namespace}: {pairs}")
        for sym in pairs:
            try:
                # 1. CoinSwitch PRO specific high-throughput orderbook, trades & ticker events
                await self._sio.emit("FETCH_ORDER_BOOK_CS_PRO", {"event": "subscribe", "pair": sym}, namespace=namespace)
                await self._sio.emit("FETCH_TRADES_CS_PRO", {"event": "subscribe", "pair": sym}, namespace=namespace)
                await self._sio.emit("FETCH_TICKER_CS_PRO", {"event": "subscribe", "pair": sym}, namespace=namespace)
                # 2. Standard fallback events
                await self._sio.emit("subscribe", {"symbol": sym, "pair": sym}, namespace=namespace)
            except Exception as e:
                self.log("WARNING", f"Failed to emit subscription for {sym}: {e}")

    async def on_tokens_changed(self, new_tokens: list) -> None:
        """Called when instrument tokens are updated in DB during active stream."""
        self.log("INFO", f"CoinSwitch tokens changed to: {new_tokens}")
        self.instrument_tokens = list(new_tokens)
        for ns in self._active_namespaces:
            await self._subscribe(namespace=ns)

    async def _heartbeat_loop(self) -> None:
        """Periodically ping socket to ensure connection liveness."""
        while self._running and self._sio and self._sio.connected:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            try:
                await self._sio.emit("ping", {})
            except Exception:
                break
