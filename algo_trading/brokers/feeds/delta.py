"""
algo_trading/brokers/feeds/delta.py
───────────────────────────────────
Delta Exchange WebSocket feed using python ``aiohttp`` client.
Supports Delta Exchange Global (wss://socket.delta.exchange)
and Delta Exchange India (wss://socket.india.delta.exchange).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import logging
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import orjson

from algo_trading.brokers.base import BaseFeed

logger = logging.getLogger("algo_trading.brokers")


class DeltaExchangeFeed(BaseFeed):
    """
    WebSocket streaming feed for Delta Exchange (Global & India).
    Receives real-time tickers (v2/ticker), trades (all_trades), orderbooks (l2_orderbook),
    and user execution updates.
    """

    BROKER_NAME = "delta_exchange"
    ALIASES = ["delta", "delta_india", "deltaexchange", "delta_feed"]

    GLOBAL_SOCKET_URL = "wss://socket.delta.exchange"
    INDIA_SOCKET_URL = "wss://public-socket.india.delta.exchange"
    INDIA_SOCKET_URL_LEGACY = "wss://socket.india.delta.exchange"
    HEARTBEAT_INTERVAL = 30  # seconds
    DEFAULT_INSTRUMENT_TOKENS = ["BTCUSD", "ETHUSD", "SOLUSD"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None

    def _determine_socket_url(self) -> str:
        """Resolve appropriate WebSocket URL based on broker configuration."""
        endpoint = (self.api_endpoint or "").lower().strip()
        if endpoint:
            if "india" in endpoint:
                return self.INDIA_SOCKET_URL
            if endpoint.startswith("wss://"):
                return self.api_endpoint

        # Check broker name / provider code
        ident = f"{self.broker_name}_{self.api_provider}_{self.account_id}".lower()
        if "india" in ident:
            return self.INDIA_SOCKET_URL
        return self.GLOBAL_SOCKET_URL

    # ──────────────────────────────────────────────────────────────
    # BaseFeed Abstract Implementation
    # ──────────────────────────────────────────────────────────────

    async def connect_and_stream(self) -> None:
        """
        Connect to Delta Exchange WebSocket, authenticate if credentials are provided,
        subscribe to instrument channels, and route incoming frames into the tick queue.
        """
        socket_url = self._determine_socket_url()
        self.log("INFO", f"Connecting to Delta Exchange WebSocket at {socket_url}...")

        async with aiohttp.ClientSession() as session:
            self._session = session
            async with session.ws_connect(socket_url, heartbeat=25.0) as ws:
                self._ws = ws
                self.log("INFO", "Connected to Delta Exchange WebSocket. Initializing channels...")

                # 1. Authenticate private channels if API credentials present
                if self.api_key and self.api_secret:
                    await self._send_auth()

                # 2. Subscribe to market data channels
                await self._subscribe(self.instrument_tokens or self.DEFAULT_INSTRUMENT_TOKENS)

                # 3. Spawn heartbeat task
                heartbeat_task = asyncio.create_task(self._heartbeat_loop())

                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_raw_message(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            await self._handle_raw_message(msg.data.decode("utf-8", errors="ignore"))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            self.log("WARNING", f"WebSocket closed or errored: {msg}")
                            break
                finally:
                    heartbeat_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await heartbeat_task
                    self._ws = None

    async def _send_auth(self) -> None:
        """Send WebSocket authentication frame using HMAC-SHA256 signature."""
        if not self._ws or self._ws.closed:
            return

        try:
            timestamp = str(int(time.time()))
            msg = "GET" + timestamp + "/live"
            signature = hmac.new(
                self.api_secret.encode("utf-8"),
                msg.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()

            auth_frame = {
                "type": "auth",
                "payload": {
                    "api-key": self.api_key,
                    "signature": signature,
                    "timestamp": timestamp,
                },
            }
            await self._ws.send_str(orjson.dumps(auth_frame).decode("utf-8"))
            self.log("INFO", "Dispatched WebSocket authentication frame.")
        except Exception as exc:
            self.log("ERROR", f"Failed to send Delta WebSocket auth: {exc}")

    async def _subscribe(self, symbols: list[str]) -> None:
        """Send public market data channel subscriptions in bounded batches."""
        if not self._ws or self._ws.closed or not symbols:
            return

        symbols_clean = [str(s).strip() for s in symbols if str(s).strip()]
        if not symbols_clean:
            return

        socket_url = self._determine_socket_url()
        ticker_channel = "ticker" if "public-socket" in socket_url else "v2/ticker"
        channel_names = [ticker_channel, "all_trades", "l2_orderbook"]
        batch_size = 50

        for i in range(0, len(symbols_clean), batch_size):
            chunk = symbols_clean[i : i + batch_size]
            for ch in channel_names:
                sub_frame = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [{"name": ch, "symbols": chunk}]
                    }
                }
                try:
                    await self._ws.send_str(orjson.dumps(sub_frame).decode("utf-8"))
                except Exception as e:
                    self.log("WARNING", f"Failed to subscribe channel {ch}: {e}")

        self.log("INFO", f"Subscribed to Delta Exchange channels for {len(symbols_clean)} symbols in batches of {batch_size}.")

    async def _unsubscribe(self, symbols: list[str]) -> None:
        """Send channel unsubscription frame in bounded batches."""
        if not self._ws or self._ws.closed or not symbols:
            return

        symbols_clean = [str(s).strip() for s in symbols if str(s).strip()]
        if not symbols_clean:
            return

        socket_url = self._determine_socket_url()
        ticker_channel = "ticker" if "public-socket" in socket_url else "v2/ticker"
        channel_names = [ticker_channel, "all_trades", "l2_orderbook"]
        batch_size = 50

        for i in range(0, len(symbols_clean), batch_size):
            chunk = symbols_clean[i : i + batch_size]
            for ch in channel_names:
                unsub_frame = {
                    "type": "unsubscribe",
                    "payload": {
                        "channels": [{"name": ch, "symbols": chunk}]
                    }
                }
                try:
                    await self._ws.send_str(orjson.dumps(unsub_frame).decode("utf-8"))
                except Exception:
                    pass

        self.log("INFO", f"Unsubscribed from Delta Exchange channels for {len(symbols_clean)} symbols.")

    async def _heartbeat_loop(self) -> None:
        """Send periodic ping frames to keep connection alive."""
        while self._ws and not self._ws.closed:
            try:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                if self._ws and not self._ws.closed:
                    ping_payload = {"type": "ping"}
                    await self._ws.send_str(orjson.dumps(ping_payload).decode("utf-8"))
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.log("WARNING", f"Error in heartbeat ping: {exc}")

    async def _handle_raw_message(self, raw_str: str) -> None:
        """Process incoming WebSocket JSON messages."""
        try:
            data = orjson.loads(raw_str)
        except Exception:
            return

        msg_type = data.get("type")
        if msg_type in ("pong", "subscriptions", "auth", "error", "success"):
            if msg_type == "error":
                self.log("WARNING", f"Delta stream error: {data.get('message', data)}")
            return

        # Delta frames typically have 'channel' or 'action' or direct fields
        channel = data.get("channel") or data.get("type", "")
        symbol = data.get("symbol") or data.get("s")

        # Route ticker / trades / orderbook frames
        if symbol or channel:
            await self.enqueue_tick(data)

    def normalize_tick(self, raw_data: Any) -> dict:
        """
        Normalize Delta Exchange WebSocket data payload into unified DeltaZero26 tick dictionary.
        """
        if not isinstance(raw_data, dict):
            return self.make_frame({"raw": raw_data})

        # Check if new Delta India compact ticker format with 'd' array
        d_items = raw_data.get("d")
        if d_items and isinstance(d_items, list) and len(d_items) > 0 and isinstance(d_items[0], dict):
            d0 = d_items[0]
            symbol = raw_data.get("sy") or raw_data.get("symbol") or d0.get("s") or ""
            ohlc = d0.get("ohlc") or []
            if len(ohlc) >= 4:
                open_price = float(ohlc[0] or 0.0)
                high_price = float(ohlc[1] or 0.0)
                low_price = float(ohlc[2] or 0.0)
                close_price = float(ohlc[3] or 0.0)
                last_price = close_price or float(d0.get("m") or raw_data.get("sp") or 0.0)
            else:
                last_price = float(d0.get("m") or raw_data.get("sp") or 0.0)
                open_price = high_price = low_price = close_price = last_price

            q = d0.get("q") or []
            if len(q) >= 4:
                buy_demand = float(q[1] or 0.0)
                sell_demand = float(q[3] or 0.0)
                bids = [[float(q[0] or 0.0), buy_demand]]
                asks = [[float(q[2] or 0.0), sell_demand]]
            else:
                buy_demand = 0.0
                sell_demand = 0.0
                bids = []
                asks = []

            to = d0.get("to") or []
            volume = float(to[0] if to else 0.0)
        else:
            # Extract trading symbol
            symbol = (
                raw_data.get("symbol")
                or raw_data.get("s")
                or raw_data.get("product_symbol")
                or raw_data.get("sy")
                or ""
            )

            # Depth structures
            bids = raw_data.get("buy") or raw_data.get("bids") or []
            asks = raw_data.get("sell") or raw_data.get("asks") or []

            # Trade list extraction
            trades = raw_data.get("trades") or []
            if trades and isinstance(trades, list):
                latest_trade = trades[0] if isinstance(trades[0], dict) else {}
                trade_price = float(latest_trade.get("price") or latest_trade.get("p") or 0.0)
                trade_vol = float(latest_trade.get("size") or latest_trade.get("s") or 0.0)
            else:
                trade_price = 0.0
                trade_vol = 0.0

            # Extract last price
            last_price = float(
                raw_data.get("close")
                or raw_data.get("mark_price")
                or raw_data.get("spot_price")
                or raw_data.get("price")
                or raw_data.get("p")
                or raw_data.get("last_price")
                or trade_price
                or (float(bids[0][0]) if bids and isinstance(bids[0], (list, tuple)) and len(bids[0]) > 0 else 0.0)
                or (float(asks[0][0]) if asks and isinstance(asks[0], (list, tuple)) and len(asks[0]) > 0 else 0.0)
                or 0.0
            )

            # Extract Open, High, Low, Close
            open_price = float(raw_data.get("open") or raw_data.get("o") or last_price)
            high_price = float(raw_data.get("high") or raw_data.get("h") or last_price)
            low_price = float(raw_data.get("low") or raw_data.get("l") or last_price)
            close_price = float(raw_data.get("close") or raw_data.get("c") or last_price)

            volume = float(raw_data.get("volume") or raw_data.get("v") or raw_data.get("size") or trade_vol or 0.0)

            # Depth / Demands
            buy_demand = float(
                raw_data.get("buy_demand")
                or raw_data.get("bid_qty")
                or (float(bids[0][1]) if bids and isinstance(bids[0], (list, tuple)) and len(bids[0]) > 1 else 0.0)
            )
            sell_demand = float(
                raw_data.get("sell_demand")
                or raw_data.get("ask_qty")
                or (float(asks[0][1]) if asks and isinstance(asks[0], (list, tuple)) and len(asks[0]) > 1 else 0.0)
            )

        # Extract exchange timestamp if provided in micro/milliseconds, fallback to now
        d0_ts = d_items[0].get("ts") if (d_items and isinstance(d_items, list) and len(d_items) > 0 and isinstance(d_items[0], dict)) else None
        ts_raw = raw_data.get("ts") or d0_ts
        if ts_raw and isinstance(ts_raw, (int, float)) and ts_raw > 1e11:
            divisor = 1e6 if ts_raw > 1e14 else 1e3
            try:
                timestamp_val = datetime.fromtimestamp(ts_raw / divisor, tz=timezone.utc).isoformat()
            except Exception:
                timestamp_val = datetime.now(timezone.utc).isoformat()
        else:
            timestamp_val = raw_data.get("timestamp") or raw_data.get("time") or datetime.now(timezone.utc).isoformat()

        normalized_data = {
            "instrument_token": symbol,
            "tradingsymbol": symbol,
            "symbol": symbol,
            "last_price": last_price,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
            "buy_demand": buy_demand,
            "sell_demand": sell_demand,
            "depth": {"buy": bids, "sell": asks},
            "date_time": timestamp_val,
            "raw": raw_data,
        }

        return self.make_frame(normalized_data)

    async def on_tokens_changed(self, old_tokens: list, new_tokens: list) -> None:
        """Handle live instrument token configuration updates."""
        if not self._ws or self._ws.closed:
            return

        old_set = set(old_tokens)
        new_set = set(new_tokens)

        added = list(new_set - old_set)
        removed = list(old_set - new_set)

        if removed:
            await self._unsubscribe(removed)
        if added:
            await self._subscribe(added)
