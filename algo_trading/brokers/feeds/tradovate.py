"""
algo_trading/brokers/feeds/tradovate.py
────────────────────────────────────────
Tradovate WebSocket feed using python ``aiohttp`` client.

Tradovate uses a REST auth API to get an access token, then connects
to a SockJS WebSocket server where it sends/receives newline-delimited commands.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

import aiohttp
import orjson

from algo_trading.brokers.base import BaseFeed

logger = logging.getLogger("algo_trading.brokers")


class TradovateFeed(BaseFeed):
    """WebSocket feed for Tradovate."""

    BROKER_NAME = "tradovate"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._seq = 1

    def _read_broker_config(self) -> str | None:
        from kalai.models import Broker
        from django.db.models import Q
        try:
            broker_obj = Broker.objects.filter(Q(account_id=self.account_id) | Q(name=self.account_id)).first()
            return broker_obj.api_endpoint if broker_obj else None
        except Exception:
            return None

    # ──────────────────────────────────────────────────────────────
    # BaseFeed abstract implementation
    # ──────────────────────────────────────────────────────────────

    async def connect_and_stream(self) -> None:
        """
        1. Fetch accessToken using HTTP POST.
        2. Open aiohttp WebSocket connection to the market data endpoint.
        3. Perform SockJS authorize command.
        4. Spawn background heartbeat loop.
        5. Subscribe to configured CME instrument symbols.
        """
        # Parse credentials from colon-separated fields
        try:
            username, app_id = self.api_key.split(":", 1)
            password, sec_key = self.api_secret.split(":", 1)
            
            api_endpoint_str = await asyncio.to_thread(self._read_broker_config)
            cid = 0
        except Exception as exc:
            self.log("ERROR", f"Invalid credential formatting in api_key or api_secret: {exc}")
            return

        # Prepare REST auth URL
        api_url = api_endpoint_str or "https://demo.tradovateapi.com/v1"
        api_url = api_url.rstrip("/")
        auth_url = f"{api_url}/auth/accesstokenrequest"

        auth_payload = {
            "name": username,
            "password": password,
            "appId": app_id,
            "appVersion": "1.0",
            "cid": cid,
            "sec": sec_key
        }

        self.log("INFO", f"Requesting access token from {auth_url}...")

        # Request token
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(auth_url, json=auth_payload) as resp:
                    if resp.status != 200:
                        err_text = await resp.text()
                        self.log("ERROR", f"REST authentication failed (HTTP {resp.status}): {err_text}")
                        return
                    res_data = await resp.json()
                    access_token = res_data.get("accessToken")
                    if not access_token:
                        self.log("ERROR", f"No accessToken found in response payload: {res_data}")
                        return
        except Exception as exc:
            self.log("ERROR", f"REST HTTP auth request failed: {exc}")
            return

        # Determine WebSocket URL
        if "demo.tradovateapi.com" in api_url:
            ws_url = "wss://md-demo.tradovateapi.com/v1/websocket"
        else:
            ws_url = "wss://md.tradovateapi.com/v1/websocket"

        self.log("INFO", f"Connecting to WebSocket at {ws_url}...")

        # Connect WebSocket
        disconnect_event = asyncio.Event()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url) as ws:
                    self._ws = ws
                    self._seq = 1
                    authorized = False

                    self.log("INFO", "WebSocket connected. Spawning heartbeat loop...")
                    heartbeat_task = asyncio.create_task(self._send_heartbeats(ws))

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.data
                            if not data:
                                continue

                            frame_type = data[0]
                            if frame_type == 'o':
                                # Connection open -> send authorize request
                                self.log("INFO", "SockJS Open frame received. Authorizing...")
                                auth_cmd = f"authorize\n{self._seq}\n\n" + orjson.dumps({"token": access_token}).decode("utf-8")
                                await ws.send_str(auth_cmd)
                                self._seq += 1
                            elif frame_type == 'h':
                                # Server heartbeat, ignore
                                pass
                            elif frame_type == 'a':
                                # Array of data messages
                                try:
                                    raw_msgs = orjson.loads(data[1:])
                                    for raw_m in raw_msgs:
                                        if isinstance(raw_m, str):
                                            inner = orjson.loads(raw_m)
                                        else:
                                            inner = raw_m

                                        # Check for authorization confirmation
                                        if not authorized:
                                            if isinstance(inner, dict) and inner.get("i") == 1:
                                                if inner.get("s") == 200:
                                                    authorized = True
                                                    self.log("INFO", f"Authorized. Subscribing to {len(self.instrument_tokens)} symbols...")
                                                    for token in self.instrument_tokens:
                                                        sub_cmd = f"md/subscribeQuote\n{self._seq}\n\n" + orjson.dumps({"symbol": str(token)}).decode("utf-8")
                                                        await ws.send_str(sub_cmd)
                                                        self._seq += 1
                                                else:
                                                    self.log("ERROR", f"Authorization failed: {inner}")
                                                    disconnect_event.set()
                                                    break

                                        # Enqueue quotes/ticks
                                        if isinstance(inner, dict) and inner.get("e") == "props":
                                            quotes = inner.get("d", {}).get("quotes", [])
                                            for q in quotes:
                                                await self.enqueue_tick(q)
                                        else:
                                            await self.enqueue_tick(inner)
                                except Exception as e:
                                    self.log("WARNING", f"Error parsing frame messages: {e}")
                            elif frame_type == 'c':
                                self.log("WARNING", f"SockJS close frame: {data}")
                                disconnect_event.set()
                                break
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            self.log("WARNING", f"WebSocket closed/error: {msg.data}")
                            disconnect_event.set()
                            break

                    # Terminate heartbeats
                    heartbeat_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await heartbeat_task

        except Exception as exc:
            self.log("ERROR", f"WebSocket runtime error: {exc}")
        finally:
            self._ws = None

        self.log("WARNING", "WebSocket connection loop completed.")

    def normalize_tick(self, raw_data: Any) -> dict:
        """Wrap a CME quote tick into the unified frame format."""
        return self.make_frame(raw_data)

    # ──────────────────────────────────────────────────────────────
    # Dynamic Token Updates
    # ──────────────────────────────────────────────────────────────

    async def on_tokens_changed(self, old_tokens: list, new_tokens: list) -> None:
        """Dynamically subscribe/unsubscribe from symbols when AlgoInfo updates."""
        ws = self._ws
        if ws is None or ws.closed:
            return

        self.log("INFO", f"Dynamic token changes detected: {len(old_tokens)} -> {len(new_tokens)}")

        # Unsubscribe from old symbols
        for token in old_tokens:
            if token not in new_tokens:
                unsub_cmd = f"md/unsubscribeQuote\n{self._seq}\n\n" + orjson.dumps({"symbol": str(token)}).decode("utf-8")
                await ws.send_str(unsub_cmd)
                self._seq += 1

        # Subscribe to new symbols
        for token in new_tokens:
            if token not in old_tokens:
                sub_cmd = f"md/subscribeQuote\n{self._seq}\n\n" + orjson.dumps({"symbol": str(token)}).decode("utf-8")
                await ws.send_str(sub_cmd)
                self._seq += 1

    # ──────────────────────────────────────────────────────────────
    # Heartbeat Task
    # ──────────────────────────────────────────────────────────────

    async def _send_heartbeats(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Send empty SockJS frames every 2.5 seconds to keep session alive."""
        try:
            while True:
                await asyncio.sleep(2.5)
                if ws.closed:
                    break
                await ws.send_str("[]")
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.log("WARNING", f"Heartbeat failed: {exc}")
