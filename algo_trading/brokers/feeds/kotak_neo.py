"""
algo_trading/brokers/feeds/kotak_neo.py
───────────────────────────────────────
Kotak Neo WebSocket feed using HSWebSocket protocol.

Connects to Kotak Neo HSM streaming server (wss://mlhsm.kotaksecurities.com),
subscribes to live market quote/depth streams, and pushes raw tick payloads
directly into the async engine's per-account ``tick_queue`` via ``normalize_tick``.

Follows the exact same lifecycle and BaseFeed structure as Zerodha.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any

from algo_trading.brokers.base import BaseFeed
from algo_trading.brokers.feeds.hs_websocket import HSWebSocket

logger = logging.getLogger("algo_trading.brokers")


def format_kotak_scrip(token: Any, segment_map: dict[str, str] | None = None) -> str:
    """
    Format a token or scrip identifier into Kotak Neo's required 'exchange|token' format.
    Examples:
        - 11536                 -> "nse_cm|11536" (or "nse_fo|11536" if found in segment_map)
        - "nse_cm|11536"        -> "nse_cm|11536"
        - "NSE|11536"           -> "nse_cm|11536"
        - "NFO|54321"           -> "nse_fo|54321"
        - "BSE|500325"          -> "bse_cm|500325"
        - "MCX|426123"          -> "mcx_fo|426123"
        - {"segment": "nse_fo", "token": "54321"} -> "nse_fo|54321"
    """
    if isinstance(token, dict):
        seg = token.get("segment") or token.get("exchange_segment") or token.get("exchange", "nse_cm")
        tok = token.get("token") or token.get("tokenid") or token.get("instrument_token", "")
        tok_str = str(tok).strip()
        if segment_map and tok_str in segment_map:
            return f"{segment_map[tok_str]}|{tok_str}"
        return f"{_map_exchange(seg)}|{tok_str}"
    
    token_str = str(token).strip()
    if "|" in token_str:
        seg, tok = token_str.split("|", 1)
        tok_str = tok.strip()
        if segment_map and tok_str in segment_map:
            return f"{segment_map[tok_str]}|{tok_str}"
        return f"{_map_exchange(seg)}|{tok_str}"
    
    # Check provided segment mapping cache
    if segment_map and token_str in segment_map:
        return f"{segment_map[token_str]}|{token_str}"
    
    # Heuristic fallback for plain tokens: F&O option/futures tokens on Kotak Neo are typically > 20000
    try:
        tkn_val = int(token_str)
        if tkn_val > 20000:
            return f"nse_fo|{token_str}"
    except (ValueError, TypeError):
        pass

    # Default to nse_cm if unmapped
    return f"nse_cm|{token_str}"


def _map_exchange(exchange: str) -> str:
    """Map common exchange strings to Kotak Neo segment identifiers."""
    ex = exchange.lower().strip()
    mapping = {
        "nse": "nse_cm",
        "nse_cm": "nse_cm",
        "equity": "nse_cm",
        "nfo": "nse_fo",
        "nfo-fut": "nse_fo",
        "nfo-opt": "nse_fo",
        "nfo_fut": "nse_fo",
        "nfo_opt": "nse_fo",
        "nse_fo": "nse_fo",
        "fno": "nse_fo",
        "bse": "bse_cm",
        "bse_cm": "bse_cm",
        "bfo": "bse_fo",
        "bfo-fut": "bse_fo",
        "bfo-opt": "bse_fo",
        "bfo_fut": "bse_fo",
        "bfo_opt": "bse_fo",
        "bse_fo": "bse_fo",
        "mcx": "mcx_fo",
        "mcx-fut": "mcx_fo",
        "mcx-opt": "mcx_fo",
        "mcx_fut": "mcx_fo",
        "mcx_opt": "mcx_fo",
        "mcx_fo": "mcx_fo",
        "cds": "cde_fo",
        "cde_fo": "cde_fo",
        "bcd": "bcs-fo",
        "bcs_fo": "bcs-fo",
        "nse_index": "nse_cm",
        "bse_index": "bse_cm",
    }
    return mapping.get(ex, ex)


class KotakNeoFeed(BaseFeed):
    """WebSocket feed for Kotak Neo API."""

    BROKER_NAME = "kotak_neo"
    ALIASES = ["kotak"]

    DEFAULT_WS_URL = "wss://mlhsm.kotaksecurities.com"
    MAX_BATCH_SIZE = 100  # Kotak HSM limit per subscription frame

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.ws_url = self.DEFAULT_WS_URL
        self._hs_ws: HSWebSocket | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._disconnect_event: asyncio.Event | None = None
        self._send_lock = threading.RLock()
        self._authenticated = False
        self._token_segment_cache: dict[str, str] = {}
        try:
            self._refresh_segment_cache()
        except Exception as exc:
            logger.warning("KotakNeoFeed segment cache pre-population warning: %s", exc)

    # ──────────────────────────────────────────────────────────────
    # BaseFeed abstract implementation
    # ──────────────────────────────────────────────────────────────

    async def connect_and_stream(self) -> None:
        """
        Open the Kotak Neo HSM WebSocket connection on a background thread.
        Subscribes to instrument tokens and enqueues incoming raw tick messages.
        Blocks until disconnect or cancellation.
        """
        self._loop = asyncio.get_running_loop()
        self._disconnect_event = asyncio.Event()
        self._authenticated = False

        # Parse token and session id from access_token
        auth_token, sid = self._parse_auth_credentials()

        if not auth_token:
            self.log("ERROR", "No valid authentication token available for Kotak Neo. Check access_token.")
            await asyncio.sleep(5)
            return

        # Pre-load segment cache from DB in worker thread
        await asyncio.to_thread(self._refresh_segment_cache)

        self._hs_ws = HSWebSocket()
        self._hs_ws._send_lock = self._send_lock

        # ── Callbacks ─────────────────────────────────────────
        def on_open():
            self.log("INFO", f"Kotak Neo WebSocket connected. Authenticating sid='{sid}'...")
            auth_req = {
                "type": "cn",
                "Authorization": auth_token,
                "Sid": sid,
            }
            try:
                with self._send_lock:
                    self._hs_ws.hs_send(json.dumps(auth_req))
                self.log("INFO", "Kotak Neo auth handshake frame sent.")
            except Exception as exc:
                self.log("ERROR", f"Error sending Kotak auth handshake: {exc}")
                return

            # Start background heartbeat ping thread (sends 'hb' every 25s)
            def _hb_loop():
                import time
                while self._hs_ws and not (self._disconnect_event and self._disconnect_event.is_set()):
                    time.sleep(25)
                    try:
                        with self._send_lock:
                            if self._hs_ws:
                                self._hs_ws.hs_send(json.dumps({"type": "hb"}))
                    except Exception:
                        break

            threading.Thread(target=_hb_loop, daemon=True).start()

        def on_message(raw_data):
            if not raw_data:
                return

            # Parse JSON string if received as string
            if isinstance(raw_data, str):
                try:
                    raw_data = json.loads(raw_data)
                except Exception:
                    pass

            # Check for connection handshake ACK
            if isinstance(raw_data, list) and len(raw_data) > 0 and isinstance(raw_data[0], dict):
                first = raw_data[0]
                if first.get("type") == "cn":
                    if first.get("stat") == "Ok" or first.get("stCode") == 200:
                        self.log("INFO", f"Kotak Neo connection authenticated: {first}")
                        self._authenticated = True
                        if self.instrument_tokens:
                            self.log("INFO", f"Subscribing to {len(self.instrument_tokens)} Kotak Neo tokens post-auth.")
                            self._subscribe_tokens(self.instrument_tokens)
                    else:
                        self.log("ERROR", f"Kotak Neo connection auth failed: {first}")
                    return
            elif isinstance(raw_data, dict) and raw_data.get("type") == "cn":
                if raw_data.get("stat") == "Ok" or raw_data.get("stCode") == 200:
                    self.log("INFO", f"Kotak Neo connection authenticated: {raw_data}")
                    self._authenticated = True
                    if self.instrument_tokens:
                        self.log("INFO", f"Subscribing to {len(self.instrument_tokens)} Kotak Neo tokens post-auth.")
                        self._subscribe_tokens(self.instrument_tokens)
                else:
                    self.log("ERROR", f"Kotak Neo connection auth failed: {raw_data}")
                return

            # Filter out control/handshake/ack frames from tick queue
            if isinstance(raw_data, list):
                valid_ticks = []
                for item in raw_data:
                    if not isinstance(item, dict):
                        continue
                    msg_type = item.get("type")
                    if msg_type in ("cn", "sub", "unsub", "hb", "cr", "cp") or "stCode" in item:
                        if msg_type in ("sub", "unsub"):
                            self.log("INFO", f"Kotak Neo subscription status: {item}")
                        continue
                    valid_ticks.append(item)
                if not valid_ticks:
                    return
                raw_data = valid_ticks
            elif isinstance(raw_data, dict):
                msg_type = raw_data.get("type")
                if msg_type in ("cn", "sub", "unsub", "hb", "cr", "cp") or "stCode" in raw_data:
                    if msg_type in ("sub", "unsub"):
                        self.log("INFO", f"Kotak Neo subscription status: {raw_data}")
                    return

            if self._loop and self._tick_queue is not None:
                if isinstance(raw_data, list):
                    # Kotak sometimes delivers a list of tick dicts in one message.
                    # Enqueue each tick individually so every record in the stream
                    # table / ProcessedTickStore represents a single instrument tick.
                    for tick_item in raw_data:
                        self._loop.call_soon_threadsafe(
                            self._tick_queue.put_nowait,
                            self.normalize_tick(tick_item),
                        )
                else:
                    self._loop.call_soon_threadsafe(
                        self._tick_queue.put_nowait,
                        self.normalize_tick(raw_data),
                    )

        def on_close(code=None, reason=None):
            self.log("WARNING", f"Kotak Neo WebSocket closed: code={code}, reason={reason}")
            self._authenticated = False
            if self._loop and self._disconnect_event and not self._disconnect_event.is_set():
                self._loop.call_soon_threadsafe(self._disconnect_event.set)

        def on_error(error):
            self.log("ERROR", f"Kotak Neo WebSocket error: {error}")

        # ── Start connection on background thread ─────────────
        self.log("INFO", f"Connecting to Kotak Neo WebSocket at {self.ws_url}…")

        def _run_socket():
            try:
                self._hs_ws.open_connection(
                    url=self.ws_url,
                    token=auth_token,
                    sid=sid,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
            except Exception as exc:
                self.log("ERROR", f"Kotak Neo connection exception: {exc}")
                if self._loop and self._disconnect_event and not self._disconnect_event.is_set():
                    self._loop.call_soon_threadsafe(self._disconnect_event.set)

        conn_thread = threading.Thread(target=_run_socket, daemon=True)
        conn_thread.start()

        # Wait for disconnect or cancellation
        try:
            await self._disconnect_event.wait()
        finally:
            self._stop_client()

    def normalize_tick(self, raw_data: Any) -> dict:
        """
        Wrap raw Kotak tick dict directly into the unified frame format.
        No normalization or field extraction (exact same design as Zerodha).
        """
        return self.make_frame(raw_data)

    # ──────────────────────────────────────────────────────────────
    # Token refresh — dynamic live re-subscription
    # ──────────────────────────────────────────────────────────────

    async def on_tokens_changed(self, old_tokens: list, new_tokens: list) -> None:
        """Called by BaseFeed when instrument tokens change in the DB."""
        if self._hs_ws is None or not self._authenticated:
            return

        await asyncio.to_thread(self._refresh_segment_cache)
        old_formatted = {format_kotak_scrip(t, self._token_segment_cache) for t in old_tokens if t}
        new_formatted = {format_kotak_scrip(t, self._token_segment_cache) for t in new_tokens if t}

        to_unsub = list(old_formatted - new_formatted)
        to_sub = list(new_formatted - old_formatted)

        self.log(
            "INFO",
            f"Dynamic token update: unsubscribing {len(to_unsub)}, subscribing {len(to_sub)}",
        )

        def _update():
            try:
                if to_unsub:
                    self._unsubscribe_tokens(to_unsub)
                if to_sub:
                    self._subscribe_tokens(to_sub)
            except Exception as exc:
                logger.error("Kotak Neo live re-subscription failed: %s", exc)

        await asyncio.to_thread(_update)

    # ──────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────

    def _refresh_segment_cache(self) -> None:
        """Load/refresh token-to-segment mapping from DB AlgoInfo and ExchangeMasterData tables."""
        from django.db import connection
        try:
            from kalai.models import AlgoInfo, Broker, ExchangeMasterData
            broker_obj = Broker.resolve(self.account_id)
            if not broker_obj:
                return

            # 1. Primary: Inspect ExchangeMasterData (cum_table, aug_table, etc.) for broker and global
            try:
                import orjson
                emd_qs = ExchangeMasterData.objects.filter(account=broker_obj) | ExchangeMasterData.objects.filter(account=None)
                for emd in emd_qs:
                    data = emd.tabledata
                    if not data:
                        continue
                    if isinstance(data, str):
                        try:
                            data = orjson.loads(data)
                        except Exception:
                            continue
                    if isinstance(data, dict):
                        data = data.get("cum_table") or data.get("aug_table") or [data]
                    if isinstance(data, list):
                        for item in data:
                            if not isinstance(item, dict):
                                continue
                            seg = (
                                item.get("exchange_segment")
                                or item.get("Exchange_type")
                                or item.get("exchange")
                                or item.get("segment")
                                or ""
                            )
                            mapped_seg = _map_exchange(seg) if seg else ""
                            for key in ("instrument_token", "pToken", "token", "Index_tkn", "Ref_stock_tkn", "tokenid"):
                                tkn = item.get(key)
                                if tkn and mapped_seg:
                                    self._token_segment_cache[str(tkn).strip()] = mapped_seg
            except Exception as emd_err:
                logger.warning("KotakNeoFeed ExchangeMasterData cache refresh warning: %s", emd_err)

            # 2. Secondary: Inspect AlgoInfo auxiliary instrument tables
            tables_to_segment = {
                "nfo_instrument_data": "nse_fo",
                "nse_instrument_data": "nse_cm",
                "mcx_instrument_data": "mcx_fo",
                "bfo_instrument_data": "bse_fo",
                "bse_instrument_data": "bse_cm",
                "cds_instrument_data": "cde_fo",
            }

            for tbl, default_seg in tables_to_segment.items():
                record = AlgoInfo.objects.filter(account=broker_obj, tablename=tbl).first()
                if not record or not record.tabledata:
                    continue
                data = record.tabledata
                if isinstance(data, str):
                    try:
                        import orjson
                        data = orjson.loads(data)
                    except Exception:
                        continue
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            tkn = (
                                item.get("instrument_token")
                                or item.get("pToken")
                                or item.get("pScripRefKey")
                                or item.get("token")
                            )
                            seg = (
                                item.get("exchange_segment")
                                or item.get("pExchSeg")
                                or item.get("segment")
                                or default_seg
                            )
                            if tkn:
                                self._token_segment_cache[str(tkn).strip()] = _map_exchange(seg)
        except Exception as e:
            logger.warning("KotakNeoFeed segment cache refresh warning: %s", e)
        finally:
            try:
                connection.close()
            except Exception:
                pass

    def _parse_auth_credentials(self) -> tuple[str, str]:
        """
        Extract auth token and sid from access_token field.
        Supports multiple formats:
          - OpenAlgo format: "trading_token:::trading_sid:::base_url:::access_token"
          - JSON format: {"token": "...", "sid": "..."}
          - Raw token: uses token as auth_token and account_id as sid
        """
        raw = (self.access_token or "").strip()
        if not raw:
            return "", ""

        # 1. OpenAlgo separator format
        if ":::" in raw:
            parts = raw.split(":::")
            auth_token = parts[0] if len(parts) > 0 else ""
            sid = parts[1] if len(parts) > 1 else self.account_id
            return auth_token, sid

        # 2. JSON format
        if raw.startswith("{") and raw.endswith("}"):
            try:
                data = json.loads(raw)
                auth_token = data.get("token") or data.get("trading_token") or data.get("access_token") or ""
                sid = data.get("sid") or data.get("trading_sid") or self.account_id
                return auth_token, sid
            except Exception:
                pass

        # 3. Standard single string token
        return raw, self.account_id

    def _subscribe_tokens(self, tokens: list) -> None:
        """Send subscription frames in batches of up to MAX_BATCH_SIZE."""
        if not self._hs_ws or not tokens:
            return

        # Ensure segment cache is populated
        if not self._token_segment_cache:
            try:
                self._refresh_segment_cache()
            except Exception:
                pass

        formatted = [format_kotak_scrip(t, self._token_segment_cache) for t in tokens if t]
        unique_formatted = list(dict.fromkeys(formatted))

        for i in range(0, len(unique_formatted), self.MAX_BATCH_SIZE):
            chunk = unique_formatted[i : i + self.MAX_BATCH_SIZE]
            scrips_str = "&".join(chunk)
            msg = {
                "type": "mws",
                "scrips": scrips_str,
                "channelnum": "1",
            }
            try:
                with self._send_lock:
                    self._hs_ws.hs_send(json.dumps(msg))
                self.log("INFO", f"Subscribed to {len(chunk)} Kotak Neo scrips: {scrips_str[:80]}...")
            except Exception as exc:
                self.log("ERROR", f"Error sending subscription frame: {exc}")

    def _unsubscribe_tokens(self, tokens: list) -> None:
        """Send unsubscribe frames in batches."""
        if not self._hs_ws or not tokens:
            return

        if not self._token_segment_cache:
            try:
                self._refresh_segment_cache()
            except Exception:
                pass

        formatted = [format_kotak_scrip(t, self._token_segment_cache) for t in tokens if t]
        unique_formatted = list(dict.fromkeys(formatted))

        for i in range(0, len(unique_formatted), self.MAX_BATCH_SIZE):
            chunk = unique_formatted[i : i + self.MAX_BATCH_SIZE]
            scrips_str = "&".join(chunk)
            msg = {
                "type": "mwu",
                "scrips": scrips_str,
                "channelnum": "1",
            }
            try:
                with self._send_lock:
                    self._hs_ws.hs_send(json.dumps(msg))
            except Exception as exc:
                self.log("ERROR", f"Error sending unsubscribe frame: {exc}")

    def _stop_client(self) -> None:
        """Safely close the Kotak Neo connection."""
        self._authenticated = False
        if self._hs_ws is not None:
            try:
                self._hs_ws.close()
            except Exception:
                pass
            self._hs_ws = None


# Alias for backward compatibility / alternate naming
KotakFeed = KotakNeoFeed

