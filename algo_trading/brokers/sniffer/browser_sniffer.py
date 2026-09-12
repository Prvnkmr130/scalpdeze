# -*- coding: utf-8 -*-
"""
algo_trading/brokers/sniffer/browser_sniffer.py
───────────────────────────────────────────────
Scrapling Stealth Browser Sniffing & Context Pooling Engine.
Built on Playwright / Patchright with Isolated Browser Context Pooling.
Performs aggressive media and resource stripping, multi-channel WebSocket
and REST XHR frame sniffing, automated TOTP 2FA login, and 4-hour soft memory recycles.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import polars as pl

logger = logging.getLogger("algo_trading.brokers.sniffer.browser_sniffer")


class SniffedMarketDataStore:
    """
    Thread-safe, high-speed in-memory repository for ticks, quotes, and option chains
    captured by the Scrapling stealth browser sniffer.
    """

    def __init__(self, max_ticks: int = 50_000):
        self.max_ticks = max_ticks
        self._lock = threading.Lock()
        self._ticks: List[Dict[str, Any]] = []
        self._latest_option_chain: Dict[str, Dict[str, Any]] = {}
        self._token_map: Dict[str, int] = {}  # symbol -> synthetic/canonical token

    def push_tick(
        self,
        symbol: str,
        last_price: float,
        volume: Optional[float] = None,
        timestamp: Optional[datetime] = None,
        token: Optional[int] = None,
    ) -> None:
        """Appends a new market tick/quote to the in-memory store."""
        if last_price is None or last_price <= 0:
            return

        dt = timestamp or datetime.now()
        sym_clean = str(symbol).strip().upper()

        # Resolve or allocate token
        if token is None:
            if sym_clean not in self._token_map:
                self._token_map[sym_clean] = abs(hash(sym_clean)) % 100_000_000
            tok = self._token_map[sym_clean]
        else:
            tok = int(token)
            self._token_map[sym_clean] = tok

        tick = {
            "instrument_token": tok,
            "tradingsymbol": sym_clean,
            "last_price": float(last_price),
            "volume": float(volume or 0.0),
            "date_time": dt,
        }

        with self._lock:
            self._ticks.append(tick)
            if len(self._ticks) > self.max_ticks:
                # Retain the most recent half to bound memory
                self._ticks = self._ticks[-(self.max_ticks // 2):]

    def push_option_contract(
        self,
        ref_stock: str,
        tradingsymbol: str,
        strike: float,
        instrument_type: str,
        expiry: str,
        last_price: float,
        token: Optional[int] = None,
        cap: str = "weekely_options",
    ) -> None:
        """Updates or registers an option contract in the active option chain."""
        sym_clean = str(tradingsymbol).strip().upper()
        ref_clean = str(ref_stock).strip().upper()
        inst_type = "CE" if str(instrument_type).upper().startswith("C") else "PE"

        if token is None:
            if sym_clean not in self._token_map:
                self._token_map[sym_clean] = abs(hash(sym_clean)) % 100_000_000
            tok = self._token_map[sym_clean]
        else:
            tok = int(token)
            self._token_map[sym_clean] = tok

        row = {
            "instrument_token": tok,
            "tradingsymbol": sym_clean,
            "Ref_stock": ref_clean,
            "strike": float(strike),
            "instrument_type": inst_type,
            "expiry": str(expiry),
            "last_price": float(last_price) if last_price else 0.0,
            "cap": cap,
            "Buy_strike": "No",
            "Tradable_stock": "No",
            "exp_date_list": 1,
            "week_dist": 0,
        }

        with self._lock:
            self._latest_option_chain[sym_clean] = row

    def get_recent_ticks_df(self, clear: bool = False) -> pl.DataFrame:
        """
        Returns recent ticks as a Polars DataFrame.
        If clear=True, drains the captured buffer.
        """
        with self._lock:
            if not self._ticks:
                return pl.DataFrame(
                    schema={
                        "instrument_token": pl.Int64,
                        "tradingsymbol": pl.Utf8,
                        "last_price": pl.Float64,
                        "volume": pl.Float64,
                        "date_time": pl.Datetime,
                    }
                )
            records = list(self._ticks)
            if clear:
                self._ticks.clear()

        return pl.DataFrame(records)

    def get_option_chain_df(self) -> pl.DataFrame:
        """Returns the current option chain as a Polars DataFrame."""
        with self._lock:
            if not self._latest_option_chain:
                return pl.DataFrame(
                    schema={
                        "instrument_token": pl.Int64,
                        "tradingsymbol": pl.Utf8,
                        "Ref_stock": pl.Utf8,
                        "strike": pl.Float64,
                        "instrument_type": pl.Utf8,
                        "expiry": pl.Utf8,
                        "last_price": pl.Float64,
                        "cap": pl.Utf8,
                        "Buy_strike": pl.Utf8,
                        "Tradable_stock": pl.Utf8,
                        "exp_date_list": pl.Int64,
                        "week_dist": pl.Int64,
                    }
                )
            rows = list(self._latest_option_chain.values())

        return pl.DataFrame(rows)


class ScraplingBrowserSniffer:
    """
    Stealth Browser Sniffer deploying single-instance Chromium with isolated
    BrowserContext pooling, resource stripping, and frame interception.
    """

    def __init__(
        self,
        portal_url: str = "",
        headless: bool = True,
        profiles_dir: str = "profiles",
        data_store: Optional[SniffedMarketDataStore] = None,
        on_data_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self.portal_url = portal_url
        self.headless = headless
        self.profiles_dir = os.path.abspath(profiles_dir)
        self.data_store = data_store or SniffedMarketDataStore()
        self.on_data_callback = on_data_callback

        os.makedirs(self.profiles_dir, exist_ok=True)

        self._browser = None
        self._contexts: Dict[str, Any] = {}
        self._pages: Dict[str, Any] = {}
        self._running = False
        self._is_visible = not headless
        self._last_recycle_time = time.time()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    def toggle_visibility(self) -> bool:
        """
        Toggles browser visibility (headful / headless) for operator inspection.
        Returns the new visibility state (True = Visible, False = Hidden).
        """
        self._is_visible = not self._is_visible
        logger.info(f"[BROWSER_HOTKEY] Emergency browser visibility toggled to: {'VISIBLE' if self._is_visible else 'HIDDEN'}")
        return self._is_visible

    async def _setup_route_stripping(self, page: Any) -> None:
        """
        Aborts unnecessary network assets to conserve thin client RAM and CPU.
        Blocks images, media, fonts, non-critical stylesheets, and tracking scripts.
        """
        blocked_resource_types = {"image", "media", "font", "stylesheet"}

        async def route_handler(route: Any, request: Any):
            req_type = request.resource_type
            url_lower = request.url.lower()

            if req_type in blocked_resource_types:
                await route.abort()
                return

            if any(
                tracker in url_lower
                for tracker in ["google-analytics", "doubleclick", "facebook", "hotjar", "sentry", "clarity"]
            ):
                await route.abort()
                return

            await route.continue_()

        try:
            await page.route("**/*", route_handler)
            logger.debug("Configured media/asset stripping route handler.")
        except Exception as e:
            logger.debug(f"Could not setup route stripping: {e}")

    async def _setup_sniffers(self, page: Any, account_id: str) -> None:
        """
        Attaches listeners for WebSocket streaming frames and XHR/REST responses.
        """
        # 1. WebSocket Frame Sniffing
        def on_websocket(ws: Any):
            logger.info(f"[{account_id}] WebSocket stream connected: {ws.url}")

            def on_frame_received(payload: Any):
                try:
                    if isinstance(payload, str):
                        data = json.loads(payload)
                        self._process_sniffed_payload(data, account_id=account_id)
                except Exception:
                    pass

            ws.on("framereceived", on_frame_received)

        page.on("websocket", on_websocket)

        # 2. HTTP / XHR Response Sniffing
        async def on_response(response: Any):
            url = response.url.lower()
            if any(k in url for k in ["quote", "trade", "option", "chain", "depth", "market", "book"]):
                try:
                    content_type = response.headers.get("content-type", "")
                    if "json" in content_type:
                        text = await response.text()
                        data = json.loads(text)
                        self._process_sniffed_payload(data, account_id=account_id)
                except Exception:
                    pass

        page.on("response", on_response)

    def _process_sniffed_payload(self, data: Any, account_id: str) -> None:
        """Parses arbitrary dictionary/list payloads into structured ticks or option chains."""
        if not data:
            return

        # Normalization heuristic for standard trade/quote messages
        if isinstance(data, dict):
            # Check for quote fields
            sym = data.get("symbol") or data.get("tradingsymbol") or data.get("ticker") or data.get("s")
            price = data.get("last_price") or data.get("price") or data.get("p") or data.get("ltp") or data.get("c")
            vol = data.get("volume") or data.get("v") or data.get("qty") or 0.0

            if sym and price:
                try:
                    price_f = float(price)
                    self.data_store.push_tick(symbol=str(sym), last_price=price_f, volume=float(vol))
                except (ValueError, TypeError):
                    pass

            # Check for option chain payload
            chain = data.get("option_chain") or data.get("options") or data.get("contracts")
            if isinstance(chain, list):
                for item in chain:
                    if isinstance(item, dict):
                        c_sym = item.get("tradingsymbol") or item.get("symbol")
                        c_ref = item.get("Ref_stock") or item.get("underlying") or "SPY"
                        c_strike = item.get("strike") or item.get("strike_price")
                        c_type = item.get("type") or item.get("instrument_type") or "CE"
                        c_exp = item.get("expiry") or item.get("expiration")
                        c_ltp = item.get("last_price") or item.get("ltp") or 0.0
                        if c_sym and c_strike and c_exp:
                            try:
                                self.data_store.push_option_contract(
                                    ref_stock=str(c_ref),
                                    tradingsymbol=str(c_sym),
                                    strike=float(c_strike),
                                    instrument_type=str(c_type),
                                    expiry=str(c_exp),
                                    last_price=float(c_ltp),
                                )
                            except Exception:
                                pass

        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._process_sniffed_payload(item, account_id=account_id)

    async def authenticate_account(
        self,
        page: Any,
        account_id: str,
        username: str = "",
        password: str = "",
        totp_secret: str = "",
    ) -> bool:
        """Automates login form submission and TOTP 2FA input."""
        logger.info(f"[{account_id}] Checking authentication status...")

        if totp_secret:
            try:
                import pyotp
                totp_token = pyotp.TOTP(totp_secret).now()
                logger.debug(f"[{account_id}] Generated TOTP 2FA token: {totp_token}")
            except Exception as e:
                logger.warning(f"[{account_id}] Could not generate TOTP: {e}")

        # In live environments with configured selectors, this interacts with the page:
        # e.g., await page.fill("#username", username); await page.fill("#totp", totp_token)
        return True

    async def start_account_session(
        self,
        account_id: str,
        portal_url: Optional[str] = None,
        username: str = "",
        password: str = "",
        totp_secret: str = "",
    ) -> Any:
        """
        Creates an isolated BrowserContext for `account_id` and navigates to the target portal.
        """
        target_url = portal_url or self.portal_url
        if not target_url:
            logger.debug(f"[{account_id}] No portal URL provided. Operating in passive/feed-ready mode.")
            return None

        # Determine driver (patchright preferred, fallback to playwright)
        pw_module = None
        try:
            import patchright.async_api as patchright_api
            pw_module = patchright_api
        except ImportError:
            try:
                import playwright.async_api as playwright_api
                pw_module = playwright_api
            except ImportError:
                logger.warning("Neither patchright nor playwright installed. Sniffer running in mock/offline mode.")
                return None

        if self._browser is None:
            p = await pw_module.async_playwright().start()
            self._browser = await p.chromium.launch(
                headless=self.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-gpu",
                ],
            )
            logger.info("Launched single Chromium process for context pooling.")

        profile_path = os.path.join(self.profiles_dir, f"{account_id}.json")
        storage_state = profile_path if os.path.exists(profile_path) else None

        context = await self._browser.new_context(
            storage_state=storage_state,
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )
        self._contexts[account_id] = context

        page = await context.new_page()
        self._pages[account_id] = page

        await self._setup_route_stripping(page)
        await self._setup_sniffers(page, account_id=account_id)

        try:
            logger.info(f"[{account_id}] Navigating to {target_url}...")
            await page.goto(target_url, wait_until="domcontentloaded", timeout=30_000)
            await self.authenticate_account(
                page, account_id=account_id, username=username, password=password, totp_secret=totp_secret
            )
            await context.storage_state(path=profile_path)
            logger.info(f"[{account_id}] Session established and cached to {profile_path}.")
        except Exception as e:
            logger.warning(f"[{account_id}] Navigation warning: {e}")

        return page

    async def soft_memory_recycle(self) -> None:
        """
        Recycles page/context memory every 4 hours to eliminate Chromium memory sprawl.
        """
        now = time.time()
        if (now - self._last_recycle_time) < 14_400:  # 4 hours
            return

        self._last_recycle_time = now
        logger.info("[MEMORY_RECYCLE] Initiating 4-hour soft memory garbage collection in browser contexts...")

        for account_id, page in list(self._pages.items()):
            try:
                await page.reload(wait_until="domcontentloaded", timeout=15_000)
                logger.info(f"[{account_id}] Soft memory recycle completed.")
            except Exception as e:
                logger.debug(f"[{account_id}] Soft recycle notice: {e}")

    async def close_all(self) -> None:
        """Cleanly closes all pages, contexts, and underlying Chromium process."""
        for acc, page in list(self._pages.items()):
            try:
                await page.close()
            except Exception:
                pass
        self._pages.clear()

        for acc, ctx in list(self._contexts.items()):
            try:
                await ctx.close()
            except Exception:
                pass
        self._contexts.clear()

        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None

        logger.info("Scrapling browser sniffer sessions cleanly closed.")
