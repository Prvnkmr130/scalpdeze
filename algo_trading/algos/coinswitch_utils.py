# -*- coding: utf-8 -*-
"""
algo_trading/algos/coinswitch_utils.py
───────────────────────────────────────
Wrapper around CoinSwitch PRO REST APIs customized for order execution, portfolio
inspection, margin calculations, candles, and multi-account trading in DeltaZero26.
Supports both CoinSwitch Spot (v2) and Perpetual Futures trading.
"""

from __future__ import annotations

import logging
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Tuple

import numpy as np
import polars as pl
import requests
import zlib
from cryptography.hazmat.primitives.asymmetric import ed25519

logger = logging.getLogger("algo_trading.algos.coinswitch_utils")


def str_to_token(s: Any) -> int:
    """Deterministic 32-bit positive integer token generator from string symbol across all processes."""
    if s is None:
        return 0
    raw_str = str(s).strip()
    if '/' in raw_str:
        raw_str = raw_str.replace('/', '').strip()
    return int(zlib.crc32(raw_str.encode('utf-8')) % (10**9))


def retry(func: Callable, max_retries: int = 3, delay: float = 1.0, backoff: float = 1.5) -> Any:
    """
    Retry helper for network and transient API operations with exponential backoff.
    Skips retrying non-recoverable authentication errors.
    """
    last_exc = None
    current_delay = delay
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exc = e
            err_msg = str(e).lower()
            if "401" in err_msg or "403" in err_msg or "unauthorized" in err_msg or "signature" in err_msg:
                raise e

            logger.warning(
                "Attempt %d/%d failed with error: %s. Retrying in %.1fs...",
                attempt,
                max_retries,
                e,
                current_delay,
            )
            time.sleep(current_delay)
            current_delay *= backoff
    if last_exc:
        raise last_exc


class CoinSwitchUtility:
    """
    Order execution, account positions, margins, holdings, and market data manager
    for CoinSwitch PRO. Supports multi-account resolution via account_id or Broker model.
    """

    BASE_URL: str = "https://coinswitch.co/trade/api/v2"
    FUTURES_BASE_URL: str = "https://coinswitch.co/trade/api/v2/futures"
    DMA_BASE_URL: str = "https://dma.coinswitch.co"

    def __init__(
        self,
        account_id: str | None = None,
        broker_obj: Any = None,
        client: Any = None,
        api_key: str | None = None,
        api_secret: str | None = None,
    ) -> None:
        """
        Initialize CoinSwitchUtility by fetching credentials from the Broker database model
        or direct API keys.

        Parameters
        ----------
        account_id : str, optional
            Account ID or name (e.g. 'coinswitch_main'). If None and no keys passed,
            loads the first active CoinSwitch account.
        broker_obj : Broker, optional
            Direct Broker model instance.
        client : Any, optional
            Pre-configured client instance.
        api_key : str, optional
            Direct Ed25519 API key string (hex encoded public key).
        api_secret : str, optional
            Direct Ed25519 API secret string (hex encoded private key).
        """
        self.session = requests.Session()
        self._private_key = None

        if client is not None:
            self.broker = broker_obj
            self.account_id = account_id or getattr(client, "account_id", "mock_coinswitch")
            self.api_key = api_key or getattr(client, "api_key", "")
            self.api_secret = api_secret or getattr(client, "api_secret", "")
            logger.info("Initialized CoinSwitchUtility with pre-supplied client for account '%s'", self.account_id)
            return

        if api_key is not None or api_secret is not None:
            self.broker = broker_obj
            self.account_id = account_id or "direct_coinswitch"
            self.api_key = (api_key or "").strip()
            self.api_secret = (api_secret or "").strip()
        else:
            from django.db.models import Q
            from kalai.models import Broker

            if broker_obj is not None:
                self.broker = broker_obj
            elif account_id:
                self.broker = Broker.objects.filter(
                    Q(account_id__iexact=account_id) | Q(name__iexact=account_id)
                ).first()
            else:
                self.broker = (
                    Broker.objects.filter(
                        Q(broker_name__name__icontains="coinswitch")
                        | Q(broker_name__name__icontains="switch")
                        | Q(name__icontains="coinswitch")
                        | Q(account_id__icontains="coinswitch")
                    )
                    .filter(enable_websocket=True)
                    .first()
                )
                if not self.broker:
                    self.broker = Broker.objects.filter(
                        Q(broker_name__name__icontains="coinswitch")
                        | Q(broker_name__name__icontains="switch")
                        | Q(name__icontains="coinswitch")
                        | Q(account_id__icontains="coinswitch")
                    ).first()

            if not self.broker:
                identifier = account_id or "auto"
                logger.warning(
                    "No Broker configuration found for CoinSwitch account '%s'. "
                    "Public market data endpoints will operate without authentication.",
                    identifier,
                )
                self.account_id = identifier
                self.api_key = (api_key or "").strip()
                self.api_secret = (api_secret or "").strip()
            else:
                self.account_id = self.broker.account_id or self.broker.name or "coinswitch"
                self.api_key = (api_key or self.broker.api_key or "").strip()
                self.api_secret = (api_secret or self.broker.api_secret or "").strip()

        if not self.api_key or not self.api_secret:
            logger.debug(
                "API credentials missing for CoinSwitch account '%s'. Public endpoints will function normally.",
                self.account_id,
            )
        else:
            # Cache private key instance for signature generation
            try:
                secret_bytes = bytes.fromhex(self.api_secret)
                self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_bytes)
            except Exception:
                # Fallback if secret was encoded as UTF-8 / base64 or 32-byte raw
                try:
                    raw_bytes = self.api_secret.encode("utf-8")[:32].ljust(32, b"\x00")
                    self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(raw_bytes)
                except Exception as e:
                    logger.warning("Failed to initialize Ed25519 private key for %s: %s", self.account_id, e)
        # Expiry Check for CoinSwitch Ed25519 Secret Key
        if self.broker and getattr(self.broker, "is_coinswitch", False):
            status = getattr(self.broker, "secret_key_expiry_status", "ACTIVE")
            days_left = getattr(self.broker, "days_until_secret_key_expiry", None)
            expires_at = getattr(self.broker, "secret_key_expires_at", None)
            exp_str = expires_at.strftime("%Y-%m-%d") if expires_at else "N/A"
            if status == "EXPIRED":
                logger.error(
                    "🚨 [CRITICAL] CoinSwitch Account '%s' Secret Key EXPIRED (%s days ago on %s). "
                    "Please renew the key in Broker Admin.",
                    self.account_id,
                    abs(days_left or 0),
                    exp_str,
                )
            elif status == "EXPIRING_SOON":
                logger.warning(
                    "⚠️ [WARNING] CoinSwitch Account '%s' Secret Key expires in %s days (on %s). "
                    "Please renew the key in Broker Admin before it lapses.",
                    self.account_id,
                    days_left,
                    exp_str,
                )

        logger.info("Initialized CoinSwitchUtility for account '%s'", self.account_id)

    # ──────────────────────────────────────────────────────────────
    # Cryptographic Authentication & Request Helpers
    # ──────────────────────────────────────────────────────────────

    def _sign_request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        is_dma: bool = False,
    ) -> tuple[dict[str, str], str]:
        """
        Sign HTTP request using Ed25519 digital signature scheme conforming to
        CoinSwitch Pro API authentication specifications.

        Message = METHOD + decoded_path + epoch (ms)
        Headers = X-AUTH-APIKEY, X-AUTH-SIGNATURE, X-AUTH-EPOCH, Content-Type
        """
        method = method.upper()
        if params:
            sep = "&" if "?" in path else "?"
            path = path + sep + urllib.parse.urlencode(params)

        epoch = str(int(time.time() * 1000))
        if is_dma or path.startswith("/v5/"):
            message = f"{method}{path}{epoch}"
        else:
            decoded_path = urllib.parse.unquote_plus(path)
            message = f"{method}{decoded_path}{epoch}"

        signature = ""
        if self._private_key:
            try:
                sig_bytes = self._private_key.sign(message.encode("utf-8"))
                signature = sig_bytes.hex()
            except Exception as exc:
                logger.error("[%s] Ed25519 signing failed: %s", self.account_id, exc)

        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
            "X-AUTH-EPOCH": epoch,
        }
        return headers, path

    def _auth_request(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        json_body: dict | None = None,
        is_futures: bool = False,
        is_options: bool = False,
    ) -> Any:
        """
        Execute an authenticated request (GET, POST, DELETE) against CoinSwitch REST API.
        Supports Spot v2, Perpetual Futures v2, and DMA Options APIs.
        """
        if not self.api_key or not self._private_key:
            logger.warning(
                "[%s] API credentials missing for CoinSwitch account '%s'. Cannot execute %s %s",
                datetime.now().isoformat(),
                self.account_id,
                method,
                endpoint,
            )
            return None

        if is_options:
            base_url = self.DMA_BASE_URL
            url_prefix = ""
        elif is_futures:
            base_url = self.FUTURES_BASE_URL
            url_prefix = "/trade/api/v2/futures"
        else:
            base_url = self.BASE_URL
            url_prefix = "/trade/api/v2"

        rel_endpoint = f"/{endpoint.lstrip('/')}"
        full_path = f"{url_prefix}{rel_endpoint}"

        headers, signed_path = self._sign_request(method, full_path, params=params, is_dma=is_options)
        
        # Build complete request URL
        query_part = signed_path[len(full_path):]
        url = f"{base_url}{rel_endpoint}{query_part}"

        response = self.session.request(
            method=method.upper(),
            url=url,
            headers=headers,
            json=json_body if json_body is not None else None,
            timeout=15,
        )

        if response.status_code not in (200, 201, 202, 204):
            logger.error(
                "[%s] CoinSwitch %s %s failed (%d): %s",
                self.account_id,
                method,
                endpoint,
                response.status_code,
                response.text,
            )
            response.raise_for_status()

        if response.status_code == 204 or not response.text:
            return {}

        return response.json()

    def _public_get(
        self,
        endpoint: str,
        params: dict | None = None,
        is_futures: bool = False,
        is_options: bool = False,
    ) -> Any:
        """
        Execute an unauthenticated public GET request against CoinSwitch API.
        """
        if is_options:
            base_url = self.DMA_BASE_URL
        elif is_futures:
            base_url = self.FUTURES_BASE_URL
        else:
            base_url = self.BASE_URL
        url = f"{base_url}/{endpoint.lstrip('/')}"

        response = self.session.get(url, params=params, timeout=15)
        if response.status_code != 200:
            logger.error(
                "[%s] CoinSwitch Public GET %s failed (%d): %s",
                self.account_id,
                endpoint,
                response.status_code,
                response.text,
            )
            response.raise_for_status()
        return response.json()

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        """
        Normalize symbol format to standard BASE/QUOTE format for CoinSwitch REST APIs.
        Handles BTCUSDT -> BTC/USDT, BTC_USDT -> BTC/USDT, B-BTC_USDT -> BTC/USDT.
        """
        sym = symbol.strip().upper()
        if sym.startswith("B-"):
            sym = sym[2:]
        if "_" in sym:
            sym = sym.replace("_", "/")
        elif "," in sym:
            sym = sym.replace(",", "/")
        elif "/" not in sym:
            # Common crypto pairs heuristics
            for quote in ("USDT", "INR", "BTC", "ETH", "USDC"):
                if sym.endswith(quote) and len(sym) > len(quote):
                    base = sym[: -len(quote)]
                    return f"{base}/{quote}"
        return sym

    # ──────────────────────────────────────────────────────────────
    # Account, Holdings, Balances & Margins
    # ──────────────────────────────────────────────────────────────

    def get_user_portfolio(self) -> dict:
        """
        Fetch Spot user portfolio balances and valuation.
        Endpoint: GET /trade/api/v2/user/portfolio
        """
        return retry(lambda: self._auth_request("GET", "user/portfolio"))

    def get_futures_wallet_balance(self) -> dict:
        """
        Fetch Perpetual Futures & DMA Options unified wallet balance, margin balance, and available margin.
        Endpoint: GET /v5/account/wallet-balance?accountType=UNIFIED (DMA API)
        """
        try:
            return self._auth_request("GET", "v5/account/wallet-balance", params={"accountType": "UNIFIED"}, is_options=True)
        except Exception as exc:
            logger.debug("[%s] DMA wallet balance query failed: %s", self.account_id, exc)
            return {}

    def chk_live_bal(self) -> tuple[float, float]:
        """
        Check and return available cash (USDT/INR) and total net capital.
        Combines spot portfolio and DMA options / futures wallet capital.

        Returns
        -------
        tuple[float, float]
            (avail_cash, net_capital)
        """
        if not self.api_key or not self._private_key:
            return 0.0, 0.0
        avail_cash = 0.0
        net_capital = 0.0

        # 1. Check Spot Portfolio (INR & crypto balances)
        try:
            portfolio = retry(lambda: self.get_user_portfolio())
            data = portfolio.get("data", portfolio) if isinstance(portfolio, dict) else portfolio
            
            # Handle total net capital if provided
            if isinstance(data, dict):
                total_val = float(data.get("total_portfolio_value_inr") or data.get("total_portfolio_value_usdt") or 0.0)
                net_capital += total_val

                # Extract available cash from balances
                balances = data.get("balances", []) or data.get("currency_balances", [])
                if isinstance(balances, list):
                    for item in balances:
                        curr = str(item.get("currency", "")).upper()
                        avail = float(item.get("available_balance") or item.get("main_balance") or 0.0)
                        if curr in ("INR", "USDT", "USDC"):
                            avail_cash += avail
                        if total_val == 0.0:
                            net_capital += float(item.get("total_balance") or avail)
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        curr = str(item.get("currency", "")).upper()
                        avail = float(item.get("available_balance") or item.get("main_balance") or item.get("balance") or 0.0)
                        tot = float(item.get("total_balance") or item.get("total") or avail)
                        if curr in ("INR", "USDT", "USDC"):
                            avail_cash += avail
                        net_capital += tot
        except Exception as e:
            logger.debug("[%s] Spot portfolio check: %s", self.account_id, e)

        # 2. Check DMA Options / Unified Futures Wallet Balance
        try:
            dma_wallet = self.get_futures_wallet_balance()
            if isinstance(dma_wallet, dict):
                # Check DMA v5 format: {"result": {"list": [...]}}
                result = dma_wallet.get("result", {})
                w_list = result.get("list", []) if isinstance(result, dict) else []
                if isinstance(w_list, list) and w_list:
                    w_info = w_list[0]
                    dma_avail = float(w_info.get("totalAvailableBalance") or w_info.get("totalMarginBalance") or 0.0)
                    dma_total = float(w_info.get("totalWalletBalance") or w_info.get("totalEquity") or dma_avail)
                    avail_cash += dma_avail
                    net_capital += dma_total
                else:
                    # Generic / mock schema: {"data": {"available_balance": 800.0, ...}}
                    data = dma_wallet.get("data", dma_wallet)
                    if isinstance(data, dict):
                        f_avail = float(data.get("available_balance") or data.get("available_margin") or data.get("totalAvailableBalance") or 0.0)
                        f_total = float(data.get("total_wallet_balance") or data.get("wallet_balance") or data.get("totalWalletBalance") or f_avail)
                        avail_cash += f_avail
                        net_capital += f_total
        except Exception as e:
            logger.debug("[%s] Futures / DMA wallet balance check: %s", self.account_id, e)

        return round(avail_cash, 2), round(net_capital, 2)

    def holdings(self) -> pl.DataFrame | None:
        """
        Fetch non-zero Spot wallet crypto holdings with balances and locked amounts.

        Returns
        -------
        pl.DataFrame or None
            Columns: currency, balance, locked_balance, total, tradingsymbol
        """
        try:
            portfolio = retry(lambda: self.get_user_portfolio())
            data = portfolio.get("data", portfolio) if isinstance(portfolio, dict) else portfolio
            balances = data if isinstance(data, list) else (data.get("balances", []) or data.get("currency_balances", []))
            if not isinstance(balances, list) or not balances:
                return pl.DataFrame()

            rows = []
            for b in balances:
                if not isinstance(b, dict):
                    continue
                avail = float(b.get("available_balance") or b.get("main_balance") or b.get("balance") or 0.0)
                locked = float(b.get("locked_balance") or b.get("blocked_balance") or 0.0)
                total = float(b.get("total_balance") or (avail + locked))
                curr = str(b.get("currency", "")).upper()

                if total > 0:
                    rows.append({
                        "currency": curr,
                        "balance": avail,
                        "locked_balance": locked,
                        "total": total,
                        "tradingsymbol": curr,
                    })

            if not rows:
                return pl.DataFrame()

            return pl.DataFrame(rows)
        except Exception as e:
            logger.error("[%s] Failed to fetch holdings: %s", self.account_id, e)
            return None

    def pos_data(self, pair: str | None = None, exchange: str = "EXCHANGE_2") -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
        """
        Fetch current active and historical day Futures derivatives positions.

        Returns
        -------
        tuple[pl.DataFrame | None, pl.DataFrame | None]
            (day_positions_df, net_positions_df)
        """
        if not self.api_key or not self._private_key:
            return None, None
        try:
            fut_exchange = "EXCHANGE_2"
            params = {"exchange": fut_exchange}
            if pair:
                params["symbol"] = self._normalize_symbol(pair)

            raw_pos = self._auth_request("GET", "positions", params=params, is_futures=True)
            data = raw_pos.get("data", raw_pos) if isinstance(raw_pos, dict) else []
            if isinstance(data, dict):
                data = data.get("positions", []) or [data]

            if not isinstance(data, list) or not data:
                return pl.DataFrame(), pl.DataFrame()

            rows = []
            for p in data:
                sym = p.get("symbol") or p.get("pair") or ""
                size = float(p.get("size") or p.get("position_amt") or p.get("contracts") or 0.0)
                entry_px = float(p.get("entry_price") or p.get("avg_price") or 0.0)
                mark_px = float(p.get("mark_price") or entry_px)
                liq_px = float(p.get("liquidation_price") or 0.0)
                unrealized_pnl = float(p.get("unrealized_pnl") or p.get("pnl") or 0.0)
                realized_pnl = float(p.get("realized_pnl") or 0.0)
                margin = float(p.get("margin") or p.get("initial_margin") or 0.0)
                leverage = int(float(p.get("leverage") or 1))

                rows.append({
                    "pair": sym,
                    "symbol": sym,
                    "active_pos": size,
                    "avg_price": entry_px,
                    "mark_price": mark_px,
                    "liquidation_price": liq_px,
                    "locked_margin": margin,
                    "unrealized_pnl": unrealized_pnl,
                    "realized_pnl": realized_pnl,
                    "leverage": leverage,
                })

            df = pl.DataFrame(rows) if rows else pl.DataFrame()
            net_df = df.filter(pl.col("active_pos").abs() > 0) if (not df.is_empty() and "active_pos" in df.columns) else pl.DataFrame()
            return df, net_df
        except Exception as e:
            logger.debug("[%s] Futures positions not available (%s), checking spot holdings fallback.", self.account_id, e)
            try:
                h_df = self.holdings()
                if h_df is not None and not h_df.is_empty():
                    rows = []
                    for row in h_df.to_dicts():
                        curr = str(row.get("currency", "")).upper()
                        if curr in ("INR", "USDT", "USDC", "USD"):
                            continue
                        bal = float(row.get("total") or row.get("balance") or 0.0)
                        if bal > 0:
                            sym = f"{curr}/INR"
                            rows.append({
                                "pair": sym,
                                "symbol": sym,
                                "active_pos": bal,
                                "avg_price": 0.0,
                                "mark_price": 0.0,
                                "liquidation_price": 0.0,
                                "locked_margin": 0.0,
                                "unrealized_pnl": 0.0,
                                "realized_pnl": 0.0,
                                "leverage": 1,
                            })
                    if rows:
                        df = pl.DataFrame(rows)
                        return df, df
            except Exception:
                pass
            return pl.DataFrame(), pl.DataFrame()

    # ──────────────────────────────────────────────────────────────
    # Order Execution & Management
    # ──────────────────────────────────────────────────────────────

    def mrk_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str = "BUY",
        exchange: str = "coinswitchx",
        product: str = "NRML",
        *args: Any,
        is_futures: bool = False,
        is_options: bool = False,
        leverage: int = 1,
        client_order_id: str | None = None,
        **kwargs: Any,
    ) -> tuple[str | int, str]:
        """
        Place a Market Order (supports Spot, Futures, and Options).
        """
        norm_sym = self._normalize_symbol(symbol)
        side = buy_sell.lower()
        cid = client_order_id or f"DZ_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"

        payload: dict[str, Any] = {
            "side": side,
            "symbol": norm_sym,
            "type": "market",
            "quantity": quantity,
            "exchange": exchange if isinstance(exchange, str) else "coinswitchx",
            "client_order_id": cid,
        }
        if is_options:
            payload["category"] = "option"
        elif is_futures:
            payload["leverage"] = leverage

        try:
            res = retry(lambda: self._auth_request("POST", "order", json_body=payload, is_futures=is_futures, is_options=is_options))
            data = res.get("data", res) if isinstance(res, dict) else {}
            order_id = data.get("order_id") or data.get("id") or cid
            logger.info("[%s] Market order placed: %s -> ID: %s", self.account_id, norm_sym, order_id)
            return order_id, "Order placed successfully"
        except Exception as e:
            logger.error("[%s] Market order failed (%s %s): %s", self.account_id, side, norm_sym, e)
            return -1, str(e)

    def lim_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str = "BUY",
        exchange: str = "coinswitchx",
        product: str = "NRML",
        price: float | None = None,
        *args: Any,
        is_futures: bool = False,
        is_options: bool = False,
        leverage: int = 1,
        client_order_id: str | None = None,
        time_in_force: str | None = None,
        **kwargs: Any,
    ) -> tuple[str | int, str]:
        """
        Place a Limit Order at a specified limit price (supports Spot, Futures, and Options).
        Supports both modern keyword arguments and legacy positional signatures.
        """
        # Resolve price from kwargs, positional args or exchange variable if swapped
        resolved_price = price or kwargs.get("price")
        resolved_exchange = exchange if isinstance(exchange, str) else "coinswitchx"
        if resolved_price is None:
            if isinstance(exchange, (int, float)):
                resolved_price = float(exchange)
                resolved_exchange = "coinswitchx"
            elif args:
                for a in args:
                    if isinstance(a, (int, float)) and not isinstance(a, bool):
                        resolved_price = float(a)
                        break

        if resolved_price is None or resolved_price <= 0:
            return -1, "Price must be provided and > 0 for Limit Orders."

        norm_sym = self._normalize_symbol(symbol)
        side = buy_sell.lower()
        cid = client_order_id or f"DZ_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"

        payload: dict[str, Any] = {
            "side": side,
            "symbol": norm_sym,
            "type": "limit",
            "price": resolved_price,
            "quantity": quantity,
            "exchange": resolved_exchange,
            "client_order_id": cid,
        }
        if is_options:
            payload["category"] = "option"
        elif is_futures:
            payload["leverage"] = leverage
        if time_in_force:
            payload["time_in_force"] = time_in_force

        try:
            res = retry(lambda: self._auth_request("POST", "order", json_body=payload, is_futures=is_futures, is_options=is_options))
            data = res.get("data", res) if isinstance(res, dict) else {}
            order_id = data.get("order_id") or data.get("id") or cid
            logger.info("[%s] Limit order placed: %s @ %.4f -> ID: %s", self.account_id, norm_sym, resolved_price, order_id)
            return order_id, "Order placed successfully"
        except Exception as e:
            logger.error("[%s] Limit order failed (%s %s @ %.4f): %s", self.account_id, side, norm_sym, resolved_price, e)
            return -1, str(e)

    def ice_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str = "BUY",
        exchange: str = "coinswitchx",
        product: str = "NRML",
        price: float | None = None,
        total_legs: int = 1,
        total_qty: float | None = None,
        ttl_value: int | None = None,
        order_type: str = "LIMIT",
        validity: str = "TTL",
        *args: Any,
        **kwargs: Any,
    ) -> tuple[str | int, str]:
        """
        Iceberg Limit order router for CoinSwitch PRO. Dispatches as a single or aggregated limit order.
        """
        effective_qty = total_qty if (total_qty is not None and total_qty > 0) else quantity
        return self.lim_ordr(
            symbol=symbol,
            quantity=effective_qty,
            buy_sell=buy_sell,
            exchange=exchange,
            product=product,
            price=price,
            *args,
            **kwargs,
        )

    def sl_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str = "BUY",
        exchange: str = "coinswitchx",
        product: str = "NRML",
        price: float | None = None,
        trig_price: float | None = None,
        *args: Any,
        is_futures: bool = False,
        leverage: int = 1,
        client_order_id: str | None = None,
        **kwargs: Any,
    ) -> tuple[str | int, str]:
        """
        Place a Stop-Loss Limit (`stop_limit`) order.
        """
        resolved_price = price or kwargs.get("price")
        resolved_trig = trig_price or kwargs.get("trig_price") or kwargs.get("trigger_price")
        resolved_exchange = exchange if isinstance(exchange, str) else "coinswitchx"
        if args and resolved_price is None and len(args) >= 1:
            resolved_price = float(args[0])
        if args and resolved_trig is None and len(args) >= 2:
            resolved_trig = float(args[1])

        if resolved_price is None or resolved_trig is None:
            return -1, "Both price and trig_price must be provided for Stop-Loss Limit orders."

        norm_sym = self._normalize_symbol(symbol)
        side = buy_sell.lower()
        cid = client_order_id or f"DZ_SL_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"

        payload: dict[str, Any] = {
            "side": side,
            "symbol": norm_sym,
            "type": "stop_limit",
            "price": resolved_price,
            "trigger_price": resolved_trig,
            "quantity": quantity,
            "exchange": resolved_exchange,
            "client_order_id": cid,
        }
        if is_futures:
            payload["leverage"] = leverage

        try:
            res = retry(lambda: self._auth_request("POST", "order", json_body=payload, is_futures=is_futures))
            data = res.get("data", res) if isinstance(res, dict) else {}
            order_id = data.get("order_id") or data.get("id") or cid
            return order_id, "Stop-Loss Limit order placed successfully"
        except Exception as e:
            logger.error("[%s] Stop-Loss Limit order failed: %s", self.account_id, e)
            return -1, str(e)

    def slmkt_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str = "BUY",
        trig_price: float | None = None,
        exchange: str = "coinswitchx",
        is_futures: bool = False,
        leverage: int = 1,
        client_order_id: str | None = None,
    ) -> tuple[str | int, str]:
        """
        Place a Stop-Loss Market (`stop_market`) order.
        """
        if trig_price is None or trig_price <= 0:
            return -1, "trig_price must be provided and > 0 for Stop-Loss Market orders."

        norm_sym = self._normalize_symbol(symbol)
        side = buy_sell.lower()
        cid = client_order_id or f"DZ_SLM_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"

        payload: dict[str, Any] = {
            "side": side,
            "symbol": norm_sym,
            "type": "stop_market",
            "trigger_price": trig_price,
            "quantity": quantity,
            "exchange": exchange,
            "client_order_id": cid,
        }
        if is_futures:
            payload["leverage"] = leverage

        try:
            res = retry(lambda: self._auth_request("POST", "order", json_body=payload, is_futures=is_futures))
            data = res.get("data", res) if isinstance(res, dict) else {}
            order_id = data.get("order_id") or data.get("id") or cid
            return order_id, "Stop-Loss Market order placed successfully"
        except Exception as e:
            logger.error("[%s] Stop-Loss Market order failed: %s", self.account_id, e)
            return -1, str(e)

    def cancel_ordr(
        self,
        order_id: str | None = None,
        client_order_id: str | None = None,
        symbol: str | None = None,
        exchange: str = "coinswitchx",
        is_futures: bool = False,
    ) -> tuple[bool, str]:
        """
        Cancel an active open order by order_id or client_order_id.
        """
        if not order_id and not client_order_id:
            return False, "Either order_id or client_order_id must be provided."

        params: dict[str, Any] = {"exchange": exchange}
        if order_id:
            params["order_id"] = order_id
        if client_order_id:
            params["client_order_id"] = client_order_id
        if symbol:
            params["symbol"] = self._normalize_symbol(symbol)

        try:
            res = retry(lambda: self._auth_request("DELETE", "order", params=params, is_futures=is_futures))
            logger.info("[%s] Order cancelled: %s", self.account_id, order_id or client_order_id)
            return True, res.get("message", "Order cancelled successfully") if isinstance(res, dict) else "Cancelled"
        except Exception as e:
            logger.error("[%s] Cancel order failed (%s): %s", self.account_id, order_id or client_order_id, e)
            return False, str(e)

    def cancel_all(
        self,
        symbol: str | None = None,
        exchange: str = "coinswitchx",
        is_futures: bool = False,
    ) -> tuple[bool, str]:
        """
        Cancel all open orders, optionally filtered by symbol.
        """
        try:
            open_orders = self.orders(is_futures=is_futures, symbol=symbol, exchange=exchange, open_only=True)
            if not open_orders:
                return True, "No open orders found to cancel."

            cancelled_count = 0
            for ord_item in open_orders:
                oid = ord_item.get("order_id") or ord_item.get("id")
                cid = ord_item.get("client_order_id")
                sym = ord_item.get("symbol") or symbol
                ok, _ = self.cancel_ordr(order_id=oid, client_order_id=cid, symbol=sym, exchange=exchange, is_futures=is_futures)
                if ok:
                    cancelled_count += 1

            return True, f"Cancelled {cancelled_count}/{len(open_orders)} open orders."
        except Exception as e:
            logger.error("[%s] Cancel all orders failed: %s", self.account_id, e)
            return False, str(e)

    def exit_position(
        self,
        symbol: str,
        is_futures: bool = True,
    ) -> tuple[str | int, str]:
        """
        Flatten/exit an open futures position by placing an opposing market order.
        """
        norm_sym = self._normalize_symbol(symbol)
        try:
            _, net_df = self.pos_data(pair=norm_sym)
            if net_df is None or net_df.is_empty():
                return -1, f"No open position found for {norm_sym}."

            match = net_df.filter(pl.col("symbol") == norm_sym) if "symbol" in net_df.columns else pl.DataFrame()
            if match.is_empty():
                return -1, f"No open position matching {norm_sym}."

            pos_size = float(match["active_pos"][0]) if "active_pos" in match.columns and len(match["active_pos"]) > 0 else 0.0
            if abs(pos_size) == 0:
                return -1, f"Position size for {norm_sym} is zero."

            opposing_side = "SELL" if pos_size > 0 else "BUY"
            exit_qty = abs(pos_size)
            logger.info("[%s] Exiting position: %s %s %.4f", self.account_id, opposing_side, norm_sym, exit_qty)

            return self.mrk_ordr(
                symbol=norm_sym,
                quantity=exit_qty,
                buy_sell=opposing_side,
                is_futures=is_futures,
            )
        except Exception as e:
            logger.error("[%s] Exit position failed for %s: %s", self.account_id, norm_sym, e)
            return -1, str(e)

    def orders(
        self,
        is_futures: bool = False,
        symbol: str | None = None,
        exchange: str = "coinswitchx",
        open_only: bool = True,
    ) -> list[dict]:
        """
        Fetch active open orders or recent orders list.
        Endpoint: GET /trade/api/v2/orders or GET /trade/api/v2/futures/orders
        """
        params: dict[str, Any] = {"exchange": exchange}
        if symbol:
            params["symbol"] = self._normalize_symbol(symbol)
        if open_only:
            params["open"] = "true"

        endpoint = "orders"
        try:
            res = retry(lambda: self._auth_request("GET", endpoint, params=params, is_futures=is_futures))
            data = res.get("data", res) if isinstance(res, dict) else []
            if isinstance(data, dict):
                data = data.get("orders", []) or [data]
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error("[%s] Failed to fetch orders: %s", self.account_id, e)
            return []

    def order_history(
        self,
        order_id: str | None = None,
        client_order_id: str | None = None,
        is_futures: bool = False,
    ) -> dict | None:
        """
        Fetch status and execution details for a specific order.
        Endpoint: GET /trade/api/v2/order or GET /trade/api/v2/futures/order
        """
        params: dict[str, Any] = {}
        if order_id:
            params["order_id"] = order_id
        if client_order_id:
            params["client_order_id"] = client_order_id

        try:
            res = retry(lambda: self._auth_request("GET", "order", params=params, is_futures=is_futures))
            data = res.get("data", res) if isinstance(res, dict) else res
            return data if isinstance(data, dict) else None
        except Exception as e:
            logger.error("[%s] Failed to fetch order history: %s", self.account_id, e)
            return None

    def order_trades(
        self,
        symbol: str | None = None,
        exchange: str = "coinswitchx",
    ) -> list[dict]:
        """
        Fetch user trade execution history.
        Endpoint: GET /trade/api/v2/user/trades or /trade/api/v2/trades
        """
        params: dict[str, Any] = {"exchange": exchange}
        if symbol:
            params["symbol"] = self._normalize_symbol(symbol)

        try:
            res = retry(lambda: self._auth_request("GET", "user/trades", params=params))
            data = res.get("data", res) if isinstance(res, dict) else []
            return data if isinstance(data, list) else []
        except Exception:
            # Fallback to /trades endpoint
            try:
                res = retry(lambda: self._auth_request("GET", "trades", params=params))
                data = res.get("data", res) if isinstance(res, dict) else []
                return data if isinstance(data, list) else []
            except Exception as e:
                logger.error("[%s] Failed to fetch trades: %s", self.account_id, e)
                return []

    # ──────────────────────────────────────────────────────────────
    # Market Data & Candlesticks
    # ──────────────────────────────────────────────────────────────

    def candles(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 100,
        exchange: str = "coinswitchx",
    ) -> pl.DataFrame | None:
        """
        Fetch historical OHLCV candlestick data into a Polars DataFrame.
        Endpoint: GET /trade/api/v2/candles
        """
        norm_sym = self._normalize_symbol(symbol)
        params = {
            "symbol": norm_sym,
            "interval": interval,
            "limit": limit,
            "exchange": exchange,
        }
        try:
            res = retry(lambda: self._public_get("candles", params=params))
            data = res.get("data", res) if isinstance(res, dict) else []
            if not isinstance(data, list) or not data:
                return pl.DataFrame()

            rows = []
            for c in data:
                if isinstance(c, list) and len(c) >= 5:
                    # Format: [timestamp, open, high, low, close, volume]
                    ts = c[0]
                    dt_val = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).replace(tzinfo=None) if (isinstance(ts, (int, float)) and ts > 1e11) else datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None) if isinstance(ts, (int, float)) else None
                    rows.append({
                        "datetime": dt_val,
                        "open": float(c[1]),
                        "high": float(c[2]),
                        "low": float(c[3]),
                        "close": float(c[4]),
                        "volume": float(c[5]) if len(c) > 5 else 0.0,
                    })
                elif isinstance(c, dict):
                    ts = c.get("time") or c.get("timestamp") or c.get("datetime")
                    dt_val = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).replace(tzinfo=None) if (isinstance(ts, (int, float)) and ts > 1e11) else datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None) if isinstance(ts, (int, float)) else None
                    rows.append({
                        "datetime": dt_val,
                        "open": float(c.get("open") or c.get("o") or 0.0),
                        "high": float(c.get("high") or c.get("h") or 0.0),
                        "low": float(c.get("low") or c.get("l") or 0.0),
                        "close": float(c.get("close") or c.get("c") or 0.0),
                        "volume": float(c.get("volume") or c.get("v") or 0.0),
                    })

            if not rows:
                return pl.DataFrame()

            df = pl.DataFrame(rows)
            if "datetime" in df.columns:
                df = df.sort("datetime")
            return df
        except Exception as e:
            logger.error("[%s] Failed to fetch candles for %s: %s", self.account_id, norm_sym, e)
            return None

    def order_book(
        self,
        symbol: str,
        exchange: str = "coinswitchx",
    ) -> dict:
        """
        Fetch live Level-2 order book depth (bids & asks).
        Endpoint: GET /trade/api/v2/depth
        """
        norm_sym = self._normalize_symbol(symbol)
        params = {"symbol": norm_sym, "exchange": exchange}
        try:
            res = retry(lambda: self._public_get("depth", params=params))
            return res.get("data", res) if isinstance(res, dict) else {}
        except Exception as e:
            logger.error("[%s] Failed to fetch order book for %s: %s", self.account_id, norm_sym, e)
            return {"bids": [], "asks": []}

    def ticker(
        self,
        symbol: str | None = None,
        exchange: str = "coinswitchx",
    ) -> dict | list[dict]:
        """
        Fetch current 24hr ticker price and volume statistics.
        Endpoint: GET /trade/api/v2/24hr/ticker or GET /trade/api/v2/24hr/all-pairs/ticker
        """
        try:
            if symbol:
                norm_sym = self._normalize_symbol(symbol)
                params = {"symbol": norm_sym, "exchange": exchange}
                res = retry(lambda: self._public_get("24hr/ticker", params=params))
                return res.get("data", res) if isinstance(res, dict) else {}
            else:
                params = {"exchange": exchange}
                res = retry(lambda: self._public_get("24hr/all-pairs/ticker", params=params))
                return res.get("data", res) if isinstance(res, dict) else []
        except Exception as e:
            logger.error("[%s] Failed to fetch ticker: %s", self.account_id, e)
            return {}

    def instruments_list(
        self,
        exchange: str = "coinswitchx",
        is_futures: bool = False,
        is_options: bool = False,
        base_coins: list[str] | None = None,
    ) -> list[dict]:
        """
        Fetch active trading pairs and precision specifications for a specific exchange.
        Supports Spot (coinswitchx, c2c1, c2c2), Perpetual Futures (EXCHANGE_2), and DMA Options (BTC, ETH, SOL, XRP, DOGE).
        """
        ex = exchange.lower()
        if is_options or ex in ("options", "option", "dma"):
            try:
                target_coins = base_coins or ["BTC", "ETH", "SOL", "XRP", "DOGE"]
                all_items = []
                for coin in target_coins:
                    cursor = None
                    while True:
                        params: dict[str, Any] = {"category": "option", "baseCoin": coin, "limit": 1000}
                        if cursor:
                            params["cursor"] = cursor
                        res = self._auth_request("GET", "v5/market/instruments-info", params=params, is_options=True)
                        data = res.get("result", {}) if isinstance(res, dict) else {}
                        items = data.get("list", []) or []
                        all_items.extend(items)
                        cursor = data.get("nextPageCursor")
                        if not cursor or not items:
                            break

                rows = []
                for item in all_items:
                    sym = str(item.get("symbol") or item.get("displayName") or "").strip()
                    if not sym:
                        continue
                    sym_id = item.get("symbolId") or str_to_token(sym)
                    base = str(item.get("baseCoin") or "BTC").upper()
                    quote = str(item.get("quoteCoin") or "USDT").upper()
                    opt_type_raw = str(item.get("optionsType") or "").lower()
                    opt_type = "PE" if opt_type_raw.startswith("p") else "CE"

                    deliv_ms = item.get("deliveryTime")
                    exp_dt = None
                    exp_str = ""
                    if deliv_ms:
                        try:
                            exp_dt = datetime.fromtimestamp(int(deliv_ms) / 1000, tz=timezone.utc).date()
                            exp_str = exp_dt.strftime("%Y-%m-%d")
                        except Exception:
                            exp_dt = None
                            exp_str = ""

                    # Parse strike from symbol: e.g. BTC-25JUN27-160000-P-USDT or ETH-25JUN27-7000-P-USDT
                    strike = 0.0
                    parts = sym.split("-")
                    if len(parts) >= 4:
                        try:
                            strike = float(parts[2])
                        except ValueError:
                            pass

                    price_filter = item.get("priceFilter", {}) if isinstance(item.get("priceFilter"), dict) else {}
                    lot_filter = item.get("lotSizeFilter", {}) if isinstance(item.get("lotSizeFilter"), dict) else {}
                    tick_sz = float(price_filter.get("tickSize") or 0.01)
                    lot_sz = float(lot_filter.get("minOrderQty") or 0.01)

                    rows.append({
                        "instrument_token": int(sym_id),
                        "exchange_token": int(sym_id),
                        "tradingsymbol": sym,
                        "name": base,
                        "exchange": "coinswitchx",
                        "segment": "OPTIONS",
                        "instrument_type": opt_type,
                        "strike": strike,
                        "expiry": exp_str,
                        "exp_date_list": exp_dt,
                        "lot_size": lot_sz,
                        "tick_size": tick_sz,
                        "base_asset": base,
                        "quote_asset": quote,
                        "status": item.get("status", "TRADING"),
                    })
                return rows
            except Exception as e:
                logger.error("[%s] Failed to fetch options instruments from DMA: %s", self.account_id, e)
                return []

        if is_futures or ex in ("exchange_2", "futures"):
            try:
                res = self._auth_request("GET", "futures/instrument_info", params={"exchange": "EXCHANGE_2"})
                data = res.get("data", {}) if isinstance(res, dict) else {}
                rows = []
                for sym, spec in data.items():
                    base = spec.get("base_asset", "").upper()
                    quote = spec.get("quote_asset", "").upper()
                    rows.append({
                        "tradingsymbol": sym,
                        "name": base,
                        "exchange": "EXCHANGE_2",
                        "segment": "FUTURES",
                        "instrument_type": "FUT",
                        "lot_size": float(spec.get("lot_size") or 1.0),
                        "tick_size": float(10 ** (-int(spec.get("price_precision") or 2))),
                        "base_asset": base,
                        "quote_asset": quote,
                        "max_leverage": int(spec.get("max_leverage") or 1),
                        "status": spec.get("status", "TRADING"),
                    })
                return rows
            except Exception as e:
                logger.error("[%s] Failed to fetch futures instrument_info: %s", self.account_id, e)
                return []

        # Spot exchanges (coinswitchx, c2c1, c2c2)
        try:
            res = self._auth_request("GET", "tradeInfo", params={"exchange": exchange})
            data = res.get("data", {}).get(exchange, {}) if isinstance(res, dict) else {}
            rows = []
            for sym, spec in data.items():
                base = sym.split("/")[0] if "/" in sym else sym
                quote = sym.split("/")[1] if "/" in sym else ""
                prec = spec.get("precision", {})
                rows.append({
                    "tradingsymbol": sym,
                    "name": base,
                    "exchange": exchange,
                    "segment": "SPOT",
                    "instrument_type": "EQ",
                    "lot_size": float(10 ** (-int(prec.get("base", 0)))),
                    "tick_size": float(10 ** (-int(prec.get("quote", 2)))),
                    "base_asset": base,
                    "quote_asset": quote,
                    "status": "TRADING",
                })
            return rows
        except Exception as e:
            logger.error("[%s] Failed to fetch spot tradeInfo for %s: %s", self.account_id, exchange, e)
            return []

    def get_all_instruments(self, base_coins: list[str] | None = None) -> dict[str, list[dict]]:
        """
        Fetch all active instruments across all CoinSwitch PRO Spot, Futures, and DMA Options exchanges.
        Returns a dict mapping exchange/segment names to their respective list of instruments:
        {'coinswitchx': [...], 'c2c1': [...], 'c2c2': [...], 'FUTURES': [...], 'OPTIONS': [...]}
        """
        all_sheets = {}
        for ex in ["coinswitchx", "c2c1", "c2c2"]:
            insts = self.instruments_list(exchange=ex, is_futures=False)
            if insts:
                all_sheets[ex] = insts

        fut_insts = self.instruments_list(exchange="EXCHANGE_2", is_futures=True)
        if fut_insts:
            all_sheets["FUTURES"] = fut_insts

        opt_insts = self.instruments_list(exchange="OPTIONS", is_options=True, base_coins=base_coins)
        if opt_insts:
            all_sheets["OPTIONS"] = opt_insts

        return all_sheets

    fetch_all_exchange_instruments = get_all_instruments

    def get_margin(
        self,
        symbol: str,
        quantity: float,
        price: float | None = None,
        leverage: int = 1,
    ) -> dict:
        """
        Calculate approximate initial margin requirement for an order.
        """
        norm_sym = self._normalize_symbol(symbol)
        ref_price = price
        if not ref_price or ref_price <= 0:
            try:
                t = self.ticker(norm_sym)
                ref_price = float(t.get("last_price") or t.get("close") or 0.0) if isinstance(t, dict) else 0.0
            except Exception:
                ref_price = 0.0

        notional = quantity * (ref_price or 0.0)
        lev = max(1, leverage)
        initial_margin = notional / lev if lev > 0 else notional

        return {
            "symbol": norm_sym,
            "quantity": quantity,
            "price": ref_price,
            "notional_value": round(notional, 4),
            "leverage": lev,
            "initial_margin": round(initial_margin, 4),
        }

    @staticmethod
    def sanitize_for_json(data: Any) -> Any:
        """Recursively convert DataFrames, datetimes, and numpy types for JSON serialization."""
        import numpy as np
        from datetime import date, datetime
        if isinstance(data, pl.DataFrame):
            return [CoinSwitchPROUtility.sanitize_for_json(row) for row in data.to_dicts()]
        elif isinstance(data, dict):
            return {str(k): CoinSwitchPROUtility.sanitize_for_json(v) for k, v in data.items()}
        elif isinstance(data, (list, tuple, set)):
            return [CoinSwitchPROUtility.sanitize_for_json(item) for item in data]
        elif isinstance(data, (datetime, date)):
            return data.isoformat()
        elif isinstance(data, float):
            if np.isnan(data) or np.isinf(data):
                return None
            return data
        elif isinstance(data, (np.floating, np.integer)):
            val = data.item()
            if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                return None
            return val
        return data

    def master_tkn_list(
        self,
        input_file: str,
        master_list: str = "",
        output_file: str = "",
        tz: str = "Asia/Kolkata",
        debug_mode: bool = False,
        month_cutoff: int = 0,
        aug_table: Optional[pl.DataFrame] = None,
        cap_config: Optional[pl.DataFrame] = None,
    ) -> Tuple[pl.DataFrame, np.ndarray, pl.DataFrame, pl.DataFrame]:
        """
        High-Performance Polars-Native Master Instrument Token Processor for CoinSwitch PRO.
        Loads configuration from Excel via polars_excel, fetches live CoinSwitch instruments,
        and assembles cum_table with crash-recovery caching.
        """
        import gc
        import threading
        import numpy as np
        import orjson
        from datetime import datetime, timedelta
        from algo_trading.algos.polars_excel import load_crypto_algo_config, write_polars_sheets_to_excel, build_aug_table
        from algo_trading.algos.crypto_master_tokens import (
            assemble_crypto_cum_table,
            today_ist,
        )

        # 1. Load Excel Config if not pre-supplied
        if aug_table is None or cap_config is None:
            cfg = load_crypto_algo_config(input_file)
            aug_table = cfg.aug_table
            cap_config = cfg.cap_config
        else:
            aug_table = build_aug_table(aug_table)

        # 2. Check crash recovery from ExchangeMasterData
        run_tkn_update = False
        today_date = today_ist(tz=tz)
        if self.broker:
            try:
                raw_master = self.broker.get_algo_state("master_tkn_list_update_time")
                if raw_master and isinstance(raw_master, list) and len(raw_master) > 0:
                    up_time = raw_master[0].get("updated_time")
                    if isinstance(up_time, str):
                        try:
                            up_time = datetime.fromisoformat(up_time).date()
                        except Exception:
                            up_time = today_date - timedelta(days=2)
                    elif isinstance(up_time, datetime):
                        up_time = up_time.date()
                    if up_time != today_date:
                        run_tkn_update = True
                else:
                    run_tkn_update = True
            except Exception:
                run_tkn_update = True
        else:
            run_tkn_update = True

        if not run_tkn_update and self.broker:
            try:
                db_cum = self.broker.get_master_data("cum_table")
                if db_cum:
                    cum_df = pl.from_dicts(db_cum) if isinstance(db_cum, list) else pl.from_dicts(orjson.loads(db_cum))
                    if not cum_df.is_empty():
                        if "Ref_stock_tkn" in cum_df.columns:
                            cum_df = cum_df.filter(pl.col("Ref_stock_tkn") > 0)

                        # Validate cached active symbols against current aug_table config
                        sym_col = "Symbol" if "Symbol" in aug_table.columns else ("name" if "name" in aug_table.columns else None)
                        current_active_symbols = set()
                        if aug_table is not None and not aug_table.is_empty() and sym_col and "Capital_share" in aug_table.columns:
                            current_active_symbols = set(
                                aug_table.filter(pl.col("Capital_share").cast(pl.Float64, strict=False) > 0)[sym_col].drop_nulls().to_list()
                            )
                        name_col = "name" if "name" in cum_df.columns else ("Symbol" if "Symbol" in cum_df.columns else None)
                        cached_active_symbols = set()
                        if name_col and "Capital_share" in cum_df.columns:
                            cached_active_symbols = set(
                                cum_df.filter(pl.col("Capital_share").cast(pl.Float64, strict=False) > 0)[name_col].drop_nulls().to_list()
                            )
                        cached_all_symbols = set(cum_df[name_col].drop_nulls().to_list()) if name_col else set()

                        if current_active_symbols and (current_active_symbols != cached_active_symbols or cached_all_symbols != current_active_symbols):
                            logger.info(
                                "Active crypto symbols in config (%s) differ from cached cum_table (%s). Re-assembling fresh cum_table.",
                                current_active_symbols,
                                cached_all_symbols,
                            )
                            run_tkn_update = True
                        else:
                            # Sync current Capital_share into cached cum_df in case weights were modified
                            if aug_table is not None and not aug_table.is_empty() and "Capital_share" in aug_table.columns and sym_col and name_col:
                                share_map = aug_table.select([pl.col(sym_col).alias(name_col), pl.col("Capital_share")]).unique(subset=[name_col])
                                cum_df = cum_df.drop("Capital_share").join(
                                    share_map,
                                    on=name_col,
                                    how="left",
                                ).with_columns(pl.col("Capital_share").fill_null(0.0))

                            inst_list_int = (
                                np.array(cum_df["instrument_token"].drop_nulls().unique().to_list(), dtype=np.int64)
                                if "instrument_token" in cum_df.columns
                                else np.array([], dtype=np.int64)
                            )
                            active_cum = cum_df.filter((pl.col("Capital_share") > 0) & (pl.col("Ref_stock_tkn") > 0)) if "Capital_share" in cum_df.columns else cum_df
                            init_ref_list = active_cum.select(pl.col("Ref_stock_tkn").alias("instrument_token")).unique() if "Ref_stock_tkn" in active_cum.columns else pl.DataFrame(schema={"instrument_token": pl.Int64})
                            index_ref_list = active_cum.filter(pl.col("Index_tkn") > 0).select(pl.col("Index_tkn").alias("instrument_token")).unique() if "Index_tkn" in active_cum.columns else pl.DataFrame(schema={"instrument_token": pl.Int64})
                            if "week_dist" in cum_df.columns:
                                cum_df = cum_df.with_columns(pl.col("week_dist").cast(pl.Int64, strict=False))
                            logger.info("Restored pre-computed cum_table from ExchangeMasterData for CoinSwitch (%d rows).", len(cum_df))
                            return cum_df, inst_list_int, init_ref_list, index_ref_list
            except Exception as ex:
                logger.warning("Failed to restore cum_table from ExchangeMasterData: %s", ex)
                run_tkn_update = True

        # 3. Fetch fresh instruments across CoinSwitch exchanges
        logger.info("Fetching fresh instruments list from CoinSwitch PRO across exchanges...")
        target_base_coins = list(set((aug_table["Symbol"].to_list() if not aug_table.is_empty() and "Symbol" in aug_table.columns else []) + ["BTC", "ETH", "SOL", "XRP", "DOGE"]))
        try:
            all_sheets_data = self.get_all_instruments(base_coins=target_base_coins)
        except Exception as e:
            logger.warning("Failed to fetch live instruments from CoinSwitch API: %s. Using offline fallback.", e)
            default_coins = ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "AVAX"]
            all_sheets_data = {
                "coinswitchx": [{"tradingsymbol": f"{c}/INR", "name": c, "exchange": "coinswitchx", "segment": "SPOT", "instrument_type": "EQ", "lot_size": 0.0001 if c == "BTC" else 0.01, "tick_size": 0.01, "base_asset": c, "quote_asset": "INR", "status": "TRADING"} for c in default_coins],
                "c2c1": [{"tradingsymbol": f"{c}/USDT", "name": c, "exchange": "c2c1", "segment": "SPOT", "instrument_type": "EQ", "lot_size": 0.0001 if c == "BTC" else 0.01, "tick_size": 0.01, "base_asset": c, "quote_asset": "USDT", "status": "TRADING"} for c in default_coins],
                "c2c2": [{"tradingsymbol": f"{c}/USDT", "name": c, "exchange": "c2c2", "segment": "SPOT", "instrument_type": "EQ", "lot_size": 0.0001 if c == "BTC" else 0.01, "tick_size": 0.01, "base_asset": c, "quote_asset": "USDT", "status": "TRADING"} for c in ["BTC", "ETH"]],
                "FUTURES": [{"tradingsymbol": f"{c}USDT", "name": c, "exchange": "EXCHANGE_2", "segment": "FUTURES", "instrument_type": "FUT", "lot_size": 0.001 if c == "BTC" else 0.01, "tick_size": 0.1 if c == "BTC" else 0.01, "base_asset": c, "quote_asset": "USDT", "max_leverage": 50 if c in ("BTC", "ETH") else 20, "status": "TRADING"} for c in default_coins],
            }

        # 4. Assemble cum_table
        cum_table, inst_list_int, init_ref_list, index_ref_list = assemble_crypto_cum_table(
            all_sheets_data=all_sheets_data,
            aug_table=aug_table,
            cap_config=cap_config,
            month_cutoff=month_cutoff,
            tz=tz,
            broker_name="coinswitch",
        )

        # 5. Persist to ExchangeMasterData for fast crash recovery
        if self.broker and not cum_table.is_empty():
            try:
                self.broker.set_master_data("cum_table", self.sanitize_for_json(cum_table), row_count=len(cum_table))
                if aug_table is not None and not aug_table.is_empty():
                    self.broker.set_master_data("aug_table", self.sanitize_for_json(aug_table), row_count=len(aug_table))
                self.broker.set_algo_state("master_tkn_list_update_time", [{"updated_time": today_date.isoformat()}])
            except Exception as ex:
                logger.warning("Failed to persist master data to DB: %s", ex)

        # 6. Background Excel export in debug mode
        if debug_mode and (output_file or master_list):
            def _export():
                try:
                    if output_file and not cum_table.is_empty():
                        write_polars_sheets_to_excel({"cum_table": cum_table}, output_file)
                except Exception as exc:
                    logger.warning("Background debug Excel export failed: %s", exc)

            threading.Thread(target=_export, daemon=True).start()

        # 7. Clean up RAM
        del all_sheets_data
        gc.collect()

        return cum_table, inst_list_int, init_ref_list, index_ref_list


# Aliases for flexible imports
CoinSwitchPROUtility = CoinSwitchUtility
