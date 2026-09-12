# -*- coding: utf-8 -*-
"""
algo_trading/algos/delta_utils.py
─────────────────────────────────
Wrapper around Delta Exchange REST APIs customized for order management, portfolio
inspection, margin calculations, candles, and multi-account trading in DeltaZero26.
Supports Delta Exchange Global (api.delta.exchange) and Delta Exchange India (api.india.delta.exchange).
Provides full dual compatibility with DeltaZero26 standard interface and official delta-rest-client methods.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import urlencode

import polars as pl
import requests

logger = logging.getLogger("algo_trading.algos.delta_utils")


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
                "[%s] Attempt %d/%d failed with error: %s. Retrying in %.1fs...",
                datetime.now().isoformat(),
                attempt,
                max_retries,
                e,
                current_delay,
            )
            time.sleep(current_delay)
            current_delay *= backoff
    if last_exc:
        raise last_exc


class DeltaExchangeUtility:
    """
    Order execution, account positions, margins, and holdings manager for Delta Exchange.
    Supports multi-account resolution via account_id or Broker model.
    """

    GLOBAL_BASE_URL: str = "https://api.delta.exchange"
    INDIA_BASE_URL: str = "https://api.india.delta.exchange"

    def __init__(
        self,
        account_id: str | None = None,
        broker_obj: Any = None,
        api_key: str | None = None,
        api_secret: str | None = None,
        base_url: str | None = None,
        client: Any = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize DeltaExchangeUtility by fetching credentials from the Broker database model
        or direct API keys.
        """
        self.client = client
        if broker_obj is None and "broker" in kwargs:
            broker_obj = kwargs["broker"]
        self.session = requests.Session()
        self._products_cache: dict[str, dict] = {}
        self._products_by_id: dict[int, dict] = {}
        self._last_products_fetch: float = 0.0

        if api_key is not None or api_secret is not None:
            self.broker = broker_obj
            self.account_id = account_id or "direct_delta"
            self.api_key = (api_key or "").strip()
            self.api_secret = (api_secret or "").strip()
            self.base_url = (base_url or self.GLOBAL_BASE_URL).rstrip("/")
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
                        Q(broker_name__code__iexact="delta_exchange")
                        | Q(broker_name__code__iexact="delta")
                        | Q(broker_name__code__iexact="delta_india")
                        | Q(api_provider__code__iexact="delta_exchange")
                        | Q(api_provider__code__iexact="delta")
                        | Q(api_provider__code__iexact="delta_india")
                        | Q(name__icontains="delta")
                    )
                    .order_by("-enable_trade")
                    .first()
                )

            if not self.broker:
                if account_id:
                    raise ValueError(f"Delta Exchange broker configuration for '{account_id}' not found in database.")
                logger.warning(
                    "[%s] Delta Exchange broker configuration not found in database. Initializing in public/read-only mode.",
                    datetime.now().isoformat(),
                )
                self.broker = None
                self.account_id = "delta_public"
                self.api_key = ""
                self.api_secret = ""
                self.base_url = (base_url or self.GLOBAL_BASE_URL).rstrip("/")
            else:
                self.account_id = self.broker.account_id or self.broker.name
                self.api_key = (self.broker.api_key or "").strip()
                self.api_secret = (self.broker.api_secret or "").strip()

                if base_url:
                    self.base_url = base_url.rstrip("/")
                elif self.broker.api_endpoint:
                    self.base_url = self.broker.api_endpoint.rstrip("/")
                else:
                    ident = f"{getattr(self.broker.broker_name, 'code', '')}_{getattr(self.broker.api_provider, 'code', '')}_{self.broker.name}".lower()
                    if "india" in ident:
                        self.base_url = self.INDIA_BASE_URL
                    else:
                        self.base_url = self.GLOBAL_BASE_URL

        if not self.api_key:
            logger.debug("API key is missing for Delta Exchange account '%s'. Only public endpoints accessible.", self.account_id)
        if not self.api_secret:
            logger.debug("API secret is missing for Delta Exchange account '%s'. Only public endpoints accessible.", self.account_id)

        logger.info(
            "[%s] Initialized DeltaExchangeUtility for account '%s' (Base URL: %s)",
            datetime.now().isoformat(),
            self.account_id,
            self.base_url,
        )

    # ──────────────────────────────────────────────────────────────
    # Authentication & REST Helpers
    # ──────────────────────────────────────────────────────────────

    def _generate_signature(self, method: str, path: str, query_string: str = "", payload: str = "") -> tuple[str, str]:
        """
        Generate HMAC-SHA256 signature for Delta Exchange API v2 with automatic clock drift offset.
        Signature message = METHOD + TIMESTAMP + PATH + QUERY_STRING + PAYLOAD
        """
        if not self.api_secret:
            raise ValueError(f"Delta Exchange API secret is missing for account '{self.account_id}'.")
        offset = getattr(self, "_server_time_offset", 0)
        timestamp = str(int(time.time()) + offset)
        path_clean = path if path.startswith("/") else f"/{path}"
        msg = method.upper() + timestamp + path_clean + (query_string if query_string else "") + payload
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            msg.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return signature, timestamp

    def _auth_request(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        data: dict | None = None,
    ) -> Any:
        """
        Execute an authenticated HTTP request against Delta Exchange API v2.
        """
        if not self.api_key or not self.api_secret:
            raise ValueError(
                f"Delta Exchange API credentials (api_key/api_secret) are missing for account '{self.account_id}'. "
                "Authenticated action cannot proceed."
            )
        path = endpoint if endpoint.startswith("/v2/") else f"/v2/{endpoint.lstrip('/')}"
        query_string = ""
        if params:
            query_string = f"?{urlencode(sorted(params.items()))}"

        payload_str = ""
        if data is not None and method.upper() in ("POST", "PUT", "DELETE", "PATCH"):
            payload_str = json.dumps(data, separators=(",", ":"))

        url = f"{self.base_url}{path}{query_string}"

        for attempt in range(2):
            signature, timestamp = self._generate_signature(
                method=method,
                path=path,
                query_string=query_string,
                payload=payload_str,
            )

            headers = {
                "Content-Type": "application/json",
                "api-key": self.api_key,
                "signature": signature,
                "timestamp": timestamp,
                "User-Agent": "DeltaZero26-Engine/1.0",
            }

            def _do_req():
                req_data = payload_str if payload_str else None
                resp = self.session.request(
                    method=method,
                    url=url,
                    data=req_data,
                    headers=headers,
                    timeout=15,
                )
                return resp

            response = retry(_do_req)

            # Auto-handle expired_signature / network clock drift or IP restrictions
            if response.status_code == 401:
                try:
                    err_json = response.json()
                    err_code = err_json.get("error", {}).get("code")
                    ctx = err_json.get("error", {}).get("context", {})
                    server_time = ctx.get("server_time")
                    if err_code == "expired_signature" and server_time and attempt == 0:
                        self._server_time_offset = int(server_time) - int(time.time())
                        logger.info("[%s] Synchronized Delta server time offset: %+ds", datetime.now().isoformat(), self._server_time_offset)
                        continue
                    elif err_code == "ip_not_whitelisted_for_api_key":
                        client_ip = ctx.get("client_ip", "unknown")
                        logger.error(
                            "[%s] [DELTA IP RESTRICTION ERROR] Machine public IP '%s' is NOT whitelisted on Delta Exchange for account '%s'. Please whitelist '%s' or disable IP restriction in Delta Exchange API settings.",
                            datetime.now().isoformat(),
                            client_ip,
                            self.account_id,
                            client_ip,
                        )
                except Exception:
                    pass

            if response.status_code not in (200, 201):
                logger.error(
                    "[%s] Delta Exchange API error (%s %s) [HTTP %s]: %s",
                    datetime.now().isoformat(),
                    method,
                    path,
                    response.status_code,
                    response.text,
                )
                try:
                    err_json = response.json()
                    error_msg = err_json.get("error", {}).get("message") or err_json.get("message") or response.text
                    raise RuntimeError(f"Delta API Error ({response.status_code}): {error_msg}")
                except Exception as parse_err:
                    if isinstance(parse_err, RuntimeError):
                        raise parse_err
                    raise RuntimeError(f"Delta API HTTP {response.status_code}: {response.text}")

            res_json = response.json()
            if isinstance(res_json, dict) and "result" in res_json:
                return res_json["result"]
            return res_json

    def _public_get(self, endpoint: str, params: dict | None = None) -> Any:
        """Execute unauthenticated GET request against public market data."""
        path = endpoint if endpoint.startswith("/v2/") else f"/v2/{endpoint.lstrip('/')}"
        url = f"{self.base_url}{path}"

        def _do_req():
            return self.session.get(url, params=params, timeout=15)

        response = retry(_do_req)
        if response.status_code not in (200, 201):
            raise RuntimeError(f"Public API error ({response.status_code}): {response.text}")

        res_json = response.json()
        if isinstance(res_json, dict) and "result" in res_json:
            return res_json["result"]
        return res_json

    # ──────────────────────────────────────────────────────────────
    # Product Master & Symbol Resolution
    # ──────────────────────────────────────────────────────────────

    def get_products(self, contract_types: list[str] | None = None, force_refresh: bool = False) -> list[dict]:
        """
        Fetch all tradeable products / instruments from Delta Exchange.
        Caches in-memory for 5 minutes.
        """
        now = time.time()
        if force_refresh or (now - self._last_products_fetch) > 300 or not self._products_cache:
            try:
                products = self._public_get("/products")
                if isinstance(products, list):
                    self._products_cache = {p.get("symbol"): p for p in products if p.get("symbol")}
                    self._products_by_id = {int(p.get("id")): p for p in products if p.get("id")}
                    self._last_products_fetch = now
            except Exception as e:
                logger.error("[%s] Error fetching Delta Exchange products: %s", datetime.now().isoformat(), e)

        all_products = list(self._products_cache.values())
        if contract_types:
            types_set = set(contract_types)
            return [p for p in all_products if p.get("contract_type") in types_set]
        return all_products

    def get_product(self, product_id_or_symbol: int | str) -> dict | None:
        """Lookup a product by integer ID or symbol string."""
        if not self._products_cache:
            self.get_products()

        if isinstance(product_id_or_symbol, int) or (isinstance(product_id_or_symbol, str) and product_id_or_symbol.isdigit()):
            pid = int(product_id_or_symbol)
            if pid in self._products_by_id:
                return self._products_by_id[pid]

        sym = str(product_id_or_symbol).strip().upper()
        if sym in self._products_cache:
            return self._products_cache[sym]

        # Refresh once if not found
        self.get_products(force_refresh=True)
        if isinstance(product_id_or_symbol, int) or (isinstance(product_id_or_symbol, str) and product_id_or_symbol.isdigit()):
            return self._products_by_id.get(int(product_id_or_symbol))
        return self._products_cache.get(sym)

    def resolve_product_id(self, symbol_or_id: int | str) -> int:
        """Resolve symbol string (e.g. 'BTCUSD') to numeric product_id."""
        if isinstance(symbol_or_id, int):
            return symbol_or_id
        if isinstance(symbol_or_id, str) and symbol_or_id.isdigit():
            return int(symbol_or_id)

        product = self.get_product(symbol_or_id)
        if not product or "id" not in product:
            raise ValueError(f"Could not resolve product ID for symbol '{symbol_or_id}'.")
        return int(product["id"])

    # ──────────────────────────────────────────────────────────────
    # Standard DeltaZero26 API Methods
    # ──────────────────────────────────────────────────────────────

    def chk_live_bal(self) -> tuple[float, float]:
        """
        Fetch available cash and total net portfolio capital across all assets (USDT, USD, BTC, INR).
        Returns: (available_cash, total_net_capital)
        """
        if not self.api_key or not self.api_secret:
            msg = f"Failed to fetch Delta Exchange live balance: API credentials (api_key/api_secret) are missing for account '{self.account_id}'. Authenticated action cannot proceed."
            try:
                from algo_trading.algos.logger import algo_logger
                algo_logger.log_sync(
                    msg,
                    tag="BROKER_API",
                    level="ERROR",
                    broker_obj=self.broker,
                    algo_name="crypto_opt_trde_polars",
                )
            except Exception:
                logger.error("[%s] %s", datetime.now().isoformat(), msg)
            return 0.0, 0.0

        try:
            balances = self._auth_request("GET", "/wallet/balances")
            available_cash = 0.0
            total_net_capital = 0.0

            if isinstance(balances, list):
                for b in balances:
                    avail = float(b.get("available_balance") or b.get("balance") or 0.0)
                    total = float(b.get("balance") or 0.0)
                    asset = b.get("asset")
                    if isinstance(asset, dict):
                        symbol = (asset.get("symbol") or b.get("asset_symbol") or "").upper()
                    else:
                        symbol = str(b.get("asset_symbol") or "").upper()

                    # Count primary stable/cash currencies directly, or total equity
                    if symbol in ("USDT", "USD", "INR", "USDC"):
                        available_cash += avail
                        total_net_capital += total
                    else:
                        # Non-cash crypto asset
                        total_net_capital += total

            return float(available_cash), float(total_net_capital)
        except Exception as exc:
            logger.error("[%s] Failed to fetch Delta Exchange live balance: %s", datetime.now().isoformat(), exc)
            return 0.0, 0.0

    def holdings(self) -> pl.DataFrame | None:
        """
        Fetch wallet asset holdings returned as a Polars DataFrame.
        Columns: asset_symbol, balance, available_balance, locked_balance, tradingsymbol
        """
        empty_holdings = pl.DataFrame(
            schema={
                "asset_symbol": pl.String,
                "balance": pl.Float64,
                "available_balance": pl.Float64,
                "locked_balance": pl.Float64,
                "tradingsymbol": pl.String,
            }
        )

        if not self.api_key or not self.api_secret:
            msg = f"Failed to fetch Delta Exchange holdings: API credentials (api_key/api_secret) are missing for account '{self.account_id}'. Authenticated action cannot proceed."
            try:
                from algo_trading.algos.logger import algo_logger
                algo_logger.log_sync(
                    msg,
                    tag="BROKER_API",
                    level="ERROR",
                    broker_obj=self.broker,
                    algo_name="crypto_opt_trde_polars",
                )
            except Exception:
                logger.error("[%s] %s", datetime.now().isoformat(), msg)
            return empty_holdings

        try:
            balances = self._auth_request("GET", "/wallet/balances")
            if not isinstance(balances, list) or not balances:
                return empty_holdings

            records = []
            for b in balances:
                asset = b.get("asset")
                if isinstance(asset, dict):
                    sym = asset.get("symbol") or b.get("asset_symbol") or ""
                else:
                    sym = str(b.get("asset_symbol") or "")
                bal = float(b.get("balance") or 0.0)
                avail = float(b.get("available_balance") or 0.0)
                locked = max(0.0, bal - avail)

                if bal > 0 or avail > 0:
                    records.append({
                        "asset_symbol": sym,
                        "balance": bal,
                        "available_balance": avail,
                        "locked_balance": locked,
                        "tradingsymbol": sym,
                    })

            if not records:
                return empty_holdings

            return pl.DataFrame(records)
        except Exception as exc:
            logger.error("[%s] Failed to fetch Delta Exchange holdings: %s", datetime.now().isoformat(), exc)
            return empty_holdings

    def pos_data(self, pair: str | None = None) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
        """
        Fetch active positions for Perpetual Futures and Options derivatives.
        Returns: (day_positions_df, net_positions_df)
        """
        empty_df = pl.DataFrame(
            schema={
                "symbol": pl.String,
                "product_id": pl.Int64,
                "size": pl.Float64,
                "entry_price": pl.Float64,
                "mark_price": pl.Float64,
                "liquidation_price": pl.Float64,
                "margin": pl.Float64,
                "unrealized_pnl": pl.Float64,
                "realized_pnl": pl.Float64,
            }
        )

        if not self.api_key or not self.api_secret:
            msg = f"Failed to fetch Delta Exchange positions: API credentials (api_key/api_secret) are missing for account '{self.account_id}'. Authenticated action cannot proceed."
            try:
                from algo_trading.algos.logger import algo_logger
                algo_logger.log_sync(
                    msg,
                    tag="BROKER_API",
                    level="ERROR",
                    broker_obj=self.broker,
                    algo_name="crypto_opt_trde_polars",
                )
            except Exception:
                logger.error("[%s] %s", datetime.now().isoformat(), msg)
            return empty_df, empty_df

        try:
            positions = self._auth_request("GET", "/positions/margined")
            if not isinstance(positions, list) or not positions:
                return empty_df, empty_df

            records = []
            for p in positions:
                size = float(p.get("size") or 0.0)
                if abs(size) <= 1e-8:
                    continue

                product = p.get("product") or {}
                sym = product.get("symbol") if isinstance(product, dict) else str(p.get("product_symbol", ""))
                pid = int(product.get("id") or p.get("product_id") or 0)

                if pair and sym.upper() != pair.upper():
                    continue

                records.append({
                    "symbol": sym,
                    "product_id": pid,
                    "size": size,
                    "entry_price": float(p.get("entry_price") or 0.0),
                    "mark_price": float(p.get("mark_price") or 0.0),
                    "liquidation_price": float(p.get("liquidation_price") or 0.0),
                    "margin": float(p.get("margin") or 0.0),
                    "unrealized_pnl": float(p.get("unrealized_pnl") or 0.0),
                    "realized_pnl": float(p.get("realized_pnl") or 0.0),
                })

            if not records:
                return empty_df, empty_df

            df = pl.DataFrame(records)
            return df, df
        except Exception as exc:
            err_msg = f"Failed to fetch Delta Exchange positions: {exc}"
            logger.error("[%s] %s", datetime.now().isoformat(), err_msg)
            try:
                from algo_trading.algos.logger import algo_logger
                algo_logger.log_sync(
                    err_msg,
                    tag="BROKER_API",
                    level="ERROR",
                    broker_obj=self.broker,
                    algo_name="crypto_opt_trde_polars",
                )
            except Exception:
                pass
            return empty_df, empty_df

    def mrk_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str = "BUY",
        product_id: int | None = None,
        **kwargs: Any,
    ) -> tuple[str | int, str]:
        """
        Place Market Order on Delta Exchange.
        Returns: (order_id, status_message)
        """
        try:
            pid = product_id or self.resolve_product_id(symbol)
            side = "buy" if buy_sell.upper().startswith("B") else "sell"
            size = int(quantity) if quantity >= 1 else quantity

            payload = {
                "product_id": pid,
                "size": size,
                "side": side,
                "order_type": "market_order",
                "time_in_force": kwargs.get("time_in_force", "ioc"),
            }
            if "bracket_stop_loss_price" in kwargs:
                payload["bracket_stop_loss_price"] = str(kwargs["bracket_stop_loss_price"])
            if "bracket_take_profit_price" in kwargs:
                payload["bracket_take_profit_price"] = str(kwargs["bracket_take_profit_price"])

            res = self._auth_request("POST", "/orders", data=payload)
            order_id = res.get("id") or res.get("order_id") or "UNKNOWN_ID"
            msg = f"Market {side.upper()} order placed successfully (ID: {order_id})"
            logger.info("[%s] %s", datetime.now().isoformat(), msg)
            return str(order_id), msg
        except Exception as exc:
            err_msg = f"Failed to place market order for {symbol}: {exc}"
            logger.error("[%s] %s", datetime.now().isoformat(), err_msg)
            return -1, err_msg

    def lim_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str = "BUY",
        price: float | None = None,
        product_id: int | None = None,
        **kwargs: Any,
    ) -> tuple[str | int, str]:
        """
        Place Limit Order on Delta Exchange.
        Returns: (order_id, status_message)
        """
        if not price or float(price) <= 0:
            return -1, "Price must be provided and > 0 for Limit Orders."

        try:
            pid = product_id or self.resolve_product_id(symbol)
            side = "buy" if buy_sell.upper().startswith("B") else "sell"
            size = int(quantity) if quantity >= 1 else quantity

            payload = {
                "product_id": pid,
                "size": size,
                "side": side,
                "order_type": "limit_order",
                "limit_price": str(price),
                "time_in_force": kwargs.get("time_in_force", "gtc"),
                "post_only": kwargs.get("post_only", False),
            }

            res = self._auth_request("POST", "/orders", data=payload)
            order_id = res.get("id") or res.get("order_id") or "UNKNOWN_ID"
            msg = f"Limit {side.upper()} order placed successfully at {price} (ID: {order_id})"
            logger.info("[%s] %s", datetime.now().isoformat(), msg)
            return str(order_id), msg
        except Exception as exc:
            err_msg = f"Failed to place limit order for {symbol} at {price}: {exc}"
            logger.error("[%s] %s", datetime.now().isoformat(), err_msg)
            return -1, err_msg

    def sl_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str = "BUY",
        price: float | None = None,
        trig_price: float | None = None,
        product_id: int | None = None,
        stop_order_type: str = "stop_loss_order",
        **kwargs: Any,
    ) -> tuple[str | int, str]:
        """
        Place Stop-Loss / Stop-Limit Order on Delta Exchange.
        Returns: (order_id, status_message)
        """
        if not trig_price or float(trig_price) <= 0:
            return -1, "Trigger price (trig_price) must be provided and > 0 for Stop Orders."

        try:
            pid = product_id or self.resolve_product_id(symbol)
            side = "buy" if buy_sell.upper().startswith("B") else "sell"
            size = int(quantity) if quantity >= 1 else quantity

            payload = {
                "product_id": pid,
                "size": size,
                "side": side,
                "order_type": stop_order_type,
                "stop_price": str(trig_price),
            }
            if price and float(price) > 0:
                payload["limit_price"] = str(price)

            res = self._auth_request("POST", "/orders", data=payload)
            order_id = res.get("id") or res.get("order_id") or "UNKNOWN_ID"
            msg = f"Stop order placed successfully at trigger {trig_price} (ID: {order_id})"
            logger.info("[%s] %s", datetime.now().isoformat(), msg)
            return str(order_id), msg
        except Exception as exc:
            err_msg = f"Failed to place stop order for {symbol}: {exc}"
            logger.error("[%s] %s", datetime.now().isoformat(), err_msg)
            return -1, err_msg

    def cancel_ordr(self, order_id: str | int | None = None, product_id: int | None = None, **kwargs: Any) -> tuple[bool, str]:
        """
        Cancel an open order on Delta Exchange.
        Returns: (success: bool, status_message: str)
        """
        if not order_id:
            return False, "order_id is required for cancellation."

        try:
            payload: dict[str, Any] = {"id": int(order_id) if str(order_id).isdigit() else order_id}
            if product_id:
                payload["product_id"] = product_id

            self._auth_request("DELETE", "/orders", data=payload)
            msg = f"Order {order_id} cancelled successfully."
            logger.info("[%s] %s", datetime.now().isoformat(), msg)
            return True, msg
        except Exception as exc:
            err_msg = f"Failed to cancel order {order_id}: {exc}"
            logger.error("[%s] %s", datetime.now().isoformat(), err_msg)
            return False, err_msg

    def cancel_all(self, product_id: int | None = None, symbol: str | None = None, **kwargs: Any) -> tuple[bool, str]:
        """
        Cancel all open orders for a product or the entire account.
        Returns: (success: bool, status_message: str)
        """
        try:
            payload: dict[str, Any] = {}
            if product_id:
                payload["product_id"] = product_id
            elif symbol:
                payload["product_id"] = self.resolve_product_id(symbol)

            self._auth_request("DELETE", "/orders/all", data=payload)
            target = f"for product {payload.get('product_id')}" if "product_id" in payload else "for entire account"
            msg = f"All open orders cancelled {target}."
            logger.info("[%s] %s", datetime.now().isoformat(), msg)
            return True, msg
        except Exception as exc:
            err_msg = f"Failed to cancel open orders: {exc}"
            logger.error("[%s] %s", datetime.now().isoformat(), err_msg)
            return False, err_msg

    def candles(
        self,
        symbol: str,
        resolution: str = "1m",
        limit: int = 100,
        start: int | None = None,
        end: int | None = None,
    ) -> pl.DataFrame | None:
        """
        Fetch historical candlestick bars from Delta Exchange /history/candles endpoint.
        Returns: Polars DataFrame with columns: date_time, open, high, low, close, volume
        """
        try:
            pid = self.resolve_product_id(symbol)
            now_sec = int(time.time())
            end_sec = end or now_sec

            # Approximate resolution to seconds
            res_sec = 60
            if resolution.endswith("m"):
                res_sec = int(resolution[:-1]) * 60
            elif resolution.endswith("h"):
                res_sec = int(resolution[:-1]) * 3600
            elif resolution.endswith("d"):
                res_sec = int(resolution[:-1]) * 86400

            start_sec = start or (end_sec - (limit * res_sec))

            params = {
                "resolution": resolution,
                "start": start_sec,
                "end": end_sec,
            }
            data = self._public_get(f"/chart/history?symbol={symbol}&resolution={resolution}&from={start_sec}&to={end_sec}")
            
            # Format: {'t': [...], 'o': [...], 'h': [...], 'l': [...], 'c': [...], 'v': [...], 's': 'ok'}
            if isinstance(data, dict) and "t" in data and "c" in data:
                timestamps = data.get("t", [])
                opens = [float(x) for x in data.get("o", [])]
                highs = [float(x) for x in data.get("h", [])]
                lows = [float(x) for x in data.get("l", [])]
                closes = [float(x) for x in data.get("c", [])]
                volumes = [float(x) for x in data.get("v", [])]

                dt_strings = [
                    datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                    for ts in timestamps
                ]

                return pl.DataFrame({
                    "date_time": dt_strings,
                    "open": opens,
                    "high": highs,
                    "low": lows,
                    "close": closes,
                    "volume": volumes,
                    "instrument_token": [symbol] * len(timestamps),
                })

            return None
        except Exception as exc:
            logger.error("[%s] Failed to fetch Delta Exchange candles for %s: %s", datetime.now().isoformat(), symbol, exc)
            return None

    # ──────────────────────────────────────────────────────────────
    # delta-rest-client Official Method Compatibility Aliases
    # ──────────────────────────────────────────────────────────────

    def get_wallet(self, asset_id: int | None = None) -> Any:
        """Alias for delta-rest-client get_wallet()"""
        endpoint = f"/wallet/balances/{asset_id}" if asset_id else "/wallet/balances"
        return self._auth_request("GET", endpoint)

    def get_ticker(self, symbol: str) -> Any:
        """Alias for delta-rest-client get_ticker()"""
        return self._public_get(f"/tickers/{symbol}")

    def get_assets(self) -> Any:
        """Alias for delta-rest-client get_assets()"""
        return self._public_get("/assets")

    def get_orders(self, status: str | None = None, product_id: int | None = None) -> list[dict]:
        """Alias for delta-rest-client get_orders()"""
        params: dict[str, Any] = {}
        if status:
            params["state"] = status
        if product_id:
            params["product_id"] = product_id
        res = self._auth_request("GET", "/orders", params=params)
        return res if isinstance(res, list) else []

    def orders(self, **kwargs: Any) -> pl.DataFrame:
        """Fetch current orders as Polars DataFrame for multi-account engines."""
        try:
            raw = self.get_orders(**kwargs)
            return pl.from_dicts(raw) if raw else pl.DataFrame()
        except Exception:
            return pl.DataFrame()

    def order_book(self, **kwargs: Any) -> pl.DataFrame:
        """Alias for orders() returning Polars DataFrame."""
        return self.orders(**kwargs)

    def create_order(
        self,
        product_id: int,
        size: float | int,
        side: str,
        order_type: str = "limit_order",
        limit_price: str | float | None = None,
        stop_price: str | float | None = None,
        **kwargs: Any,
    ) -> dict:
        """Alias for delta-rest-client create_order()"""
        payload: dict[str, Any] = {
            "product_id": product_id,
            "size": size,
            "side": side.lower(),
            "order_type": order_type,
            **kwargs,
        }
        if limit_price is not None:
            payload["limit_price"] = str(limit_price)
        if stop_price is not None:
            payload["stop_price"] = str(stop_price)

        return self._auth_request("POST", "/orders", data=payload)

    def batch_create(self, product_id: int, orders: list[dict]) -> list[dict]:
        """Alias for delta-rest-client batch_create()"""
        payload = {"product_id": product_id, "orders": orders}
        return self._auth_request("POST", "/orders/batch", data=payload)

    def cancel_order(self, product_id: int, order_id: int | str) -> dict:
        """Alias for delta-rest-client cancel_order()"""
        payload = {"product_id": product_id, "id": int(order_id) if str(order_id).isdigit() else order_id}
        return self._auth_request("DELETE", "/orders", data=payload)

    def get_positions(self, product_id: int | None = None) -> Any:
        """Alias for delta-rest-client get_position()"""
        if product_id:
            return self._auth_request("GET", f"/positions?product_id={product_id}")
        return self._auth_request("GET", "/positions/margined")

    def get_l2_orderbook(self, product_id: int) -> dict:
        """Alias for delta-rest-client get_L2_orders()"""
        return self._public_get(f"/l2orderbook/{product_id}")

    @staticmethod
    def sanitize_for_json(data: Any) -> Any:
        """Recursively convert DataFrames, datetimes, and numpy types for JSON serialization."""
        import numpy as np
        import polars as pl
        from datetime import date, datetime
        if isinstance(data, pl.DataFrame):
            return [DeltaExchangeUtility.sanitize_for_json(row) for row in data.to_dicts()]
        elif isinstance(data, dict):
            return {str(k): DeltaExchangeUtility.sanitize_for_json(v) for k, v in data.items()}
        elif isinstance(data, (list, tuple, set)):
            return [DeltaExchangeUtility.sanitize_for_json(item) for item in data]
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
        aug_table: Optional[Any] = None,
        cap_config: Optional[Any] = None,
    ) -> Tuple[Any, Any, Any, Any]:
        """
        High-Performance Polars-Native Master Instrument Token Processor for Delta Exchange.
        Loads configuration from Excel via polars_excel, fetches live Delta Exchange products,
        and assembles cum_table with crash-recovery caching.
        """
        import gc
        import threading
        import numpy as np
        import polars as pl
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
                            logger.info("Restored pre-computed cum_table from ExchangeMasterData for Delta Exchange (%d rows).", len(cum_df))
                            return cum_df, inst_list_int, init_ref_list, index_ref_list
            except Exception as ex:
                logger.warning("Failed to restore cum_table from ExchangeMasterData: %s", ex)
                run_tkn_update = True

        # 3. Fetch fresh products from Delta Exchange API
        logger.info("Fetching fresh products list from Delta Exchange...")
        all_sheets_data: Dict[str, list] = {"c2c1": [], "FUTURES": [], "OPTIONS": []}
        try:
            products = self.get_products()
            if isinstance(products, list):
                for p in products:
                    if not isinstance(p, dict):
                        continue
                    ct = str(p.get("contract_type") or "").lower()
                    if "option" in ct:
                        all_sheets_data["OPTIONS"].append(p)
                    elif "spot" in ct:
                        all_sheets_data["c2c1"].append(p)
                    else:
                        all_sheets_data["FUTURES"].append(p)
        except Exception as e:
            logger.warning("Failed to fetch live products from Delta Exchange API: %s. Using offline fallback.", e)
            default_coins = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
            all_sheets_data = {
                "c2c1": [{"symbol": f"{c}USDT", "name": c, "segment": "SPOT", "contract_type": "spot", "tick_size": 0.01, "lot_size": 1.0} for c in default_coins],
                "FUTURES": [{"symbol": f"{c}USD", "name": c, "segment": "FUTURES", "contract_type": "perpetual_futures", "tick_size": 0.1, "lot_size": 1.0} for c in default_coins],
                "OPTIONS": [],
            }

        # 4. Assemble cum_table
        cum_table, inst_list_int, init_ref_list, index_ref_list = assemble_crypto_cum_table(
            all_sheets_data=all_sheets_data,
            aug_table=aug_table,
            cap_config=cap_config,
            month_cutoff=month_cutoff,
            tz=tz,
            broker_name="delta",
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
DeltaUtility = DeltaExchangeUtility

