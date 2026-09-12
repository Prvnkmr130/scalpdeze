# -*- coding: utf-8 -*-
"""
algo_trading/algos/coindcx_utils.py
───────────────────────────────────
Wrapper around CoinDCX REST APIs customized for order management, portfolio
inspection, margin calculations, candles, and multi-account trading in DeltaZero26.
Supports both CoinDCX Futures (derivatives) and Spot trading.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
import requests

logger = logging.getLogger("algo_trading.algos.coindcx_utils")


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


class CoinDCXUtility:
    """
    Order execution, account positions, margins, and holdings manager for CoinDCX.
    Supports multi-account resolution via account_id or Broker model.
    """

    BASE_URL: str = "https://api.coindcx.com"
    PUBLIC_BASE_URL: str = "https://public.coindcx.com"

    def __init__(
        self,
        account_id: str | None = None,
        broker_obj: Any = None,
        api_key: str | None = None,
        api_secret: str | None = None,
        client: Any = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize CoinDCXUtility by fetching credentials from the Broker database model
        or direct API keys.

        Parameters
        ----------
        account_id : str, optional
            Account ID or name (e.g. 'coindcx'). If None and no keys passed,
            loads the first active CoinDCX account.
        broker_obj : Broker, optional
            Direct Broker model instance.
        api_key : str, optional
            Direct API key string.
        api_secret : str, optional
            Direct API secret string.
        client : Any, optional
            Pre-configured client instance for testing/mocking.
        """
        self.client = client
        if broker_obj is None and "broker" in kwargs:
            broker_obj = kwargs["broker"]
        self.session = requests.Session()

        if api_key is not None or api_secret is not None:
            self.broker = broker_obj
            self.account_id = account_id or "direct_coindcx"
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
                        Q(broker_name__code__iexact="coindcx")
                        | Q(api_provider__code__iexact="coindcx")
                        | Q(name__icontains="coindcx")
                    )
                    .order_by("-enable_websocket")
                    .first()
                )

            if not self.broker:
                if account_id:
                    raise ValueError(f"CoinDCX broker configuration for '{account_id}' not found in database.")
                logger.warning(
                    "[%s] CoinDCX broker configuration not found in database. Initializing in public market data mode.",
                    datetime.now().isoformat(),
                )
                self.broker = None
                self.account_id = "coindcx_public"
                self.api_key = ""
                self.api_secret = ""
            else:
                self.account_id = self.broker.account_id or self.broker.name
                self.api_key = (self.broker.api_key or "").strip()
                self.api_secret = (self.broker.api_secret or "").strip()

        if not self.api_key or not self.api_secret:
            logger.warning(
                "[%s] API credentials missing for CoinDCX account '%s'. Live orders/balances disabled.",
                datetime.now().isoformat(),
                self.account_id,
            )

        logger.info(
            "[%s] Initialized CoinDCXUtility for account '%s'",
            datetime.now().isoformat(),
            self.account_id,
        )

    def _sign_payload(self, body: dict) -> tuple[str, str]:
        """
        Generate JSON serialized string and HMAC-SHA256 signature for CoinDCX API authentication.
        """
        if not self.api_secret:
            raise ValueError(f"CoinDCX API secret is missing for account '{self.account_id}'.")
        json_body = json.dumps(body, separators=(",", ":"))
        secret_bytes = self.api_secret.encode("utf-8")
        signature = hmac.new(secret_bytes, json_body.encode("utf-8"), hashlib.sha256).hexdigest()
        return json_body, signature

    def _auth_get(self, endpoint: str, params: dict | None = None, base_url: str | None = None) -> Any:
        """
        Execute an authenticated GET request against the CoinDCX API with signature headers.
        """
        if not self.api_key or not self.api_secret:
            raise ValueError(
                f"CoinDCX credentials (api_key/api_secret) are missing for account '{self.account_id}'. "
                "Authenticated action cannot proceed."
            )
        url = f"{base_url or self.BASE_URL}/{endpoint.lstrip('/')}"
        body = params.copy() if params else {}
        if "timestamp" not in body:
            body["timestamp"] = int(round(time.time() * 1000))

        json_body, signature = self._sign_payload(body)
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
        }

        response = self.session.get(url, data=json_body, headers=headers, timeout=15)
        if response.status_code not in (200, 201):
            logger.error(
                "[%s] CoinDCX GET (auth) %s failed (%d): %s",
                self.account_id,
                endpoint,
                response.status_code,
                response.text,
            )
            response.raise_for_status()

        return response.json()

    def _post(self, endpoint: str, payload: dict | None = None, base_url: str | None = None) -> Any:
        """
        Execute an authenticated POST request against the CoinDCX API.
        """
        if not self.api_key or not self.api_secret:
            raise ValueError(
                f"CoinDCX credentials (api_key/api_secret) are missing for account '{self.account_id}'. "
                "Authenticated action cannot proceed."
            )
        url = f"{base_url or self.BASE_URL}/{endpoint.lstrip('/')}"
        body = payload.copy() if payload else {}
        if "timestamp" not in body:
            body["timestamp"] = int(round(time.time() * 1000))

        json_body, signature = self._sign_payload(body)
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
        }

        response = self.session.post(url, data=json_body, headers=headers, timeout=15)
        if response.status_code not in (200, 201):
            logger.error(
                "[%s] CoinDCX POST %s failed (%d): %s",
                self.account_id,
                endpoint,
                response.status_code,
                response.text,
            )
            response.raise_for_status()

        return response.json()

    def _get(self, endpoint: str, params: dict | None = None, base_url: str | None = None) -> Any:
        """
        Execute a GET request (public or standard) against CoinDCX API.
        """
        url = f"{base_url or self.BASE_URL}/{endpoint.lstrip('/')}"
        response = self.session.get(url, params=params, timeout=15)
        if response.status_code != 200:
            logger.error(
                "[%s] CoinDCX GET %s failed (%d): %s",
                self.account_id,
                endpoint,
                response.status_code,
                response.text,
            )
            response.raise_for_status()
        return response.json()

    # ──────────────────────────────────────────────────────────────
    # Account, Holdings, Balances & Margins
    # ──────────────────────────────────────────────────────────────

    def get_spot_balances(self) -> list[dict]:
        """Fetch spot wallet balances for the account."""
        if not self.api_key or not self.api_secret:
            return []
        return retry(lambda: self._post("exchange/v1/users/balances"))

    def get_futures_wallets(self) -> list[dict]:
        """Fetch futures wallet balances and margin information."""
        if not self.api_key or not self.api_secret:
            return []
        return retry(lambda: self._auth_get("exchange/v1/derivatives/futures/wallets"))

    def chk_live_bal(self) -> tuple[float, float]:
        """
        Check and return available cash (USDT) and total net capital.
        Combines spot USDT balance and futures USDT wallet capital.

        Returns
        -------
        tuple[float, float]
            (avail_cash, net_capital)
        """
        if not self.api_key or not self.api_secret:
            return 0.0, 0.0
        avail_cash = 0.0
        net_capital = 0.0
        try:
            # 1. Check Futures Wallet
            try:
                futures_wallets = retry(lambda: self.get_futures_wallets())
                if isinstance(futures_wallets, list):
                    for w in futures_wallets:
                        if w.get("currency_short_name", "").upper() == "USDT":
                            balance = float(w.get("balance", 0.0) or 0.0)
                            locked = float(w.get("locked_balance", 0.0) or 0.0)
                            cross_margin = float(w.get("cross_margin", 0.0) or 0.0)
                            avail_cash += max(0.0, balance - locked - cross_margin)
                            net_capital += balance
            except Exception as e:
                logger.debug("[%s] Futures wallet check: %s", self.account_id, e)

            # 2. Check Spot Balances
            try:
                spot_balances = retry(lambda: self.get_spot_balances())
                if isinstance(spot_balances, list):
                    for b in spot_balances:
                        if b.get("currency", "").upper() == "USDT":
                            balance = float(b.get("balance", 0.0) or 0.0)
                            locked = float(b.get("locked_balance", 0.0) or 0.0)
                            avail_cash += max(0.0, balance - locked)
                            net_capital += balance
            except Exception as e:
                logger.debug("[%s] Spot balance check: %s", self.account_id, e)

            return round(avail_cash, 4), round(net_capital, 4)
        except Exception as e:
            logger.error("[%s] Failed to fetch balances: %s", self.account_id, e)
            return 0.0, 0.0

    def holdings(self) -> pl.DataFrame | None:
        """
        Fetch and return non-zero spot holdings as a Polars DataFrame.
        """
        if not self.api_key or not self.api_secret:
            return None
        try:
            raw = retry(lambda: self.get_spot_balances())
            if isinstance(raw, list) and raw:
                rows = []
                for item in raw:
                    if not isinstance(item, dict):
                        continue
                    b = float(item.get("balance") or 0.0)
                    lb = float(item.get("locked_balance") or 0.0)
                    tot = b + lb
                    if tot > 0.0:
                        curr = str(item.get("currency") or "").strip()
                        rows.append({
                            "currency": curr,
                            "balance": b,
                            "locked_balance": lb,
                            "total": tot,
                            "tradingsymbol": curr,
                        })
                return pl.DataFrame(rows) if rows else pl.DataFrame()
        except Exception as e:
            logger.error("[%s] Failed to fetch holdings: %s", self.account_id, e)
        return None

    def pos_data(self, pair: str | None = None) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
        """
        Fetch open futures positions and return (day_positions_df, net_positions_df).

        Parameters
        ----------
        pair : str, optional
            Filter positions by instrument pair (e.g. 'B-BTC_USDT').
        """
        if not self.api_key or not self.api_secret:
            return None, None
        pos_day_frame = None
        pos_net_frame = None
        try:
            payload: dict[str, Any] = {
                "page": "1",
                "size": "50",
            }
            if pair:
                payload["pairs"] = pair

            positions = retry(
                lambda: self._post("exchange/v1/derivatives/futures/positions", payload)
            )

            if isinstance(positions, list) and positions:
                rows = []
                for item in positions:
                    if not isinstance(item, dict):
                        continue
                    p_sym = str(item.get("pair") or "").strip()
                    rows.append({
                        "pair": p_sym,
                        "tradingsymbol": p_sym,
                        "active_pos": float(item.get("active_pos") or 0.0),
                        "avg_price": float(item.get("avg_price") or 0.0),
                        "liquidation_price": float(item.get("liquidation_price") or 0.0),
                        "locked_margin": float(item.get("locked_margin") or 0.0),
                        "locked_user_margin": float(item.get("locked_user_margin") or 0.0),
                        "unrealized_pnl": float(item.get("unrealized_pnl") or 0.0),
                        "realized_pnl": float(item.get("realized_pnl") or 0.0),
                    })
                df = pl.DataFrame(rows) if rows else pl.DataFrame()
                pos_net_frame = df
                pos_day_frame = df
        except Exception as e:
            logger.error("[%s] Failed to fetch positions: %s", self.account_id, e)

        return pos_day_frame, pos_net_frame

    # ──────────────────────────────────────────────────────────────
    # Order Books, History & Trades
    # ──────────────────────────────────────────────────────────────

    def orders(self, is_futures: bool = True, pair: str | None = None) -> list[dict]:
        """
        Return active / open orders for today.

        Parameters
        ----------
        is_futures : bool, default True
            If True, retrieves futures active orders; if False, spot active orders.
        pair : str, optional
            Trading pair / market filter.
        """
        if not self.api_key or not self.api_secret:
            return []
        try:
            if is_futures:
                payload: dict[str, Any] = {"page": "1", "size": "50", "status": "open"}
                if pair:
                    payload["pairs"] = pair
                try:
                    res = retry(
                        lambda: self._post("exchange/v1/derivatives/futures/orders", payload)
                    )
                    return res if isinstance(res, list) else res.get("orders", [])
                except Exception:
                    res = retry(lambda: self._post("exchange/v1/orders/active_orders", payload))
                    return res if isinstance(res, list) else res.get("orders", [])
            else:
                payload = {}
                if pair:
                    payload["market"] = pair
                res = retry(lambda: self._post("exchange/v1/orders/active_orders", payload))
                return res if isinstance(res, list) else res.get("orders", [])
        except Exception as e:
            logger.error("[%s] Failed to fetch active orders: %s", self.account_id, e)
            return []

    def order_history(self, ord_id: str, is_futures: bool = True) -> list[dict] | dict:
        """
        Get status / history of a specific order ID.
        """
        if not self.api_key or not self.api_secret:
            return {}
        try:
            if is_futures:
                payload = {"id": ord_id}
                return retry(
                    lambda: self._post("exchange/v1/derivatives/futures/orders/status", payload)
                )
            else:
                payload = {"id": ord_id}
                return retry(lambda: self._post("exchange/v1/orders/status", payload))
        except Exception as e:
            logger.error("[%s] Failed to fetch order history for %s: %s", self.account_id, ord_id, e)
            return {}

    def order_trades(self, ord_id: str | None = None, is_futures: bool = True) -> list[dict]:
        """
        Return recent trade executions.
        """
        if not self.api_key or not self.api_secret:
            return []
        try:
            if is_futures:
                payload = {"page": "1", "size": "50"}
                res = retry(
                    lambda: self._post(
                        "exchange/v1/derivatives/futures/orders/trade_history", payload
                    )
                )
                trades = res if isinstance(res, list) else res.get("trades", [])
                if ord_id:
                    trades = [t for t in trades if str(t.get("order_id")) == str(ord_id)]
                return trades
            else:
                payload = {"limit": 50}
                res = retry(lambda: self._post("exchange/v1/orders/trade_history", payload))
                trades = res if isinstance(res, list) else res.get("trades", [])
                if ord_id:
                    trades = [t for t in trades if str(t.get("order_id")) == str(ord_id)]
                return trades
        except Exception as e:
            logger.error("[%s] Failed to fetch trade history: %s", self.account_id, e)
            return []

    # ──────────────────────────────────────────────────────────────
    # Order Placement Methods
    # ──────────────────────────────────────────────────────────────

    def _is_futures_symbol(self, symbol: str, is_futures: bool | None) -> bool:
        """Helper to determine if an operation should target Futures or Spot."""
        if is_futures is not None:
            return is_futures
        return symbol.startswith("B-") or symbol.startswith("I-")

    def mrk_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str,
        leverage: int = 1,
        is_futures: bool | None = None,
        client_order_id: str | None = None,
    ) -> tuple[str | int, str]:
        """
        Place a Market order.
        """
        if not self.api_key or not self.api_secret:
            return -1, "CoinDCX session unauthenticated: missing API key/secret"
        try:
            side = buy_sell.lower()
            futures = self._is_futures_symbol(symbol, is_futures)

            if futures:
                payload: dict[str, Any] = {
                    "order": {
                        "side": side,
                        "pair": symbol,
                        "order_type": "market_order",
                        "total_quantity": float(quantity),
                        "leverage": int(leverage),
                    }
                }
                if client_order_id:
                    payload["order"]["client_order_id"] = str(client_order_id)

                res = retry(
                    lambda: self._post("exchange/v1/derivatives/futures/orders/create", payload)
                )
            else:
                payload = {
                    "side": side,
                    "order_type": "market_order",
                    "market": symbol,
                    "total_quantity": float(quantity),
                }
                if client_order_id:
                    payload["client_order_id"] = str(client_order_id)

                res = retry(lambda: self._post("exchange/v1/orders/create", payload))

            order_id = res.get("id") or (
                res.get("orders", [{}])[0].get("id") if isinstance(res.get("orders"), list) else None
            )
            order_id_str = str(order_id) if order_id else "ok"
            logger.info(
                "[%s] Market order placed successfully for %s (%s). Order ID: %s",
                self.account_id,
                symbol,
                buy_sell.upper(),
                order_id_str,
            )
            return order_id_str, "order placed"
        except Exception as e:
            logger.error("[%s] Market order failed for %s: %s", self.account_id, symbol, e)
            return -1, f"Order placement failed: {e}"

    def lim_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str,
        price: float,
        leverage: int = 1,
        is_futures: bool | None = None,
        client_order_id: str | None = None,
        time_in_force: str = "good_till_cancel",
    ) -> tuple[str | int, str]:
        """
        Place a Limit order at a specific price.
        """
        if not self.api_key or not self.api_secret:
            return -1, "CoinDCX session unauthenticated: missing API key/secret"
        try:
            side = buy_sell.lower()
            futures = self._is_futures_symbol(symbol, is_futures)

            if futures:
                payload: dict[str, Any] = {
                    "order": {
                        "side": side,
                        "pair": symbol,
                        "order_type": "limit_order",
                        "total_quantity": float(quantity),
                        "price": str(price),
                        "leverage": int(leverage),
                        "time_in_force": time_in_force,
                    }
                }
                if client_order_id:
                    payload["order"]["client_order_id"] = str(client_order_id)

                res = retry(
                    lambda: self._post("exchange/v1/derivatives/futures/orders/create", payload)
                )
            else:
                payload = {
                    "side": side,
                    "order_type": "limit_order",
                    "market": symbol,
                    "price_per_unit": float(price),
                    "total_quantity": float(quantity),
                }
                if client_order_id:
                    payload["client_order_id"] = str(client_order_id)

                res = retry(lambda: self._post("exchange/v1/orders/create", payload))

            order_id = res.get("id") or (
                res.get("orders", [{}])[0].get("id") if isinstance(res.get("orders"), list) else None
            )
            order_id_str = str(order_id) if order_id else "ok"
            logger.info(
                "[%s] Limit order placed successfully for %s @ %s. Order ID: %s",
                self.account_id,
                symbol,
                price,
                order_id_str,
            )
            return order_id_str, "order placed"
        except Exception as e:
            logger.error("[%s] Limit order failed for %s: %s", self.account_id, symbol, e)
            return -1, f"Order placement failed: {e}"

    def sl_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str,
        price: float,
        trig_price: float,
        leverage: int = 1,
        is_futures: bool | None = None,
        client_order_id: str | None = None,
    ) -> tuple[str | int, str]:
        """
        Place a Stop-Loss Limit (stop_limit) order.
        buy_sell : str
            'BUY' or 'SELL'.
        price : float
            Execution limit price once triggered.
        trig_price : float
            Stop trigger price.
        leverage : int, default 1
            Leverage multiplier for Futures orders.
        """
        try:
            side = buy_sell.lower()
            futures = self._is_futures_symbol(symbol, is_futures)

            if futures:
                payload: dict[str, Any] = {
                    "order": {
                        "side": side,
                        "pair": symbol,
                        "order_type": "stop_limit",
                        "total_quantity": float(quantity),
                        "price": str(price),
                        "stop_price": str(trig_price),
                        "leverage": int(leverage),
                    }
                }
                if client_order_id:
                    payload["order"]["client_order_id"] = str(client_order_id)

                res = retry(
                    lambda: self._post("exchange/v1/derivatives/futures/orders/create", payload)
                )
            else:
                payload = {
                    "side": side,
                    "order_type": "stop_limit",
                    "market": symbol,
                    "price_per_unit": float(price),
                    "stop_price": float(trig_price),
                    "total_quantity": float(quantity),
                }
                if client_order_id:
                    payload["client_order_id"] = str(client_order_id)

                res = retry(lambda: self._post("exchange/v1/orders/create", payload))

            order_id = res.get("id") or (
                res.get("orders", [{}])[0].get("id") if isinstance(res.get("orders"), list) else None
            )
            order_id_str = str(order_id) if order_id else "ok"
            logger.info(
                "[%s] Stop-Loss limit order placed for %s (Trigger: %s, Price: %s). Order ID: %s",
                self.account_id,
                symbol,
                trig_price,
                price,
                order_id_str,
            )
            return order_id_str, "order placed"
        except Exception as e:
            logger.error("[%s] SL order failed for %s: %s", self.account_id, symbol, e)
            return -1, f"Order placement failed: {e}"

    def slmkt_ordr(
        self,
        symbol: str,
        quantity: float,
        buy_sell: str,
        trig_price: float,
        leverage: int = 1,
        is_futures: bool | None = None,
        client_order_id: str | None = None,
    ) -> tuple[str | int, str]:
        """
        Place a Stop-Loss Market (take_profit or stop_market) trigger order.
        """
        if not self.api_key or not self.api_secret:
            return -1, "CoinDCX session unauthenticated: missing API key/secret"
        try:
            side = buy_sell.lower()
            futures = self._is_futures_symbol(symbol, is_futures)

            if futures:
                payload: dict[str, Any] = {
                    "order": {
                        "side": side,
                        "pair": symbol,
                        "order_type": "take_profit",
                        "total_quantity": float(quantity),
                        "stop_price": str(trig_price),
                        "leverage": int(leverage),
                    }
                }
                if client_order_id:
                    payload["order"]["client_order_id"] = str(client_order_id)

                res = retry(
                    lambda: self._post("exchange/v1/derivatives/futures/orders/create", payload)
                )
            else:
                payload = {
                    "side": side,
                    "order_type": "take_profit",
                    "market": symbol,
                    "stop_price": float(trig_price),
                    "total_quantity": float(quantity),
                }
                if client_order_id:
                    payload["client_order_id"] = str(client_order_id)

                res = retry(lambda: self._post("exchange/v1/orders/create", payload))

            order_id = res.get("id") or (
                res.get("orders", [{}])[0].get("id") if isinstance(res.get("orders"), list) else None
            )
            order_id_str = str(order_id) if order_id else "ok"
            logger.info(
                "[%s] SL-Market order placed for %s (Trigger: %s). Order ID: %s",
                self.account_id,
                symbol,
                trig_price,
                order_id_str,
            )
            return order_id_str, "order placed"
        except Exception as e:
            logger.error("[%s] SL-M order failed for %s: %s", self.account_id, symbol, e)
            return -1, f"Order placement failed: {e}"

    def cancel_ordr(self, order_id: str, is_futures: bool = True) -> Any:
        """
        Cancel an open order by ID.
        """
        if not self.api_key or not self.api_secret:
            return {"error": "CoinDCX session unauthenticated: missing API key/secret"}
        payload = {"id": order_id}
        if is_futures:
            return retry(
                lambda: self._post("exchange/v1/derivatives/futures/orders/cancel", payload)
            )
        else:
            return retry(lambda: self._post("exchange/v1/orders/cancel", payload))

    def cancel_all(self, symbol: str | None = None, is_futures: bool = True) -> Any:
        """
        Cancel all open orders for a pair or market.
        """
        if is_futures:
            payload: dict[str, Any] = {}
            if symbol:
                payload["pair"] = symbol
            return retry(
                lambda: self._post("exchange/v1/derivatives/futures/orders/cancel_all", payload)
            )
        else:
            payload = {}
            if symbol:
                payload["market"] = symbol
            return retry(lambda: self._post("exchange/v1/orders/cancel_all", payload))

    def exit_ordr(self, ord_id: str, is_futures: bool = True) -> Any:
        """
        Alias for cancelling/exiting an open order.
        """
        return self.cancel_ordr(order_id=ord_id, is_futures=is_futures)

    def exit_position(self, pair: str, is_futures: bool = True) -> tuple[str | int, str]:
        """
        Flatten/close an active open position by submitting an opposing market order.
        """
        try:
            _, net_df = self.pos_data(pair=pair)
            if net_df is None or (hasattr(net_df, "is_empty") and net_df.is_empty()):
                return -1, f"No open position found for {pair}"

            if hasattr(net_df, "filter"):
                row = net_df.filter(pl.col("pair") == pair) if "pair" in net_df.columns else pl.DataFrame()
            else:
                row = net_df

            if row is None or (hasattr(row, "is_empty") and row.is_empty()):
                return -1, f"No open position found for {pair}"

            active_pos = float(row["active_pos"][0]) if "active_pos" in row.columns else 0.0
            if active_pos == 0.0:
                return -1, f"Position for {pair} is already flat (0 quantity)"

            opposite_side = "SELL" if active_pos > 0 else "BUY"
            exit_qty = abs(active_pos)
            logger.info(
                "[%s] Exiting position on %s: %s %s units",
                self.account_id,
                pair,
                opposite_side,
                exit_qty,
            )
            return self.mrk_ordr(symbol=pair, quantity=exit_qty, buy_sell=opposite_side, is_futures=is_futures)
        except Exception as e:
            logger.error("[%s] Failed to exit position on %s: %s", self.account_id, pair, e)
            return -1, f"Exit position failed: {e}"

    # ──────────────────────────────────────────────────────────────
    # Market Data & Instruments
    # ──────────────────────────────────────────────────────────────

    def instruments_list(self, is_futures: bool = True) -> list[dict]:
        """
        Fetch active instrument specifications and market details.
        """
        try:
            if is_futures:
                res = retry(
                    lambda: self._get("exchange/v1/derivatives/futures/data/active_instruments")
                )
                return res if isinstance(res, list) else []
            else:
                res = retry(lambda: self._get("exchange/v1/markets_details"))
                return res if isinstance(res, list) else []
        except Exception as e:
            logger.error("[%s] Failed to fetch instruments: %s", self.account_id, e)
            return []

    def ticker(self, market: str | None = None) -> list[dict] | dict:
        """
        Fetch current market ticker data (LTP, 24h high/low, volume).
        """
        try:
            data = retry(lambda: self._get("exchange/ticker"))
            if market and isinstance(data, list):
                m_upper = market.upper().replace("-", "").replace("_", "")
                for item in data:
                    item_m = item.get("market", "").upper().replace("-", "").replace("_", "")
                    if item_m == m_upper:
                        return item
            return data
        except Exception as e:
            logger.error("[%s] Failed to fetch ticker: %s", self.account_id, e)
            return []

    def order_book(self, pair: str) -> dict:
        """
        Fetch live Level-2 order book depth for a specific pair from public market data API.
        """
        try:
            return retry(
                lambda: self._get(
                    "market_data/orderbook",
                    params={"pair": pair},
                    base_url=self.PUBLIC_BASE_URL,
                )
            )
        except Exception as e:
            logger.error("[%s] Failed to fetch orderbook for %s: %s", self.account_id, pair, e)
            return {}

    def candles(
        self,
        pair: str,
        interval: str = "1m",
        limit: int = 500,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> pl.DataFrame | None:
        """
        Fetch historical candlestick (OHLCV) data as a Polars DataFrame.

        Parameters
        ----------
        pair : str
            Trading pair (e.g. 'B-BTC_USDT').
        interval : str, default '1m'
            Candle duration: '1m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '1d', '1w'.
        limit : int, default 500
            Number of candles (max 1000).
        start_time : int, optional
            Start timestamp in milliseconds.
        end_time : int, optional
            End timestamp in milliseconds.

        Returns
        -------
        pl.DataFrame | None
            DataFrame with columns ['datetime', 'time', 'open', 'high', 'low', 'close', 'volume'].
        """
        try:
            params: dict[str, Any] = {
                "pair": pair,
                "interval": interval,
                "limit": min(limit, 1000),
            }
            if start_time:
                params["startTime"] = start_time
            if end_time:
                params["endTime"] = end_time

            raw = retry(
                lambda: self._get(
                    "market_data/candles",
                    params=params,
                    base_url=self.PUBLIC_BASE_URL,
                )
            )

            if isinstance(raw, list) and raw:
                rows = []
                for item in raw:
                    if not isinstance(item, dict):
                        continue
                    ts = item.get("time")
                    dt_val = None
                    if isinstance(ts, (int, float)):
                        try:
                            from datetime import datetime, timezone
                            dt_val = datetime.fromtimestamp(ts / 1000.0 if ts > 1e11 else ts, tz=timezone.utc).replace(tzinfo=None)
                        except Exception:
                            dt_val = None
                    rows.append({
                        "datetime": dt_val,
                        "time": ts,
                        "open": float(item.get("open") or 0.0),
                        "high": float(item.get("high") or 0.0),
                        "low": float(item.get("low") or 0.0),
                        "close": float(item.get("close") or 0.0),
                        "volume": float(item.get("volume") or 0.0),
                    })
                return pl.DataFrame(rows) if rows else pl.DataFrame()
        except Exception as e:
            logger.error("[%s] Failed to fetch candles for %s: %s", self.account_id, pair, e)
        return None

    def get_margin(self, orders_data: pl.DataFrame | list[dict] | Any) -> list[dict]:
        """
        Calculate approximate margin required for a batch of order parameters.

        Parameters
        ----------
        orders_data : pl.DataFrame | list[dict]
            List or DataFrame containing 'quantity', 'price', and optional 'leverage'.

        Returns
        -------
        list[dict]
            Margin estimation records with required margin amounts.
        """
        if hasattr(orders_data, "to_dicts"):
            records = orders_data.to_dicts()
        elif hasattr(orders_data, "to_dict"):
            records = orders_data.to_dict(orient="records") if hasattr(orders_data, "to_dict") else orders_data
        elif isinstance(orders_data, list):
            records = orders_data
        else:
            records = []

        results = []
        for item in records:
            qty = float(item.get("quantity", 0.0) or item.get("total_quantity", 0.0) or 0.0)
            price = float(item.get("price", 0.0) or item.get("price_per_unit", 0.0) or 0.0)
            leverage = max(1, int(item.get("leverage", 1) or 1))
            notional = qty * price
            initial_margin = notional / leverage if leverage > 0 else notional
            results.append({
                "symbol": item.get("symbol") or item.get("pair") or item.get("market"),
                "quantity": qty,
                "price": price,
                "leverage": leverage,
                "notional_value": round(notional, 4),
                "initial_margin": round(initial_margin, 4),
            })
        return results


if __name__ == "__main__":
    import os
    import django

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
    django.setup()

    try:
        util = CoinDCXUtility()
        print(f"Successfully connected to CoinDCX account: {util.account_id}")
        cash, cap = util.chk_live_bal()
        print(f"Available Cash (USDT): ${cash:,.2f} | Net Capital: ${cap:,.2f}")
    except Exception as exc:
        print(f"Error testing CoinDCXUtility: {exc}")
