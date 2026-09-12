# -*- coding: utf-8 -*-
"""
algo_trading/algos/kotak_utils.py
───────────────────────────────────
Wrapper around Kotak Neo Trade API v2 customized for order management,
portfolio inspection, margin calculations, and multi-account trading in DeltaZero26.
Compatible with Python 3.14+ without requiring external SDK installations.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import numpy as np
import polars as pl
import requests

logger = logging.getLogger("algo_trading.algos.kotak_utils")


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
            if "401" in err_msg or "403" in err_msg or "unauthorized" in err_msg or "token" in err_msg:
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


class KotakNeoUtility:
    """
    Order execution, account positions, margins, and holdings manager for Kotak Neo.
    Supports multi-account resolution via account_id or direct Broker instances.
    """

    BASE_URL = "https://mis.kotaksecurities.com"
    ORDER_SOURCE = "NEOTRADEAPI"

    # Segment and Product Mapping
    EXCHANGE_MAP = {
        "NSE": "nse_cm",
        "NSE_CM": "nse_cm",
        "EQUITY": "nse_cm",
        "NFO": "nse_fo",
        "NSE_FO": "nse_fo",
        "BSE": "bse_cm",
        "BSE_CM": "bse_cm",
        "BFO": "bse_fo",
        "BSE_FO": "bse_fo",
        "CDS": "cde_fo",
        "CDE_FO": "cde_fo",
        "MCX": "mcx_fo",
        "MCX_FO": "mcx_fo",
    }

    PRODUCT_MAP = {
        "MIS": "MIS",
        "INTRADAY": "INTRADAY",
        "CNC": "CNC",
        "NRML": "NRML",
        "NORMAL": "NRML",
        "CO": "CO",
        "BO": "BO",
    }

    def __init__(
        self,
        account_id: str | None = None,
        broker_obj: Any = None,
        api_key: str | None = None,
        access_token: str | None = None,
        client: Any = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize KotakNeoUtility by fetching credentials from the Broker database model
        or direct parameters.

        Parameters
        ----------
        account_id : str, optional
            Account ID or name (e.g. 'W1NPY' or 'Prvn_kotak'). If None, loads the first active Kotak Neo account.
        broker_obj : Broker, optional
            Direct Broker model instance.
        api_key : str, optional
            Consumer Key (API key) if passing directly.
        access_token : str, optional
            Session token (token or token:::sid) if passing directly.
        client : Any, optional
            Pre-configured client instance for testing/mocking.
        """
        self.client = client
        if broker_obj is None and "broker" in kwargs:
            broker_obj = kwargs["broker"]
        self.session = requests.Session()
        self._scrip_master_cache: dict[str, list[dict]] = {}

        if api_key is not None or access_token is not None:
            self.broker = broker_obj
            self.account_id = account_id or "direct_kotak"
            self.api_key = (api_key or "").strip()
            self.raw_access_token = (access_token or "").strip()
        else:
            from kalai.models import Broker
            from django.db.models import Q

            if broker_obj is not None:
                self.broker = broker_obj
            elif account_id:
                self.broker = Broker.objects.filter(
                    Q(account_id__iexact=account_id) | Q(name__iexact=account_id)
                ).first()
            else:
                # Default to first active Kotak Neo account
                self.broker = (
                    Broker.objects.filter(
                        Q(broker_name__code__iexact="kotak_neo")
                        | Q(api_provider__code__iexact="kotak_neo")
                        | Q(name__icontains="kotak")
                    )
                    .order_by("-enable_websocket")
                    .first()
                )

            if not self.broker:
                if account_id:
                    raise ValueError(f"Kotak Neo broker configuration for '{account_id}' not found in database.")
                logger.warning(
                    "[%s] Kotak Neo broker configuration not found in database. Initializing in public/read-only mode.",
                    datetime.now().isoformat(),
                )
                self.broker = None
                self.account_id = "kotak_public"
                self.api_key = ""
                self.raw_access_token = ""
            else:
                self.account_id = self.broker.account_id or self.broker.name
                self.api_key = (self.broker.api_key or "").strip()
                self.raw_access_token = (self.broker.access_token or "").strip()

        if not self.raw_access_token:
            logger.warning(
                "[%s] Access token is missing for Kotak Neo account '%s'. Live orders/positions will be disabled until authenticated via Broker Login.",
                datetime.now().isoformat(),
                self.account_id,
            )
            self.token = ""
            self.sid = self.account_id
        elif ":::" in self.raw_access_token:
            parts = self.raw_access_token.split(":::")
            self.token = parts[0].strip()
            self.sid = parts[1].strip() if len(parts) > 1 else self.account_id
        else:
            self.token = self.raw_access_token
            self.sid = self.account_id

        self._headers = {
            "Authorization": self.api_key,
            "neo-fin-key": "neotradeapi",
            "Auth": self.token,
            "Sid": self.sid,
        }

        logger.info(
            "[%s] Initialized KotakNeoUtility for account '%s' (sid='%s')",
            datetime.now().isoformat(),
            self.account_id,
            self.sid,
        )

    def get_access_token(self) -> str:
        """Return the active access token for Kotak Neo."""
        return self.raw_access_token

    def _map_exchange(self, exchg: str) -> str:
        """Normalize exchange code to Kotak Neo exchange_segment."""
        return self.EXCHANGE_MAP.get(exchg.upper().strip(), "nse_cm")

    def _map_product(self, prod: str) -> str:
        """Normalize product type to Kotak Neo product code."""
        return self.PRODUCT_MAP.get(prod.upper().strip(), "MIS")

    # ──────────────────────────────────────────────────────────────
    # Portfolio, Positions, and Margins
    # ──────────────────────────────────────────────────────────────

    def holdings(self) -> pl.DataFrame | None:
        """Fetch and return account holdings as a Polars DataFrame."""
        if not self.token and not self.client:
            return None
        url = f"{self.BASE_URL}/portfolio/v1/holdings"
        try:
            resp = retry(lambda: requests.get(url, headers=self._headers, timeout=10))
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("data") or data.get("holdings") or data
                if isinstance(items, list) and len(items) > 0:
                    df = pl.DataFrame(items)
                    if not df.is_empty() and "tradingsymbol" in df.columns:
                        df = df.with_columns(
                            pl.col("tradingsymbol").cast(pl.Utf8).str.replace_all(r"\*", "")
                        )
                    return df
            elif resp.status_code in (404, 424):
                logger.debug("No holdings found for %s", self.account_id)
                return pl.DataFrame()
            else:
                logger.warning("Kotak holdings API returned %s: %s", resp.status_code, resp.text[:200])
        except Exception as e:
            logger.error("Failed to fetch holdings for %s: %s", self.account_id, e)
        return None

    def pos_data(self) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
        """Fetch positions and return (day_positions_df, net_positions_df)."""
        if not self.token and not self.client:
            return None, None
        url = f"{self.BASE_URL}/quick/user/positions"
        pos_day_frame = None
        pos_net_frame = None
        try:
            resp = retry(lambda: requests.get(url, headers=self._headers, timeout=10))
            if resp.status_code == 200:
                data = resp.json()
                positions = data.get("data") or data.get("positions") or []
                if isinstance(positions, list) and len(positions) > 0:
                    df = pl.DataFrame(positions)
                    if not df.is_empty():
                        # Rename Kotak Neo position fields to unified names
                        rename_map = {
                            "ts": "tradingsymbol",
                            "trdSym": "tradingsymbol",
                            "sym": "tradingsymbol",
                            "flBuyQty": "buy_quantity",
                            "flSellQty": "sell_quantity",
                            "netQty": "quantity",
                            "buyAmt": "buy_value",
                            "sellAmt": "sell_value",
                            "cfBuyQty": "overnight_buy_quantity",
                            "cfSellQty": "overnight_sell_quantity",
                            "flBuyAmt": "total_buy_value",
                            "flSellAmt": "total_sell_value",
                            "urPnl": "unrealised",
                            "rPnl": "realised",
                            "pnl": "pnl",
                        }
                        existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
                        if existing_renames:
                            df = df.rename(existing_renames)
                        if "tradingsymbol" in df.columns:
                            df = df.with_columns(
                                pl.col("tradingsymbol").cast(pl.Utf8).str.replace_all(r"\*", "")
                            )
                        pos_net_frame = df.clone()
                        pos_day_frame = df.clone()
            elif resp.status_code in (404, 5203):
                logger.debug("No positions found for %s", self.account_id)
        except Exception as e:
            logger.error("Failed to fetch positions for %s: %s", self.account_id, e)

        return pos_day_frame, pos_net_frame

    def chk_live_bal(self) -> tuple[float, float]:
        """
        Check and return available cash and total net capital (avail_cash, net_capital).
        """
        if not self.token and not self.client:
            return 0.0, 0.0
        url = f"{self.BASE_URL}/quick/user/limits"
        body = {"seg": "ALL", "exch": "ALL", "prod": "ALL"}
        try:
            resp = retry(
                lambda: requests.post(
                    url,
                    headers=self._headers,
                    data={"jData": json.dumps(body)},
                    timeout=10,
                )
            )
            if resp.status_code == 200:
                data = resp.json()
                avail_cash = float(
                    data.get("AvailableLimitMargin")
                    or data.get("NetCashAvailable")
                    or data.get("CollateralValue")
                    or data.get("BoardLotLimit")
                    or 0.0
                )
                net_cap = float(
                    data.get("Net")
                    or data.get("TotalMargin")
                    or data.get("CollateralValue")
                    or avail_cash
                )
                return avail_cash, net_cap
            else:
                logger.warning("Kotak limits API returned %s: %s", resp.status_code, resp.text[:200])
        except Exception as e:
            logger.error("Failed to fetch balances for %s: %s", self.account_id, e)

        return 0.0, 0.0

    # ──────────────────────────────────────────────────────────────
    # Order Book, History, and Trades
    # ──────────────────────────────────────────────────────────────

    def orders(self) -> list[dict]:
        """Return all orders for today."""
        if not self.token and not self.client:
            return []
        url = f"{self.BASE_URL}/quick/user/orders"
        try:
            resp = retry(lambda: requests.get(url, headers=self._headers, timeout=10))
            if resp.status_code == 200:
                data = resp.json()
                return data.get("data") or data.get("orders") or []
        except Exception as e:
            logger.error("Failed to fetch orders for %s: %s", self.account_id, e)
        return []

    def order_history(self, ord_id: str) -> list[dict]:
        """Get history of a specific order ID."""
        if not self.token and not self.client:
            return []
        url = f"{self.BASE_URL}/quick/order/history"
        try:
            resp = retry(
                lambda: requests.get(
                    url,
                    headers=self._headers,
                    params={"nOrdNo": str(ord_id).strip()},
                    timeout=10,
                )
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("data") or data.get("orderHistory") or []
        except Exception as e:
            logger.error("Failed to fetch order history for order %s: %s", ord_id, e)
        return []

    def order_trades(self, ord_id: str) -> list[dict]:
        """Return trades generated by a specific order ID."""
        if not self.token and not self.client:
            return []
        url = f"{self.BASE_URL}/quick/user/trades"
        try:
            resp = retry(lambda: requests.get(url, headers=self._headers, timeout=10))
            if resp.status_code == 200:
                data = resp.json()
                all_trades = data.get("data") or data.get("trades") or []
                return [t for t in all_trades if str(t.get("nOrdNo")) == str(ord_id).strip()]
        except Exception as e:
            logger.error("Failed to fetch trades for order %s: %s", ord_id, e)
        return []

    # ──────────────────────────────────────────────────────────────
    # Order Placement Operations
    # ──────────────────────────────────────────────────────────────

    def mrk_ordr(
        self,
        symbol: str,
        quantity: int,
        buy_sell: str,
        exchg: str = "NSE",
        prod: str = "MIS",
        ttl_value: int | None = None,
    ) -> tuple[str | int, str]:
        """Place a regular market order."""
        return self._place_raw_order(
            symbol=symbol,
            quantity=quantity,
            buy_sell=buy_sell,
            exchg=exchg,
            prod=prod,
            price=0.0,
            order_type="MKT",
            validity="TTL" if ttl_value else "DAY",
        )

    def lim_ordr(
        self,
        symbol: str,
        quantity: int,
        buy_sell: str,
        exchg: str = "NSE",
        prod: str = "MIS",
        price: float = 0.0,
        ttl_value: int | None = None,
        validity: str = "DAY",
    ) -> tuple[str | int, str]:
        """Place a limit order."""
        return self._place_raw_order(
            symbol=symbol,
            quantity=quantity,
            buy_sell=buy_sell,
            exchg=exchg,
            prod=prod,
            price=price,
            order_type="L",
            validity="TTL" if ttl_value else validity,
        )

    def slmkt_ordr(
        self,
        symbol: str,
        quantity: int,
        buy_sell: str,
        exchg: str = "NSE",
        prod: str = "MIS",
        trig_price: float = 0.0,
    ) -> tuple[str | int, str]:
        """Place a Stop-Loss Market (SL-M) order."""
        return self._place_raw_order(
            symbol=symbol,
            quantity=quantity,
            buy_sell=buy_sell,
            exchg=exchg,
            prod=prod,
            price=0.0,
            trigger_price=trig_price,
            order_type="SL-M",
            validity="DAY",
        )

    def sl_ordr(
        self,
        symbol: str,
        quantity: int,
        buy_sell: str,
        exchg: str = "NSE",
        prod: str = "MIS",
        price: float = 0.0,
        trig_price: float = 0.0,
    ) -> tuple[str | int, str]:
        """Place a Stop-Loss Limit (SL) order."""
        return self._place_raw_order(
            symbol=symbol,
            quantity=quantity,
            buy_sell=buy_sell,
            exchg=exchg,
            prod=prod,
            price=price,
            trigger_price=trig_price,
            order_type="SL",
            validity="DAY",
        )

    def ice_ordr(
        self,
        symbol: str,
        quantity: int,
        buy_sell: str,
        exchg: str = "NSE",
        prod: str = "MIS",
        price: float = 0.0,
        legs: int = 2,
        ice_qty: int = 1,
        ttl_value: int | None = None,
        order_type: str = "LIMIT",
        validity: str = "DAY",
    ) -> tuple[str | int, str]:
        """Place an iceberg / disclosed quantity order."""
        disclosed_qty = int(ice_qty) if ice_qty > 0 else (int(quantity) // max(1, legs))
        return self._place_raw_order(
            symbol=symbol,
            quantity=quantity,
            buy_sell=buy_sell,
            exchg=exchg,
            prod=prod,
            price=price if order_type.upper() in ("LIMIT", "L") else 0.0,
            order_type="L" if order_type.upper() in ("LIMIT", "L") else "MKT",
            validity="TTL" if ttl_value else validity,
            disclosed_quantity=disclosed_qty,
        )

    def _place_raw_order(
        self,
        symbol: str,
        quantity: int,
        buy_sell: str,
        exchg: str = "NSE",
        prod: str = "MIS",
        price: float = 0.0,
        trigger_price: float | None = None,
        order_type: str = "MKT",
        validity: str = "DAY",
        disclosed_quantity: int | None = None,
        tag: str | None = None,
    ) -> tuple[str | int, str]:
        """
        Low-level REST order placement dispatch for Kotak Neo.
        """
        if not self.token and not self.client:
            return -1, "Kotak Neo session unauthenticated: missing access token"
        url = f"{self.BASE_URL}/quick/order/rule/ms/place"
        trans_type = "B" if buy_sell.upper().startswith("B") else "S"
        seg = self._map_exchange(exchg)
        product = self._map_product(prod)

        body_params = {
            "am": "NO",
            "dq": str(disclosed_quantity or 0),
            "es": seg,
            "mp": "0",
            "pc": product,
            "pr": str(price) if price and price > 0 else "0",
            "pt": order_type.upper(),
            "qt": str(int(quantity)),
            "rt": validity.upper(),
            "tp": str(trigger_price) if trigger_price and trigger_price > 0 else "0",
            "ts": symbol.strip(),
            "tt": trans_type,
            "ig": tag or "DZ26",
            "os": self.ORDER_SOURCE,
        }

        try:
            resp = retry(
                lambda: requests.post(
                    url,
                    headers=self._headers,
                    data={"jData": json.dumps(body_params)},
                    timeout=10,
                )
            )
            resp_data = resp.json() if resp.status_code == 200 else {}
            if resp.status_code == 200 and resp_data.get("nOrdNo"):
                ord_id = str(resp_data.get("nOrdNo"))
                logger.info("[%s] Kotak Neo order placed successfully. Order ID: %s", self.account_id, ord_id)
                return ord_id, "order placed"
            else:
                err_msg = (
                    resp_data.get("errMsg")
                    or resp_data.get("desc")
                    or resp_data.get("message")
                    or resp.text[:200]
                )
                logger.error("[%s] Kotak Neo order failed for %s: %s", self.account_id, symbol, err_msg)
                return -1, f"Order placement failed: {err_msg}"
        except Exception as e:
            logger.error("[%s] Order placement exception for %s: %s", self.account_id, symbol, e)
            return -1, f"Order placement failed: {e}"

    def cancel_ordr(
        self,
        variety: str = "regular",
        order_id: str = "",
        parent_order_id: str | None = None,
    ) -> Any:
        """Cancel an open order on Kotak Neo."""
        if not self.token and not self.client:
            return {"error": "Kotak Neo session unauthenticated: missing access token"}
        url = f"{self.BASE_URL}/quick/order/cancel"
        body = {
            "on": str(order_id).strip(),
            "am": "NO",
        }
        try:
            resp = retry(
                lambda: requests.post(
                    url,
                    headers=self._headers,
                    data={"jData": json.dumps(body)},
                    timeout=10,
                )
            )
            return resp.json() if resp.status_code == 200 else {"status": resp.status_code, "text": resp.text}
        except Exception as e:
            logger.error("Failed to cancel Kotak order %s: %s", order_id, e)
            return {"error": str(e)}

    def exit_ordr(
        self,
        ord_id: str,
        parent_order_id: str | None = None,
    ) -> Any:
        """Exit an open order / cover / bracket order on Kotak Neo."""
        return self.cancel_ordr(order_id=ord_id, parent_order_id=parent_order_id)

    # ──────────────────────────────────────────────────────────────
    # Scrip Master & Instruments List
    # ──────────────────────────────────────────────────────────────

    def instruments_list(self, exchg: str = "NSE") -> list[dict]:
        """
        Fetch all instruments for an exchange from Kotak Neo's daily transformed master files.
        Supported exchanges: 'NSE', 'NFO', 'BSE', 'BFO', 'MCX', 'CDS'.
        """
        exchg_key = self._map_exchange(exchg)
        if exchg_key in self._scrip_master_cache:
            return self._scrip_master_cache[exchg_key]

        try:
            # 1. Fetch live daily file paths from masterscrip endpoint
            fp_url = f"{self.BASE_URL}/script-details/1.0/masterscrip/file-paths"
            resp = requests.get(fp_url, headers=self._headers, timeout=10)
            if resp.status_code == 200:
                files_paths = resp.json().get("data", {}).get("filesPaths", [])
                target_url = None
                for fp in files_paths:
                    if f"{exchg_key}.csv" in fp or (exchg_key == "nse_cm" and "nse_cm-v1.csv" in fp):
                        target_url = fp
                        break

                if target_url:
                    r_csv = requests.get(target_url, timeout=20)
                    if r_csv.status_code == 200:
                        df = pl.read_csv(io.BytesIO(r_csv.content), infer_schema_length=0, truncate_ragged_lines=True)
                        records = df.to_dicts()
                        logger.info("Loaded %d Kotak scrips for exchange '%s'", len(records), exchg)
                        return records
        except Exception as e:
            logger.error("Failed to load Kotak instruments for %s: %s", exchg, e)

        return []

    def get_margin(self, orders_data: pl.DataFrame | list[dict]) -> list[dict]:
        """Calculate margin requirements for orders."""
        url = f"{self.BASE_URL}/quick/user/check-margin"
        if isinstance(orders_data, pl.DataFrame):
            orders_list = orders_data.to_dicts()
        elif hasattr(orders_data, "to_dict"):
            orders_list = orders_data.to_dict("records")
        else:
            orders_list = orders_data

        results = []
        for ord_item in orders_list:
            try:
                resp = retry(
                    lambda: requests.post(
                        url,
                        headers=self._headers,
                        data={"jData": json.dumps(ord_item)},
                        timeout=10,
                    )
                )
                if resp.status_code == 200:
                    results.append(resp.json())
            except Exception as e:
                logger.error("Margin calculation error: %s", e)
        return results

    @staticmethod
    def sanitize_for_json(data: Any) -> Any:
        """Recursively convert DataFrames, datetimes, and numpy types for JSON serialization."""
        import numpy as np
        from datetime import date, datetime
        if isinstance(data, pl.DataFrame):
            return [KotakNeoUtility.sanitize_for_json(row) for row in data.to_dicts()]
        elif isinstance(data, dict):
            return {str(k): KotakNeoUtility.sanitize_for_json(v) for k, v in data.items()}
        elif isinstance(data, (list, tuple, set)):
            return [KotakNeoUtility.sanitize_for_json(item) for item in data]
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

    @staticmethod
    def _normalize_kotak_scrip(raw: Any) -> pl.DataFrame:
        """Normalizes Kotak Neo scrip records into the standard Indian exchange schema."""
        import re
        if not raw or not isinstance(raw, list):
            return pl.DataFrame()

        MONTH_REGEX = r"(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})"
        MONTH_MAP = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
        }

        clean = []
        for r in raw:
            if not isinstance(r, dict):
                continue
            if "instrument_token" in r and "tradingsymbol" in r and "instrument_type" in r and "pSymbol" not in r:
                row = dict(r)
                if "expiry" in row and row["expiry"] is not None:
                    row["expiry"] = str(row["expiry"])
                clean.append(row)
                continue

            tkn_str = r.get("pSymbol") or r.get("pToken") or r.get("token") or "0"
            try:
                tkn = int(float(tkn_str))
            except Exception:
                tkn = 0

            tsym = r.get("pTrdSymbol") or r.get("trading_symbol") or ""
            name = r.get("pSymbolName") or r.get("symbol_name") or tsym
            ref_key = r.get("pScripRefKey") or ""

            exp_str = ""
            for text in (ref_key, tsym):
                if not text:
                    continue
                m = re.search(MONTH_REGEX, text, re.IGNORECASE)
                if m:
                    d, mon, y = m.groups()
                    mon_u = mon.upper()
                    if mon_u in MONTH_MAP and 1 <= int(d) <= 31:
                        exp_str = f"20{y}-{MONTH_MAP[mon_u]:02d}-{int(d):02d}"
                        break

            strike_raw = r.get("dStrikePrice;") or r.get("dStrikePrice") or r.get("strike") or 0.0
            try:
                strike_val = float(strike_raw)
                strike = round(strike_val / 100.0, 2) if strike_val > 0 else 0.0
            except Exception:
                strike = 0.0

            lot_raw = r.get("lLotSize") or r.get("iLotSize") or r.get("lot_size") or 1
            try:
                lot_size = int(float(lot_raw))
            except Exception:
                lot_size = 1

            tick_raw = r.get("dTickSize ") or r.get("dTickSize") or r.get("tick_size") or 0.05
            try:
                tick_val = float(tick_raw)
                tick_size = round(tick_val / 100.0, 4) if tick_val >= 1.0 else tick_val
            except Exception:
                tick_size = 0.05

            opt_type = (r.get("pOptionType") or "").strip().upper()
            inst_type = (r.get("pInstType") or "").strip().upper()
            if opt_type in ("CE", "PE"):
                instrument_type = opt_type
            elif "FUT" in inst_type or opt_type == "XX" or "FUT" in tsym:
                instrument_type = "FUT"
            else:
                instrument_type = "EQ"

            seg_raw = (r.get("pExchSeg") or "").lower()
            if "fo" in seg_raw:
                exch = "NFO" if "nse" in seg_raw else ("MCX" if "mcx" in seg_raw else ("BFO" if "bse" in seg_raw else "CDS"))
                segment = f"{exch}-OPT" if instrument_type in ("CE", "PE") else f"{exch}-FUT"
            elif "mcx" in seg_raw:
                exch = "MCX"
                segment = "MCX-OPT" if instrument_type in ("CE", "PE") else "MCX-FUT"
            elif "bse" in seg_raw:
                exch = "BSE"
                segment = "BSE"
            else:
                exch = "NSE"
                segment = "NSE"

            clean.append({
                "instrument_token": tkn,
                "exchange_token": tkn,
                "tradingsymbol": tsym,
                "name": name,
                "last_price": 0.0,
                "expiry": exp_str,
                "strike": strike,
                "tick_size": tick_size,
                "lot_size": lot_size,
                "instrument_type": instrument_type,
                "segment": segment,
                "exchange": exch,
            })

        try:
            return pl.from_dicts(clean, infer_schema_length=None) if clean else pl.DataFrame()
        except Exception:
            return pl.DataFrame()

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
        High-Performance Polars-Native Master Instrument Token Processor for Kotak Neo.
        Loads configuration from Excel via polars_excel, fetches live Kotak Neo ScripMaster files,
        normalizes expiries/strikes, and assembles cum_table with crash-recovery caching.
        """
        import gc
        import threading
        import orjson
        from datetime import datetime, timedelta
        from algo_trading.algos.polars_excel import load_indian_algo_config, write_polars_sheets_to_excel, build_aug_table
        from algo_trading.algos.indian_master_tokens import (
            assemble_indian_cum_table,
            normalize_instruments_schema,
            today_ist,
        )

        # 1. Load Excel Config if not pre-supplied
        if aug_table is None or cap_config is None:
            cfg = load_indian_algo_config(input_file)
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
                            cum_df = cum_df.filter(pl.col("Ref_stock_tkn") != -1)

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
                                "Active symbols in config (%s) differ from cached cum_table (%s). Re-assembling fresh cum_table.",
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
                            logger.info("Restored pre-computed cum_table from ExchangeMasterData for Kotak Neo (%d rows).", len(cum_df))
                            return cum_df, inst_list_int, init_ref_list, index_ref_list
            except Exception as ex:
                logger.warning("Failed to restore cum_table from ExchangeMasterData: %s", ex)
                run_tkn_update = True

        # 3. Fetch fresh instruments across Kotak Neo exchanges
        logger.info("Fetching fresh instruments list from Kotak Neo ScripMaster across exchanges...")
        raw_instruments: dict[str, pl.DataFrame] = {}
        for exchg in ["NSE", "NFO", "CDS", "MCX", "BFO", "BSE"]:
            try:
                raw_list = self.instruments_list(exchg)
                if raw_list:
                    normalized_df = self._normalize_kotak_scrip(raw_list)
                    raw_instruments[exchg] = normalize_instruments_schema(normalized_df, exchg)
                else:
                    raw_instruments[exchg] = pl.DataFrame()
            except Exception as e:
                logger.error("Error fetching Kotak Neo instruments for %s: %s", exchg, e)
                raw_instruments[exchg] = pl.DataFrame()

        # 4. Assemble cum_table
        cum_table, inst_list_int, init_ref_list, index_ref_list = assemble_indian_cum_table(
            raw_instruments=raw_instruments,
            aug_table=aug_table,
            cap_config=cap_config,
            month_cutoff=month_cutoff,
            tz=tz,
            broker_name="kotak_neo",
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
            def _export(export_insts, export_cum):
                try:
                    if output_file and not export_cum.is_empty():
                        write_polars_sheets_to_excel({"cum_table": export_cum}, output_file)
                    if master_list and export_insts:
                        valid_sheets = {k: v for k, v in export_insts.items() if not v.is_empty()}
                        if valid_sheets:
                            write_polars_sheets_to_excel(valid_sheets, master_list)
                except Exception as exc:
                    logger.warning("Background debug Excel export failed: %s", exc)

            threading.Thread(target=_export, args=(raw_instruments if master_list else None, cum_table), daemon=True).start()

        # 7. Clean up RAM
        del raw_instruments
        gc.collect()

        return cum_table, inst_list_int, init_ref_list, index_ref_list



# Aliases for flexible imports
KotakUtility = KotakNeoUtility


if __name__ == "__main__":
    import os
    import django

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
    django.setup()

    try:
        util = KotakNeoUtility()
        print(f"Successfully connected to Kotak Neo account: {util.account_id} (sid: {util.sid})")
        cash, cap = util.chk_live_bal()
        print(f"Available Margin: Rs {cash:,.2f} | Net Capital: Rs {cap:,.2f}")
    except Exception as exc:
        print(f"Error testing KotakNeoUtility: {exc}")
