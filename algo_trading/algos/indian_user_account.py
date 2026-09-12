# -*- coding: utf-8 -*-
"""
algo_trading/algos/indian_user_account.py
─────────────────────────────────────────
Multi-account user container for Indian brokers (Zerodha Kite, Kotak Neo).
Manages ThreadPool-accelerated balance/position syncing, order book polling,
and individual position stop-loss tracking using pure Polars.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl

logger = logging.getLogger("algo_trading.algos.indian_user_account")


class IndianUserAccount:
    """
    Encapsulates broker session, financial balances, positions, orders,
    and trailing stop losses for an Indian trading account using Polars.
    """

    def __init__(
        self,
        user_id: str,
        client: Any,
        broker_obj: Optional[Any] = None,
        capital_allowed: float = 100000.0,
        capital_multiplier: float = 1.0,
        enabled: bool = True,
    ):
        self.user_id = user_id
        self.client = client
        self.broker_obj = broker_obj
        self.capital_allowed = capital_allowed
        self.capital_multiplier = capital_multiplier
        self.enabled = enabled

        # Financial & Position State
        self.live_balance: float = 0.0
        self.avail_cash: float = 0.0
        self.hold_frame: pl.DataFrame = pl.DataFrame()
        self.pos_day_frame: pl.DataFrame = pl.DataFrame()
        self.pos_net_frame: pl.DataFrame = pl.DataFrame()
        self.open_positions: pl.DataFrame = pl.DataFrame()
        self.order_status: pl.DataFrame = pl.DataFrame()

        # Stop-loss & Order tracking
        sl_schema = {
            "order_id": pl.Utf8,
            "buy_price": pl.Float64,
            "instrument_token": pl.Int64,
            "date_time": pl.Datetime,
            "tradingsymbol": pl.Utf8,
        }
        self.stop_loss_info: pl.DataFrame = pl.DataFrame(schema=sl_schema)

        strike_schema = {
            "ref_symbol": pl.Utf8,
            "strike_value": pl.Float64,
            "instrument_token": pl.Int64,
            "date_time": pl.Datetime,
            "tradingsymbol": pl.Utf8,
        }
        self.strike_entry_info: pl.DataFrame = pl.DataFrame(schema=strike_schema)

        self.order_pending_counter: Dict[int, int] = {}
        self.recent_sell_order_time: Dict[str, datetime] = {}
        self.recent_buy_order_time: Dict[str, datetime] = {}

        # ── State Sync Timestamps & Tiered TTLs (in seconds) ───────────────────
        self._last_balance_sync: float = 0.0
        self._last_holdings_sync: float = 0.0
        self._last_positions_sync: float = 0.0
        self._last_orders_sync: float = 0.0

        self.balance_ttl: float = 30.0       # Cash balance TTL (seconds)
        self.holdings_ttl: float = 300.0     # Long-term equity holdings TTL (seconds)
        self.positions_ttl: float = 10.0     # Positions TTL during idle market (seconds)
        self.orders_idle_ttl: float = 15.0   # Orders TTL when no pending orders exist
        self.orders_active_ttl: float = 2.0  # Orders TTL while pending orders are in-flight

        # Event-driven dirty & pending flags
        self.has_pending_orders: bool = False
        self.dirty_balance: bool = False
        self.dirty_positions: bool = False

    @staticmethod
    def _parse_df(val: Any) -> pl.DataFrame:
        """Converts raw API response or list of dicts to a Polars DataFrame safely."""
        if val is None:
            return pl.DataFrame()
        if isinstance(val, pl.DataFrame):
            return val
        if hasattr(val, "to_dicts"):
            try:
                return pl.from_dicts(val.to_dicts())
            except Exception:
                pass
        if hasattr(val, "to_dict"):
            try:
                records = val.to_dict(orient="records") if hasattr(val, "to_dict") else val
                return pl.from_dicts(records) if records else pl.DataFrame()
            except Exception:
                pass
        if isinstance(val, str):
            try:
                try:
                    import orjson
                    parsed = orjson.loads(val)
                except Exception:
                    import json
                    parsed = json.loads(val)
                if isinstance(parsed, list):
                    return pl.from_dicts(parsed)
                elif isinstance(parsed, dict):
                    return pl.from_dicts([parsed])
                return pl.DataFrame()
            except Exception:
                return pl.DataFrame()
        if isinstance(val, list):
            if not val:
                return pl.DataFrame()
            try:
                return pl.from_dicts(val)
            except Exception:
                return pl.DataFrame()
        if isinstance(val, dict):
            try:
                return pl.from_dicts([val])
            except Exception:
                return pl.DataFrame()
        return pl.DataFrame()

    def mark_order_placed(
        self,
        order_id: Any,
        symbol: str,
        qty: int,
        price: float,
        transaction_type: str = "BUY"
    ) -> None:
        """
        Optimistically updates local account balance and sets dirty flags.
        Prevents over-allocation in the same trading cycle.
        """
        if str(transaction_type).upper() == "BUY" and price > 0 and qty > 0:
            cost = float(qty * price)
            self.live_balance = max(0.0, self.live_balance - cost)
            self.avail_cash = max(0.0, self.avail_cash - cost)

        self.has_pending_orders = True
        self.dirty_balance = True
        self.dirty_positions = True
        self._last_orders_sync = 0.0  # Force immediate refresh on next cycle

    def sync_account_state(
        self,
        force_all: bool = False,
        force_positions: bool = False,
        force_balance: bool = False,
        debug_mode: bool = False,
    ) -> None:
        """
        Intelligently and selectively syncs account state.
        Only dispatches API calls for endpoints whose TTL has expired or whose dirty flag is set.
        """
        if not self.enabled or self.client is None:
            return

        now = time.time()
        tasks: Dict[str, Any] = {}

        # 1. Evaluate Holdings (Long TTL, rarely changes intra-day)
        if force_all or (now - self._last_holdings_sync >= self.holdings_ttl):
            if hasattr(self.client, "holdings"):
                tasks["holdings"] = self.client.holdings

        # 2. Evaluate Balance (TTL or event-triggered / signal-forced)
        if force_all or force_balance or self.dirty_balance or (now - self._last_balance_sync >= self.balance_ttl):
            if hasattr(self.client, "chk_live_bal"):
                tasks["balance"] = self.client.chk_live_bal

        # 3. Evaluate Orders (Adaptive TTL: fast when orders pending, slow when idle)
        ord_ttl = self.orders_active_ttl if self.has_pending_orders else self.orders_idle_ttl
        if force_all or self.has_pending_orders or (now - self._last_orders_sync >= ord_ttl):
            if hasattr(self.client, "orders"):
                tasks["orders"] = self.client.orders

        # 4. Evaluate Positions (TTL or event-triggered / signal-forced)
        if force_all or force_positions or self.dirty_positions or (now - self._last_positions_sync >= self.positions_ttl):
            if hasattr(self.client, "pos_data"):
                tasks["positions"] = self.client.pos_data

        # If all cached data is fresh, skip network roundtrips completely
        if not tasks:
            return

        def _run_with_account_context(task_fn):
            def _inner():
                from algo_trading.algos.logger import algo_logger
                algo_logger.set_active_account(self.broker_obj, algo_name="indian_opt_trde_polars")
                try:
                    return task_fn()
                finally:
                    algo_logger.clear_active_account()
            return _inner

        try:
            with ThreadPoolExecutor(max_workers=min(4, max(1, len(tasks)))) as executor:
                futures = {k: executor.submit(_run_with_account_context(fn)) for k, fn in tasks.items()}

                if "holdings" in futures:
                    try:
                        raw_hold = futures["holdings"].result()
                        self.hold_frame = self._parse_df(raw_hold)
                        self._last_holdings_sync = now
                    except Exception as ex:
                        logger.warning("[%s] Holdings sync failed: %s", self.user_id, ex)

                if "balance" in futures:
                    try:
                        self.avail_cash, self.live_balance = futures["balance"].result()
                        self._last_balance_sync = now
                        self.dirty_balance = False
                    except Exception as ex:
                        logger.warning("[%s] Balance sync failed: %s", self.user_id, ex)

                if "orders" in futures:
                    try:
                        raw_orders = futures["orders"].result()
                        self.order_status = self._parse_df(raw_orders)
                        self._last_orders_sync = now
                        # Update pending orders flag from fresh order status
                        if not self.order_status.is_empty() and "status" in self.order_status.columns:
                            pending_count = len(self.order_status.filter(
                                pl.col("status").str.to_uppercase().is_in(["OPEN", "PENDING", "TRIGGER PENDING"])
                            ))
                            self.has_pending_orders = (pending_count > 0)
                        else:
                            self.has_pending_orders = False
                    except Exception as ex:
                        logger.warning("[%s] Orders sync failed: %s", self.user_id, ex)

                if "positions" in futures:
                    try:
                        day_raw, net_raw = futures["positions"].result()
                        self.pos_day_frame = self._parse_df(day_raw)
                        self.pos_net_frame = self._parse_df(net_raw)
                        self.open_positions = self.extract_open_positions()
                        self._last_positions_sync = now
                        self.dirty_positions = False
                    except Exception as ex:
                        logger.warning("[%s] Positions sync failed: %s", self.user_id, ex)

            logger.info(
                "[%s] Selective State Sync (Endpoints: %s): Balance=Rs.%.2f, OpenPositions=%d, Orders=%d, PendingOrders=%s",
                self.user_id, list(tasks.keys()), self.live_balance, len(self.open_positions), len(self.order_status), self.has_pending_orders
            )
        except Exception as e:
            logger.error("[%s] Failed to sync account state: %s", self.user_id, e)
            if debug_mode:
                self.avail_cash = getattr(self, "capital_allowed", 50000.0)
                self.live_balance = getattr(self, "capital_allowed", 50000.0)

    def extract_open_positions(self) -> pl.DataFrame:
        """Extract open positions from day and net frames using Polars expressions."""
        chunks: List[pl.DataFrame] = []
        net = self.pos_net_frame
        day = self.pos_day_frame

        if not net.is_empty() and "quantity" in net.columns:
            overnight = pl.col("overnight_quantity") if "overnight_quantity" in net.columns else pl.lit(0)
            net_open = net.filter((pl.col("quantity") != 0) | (overnight != 0))
            if not net_open.is_empty():
                chunks.append(net_open)

        if not day.is_empty() and "quantity" in day.columns:
            overnight_day = pl.col("overnight_quantity") if "overnight_quantity" in day.columns else pl.lit(0)
            day_open = day.filter((pl.col("quantity") != 0) | (overnight_day != 0))
            if not day_open.is_empty():
                chunks.append(day_open)

        if chunks:
            open_pos = pl.concat(chunks, how="diagonal")
            if "instrument_token" in open_pos.columns:
                open_pos = open_pos.unique(subset=["instrument_token"], keep="last")
            return open_pos

        return pl.DataFrame()
