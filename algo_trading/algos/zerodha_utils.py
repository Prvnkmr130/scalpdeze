# -*- coding: utf-8 -*-
"""
algo_trading/algos/zerodha_utils.py
───────────────────────────────────
Wrapper around Zerodha KiteConnect API customized for order management,
portfolio inspection, margin calculations, and multi-account trading in DeltaZero26.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import numpy as np
import polars as pl
from kiteconnect import KiteConnect

try:
    from kiteconnect.exceptions import TokenException, PermissionException
except ImportError:
    TokenException = PermissionException = None

logger = logging.getLogger("algo_trading.algos.zerodha_utils")


def retry(func: Callable, max_retries: int = 3, delay: float = 1.0) -> Any:
    """
    Retry helper for network and transient API operations.
    Skips retrying non-recoverable authentication errors.
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exc = e
            # Fail fast on non-recoverable token / auth errors
            if TokenException and isinstance(e, (TokenException, PermissionException)):
                raise e
            err_msg = str(e).lower()
            if "token" in err_msg or "401" in err_msg or "403" in err_msg or "incorrect `api_key`" in err_msg:
                raise e

            logger.warning(
                "Attempt %d/%d failed with error: %s. Retrying in %.1fs...",
                attempt,
                max_retries,
                e,
                delay,
            )
            time.sleep(delay)
    if last_exc:
        raise last_exc


class ZerodhaUtility:
    """
    Order execution, account positions, margins, and holdings manager for Zerodha KiteConnect.
    Supports multi-account resolution via account_id.
    """

    def __init__(
        self,
        account_id: str | None = None,
        broker_obj: Any = None,
        client: Any = None,
        api_key: str | None = None,
        access_token: str | None = None,
    ) -> None:
        """
        Initialize ZerodhaUtility by fetching credentials from the Broker database model.

        Parameters
        ----------
        account_id : str, optional
            Account ID or name (e.g. 'HS6525'). If None, loads the first active Zerodha account.
        broker_obj : Broker, optional
            Direct Broker model instance.
        client : KiteConnect, optional
            Pre-configured or mock KiteConnect client instance.
        api_key : str, optional
            Direct KiteConnect API key.
        access_token : str, optional
            Direct KiteConnect access token.
        """
        if client is not None:
            self.client = client
            self.broker = broker_obj
            self.account_id = account_id or getattr(client, "account_id", "mock_zerodha")
            self.api_key = api_key or getattr(client, "api_key", "")
            self.access_token = access_token or getattr(client, "access_token", "")
            logger.info("Initialized ZerodhaUtility with pre-supplied client for account '%s'", self.account_id)
            return

        if api_key is not None or access_token is not None:
            self.broker = broker_obj
            self.account_id = account_id or "direct_zerodha"
            self.api_key = (api_key or "").strip()
            self.access_token = (access_token or "").strip()
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
                # Default to first active Zerodha account
                self.broker = (
                    Broker.objects.filter(
                        Q(broker_name__code__iexact="zerodha")
                        | Q(api_provider__code__iexact="zerodha")
                        | Q(name__icontains="zerodha")
                    )
                    .order_by("-enable_websocket")
                    .first()
                )

            if not self.broker:
                identifier = account_id or "default Zerodha"
                logger.warning("Zerodha broker configuration for '%s' not found in database.", identifier)
                self.account_id = identifier
                self.api_key = (api_key or "").strip()
                self.access_token = (access_token or "").strip()
            else:
                self.account_id = self.broker.account_id or self.broker.name
                self.api_key = (api_key or self.broker.api_key or "").strip()
                self.access_token = (access_token or self.broker.access_token or "").strip()

        # Initialize KiteConnect client (allow public instrument discovery even if token missing)
        self.client = KiteConnect(api_key=self.api_key or "kite_public")
        if self.access_token:
            self.client.set_access_token(self.access_token)
        else:
            logger.debug(
                "Access token is missing for Zerodha account '%s'. Live trading orders will require authentication.",
                self.account_id,
            )
        logger.info("Initialized ZerodhaUtility for account '%s'", self.account_id)

    def get_access_token(self) -> str:
        """Return the active access token for Zerodha."""
        return self.access_token

    def holdings(self) -> pl.DataFrame | None:
        """Fetch and return account holdings as a Polars DataFrame."""
        if not self.access_token:
            logger.debug("[%s] Cannot fetch holdings: Access token is missing.", self.account_id)
            return pl.DataFrame()
        try:
            raw = retry(lambda: self.client.holdings())
            if isinstance(raw, list) and len(raw) > 0:
                hold_df = pl.DataFrame(raw)
                if not hold_df.is_empty() and "tradingsymbol" in hold_df.columns:
                    hold_df = hold_df.with_columns(
                        pl.col("tradingsymbol").cast(pl.Utf8).str.replace_all(r"\*", "")
                    )
                return hold_df
            return pl.DataFrame()
        except Exception as e:
            logger.error("Failed to fetch holdings for %s: %s", self.account_id, e)
        return None

    def pos_data(self) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
        """Fetch positions and return (day_positions_df, net_positions_df)."""
        if not self.access_token:
            logger.debug("[%s] Cannot fetch positions: Access token is missing.", self.account_id)
            return None, None
        pos_day_frame = None
        pos_net_frame = None
        try:
            ps = retry(lambda: self.client.positions())
            net_items = ps.get("net") or []
            if isinstance(net_items, list) and len(net_items) > 0:
                pos_net_frame = pl.DataFrame(net_items)
                if not pos_net_frame.is_empty() and "tradingsymbol" in pos_net_frame.columns:
                    pos_net_frame = pos_net_frame.with_columns(
                        pl.col("tradingsymbol").cast(pl.Utf8).str.replace_all(r"\*", "")
                    )

            day_items = ps.get("day") or []
            if isinstance(day_items, list) and len(day_items) > 0:
                pos_day_frame = pl.DataFrame(day_items)
                if not pos_day_frame.is_empty() and "tradingsymbol" in pos_day_frame.columns:
                    pos_day_frame = pos_day_frame.with_columns(
                        pl.col("tradingsymbol").cast(pl.Utf8).str.replace_all(r"\*", "")
                    )
        except Exception as e:
            logger.error("Failed to fetch positions for %s: %s", self.account_id, e)

        return pos_day_frame, pos_net_frame

    def chk_live_bal(self) -> tuple[float, float]:
        """Check and return available cash and total net capital (avail_cash, net_capital)."""
        if not self.access_token:
            logger.debug("[%s] Cannot fetch live balances: Access token is missing.", self.account_id)
            return 0.0, 0.0
        try:
            capital_info = retry(lambda: self.client.margins())
            equity = capital_info.get("equity", {})
            avail_cash = float(equity.get("available", {}).get("cash", 0.0))
            net_cap = float(equity.get("net", 0.0))
            return avail_cash, net_cap
        except Exception as e:
            logger.error("Failed to fetch balances for %s: %s", self.account_id, e)
            return 0.0, 0.0

    def order_history(self, ord_id: str) -> list[dict]:
        """Get history of a specific order ID."""
        if not self.access_token:
            return []
        return retry(lambda: self.client.order_history(ord_id))

    def orders(self) -> list[dict]:
        """Return all orders for today."""
        if not self.access_token:
            return []
        return retry(lambda: self.client.orders())

    def order_trades(self, ord_id: str) -> list[dict]:
        """Return trades generated by a specific order ID."""
        if not self.access_token:
            return []
        return retry(lambda: self.client.order_trades(ord_id))

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
        if not self.access_token:
            return -1, f"Order placement failed: Access token missing for account '{self.account_id}'"
        try:
            kwargs: dict[str, Any] = {
                "variety": "regular",
                "exchange": exchg,
                "tradingsymbol": symbol,
                "transaction_type": buy_sell.upper(),
                "quantity": int(quantity),
                "product": prod.upper(),
                "order_type": "MARKET",
                "price": None,
                "validity": "TTL" if ttl_value else "DAY",
            }
            if ttl_value:
                kwargs["validity_ttl"] = ttl_value

            order_id = self.client.place_order(**kwargs)
            logger.info("[%s] Market order placed successfully. Order ID: %s", self.account_id, order_id)
            return order_id, "order placed"
        except Exception as e:
            logger.error("[%s] Market order failed for %s: %s", self.account_id, symbol, e)
            return -1, f"Order placement failed: {e}"

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
        if not self.access_token:
            return -1, f"Order placement failed: Access token missing for account '{self.account_id}'"
        try:
            kwargs: dict[str, Any] = {
                "variety": "regular",
                "exchange": exchg,
                "tradingsymbol": symbol,
                "transaction_type": buy_sell.upper(),
                "quantity": int(quantity),
                "product": prod.upper(),
                "order_type": "LIMIT",
                "price": float(price),
                "validity": "TTL" if ttl_value else validity,
            }
            if ttl_value:
                kwargs["validity_ttl"] = ttl_value

            order_id = self.client.place_order(**kwargs)
            logger.info("[%s] Limit order placed successfully. Order ID: %s", self.account_id, order_id)
            return order_id, "order placed"
        except Exception as e:
            logger.error("[%s] Limit order failed for %s: %s", self.account_id, symbol, e)
            return -1, f"Order placement failed: {e}"

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
        """Place an iceberg order."""
        if not self.access_token:
            return -1, f"Order placement failed: Access token missing for account '{self.account_id}'"
        try:
            kwargs: dict[str, Any] = {
                "variety": "iceberg",
                "exchange": exchg,
                "tradingsymbol": symbol,
                "transaction_type": buy_sell.upper(),
                "quantity": int(quantity),
                "product": prod.upper(),
                "order_type": order_type.upper(),
                "price": float(price) if order_type.upper() == "LIMIT" else None,
                "validity": "TTL" if ttl_value else validity,
                "iceberg_legs": int(legs),
                "iceberg_quantity": int(ice_qty),
            }
            if ttl_value:
                kwargs["validity_ttl"] = ttl_value

            order_id = self.client.place_order(**kwargs)
            logger.info("[%s] Iceberg order placed successfully. Order ID: %s", self.account_id, order_id)
            return order_id, "order placed"
        except Exception as e:
            logger.error("[%s] Iceberg order failed for %s: %s", self.account_id, symbol, e)
            return -1, f"Order placement failed: {e}"

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
        if not self.access_token:
            return -1, f"Order placement failed: Access token missing for account '{self.account_id}'"
        try:
            order_id = self.client.place_order(
                variety="regular",
                exchange=exchg,
                tradingsymbol=symbol,
                transaction_type=buy_sell.upper(),
                quantity=int(quantity),
                product=prod.upper(),
                order_type="SL-M",
                price=None,
                validity="DAY",
                trigger_price=float(trig_price),
            )
            logger.info("[%s] SL-M order placed successfully. Order ID: %s", self.account_id, order_id)
            return order_id, "order placed"
        except Exception as e:
            logger.error("[%s] SL-M order failed for %s: %s", self.account_id, symbol, e)
            return -1, f"Order placement failed: {e}"

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
        if not self.access_token:
            return -1, f"Order placement failed: Access token missing for account '{self.account_id}'"
        try:
            order_id = self.client.place_order(
                variety="regular",
                exchange=exchg,
                tradingsymbol=symbol,
                transaction_type=buy_sell.upper(),
                quantity=int(quantity),
                product=prod.upper(),
                order_type="SL",
                price=float(price),
                validity="DAY",
                trigger_price=float(trig_price),
            )
            logger.info("[%s] SL limit order placed successfully. Order ID: %s", self.account_id, order_id)
            return order_id, "order placed"
        except Exception as e:
            logger.error("[%s] SL order failed for %s: %s", self.account_id, symbol, e)
            return -1, f"Order placement failed: {e}"

    def cancel_ordr(
        self,
        variety: str = "regular",
        order_id: str = "",
        parent_order_id: str | None = None,
    ) -> Any:
        """Cancel an open order."""
        if not self.access_token:
            logger.warning("[%s] Cannot cancel order: Access token missing.", self.account_id)
            return None
        return self.client.cancel_order(
            variety=variety,
            order_id=order_id,
            parent_order_id=parent_order_id,
        )

    def exit_ordr(
        self,
        ord_id: str,
        parent_order_id: str | None = None,
    ) -> Any:
        """Exit an open position / bracket order."""
        if not self.access_token:
            logger.warning("[%s] Cannot exit order: Access token missing.", self.account_id)
            return None
        return retry(
            lambda: self.client.exit_order(
                variety="regular",
                order_id=ord_id,
                parent_order_id=parent_order_id,
            )
        )

    def instruments_list(self, exchg: str = "NSE") -> list[dict]:
        """Fetch all instruments for an exchange (e.g. 'NSE', 'NFO', 'BSE', 'MCX')."""
        raw = retry(lambda: self.client.instruments(exchange=exchg))
        if raw and isinstance(raw, list):
            for row in raw:
                exp = row.get("expiry")
                if isinstance(exp, (datetime, date)):
                    row["expiry"] = exp.isoformat()
                elif not exp:
                    row["expiry"] = None
        return raw

    def get_margin(self, orders_data: pl.DataFrame | list[dict]) -> list[dict]:
        """Calculate margins for a batch of order parameters."""
        if isinstance(orders_data, pl.DataFrame):
            params = orders_data.to_dicts()
        elif hasattr(orders_data, "to_dict"):
            params = orders_data.to_dict("records")
        else:
            params = orders_data
        return retry(lambda: self.client.order_margins(params))

    @staticmethod
    def sanitize_for_json(data: Any) -> Any:
        """Recursively convert DataFrames, datetimes, and numpy types for JSON serialization."""
        import numpy as np
        from datetime import date, datetime
        if isinstance(data, pl.DataFrame):
            return [ZerodhaUtility.sanitize_for_json(row) for row in data.to_dicts()]
        elif isinstance(data, dict):
            return {str(k): ZerodhaUtility.sanitize_for_json(v) for k, v in data.items()}
        elif isinstance(data, (list, tuple, set)):
            return [ZerodhaUtility.sanitize_for_json(item) for item in data]
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
        High-Performance Polars-Native Master Instrument Token Processor for Zerodha.
        Loads configuration from Excel via polars_excel, fetches live Kite instruments,
        and assembles cum_table with crash-recovery caching.
        """
        import gc
        import threading
        import numpy as np
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
                            logger.info("Restored pre-computed cum_table from ExchangeMasterData for Zerodha (%d rows).", len(cum_df))
                            return cum_df, inst_list_int, init_ref_list, index_ref_list
            except Exception as ex:
                logger.warning("Failed to restore cum_table from ExchangeMasterData: %s", ex)
                run_tkn_update = True

        # 3. Fetch fresh instruments across exchanges
        logger.info("Fetching fresh instruments list from Zerodha Kite across exchanges...")
        raw_instruments: Dict[str, pl.DataFrame] = {}
        for exchg in ["NSE", "NFO", "CDS", "MCX", "BFO", "BSE"]:
            try:
                raw_list = self.instruments_list(exchg)
                if raw_list:
                    raw_df = pl.from_dicts(raw_list)
                    raw_instruments[exchg] = normalize_instruments_schema(raw_df, exchg)
                else:
                    raw_instruments[exchg] = pl.DataFrame()
            except Exception as e:
                logger.error("Error fetching Zerodha instruments for %s: %s", exchg, e)
                raw_instruments[exchg] = pl.DataFrame()

        # 4. Assemble cum_table
        cum_table, inst_list_int, init_ref_list, index_ref_list = assemble_indian_cum_table(
            raw_instruments=raw_instruments,
            aug_table=aug_table,
            cap_config=cap_config,
            month_cutoff=month_cutoff,
            tz=tz,
            broker_name="zerodha",
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



if __name__ == "__main__":
    import os
    import django

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
    django.setup()

    try:
        util = ZerodhaUtility()
        print(f"Successfully connected to Zerodha account: {util.account_id}")
        cash, cap = util.chk_live_bal()
        print(f"Available Cash: Rs {cash:,.2f} | Net Capital: Rs {cap:,.2f}")
    except Exception as exc:
        print(f"Error testing ZerodhaUtility: {exc}")
