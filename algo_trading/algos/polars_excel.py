# -*- coding: utf-8 -*-
"""
algo_trading/algos/polars_excel.py
───────────────────────────────────
High-performance, pure Polars & openpyxl spreadsheet utilities.
Provides zero-pandas reading and writing of single/multi-sheet Excel files.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union
import openpyxl
import polars as pl


def read_excel_sheet_polars(file_path: str, sheet_name: str) -> pl.DataFrame:
    """
    Read a specific worksheet from an Excel workbook directly into a Polars DataFrame.
    """
    if not os.path.exists(file_path):
        return pl.DataFrame()

    wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
    try:
        if sheet_name not in wb.sheetnames:
            return pl.DataFrame()

        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return pl.DataFrame()

        header = [str(c) if c is not None else f"col_{i}" for i, c in enumerate(rows[0])]
        data = rows[1:]
        if not data:
            return pl.DataFrame(schema=header)

        return pl.DataFrame(data, schema=header, orient="row")
    finally:
        wb.close()


def read_excel_stoploss_columns(file_path: str, sheet_name: str = "stoploss_tbl") -> Set[str]:
    """
    Extract configured stock symbol prefixes from row 0 of stoploss_tbl / derloss_tbl.
    """
    if not os.path.exists(file_path):
        return set()

    wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
    try:
        if sheet_name not in wb.sheetnames:
            return set()

        ws = wb[sheet_name]
        row_iter = ws.iter_rows(values_only=True)
        try:
            first_row = next(row_iter)
            return {str(c).strip() for c in first_row if c is not None and str(c).strip()}
        except StopIteration:
            return set()
    finally:
        wb.close()


def write_polars_sheets_to_excel(
    sheets_dict: Dict[str, Union[pl.DataFrame, Dict[str, List[Any]], None]],
    output_path: str,
) -> str:
    """
    Write multiple Polars DataFrames or structured dictionaries into a formatted
    multi-sheet Excel workbook using openpyxl without Pandas.
    """
    target_dir = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(target_dir, exist_ok=True)

    wb = openpyxl.Workbook()
    first = True

    for sheet_name, df_or_dict in sheets_dict.items():
        if df_or_dict is None:
            continue

        if first:
            ws = wb.active
            ws.title = sheet_name
            first = False
        else:
            ws = wb.create_sheet(title=sheet_name)

        if isinstance(df_or_dict, pl.DataFrame):
            if not df_or_dict.is_empty():
                ws.append(df_or_dict.columns)
                for row in df_or_dict.iter_rows():
                    ws.append(list(row))
            else:
                ws.append(df_or_dict.columns if df_or_dict.columns else ["Info"])
        elif isinstance(df_or_dict, dict):
            cols = list(df_or_dict.keys())
            ws.append(cols)
            row_count = max((len(v) for v in df_or_dict.values()), default=0)
            for r_idx in range(row_count):
                row = [df_or_dict[c][r_idx] if r_idx < len(df_or_dict[c]) else None for c in cols]
                ws.append(row)

    if first:
        wb.active.title = "Empty"

    final_path = output_path
    try:
        wb.save(output_path)
    except (PermissionError, OSError):
        base_name, ext = os.path.splitext(output_path)
        ts_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_path = f"{base_name}_{ts_suffix}{ext}"
        wb.save(final_path)

    return final_path


# ============================================================================
# TYPED ALGORITHM CONFIGURATION CONTAINERS
# ============================================================================

from dataclasses import dataclass, field
from datetime import date


@dataclass
class IndianAlgoConfig:
    """Strongly-typed parsed configuration container for Indian exchanges."""
    stock_config: pl.DataFrame = field(default_factory=pl.DataFrame)
    nse_holiday_info: pl.DataFrame = field(default_factory=pl.DataFrame)
    stock_data_info: pl.DataFrame = field(default_factory=pl.DataFrame)
    aug_table: pl.DataFrame = field(default_factory=pl.DataFrame)
    cap_config: pl.DataFrame = field(default_factory=pl.DataFrame)
    loss_table_stocks: Set[str] = field(default_factory=set)
    der_loss_table_stocks: Set[str] = field(default_factory=set)
    holiday_dates: List[date] = field(default_factory=list)

    # Scalar parameters with robust production defaults
    percent_of_capital_utilization: float = 1.0
    debounce_counter_threshold: int = 3
    order_pending_counter_threshold: int = 5
    margin_per_stock: float = 50000.0
    live_balance_lower_limit: float = 10000.0
    buy_ttl_value: int = 60
    sell_ttl_value: int = 60
    strike_choice_CE: str = "ATM"
    strike_choice_PE: str = "ATM"
    hedge_threshold: float = 1.5
    capital_allowed: float = 100000.0
    stoploss_threshold: float = 0.15


@dataclass
class CryptoAlgoConfig:
    """Strongly-typed parsed configuration container for Crypto exchanges."""
    stock_config: pl.DataFrame = field(default_factory=pl.DataFrame)
    nse_holiday_info: pl.DataFrame = field(default_factory=pl.DataFrame)
    stock_data_info: pl.DataFrame = field(default_factory=pl.DataFrame)
    aug_table: pl.DataFrame = field(default_factory=pl.DataFrame)
    cap_config: pl.DataFrame = field(default_factory=pl.DataFrame)
    loss_table_stocks: Set[str] = field(default_factory=set)
    der_loss_table_stocks: Set[str] = field(default_factory=set)
    holiday_dates: List[date] = field(default_factory=list)

    # Scalar parameters with robust production defaults
    percent_of_capital_utilization: float = 1.0
    debounce_counter_threshold: int = 3
    order_pending_counter_threshold: int = 5
    margin_per_stock: float = 50000.0
    live_balance_lower_limit: float = 10000.0
    buy_ttl_value: int = 60
    sell_ttl_value: int = 60
    strike_choice_CE: str = "ATM"
    strike_choice_PE: str = "ATM"
    hedge_threshold: float = 1.5
    capital_allowed: float = 100000.0
    stoploss_threshold: float = 0.15


def _extract_sheet_polars(ws: Any) -> pl.DataFrame:
    """Extract openpyxl worksheet rows into a Polars DataFrame."""
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return pl.DataFrame()
    header = [str(c) if c is not None else f"col_{i}" for i, c in enumerate(rows[0])]
    data = rows[1:]
    if not data:
        return pl.DataFrame(schema=header)
    return pl.DataFrame(data, schema=header, orient="row")


def _extract_stoploss_set(ws: Any) -> Set[str]:
    """Extract symbol set from the first row of a stoploss worksheet."""
    row_iter = ws.iter_rows(values_only=True)
    try:
        first_row = next(row_iter)
        return {str(c).strip() for c in first_row if c is not None and str(c).strip()}
    except StopIteration:
        return set()


def build_aug_table(stock_data_info: pl.DataFrame) -> pl.DataFrame:
    """
    Filter raw stock/crypto data into tradable aug_table strictly requiring Capital_share > 0
    and Max_lots_per_order > 0.
    Note: Tradable_stock is deliberately omitted as per configuration requirements.
    """
    if stock_data_info.is_empty():
        return pl.DataFrame()
    cap_filter = (
        pl.col("Capital_share").cast(pl.Float64, strict=False).fill_null(0.0) > 0
    ) if "Capital_share" in stock_data_info.columns else pl.lit(True)
    lots_filter = (
        pl.col("Max_lots_per_order").cast(pl.Int64, strict=False).fill_null(0) > 0
    ) if "Max_lots_per_order" in stock_data_info.columns else pl.lit(True)
    return stock_data_info.filter(cap_filter & lots_filter)


_build_aug_table = build_aug_table


def load_indian_algo_config(file_path: str) -> IndianAlgoConfig:
    """
    Ingests Indian exchange configuration spreadsheet (e.g. token_ref.xlsx)
    using a single-pass openpyxl workbook load and converts to Polars with zero Pandas dependencies.
    """
    if not os.path.exists(file_path):
        return IndianAlgoConfig()

    cfg = IndianAlgoConfig()
    wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
    try:
        sheet_names = wb.sheetnames

        # 1. Config sheet (nfo_config or stock_config)
        cfg_name = "nfo_config" if "nfo_config" in sheet_names else ("stock_config" if "stock_config" in sheet_names else None)
        if cfg_name:
            cfg.stock_config = _extract_sheet_polars(wb[cfg_name])

        # 2. Stock data list (nfo_list or Stock_list)
        list_name = "nfo_list" if "nfo_list" in sheet_names else ("Stock_list" if "Stock_list" in sheet_names else None)
        if list_name:
            cfg.stock_data_info = _extract_sheet_polars(wb[list_name])
            cfg.aug_table = _build_aug_table(cfg.stock_data_info)

        # 3. Holiday info (nse_holiday_info)
        if "nse_holiday_info" in sheet_names:
            cfg.nse_holiday_info = _extract_sheet_polars(wb["nse_holiday_info"])
            if not cfg.nse_holiday_info.is_empty() and "Date" in cfg.nse_holiday_info.columns:
                h_dates: List[date] = []
                for val in cfg.nse_holiday_info["Date"].to_list():
                    if isinstance(val, datetime):
                        h_dates.append(val.date())
                    elif isinstance(val, date):
                        h_dates.append(val)
                    elif isinstance(val, str):
                        try:
                            h_dates.append(datetime.fromisoformat(val).date())
                        except Exception:
                            pass
                cfg.holiday_dates = h_dates

        # 4. Stop-loss tables
        if "stoploss_tbl" in sheet_names:
            cfg.loss_table_stocks = _extract_stoploss_set(wb["stoploss_tbl"])
        if "derloss_tbl" in sheet_names:
            cfg.der_loss_table_stocks = _extract_stoploss_set(wb["derloss_tbl"])

        # 5. Parse scalar parameters and cap_config from stock_config
        if not cfg.stock_config.is_empty() and len(cfg.stock_config) > 20:
            try:
                cfg.percent_of_capital_utilization = float(cfg.stock_config.item(7, 1) or 1.0)
            except Exception:
                pass
            try:
                cap_slice = cfg.stock_config.slice(1, 4).select(cfg.stock_config.columns[4:])
                new_cols = [str(x) for x in cap_slice.row(0)]
                raw_cap = cap_slice.slice(1).rename(dict(zip(cap_slice.columns, new_cols)))
                exprs = []
                for c_name in raw_cap.columns:
                    if c_name in ("tradable", "minimum_lots_to_buy", "maximum_lots_to_buy", "preference"):
                        exprs.append(pl.col(c_name).cast(pl.Float64, strict=False).fill_null(1.0).cast(pl.Int64).alias(c_name))
                    elif c_name in ("lower_price_limit", "upper_price_limit"):
                        exprs.append(pl.col(c_name).cast(pl.Float64, strict=False).alias(c_name))
                    else:
                        exprs.append(pl.col(c_name))
                cfg.cap_config = raw_cap.with_columns(exprs) if exprs else raw_cap
            except Exception:
                cfg.cap_config = pl.DataFrame()

            try:
                cfg.debounce_counter_threshold = int(cfg.stock_config.item(8, 1) or 3)
                cfg.margin_per_stock = float(cfg.stock_config.item(9, 1) or 50000.0)
                cfg.live_balance_lower_limit = float(cfg.stock_config.item(10, 1) or 10000.0)
                cfg.order_pending_counter_threshold = int(cfg.stock_config.item(12, 1) or 5)
                cfg.buy_ttl_value = int(cfg.stock_config.item(14, 1) or 60)
                cfg.sell_ttl_value = int(cfg.stock_config.item(15, 1) or 60)
                cfg.strike_choice_CE = str(cfg.stock_config.item(16, 1) or "ATM")
                cfg.strike_choice_PE = str(cfg.stock_config.item(17, 1) or "ATM")
                cfg.hedge_threshold = float(cfg.stock_config.item(18, 1) or 1.5)
                cfg.capital_allowed = float(cfg.stock_config.item(19, 1) or 100000.0)
                cfg.stoploss_threshold = float(cfg.stock_config.item(20, 1) or 0.15)
            except Exception:
                pass

    finally:
        wb.close()

    return cfg


def load_crypto_algo_config(file_path: str) -> CryptoAlgoConfig:
    """
    Ingests Crypto exchange configuration spreadsheet (e.g. token_ref_bitcoin.xlsx)
    using a single-pass openpyxl workbook load and converts to Polars with zero Pandas dependencies.
    """
    if not os.path.exists(file_path):
        return CryptoAlgoConfig()

    cfg = CryptoAlgoConfig()
    wb = openpyxl.load_workbook(file_path, data_only=True, read_only=True)
    try:
        sheet_names = wb.sheetnames

        cfg_name = "bit_config" if "bit_config" in sheet_names else ("nfo_config" if "nfo_config" in sheet_names else "stock_config")
        if cfg_name in sheet_names:
            cfg.stock_config = _extract_sheet_polars(wb[cfg_name])

        list_name = "bit_list" if "bit_list" in sheet_names else ("nfo_list" if "nfo_list" in sheet_names else "Stock_list")
        if list_name in sheet_names:
            cfg.stock_data_info = _extract_sheet_polars(wb[list_name])
            cfg.aug_table = _build_aug_table(cfg.stock_data_info)

        hol_name = "bit_holiday_info" if "bit_holiday_info" in sheet_names else ("nse_holiday_info" if "nse_holiday_info" in sheet_names else None)
        if hol_name and hol_name in sheet_names:
            cfg.nse_holiday_info = _extract_sheet_polars(wb[hol_name])

        if "stoploss_tbl" in sheet_names:
            cfg.loss_table_stocks = _extract_stoploss_set(wb["stoploss_tbl"])
        if "derloss_tbl" in sheet_names:
            cfg.der_loss_table_stocks = _extract_stoploss_set(wb["derloss_tbl"])

        if not cfg.stock_config.is_empty() and len(cfg.stock_config) > 20:
            try:
                cfg.percent_of_capital_utilization = float(cfg.stock_config.item(7, 1) or 1.0)
            except Exception:
                pass
            try:
                cap_slice = cfg.stock_config.slice(1, 4).select(cfg.stock_config.columns[4:])
                new_cols = [str(x) for x in cap_slice.row(0)]
                raw_cap = cap_slice.slice(1).rename(dict(zip(cap_slice.columns, new_cols)))
                exprs = []
                for c_name in raw_cap.columns:
                    if c_name in ("tradable", "minimum_lots_to_buy", "maximum_lots_to_buy", "preference"):
                        exprs.append(pl.col(c_name).cast(pl.Float64, strict=False).fill_null(1.0).cast(pl.Int64).alias(c_name))
                    elif c_name in ("lower_price_limit", "upper_price_limit"):
                        exprs.append(pl.col(c_name).cast(pl.Float64, strict=False).alias(c_name))
                    else:
                        exprs.append(pl.col(c_name))
                cfg.cap_config = raw_cap.with_columns(exprs) if exprs else raw_cap
            except Exception:
                cfg.cap_config = pl.DataFrame()

            try:
                cfg.debounce_counter_threshold = int(cfg.stock_config.item(8, 1) or 3)
                cfg.margin_per_stock = float(cfg.stock_config.item(9, 1) or 50000.0)
                cfg.live_balance_lower_limit = float(cfg.stock_config.item(10, 1) or 10000.0)
                cfg.order_pending_counter_threshold = int(cfg.stock_config.item(12, 1) or 5)
                cfg.buy_ttl_value = int(cfg.stock_config.item(14, 1) or 60)
                cfg.sell_ttl_value = int(cfg.stock_config.item(15, 1) or 60)
                cfg.strike_choice_CE = str(cfg.stock_config.item(16, 1) or "ATM")
                cfg.strike_choice_PE = str(cfg.stock_config.item(17, 1) or "ATM")
                cfg.hedge_threshold = float(cfg.stock_config.item(18, 1) or 1.5)
                cfg.capital_allowed = float(cfg.stock_config.item(19, 1) or 100000.0)
                cfg.stoploss_threshold = float(cfg.stock_config.item(20, 1) or 0.15)
            except Exception:
                pass

    finally:
        wb.close()

    return cfg

