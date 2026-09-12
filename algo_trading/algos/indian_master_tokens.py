# -*- coding: utf-8 -*-
"""
algo_trading/algos/indian_master_tokens.py
──────────────────────────────────────────
High-performance, pure Polars master token list and cum_table assembler
for Indian exchanges (NSE, NFO, CDS, MCX, BSE, BFO).
Broker-agnostic: operates on normalized instrument DataFrames.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import polars as pl

from algo_trading.algos.broker_token_mapper import BrokerTokenMapper, token_mapper


def today_ist(tz: str = "Asia/Kolkata") -> date:
    """Returns the current date in Asia/Kolkata timezone."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(tz)).date()
    except Exception:
        return datetime.now().date()


def now_ist_naive(tz: str = "Asia/Kolkata") -> datetime:
    """Returns current datetime in Asia/Kolkata as a naive datetime object."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(tz)).replace(tzinfo=None)
    except Exception:
        return datetime.now().replace(tzinfo=None)


logger = logging.getLogger("algo_trading.algos.indian_master_tokens")



def normalize_instruments_schema(raw_df: pl.DataFrame, exchange: str) -> pl.DataFrame:
    """
    Ensures an instrument DataFrame conforms to the canonical Indian exchange schema:
    - instrument_token: Int64
    - tradingsymbol: Utf8
    - name: Utf8
    - expiry: Utf8
    - instrument_type: Utf8
    - segment: Utf8
    - exchange: Utf8
    - strike: Float64
    - lot_size: Int64
    - tick_size: Float64
    """
    if raw_df.is_empty():
        return pl.DataFrame(schema={
            "instrument_token": pl.Int64,
            "tradingsymbol": pl.Utf8,
            "name": pl.Utf8,
            "expiry": pl.Utf8,
            "instrument_type": pl.Utf8,
            "segment": pl.Utf8,
            "exchange": pl.Utf8,
            "strike": pl.Float64,
            "lot_size": pl.Int64,
            "tick_size": pl.Float64,
        })

    cols = raw_df.columns
    exprs = []

    # instrument_token
    if "instrument_token" in cols:
        exprs.append(pl.col("instrument_token").cast(pl.Int64, strict=False).alias("instrument_token"))
    elif "token" in cols:
        exprs.append(pl.col("token").cast(pl.Int64, strict=False).alias("instrument_token"))
    else:
        exprs.append(pl.lit(0, dtype=pl.Int64).alias("instrument_token"))

    # tradingsymbol
    if "tradingsymbol" in cols:
        exprs.append(pl.col("tradingsymbol").cast(pl.Utf8).alias("tradingsymbol"))
    elif "trading_symbol" in cols:
        exprs.append(pl.col("trading_symbol").cast(pl.Utf8).alias("tradingsymbol"))
    else:
        exprs.append(pl.lit("", dtype=pl.Utf8).alias("tradingsymbol"))

    # name
    if "name" in cols:
        exprs.append(pl.col("name").cast(pl.Utf8).alias("name"))
    elif "symbol_name" in cols:
        exprs.append(pl.col("symbol_name").cast(pl.Utf8).alias("name"))
    else:
        exprs.append(pl.col("tradingsymbol").cast(pl.Utf8).alias("name"))

    # expiry
    if "expiry" in cols:
        exprs.append(pl.col("expiry").cast(pl.Utf8).alias("expiry"))
    else:
        exprs.append(pl.lit("", dtype=pl.Utf8).alias("expiry"))

    # instrument_type
    if "instrument_type" in cols:
        exprs.append(pl.col("instrument_type").cast(pl.Utf8).alias("instrument_type"))
    else:
        exprs.append(pl.lit("EQ", dtype=pl.Utf8).alias("instrument_type"))

    # segment
    if "segment" in cols:
        exprs.append(pl.col("segment").cast(pl.Utf8).alias("segment"))
    else:
        exprs.append(pl.lit(exchange, dtype=pl.Utf8).alias("segment"))

    # exchange
    exprs.append(pl.lit(exchange, dtype=pl.Utf8).alias("exchange"))

    # strike
    if "strike" in cols:
        exprs.append(pl.col("strike").cast(pl.Float64, strict=False).fill_null(0.0).alias("strike"))
    else:
        exprs.append(pl.lit(0.0, dtype=pl.Float64).alias("strike"))

    # lot_size
    if "lot_size" in cols:
        exprs.append(pl.col("lot_size").cast(pl.Int64, strict=False).fill_null(1).alias("lot_size"))
    else:
        exprs.append(pl.lit(1, dtype=pl.Int64).alias("lot_size"))

    # tick_size
    if "tick_size" in cols:
        exprs.append(pl.col("tick_size").cast(pl.Float64, strict=False).fill_null(0.05).alias("tick_size"))
    else:
        exprs.append(pl.lit(0.05, dtype=pl.Float64).alias("tick_size"))

    return raw_df.select(exprs)


def assemble_indian_cum_table(
    raw_instruments: Dict[str, pl.DataFrame],
    aug_table: pl.DataFrame,
    cap_config: pl.DataFrame,
    month_cutoff: int = 0,
    tz: str = "Asia/Kolkata",
    broker_name: str = "zerodha",
) -> Tuple[pl.DataFrame, np.ndarray, pl.DataFrame, pl.DataFrame]:
    """
    High-Performance Polars-Native Master Instrument Token Processor for Indian Exchanges.
    Filters derivative contracts across NFO, CDS, MCX, BFO, BSE, matches with aug_table,
    resolves active futures and indexes via BrokerTokenMapper, and assembles the target cum_table.

    Returns:
    --------
    (cum_table, inst_list_int, init_ref_list, index_ref_list)
    """
    if not aug_table.is_empty() and "Capital_share" in aug_table.columns:
        aug_table = aug_table.filter(
            pl.col("Capital_share").cast(pl.Float64, strict=False).fill_null(0.0) > 0
        )
    if aug_table.is_empty():
        return pl.DataFrame(), np.array([], dtype=np.int64), pl.DataFrame(schema={"instrument_token": pl.Int64}), pl.DataFrame(schema={"instrument_token": pl.Int64})

    # Extract candidate underlying and index symbols from active aug_table
    active_sym_set = set()
    for col in ["Symbol", "Stock", "Ref_stock", "Nifty_index"]:
        if col in aug_table.columns:
            for s in aug_table[col].drop_nulls().to_list():
                clean_s = str(s).strip().upper()
                if clean_s:
                    active_sym_set.add(clean_s)

    # Standard benchmark indices and exchange reference roots
    BENCHMARK_INDEX_ROOTS = {
        "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX",
        "NIFTY 50", "NIFTY BANK", "NIFTY FIN SERVICE", "BSE SENSEX",
        "SENSEX50", "NIFTY_50", "NIFTY_BANK"
    }
    candidate_symbols = active_sym_set | BENCHMARK_INDEX_ROOTS

    # Filter raw instruments to active underlyings and benchmark indices to prevent memory bloat
    filtered_raw_instruments: Dict[str, pl.DataFrame] = {}
    for exch_k, df_k in raw_instruments.items():
        if df_k is not None and not df_k.is_empty():
            if candidate_symbols and "name" in df_k.columns:
                cond = (
                    pl.col("name").is_in(candidate_symbols)
                    | (pl.col("tradingsymbol").is_in(candidate_symbols) if "tradingsymbol" in df_k.columns else pl.lit(False))
                )
                if "segment" in df_k.columns:
                    cond = cond | pl.col("segment").cast(pl.Utf8).str.contains("INDEX")
                if "instrument_type" in df_k.columns:
                    cond = cond | (pl.col("instrument_type") == "INDEX")
                f_df = df_k.filter(cond)
            else:
                f_df = df_k
            filtered_raw_instruments[exch_k] = f_df
            if not f_df.is_empty():
                token_mapper.register_broker_instruments(broker_name, f_df)
        else:
            filtered_raw_instruments[exch_k] = pl.DataFrame()

    # 1. Combine Derivatives (NFO, CDS, MCX, BFO)
    der_tables = [
        filtered_raw_instruments[k]
        for k in ["NFO", "CDS", "MCX", "BFO"]
        if k in filtered_raw_instruments and filtered_raw_instruments[k] is not None and not filtered_raw_instruments[k].is_empty()
    ]

    if der_tables:
        nfo_cds_mcx = pl.concat(der_tables, how="diagonal")
    else:
        nfo_cds_mcx = pl.DataFrame(schema={
            "instrument_token": pl.Int64,
            "tradingsymbol": pl.Utf8,
            "name": pl.Utf8,
            "expiry": pl.Utf8,
            "segment": pl.Utf8,
            "instrument_type": pl.Utf8,
            "exchange": pl.Utf8,
            "strike": pl.Float64,
            "lot_size": pl.Int64,
            "tick_size": pl.Float64,
        })

    # 2. Filter valid derivative symbols & classify caps
    if not nfo_cds_mcx.is_empty():
        nfo_cds_mcx = nfo_cds_mcx.filter(
            pl.col("name").is_not_null()
            & (pl.col("name").cast(pl.Utf8) != "")
            & (pl.col("name").cast(pl.Utf8) != "nan")
            & pl.col("tradingsymbol").str.contains(pl.col("name"), literal=True)
        )

        now_dt = datetime.today()
        exp_date_col = pl.col("expiry").str.to_date(strict=False)
        exp_days = (exp_date_col - pl.lit(now_dt.date())).dt.total_days()
        exp_month = exp_date_col.dt.month()

        nfo_cds_mcx = nfo_cds_mcx.with_columns([
            exp_days.alias("exp_date_list"),
            exp_month.alias("exp_month"),
        ])

        # Positional classification
        name_len_plus2 = pl.col("name").str.len_chars() + 2
        match_str = pl.col("tradingsymbol").str.slice(name_len_plus2, 3)
        match_str1 = pl.col("tradingsymbol").str.slice(name_len_plus2)

        is_digit = match_str.str.contains(r"^\d+$")
        is_alpha = match_str.str.contains(r"^[A-Za-z]+$")
        is_alpha1 = match_str1.str.contains(r"^[A-Za-z]+$")
        is_opt = pl.col("segment").cast(pl.Utf8).str.contains("-OPT") | pl.col("instrument_type").is_in(["CE", "PE"])
        is_fut = pl.col("segment").cast(pl.Utf8).str.contains("-FUT") | (pl.col("instrument_type") == "FUT")

        cap_expr = (
            pl.when(is_digit & ~is_alpha & is_opt).then(pl.lit("weekely_options"))
            .when(is_alpha & is_opt).then(pl.lit("monthly_options"))
            .when(is_alpha & is_alpha1 & is_fut).then(pl.lit("monthly_futures"))
            .when(is_digit & ~is_alpha & is_fut).then(pl.lit("weekely_futures"))
            .otherwise(None)
        )
        nfo_cds_mcx = nfo_cds_mcx.with_columns(cap_expr.alias("cap"))

    # 3. Token-to-symbol dictionary across all exchanges
    sym_tkn_dict: Dict[str, int] = {}
    for df_inst in [nfo_cds_mcx, filtered_raw_instruments.get("NSE"), filtered_raw_instruments.get("BSE")]:
        if df_inst is not None and not df_inst.is_empty() and "tradingsymbol" in df_inst.columns and "instrument_token" in df_inst.columns:
            for ts, tkn in zip(df_inst["tradingsymbol"].to_list(), df_inst["instrument_token"].to_list()):
                if ts and tkn is not None:
                    sym_tkn_dict[str(ts)] = int(tkn)

    # 4. Resolve Ref_stock, Ref_stock_tkn, and Index_tkn for aug_table
    resolved_ref_stock = []
    resolved_ref_tkn = []
    resolved_idx_tkn = []

    for row in aug_table.to_dicts():
        sym = str(row.get("Symbol", ""))
        ref_sym = str(row.get("Ref_stock", sym))
        idx_sym = str(row.get("Nifty_index", sym))

        cds_fut_all = (
            nfo_cds_mcx.filter(
                (pl.col("name") == ref_sym)
                & (pl.col("instrument_type") == "FUT")
                & (pl.col("cap") == "monthly_futures")
            )
            if not nfo_cds_mcx.is_empty()
            else pl.DataFrame()
        )

        if cds_fut_all.is_empty() and not nfo_cds_mcx.is_empty():
            cds_fut_all = nfo_cds_mcx.filter(
                (pl.col("name") == ref_sym) & (pl.col("instrument_type") == "FUT")
            )

        cds_fut = (
            cds_fut_all.filter(pl.col("exp_date_list") > month_cutoff)
            if not cds_fut_all.is_empty()
            else pl.DataFrame()
        )
        if cds_fut.is_empty() and not cds_fut_all.is_empty():
            cds_fut = cds_fut_all.filter(pl.col("exp_date_list") >= month_cutoff)
        if cds_fut.is_empty():
            cds_fut = cds_fut_all

        cds_opt_list = (
            nfo_cds_mcx.filter((pl.col("name") == ref_sym) & (pl.col("cap") == "monthly_options"))
            if not nfo_cds_mcx.is_empty()
            else pl.DataFrame()
        )

        cds_curr_month = None
        if not cds_opt_list.is_empty():
            valid_opt = cds_opt_list.filter(pl.col("exp_month") > month_cutoff)
            if not valid_opt.is_empty() and "exp_date_list" in valid_opt.columns:
                min_exp_days = valid_opt["exp_date_list"].min()
                min_date_opt = valid_opt.filter(pl.col("exp_date_list") == min_exp_days)
                if not min_date_opt.is_empty() and "exp_month" in min_date_opt.columns:
                    cds_curr_month = min_date_opt["exp_month"].min()
            if cds_curr_month is None and "exp_month" in cds_opt_list.columns:
                cds_curr_month = cds_opt_list["exp_month"].min()

        cds_ref_info = None
        if not cds_fut.is_empty():
            if cds_curr_month is not None and "exp_month" in cds_fut.columns:
                mask_curr = cds_fut.filter(pl.col("exp_month") == cds_curr_month)
                if not mask_curr.is_empty():
                    cds_ref_info = str(mask_curr["tradingsymbol"][0])
                else:
                    today_date = datetime.today().date()
                    if "exp_date_list" in cds_fut.columns and "expiry" in cds_fut.columns:
                        future_only = cds_fut.filter(
                            pl.col("expiry").str.to_date(strict=False) > pl.lit(today_date)
                        )
                        if not future_only.is_empty():
                            min_exp = future_only["exp_date_list"].min()
                            mask_next = future_only.filter(pl.col("exp_date_list") == min_exp)
                            if not mask_next.is_empty():
                                cds_ref_info = str(mask_next["tradingsymbol"][0])

            if cds_ref_info is None:
                sorted_fut = cds_fut.sort("expiry") if "expiry" in cds_fut.columns else cds_fut
                cds_ref_info = str(sorted_fut["tradingsymbol"][0])
        else:
            cds_ref_info = ref_sym

        # Exchange_type check: Only NSE/BSE derivative exchanges carry an external cash index.
        # MCX, CDS, etc. use the underlying futures contract itself as the reference index.
        row_exchange_type = str(row.get("Exchange_type", "") or "").strip().lower()
        NSE_BSE_EXCHANGES = {"nfo", "bfo", "nse", "bse", "nse_fo", "bse_fo"}
        uses_external_index = (row_exchange_type in NSE_BSE_EXCHANGES)

        if not uses_external_index and not cds_fut.is_empty() and "exchange" in cds_fut.columns:
            fut_exchanges = set(cds_fut["exchange"].drop_nulls().to_list())
            uses_external_index = bool(fut_exchanges.intersection({"NSE", "BSE", "NFO", "BFO"}))

        if uses_external_index:
            index_symbol = idx_sym
        else:
            index_symbol = cds_ref_info

        ref_token = sym_tkn_dict.get(cds_ref_info, -1)

        # Multi-broker index token resolution via BrokerTokenMapper
        idx_token = sym_tkn_dict.get(index_symbol, -1)
        if idx_token == -1:
            # Try broker-normalized index symbol (e.g. 'NIFTY 50' -> 'NIFTY' on Kotak Neo)
            norm_sym = BrokerTokenMapper.normalize_index_symbol(index_symbol, target_broker=broker_name)
            idx_token = sym_tkn_dict.get(norm_sym, -1)
        if idx_token == -1:
            idx_token = token_mapper.get_index_token(index_symbol, broker_name=broker_name)
        if idx_token == -1:
            idx_token = ref_token

        resolved_ref_stock.append(cds_ref_info)
        resolved_ref_tkn.append(ref_token)
        resolved_idx_tkn.append(idx_token)

    aug_table = aug_table.with_columns([
        pl.Series("Ref_stock", resolved_ref_stock, dtype=pl.Utf8),
        pl.Series("Ref_stock_tkn", resolved_ref_tkn, dtype=pl.Int64),
        pl.Series("Index_tkn", resolved_idx_tkn, dtype=pl.Int64),
    ])

    # 5. Assemble cum_table using Polars join
    if not nfo_cds_mcx.is_empty() and not aug_table.is_empty():
        aug_stocks = aug_table["Symbol"].to_list() if "Symbol" in aug_table.columns else []
        matched_nfo = nfo_cds_mcx.filter(pl.col("name").is_in(aug_stocks))

        if not matched_nfo.is_empty():
            cum_joined = matched_nfo.join(aug_table, left_on="name", right_on="Symbol", how="left")
        else:
            cum_joined = aug_table

        now_9am = now_ist_naive(tz=tz).replace(hour=9, minute=0, second=0, microsecond=0)

        cum_joined = cum_joined.with_columns([
            pl.lit("No").alias("current_month"),
            pl.lit(0).alias("CE_jump"),
            pl.lit(0).alias("PE_jump"),
            pl.lit(0).alias("day_fall"),
            pl.lit(0).alias("day_rise"),
            pl.lit(0).alias("buy_signal_PE"),
            pl.lit(0).alias("buy_signal_CE"),
            pl.lit(0.0).alias("current_value"),
            pl.lit("NA").alias("ATM_ITM_OTM"),
            pl.lit("NA").alias("Buy_strike"),
            pl.lit(now_9am).alias("recent_sell_order_time"),
            pl.lit(now_9am).alias("recent_buy_order_time"),
        ])

        if "expiry" in cum_joined.columns:
            exp_date_s = cum_joined["expiry"].str.to_date(strict=False)
            week_dist = exp_date_s.dt.week() - datetime.today().isocalendar()[1]
            month_dist = exp_date_s.dt.month() - datetime.today().month
            cum_joined = cum_joined.with_columns([
                week_dist.alias("week_dist"),
                month_dist.alias("month_dist"),
            ])

        if "instrument_token" in cum_joined.columns:
            cum_joined = cum_joined.unique(subset=["instrument_token"])

        # Compute front monthly expiry current_month: 'Yes'
        if all(c in cum_joined.columns for c in ["exp_date_list", "cap", "Ref_stock"]):
            ref_expiry_tbl = (
                cum_joined.filter(pl.col("exp_date_list") > month_cutoff)
                .filter(pl.col("cap") == "monthly_options")
                .group_by("Ref_stock")
                .agg(pl.col("exp_date_list").min().alias("_min_exp"))
            )
            cum_joined = cum_joined.join(ref_expiry_tbl, on="Ref_stock", how="left")
            cum_joined = cum_joined.with_columns(
                pl.when(
                    (pl.col("exp_date_list") >= month_cutoff)
                    & pl.col("_min_exp").is_not_null()
                    & (pl.col("exp_date_list") <= pl.col("_min_exp"))
                )
                .then(pl.lit("Yes"))
                .otherwise(pl.lit("No"))
                .alias("current_month")
            ).drop("_min_exp")

        # Join cap_config
        if not cap_config.is_empty() and "cap" in cum_joined.columns and "cap" in cap_config.columns:
            cap_cols_to_join = [c for c in cap_config.columns if c != "cap"]
            if cap_cols_to_join:
                cum_joined = cum_joined.join(
                    cap_config.select(["cap"] + cap_cols_to_join), on="cap", how="left"
                )
        # Ensure cap_config numeric columns have guaranteed dtypes
        for _int_col in ["tradable", "minimum_lots_to_buy", "maximum_lots_to_buy", "preference"]:
            if _int_col in cum_joined.columns:
                cum_joined = cum_joined.with_columns(
                    pl.col(_int_col).cast(pl.Float64, strict=False).fill_null(1.0).cast(pl.Int64).alias(_int_col)
                )
            else:
                cum_joined = cum_joined.with_columns(pl.lit(1).alias(_int_col))

        for _flt_col in ["lower_price_limit", "upper_price_limit"]:
            if _flt_col in cum_joined.columns:
                cum_joined = cum_joined.with_columns(
                    pl.col(_flt_col).cast(pl.Float64, strict=False).fill_null(-1.0).alias(_flt_col)
                )
            else:
                cum_joined = cum_joined.with_columns(pl.lit(-1.0).alias(_flt_col))

        canon_expr = (
            pl.when(pl.col("instrument_type").is_in(["CE", "PE"]))
            .then(
                pl.concat_str([
                    pl.lit("OPT:"),
                    pl.col("name").cast(pl.Utf8),
                    pl.lit(":"),
                    pl.col("expiry").cast(pl.Utf8),
                    pl.lit(":"),
                    pl.col("strike").cast(pl.Float64).round(2).cast(pl.Utf8),
                    pl.lit(":"),
                    pl.col("instrument_type").cast(pl.Utf8),
                ])
            )
            .when(pl.col("instrument_type") == "FUT")
            .then(
                pl.concat_str([
                    pl.lit("FUT:"),
                    pl.col("name").cast(pl.Utf8),
                    pl.lit(":"),
                    pl.col("expiry").cast(pl.Utf8),
                ])
            )
            .otherwise(
                pl.concat_str([
                    pl.lit("EQ:"),
                    pl.col("tradingsymbol").cast(pl.Utf8),
                ])
            )
        )
        cum_joined = cum_joined.with_columns(canon_expr.alias("canonical_key"))

        cum_table = cum_joined
    else:
        cum_table = aug_table

    # 6. Extract token references
    if not cum_table.is_empty() and "Ref_stock_tkn" in cum_table.columns:
        cum_table = cum_table.filter(pl.col("Ref_stock_tkn") != -1)
        inst_list_int = (
            np.array(cum_table["instrument_token"].drop_nulls().unique().to_list(), dtype=np.int64)
            if "instrument_token" in cum_table.columns
            else np.array([], dtype=np.int64)
        )
        active_cum = (
            cum_table.filter(pl.col("Capital_share").cast(pl.Float64, strict=False).fill_null(0.0) > 0)
            if "Capital_share" in cum_table.columns
            else cum_table
        )
        init_ref_list = (
            active_cum.filter(pl.col("Ref_stock_tkn") > 0)
            .select(pl.col("Ref_stock_tkn").alias("instrument_token"))
            .unique()
            if "Ref_stock_tkn" in active_cum.columns
            else pl.DataFrame(schema={"instrument_token": pl.Int64})
        )
        index_ref_list = (
            active_cum.filter(pl.col("Index_tkn") > 0)
            .select(pl.col("Index_tkn").alias("instrument_token"))
            .unique()
            if "Index_tkn" in active_cum.columns
            else pl.DataFrame(schema={"instrument_token": pl.Int64})
        )
    else:
        inst_list_int = np.array([], dtype=np.int64)
        init_ref_list = pl.DataFrame(schema={"instrument_token": pl.Int64})
        index_ref_list = pl.DataFrame(schema={"instrument_token": pl.Int64})

    logger.info(
        "Assembled Indian cum_table: %d rows, %d tokens, %d ref tokens.",
        len(cum_table),
        len(inst_list_int),
        len(init_ref_list),
    )

    return cum_table, inst_list_int, init_ref_list, index_ref_list
