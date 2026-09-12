# -*- coding: utf-8 -*-
"""
algo_trading/algos/crypto_master_tokens.py
──────────────────────────────────────────
Pure Polars master token list and cum_table assembler for Crypto exchanges
(CoinSwitch PRO, Delta Exchange Global & India).
24/7 continuous trading format: Perpetual futures, spot pairs, USDT options.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import polars as pl

from algo_trading.algos.indian_master_tokens import today_ist, now_ist_naive
from algo_trading.algos.broker_token_mapper import clean_crypto_root, token_mapper

logger = logging.getLogger("algo_trading.algos.crypto_master_tokens")


def str_to_token(s: str) -> int:
    """Deterministic hash mapping from contract symbol string to 64-bit int token."""
    import hashlib
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:12], 16) % (10**9)


def normalize_crypto_instruments(raw_list: List[Dict[str, Any]]) -> pl.DataFrame:
    """Clean and normalize raw crypto contract dicts into canonical Polars DataFrame."""
    if not raw_list:
        return pl.DataFrame(schema={
            "instrument_token": pl.Int64,
            "exchange_token": pl.Int64,
            "tradingsymbol": pl.Utf8,
            "name": pl.Utf8,
            "last_price": pl.Float64,
            "expiry": pl.Utf8,
            "strike": pl.Float64,
            "tick_size": pl.Float64,
            "lot_size": pl.Float64,
            "instrument_type": pl.Utf8,
            "segment": pl.Utf8,
            "exchange": pl.Utf8,
            "max_leverage": pl.Int64,
            "status": pl.Utf8,
        })

    clean_rows = []
    for r in raw_list:
        if not isinstance(r, dict):
            continue
        sym = str(r.get("tradingsymbol") or r.get("symbol") or "").strip()
        if not sym:
            continue

        # Extract underlying base asset symbol
        underlying = r.get("underlying_asset")
        if isinstance(underlying, dict):
            name = str(underlying.get("symbol") or "").upper()
        elif isinstance(underlying, str) and underlying.strip():
            name = underlying.strip().upper()
        else:
            name = str(r.get("base_asset") or "").strip().upper()

        if not name or name in ("C", "P") or "-" in name or "/" in name or "_" in name:
            name = clean_crypto_root(sym) or clean_crypto_root(str(r.get("name") or ""))

        exch = str(r.get("exchange") or "crypto")
        contract_type = str(r.get("contract_type") or "").lower()

        # Segment classification
        if "option" in contract_type or sym.startswith(("C-", "P-")) or sym.endswith(("-C", "-P")) or any(x in sym for x in ("-C-", "-P-", "-CE", "-PE")):
            seg = "OPTIONS"
        elif "spot" in contract_type or ("_" in sym and not any(x in sym for x in ("MAR", "JUN", "SEP", "DEC", "202"))):
            seg = "SPOT"
        elif "fut" in contract_type or exch.upper() == "EXCHANGE_2" or "USDT" in sym:
            seg = "FUTURES"
        else:
            seg = str(r.get("segment") or "SPOT")

        # Instrument type classification
        if contract_type == "call_options" or sym.startswith("C-") or sym.endswith(("-C", "-CE")) or "-C-" in sym:
            inst_type = "CE"
        elif contract_type == "put_options" or sym.startswith("P-") or sym.endswith(("-P", "-PE")) or "-P-" in sym:
            inst_type = "PE"
        elif seg == "FUTURES":
            inst_type = "FUT"
        else:
            inst_type = "EQ"

        tkn_val = r.get("instrument_token") or r.get("exchange_token") or r.get("id")
        if tkn_val is not None:
            try:
                tkn = int(tkn_val)
            except (ValueError, TypeError):
                tkn = str_to_token(sym)
        else:
            tkn = str_to_token(sym)

        try:
            lot_size = float(r.get("lot_size") or 1.0)
        except Exception:
            lot_size = 1.0
        try:
            tick_size = float(r.get("tick_size") or 0.01)
        except Exception:
            tick_size = 0.01
        try:
            max_lev = int(r.get("max_leverage") or 1)
        except Exception:
            max_lev = 1

        strike_raw = r.get("strike") or r.get("strike_price") or 0.0
        try:
            strike = float(strike_raw)
        except Exception:
            strike = 0.0

        expiry = str(r.get("expiry") or "")
        if not expiry and "settlement_time" in r and r["settlement_time"]:
            try:
                raw_st = str(r["settlement_time"]).split("T")[0]
                expiry = raw_st
            except Exception:
                pass

        clean_rows.append({
            "instrument_token": tkn,
            "exchange_token": tkn,
            "tradingsymbol": sym,
            "name": name,
            "last_price": 0.0,
            "expiry": expiry,
            "strike": strike,
            "tick_size": tick_size,
            "lot_size": lot_size,
            "instrument_type": inst_type,
            "segment": seg,
            "exchange": exch,
            "max_leverage": max_lev,
            "status": str(r.get("status") or "TRADING"),
        })

    return pl.from_dicts(clean_rows, infer_schema_length=None) if clean_rows else pl.DataFrame()


KNOWN_CRYPTO_BASE_PRICES: Dict[str, float] = {
    "BTC": 85000.0,
    "ETH": 3000.0,
    "SOL": 130.0,
    "XRP": 1.5,
    "BNB": 600.0,
    "DOGE": 0.15,
    "ADA": 0.5,
    "AVAX": 25.0,
    "LINK": 15.0,
}


def generate_synthetic_crypto_options(
    sym: str,
    base_price: float,
    expiries: List[str],
    exchange: str = "DELTA",
) -> pl.DataFrame:
    """
    Generate synthetic CE and PE option contracts with standard strike grids
    centered around base_price for crypto underlyings that lack exchange-listed
    options (e.g., SOL, XRP on Delta Exchange).
    """
    clean_s = clean_crypto_root(sym)
    if base_price >= 1000:
        step = 500.0
    elif base_price >= 100:
        step = 5.0
    elif base_price >= 10:
        step = 0.5
    elif base_price >= 1.0:
        step = 0.05
    else:
        step = 0.01

    center_strike = round(base_price / step) * step
    strikes = [round(center_strike + i * step, 4) for i in range(-5, 6)]

    rows = []
    now_dt = datetime.today()
    for exp_str in expiries:
        try:
            exp_dt = datetime.strptime(str(exp_str).strip(), "%Y-%m-%d")
        except Exception:
            continue
        exp_days = (exp_dt.date() - now_dt.date()).days
        if exp_days <= 1:
            continue
        exp_code = exp_dt.strftime("%d%m%y")
        for st in strikes:
            st_int = int(st) if st == int(st) else st
            for itype in ["CE", "PE"]:
                sym_code = f"{itype[0]}-{clean_s}-{st_int}-{exp_code}"
                tkn_id = abs(hash(sym_code)) % 10000000 + 100000
                rows.append({
                    "instrument_token": tkn_id,
                    "exchange_token": tkn_id,
                    "tradingsymbol": sym_code,
                    "name": clean_s,
                    "last_price": 0.0,
                    "expiry": exp_str,
                    "strike": float(st),
                    "tick_size": 0.01,
                    "lot_size": 1.0,
                    "instrument_type": itype,
                    "segment": "OPTIONS",
                    "exchange": exchange,
                    "max_leverage": 10,
                    "status": "TRADING",
                })
    return pl.from_dicts(rows) if rows else pl.DataFrame()


def assemble_crypto_cum_table(
    all_sheets_data: Dict[str, List[Dict[str, Any]]],
    aug_table: pl.DataFrame,
    cap_config: pl.DataFrame,
    month_cutoff: int = 0,
    tz: str = "Asia/Kolkata",
    broker_name: str = "delta",
) -> Tuple[pl.DataFrame, np.ndarray, pl.DataFrame, pl.DataFrame]:
    """
    High-Performance Pure Polars Master Token List and cum_table Assembler for Crypto Exchanges.
    Normalizes spot, perpetuals, and options across crypto exchanges (Delta Exchange, CoinSwitch PRO).
    """
    if not aug_table.is_empty() and "Capital_share" in aug_table.columns:
        aug_table = aug_table.filter(
            pl.col("Capital_share").cast(pl.Float64, strict=False).fill_null(0.0) > 0
        )
    if aug_table.is_empty():
        return (
            pl.DataFrame(),
            np.array([], dtype=np.int64),
            pl.DataFrame(schema={"instrument_token": pl.Int64}),
            pl.DataFrame(schema={"instrument_token": pl.Int64}),
        )

    b_key = broker_name.lower().strip()
    if "coinswitch" in b_key:
        b_key = "coinswitch"
    elif "delta" in b_key:
        b_key = "delta"

    # 1. Normalize individual sheet tables
    normalized_sheets: Dict[str, pl.DataFrame] = {}
    for k, rows in all_sheets_data.items():
        normalized_sheets[k] = normalize_crypto_instruments(rows)

    combined_dfs = [df for df in normalized_sheets.values() if not df.is_empty()]
    nfo_cds_mcx = pl.concat(combined_dfs, how="diagonal_relaxed") if combined_dfs else pl.DataFrame()

    # Ensure all underlyings configured in aug_table that require options have forward option chains (> tomorrow)
    if not aug_table.is_empty() and "Symbol" in aug_table.columns:
        aug_roots = [clean_crypto_root(s) for s in aug_table["Symbol"].to_list()]
        tomorrow_d = (datetime.today() + timedelta(days=1)).date()
        valid_forward_opts = set()
        if not nfo_cds_mcx.is_empty() and "instrument_type" in nfo_cds_mcx.columns and "expiry" in nfo_cds_mcx.columns:
            opt_sub = nfo_cds_mcx.filter(
                pl.col("instrument_type").is_in(["CE", "PE"])
                & (pl.col("expiry").str.to_date(strict=False) > pl.lit(tomorrow_d))
            )
            if not opt_sub.is_empty() and "name" in opt_sub.columns:
                valid_forward_opts = set(clean_crypto_root(s) for s in opt_sub["name"].drop_nulls().to_list())

        missing_opts = [s for s in aug_roots if s and s not in valid_forward_opts]
        if missing_opts:
            ref_expiries = [
                e for e in nfo_cds_mcx.filter(
                    pl.col("expiry").str.to_date(strict=False) > pl.lit(tomorrow_d)
                )["expiry"].drop_nulls().unique().to_list()
                if e and str(e).strip()
            ] if not nfo_cds_mcx.is_empty() and "expiry" in nfo_cds_mcx.columns else []

            if not ref_expiries:
                today_d = datetime.today().date()
                ref_expiries = []
                for d_offset in range(1, 35):
                    cur = today_d + timedelta(days=d_offset)
                    if cur.weekday() == 4:
                        ref_expiries.append(cur.strftime("%Y-%m-%d"))

            synth_dfs = []
            for m_sym in missing_opts:
                bp = KNOWN_CRYPTO_BASE_PRICES.get(m_sym, 100.0)
                for s_df in [normalized_sheets.get("c2c1"), normalized_sheets.get("FUTURES")]:
                    if s_df is not None and not s_df.is_empty() and "name" in s_df.columns:
                        cand_s = s_df.filter(pl.col("name") == m_sym)
                        if not cand_s.is_empty() and "last_price" in cand_s.columns:
                            px = cand_s["last_price"][0]
                            if px and px > 0:
                                bp = float(px)
                                break
                synth_df = generate_synthetic_crypto_options(m_sym, bp, ref_expiries, exchange=b_key.upper())
                if not synth_df.is_empty():
                    synth_dfs.append(synth_df)
                    logger.info("Generated %d synthetic option contracts for %s (%s, BasePrice=%.2f).", synth_df.height, m_sym, b_key, bp)

            if synth_dfs:
                combined_dfs.extend(synth_dfs)
                nfo_cds_mcx = pl.concat(combined_dfs, how="diagonal_relaxed")

    if not nfo_cds_mcx.is_empty():
        now_dt = datetime.today()
        exp_date_col = pl.col("expiry").str.to_date(strict=False)
        exp_days = (exp_date_col - pl.lit(now_dt.date())).dt.total_days()
        exp_month = exp_date_col.dt.month()

        nfo_cds_mcx = nfo_cds_mcx.with_columns([
            exp_days.alias("exp_date_list"),
            exp_month.alias("exp_month"),
        ])

        is_opt = pl.col("segment") == "OPTIONS"
        is_fut = pl.col("segment") == "FUTURES"
        is_spot = pl.col("segment") == "SPOT"

        cap_expr = (
            pl.when(is_opt & (pl.col("exp_date_list") > 15)).then(pl.lit("monthly_options"))
            .when(is_opt).then(pl.lit("weekely_options"))
            .when(is_fut).then(pl.lit("crypto_perp_futures"))
            .when(is_spot).then(pl.lit("crypto_spot"))
            .otherwise(pl.lit("crypto"))
        )
        nfo_cds_mcx = nfo_cds_mcx.with_columns(cap_expr.alias("cap"))

        # Extract candidate active underlying symbols from aug_table
        active_crypto_candidates = set()
        if not aug_table.is_empty():
            for col in ["Symbol", "Stock", "Ref_stock", "Nifty_index"]:
                if col in aug_table.columns:
                    for s in aug_table[col].drop_nulls().to_list():
                        raw_s = str(s).strip().upper()
                        if raw_s:
                            active_crypto_candidates.add(raw_s)
                            root = clean_crypto_root(raw_s)
                            if root:
                                active_crypto_candidates.add(root)
                                active_crypto_candidates.add(f"{root}USD")
                                active_crypto_candidates.add(f"{root}USDT")
                                active_crypto_candidates.add(f"{root}_USDT")
                                active_crypto_candidates.add(f"{root}/USDT")
                                active_crypto_candidates.add(f"{root}/INR")

        # Register instruments into global BrokerTokenMapper (filtered to active underlyings)
        try:
            if active_crypto_candidates and not nfo_cds_mcx.is_empty():
                inst_to_register = nfo_cds_mcx.filter(
                    pl.col("name").is_in(active_crypto_candidates)
                    | pl.col("tradingsymbol").is_in(active_crypto_candidates)
                )
            else:
                inst_to_register = nfo_cds_mcx
            if not inst_to_register.is_empty():
                token_mapper.register_broker_instruments(b_key, inst_to_register)
        except Exception as ex:
            logger.debug("Crypto BrokerTokenMapper registration: %s", ex)

    # 2. Resolve Ref_stock, Ref_stock_tkn, and Index_tkn for aug_table
    resolved_ref_stock = []
    resolved_ref_tkn = []
    resolved_idx_tkn = []

    c2c1_inst = normalized_sheets.get("c2c1", pl.DataFrame())
    fut_inst = normalized_sheets.get("FUTURES", pl.DataFrame())

    for row in aug_table.to_dicts():
        sym = str(row.get("Symbol", ""))
        ref_sym = str(row.get("Ref_stock", "") or "")
        idx_sym = str(row.get("Nifty_index", "") or "")

        c_root = clean_crypto_root(sym) or clean_crypto_root(ref_sym)

        # Structured candidate matching based on broker convention
        if b_key == "delta":
            candidates = [f"{c_root}USD", f"{c_root}USDT", f"{c_root}_USDT", c_root]
            if ref_sym:
                c_cand = clean_crypto_root(ref_sym)
                if c_cand not in candidates:
                    candidates.extend([f"{c_cand}USD", f"{c_cand}USDT", c_cand])
        else:  # coinswitch
            candidates = [f"{c_root}/USDT", f"{c_root}USDT", f"{c_root}/INR", c_root]
            if ref_sym and ref_sym not in candidates:
                candidates.insert(0, ref_sym)

        # Search for underlying futures or spot contract across sheets
        match_df = pl.DataFrame()
        search_sheets = [fut_inst, c2c1_inst, nfo_cds_mcx]
        for sheet in search_sheets:
            if sheet is not None and not sheet.is_empty():
                for cand in candidates:
                    m = sheet.filter((pl.col("tradingsymbol") == cand) | ((pl.col("name") == c_root) & (pl.col("instrument_type") == "FUT")))
                    if not m.is_empty():
                        match_df = m
                        break
                if not match_df.is_empty():
                    break

        if not match_df.is_empty():
            ref_stock_name = str(match_df["tradingsymbol"][0])
            ref_tkn = int(match_df["instrument_token"][0])
        else:
            ref_stock_name = f"{c_root}USD" if b_key == "delta" else f"{c_root}/USDT"
            ref_tkn = str_to_token(ref_stock_name)

        # Index token resolution
        idx_tkn = ref_tkn
        if idx_sym:
            clean_idx = clean_crypto_root(idx_sym)
            idx_candidates = [f"{clean_idx}USD", f"{clean_idx}/USDT", f"{clean_idx}USDT", clean_idx]
            for sheet in search_sheets:
                if sheet is not None and not sheet.is_empty():
                    for cand in idx_candidates:
                        m = sheet.filter(pl.col("tradingsymbol") == cand)
                        if not m.is_empty():
                            idx_tkn = int(m["instrument_token"][0])
                            break
                    if idx_tkn != ref_tkn:
                        break

        resolved_ref_stock.append(ref_stock_name)
        resolved_ref_tkn.append(ref_tkn)
        resolved_idx_tkn.append(idx_tkn)

    aug_table = aug_table.with_columns([
        pl.Series("Ref_stock", resolved_ref_stock, dtype=pl.Utf8),
        pl.Series("Ref_stock_tkn", resolved_ref_tkn, dtype=pl.Int64),
        pl.Series("Index_tkn", resolved_idx_tkn, dtype=pl.Int64),
    ])

    # 3. Assemble cum_table using Polars join
    if not nfo_cds_mcx.is_empty() and not aug_table.is_empty():
        aug_stocks = aug_table["Symbol"].to_list() if "Symbol" in aug_table.columns else []
        matched_inst = nfo_cds_mcx.filter(pl.col("name").is_in(aug_stocks))

        if not matched_inst.is_empty():
            drop_cols = [c for c in [
                "instrument_token", "exchange_token", "tradingsymbol", "exchange", "segment",
                "instrument_type", "strike", "expiry", "tick_size", "lot_size", "status"
            ] if c in aug_table.columns]
            join_aug = aug_table.drop(drop_cols) if drop_cols else aug_table
            cum_joined = matched_inst.join(join_aug, left_on="name", right_on="Symbol", how="left")
        else:
            cum_joined = aug_table

        now_9am = now_ist_naive(tz=tz).replace(hour=9, minute=0, second=0, microsecond=0)
        today_date = now_9am.date()

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
            today_w = pl.lit(today_date).dt.truncate("1w")
            week_dist = ((exp_date_s.dt.truncate("1w") - today_w).dt.total_days().fill_null(0) // 7).cast(pl.Int64)
            cum_joined = cum_joined.with_columns([week_dist.alias("week_dist")])

        if "instrument_token" in cum_joined.columns:
            cum_joined = cum_joined.unique(subset=["instrument_token"])

        # Compute current_month
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

        # Attach canonical_key column
        if all(col in cum_joined.columns for col in ["instrument_type", "name", "expiry", "strike"]):
            cum_joined = cum_joined.with_columns(
                pl.struct(["instrument_type", "name", "expiry", "strike"]).map_elements(
                    lambda x: (
                        f"OPT:{clean_crypto_root(x['name'])}:{x['expiry'] or ''}:{x['strike'] or 0.0}:{x['instrument_type']}"
                        if x["instrument_type"] in ("CE", "PE")
                        else (
                            f"FUT:{clean_crypto_root(x['name'])}:{x['expiry'] or 'PERP'}"
                            if x["instrument_type"] == "FUT"
                            else f"EQ:{clean_crypto_root(x['name'])}"
                        )
                    ),
                    return_dtype=pl.Utf8,
                ).alias("canonical_key")
            )

        cum_table = cum_joined
    else:
        cum_table = aug_table

    # 4. Extract token lists strictly requiring Capital_share > 0 and positive tokens
    if not cum_table.is_empty() and "Ref_stock_tkn" in cum_table.columns:
        cum_table = cum_table.filter(pl.col("Ref_stock_tkn") > 0)
        inst_list_int = (
            np.array(cum_table["instrument_token"].drop_nulls().unique().to_list(), dtype=np.int64)
            if "instrument_token" in cum_table.columns
            else np.array([], dtype=np.int64)
        )
        active_cum = (
            cum_table.filter((pl.col("Capital_share") > 0) & (pl.col("Ref_stock_tkn") > 0))
            if "Capital_share" in cum_table.columns
            else cum_table
        )
        init_ref_list = active_cum.select(pl.col("Ref_stock_tkn").alias("instrument_token")).unique()
        index_ref_list = (
            active_cum.filter(pl.col("Index_tkn") > 0).select(pl.col("Index_tkn").alias("instrument_token")).unique()
            if "Index_tkn" in active_cum.columns
            else pl.DataFrame(schema={"instrument_token": pl.Int64})
        )
    else:
        inst_list_int = np.array([], dtype=np.int64)
        init_ref_list = pl.DataFrame(schema={"instrument_token": pl.Int64})
        index_ref_list = pl.DataFrame(schema={"instrument_token": pl.Int64})

    logger.info(
        "Assembled Crypto cum_table for '%s': %d rows, %d tokens, %d ref tokens.",
        b_key, len(cum_table), len(inst_list_int), len(init_ref_list)
    )

    return cum_table, inst_list_int, init_ref_list, index_ref_list
