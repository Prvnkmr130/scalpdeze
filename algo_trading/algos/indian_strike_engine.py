# -*- coding: utf-8 -*-
"""
algo_trading/algos/indian_strike_engine.py
──────────────────────────────────────────
Optimized, Polars-native strike detection engine for Indian options contracts.
Identifies ATM/OTM CE and PE strikes relative to moving candle averages and price jumps.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

import numpy as np
import polars as pl

from algo_trading.algos.indian_market_session import tomorrow_ist

logger = logging.getLogger("algo_trading.algos.indian_strike_engine")


def detect_strike_side(
    cum_table: pl.DataFrame,
    ref_stock_name: str,
    inst_type: str,
    cap_info: str,
    strike_dist: float,
    candle_avg: float,
    month_cutoff: int = 0,
    tz: str = "Asia/Kolkata",
) -> pl.DataFrame:
    """
    Selects the optimal active strike for a single instrument side (CE or PE).
    Returns updated cum_table with Buy_strike and Tradable_stock flags set.
    """
    if candle_avg is None or np.isnan(candle_avg) or candle_avg <= 0:
        return cum_table
    est_strike = float(candle_avg + strike_dist)
    if est_strike <= 0:
        return cum_table

    candidates = cum_table.filter(
        (pl.col("Ref_stock") == ref_stock_name)
        & (pl.col("instrument_type") == inst_type)
        & (pl.col("cap") == cap_info)
    )
    if candidates.is_empty():
        return cum_table

    # Expiry filters
    expiry_cutoff = pl.lit(tomorrow_ist(tz=tz))
    candidates = candidates.filter(
        (pl.col("expiry").str.to_date(strict=False) > expiry_cutoff)
        & (pl.col("exp_date_list") > month_cutoff)
    )
    if candidates.is_empty():
        return cum_table

    min_week = candidates["week_dist"].min()
    candidates = candidates.filter(pl.col("week_dist") == min_week)
    if candidates.is_empty():
        return cum_table

    # Compute strike distance ratio
    dist_calc = candidates.with_columns(((pl.col("strike") / est_strike).round(6)).alias("ratio"))
    if inst_type == "CE":
        selected = dist_calc.filter(pl.col("ratio") >= 1.0).sort("ratio", descending=False).head(1)
    else:
        selected = dist_calc.filter(pl.col("ratio") <= 1.0).sort("ratio", descending=True).head(1)

    if not selected.is_empty():
        sel_tkn = selected["instrument_token"][0]
        sel_sym = selected["tradingsymbol"][0]
        sel_strike = float(selected["strike"][0])

        tradable_expr = pl.col("Tradable_stock") if "Tradable_stock" in cum_table.columns else pl.lit("No")
        sel_tkn_str = str(sel_tkn).strip()
        cum_table = cum_table.with_columns(
            pl.when(pl.col("instrument_token").cast(pl.Utf8) == sel_tkn_str).then(pl.lit("Yes")).otherwise(pl.col("Buy_strike")).alias("Buy_strike"),
            pl.when(pl.col("instrument_token").cast(pl.Utf8) == sel_tkn_str).then(pl.lit("Yes")).otherwise(tradable_expr).alias("Tradable_stock"),
        )
        logger.info("[%s] %s Strike Selected: %s (EstStrike=%.2f, Strike=%.2f)", ref_stock_name, inst_type, sel_sym, est_strike, sel_strike)
    else:
        logger.debug("[%s] %s: No strike selected (EstStrike=%.2f, Candidates=%d)", ref_stock_name, inst_type, est_strike, len(candidates))

    return cum_table


def strike_detect(
    cum_table: pl.DataFrame,
    tick_data: pl.DataFrame,
    fwd_3_all: pl.DataFrame,
    month_cutoff: int = 0,
    tz: str = "Asia/Kolkata",
) -> pl.DataFrame:
    """
    Optimized Strike Detection Engine in Polars.
    Scans all target reference stocks, computes candle averages, and selects
    front unexpired weekly/monthly options strikes.
    """
    if cum_table.is_empty():
        logger.warning("[STRIKE_DETECT] Skipping strike detection: cum_table is empty.")
        return cum_table
    if "Ref_stock_tkn" not in cum_table.columns:
        logger.warning("[STRIKE_DETECT] Skipping strike detection: 'Ref_stock_tkn' column missing in cum_table.")
        return cum_table

    # Reset flags
    cum_table = cum_table.with_columns([
        pl.lit("NA").alias("Buy_strike"),
        pl.lit("No").alias("Tradable_stock"),
    ])

    ref_tkns = cum_table["Ref_stock_tkn"].drop_nulls().unique().to_list()
    all_ref_tkns = [t for t in ref_tkns if t is not None and str(t).strip() not in ("-1", "")]

    logger.info(
        "[STRIKE_DETECT] Executing strike detection for %d underlyings across %d ticks...",
        len(all_ref_tkns),
        len(tick_data),
    )

    for ref_tkn in all_ref_tkns:
        ref_tkn_str = str(ref_tkn).strip()
        if not ref_tkn_str or ref_tkn_str == "-1":
            continue

        matching_ref = cum_table.filter(pl.col("Ref_stock_tkn").cast(pl.Utf8) == ref_tkn_str)
        if matching_ref.is_empty():
            continue

        # Prioritize Cap_info (user configuration), NEVER fall back to contract-level matching_ref['cap'][0]
        cap_info = matching_ref["Cap_info"][0] if ("Cap_info" in matching_ref.columns and matching_ref["Cap_info"][0] is not None) else "monthly_options"
        ref_stock_name = str(matching_ref["Ref_stock"][0]) if "Ref_stock" in matching_ref.columns else ""
        idx_stock_name = str(matching_ref["Nifty_index"][0]) if ("Nifty_index" in matching_ref.columns and matching_ref["Nifty_index"][0] is not None) else ""
        base_sym = str(matching_ref["Symbol"][0]) if ("Symbol" in matching_ref.columns and matching_ref["Symbol"][0] is not None) else ""
        idx_tkn = matching_ref["Index_tkn"][0] if "Index_tkn" in matching_ref.columns else None

        # Alphanumeric tokens matching: match instrument_token against Ref_stock_tkn and Index_tkn
        token_candidates = [ref_tkn_str]
        if idx_tkn is not None and str(idx_tkn).strip() not in ("-1", ""):
            token_candidates.append(str(idx_tkn).strip())

        token_filter = pl.col("instrument_token").cast(pl.Utf8).is_in(token_candidates)

        # Symbol names matching: match tradingsymbol strictly against text symbol names
        symbol_candidates = [s for s in [ref_stock_name, idx_stock_name, base_sym] if s and str(s).strip()]
        if not tick_data.is_empty() and "tradingsymbol" in tick_data.columns and symbol_candidates:
            token_filter = token_filter | pl.col("tradingsymbol").is_in(symbol_candidates)

        recent_ticks = tick_data.filter(token_filter) if not tick_data.is_empty() else pl.DataFrame()

        ref_fwd_3 = fwd_3_all.filter(
            pl.col("instrument_token").cast(pl.Utf8).is_in(token_candidates)
        ) if not fwd_3_all.is_empty() and "instrument_token" in fwd_3_all.columns else pl.DataFrame()

        try:
            candle_avg = None
            if not ref_fwd_3.is_empty() and len(ref_fwd_3) >= 2:
                mean_val = (ref_fwd_3["close"][1] + ref_fwd_3["open"][1]) / 2.0
                candle_avg = float(mean_val)
            elif not ref_fwd_3.is_empty() and len(ref_fwd_3) == 1:
                mean_val = (ref_fwd_3["close"][0] + ref_fwd_3["open"][0]) / 2.0
                candle_avg = float(mean_val)

            if (candle_avg is None or np.isnan(candle_avg)) and not recent_ticks.is_empty() and "last_price" in recent_ticks.columns:
                valid_lps = recent_ticks.filter(pl.col("last_price") > 0)["last_price"]
                if not valid_lps.is_empty():
                    candle_avg = float(valid_lps[-1])

            if (candle_avg is None or np.isnan(candle_avg) or candle_avg <= 0) and "last_price" in matching_ref.columns:
                m_lp = matching_ref["last_price"][0]
                if m_lp is not None and float(m_lp) > 0:
                    candle_avg = float(m_lp)

            if candle_avg is None or np.isnan(candle_avg) or candle_avg <= 0:
                logger.warning("[%s] Unable to compute valid candle_avg (val=%s). Skipping strike detection.", ref_stock_name, candle_avg)
                continue

            strike_dist_ce = float(matching_ref["Strike_dist_CE"][0]) if ("Strike_dist_CE" in matching_ref.columns and matching_ref["Strike_dist_CE"][0] is not None) else 0.0
            strike_dist_pe = float(matching_ref["Strike_dist_PE"][0]) if ("Strike_dist_PE" in matching_ref.columns and matching_ref["Strike_dist_PE"][0] is not None) else 0.0

            cum_table = detect_strike_side(cum_table, ref_stock_name, "CE", cap_info, strike_dist_ce, candle_avg, month_cutoff, tz)
            cum_table = detect_strike_side(cum_table, ref_stock_name, "PE", cap_info, strike_dist_pe, candle_avg, month_cutoff, tz)
        except Exception as e:
            logger.error("Error in strike_detect for %s: %s", ref_stock_name, e)

    selected_count = 0
    if "Buy_strike" in cum_table.columns:
        selected_count = cum_table.filter(pl.col("Buy_strike") == "Yes").height
    logger.info("[STRIKE_DETECT] Strike detection completed: %d active strike(s) selected.", selected_count)

    return cum_table
