# -*- coding: utf-8 -*-
"""
algo_trading/algos/indian_candle_engine.py
──────────────────────────────────────────
High-performance, SIMD-vectorized candle downsampler and technical indicators
for Indian exchanges using pure Polars (group_by_dynamic in Rust).
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, time as dt_time, timedelta
from typing import Any, Callable, Dict, List, Optional

import polars as pl

from algo_trading.algos.indian_master_tokens import today_ist

logger = logging.getLogger("algo_trading.algos.indian_candle_engine")


def group_by_rolling_window(
    df_hist: pl.DataFrame,
    raw_df: pl.DataFrame,
    window_size: str,
    tkn_to_exchg_fn: Optional[Callable[[int], str]] = None,
    max_bars: int = 100,
) -> pl.DataFrame:
    """
    SIMD Vectorized Multi-Token Hierarchical Candle Resampler in Polars:
    Uses Rust multi-threaded group_by_dynamic to downsample ticks across tokens simultaneously.
    Accommodates 09:00 origin for MCX/CDS and 09:15 origin for NSE/NFO/BSE/BFO.
    """
    freq_map = {
        "1min": "1m", "1m": "1m",
        "3min": "3m", "3m": "3m",
        "5min": "5m", "5m": "5m",
        "10min": "10m", "10m": "10m",
        "15min": "15m", "15m": "15m",
        "30min": "30m", "30m": "30m",
        "60min": "60m", "60m": "60m",
        "0.5D": "375m", "1D": "1d", "1d": "1d"
    }
    polars_freq = freq_map.get(window_size, "1m")

    if raw_df.is_empty():
        return df_hist

    sub_df = raw_df.filter(pl.col("date_time").is_not_null())
    if sub_df.is_empty():
        return df_hist

    if "date_time" in sub_df.columns:
        if sub_df["date_time"].dtype != pl.Datetime:
            try:
                sub_df = sub_df.with_columns(pl.col("date_time").cast(pl.Utf8).str.slice(0, 19).str.to_datetime(strict=False))
            except Exception:
                pass
        try:
            if hasattr(sub_df["date_time"].dtype, "time_zone") and sub_df["date_time"].dtype.time_zone:
                sub_df = sub_df.with_columns(pl.col("date_time").dt.replace_time_zone(None))
        except Exception:
            pass

    if not df_hist.is_empty() and "date_time" in df_hist.columns:
        if df_hist["date_time"].dtype != pl.Datetime:
            try:
                df_hist = df_hist.with_columns(pl.col("date_time").cast(pl.Utf8).str.slice(0, 19).str.to_datetime(strict=False))
            except Exception:
                pass
        try:
            if hasattr(df_hist["date_time"].dtype, "time_zone") and df_hist["date_time"].dtype.time_zone:
                df_hist = df_hist.with_columns(pl.col("date_time").dt.replace_time_zone(None))
        except Exception:
            pass

    try:
        today_date = sub_df["date_time"].dt.date()[0]
        origin_900 = datetime.combine(today_date, dt_time(9, 0, 0))
        origin_915 = datetime.combine(today_date, dt_time(9, 15, 0))
    except Exception:
        today_date = today_ist()
        origin_900 = datetime.combine(today_date, dt_time(9, 0, 0))
        origin_915 = datetime.combine(today_date, dt_time(9, 15, 0))

    has_ohlc = ("open" in sub_df.columns and "close" in sub_df.columns)
    has_last_price = ("last_price" in sub_df.columns)

    unique_tokens = sub_df["instrument_token"].unique().to_list() if "instrument_token" in sub_df.columns else [None]

    if tkn_to_exchg_fn:
        tokens_900 = [t for t in unique_tokens if t is not None and tkn_to_exchg_fn(t) in ("MCX", "CDS")]
    else:
        tokens_900 = []
    tokens_915 = [t for t in unique_tokens if t is None or t not in tokens_900]

    resampled_blocks: List[pl.DataFrame] = []

    for token_subset, origin_time in [(tokens_900, origin_900), (tokens_915, origin_915)]:
        if not token_subset:
            continue

        curr_df = sub_df.filter(pl.col("instrument_token").is_in(token_subset))
        if curr_df.is_empty():
            continue

        curr_sorted = curr_df.sort("date_time")

        if has_last_price:
            block = (
                curr_sorted.group_by_dynamic(
                    "date_time",
                    every=polars_freq,
                    group_by="instrument_token",
                    closed="left",
                    label="left",
                    start_by="window",
                )
                .agg([
                    pl.col("last_price").first().alias("open"),
                    pl.col("last_price").max().alias("high"),
                    pl.col("last_price").min().alias("low"),
                    pl.col("last_price").last().alias("close"),
                ])
            )
        elif has_ohlc:
            block = (
                curr_sorted.group_by_dynamic(
                    "date_time",
                    every=polars_freq,
                    group_by="instrument_token",
                    closed="left",
                    label="left",
                    start_by="window",
                )
                .agg([
                    pl.col("open").first().alias("open"),
                    pl.col("high").max().alias("high"),
                    pl.col("low").min().alias("low"),
                    pl.col("close").last().alias("close"),
                ])
            )
        else:
            continue

        resampled_blocks.append(block)

    if resampled_blocks:
        resampled_df = pl.concat(resampled_blocks, how="diagonal")
    else:
        resampled_df = pl.DataFrame(schema={
            "open": pl.Float64, "low": pl.Float64, "high": pl.Float64,
            "close": pl.Float64, "instrument_token": pl.Int64, "date_time": pl.Datetime
        })

    desired_cols = ["open", "low", "high", "close", "instrument_token", "date_time"]
    if not df_hist.is_empty() and not resampled_df.is_empty():
        cols = [c for c in desired_cols if c in resampled_df.columns and c in df_hist.columns]
        # Preserve historical extremes and original open on candle overlap:
        # Put df_hist first so first() takes df_hist.open, max() merges high, min() merges low, and last() takes resampled_df.close
        combined = pl.concat([df_hist.select(cols), resampled_df.select(cols)], how="diagonal")
        subset_cols = ["instrument_token", "date_time"] if "instrument_token" in combined.columns else ["date_time"]
        final_df = (
            combined.group_by(subset_cols, maintain_order=True)
            .agg([
                pl.when(pl.col("high").first() == pl.col("low").first())
                .then(pl.col("open").last())
                .otherwise(pl.col("open").first())
                .alias("open"),
                pl.col("high").max().alias("high"),
                pl.col("low").min().alias("low"),
                pl.col("close").last().alias("close"),
            ])
        )
    elif not resampled_df.is_empty():
        final_df = resampled_df
    else:
        final_df = df_hist

    if not final_df.is_empty() and "date_time" in final_df.columns:
        final_df = final_df.sort("date_time", descending=True)
        if max_bars > 0:
            effective_max = max(25, max_bars)
            if "instrument_token" in final_df.columns:
                final_df = final_df.filter(
                    pl.int_range(0, pl.len()).over("instrument_token") < effective_max
                )
            else:
                final_df = final_df.head(effective_max)

    return final_df


def heikin_ashi(df: pl.DataFrame) -> pl.DataFrame:
    """Vectorized SIMD Heikin-Ashi candlestick transformer in Polars."""
    if df.is_empty():
        return df

    ha_close = (pl.col("open") + pl.col("high") + pl.col("low") + pl.col("close")) / 4
    ha_open = ((pl.col("open").shift(1) + pl.col("close").shift(1)) / 2).fill_null(pl.col("open"))

    ha = df.with_columns([
        ha_close.alias("close"),
        ha_open.alias("open"),
    ]).with_columns([
        pl.max_horizontal("open", "close", "high").alias("high"),
        pl.min_horizontal("open", "close", "low").alias("low"),
    ])
    return ha


TIMEFRAME_SECONDS: Dict[str, int] = {
    "1min": 60, "1m": 60,
    "3min": 180, "3m": 180,
    "5min": 300, "5m": 300,
    "10min": 600, "10m": 600,
    "15min": 900, "15m": 900,
    "30min": 1800, "30m": 1800,
    "60min": 3600, "60m": 3600,
    "0.5D": 22500, "375m": 22500,
    "1D": 86400, "1d": 86400,
}


def get_candle_bucket_start(
    dt: datetime,
    timeframe: str,
    origin_time: dt_time = dt_time(9, 15),
) -> datetime:
    """
    Computes the exact starting timestamp of the candle bucket containing dt,
    anchored to the specified origin_time (e.g. 09:15 for NSE/NFO, 09:00 for MCX/CDS, 00:00 for Crypto).
    """
    window_sec = TIMEFRAME_SECONDS.get(timeframe, 60)
    origin_dt = datetime.combine(dt.date(), origin_time)
    delta_sec = (dt - origin_dt).total_seconds()
    bucket_num = math.floor(delta_sec / window_sec)
    return origin_dt + timedelta(seconds=bucket_num * window_sec)


def update_candles_incremental(
    df_hist: pl.DataFrame,
    raw_df: pl.DataFrame,
    window_size: str,
    max_bars: int = 25,
    default_origin: str = "09:15",
    tkn_to_exchg_fn: Optional[Callable[[int], str]] = None,
) -> pl.DataFrame:
    """
    High-throughput O(1) incremental multi-timeframe candle updater:
    - Same Bucket: Updates close = current_price, high = max(high, current_price), low = min(low, current_price) in place.
    - New Bucket: When timeframe delta is reached, creates a new candle where open = previous_candle.close,
      high = max(open, current_price), low = min(open, current_price), close = current_price, date_time = bucket_start.
    - Pruning: Retains at least max_bars (default 25) strictly on a per-token basis.
    - Cold-Start Bootstrap: If df_hist has fewer than 2 bars and raw_df has > 50 ticks, delegates to group_by_rolling_window.
    """
    if raw_df.is_empty():
        return df_hist

    if "date_time" in raw_df.columns:
        if raw_df["date_time"].dtype != pl.Datetime:
            try:
                raw_df = raw_df.with_columns(pl.col("date_time").cast(pl.Utf8).str.slice(0, 19).str.to_datetime(strict=False))
            except Exception:
                pass
        try:
            if hasattr(raw_df["date_time"].dtype, "time_zone") and raw_df["date_time"].dtype.time_zone:
                raw_df = raw_df.with_columns(pl.col("date_time").dt.replace_time_zone(None))
        except Exception:
            pass

    if not df_hist.is_empty() and "date_time" in df_hist.columns:
        if df_hist["date_time"].dtype != pl.Datetime:
            try:
                df_hist = df_hist.with_columns(pl.col("date_time").cast(pl.Utf8).str.slice(0, 19).str.to_datetime(strict=False))
            except Exception:
                pass
        try:
            if hasattr(df_hist["date_time"].dtype, "time_zone") and df_hist["date_time"].dtype.time_zone:
                df_hist = df_hist.with_columns(pl.col("date_time").dt.replace_time_zone(None))
        except Exception:
            pass

    window_sec = TIMEFRAME_SECONDS.get(window_size, 60)
    effective_max = max(25, max_bars) if max_bars > 0 else 25

    # Multi-interval batch detection:
    # If raw_df spans multiple window intervals (e.g. startup warmup or reconnect bursts),
    # or df_hist is empty, delegate to group_by_rolling_window
    needs_bootstrap = False
    if len(raw_df) > 50:
        if df_hist.is_empty():
            needs_bootstrap = True
        elif "date_time" in raw_df.columns:
            try:
                valid_dts = raw_df["date_time"].drop_nulls()
                if not valid_dts.is_empty():
                    dt_span = (valid_dts.max() - valid_dts.min()).total_seconds()
                    if dt_span > window_sec:
                        needs_bootstrap = True
            except Exception:
                pass

    if needs_bootstrap:
        return group_by_rolling_window(
            df_hist=df_hist,
            raw_df=raw_df,
            window_size=window_size,
            tkn_to_exchg_fn=tkn_to_exchg_fn,
            max_bars=max_bars,
        )

    try:
        parts = [int(p) for p in default_origin.split(":")]
        def_origin_t = dt_time(parts[0], parts[1])
    except Exception:
        def_origin_t = dt_time(9, 15)

    # Extract OHLC snapshot per token from raw_df
    token_snapshots: Dict[int, Dict[str, Any]] = {}
    has_lp = ("last_price" in raw_df.columns)
    has_ohlc = ("open" in raw_df.columns and "close" in raw_df.columns)

    for row in raw_df.sort("date_time").to_dicts():
        tok = row.get("instrument_token")
        if tok is None:
            continue
        dt = row.get("date_time")
        if dt is None:
            continue

        if has_lp and row.get("last_price") is not None:
            lp = float(row["last_price"])
            o = h = l = c = lp
        elif has_ohlc and row.get("open") is not None and row.get("close") is not None:
            o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        else:
            continue

        if tok not in token_snapshots:
            token_snapshots[tok] = {"open": o, "high": h, "low": l, "close": c, "date_time": dt}
        else:
            curr = token_snapshots[tok]
            curr["close"] = c
            if h > curr["high"]:
                curr["high"] = h
            if l < curr["low"]:
                curr["low"] = l
            curr["date_time"] = dt

    # Group df_hist by token (ensuring descending order by date_time so bars[0] is strictly the latest bar)
    hist_by_token: Dict[int, List[Dict[str, Any]]] = {}
    if not df_hist.is_empty():
        df_hist_sorted = df_hist.sort("date_time", descending=True) if "date_time" in df_hist.columns else df_hist
        for row in df_hist_sorted.to_dicts():
            tok = row.get("instrument_token")
            if tok is not None:
                if tok not in hist_by_token:
                    hist_by_token[tok] = []
                hist_by_token[tok].append(row)

    effective_max = max(25, max_bars) if max_bars > 0 else 25

    for tok, snap in token_snapshots.items():
        if tkn_to_exchg_fn:
            exch = str(tkn_to_exchg_fn(tok)).upper()
            if exch in ("MCX", "CDS"):
                origin_t = dt_time(9, 0)
            elif exch == "CRYPTO":
                origin_t = dt_time(0, 0)
            else:
                origin_t = def_origin_t
        else:
            origin_t = def_origin_t

        b_start = get_candle_bucket_start(snap["date_time"], window_size, origin_t)
        bars = hist_by_token.get(tok, [])

        if not bars:
            bars.append({
                "open": snap["open"],
                "high": snap["high"],
                "low": snap["low"],
                "close": snap["close"],
                "instrument_token": tok,
                "date_time": b_start,
            })
            hist_by_token[tok] = bars
            continue

        latest = bars[0]
        if b_start == latest["date_time"]:
            # Same candle: update close, high, low in place
            latest["close"] = snap["close"]
            if snap["high"] > latest["high"]:
                latest["high"] = snap["high"]
            if snap["low"] < latest["low"]:
                latest["low"] = snap["low"]
        elif b_start > latest["date_time"]:
            # New candle: open equals previous candle close
            prev_close = float(latest["close"])
            new_bar = {
                "open": prev_close,
                "high": max(prev_close, snap["high"]),
                "low": min(prev_close, snap["low"]),
                "close": snap["close"],
                "instrument_token": tok,
                "date_time": b_start,
            }
            bars.insert(0, new_bar)
            if len(bars) > effective_max:
                bars.pop()

    all_rows: List[Dict[str, Any]] = []
    for tok_bars in hist_by_token.values():
        all_rows.extend(tok_bars)

    schema = {
        "open": pl.Float64, "low": pl.Float64, "high": pl.Float64, "close": pl.Float64,
        "instrument_token": pl.Int64, "date_time": pl.Datetime
    }
    if not all_rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(all_rows, schema=schema).sort("date_time", descending=True)

