# -*- coding: utf-8 -*-
"""
algo_trading/algos/scraped_signal_engine.py
───────────────────────────────────────────
Decoupled Quantitative Signal Engine for the U-Exchange.
Execution Mode: Pure Market Intelligence & Multi-Account Signal Generation
(Zero Order API, Zero Paper Trading).

Performs:
1. SIMD O(1) multi-timeframe candle downsampling (3m base to 1D).
2. Dynamic strike detection (est_strike = candle_avg + Strike_dist).
3. Heikin-Ashi & Directional Momentum evaluation (Paths 1 to 13_2).
4. Tier-2 multi-account signal fan-out.
5. Standardized ISO algolog telemetry ([MOM_SIGNAL], [MARKET_SIGNALS], [CYCLE_SUMMARY]).
6. Daily EOD Excel Report auto-export (reports/YYYY-MM-DD_U_Exchange_Signals.xlsx).
"""

from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, date
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import polars as pl

from algo_trading.algos.indian_candle_engine import (
    group_by_rolling_window,
    update_candles_incremental,
)
from algo_trading.algos.logger import algo_logger
from algo_trading.algos.u_exchange_session import (
    get_now_et,
    now_et_naive,
    today_et,
    is_eod_report_time,
    is_active_session_time,
)
from algo_trading.brokers.sniffer.browser_sniffer import SniffedMarketDataStore

logger = logging.getLogger("algo_trading.algos.scraped_signal_engine")
_CURRENT_ALGO_NAME = "u_exchange_signal_engine"


def calculate_heikin_ashi_indicators(candles: pl.DataFrame) -> pl.DataFrame:
    """
    Computes Heikin-Ashi candlestick transformations (ha_open, ha_high, ha_low, ha_close)
    over standard OHLC candles in Polars.
    """
    if candles.is_empty() or "open" not in candles.columns or "close" not in candles.columns:
        return pl.DataFrame()

    df = candles.sort("date_time", descending=False)
    records = df.to_dicts()
    if not records:
        return pl.DataFrame()

    ha_records = []
    prev_ha_open = None
    prev_ha_close = None

    for idx, row in enumerate(records):
        o = float(row.get("open") or 0.0)
        h = float(row.get("high") or o)
        l = float(row.get("low") or o)
        c = float(row.get("close") or o)

        ha_close = (o + h + l + c) / 4.0

        if idx == 0 or prev_ha_open is None or prev_ha_close is None:
            ha_open = (o + c) / 2.0
        else:
            ha_open = (prev_ha_open + prev_ha_close) / 2.0

        ha_high = max(h, ha_open, ha_close)
        ha_low = min(l, ha_open, ha_close)

        row_copy = dict(row)
        row_copy["ha_open"] = ha_open
        row_copy["ha_high"] = ha_high
        row_copy["ha_low"] = ha_low
        row_copy["ha_close"] = ha_close

        ha_records.append(row_copy)
        prev_ha_open = ha_open
        prev_ha_close = ha_close

    return pl.DataFrame(ha_records)



class ScrapedSignalEngine:
    """
    Autonomous Quantitative Analytics & Signal Generation Engine.
    """

    def __init__(
        self,
        data_store: SniffedMarketDataStore,
        reports_dir: str = "reports",
        on_signal_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self.data_store = data_store
        self.reports_dir = os.path.abspath(reports_dir)
        self.on_signal_callback = on_signal_callback

        os.makedirs(self.reports_dir, exist_ok=True)

        # In-memory multi-timeframe candle states
        self.fwd_3_all = pl.DataFrame()
        self.fwd_5_all = pl.DataFrame()
        self.fwd_10_all = pl.DataFrame()
        self.fwd_15_all = pl.DataFrame()
        self.fwd_30_all = pl.DataFrame()
        self.fwd_60_all = pl.DataFrame()
        self.day_cdl_all = pl.DataFrame()

        # Signal log history for EOD Excel Export
        self._generated_signals: List[Dict[str, Any]] = []
        self._eod_exported_date: Optional[date] = None

    def update_candles_from_ticks(self, raw_ticks: pl.DataFrame) -> None:
        """
        Ingests sniffed ticks and incrementally aggregates multi-timeframe candles.
        """
        if raw_ticks.is_empty():
            return

        # Ensure required columns
        req_cols = {"instrument_token", "last_price", "date_time"}
        if not req_cols.issubset(set(raw_ticks.columns)):
            return

        # Strip any broker summary rolling OHLC to prevent daily price skew
        cols_to_keep = [c for c in raw_ticks.columns if c not in ("open", "high", "low", "close")]
        clean_ticks = raw_ticks.select(cols_to_keep)

        # 1. Downsample raw ticks into 3-minute base candles
        self.fwd_3_all = group_by_rolling_window(
            df_hist=self.fwd_3_all,
            raw_df=clean_ticks,
            window_size="3m",
            max_bars=100,
        )

        if self.fwd_3_all.is_empty():
            return

        # Extract active (latest) 3m bar per token for O(1) incremental rollup
        latest_3m = self.fwd_3_all.group_by("instrument_token").tail(1)

        # 2. Incrementally update higher timeframes
        self.fwd_5_all = update_candles_incremental(self.fwd_5_all, latest_3m, timeframe_minutes=5)
        self.fwd_10_all = update_candles_incremental(self.fwd_10_all, latest_3m, timeframe_minutes=10)
        self.fwd_15_all = update_candles_incremental(self.fwd_15_all, latest_3m, timeframe_minutes=15)
        self.fwd_30_all = update_candles_incremental(self.fwd_30_all, latest_3m, timeframe_minutes=30)
        self.fwd_60_all = update_candles_incremental(self.fwd_60_all, latest_3m, timeframe_minutes=60)
        self.day_cdl_all = update_candles_incremental(self.day_cdl_all, latest_3m, timeframe_minutes=390)

    def detect_strikes(
        self,
        ref_symbols: List[str],
        option_chain: pl.DataFrame,
        strike_offsets: Optional[Dict[str, Tuple[float, float]]] = None,
    ) -> pl.DataFrame:
        """
        Dynamically detects ATM/OTM Call (CE) and Put (PE) strikes relative to moving candle averages.
        Returns filtered candidate strikes DataFrame.
        """
        if option_chain.is_empty() or not ref_symbols:
            return pl.DataFrame()

        selected_strikes: List[Dict[str, Any]] = []
        offsets = strike_offsets or {}

        for sym in ref_symbols:
            # 1. Compute candle average from 3-minute base candles
            candle_avg = None
            if not self.fwd_3_all.is_empty():
                sym_bars = self.fwd_3_all.filter(pl.col("instrument_token").is_not_null())
                if not sym_bars.is_empty():
                    candle_avg = float(sym_bars["close"][-1])

            if candle_avg is None or candle_avg <= 0:
                # Fallback to latest tick in option chain for the reference stock
                matching = option_chain.filter(pl.col("Ref_stock") == sym)
                if not matching.is_empty():
                    candle_avg = float(matching["last_price"].mean() or 0.0)

            if not candle_avg or candle_avg <= 0:
                continue

            dist_ce, dist_pe = offsets.get(sym, (0.0, 0.0))
            est_strike_ce = candle_avg + dist_ce
            est_strike_pe = candle_avg + dist_pe

            candidates = option_chain.filter(pl.col("Ref_stock") == sym)
            if candidates.is_empty():
                continue

            # CE Strike Selection (ratio >= 1.0)
            ce_candidates = (
                candidates.filter(pl.col("instrument_type") == "CE")
                .with_columns(((pl.col("strike") / est_strike_ce).round(6)).alias("ratio"))
                .filter(pl.col("ratio") >= 1.0)
                .sort("ratio", descending=False)
            )
            if not ce_candidates.is_empty():
                row = ce_candidates.to_dicts()[0]
                row["Buy_strike"] = "Yes"
                selected_strikes.append(row)

            # PE Strike Selection (ratio <= 1.0)
            pe_candidates = (
                candidates.filter(pl.col("instrument_type") == "PE")
                .with_columns(((pl.col("strike") / est_strike_pe).round(6)).alias("ratio"))
                .filter(pl.col("ratio") <= 1.0)
                .sort("ratio", descending=True)
            )
            if not pe_candidates.is_empty():
                row = pe_candidates.to_dicts()[0]
                row["Buy_strike"] = "Yes"
                selected_strikes.append(row)

        return pl.DataFrame(selected_strikes) if selected_strikes else pl.DataFrame()

    def evaluate_momentum_signals(self, candles: pl.DataFrame) -> Dict[str, Any]:
        """
        Computes Heikin-Ashi candles and evaluates directional momentum paths 1 to 13_2.
        Returns a dictionary of resolved signals.
        """
        signals = {
            "buy_signal_CE": 0,
            "buy_signal_PE": 0,
            "CE_jump": 0,
            "PE_jump": 0,
            "path_name": "None",
            "ha_trend": "NEUTRAL",
        }

        if candles.is_empty() or len(candles) < 3:
            return signals

        # Calculate Heikin-Ashi indicators
        ha_df = calculate_heikin_ashi_indicators(candles)
        if ha_df.is_empty():
            return signals

        latest = ha_df.to_dicts()[-1]
        prev = ha_df.to_dicts()[-2]

        ha_close = latest.get("ha_close", 0.0)
        ha_open = latest.get("ha_open", 0.0)
        ha_high = latest.get("ha_high", 0.0)
        ha_low = latest.get("ha_low", 0.0)

        prev_close = prev.get("ha_close", 0.0)
        prev_open = prev.get("ha_open", 0.0)

        # Bullish momentum paths (Call Buy: Path 1 / 13_1)
        if ha_close > ha_open and prev_close > prev_open:
            signals["ha_trend"] = "BULLISH"
            # Path 13_1: Strong green candle with no lower shadow
            if abs(ha_open - ha_low) < 0.05 * (ha_high - ha_low) if (ha_high - ha_low) > 0 else True:
                signals["buy_signal_CE"] = 1
                signals["CE_jump"] = 1
                signals["path_name"] = "Path_13_1_Bullish_Breakout"
            else:
                signals["buy_signal_CE"] = 1
                signals["path_name"] = "Path_1_Bullish_Followthrough"

        # Bearish momentum paths (Put Buy: Path 2 / 13_2)
        elif ha_close < ha_open and prev_close < prev_open:
            signals["ha_trend"] = "BEARISH"
            # Path 13_2: Strong red candle with no upper shadow
            if abs(ha_high - ha_open) < 0.05 * (ha_high - ha_low) if (ha_high - ha_low) > 0 else True:
                signals["buy_signal_PE"] = 1
                signals["PE_jump"] = 1
                signals["path_name"] = "Path_13_2_Bearish_Breakdown"
            else:
                signals["buy_signal_PE"] = 1
                signals["path_name"] = "Path_2_Bearish_Followthrough"

        return signals

    def execute_analytical_cycle(
        self,
        ref_symbols: Optional[List[str]] = None,
        active_accounts: Optional[List[Any]] = None,
    ) -> Dict[str, Any]:
        """
        Executes one 2.0s analytical iteration across Tier 1 (Market Calculations)
        and Tier 2 (Multi-Account Fan-Out).
        """
        start_t = time.time()
        now_dt = now_et_naive()
        today_d = today_et()
        ref_syms = ref_symbols or ["SPY", "QQQ", "AAPL"]

        # 1. Ingest sniffed market ticks
        recent_ticks = self.data_store.get_recent_ticks_df(clear=True)
        self.update_candles_from_ticks(recent_ticks)

        # 2. Option chain retrieval and strike detection
        option_chain = self.data_store.get_option_chain_df()
        selected_strikes = self.detect_strikes(ref_symbols=ref_syms, option_chain=option_chain)

        # 3. Technical Momentum Evaluation
        mom_results = self.evaluate_momentum_signals(self.fwd_3_all)

        # ISO Standardized Telemetry
        tick_count = len(recent_ticks)
        strike_count = len(selected_strikes)
        algo_logger.log_sync(
            tag="MARKET_SIGNALS",
            message=f"[MARKET_SIGNALS] Ingested {tick_count} ticks. Ref symbols: {ref_syms}. Selected strikes: {strike_count}.",
            level="INFO",
            algo_name=_CURRENT_ALGO_NAME,
        )

        if mom_results["buy_signal_CE"] == 1 or mom_results["buy_signal_PE"] == 1:
            algo_logger.log_sync(
                tag="MOM_SIGNAL",
                message=(
                    f"[MOM_SIGNAL] Path: {mom_results['path_name']} | CE_Signal={mom_results['buy_signal_CE']} | "
                    f"PE_Signal={mom_results['buy_signal_PE']} | Trend={mom_results['ha_trend']}"
                ),
                level="INFO",
                algo_name=_CURRENT_ALGO_NAME,
            )

        # 4. Tier 2: Multi-Account Signal Fan-Out
        accounts_notified = 0
        if active_accounts and (mom_results["buy_signal_CE"] == 1 or mom_results["buy_signal_PE"] == 1):
            for acc in active_accounts:
                acc_id = getattr(acc, "account_id", str(acc))
                signal_record = {
                    "timestamp": now_dt.isoformat(),
                    "account_id": acc_id,
                    "trend": mom_results["ha_trend"],
                    "path_name": mom_results["path_name"],
                    "buy_signal_CE": mom_results["buy_signal_CE"],
                    "buy_signal_PE": mom_results["buy_signal_PE"],
                    "CE_jump": mom_results["CE_jump"],
                    "PE_jump": mom_results["PE_jump"],
                }
                self._generated_signals.append(signal_record)
                accounts_notified += 1

                if self.on_signal_callback:
                    try:
                        self.on_signal_callback(signal_record)
                    except Exception as e:
                        logger.error(f"Error in on_signal_callback: {e}")

        # 5. Check EOD Excel Export
        if is_eod_report_time(now_dt) and self._eod_exported_date != today_d:
            self.export_daily_eod_report(today_d)
            self._eod_exported_date = today_d

        cycle_latency = time.time() - start_t
        algo_logger.log_sync(
            tag="CYCLE_SUMMARY",
            message=f"[CYCLE_SUMMARY] Cycle complete in {cycle_latency:.3f}s. Active accounts fanned out: {accounts_notified}.",
            level="INFO",
            algo_name=_CURRENT_ALGO_NAME,
        )

        return {
            "latency": cycle_latency,
            "ticks_ingested": tick_count,
            "strikes_selected": strike_count,
            "momentum": mom_results,
            "accounts_notified": accounts_notified,
        }

    def export_daily_eod_report(self, report_date: date) -> Optional[str]:
        """
        Exports formatted multi-sheet Excel report `reports/YYYY-MM-DD_U_Exchange_Signals.xlsx`.
        """
        filename = f"{report_date.isoformat()}_U_Exchange_Signals.xlsx"
        report_path = os.path.join(self.reports_dir, filename)

        logger.info(f"[EOD_REPORT] Generating daily Excel report: {report_path}...")

        try:
            from algo_trading.algos.polars_excel import write_polars_sheets_to_excel

            signals_df = (
                pl.DataFrame(self._generated_signals)
                if self._generated_signals
                else pl.DataFrame({"Info": ["No high-conviction signals generated today."]})
            )
            candles_df = (
                self.fwd_3_all.tail(50)
                if not self.fwd_3_all.is_empty()
                else pl.DataFrame({"Info": ["No candle bars captured."]})
            )
            health_df = pl.DataFrame({
                "Report_Date": [report_date.isoformat()],
                "Total_Signals": [len(self._generated_signals)],
                "Export_Timestamp": [datetime.now().isoformat()],
            })

            sheets = {
                "Signals": signals_df,
                "Candles_Summary": candles_df,
                "Health_Summary": health_df,
            }

            final_path = write_polars_sheets_to_excel(sheets_dict=sheets, output_path=report_path)
            logger.info(f"[EOD_REPORT] Successfully exported: {final_path}")
            return final_path
        except Exception as e:
            logger.error(f"[EOD_REPORT] Failed to generate daily Excel report: {e}")
            return None


def run_scraped_signal_cycle(
    engine: ScrapedSignalEngine,
    ref_symbols: Optional[List[str]] = None,
    active_accounts: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    """Standard callable adapter for algo execution loop."""
    return engine.execute_analytical_cycle(ref_symbols=ref_symbols, active_accounts=active_accounts)
