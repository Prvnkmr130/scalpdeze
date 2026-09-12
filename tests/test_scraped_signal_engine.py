# -*- coding: utf-8 -*-
"""
tests/test_scraped_signal_engine.py
───────────────────────────────────
Unit tests for ScrapedSignalEngine and SniffedMarketDataStore.
"""

import os
from datetime import datetime, date, timedelta
import polars as pl
import pytest

from algo_trading.brokers.sniffer.browser_sniffer import SniffedMarketDataStore
from algo_trading.algos.scraped_signal_engine import ScrapedSignalEngine


def test_sniffed_market_data_store():
    """Verify market data store collects and returns ticks and option chains in Polars."""
    store = SniffedMarketDataStore(max_ticks=100)

    # Push ticks
    store.push_tick(symbol="SPY", last_price=550.25, volume=1000)
    store.push_tick(symbol="QQQ", last_price=480.50, volume=500)

    df_ticks = store.get_recent_ticks_df()
    assert len(df_ticks) == 2
    assert "SPY" in df_ticks["tradingsymbol"].to_list()
    assert "QQQ" in df_ticks["tradingsymbol"].to_list()

    # Push option contract
    store.push_option_contract(
        ref_stock="SPY",
        tradingsymbol="SPY260918C00550000",
        strike=550.0,
        instrument_type="CE",
        expiry="2026-09-18",
        last_price=4.50,
    )
    store.push_option_contract(
        ref_stock="SPY",
        tradingsymbol="SPY260918P00550000",
        strike=550.0,
        instrument_type="PE",
        expiry="2026-09-18",
        last_price=4.20,
    )

    df_chain = store.get_option_chain_df()
    assert len(df_chain) == 2
    assert "SPY260918C00550000" in df_chain["tradingsymbol"].to_list()
    assert "SPY260918P00550000" in df_chain["tradingsymbol"].to_list()


def test_strike_detection():
    """Verify ATM/OTM strike selection for Call (CE) and Put (PE)."""
    store = SniffedMarketDataStore()
    engine = ScrapedSignalEngine(data_store=store)

    # Populate option chain with strikes around 550
    strikes_data = [
        ("SPY", "SPY_545_CE", 545.0, "CE", "2026-09-18", 7.0),
        ("SPY", "SPY_550_CE", 550.0, "CE", "2026-09-18", 4.0),
        ("SPY", "SPY_555_CE", 555.0, "CE", "2026-09-18", 2.0),
        ("SPY", "SPY_545_PE", 545.0, "PE", "2026-09-18", 2.1),
        ("SPY", "SPY_550_PE", 550.0, "PE", "2026-09-18", 4.1),
        ("SPY", "SPY_555_PE", 555.0, "PE", "2026-09-18", 7.1),
    ]
    for ref, sym, strike, t, exp, ltp in strikes_data:
        store.push_option_contract(ref, sym, strike, t, exp, ltp)

    chain_df = store.get_option_chain_df()

    # Pre-populate 3m candles with close = 550.0
    now = datetime.now()
    engine.fwd_3_all = pl.DataFrame({
        "instrument_token": [101, 101, 101],
        "date_time": [now - timedelta(minutes=6), now - timedelta(minutes=3), now],
        "open": [548.0, 549.0, 549.5],
        "high": [550.0, 551.0, 551.0],
        "low": [547.0, 548.5, 549.0],
        "close": [549.0, 549.5, 550.0],
        "volume": [100.0, 150.0, 200.0],
    })

    selected = engine.detect_strikes(ref_symbols=["SPY"], option_chain=chain_df)
    assert len(selected) == 2

    types = selected["instrument_type"].to_list()
    assert "CE" in types
    assert "PE" in types

    ce_row = selected.filter(pl.col("instrument_type") == "CE").to_dicts()[0]
    pe_row = selected.filter(pl.col("instrument_type") == "PE").to_dicts()[0]

    # ATM CE >= 550 -> strike 550
    assert ce_row["strike"] == 550.0
    # ATM PE <= 550 -> strike 550
    assert pe_row["strike"] == 550.0


def test_momentum_signals_evaluation():
    """Verify Heikin-Ashi momentum signal generation."""
    store = SniffedMarketDataStore()
    engine = ScrapedSignalEngine(data_store=store)

    now = datetime.now()
    # Bullish breakout series (ha_close > ha_open)
    bullish_candles = pl.DataFrame({
        "date_time": [now - timedelta(minutes=9), now - timedelta(minutes=6), now - timedelta(minutes=3), now],
        "open": [100.0, 102.0, 104.0, 106.0],
        "high": [103.0, 105.0, 107.0, 109.0],
        "low": [99.5, 102.0, 104.0, 106.0],  # flat bottom
        "close": [102.5, 104.5, 106.5, 108.5],
        "volume": [1000.0, 1200.0, 1500.0, 1800.0],
    })

    signals = engine.evaluate_momentum_signals(bullish_candles)
    assert signals["ha_trend"] == "BULLISH"
    assert signals["buy_signal_CE"] == 1
    assert signals["buy_signal_PE"] == 0
    assert "Bullish" in signals["path_name"]


def test_daily_eod_excel_export(tmp_path):
    """Verify multi-sheet daily Excel report generation."""
    store = SniffedMarketDataStore()
    engine = ScrapedSignalEngine(data_store=store, reports_dir=str(tmp_path))

    # Add mock signal
    engine._generated_signals.append({
        "timestamp": datetime.now().isoformat(),
        "account_id": "ACC_TEST",
        "trend": "BULLISH",
        "path_name": "Path_13_1_Bullish_Breakout",
        "buy_signal_CE": 1,
        "buy_signal_PE": 0,
        "CE_jump": 1,
        "PE_jump": 0,
    })

    report_file = engine.export_daily_eod_report(date(2026, 9, 14))
    assert report_file is not None
    assert os.path.exists(report_file)
    assert report_file.endswith(".xlsx")
