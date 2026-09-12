"""
tests/test_indian_strike_detect.py
──────────────────────────────────
Unit tests for Indian options strike detection, tick normalization, and dynamic token updates.
"""

from datetime import datetime, timedelta
import polars as pl
import pytest

from algo_trading.algos.indian_opt_trde_polars import (
    TradeAlgo,
    str_to_token,
)
from algo_trading.algos.indian_strike_engine import strike_detect as standalone_strike_detect


@pytest.fixture
def mock_indian_trade_algo():
    """Builds a lightweight TradeAlgo fixture for Indian options engine."""
    algo = TradeAlgo.__new__(TradeAlgo)
    algo.accounts = []
    algo.primary_broker = None
    algo.primary_account_id = "test_acc"
    algo.debug_mode = True
    algo.data_ready = True
    algo.month_cutoff = 0
    algo.fwd_1_all = pl.DataFrame()
    algo.fwd_3_all = pl.DataFrame()
    algo.fwd_10_all = pl.DataFrame()
    algo.fwd_30_all = pl.DataFrame()
    algo.day_cdl_all = pl.DataFrame()

    tomorrow_str = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
    algo.cum_table = pl.DataFrame([
        {
            "instrument_token": 256265,
            "tradingsymbol": "NIFTY 50",
            "instrument_type": "EQ",
            "Ref_stock": "NIFTY",
            "Ref_stock_tkn": 256265,
            "strike": 0.0,
            "Strike_dist_CE": 50.0,
            "Strike_dist_PE": -50.0,
            "cap": "monthly_options",
            "Cap_info": "monthly_options",
            "expiry": tomorrow_str,
            "exp_date_list": 2,
            "week_dist": 0.28,
            "Capital_share": 1,
            "Buy_strike": "NA",
            "Tradable_stock": "No",
        },
        {
            "instrument_token": 100001,
            "tradingsymbol": "NIFTY26SEP24500CE",
            "instrument_type": "CE",
            "Ref_stock": "NIFTY",
            "Ref_stock_tkn": 256265,
            "strike": 24500.0,
            "Strike_dist_CE": 50.0,
            "Strike_dist_PE": -50.0,
            "cap": "monthly_options",
            "Cap_info": "monthly_options",
            "expiry": tomorrow_str,
            "exp_date_list": 2,
            "week_dist": 0.28,
            "Capital_share": 1,
            "Buy_strike": "NA",
            "Tradable_stock": "No",
        },
        {
            "instrument_token": 100002,
            "tradingsymbol": "NIFTY26SEP24400PE",
            "instrument_type": "PE",
            "Ref_stock": "NIFTY",
            "Ref_stock_tkn": 256265,
            "strike": 24400.0,
            "Strike_dist_CE": 50.0,
            "Strike_dist_PE": -50.0,
            "cap": "monthly_options",
            "Cap_info": "monthly_options",
            "expiry": tomorrow_str,
            "exp_date_list": 2,
            "week_dist": 0.28,
            "Capital_share": 1,
            "Buy_strike": "NA",
            "Tradable_stock": "No",
        },
    ])
    algo.all_ref_tkns = [256265]
    algo.init_ref_list = pl.DataFrame({"instrument_token": [256265]})
    algo.index_ref_list = pl.DataFrame()
    return algo


def test_indian_normalize_ticks(mock_indian_trade_algo):
    """Verifies normalize_ticks maps tradingsymbol to canonical cum_table token."""
    algo = mock_indian_trade_algo

    raw_ticks = pl.DataFrame({
        "instrument_token": [999999],
        "tradingsymbol": ["NIFTY 50"],
        "last_price": [24450.0],
        "date_time": [datetime.now()],
    })

    norm_ticks = algo.normalize_ticks(raw_ticks)
    assert norm_ticks["instrument_token"][0] == 256265


def test_indian_strike_detect_selects_active_strikes(mock_indian_trade_algo):
    """Verifies strike_detect correctly identifies and tags ATM strikes in Indian engine."""
    algo = mock_indian_trade_algo

    ticks = pl.DataFrame({
        "instrument_token": [256265],
        "tradingsymbol": ["NIFTY 50"],
        "last_price": [24450.0],
        "date_time": [datetime.now()],
    })

    algo.strike_detect(ticks)

    selected = algo.cum_table.filter(pl.col("Buy_strike") == "Yes")
    assert len(selected) == 2
    types = selected["instrument_type"].to_list()
    assert "CE" in types
    assert "PE" in types


def test_standalone_strike_detect_function(mock_indian_trade_algo):
    """Verifies indian_strike_engine.strike_detect standalone function works with dual-key matching."""
    algo = mock_indian_trade_algo

    ticks = pl.DataFrame({
        "instrument_token": [256265],
        "tradingsymbol": ["NIFTY"],
        "last_price": [24450.0],
        "date_time": [datetime.now()],
    })

    result_cum = standalone_strike_detect(
        cum_table=algo.cum_table,
        tick_data=ticks,
        fwd_3_all=pl.DataFrame(),
    )

    selected = result_cum.filter(pl.col("Buy_strike") == "Yes")
    assert len(selected) == 2
