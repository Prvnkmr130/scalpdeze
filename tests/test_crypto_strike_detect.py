"""
tests/test_crypto_strike_detect.py
──────────────────────────────────
Unit tests for crypto options strike detection, tick normalization, and dynamic token updates.
"""

from datetime import datetime, timedelta
import polars as pl
import pytest
from unittest.mock import MagicMock, patch

from algo_trading.algos.crypto_opt_trde_polars import (
    TradeAlgo,
    str_to_token,
)


@pytest.fixture
def mock_crypto_trade_algo():
    """Builds a lightweight TradeAlgo fixture with mock cum_table and underlyings."""
    with patch.object(TradeAlgo, "load_master_token_list"), \
         patch.object(TradeAlgo, "insert_instrument_token"), \
         patch.object(TradeAlgo, "update_config_info"), \
         patch.object(TradeAlgo, "read_db_candle"):

        algo = TradeAlgo.__new__(TradeAlgo)
        algo.accounts = {}
        algo.primary_broker = None
        algo.primary_account_id = "test_acc"
        algo.debug_mode = True
        algo.month_cutoff = 0
        algo.fwd_1_all = pl.DataFrame()
        algo.fwd_3_all = pl.DataFrame()
        algo.fwd_5_all = pl.DataFrame()
        algo.fwd_10_all = pl.DataFrame()
        algo.fwd_15_all = pl.DataFrame()
        algo.fwd_30_all = pl.DataFrame()
        algo.fwd_60_all = pl.DataFrame()
        algo.day_cdl_all = pl.DataFrame()

        # Build mock cum_table with underlying BTCUSD (tkn=27) and options
        tomorrow_str = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
        algo.cum_table = pl.DataFrame([
            {
                "instrument_token": 27,
                "tradingsymbol": "BTCUSD",
                "instrument_type": "FUT",
                "Ref_stock": "BTCUSD",
                "Ref_stock_tkn": 27,
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
                "instrument_token": 151209,
                "tradingsymbol": "C-BTC-80000-070926",
                "instrument_type": "CE",
                "Ref_stock": "BTCUSD",
                "Ref_stock_tkn": 27,
                "strike": 80000.0,
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
                "instrument_token": 151294,
                "tradingsymbol": "P-BTC-79000-070926",
                "instrument_type": "PE",
                "Ref_stock": "BTCUSD",
                "Ref_stock_tkn": 27,
                "strike": 79000.0,
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
        algo.all_ref_tkns = [27]
        algo.init_ref_list = pl.DataFrame({"instrument_token": [27]})
        algo.index_ref_list = pl.DataFrame()
        return algo


def test_normalize_ticks_maps_tradingsymbol_to_token(mock_crypto_trade_algo):
    """Verifies normalize_ticks converts hashed or raw tradingsymbols into canonical cum_table tokens."""
    algo = mock_crypto_trade_algo

    # Input ticks with hashed token
    raw_ticks = pl.DataFrame({
        "instrument_token": [775262187],
        "tradingsymbol": ["BTCUSD"],
        "last_price": [79500.0],
        "date_time": [datetime.now()],
    })

    norm_ticks = algo.normalize_ticks(raw_ticks)
    assert norm_ticks["instrument_token"][0] == 27


def test_strike_detect_selects_active_strikes(mock_crypto_trade_algo):
    """Verifies strike_detect correctly identifies and tags ATM strikes as Buy_strike='Yes'."""
    algo = mock_crypto_trade_algo

    ticks = pl.DataFrame({
        "instrument_token": [27],
        "tradingsymbol": ["BTCUSD"],
        "last_price": [79500.0],
        "date_time": [datetime.now()],
    })

    algo.strike_detect(ticks)

    selected = algo.cum_table.filter(pl.col("Buy_strike") == "Yes")
    assert len(selected) == 2
    types = selected["instrument_type"].to_list()
    assert "CE" in types
    assert "PE" in types


def test_token_list_update_includes_selected_strikes(mock_crypto_trade_algo):
    """Verifies token_list_update expands the subscribed token set to include detected strikes."""
    algo = mock_crypto_trade_algo

    ticks = pl.DataFrame({
        "instrument_token": [27],
        "tradingsymbol": ["BTCUSD"],
        "last_price": [79500.0],
        "date_time": [datetime.now()],
    })

    algo.strike_detect(ticks)
    updated = algo.token_list_update()

    tokens = updated["instrument_token"].to_list()
    assert 27 in tokens  # Reference underlying
    assert 151209 in tokens  # CE strike
    assert 151294 in tokens  # PE strike
