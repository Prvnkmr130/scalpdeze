import os
import django
from datetime import datetime, timedelta, date, time as dt_time
from unittest.mock import MagicMock, patch
import polars as pl

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
django.setup()

from algo_trading.algos.indian_candle_engine import (
    group_by_rolling_window,
    get_candle_bucket_start,
    update_candles_incremental,
)
from algo_trading.algos.indian_opt_trde_polars import TradeAlgo as IndianTradeAlgo, today_ist
from algo_trading.algos.crypto_opt_trde_polars import TradeAlgo as CryptoTradeAlgo


def test_get_candle_bucket_start_origins():
    """Verify bucket start calculation across different exchange origins and timeframes."""
    # 1. NSE/NFO 10m (origin 09:15)
    dt_nse = datetime(2026, 9, 10, 9, 27, 45)
    b_nse = get_candle_bucket_start(dt_nse, "10m", dt_time(9, 15))
    assert b_nse == datetime(2026, 9, 10, 9, 25, 0)

    # 2. MCX 10m (origin 09:00)
    dt_mcx = datetime(2026, 9, 10, 17, 53, 20)
    b_mcx = get_candle_bucket_start(dt_mcx, "10m", dt_time(9, 0))
    assert b_mcx == datetime(2026, 9, 10, 17, 50, 0)

    # 3. Crypto 30m (origin 00:00)
    dt_crypto = datetime(2026, 9, 10, 14, 45, 10)
    b_crypto = get_candle_bucket_start(dt_crypto, "30m", dt_time(0, 0))
    assert b_crypto == datetime(2026, 9, 10, 14, 30, 0)


def test_update_candles_incremental_same_bucket_update():
    """Verify in-place update of close, high, low when ticks arrive within the same bucket."""
    t0 = datetime(2026, 9, 10, 9, 15, 0)
    df_hist = pl.DataFrame([{
        "instrument_token": 101,
        "date_time": t0,
        "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0
    }])

    # Incoming tick at 09:18 (same 10m bucket starting at 09:15) with price higher and lower
    raw_tick1 = pl.DataFrame([{
        "instrument_token": 101,
        "date_time": datetime(2026, 9, 10, 9, 18, 0),
        "last_price": 105.0
    }])
    updated = update_candles_incremental(df_hist, raw_tick1, "10m", max_bars=25, default_origin="09:15")

    assert len(updated) == 1
    row = updated.to_dicts()[0]
    assert row["open"] == 100.0, "Open must remain unchanged within same bucket"
    assert row["high"] == 105.0, "High must expand to new peak"
    assert row["low"] == 99.0
    assert row["close"] == 105.0, "Close must match latest tick price"
    assert row["date_time"] == t0


def test_update_candles_incremental_new_bucket_open_equals_prev_close():
    """Verify that when time delta is reached, a new candle is created with open equal to previous close."""
    t0 = datetime(2026, 9, 10, 9, 15, 0)
    df_hist = pl.DataFrame([{
        "instrument_token": 101,
        "date_time": t0,
        "open": 100.0, "high": 105.0, "low": 99.0, "close": 103.5
    }])

    # Tick at 09:25:30 belongs to the NEW 10m bucket starting at 09:25:00
    raw_tick = pl.DataFrame([{
        "instrument_token": 101,
        "date_time": datetime(2026, 9, 10, 9, 25, 30),
        "last_price": 106.0
    }])
    updated = update_candles_incremental(df_hist, raw_tick, "10m", max_bars=25, default_origin="09:15")

    assert len(updated) == 2
    rows = updated.to_dicts()

    # Index 0 is the newest candle (09:25:00)
    new_bar = rows[0]
    assert new_bar["date_time"] == datetime(2026, 9, 10, 9, 25, 0)
    assert new_bar["open"] == 103.5, "New candle open must equal previous candle close"
    assert new_bar["close"] == 106.0
    assert new_bar["high"] == 106.0
    assert new_bar["low"] == 103.5

    # Index 1 is the previous candle (09:15:00)
    prev_bar = rows[1]
    assert prev_bar["date_time"] == t0
    assert prev_bar["close"] == 103.5


def test_group_by_rolling_window_per_token_25_bars_retention():
    """
    Verify that group_by_rolling_window retains at least 25 bars per instrument token
    and removes only the oldest excess bars on a per-token basis without truncating
    tokens that have fewer than 25 bars.
    """
    base_time = datetime(2026, 9, 10, 15, 0, 0)
    
    # Token 101 has 30 bars (spaced by 10m)
    times_101 = [base_time - timedelta(minutes=10 * i) for i in range(30)]
    df_101 = pl.DataFrame({
        "instrument_token": [101] * 30,
        "date_time": times_101,
        "open": [100.0] * 30,
        "high": [105.0] * 30,
        "low": [95.0] * 30,
        "close": [102.0] * 30,
    })

    # Token 202 has 10 bars
    times_202 = [base_time - timedelta(minutes=10 * i) for i in range(10)]
    df_202 = pl.DataFrame({
        "instrument_token": [202] * 10,
        "date_time": times_202,
        "open": [50.0] * 10,
        "high": [52.0] * 10,
        "low": [48.0] * 10,
        "close": [51.0] * 10,
    })

    df_hist = pl.concat([df_101, df_202])
    
    # Fresh tick for 101 and 202
    df_raw = pl.DataFrame({
        "instrument_token": [101, 202],
        "date_time": [base_time + timedelta(minutes=10), base_time + timedelta(minutes=10)],
        "last_price": [103.0, 52.0],
        "open": [103.0, 52.0],
        "high": [103.0, 52.0],
        "low": [103.0, 52.0],
        "close": [103.0, 52.0],
    })

    res = group_by_rolling_window(df_hist, df_raw, "10m", max_bars=25)

    # Token 101 should have exactly 25 bars (oldest excess removed)
    t101_bars = res.filter(pl.col("instrument_token") == 101)
    assert len(t101_bars) == 25

    # Token 202 should have 11 bars (10 hist + 1 new, none discarded since < 25)
    t202_bars = res.filter(pl.col("instrument_token") == 202)
    assert len(t202_bars) == 11

    # Most recent bar for 101 is the new tick
    assert t101_bars["date_time"].max() == base_time + timedelta(minutes=10)


def test_read_db_candle_loads_all_tables_without_date_discard():
    """Verify that read_db_candle loads prior intraday tables to warm start technical indicators."""
    algo = IndianTradeAlgo.__new__(IndianTradeAlgo)
    algo.primary_broker = MagicMock()
    algo.primary_broker.account_id = "W1NPY"

    today = today_ist()
    yesterday = today - timedelta(days=1)

    hist_intraday = [
        {"instrument_token": 101, "date_time": f"{yesterday} 15:20:00", "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0}
    ]
    yesterday_day_cdl = [
        {"instrument_token": 101, "date_time": f"{yesterday} 15:30:00", "open": 98.0, "high": 106.0, "low": 94.0, "close": 102.0}
    ]

    def mock_get_table_data(account, tablename):
        if tablename in ('fwd_10_all', 'fwd_30_all', 'fwd_60_all'):
            return {tablename: hist_intraday}
        elif tablename == 'day_cdl_all':
            return {tablename: yesterday_day_cdl}
        return {}

    with patch("kalai.models.AlgoInfo.get_table_data", side_effect=mock_get_table_data):
        algo.read_db_candle()

        # Intraday tables must be loaded
        assert hasattr(algo, 'fwd_10_all')
        assert len(algo.fwd_10_all) == 1
        assert algo.fwd_10_all['instrument_token'][0] == 101

        assert hasattr(algo, 'day_cdl_all')
        assert len(algo.day_cdl_all) == 1


def test_check_eod_cleanup_preserves_intraday_candles_in_db():
    """Verify check_eod_cleanup preserves day_cdl_all into prev_day_cdl_all, and does NOT wipe fwd_10_all in DB."""
    algo = IndianTradeAlgo.__new__(IndianTradeAlgo)
    algo.tz = "Asia/Kolkata"
    algo.current_trading_day = today_ist()
    algo._eod_cleared_date = None

    mock_broker = MagicMock()
    mock_broker.account_id = "W1NPY"
    algo._get_target_brokers = MagicMock(return_value=[mock_broker])
    algo.sanitize_for_json = lambda df: df.to_dicts()

    sample_day_cdl = pl.DataFrame({
        "instrument_token": [101],
        "date_time": ["2026-09-09 15:30:00"],
        "open": [100.0],
        "high": [105.0],
        "low": [95.0],
        "close": [102.0],
    })
    algo.day_cdl_all = sample_day_cdl
    algo.prev_day_cdl_all = pl.DataFrame()
    algo.fwd_10_all = sample_day_cdl.clone()
    algo.tick_data = pl.DataFrame({"instrument_token": [101], "last_price": [102.0]})
    algo.delta_tick_data = pl.DataFrame({"instrument_token": [101], "last_price": [102.0]})

    algo.check_eod_cleanup(force=True)

    assert not algo.prev_day_cdl_all.is_empty()
    assert algo.prev_day_cdl_all["instrument_token"][0] == 101
    assert algo.tick_data.is_empty()
    assert algo.delta_tick_data.is_empty()

    calls = {call[0][0]: call[0][1] for call in mock_broker.set_algo_state.call_args_list}
    assert "prev_day_cdl_all" in calls
    assert "fwd_10_all" not in calls, "fwd_10_all must NOT be reset to [] in DB on EOD cleanup"


def test_crypto_engine_candle_retention_parity():
    """Verify that CryptoTradeAlgo update_all_candles_batch maintains per-token 25 bars."""
    algo = CryptoTradeAlgo.__new__(CryptoTradeAlgo)
    algo.primary_broker = MagicMock()
    algo.primary_broker.account_id = "Prvn_coinswitch"
    algo.fwd_1_all = pl.DataFrame()
    algo.fwd_3_all = pl.DataFrame()
    algo.fwd_5_all = pl.DataFrame()
    algo.fwd_10_all = pl.DataFrame()
    algo.fwd_15_all = pl.DataFrame()
    algo.fwd_30_all = pl.DataFrame()
    algo.fwd_60_all = pl.DataFrame()
    algo.day_cdl_all = pl.DataFrame()
    algo.prev_day_cdl_all = pl.DataFrame()
    algo.write_db_candle = MagicMock()

    now = datetime(2026, 9, 10, 12, 0, 0)
    ticks_1001 = [
        {"instrument_token": 1001, "date_time": now - timedelta(minutes=i), "last_price": 50000.0 + i}
        for i in range(30)
    ]
    ticks_1002 = [
        {"instrument_token": 1002, "date_time": now - timedelta(minutes=i), "last_price": 3000.0 + i}
        for i in range(10)
    ]
    tick_df = pl.DataFrame(ticks_1001 + ticks_1002)

    algo.update_all_candles_batch(tick_df)

    btc_3m = algo.fwd_3_all.filter(pl.col("instrument_token") == 1001)
    assert len(btc_3m) <= 25, "3m candles are capped at max_bars=25"

    assert not algo.fwd_10_all.is_empty()
    tokens = algo.fwd_10_all["instrument_token"].unique().to_list()
    assert 1001 in tokens
    assert 1002 in tokens


def test_pure_last_price_ignores_24h_summary_extremes():
    """Verify that broker summary 24h OHLC fields in raw ticks do NOT pollute candle open/high/low/close."""
    now = datetime(2026, 9, 10, 12, 0, 0)
    # Ticks with extreme broker 24h summary fields (high=99999, low=1) but genuine last_price=76500-76600
    ticks = [
        {
            "instrument_token": 27,
            "date_time": now + timedelta(seconds=i * 10),
            "last_price": 76500.0 + i,
            "open": 99999.0,
            "high": 99999.0,
            "low": 1.0,
            "close": 99999.0,
        }
        for i in range(10)
    ]
    raw_df = pl.DataFrame(ticks)

    cdl = group_by_rolling_window(pl.DataFrame(), raw_df, window_size="10min", max_bars=25)
    assert not cdl.is_empty()
    row = cdl.row(0, named=True)
    assert row["high"] < 80000.0, f"Expected real market high based on last_price, got {row['high']}"
    assert row["low"] > 70000.0, f"Expected real market low based on last_price, got {row['low']}"
    assert row["open"] == 76500.0
    assert row["close"] == 76509.0


def test_multi_interval_batch_bootstrap_parity():
    """Verify that a batch of ticks spanning multiple hours properly downsamples into multiple candle bars."""
    now = datetime(2026, 9, 10, 8, 0, 0)
    # 30 intervals of 10 minutes = 300 minutes (5 hours)
    ticks = []
    for interval in range(30):
        t_base = now + timedelta(minutes=interval * 10)
        for s in [0, 60, 120, 300]:
            ticks.append({
                "instrument_token": 27,
                "date_time": t_base + timedelta(seconds=s),
                "last_price": 76000.0 + interval * 10,
            })
    raw_df = pl.DataFrame(ticks)

    # Existing df_hist has 2 old bars
    df_hist = pl.DataFrame({
        "open": [75000.0, 75100.0],
        "high": [75200.0, 75300.0],
        "low": [74900.0, 75000.0],
        "close": [75100.0, 75200.0],
        "instrument_token": [27, 27],
        "date_time": [now - timedelta(minutes=20), now - timedelta(minutes=10)],
    })

    # When update_candles_incremental receives multi-interval batch, it must delegate and produce 25+ bars
    res = update_candles_incremental(df_hist, raw_df, window_size="10min", max_bars=25)
    assert len(res) == 25, f"Expected 25 bars from multi-interval bootstrap, got {len(res)}"


def test_group_by_rolling_window_merges_overlapping_intervals_without_distortion():
    """
    Verify that when group_by_rolling_window merges new ticks into an existing candle
    (such as a daily or hourly bar currently in progress), it preserves the original open,
    takes the true maximum high and minimum low, and updates to the latest close,
    rather than overwriting the entire historical bar with the partial tick batch.
    """
    day_ts = datetime(2026, 9, 11, 0, 0, 0)
    # Existing historical daily bar formed over the day
    df_hist = pl.DataFrame({
        "open": [76554.0],
        "high": [79701.0],
        "low": [76069.5],
        "close": [77800.0],
        "instrument_token": [27],
        "date_time": [day_ts],
    })

    # Incoming batch of recent ticks (e.g. from 23:10 to 23:20 with smaller range)
    recent_ticks = pl.DataFrame([
        {"instrument_token": 27, "last_price": 77115.0, "date_time": datetime(2026, 9, 11, 23, 10, 0)},
        {"instrument_token": 27, "last_price": 77180.0, "date_time": datetime(2026, 9, 11, 23, 15, 0)},
        {"instrument_token": 27, "last_price": 77168.0, "date_time": datetime(2026, 9, 11, 23, 20, 0)},
    ])

    merged = group_by_rolling_window(df_hist, recent_ticks, window_size="1D", max_bars=25)
    assert len(merged) == 1
    row = merged.to_dicts()[0]

    # Original open must be preserved
    assert row["open"] == 76554.0, f"Expected open 76554.0, got {row['open']}"
    # True day high (79701.0) must NOT be lost to the batch high (77180.0)
    assert row["high"] == 79701.0, f"Expected high 79701.0, got {row['high']}"
    # True day low (76069.5) must NOT be lost to the batch low (77115.0)
    assert row["low"] == 76069.5, f"Expected low 76069.5, got {row['low']}"
    # Latest close must be updated
    assert row["close"] == 77168.0, f"Expected close 77168.0, got {row['close']}"


def test_group_by_rolling_window_heals_flat_degenerate_bars_and_handles_iso_strings():
    """
    Verify that when df_hist contains a degenerate flat bar (open == high == low == close),
    and new ticks arrive with genuine price action and ISO timezone strings,
    the flat bar is healed with the real open, high, low, and close from the ticks.
    """
    day_ts = datetime(2026, 9, 11, 0, 0, 0)
    # Degenerate flat bar (e.g. from single tick startup artifact)
    df_hist = pl.DataFrame({
        "open": [77199.0],
        "high": [77199.0],
        "low": [77199.0],
        "close": [77199.0],
        "instrument_token": [27],
        "date_time": [day_ts],
    })

    # Incoming ticks with ISO strings containing UTC timezone offsets
    recent_ticks = pl.DataFrame([
        {"instrument_token": 27, "last_price": 76554.0, "date_time": "2026-09-11T00:00:01.271083+00:00"},
        {"instrument_token": 27, "last_price": 79701.0, "date_time": "2026-09-11T12:30:00.123456+00:00"},
        {"instrument_token": 27, "last_price": 76069.5, "date_time": "2026-09-11T18:45:00.000000+00:00"},
        {"instrument_token": 27, "last_price": 77199.0, "date_time": "2026-09-11T23:59:56.725469+00:00"},
    ])

    merged = group_by_rolling_window(df_hist, recent_ticks, window_size="1D", max_bars=25)
    assert len(merged) == 1
    row = merged.to_dicts()[0]

    # Open must be replaced by the genuine tick open (76554.0), not the degenerate flat bar's 77199.0
    assert row["open"] == 76554.0, f"Expected healed open 76554.0, got {row['open']}"
    assert row["high"] == 79701.0, f"Expected high 79701.0, got {row['high']}"
    assert row["low"] == 76069.5, f"Expected low 76069.5, got {row['low']}"
    assert row["close"] == 77199.0, f"Expected close 77199.0, got {row['close']}"


