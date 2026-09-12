# -*- coding: utf-8 -*-
"""
tests/test_u_exchange_session.py
────────────────────────────────
Unit tests for U-Exchange session bounds, holiday engine, and RTC wake scheduling.
"""

from datetime import date, datetime, time as dt_time, timedelta
import pytest

from algo_trading.algos.u_exchange_session import (
    UExchangeSession,
    get_us_exchange_holidays,
    is_us_holiday,
    is_weekend_et,
    is_trading_day,
    get_next_trading_day,
    is_pre_market_time,
    is_regular_trading_hours,
    is_post_market_time,
    is_hibernate_time,
    get_next_trading_day_wake_datetime,
)


def test_us_exchange_holidays_2026():
    """Verify official US exchange holidays are accurately computed for 2026."""
    holidays = get_us_exchange_holidays(2026)
    
    # 2026 expected holidays:
    # New Year's: 2026-01-01 (Thu)
    # MLK: 2026-01-19 (Mon)
    # Presidents Day: 2026-02-16 (Mon)
    # Good Friday: 2026-04-03 (Fri)
    # Memorial Day: 2026-05-25 (Mon)
    # Juneteenth: 2026-06-19 (Fri)
    # Independence Day: 2026-07-03 (Fri, observed since July 4 is Saturday)
    # Labor Day: 2026-09-07 (Mon)
    # Thanksgiving: 2026-11-26 (Thu)
    # Christmas: 2026-12-25 (Fri)
    
    assert date(2026, 1, 1) in holidays
    assert date(2026, 1, 19) in holidays
    assert date(2026, 2, 16) in holidays
    assert date(2026, 4, 3) in holidays  # Good Friday 2026
    assert date(2026, 5, 25) in holidays
    assert date(2026, 6, 19) in holidays
    assert date(2026, 7, 3) in holidays  # Observed Independence Day
    assert date(2026, 9, 7) in holidays
    assert date(2026, 11, 26) in holidays
    assert date(2026, 12, 25) in holidays
    assert len(holidays) == 10


def test_is_weekend_and_trading_day():
    """Verify weekend detection and trading day validation."""
    saturday = date(2026, 9, 12)
    sunday = date(2026, 9, 13)
    monday = date(2026, 9, 14)

    assert is_weekend_et(saturday) is True
    assert is_weekend_et(sunday) is True
    assert is_weekend_et(monday) is False

    assert is_trading_day(saturday) is False
    assert is_trading_day(sunday) is False
    assert is_trading_day(monday) is True

    # Holiday is not a trading day
    thanksgiving = date(2026, 11, 26)
    assert is_trading_day(thanksgiving) is False


def test_get_next_trading_day_skips_weekends_and_holidays():
    """Verify get_next_trading_day correctly advances across weekends and holidays."""
    friday = date(2026, 9, 11)
    next_day = get_next_trading_day(from_date=friday)
    assert next_day == date(2026, 9, 14)  # Monday

    # Before Good Friday 2026 (Thursday 2026-04-02)
    thursday = date(2026, 4, 2)
    next_after_thursday = get_next_trading_day(from_date=thursday)
    assert next_after_thursday == date(2026, 4, 6)  # Skips Good Friday (04-03), Sat, Sun -> Mon (04-06)


def test_session_time_bounds():
    """Verify session phase categorization for various times."""
    # Pre-Market: 04:00 to 09:30 ET
    dt_pre = datetime(2026, 9, 14, 5, 30, 0)
    assert is_pre_market_time(dt_pre) is True
    assert is_regular_trading_hours(dt_pre) is False

    # RTH: 09:30 to 16:00 ET
    dt_rth = datetime(2026, 9, 14, 10, 15, 0)
    assert is_regular_trading_hours(dt_rth) is True
    assert is_pre_market_time(dt_rth) is False

    # Post-Market: 16:00 to 16:15 ET
    dt_post = datetime(2026, 9, 14, 16, 5, 0)
    assert is_post_market_time(dt_post) is True
    assert is_regular_trading_hours(dt_post) is False

    # Hibernate cutoff: >= 16:15 ET
    dt_hib = datetime(2026, 9, 14, 16, 15, 0)
    assert is_hibernate_time(dt_hib) is True


def test_next_wake_datetime():
    """Verify next wake time computation for Task Scheduler."""
    # Friday post-market at 16:30 ET -> next wake is Monday 09:15 ET
    friday_night = datetime(2026, 9, 11, 16, 30, 0)
    wake_dt = get_next_trading_day_wake_datetime(
        enable_pre_market=False,
        rth_wake_time="09:15",
        from_dt=friday_night,
    )
    assert wake_dt == datetime(2026, 9, 14, 9, 15, 0)

    # Monday early morning at 06:00 ET before RTH wake -> wake is today at 09:15 ET
    monday_early = datetime(2026, 9, 14, 6, 0, 0)
    wake_today = get_next_trading_day_wake_datetime(
        enable_pre_market=False,
        rth_wake_time="09:15",
        from_dt=monday_early,
    )
    assert wake_today == datetime(2026, 9, 14, 9, 15, 0)
