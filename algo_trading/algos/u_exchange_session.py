# -*- coding: utf-8 -*-
"""
algo_trading/algos/u_exchange_session.py
────────────────────────────────────────
Dedicated market timing, exchange hours, holiday, and session evaluation engine
for the U-Exchange (US Equities & Options — America/New_York timezone).
Supports Pre-Market, Regular Trading Hours (RTH), Post-Market Settlement,
and next-trading-day RTC wake scheduling.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time as dt_time, timedelta
from typing import List, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger("algo_trading.algos.u_exchange_session")

ET_TZ = ZoneInfo("America/New_York")


def get_now_et() -> datetime:
    """Returns the current timestamp in America/New_York as a timezone-aware datetime."""
    return datetime.now(ET_TZ)


def now_et_naive() -> datetime:
    """Returns current America/New_York time as a timezone-naive datetime."""
    now = get_now_et()
    return now.replace(tzinfo=None)


def today_et() -> date:
    """Returns today's calendar date in America/New_York."""
    return get_now_et().date()


def tomorrow_et() -> date:
    """Returns tomorrow's calendar date in America/New_York."""
    return today_et() + timedelta(days=1)


def is_weekend_et(d: Optional[date] = None) -> bool:
    """Returns True if the given date (defaulting to today ET) is Saturday or Sunday."""
    target = d or today_et()
    return target.weekday() in (5, 6)


def _easter_date(year: int) -> date:
    """Computes Easter Sunday for a given Gregorian year (Meeus/Jones/Butcher algorithm)."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _observed_date(d: date) -> date:
    """Applies standard US Federal / Exchange observation rule (Sat -> Fri, Sun -> Mon)."""
    if d.weekday() == 5:  # Saturday
        return d - timedelta(days=1)
    elif d.weekday() == 6:  # Sunday
        return d + timedelta(days=1)
    return d


def get_us_exchange_holidays(year: int) -> List[date]:
    """
    Returns the official list of US stock and options exchange (NYSE/NASDAQ/CBOE)
    market holidays for the given year.
    """
    holidays: List[date] = []

    # 1. New Year's Day (Jan 1)
    nyd = _observed_date(date(year, 1, 1))
    if nyd.year == year:
        holidays.append(nyd)

    # 2. Martin Luther King, Jr. Day (Third Monday of January)
    jan_first = date(year, 1, 1)
    mlk = jan_first + timedelta(days=(0 - jan_first.weekday() + 7) % 7 + 14)
    holidays.append(mlk)

    # 3. Washington's Birthday / Presidents' Day (Third Monday of February)
    feb_first = date(year, 2, 1)
    presidents = feb_first + timedelta(days=(0 - feb_first.weekday() + 7) % 7 + 14)
    holidays.append(presidents)

    # 4. Good Friday (Friday before Easter)
    easter = _easter_date(year)
    good_friday = easter - timedelta(days=2)
    holidays.append(good_friday)

    # 5. Memorial Day (Last Monday of May)
    may_last = date(year, 5, 31)
    memorial = may_last - timedelta(days=(may_last.weekday() - 0) % 7)
    holidays.append(memorial)

    # 6. Juneteenth National Independence Day (June 19, observed since 2022)
    if year >= 2022:
        holidays.append(_observed_date(date(year, 6, 19)))

    # 7. Independence Day (July 4)
    holidays.append(_observed_date(date(year, 7, 4)))

    # 8. Labor Day (First Monday of September)
    sep_first = date(year, 9, 1)
    labor = sep_first + timedelta(days=(0 - sep_first.weekday() + 7) % 7)
    holidays.append(labor)

    # 9. Thanksgiving Day (Fourth Thursday of November)
    nov_first = date(year, 11, 1)
    thanksgiving = nov_first + timedelta(days=(3 - nov_first.weekday() + 7) % 7 + 21)
    holidays.append(thanksgiving)

    # 10. Christmas Day (December 25)
    holidays.append(_observed_date(date(year, 12, 25)))

    return sorted(holidays)


def is_us_holiday(d: Optional[date] = None) -> bool:
    """Returns True if the given date (default today ET) is an official US exchange holiday."""
    target = d or today_et()
    holidays = get_us_exchange_holidays(target.year)
    return target in holidays


def is_trading_day(d: Optional[date] = None) -> bool:
    """Returns True if the given date is an active trading day (not a weekend and not a holiday)."""
    target = d or today_et()
    return (not is_weekend_et(target)) and (not is_us_holiday(target))


def get_next_trading_day(from_date: Optional[date] = None) -> date:
    """Returns the next valid trading day strictly after `from_date` (default today ET)."""
    curr = (from_date or today_et()) + timedelta(days=1)
    while not is_trading_day(curr):
        curr += timedelta(days=1)
    return curr


def is_pre_market_time(now_dt: Optional[datetime] = None) -> bool:
    """Returns True if current time is within US Pre-Market window (04:00:00 to 09:30:00 ET)."""
    dt = now_dt or now_et_naive()
    current_time = dt.time()
    return dt_time(4, 0, 0) <= current_time < dt_time(9, 30, 0)


def is_regular_trading_hours(now_dt: Optional[datetime] = None) -> bool:
    """Returns True if current time is within US Regular Trading Hours (09:30:00 to 16:00:00 ET)."""
    dt = now_dt or now_et_naive()
    current_time = dt.time()
    return dt_time(9, 30, 0) <= current_time <= dt_time(16, 0, 0)


def is_post_market_time(now_dt: Optional[datetime] = None) -> bool:
    """Returns True if current time is within US Post-Market Settlement (16:00:00 to 16:15:00 ET)."""
    dt = now_dt or now_et_naive()
    current_time = dt.time()
    return dt_time(16, 0, 0) < current_time <= dt_time(16, 15, 0)


def is_eod_report_time(now_dt: Optional[datetime] = None) -> bool:
    """Returns True if current time is past EOD report generation cutoff (16:05:00 ET)."""
    dt = now_dt or now_et_naive()
    current_time = dt.time()
    return current_time >= dt_time(16, 5, 0)


def is_hibernate_time(now_dt: Optional[datetime] = None) -> bool:
    """Returns True if current time is at or past post-market auto-hibernate cutoff (16:15:00 ET)."""
    dt = now_dt or now_et_naive()
    current_time = dt.time()
    return current_time >= dt_time(16, 15, 0)


def is_active_session_time(enable_pre_market: bool = False, now_dt: Optional[datetime] = None) -> bool:
    """
    Returns True if current ET time is within active streaming / analytical window on a trading day.
    """
    dt = now_dt or now_et_naive()
    if not is_trading_day(dt.date()):
        return False

    if enable_pre_market and is_pre_market_time(dt):
        return True

    return is_regular_trading_hours(dt) or is_post_market_time(dt)


def get_next_trading_day_wake_datetime(
    enable_pre_market: bool = False,
    rth_wake_time: str = "09:15",
    pre_market_wake_time: str = "03:45",
    from_dt: Optional[datetime] = None,
) -> datetime:
    """
    Computes the exact naive datetime in local ET for when the PC should wake up next.
    If today is still before the wake time on an active trading day, returns today's wake time.
    Otherwise, returns the wake time on the next valid trading day.
    """
    now = from_dt or now_et_naive()
    today_d = now.date()

    wake_str = pre_market_wake_time if enable_pre_market else rth_wake_time
    parts = [int(p) for p in wake_str.split(":")]
    wake_t = dt_time(parts[0], parts[1], 0)

    # Check if today is a trading day and wake time is still in the future
    if is_trading_day(today_d) and now.time() < wake_t:
        return datetime.combine(today_d, wake_t)

    # Otherwise next trading day
    next_d = get_next_trading_day(from_date=today_d)
    return datetime.combine(next_d, wake_t)


class UExchangeSession:
    """
    Session coordinator and state holder for U-Exchange execution.
    """

    def __init__(self, enable_pre_market: bool = False):
        self.enable_pre_market = enable_pre_market

    @property
    def is_trading_day(self) -> bool:
        return is_trading_day()

    @property
    def is_active_session(self) -> bool:
        return is_active_session_time(enable_pre_market=self.enable_pre_market)

    @property
    def session_phase(self) -> str:
        """Returns 'PRE_MARKET', 'RTH', 'POST_MARKET', or 'OFF_HOURS'."""
        if not is_trading_day():
            return "OFF_HOURS"
        if is_pre_market_time():
            return "PRE_MARKET"
        if is_regular_trading_hours():
            return "RTH"
        if is_post_market_time():
            return "POST_MARKET"
        return "OFF_HOURS"

    def next_wake_time(self) -> datetime:
        return get_next_trading_day_wake_datetime(enable_pre_market=self.enable_pre_market)
