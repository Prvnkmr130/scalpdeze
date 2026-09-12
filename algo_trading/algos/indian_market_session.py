# -*- coding: utf-8 -*-
"""
algo_trading/algos/indian_market_session.py
───────────────────────────────────────────
Dedicated market timing, exchange hours, and holiday evaluation engine
for Indian financial markets (NSE, NFO, CDS, MCX, BSE, BFO).
"""

from __future__ import annotations

import logging
from datetime import datetime, time as dt_time, timedelta, date
from typing import Any, List, Optional

import polars as pl

from algo_trading.algos.indian_master_tokens import today_ist, now_ist_naive

logger = logging.getLogger("algo_trading.algos.indian_market_session")


def time_ist(tz: str = "Asia/Kolkata") -> dt_time:
    """Returns the current time in Asia/Kolkata as a naive time object."""
    return now_ist_naive(tz=tz).time()


def is_weekend_ist(tz: str = "Asia/Kolkata") -> bool:
    """Returns True if today is Saturday or Sunday in Asia/Kolkata."""
    return today_ist(tz=tz).weekday() in (5, 6)


def tomorrow_ist(tz: str = "Asia/Kolkata") -> date:
    """Returns tomorrow's date in Asia/Kolkata timezone."""
    return today_ist(tz=tz) + timedelta(days=1)


def check_for_orderplacing_time(tz: str = "Asia/Kolkata", day_light_saving: bool = True) -> bool:
    """True if within general Indian exchange trading hours (09:00 to 15:30 or 23:30 for MCX)."""
    now_dt = now_ist_naive(tz=tz)
    start_time = now_dt.replace(hour=9, minute=0, second=0, microsecond=0)
    end_time = now_dt.replace(hour=23, minute=30 if day_light_saving else 55, second=0, microsecond=0)
    return start_time <= now_dt <= end_time


def initialising_time(tz: str = "Asia/Kolkata", day_light_saving: bool = True) -> bool:
    """True when engine initialization should run (prior to market open)."""
    now_dt = now_ist_naive(tz=tz)
    init_start = now_dt.replace(hour=8, minute=30, second=0, microsecond=0)
    init_end = now_dt.replace(hour=23, minute=55, second=0, microsecond=0)
    return init_start <= now_dt <= init_end


def is_post_trade_time(tz: str = "Asia/Kolkata", day_light_saving: bool = True) -> bool:
    """True after Indian market close (15:30 onwards or 23:30+ for MCX)."""
    now_dt = now_ist_naive(tz=tz)
    post_time = now_dt.replace(hour=15, minute=30, second=0, microsecond=0)
    return now_dt >= post_time


class IndianMarketSession:
    """
    Evaluates exchange market hours, holidays, opening/closing bounds,
    and intra-day trading windows for Indian exchanges.
    """

    def __init__(
        self,
        holiday_dates: Optional[List[date]] = None,
        nse_holiday_info: Optional[pl.DataFrame] = None,
        special_session: bool = False,
        day_light_saving: bool = True,
        timezone: str = "Asia/Kolkata",
        debug_mode: bool = False,
    ):
        self.holiday_dates: List[date] = holiday_dates or []
        self.nse_holiday_info: pl.DataFrame = nse_holiday_info if nse_holiday_info is not None else pl.DataFrame()
        self.special_session = special_session
        self.day_light_saving = day_light_saving
        self.tz = timezone
        self.debug_mode = debug_mode

        if not self.holiday_dates and not self.nse_holiday_info.is_empty():
            self.holiday_dates = self._extract_holiday_dates()

    def _extract_holiday_dates(self) -> List[date]:
        if self.nse_holiday_info.is_empty() or "Date" not in self.nse_holiday_info.columns:
            return []
        try:
            col = self.nse_holiday_info["Date"]
            if col.dtype == pl.String:
                dates = col.str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).drop_nulls().to_list()
                return [d for d in dates if isinstance(d, date)]
            elif col.dtype in (pl.Datetime, pl.Date):
                dates = col.dt.date().drop_nulls().to_list()
                return [d for d in dates if isinstance(d, date)]
        except Exception:
            pass
        return []

    def is_weekend(self) -> bool:
        if self.debug_mode:
            return False
        return is_weekend_ist(tz=self.tz)

    def is_holiday(self, exchg: str = "NSE") -> bool:
        if self.debug_mode:
            return False
        return today_ist(tz=self.tz) in self.holiday_dates

    def special_session_chk(self, exchg: str) -> bool:
        if self.debug_mode:
            return True
        if exchg in ("MCX", "CDS") and self.special_session:
            return not self.special_session
        return self.special_session

    def next_session_closed(self, exchg: str) -> bool:
        if self.debug_mode:
            return False
        tomorrow = tomorrow_ist(tz=self.tz)
        if datetime.today().weekday() == 4:  # Friday
            return True
        return tomorrow in self.holiday_dates

    def exchg_time_buy_chk(self, exchg: str) -> bool:
        if self.debug_mode:
            return True
        t = time_ist(tz=self.tz)
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=9, minute=1, second=0, microsecond=0)

        if not self.session_end(exchg):
            hold_time = (13 <= t.minute <= 20) or (43 <= t.minute <= 50)
        else:
            hold_time = False

        if exchg == "CDS":
            end_time = now_naive.replace(hour=17, minute=0, second=0, microsecond=0)
        elif exchg == "MCX":
            if not self.session_end(exchg):
                hold_time = (58 <= t.minute <= 59 or 0 <= t.minute <= 5) or (28 <= t.minute <= 35)
            else:
                hold_time = False
            start_time = now_naive.replace(hour=9, minute=1, second=0, microsecond=0)
            end_time = now_naive.replace(
                hour=23, minute=30 if self.day_light_saving else 55, second=0, microsecond=0
            )
        else:
            end_time = now_naive.replace(hour=15, minute=40, second=0, microsecond=0)

        if now_naive < start_time or now_naive > end_time:
            return False
        return not hold_time

    def hedge_time_chk(self, exchg: str) -> bool:
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=9, minute=15, second=0, microsecond=0)
        if exchg == "CDS":
            end_time = now_naive.replace(hour=17, minute=0, second=0, microsecond=0)
        elif exchg == "MCX":
            end_time = now_naive.replace(
                hour=23, minute=30 if self.day_light_saving else 55, second=0, microsecond=0
            )
        else:
            end_time = now_naive.replace(hour=15, minute=30, second=0, microsecond=0)

        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def half_time(self, exchg: str) -> bool:
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == "CDS":
            start_time = now_naive.replace(hour=16, minute=30, second=0, microsecond=0)
            end_time = now_naive.replace(hour=16, minute=30, second=50, microsecond=0)
        elif exchg == "MCX":
            start_time = now_naive.replace(hour=18, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(
                hour=23, minute=30 if self.day_light_saving else 55, second=50, microsecond=0
            )
        else:
            start_time = now_naive.replace(hour=9, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(hour=10, minute=15, second=0, microsecond=0)

        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def exchg_time_sell_chk(self, exchg: str) -> bool:
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        if exchg in ("CDS", "MCX"):
            start_time = now_naive.replace(hour=9, minute=3, second=0, microsecond=0)
            if exchg == "CDS":
                end_time = now_naive.replace(hour=17, minute=0, second=0, microsecond=0)
            else:
                end_time = now_naive.replace(
                    hour=23, minute=30 if self.day_light_saving else 55, second=0, microsecond=0
                )
        else:
            start_time = now_naive.replace(hour=9, minute=1, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=40, second=0, microsecond=0)

        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def session_end(self, exchg: str) -> bool:
        if self.debug_mode:
            return False
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == "MCX":
            start_time = now_naive.replace(hour=23, minute=0 if self.day_light_saving else 30, second=0, microsecond=0)
            end_time = now_naive.replace(hour=23, minute=30 if self.day_light_saving else 55, second=0, microsecond=0)
        elif exchg == "CDS":
            start_time = now_naive.replace(hour=15, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=30, second=0, microsecond=0)
        else:
            start_time = now_naive.replace(hour=15, minute=20, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=40, second=0, microsecond=0)

        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def prev_cdl_save_time(self) -> bool:
        if self.debug_mode:
            return False
        now_naive = now_ist_naive(tz=self.tz)
        start_1 = now_naive.replace(hour=15, minute=39, second=0, microsecond=0)
        end_1 = now_naive.replace(hour=15, minute=40, second=0, microsecond=0)

        start_2 = now_naive.replace(hour=23, minute=30 if self.day_light_saving else 55, second=0, microsecond=0)
        end_2 = now_naive.replace(hour=23, minute=31 if self.day_light_saving else 56, second=0, microsecond=0)

        return (start_1 <= now_naive <= end_1) or (start_2 <= now_naive <= end_2)

    def pre_trade_sl_update_time(self) -> bool:
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=12, minute=45, second=0, microsecond=0)
        end_time = now_naive.replace(hour=12, minute=46, second=0, microsecond=0)
        return start_time <= now_naive <= end_time

    def sl_update_time(self, exchg: str) -> bool:
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=10, minute=15, second=0, microsecond=0)
        end_time = now_naive.replace(hour=10, minute=16, second=0, microsecond=0)

        if exchg == "MCX":
            start_time = now_naive.replace(hour=17, minute=30, second=0, microsecond=0)
            end_time = now_naive.replace(hour=17, minute=31, second=0, microsecond=0)
        elif exchg == "CDS":
            start_time = now_naive.replace(hour=17, minute=30, second=0, microsecond=0)
            end_time = now_naive.replace(hour=17, minute=30, second=59, microsecond=0)

        if self.session_end(exchg) and self.next_session_closed(exchg):
            return True
        return start_time <= now_naive <= end_time

    def strike_update_time(self, exchg: str) -> bool:
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=9, minute=15, second=0, microsecond=0)
        end_time = now_naive.replace(hour=9, minute=15, second=5, microsecond=0)

        if exchg == "MCX":
            start_time = now_naive.replace(hour=9, minute=29, second=40, microsecond=0)
            end_time = now_naive.replace(hour=9, minute=30, second=1, microsecond=0)
        elif exchg == "CDS":
            start_time = now_naive.replace(hour=9, minute=0, second=3, microsecond=0)
            end_time = now_naive.replace(hour=9, minute=0, second=8, microsecond=0)

        return start_time <= now_naive <= end_time

    def session_reset(self, exchg: str) -> bool:
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == "MCX":
            start_time = now_naive.replace(hour=23, minute=10 if self.day_light_saving else 35, second=0, microsecond=0)
            end_time = now_naive.replace(hour=23, minute=20 if self.day_light_saving else 45, second=0, microsecond=0)
        elif exchg == "CDS":
            start_time = now_naive.replace(hour=16, minute=40, second=0, microsecond=0)
            end_time = now_naive.replace(hour=16, minute=50, second=0, microsecond=0)
        else:
            start_time = now_naive.replace(hour=15, minute=10, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=20, second=0, microsecond=0)

        return start_time <= now_naive <= end_time

    def session_start(self, exchg: str) -> bool:
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == "MCX":
            start_time = now_naive.replace(hour=9, minute=1, second=0, microsecond=0)
            end_time = now_naive.replace(hour=9, minute=45, second=0, microsecond=0)
        elif exchg == "CDS":
            start_time = now_naive.replace(hour=9, minute=1, second=0, microsecond=0)
            end_time = now_naive.replace(hour=9, minute=30, second=0, microsecond=0)
        else:
            start_time = now_naive.replace(hour=9, minute=25, second=0, microsecond=0)
            end_time = now_naive.replace(hour=9, minute=45, second=0, microsecond=0)

        return start_time <= now_naive <= end_time

    def expiry_sell_time(self, exchg: str) -> bool:
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == "MCX":
            start_time = now_naive.replace(hour=11, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(
                hour=23, minute=30 if self.day_light_saving else 55, second=0, microsecond=0
            )
        elif exchg == "CDS":
            start_time = now_naive.replace(hour=15, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(hour=17, minute=0, second=0, microsecond=0)
        else:
            start_time = now_naive.replace(hour=15, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=40, second=0, microsecond=0)

        return start_time <= now_naive <= end_time

    def post_trade_time(self, exchg: str) -> bool:
        if self.debug_mode:
            return False
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == "MCX":
            start_time = now_naive.replace(hour=23, minute=30 if self.day_light_saving else 55, second=0, microsecond=0)
            end_time = now_naive.replace(hour=23, minute=30 if self.day_light_saving else 55, second=10, microsecond=0)
        elif exchg == "CDS":
            start_time = now_naive.replace(hour=17, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(hour=17, minute=0, second=10, microsecond=0)
        else:
            start_time = now_naive.replace(hour=15, minute=40, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=40, second=10, microsecond=0)

        return start_time <= now_naive <= end_time
