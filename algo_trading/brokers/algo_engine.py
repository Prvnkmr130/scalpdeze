"""
algo_trading/brokers/algo_engine.py
───────────────────────────────
Main orchestrator for the synchronous sequential Algorithm Execution engine.

Fetches active broker accounts and executes all registered algorithms in strict
sequential order on a fixed cycle interval (default 2.0 seconds).
Eliminates asyncpg and thread context switches for deterministic, low-memory execution.

Usage:
    python manage.py run_algo_engine
"""

from __future__ import annotations

import gc
import logging
import os
import signal
import sys
import time
from datetime import datetime, time as dt_time, timedelta
from typing import List, Optional

logger = logging.getLogger("algo_trading.brokers.algo_engine")

from algo_trading.algos import load_all_algos, run_algos_sequential, REGISTERED_ALGOS
from algo_trading.algos.logger import algo_logger
from algo_trading.brokers.config import engine_config


def is_account_active(
    now: datetime,
    enable_trade: bool,
    ws_start_time: Optional[dt_time] = None,
    ws_stop_time: Optional[dt_time] = None,
    ws_operating_days: Optional[str] = None,
    enable_schedule: bool = True,
    is_crypto: bool = False,
) -> bool:
    """
    Determines if an account is currently active based on its trading status,
    crypto market nature (24/7/365), and configured schedule constraints.
    - If enable_trade is False, account is inactive.
    - Crypto accounts (Delta, CoinSwitch, etc.) trade 24/7/365 without schedule constraints.
    - If enable_schedule is False, account runs 24/7 without schedule constraints.
    - Otherwise, standard market hours and operating days apply.
    """
    if not enable_trade:
        return False

    # Cryptocurrency markets operate 24/7/365
    if is_crypto:
        return True

    # If scheduling is explicitly disabled, the account runs without time constraints
    if not enable_schedule:
        return True

    current_time = now.time()
    day_code = now.strftime("%a").upper()[:3]

    op_days = ws_operating_days or "ALL"
    if op_days == "ALL":
        is_operating_day = True
    elif op_days == "WEEKDAYS":
        is_operating_day = day_code in ["MON", "TUE", "WED", "THU", "FRI"]
    else:
        op_days_list = [d.strip().upper()[:3] for d in op_days.split(",")]
        is_operating_day = "ALL" in op_days_list or day_code in op_days_list

    if not is_operating_day:
        return False

    if ws_start_time and ws_stop_time:
        if ws_start_time <= ws_stop_time:
            return ws_start_time <= current_time <= ws_stop_time
        else:
            return current_time >= ws_start_time or current_time <= ws_stop_time

    return True


def get_active_scheduled_accounts() -> List[str]:
    """
    Queries the database via Django ORM and returns account IDs for currently active brokers.
    """
    try:
        from kalai.models import Broker
        brokers = Broker.objects.filter(enable_trade=True)
        now = datetime.now()
        active_ids: List[str] = []
        for b in brokers:
            acc_id = b.account_id or b.name
            if acc_id and is_account_active(
                now=now,
                enable_trade=b.enable_trade,
                ws_start_time=b.ws_start_time,
                ws_stop_time=b.ws_stop_time,
                ws_operating_days=b.ws_operating_days,
                enable_schedule=getattr(b, "enable_schedule", True),
                is_crypto=getattr(b, "is_crypto", False),
            ):
                active_ids.append(acc_id)
        return active_ids
    except Exception as e:
        logger.error(f"Failed to query active accounts: {e}")
        return []


def run_engine_sequential(loop_interval_seconds: Optional[float] = None) -> None:
    """
    Synchronous sequential engine execution loop.
    Executes all registered algorithms sequentially for active accounts every `loop_interval_seconds` (default 2.0s).
    """
    interval = loop_interval_seconds if loop_interval_seconds is not None else engine_config.ALGO_LOOP_INTERVAL
    logger.info(f"Starting Synchronous Sequential Algorithm Engine [Loop Interval: {interval:.2f}s]...")

    load_all_algos(filter_by_enabled=True)
    if not REGISTERED_ALGOS:
        logger.warning("No trading algorithms registered! Engine running in standby.")

    running = True

    def _stop_handler(signum, frame):
        nonlocal running
        sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        logger.info(f"Received signal {sig_name}. Stopping sequential algorithm engine cleanly...")
        running = False

    try:
        signal.signal(signal.SIGINT, _stop_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _stop_handler)
    except Exception:
        pass

    iteration_count = 0
    last_account_check = 0.0
    active_accounts: List[str] = []

    while running:
        cycle_start = time.time()

        # Re-check active accounts periodically (every 5 seconds)
        if (cycle_start - last_account_check) > 5.0:
            new_accounts = get_active_scheduled_accounts()
            if set(new_accounts) != set(active_accounts):
                active_accounts = new_accounts
                load_all_algos(filter_by_enabled=True)
            last_account_check = cycle_start
            if active_accounts:
                logger.debug(f"Active accounts scheduled: {active_accounts}")

        if active_accounts:
            res = run_algos_sequential(accounts=active_accounts, filter_by_enabled=False, algos=REGISTERED_ALGOS)
            iteration_count += 1
            if res.get("errors"):
                logger.warning(f"Cycle {iteration_count} completed with {len(res['errors'])} error(s): {res['errors']}")

        # Periodic query cache clearing and garbage collection every 10 cycles
        if iteration_count % 10 == 0:
            from django.db import reset_queries
            reset_queries()
            gc.collect()
            try:
                import ctypes
                if sys.platform == "win32":
                    ctypes.windll.psapi.EmptyWorkingSet(ctypes.windll.kernel32.GetCurrentProcess())
                else:
                    ctypes.CDLL("libc.so.6").malloc_trim(0)
            except Exception:
                pass

        cycle_duration = time.time() - cycle_start
        sleep_seconds = max(0.0, interval - cycle_duration)

        if sleep_seconds > 0 and running:
            time.sleep(sleep_seconds)

    # Clean shutdown
    algo_logger.flush_sync(force=True)
    logger.info("Sequential algorithm engine cleanly stopped.")


def start_algo_engine() -> None:
    """
    Main entrypoint — sets up Django and starts the sequential algo engine.
    """
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
    import django
    django.setup()

    try:
        run_engine_sequential()
    except KeyboardInterrupt:
        logger.info("Algorithm engine stopped via KeyboardInterrupt.")
