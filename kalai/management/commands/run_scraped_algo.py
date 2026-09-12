# -*- coding: utf-8 -*-
"""
kalai/management/commands/run_scraped_algo.py
─────────────────────────────────────────────
Unified Management Command: run_scraped_algo
Orchestrator for the Windows Thin Client Quantitative Signal Engine.

Coordinates:
- U-Exchange market session & calendar gates.
- Scrapling stealth browser sniffer & context pooling.
- Dual-NIC network failover manager.
- Hardware CPU thermal watchdog with adaptive loop throttling.
- Windows native desktop SysTray icon, toasts, and Ctrl+Alt+B hotkey.
- SIMD Polars quantitative momentum & strike detection engine.
- EOD persistence, Excel export, and Task Scheduler RTC wake/hibernate triggers.

Usage:
    python manage.py run_scraped_algo
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, List, Optional

from django.core.management.base import BaseCommand

logger = logging.getLogger("kalai.management.commands.run_scraped_algo")


class Command(BaseCommand):
    help = "Start the autonomous Windows Thin Client Quantitative Signal Engine (U-Exchange)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--no-tray",
            action="store_true",
            help="Disable the Windows System Tray icon applet.",
        )
        parser.add_argument(
            "--no-failover",
            action="store_true",
            help="Disable the Dual-NIC network failover monitor.",
        )
        parser.add_argument(
            "--no-power",
            action="store_true",
            help="Disable Task Scheduler RTC wake and post-market auto-hibernation.",
        )
        parser.add_argument(
            "--symbols",
            type=str,
            default="SPY,QQQ,AAPL",
            help="Comma-separated target reference underlying symbols (default: SPY,QQQ,AAPL).",
        )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS("==================================================================")
        )
        self.stdout.write(
            self.style.SUCCESS(" Starting Windows Thin Client Quantitative Signal Engine (U-Exchange)")
        )
        self.stdout.write(
            self.style.SUCCESS("==================================================================")
        )

        from algo_trading.app_config import config
        from algo_trading.algos.logger import algo_logger
        from algo_trading.algos.u_exchange_session import (
            UExchangeSession,
            is_active_session_time,
            is_eod_report_time,
            is_hibernate_time,
            now_et_naive,
            today_et,
        )
        from algo_trading.brokers.sniffer.browser_sniffer import (
            ScraplingBrowserSniffer,
            SniffedMarketDataStore,
        )
        from algo_trading.brokers.sniffer.network_failover import DualNicFailoverManager
        from algo_trading.brokers.sniffer.power_manager import WindowsPowerManager
        from algo_trading.brokers.sniffer.thermal_guard import ThermalGuard
        from algo_trading.brokers.sniffer.tray_applet import (
            WindowsTrayApplet,
            send_windows_toast,
            setup_global_hotkey,
        )
        from algo_trading.algos.scraped_signal_engine import ScrapedSignalEngine

        ref_symbols = [s.strip().upper() for s in options["symbols"].split(",") if s.strip()]
        enable_pre_market = config.ENABLE_PRE_MARKET

        # 1. Initialize System Components
        session = UExchangeSession(enable_pre_market=enable_pre_market)
        data_store = SniffedMarketDataStore()
        browser_sniffer = ScraplingBrowserSniffer(
            portal_url=config.PORTAL_URL,
            headless=config.SCRAPLING_HEADLESS,
            data_store=data_store,
        )

        thermal_guard = ThermalGuard(
            warning_temp=config.THERMAL_WARNING_TEMP,
            critical_temp=config.THERMAL_CRITICAL_TEMP,
            check_interval=config.THERMAL_CHECK_INTERVAL,
            base_loop_interval=config.ALGO_LOOP_INTERVAL_SECONDS,
        )

        power_manager = WindowsPowerManager(
            enable_auto_wake=config.ENABLE_AUTO_WAKE and not options["no_power"],
            enable_auto_hibernate=config.ENABLE_AUTO_HIBERNATE and not options["no_power"],
        )

        # Desktop Toast & Signal Dispatcher
        def _on_signal_detected(sig: dict):
            title = f"[SIGNAL ALERT] {sig.get('path_name', 'Signal')}"
            body = (
                f"Trend: {sig.get('trend')} | CE: {sig.get('buy_signal_CE')} | "
                f"PE: {sig.get('buy_signal_PE')} | Account: {sig.get('account_id')}"
            )
            send_windows_toast(title=title, body=body, audio_chime=True)

        signal_engine = ScrapedSignalEngine(
            data_store=data_store,
            on_signal_callback=_on_signal_detected,
        )

        # 2. Network Failover Manager
        failover_mgr = None
        if not options["no_failover"]:
            def _on_failover_switch(nic_type: str):
                tray.set_status("FAILOVER" if nic_type == "BACKUP" else "ACTIVE")
                send_windows_toast(
                    title="Network Routing Switched",
                    body=f"Traffic rerouted to {nic_type} adapter: {config.BACKUP_NIC_ALIAS if nic_type == 'BACKUP' else config.PRIMARY_NIC_ALIAS}",
                    audio_chime=True,
                )

            failover_mgr = DualNicFailoverManager(
                primary_alias=config.PRIMARY_NIC_ALIAS,
                backup_alias=config.BACKUP_NIC_ALIAS,
                check_interval=config.FAILOVER_CHECK_INTERVAL,
                hysteresis_seconds=config.FAILOVER_HYSTERESIS_SECONDS,
                on_failover_callback=_on_failover_switch,
            )
            failover_mgr.start_monitoring()

        # 3. System Tray & Hotkey
        running = True

        def _request_shutdown():
            nonlocal running
            logger.info("Shutdown requested.")
            running = False

        tray = WindowsTrayApplet(
            on_toggle_browser=browser_sniffer.toggle_visibility,
            on_recycle_memory=lambda: asyncio.run(browser_sniffer.soft_memory_recycle()),
            on_hibernate_pc=lambda: power_manager.hibernate_pc(force=True),
            on_exit=_request_shutdown,
        )

        if not options["no_tray"]:
            tray.start_background()
            setup_global_hotkey(browser_sniffer.toggle_visibility)

        def _signal_handler(signum, frame):
            _request_shutdown()

        try:
            signal.signal(signal.SIGINT, _signal_handler)
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, _signal_handler)
        except Exception:
            pass

        # 4. Query active accounts from DB
        from kalai.models import Broker
        active_brokers = list(Broker.objects.filter(enable_trade=True))
        if not active_brokers:
            # Create or use fallback local virtual account
            logger.info("No active Broker accounts in DB; running in single standalone Thin Client mode.")

        # Launch browser sniffer sessions if portal configured
        if config.PORTAL_URL:
            async def _init_browser():
                if active_brokers:
                    for b in active_brokers:
                        acc_id = b.account_id or b.name
                        await browser_sniffer.start_account_session(
                            account_id=acc_id,
                            portal_url=config.PORTAL_URL,
                            username=config.PORTAL_USERNAME,
                            password=config.PORTAL_PASSWORD,
                            totp_secret=b.totp_secret or config.PORTAL_TOTP_SECRET,
                        )
                else:
                    await browser_sniffer.start_account_session(
                        account_id="LOCAL_USER",
                        portal_url=config.PORTAL_URL,
                        username=config.PORTAL_USERNAME,
                        password=config.PORTAL_PASSWORD,
                        totp_secret=config.PORTAL_TOTP_SECRET,
                    )

            try:
                asyncio.run(_init_browser())
            except Exception as e:
                logger.warning(f"Browser sniffer initialization warning: {e}")

        self.stdout.write(self.style.SUCCESS("Signal Engine initialized and active. Press Ctrl+C to stop."))

        # 5. Main Execution Loop
        iteration = 0
        last_eod_report_date = None

        while running:
            now_dt = now_et_naive()
            phase = session.session_phase

            # ── Session Lifecycle Evaluation ──
            if phase == "OFF_HOURS":
                tray.set_status("OFF_HOURS")
                # If post-market hibernate cutoff reached (>= 16:15 ET), hibernate PC
                if is_hibernate_time(now_dt) and config.ENABLE_AUTO_HIBERNATE and not options["no_power"]:
                    next_wake = session.next_wake_time()
                    logger.info(f"[SESSION_CLOSE] Post-market complete. Scheduling wake for {next_wake} and hibernating...")
                    power_manager.schedule_next_wake(next_wake)
                    algo_logger.flush_sync(force=True)
                    power_manager.hibernate_pc()
                    running = False
                    break

                time.sleep(10.0)
                continue

            elif phase == "PRE_MARKET":
                tray.set_status("STANDBY")
                if not enable_pre_market:
                    time.sleep(10.0)
                    continue

            elif phase in ("RTH", "POST_MARKET"):
                tray.set_status("ACTIVE")

            # ── Thermal Throttling Matrix ──
            loop_interval = thermal_guard.get_adaptive_loop_interval()

            # ── Analytical Cycle ──
            cycle_res = signal_engine.execute_analytical_cycle(
                ref_symbols=ref_symbols,
                active_accounts=active_brokers or ["LOCAL_USER"],
            )
            iteration += 1

            # ── Memory Recycling Check ──
            if iteration % 20 == 0:
                try:
                    asyncio.run(browser_sniffer.soft_memory_recycle())
                except Exception:
                    pass

                # Windows process working set trim
                try:
                    import ctypes
                    if sys.platform == "win32":
                        ctypes.windll.psapi.EmptyWorkingSet(ctypes.windll.kernel32.GetCurrentProcess())
                except Exception:
                    pass

            # Sleep remaining cycle duration
            duration = cycle_res.get("latency", 0.0)
            sleep_time = max(0.0, loop_interval - duration)
            if sleep_time > 0 and running:
                time.sleep(sleep_time)

        # Clean Shutdown
        self.stdout.write(self.style.NOTICE("Stopping Signal Engine cleanly..."))
        if failover_mgr:
            failover_mgr.stop_monitoring()
        if not options["no_tray"]:
            tray.stop()
        try:
            asyncio.run(browser_sniffer.close_all())
        except Exception:
            pass
        algo_logger.flush_sync(force=True)
        self.stdout.write(self.style.SUCCESS("Signal Engine terminated."))
