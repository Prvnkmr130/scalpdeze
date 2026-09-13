# -*- coding: utf-8 -*-
"""
algo_trading/brokers/sniffer
────────────────────────────
Windows Thin Client Scrapling Stealth Browser Sniffing, Dual-NIC Failover,
Hardware Thermal Protection, and Power Management Package.
"""

from .browser_sniffer import ScraplingBrowserSniffer, SniffedMarketDataStore
from .network_failover import DualNicFailoverManager
from .power_manager import WindowsPowerManager
from .thermal_guard import ThermalGuard
from .tray_applet import WindowsTrayApplet, send_windows_toast, setup_global_hotkey

__all__ = [
    "ScraplingBrowserSniffer",
    "SniffedMarketDataStore",
    "DualNicFailoverManager",
    "WindowsPowerManager",
    "ThermalGuard",
    "WindowsTrayApplet",
    "send_windows_toast",
    "setup_global_hotkey",
]
