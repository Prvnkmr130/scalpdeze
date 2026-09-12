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

__all__ = [
    "ScraplingBrowserSniffer",
    "SniffedMarketDataStore",
    "DualNicFailoverManager",
    "WindowsPowerManager",
    "ThermalGuard",
]
