# -*- coding: utf-8 -*-
"""
algo_trading/brokers/sniffer/network_failover.py
───────────────────────────────────────────────
Dual-Network Interface Failover Manager with Anti-Flapping Hysteresis.
Monitors internet and exchange connectivity every 5 seconds on the Primary adapter.
Automatically switches Windows routing metrics (Primary Ethernet ↔ Backup Wi-Fi/4G)
via PowerShell upon 3 consecutive failures, and enforces a 60s stability window before reverting.
"""

from __future__ import annotations

import logging
import socket
import subprocess
import sys
import threading
import time
from typing import Callable, List, Optional

logger = logging.getLogger("algo_trading.brokers.sniffer.network_failover")


class DualNicFailoverManager:
    """
    Monitors dual network interfaces on Windows and manages failover routing metrics.
    """

    def __init__(
        self,
        primary_alias: str = "Ethernet",
        backup_alias: str = "Wi-Fi",
        probe_hosts: Optional[List[str]] = None,
        check_interval: float = 5.0,
        hysteresis_seconds: float = 60.0,
        on_failover_callback: Optional[Callable[[str], None]] = None,
    ):
        self.primary_alias = primary_alias
        self.backup_alias = backup_alias
        self.probe_hosts = probe_hosts or ["8.8.8.8", "1.1.1.1"]
        self.check_interval = check_interval
        self.hysteresis_seconds = hysteresis_seconds
        self.on_failover_callback = on_failover_callback

        self.current_active_nic = "PRIMARY"  # "PRIMARY" or "BACKUP"
        self._consecutive_primary_failures = 0
        self._primary_recovery_start_time: Optional[float] = None
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None

    def probe_host(self, host: str, port: int = 53, timeout: float = 1.5) -> bool:
        """Lightweight TCP probe to test endpoint reachability."""
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except (socket.timeout, OSError):
            return False

    def is_network_healthy(self) -> bool:
        """Tests if any configured probe host is reachable."""
        for host in self.probe_hosts:
            if self.probe_host(host):
                return True
        return False

    def switch_metric(self, primary_metric: int, backup_metric: int) -> bool:
        """
        Executes PowerShell to reassign interface metrics and flush DNS cache on Windows.
        """
        if sys.platform != "win32":
            logger.debug("Skipping PowerShell Set-NetIPInterface: Non-Windows OS.")
            return True

        ps_cmd = (
            f"Set-NetIPInterface -InterfaceAlias '{self.primary_alias}' -InterfaceMetric {primary_metric} -ErrorAction SilentlyContinue; "
            f"Set-NetIPInterface -InterfaceAlias '{self.backup_alias}' -InterfaceMetric {backup_metric} -ErrorAction SilentlyContinue; "
            f"Clear-DnsClientCache -ErrorAction SilentlyContinue"
        )

        try:
            res = subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_cmd],
                capture_output=True,
                text=True,
                timeout=10,
            )
            return res.returncode == 0
        except Exception as e:
            logger.error(f"Error executing metric switch command: {e}")
            return False

    def trigger_failover_to_backup(self) -> None:
        """Switches default traffic routing to the backup network adapter."""
        logger.warning(
            f"[NETWORK_FAILOVER] Primary adapter ('{self.primary_alias}') dropped 3 consecutive probes. "
            f"Executing failover to backup adapter ('{self.backup_alias}')..."
        )
        self.switch_metric(primary_metric=60, backup_metric=10)
        self.current_active_nic = "BACKUP"
        self._primary_recovery_start_time = None

        if self.on_failover_callback:
            try:
                self.on_failover_callback("BACKUP")
            except Exception as e:
                logger.error(f"Error in failover callback: {e}")

    def trigger_revert_to_primary(self) -> None:
        """Reverts default traffic routing priority back to the primary network adapter."""
        logger.info(
            f"[NETWORK_FAILOVER] Primary adapter ('{self.primary_alias}') sustained {self.hysteresis_seconds}s "
            f"of continuous stability. Gracefully reverting priority to primary adapter..."
        )
        self.switch_metric(primary_metric=10, backup_metric=60)
        self.current_active_nic = "PRIMARY"
        self._consecutive_primary_failures = 0
        self._primary_recovery_start_time = None

        if self.on_failover_callback:
            try:
                self.on_failover_callback("PRIMARY")
            except Exception as e:
                logger.error(f"Error in revert callback: {e}")

    def probe_cycle(self) -> None:
        """Executes a single failover probing and hysteresis evaluation cycle."""
        healthy = self.is_network_healthy()

        if self.current_active_nic == "PRIMARY":
            if not healthy:
                self._consecutive_primary_failures += 1
                logger.warning(
                    f"[NETWORK_PROBE] Primary probe failed ({self._consecutive_primary_failures}/3)."
                )
                if self._consecutive_primary_failures >= 3:
                    self.trigger_failover_to_backup()
            else:
                self._consecutive_primary_failures = 0

        elif self.current_active_nic == "BACKUP":
            # On backup adapter, evaluate if primary connection has recovered
            if healthy:
                now = time.time()
                if self._primary_recovery_start_time is None:
                    self._primary_recovery_start_time = now
                    logger.info("[NETWORK_PROBE] Connectivity restored. Starting 60s anti-flapping hysteresis...")
                elif (now - self._primary_recovery_start_time) >= self.hysteresis_seconds:
                    self.trigger_revert_to_primary()
            else:
                if self._primary_recovery_start_time is not None:
                    logger.warning("[NETWORK_PROBE] Connection fluttered during recovery. Resetting 60s hysteresis timer.")
                    self._primary_recovery_start_time = None

    def start_monitoring(self) -> None:
        """Starts background network probing thread."""
        if self._running:
            return

        self._running = True

        def _loop():
            while self._running:
                try:
                    self.probe_cycle()
                except Exception as e:
                    logger.debug(f"Exception in network failover monitor cycle: {e}")
                time.sleep(self.check_interval)

        self._monitor_thread = threading.Thread(target=_loop, name="NetworkFailoverMonitor", daemon=True)
        self._monitor_thread.start()
        logger.info(
            f"Dual-NIC Failover Monitor active: Primary='{self.primary_alias}', Backup='{self.backup_alias}', "
            f"Interval={self.check_interval}s, Hysteresis={self.hysteresis_seconds}s."
        )

    def stop_monitoring(self) -> None:
        """Stops background probing."""
        self._running = False
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=2.0)
        logger.info("Dual-NIC Failover Monitor stopped.")
