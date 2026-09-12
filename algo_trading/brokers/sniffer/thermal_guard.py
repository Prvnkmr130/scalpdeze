# -*- coding: utf-8 -*-
"""
algo_trading/brokers/sniffer/thermal_guard.py
─────────────────────────────────────────────
Hardware & Thermal Protection Guard for Thin Client PCs.
Monitors CPU temperatures via Windows WMI ACPI sensors or psutil every 30s.
Applies an adaptive execution loop throttling matrix to prevent CPU/chassis overheating.
"""

from __future__ import annotations

import logging
import sys
import time
from typing import Optional

logger = logging.getLogger("algo_trading.brokers.sniffer.thermal_guard")


class ThermalGuard:
    """
    Monitors CPU thermal zone temperatures on Windows Thin Clients
    and provides adaptive loop interval adjustments to safeguard hardware.
    """

    def __init__(
        self,
        warning_temp: float = 75.0,
        critical_temp: float = 82.0,
        check_interval: float = 30.0,
        base_loop_interval: float = 2.0,
    ):
        self.warning_temp = warning_temp
        self.critical_temp = critical_temp
        self.check_interval = check_interval
        self.base_loop_interval = base_loop_interval

        self._last_check_time: float = 0.0
        self._current_temp: float = 45.0  # safe default
        self._current_status: str = "NORMAL"  # NORMAL, ELEVATED, CRITICAL
        self._wmi_client = None

        if sys.platform == "win32":
            try:
                import wmi
                # ACPI thermal zone is under root\wmi
                self._wmi_client = wmi.WMI(namespace="root\\wmi")
            except Exception as e:
                logger.debug(f"Could not initialize WMI client for thermal monitoring: {e}")

    def read_cpu_temperature(self) -> float:
        """
        Reads the current CPU temperature in Celsius.
        Uses WMI MSAcpi_ThermalZoneTemperature if available, then psutil sensors.
        """
        # 1. Try Windows WMI ACPI Thermal Zone
        if self._wmi_client is not None:
            try:
                zones = self._wmi_client.MSAcpi_ThermalZoneTemperature()
                if zones:
                    # CurrentTemperature is in tenths of Kelvin: (val / 10.0) - 273.15
                    temps_c = [(z.CurrentTemperature / 10.0) - 273.15 for z in zones if hasattr(z, "CurrentTemperature")]
                    if temps_c:
                        return max(temps_c)
            except Exception as e:
                logger.debug(f"WMI thermal read failed: {e}")

        # 2. Try psutil sensors_temperatures fallback
        try:
            import psutil
            if hasattr(psutil, "sensors_temperatures"):
                temps_dict = psutil.sensors_temperatures()
                if temps_dict:
                    all_temps = []
                    for name, entries in temps_dict.items():
                        for entry in entries:
                            if hasattr(entry, "current") and entry.current is not None:
                                all_temps.append(entry.current)
                    if all_temps:
                        return max(all_temps)
        except Exception:
            pass

        # Return last known or nominal fallback
        return self._current_temp

    def check_thermal_status(self) -> str:
        """
        Periodic thermal probe. Returns 'NORMAL', 'ELEVATED', or 'CRITICAL'.
        """
        now = time.time()
        if now - self._last_check_time < self.check_interval:
            return self._current_status

        self._last_check_time = now
        temp = self.read_cpu_temperature()
        self._current_temp = temp

        if temp >= self.critical_temp:
            if self._current_status != "CRITICAL":
                logger.critical(
                    f"[THERMAL_CRITICAL] CPU Temperature is {temp:.1f}°C (>= {self.critical_temp}°C). "
                    "Throttling execution loop to 5.0s and pausing non-essential DOM parsing!"
                )
            self._current_status = "CRITICAL"
        elif temp >= self.warning_temp:
            if self._current_status != "ELEVATED":
                logger.warning(
                    f"[THERMAL_WARNING] CPU Temperature is {temp:.1f}°C (>= {self.warning_temp}°C). "
                    "Relaxing execution loop to 3.5s to reduce thermal stress."
                )
            self._current_status = "ELEVATED"
        else:
            if self._current_status != "NORMAL":
                logger.info(f"[THERMAL_RESTORED] CPU Temperature cooled to {temp:.1f}°C. Resuming standard 2.0s loop.")
            self._current_status = "NORMAL"

        return self._current_status

    @property
    def current_temperature(self) -> float:
        return self._current_temp

    @property
    def status(self) -> str:
        return self._current_status

    def get_adaptive_loop_interval(self) -> float:
        """
        Returns the throttled loop interval in seconds based on thermal status.
        Normal: base (2.0s), Elevated: 3.5s, Critical: 5.0s.
        """
        status = self.check_thermal_status()
        if status == "CRITICAL":
            return 5.0
        elif status == "ELEVATED":
            return 3.5
        return self.base_loop_interval

    def should_pause_dom_parsing(self) -> bool:
        """Indicates whether heavy non-essential DOM parsing should be skipped."""
        return self._current_status == "CRITICAL"
