# -*- coding: utf-8 -*-
"""
algo_trading/brokers/sniffer/power_manager.py
─────────────────────────────────────────────
Autonomous PC Power Lifecycle Manager for Windows Thin Client.
Coordinates:
1. Dynamic Windows Task Scheduler RTC Wake Timers (`WakeToRun`)
   so the PC wakes automatically before market open on trading days.
2. Post-market clean PC Hibernation (`shutdown /h`).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime
from typing import Optional

logger = logging.getLogger("algo_trading.brokers.sniffer.power_manager")


class WindowsPowerManager:
    """
    Manages Windows 10/11 OS RTC Wake Timers and PC Hibernation.
    """

    TASK_NAME = "DeltaZero26_Market_Wake"

    def __init__(
        self,
        enable_auto_wake: bool = True,
        enable_auto_hibernate: bool = True,
        script_path: Optional[str] = None,
    ):
        self.enable_auto_wake = enable_auto_wake
        self.enable_auto_hibernate = enable_auto_hibernate
        self.script_path = script_path or os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "run_watchdog.ps1")
        )

    def schedule_next_wake(self, wake_dt: datetime) -> bool:
        """
        Dynamically configures or updates the Windows Task Scheduler task
        with `WakeToRun` set for `wake_dt`.
        """
        if not self.enable_auto_wake:
            logger.info("Auto-wake scheduling is disabled by configuration.")
            return False

        if sys.platform != "win32":
            logger.warning("PowerManager.schedule_next_wake skipped: Not running on Windows.")
            return False

        time_str = wake_dt.strftime("%H:%M")
        date_str = wake_dt.strftime("%m/%d/%Y")

        logger.info(
            f"Scheduling next Windows Task Scheduler RTC Wake for: {wake_dt.isoformat()} (Date: {date_str}, Time: {time_str})"
        )

        ps_script = f"""
        $TaskName = "{self.TASK_NAME}"
        $Time = "{time_str}"
        $Date = "{date_str}"
        $Script = "{self.script_path}"
        $WorkDir = Split-Path -Parent $Script

        try {{
            $Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File `"$Script`"" -WorkingDirectory $WorkDir
            $Trigger = New-ScheduledTaskTrigger -Once -At "$Date $Time"
            $Settings = New-ScheduledTaskSettingsSet -WakeToRun -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
            $Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

            Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
            Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal | Out-Null
            Write-Output "SUCCESS"
        }} catch {{
            Write-Error $_
            exit 1
        }}
        """

        try:
            res = subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_script],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if "SUCCESS" in res.stdout:
                logger.info(f"Successfully registered Windows RTC wake task '{self.TASK_NAME}' for {date_str} {time_str}.")
                return True
            else:
                logger.warning(f"Failed to register RTC wake task. stdout: {res.stdout.strip()}, stderr: {res.stderr.strip()}")
                return False
        except Exception as e:
            logger.error(f"Exception updating Windows Task Scheduler wake task: {e}")
            return False

    def hibernate_pc(self, force: bool = False) -> bool:
        """
        Puts the Windows Thin Client PC into Hibernation (`shutdown /h`).
        """
        if not self.enable_auto_hibernate and not force:
            logger.info("Auto-hibernate is disabled by configuration.")
            return False

        logger.info("[POWER_LIFECYCLE] Initiating Windows PC Hibernation (shutdown /h)...")

        if sys.platform != "win32":
            logger.warning("PowerManager.hibernate_pc skipped: Not running on Windows.")
            return False

        try:
            # shutdown /h activates hibernate state immediately
            subprocess.Popen(["shutdown", "/h"])
            return True
        except Exception as e:
            logger.error(f"Error executing shutdown /h: {e}. Attempting fallback via powrprof.dll...")
            try:
                subprocess.Popen(["rundll32.exe", "powrprof.dll,SetSuspendState", "0,1,0"])
                return True
            except Exception as e2:
                logger.critical(f"Failed to hibernate PC: {e2}")
                return False
