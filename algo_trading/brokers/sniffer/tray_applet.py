# -*- coding: utf-8 -*-
"""
algo_trading/brokers/sniffer/tray_applet.py
───────────────────────────────────────────
Windows Desktop Native Integration Layer:
1. System Tray (SysTray) Applet (pystray) with dynamic color status (Green/Yellow/Red).
2. Windows Toast Notifications with audible chimes (windows-toasts).
3. Global Emergency Hotkey (Ctrl + Alt + B) for instant browser visibility toggling.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import webbrowser
from typing import Callable, Optional

logger = logging.getLogger("algo_trading.brokers.sniffer.tray_applet")


def send_windows_toast(
    title: str,
    body: str,
    audio_chime: bool = True,
    visualizer_url: Optional[str] = None,
) -> bool:
    """
    Dispatches a native Windows 10/11 desktop toast notification.
    """
    from algo_trading.app_config import config

    if visualizer_url is None:
        visualizer_url = f"http://localhost:{config.BIND_PORT}/admin/kalai/candle-visualizer/"

    if config.is_debug and not title.startswith("[DEBUG]"):
        title = f"[DEBUG] {title}"

    logger.info(f"[DESKTOP_TOAST] {title}: {body}")

    if sys.platform != "win32":
        return False

    try:
        from windows_toasts import (
            AudioUri,
            InteractableWindowsToaster,
            Toast,
            ToastAudio,
            ToastButton,
        )

        toaster = InteractableWindowsToaster(f"DeltaZero26 [{config.APP_INSTANCE.upper()}]")
        toast = Toast()
        toast.text_fields = [title, body]

        if audio_chime:
            toast.audio = ToastAudio(sound=AudioUri.Default, looping=False)

        # Action button to open local visualizer
        target_vis = visualizer_url
        def _open_vis(args):
            webbrowser.open(target_vis)

        toast.AddAction(ToastButton("Open Visualizer", arguments="open_vis"))
        toast.on_activated = _open_vis

        toaster.show_toast(toast)
        return True
    except ImportError:
        # Fallback to powershell notification
        try:
            ps_script = f"""
            [reflection.assembly]::loadwithpartialname('System.Windows.Forms') | Out-Null
            $notify = New-Object System.Windows.Forms.NotifyIcon
            $notify.Icon = [System.Drawing.SystemIcons]::Information
            $notify.Visible = $True
            $notify.ShowBalloonTip(5000, '{title}', '{body}', [System.Windows.Forms.ToolTipIcon]::Info)
            """
            import subprocess
            subprocess.Popen(["powershell", "-NoProfile", "-Command", ps_script])
            return True
        except Exception:
            return False
    except Exception as e:
        logger.debug(f"Could not display Windows Toast: {e}")
        return False


def setup_global_hotkey(
    toggle_callback: Callable[[], bool],
    hotkey: Optional[str] = None,
) -> bool:
    """
    Registers a global emergency hotkey (e.g. Ctrl + Alt + B for prod, Ctrl + Shift + B for debug).
    Gracefully logs and avoids crashing if hotkey is already registered by another instance.
    """
    from algo_trading.app_config import config

    hk = hotkey or config.HOTKEY_TOGGLE_BROWSER
    if not hk:
        logger.info("No global hotkey configured for this instance.")
        return True

    try:
        import keyboard

        def _on_hotkey():
            new_state = toggle_callback()
            send_windows_toast(
                title="[BROWSER VISIBILITY]",
                body=f"Chromium window toggled to: {'VISIBLE' if new_state else 'HIDDEN'}",
                audio_chime=False,
            )

        keyboard.add_hotkey(hk.lower().strip(), _on_hotkey)
        logger.info(f"Registered global emergency hotkey: {hk.upper()} (Browser Visibility Toggle).")
        return True
    except Exception as e:
        logger.warning(f"Could not register global hotkey '{hk}': {e} (Another instance may be using it)")
        return False


class WindowsTrayApplet:
    """
    Native Windows System Tray (SysTray) Applet powered by pystray.
    """

    def __init__(
        self,
        on_toggle_browser: Optional[Callable[[], bool]] = None,
        on_switch_nic: Optional[Callable[[], None]] = None,
        on_recycle_memory: Optional[Callable[[], None]] = None,
        on_hibernate_pc: Optional[Callable[[], None]] = None,
        on_exit: Optional[Callable[[], None]] = None,
        visualizer_url: Optional[str] = None,
        instance_name: Optional[str] = None,
        bind_port: Optional[int] = None,
    ):
        from algo_trading.app_config import config

        self.instance_name = (instance_name or config.APP_INSTANCE).upper()
        self.bind_port = bind_port or config.BIND_PORT
        self.on_toggle_browser = on_toggle_browser
        self.on_switch_nic = on_switch_nic
        self.on_recycle_memory = on_recycle_memory
        self.on_hibernate_pc = on_hibernate_pc
        self.on_exit = on_exit
        self.visualizer_url = visualizer_url or f"http://localhost:{self.bind_port}/admin/kalai/candle-visualizer/"

        self.current_status = "ACTIVE"  # "ACTIVE", "STANDBY", "FAILOVER"
        self._icon = None
        self._tray_thread: Optional[threading.Thread] = None

    def _create_image(self, color: str = "green"):
        """Generates a simple 64x64 solid circular icon for the system tray."""
        try:
            from PIL import Image, ImageDraw

            img = Image.new("RGBA", (64, 64), color=(0, 0, 0, 0))
            draw = ImageDraw.Draw(img)

            # Active color based on instance
            if self.instance_name in ("DEBUG", "DEV"):
                active_color = (0, 188, 212, 255)   # Cyan for Debug
            elif self.instance_name not in ("PROD", "PRODUCTION", "DEFAULT"):
                active_color = (52, 152, 219, 255)  # Blue for Instance N
            else:
                active_color = (46, 204, 113, 255)  # Green for Prod

            color_map = {
                "green": active_color,
                "yellow": (241, 196, 15, 255),
                "red": (231, 76, 60, 255),
            }
            fill_col = color_map.get(color.lower(), active_color)
            draw.ellipse((8, 8, 56, 56), fill=fill_col, outline=(255, 255, 255, 200), width=3)
            return img
        except ImportError:
            return None

    def set_status(self, status: str) -> None:
        """Updates the tray icon color and tooltip."""
        self.current_status = status.upper()
        if not self._icon:
            return

        color = "green"
        if self.current_status in ("STANDBY", "OFF_HOURS", "POST_MARKET"):
            color = "yellow"
        elif self.current_status in ("FAILOVER", "CRITICAL", "ERROR"):
            color = "red"

        img = self._create_image(color)
        if img:
            self._icon.icon = img
        self._icon.title = f"DeltaZero26 [{self.instance_name}] [{self.current_status}]"

    def _menu_toggle_browser(self, icon, item):
        if self.on_toggle_browser:
            new_state = self.on_toggle_browser()
            send_windows_toast(
                title="Browser Window",
                body=f"Browser is now {'VISIBLE' if new_state else 'HIDDEN'}",
                audio_chime=False,
            )

    def _menu_open_visualizer(self, icon, item):
        webbrowser.open(self.visualizer_url)

    def _menu_switch_nic(self, icon, item):
        if self.on_switch_nic:
            self.on_switch_nic()

    def _menu_recycle_memory(self, icon, item):
        if self.on_recycle_memory:
            self.on_recycle_memory()
            send_windows_toast("Memory Recycle", "Browser context memory soft-recycle triggered.", audio_chime=False)

    def _menu_hibernate(self, icon, item):
        if self.on_hibernate_pc:
            self.on_hibernate_pc()

    def _menu_exit(self, icon, item):
        if self._icon:
            self._icon.stop()
        if self.on_exit:
            self.on_exit()

    def run(self) -> None:
        """Launches the pystray icon loop."""
        try:
            import pystray
            from pystray import Menu, MenuItem

            menu = Menu(
                MenuItem("Toggle Browser (Ctrl+Alt+B)", self._menu_toggle_browser),
                MenuItem("Open Live Visualizer", self._menu_open_visualizer),
                MenuItem("Switch Network Interface", self._menu_switch_nic),
                MenuItem("Recycle Browser Memory", self._menu_recycle_memory),
                MenuItem("Force PC Hibernate", self._menu_hibernate),
                MenuItem("Exit Signal Engine", self._menu_exit),
            )

            icon_img = self._create_image("green")
            self._icon = pystray.Icon(
                name="DeltaZero26",
                icon=icon_img,
                title="DeltaZero26 Signal Engine [ACTIVE]",
                menu=menu,
            )

            self._icon.run()
        except ImportError:
            logger.debug("pystray not installed; SysTray applet running in headless mode.")
        except Exception as e:
            logger.debug(f"SysTray error: {e}")

    def start_background(self) -> None:
        """Starts the SysTray applet in a separate daemon thread."""
        self._tray_thread = threading.Thread(target=self.run, name="SysTrayApplet", daemon=True)
        self._tray_thread.start()
        logger.info("Windows System Tray applet started in background.")

    def stop(self) -> None:
        if self._icon:
            self._icon.stop()
