# -*- coding: utf-8 -*-
"""
tests/test_network_failover.py
──────────────────────────────
Unit tests for Dual-NIC network failover manager and anti-flapping hysteresis.
"""

from unittest.mock import MagicMock, patch
import pytest

from algo_trading.brokers.sniffer.network_failover import DualNicFailoverManager


def test_failover_trigger_after_3_failures():
    """Verify that 3 consecutive failures trigger failover to the backup adapter."""
    callback = MagicMock()
    mgr = DualNicFailoverManager(
        primary_alias="Ethernet",
        backup_alias="Wi-Fi",
        check_interval=0.1,
        hysteresis_seconds=1.0,
        on_failover_callback=callback,
    )

    # Mock switch_metric so PowerShell is not actually invoked during unit test
    with patch.object(mgr, "switch_metric", return_value=True) as mock_switch:
        with patch.object(mgr, "is_network_healthy", return_value=False):
            # Failure 1
            mgr.probe_cycle()
            assert mgr.current_active_nic == "PRIMARY"
            assert mgr._consecutive_primary_failures == 1
            mock_switch.assert_not_called()

            # Failure 2
            mgr.probe_cycle()
            assert mgr.current_active_nic == "PRIMARY"
            assert mgr._consecutive_primary_failures == 2
            mock_switch.assert_not_called()

            # Failure 3 -> triggers failover
            mgr.probe_cycle()
            assert mgr.current_active_nic == "BACKUP"
            mock_switch.assert_called_once_with(primary_metric=60, backup_metric=10)
            callback.assert_called_once_with("BACKUP")


def test_anti_flapping_hysteresis():
    """Verify primary recovery requires sustaining hysteresis_seconds before reverting."""
    callback = MagicMock()
    mgr = DualNicFailoverManager(
        primary_alias="Ethernet",
        backup_alias="Wi-Fi",
        check_interval=0.1,
        hysteresis_seconds=0.5,  # 0.5s for fast test
        on_failover_callback=callback,
    )
    mgr.current_active_nic = "BACKUP"

    with patch.object(mgr, "switch_metric", return_value=True) as mock_switch:
        # 1. First healthy probe starts the hysteresis timer
        with patch.object(mgr, "is_network_healthy", return_value=True):
            mgr.probe_cycle()
            assert mgr.current_active_nic == "BACKUP"
            assert mgr._primary_recovery_start_time is not None
            mock_switch.assert_not_called()

            # 2. Immediate second probe within 0.5s -> should NOT revert yet
            mgr.probe_cycle()
            assert mgr.current_active_nic == "BACKUP"
            mock_switch.assert_not_called()

            # 3. Simulate elapsed time > 0.5s
            import time
            time.sleep(0.55)
            mgr.probe_cycle()
            assert mgr.current_active_nic == "PRIMARY"
            mock_switch.assert_called_once_with(primary_metric=10, backup_metric=60)
            callback.assert_called_once_with("PRIMARY")
