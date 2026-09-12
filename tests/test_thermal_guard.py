# -*- coding: utf-8 -*-
"""
tests/test_thermal_guard.py
───────────────────────────
Unit tests for CPU thermal guard and adaptive execution loop throttling matrix.
"""

from unittest.mock import patch
import pytest

from algo_trading.brokers.sniffer.thermal_guard import ThermalGuard


def test_thermal_throttling_matrix():
    """Verify loop interval adjustments across Normal, Elevated, and Critical states."""
    guard = ThermalGuard(
        warning_temp=75.0,
        critical_temp=82.0,
        check_interval=0.0,  # force check on every call
        base_loop_interval=2.0,
    )

    # 1. Normal temperature (<75°C) -> 2.0s
    with patch.object(guard, "read_cpu_temperature", return_value=60.0):
        interval = guard.get_adaptive_loop_interval()
        assert interval == 2.0
        assert guard.status == "NORMAL"
        assert guard.should_pause_dom_parsing() is False

    # 2. Elevated temperature (75°C - 82°C) -> 3.5s
    with patch.object(guard, "read_cpu_temperature", return_value=78.5):
        interval = guard.get_adaptive_loop_interval()
        assert interval == 3.5
        assert guard.status == "ELEVATED"
        assert guard.should_pause_dom_parsing() is False

    # 3. Critical temperature (>=82°C) -> 5.0s & DOM paused
    with patch.object(guard, "read_cpu_temperature", return_value=85.0):
        interval = guard.get_adaptive_loop_interval()
        assert interval == 5.0
        assert guard.status == "CRITICAL"
        assert guard.should_pause_dom_parsing() is True

    # 4. Temperature cools down -> returns to Normal 2.0s
    with patch.object(guard, "read_cpu_temperature", return_value=65.0):
        interval = guard.get_adaptive_loop_interval()
        assert interval == 2.0
        assert guard.status == "NORMAL"
        assert guard.should_pause_dom_parsing() is False
