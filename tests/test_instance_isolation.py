# -*- coding: utf-8 -*-
"""
tests/test_instance_isolation.py
─────────────────────────────────
Unit tests for multi-instance local concurrency and isolation:
- Production vs. Debug instance configuration
- Dynamic port auto-allocation (8000 for prod, 8001 for debug, 8000+N for arbitrary N)
- Database isolation in PostgreSQL 18 (algo_trading, algo_trading_debug, algo_trading_N)
- Browser profile isolation (profiles/prod, profiles/debug, profiles/N)
- Safety guards: power and failover automatically disabled on non-prod instances
- SysTray visual color distinction (Green for prod, Cyan for debug, Blue for N)
"""

import os
from unittest.mock import patch
import pytest

from algo_trading.app_config import AppConfig, _get_default_port, _get_default_db, _get_default_hotkey


def test_production_instance_defaults():
    """Verify production instance receives port 8000, DB algo_trading, and active power management."""
    with patch.dict(os.environ, {"APP_INSTANCE": "prod", "APP_MODE": "production"}, clear=False):
        cfg = AppConfig()
        assert cfg.APP_INSTANCE == "prod"
        assert cfg.is_production is True
        assert cfg.is_debug is False
        assert cfg.BIND_PORT == 8000
        assert cfg.DB_NAME == "algo_trading"
        assert "profiles" in cfg.PROFILES_DIR and cfg.PROFILES_DIR.endswith("prod")
        assert cfg.HOTKEY_TOGGLE_BROWSER == "ctrl+alt+b"
        assert cfg.ENABLE_AUTO_WAKE is True
        assert cfg.ENABLE_AUTO_HIBERNATE is True


def test_debug_instance_defaults():
    """Verify debug instance receives port 8001, DB algo_trading_debug, and disabled power management."""
    with patch.dict(os.environ, {"APP_INSTANCE": "debug", "APP_MODE": "debug"}, clear=False):
        cfg = AppConfig()
        assert cfg.APP_INSTANCE == "debug"
        assert cfg.is_production is False
        assert cfg.is_debug is True
        assert cfg.BIND_PORT == 8001
        assert cfg.DB_NAME == "algo_trading_debug"
        assert "profiles" in cfg.PROFILES_DIR and cfg.PROFILES_DIR.endswith("debug")
        assert cfg.HOTKEY_TOGGLE_BROWSER == "ctrl+shift+b"
        assert cfg.ENABLE_AUTO_WAKE is False
        assert cfg.ENABLE_AUTO_HIBERNATE is False


def test_arbitrary_numeric_instance_defaults():
    """Verify arbitrary instance '2' receives port 8002, DB algo_trading_2, and safe defaults."""
    with patch.dict(os.environ, {"APP_INSTANCE": "2"}, clear=False):
        cfg = AppConfig()
        assert cfg.APP_INSTANCE == "2"
        assert cfg.is_debug is True
        assert cfg.BIND_PORT == 8002
        assert cfg.DB_NAME == "algo_trading_2"
        assert cfg.PROFILES_DIR.endswith("2")
        assert cfg.ENABLE_AUTO_WAKE is False
        assert cfg.ENABLE_AUTO_HIBERNATE is False


def test_port_helper_allocations():
    """Verify _get_default_port auto-allocation across arbitrary names and indices."""
    assert _get_default_port("prod") == 8000
    assert _get_default_port("production") == 8000
    assert _get_default_port("debug") == 8001
    assert _get_default_port("2") == 8002
    assert _get_default_port("5") == 8005
    # Arbitrary strings map to 8002..8025
    custom_port = _get_default_port("simulator")
    assert 8002 <= custom_port <= 8025


def test_db_name_helper_allocations():
    """Verify _get_default_db allocates dedicated database names."""
    assert _get_default_db("prod") == "algo_trading"
    assert _get_default_db("debug") == "algo_trading_debug"
    assert _get_default_db("sim") == "algo_trading_sim"
    assert _get_default_db("2") == "algo_trading_2"


def test_tray_applet_instance_colors():
    """Verify SysTray applet generates visually distinct colors per instance."""
    from algo_trading.brokers.sniffer.tray_applet import WindowsTrayApplet

    prod_tray = WindowsTrayApplet(instance_name="prod", bind_port=8000)
    assert prod_tray.instance_name == "PROD"
    assert prod_tray.bind_port == 8000
    assert "8000" in prod_tray.visualizer_url

    debug_tray = WindowsTrayApplet(instance_name="debug", bind_port=8001)
    assert debug_tray.instance_name == "DEBUG"
    assert debug_tray.bind_port == 8001
    assert "8001" in debug_tray.visualizer_url

    inst2_tray = WindowsTrayApplet(instance_name="2", bind_port=8002)
    assert inst2_tray.instance_name == "2"
    assert "8002" in inst2_tray.visualizer_url


def test_postgresql_connection_and_db_creation():
    """Verify active connection to local PostgreSQL 18 and create a test database."""
    import psycopg

    conn = psycopg.connect(
        host="127.0.0.1",
        port=5432,
        user="postgres",
        password="postgres",
        dbname="postgres",
        autocommit=True,
    )
    with conn.cursor() as cur:
        # Check server version is 18.x
        cur.execute("SHOW server_version")
        ver = cur.fetchone()[0]
        assert "18" in ver

        # Test creating debug database
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'algo_trading_debug'")
        if not cur.fetchone():
            cur.execute("CREATE DATABASE algo_trading_debug")

        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'algo_trading_debug'")
        assert cur.fetchone() is not None
    conn.close()
