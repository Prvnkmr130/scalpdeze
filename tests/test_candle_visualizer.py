import os
import json
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
django.setup()

from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from django.test import RequestFactory, override_settings
from django.contrib.admin.sites import AdminSite
from kalai.models import Broker
from kalai.admin import AccountAdmin


def test_candle_visualizer_view_debug_permission():
    site = AdminSite()
    admin_inst = AccountAdmin(Broker, site)
    rf = RequestFactory()

    # 1. When DEBUG is False -> must return 403 Forbidden
    with override_settings(DEBUG=False):
        req = rf.get("/admin/kalai/broker/candle-visualizer/")
        req.user = MagicMock(is_active=True, is_staff=True)
        resp = admin_inst.candle_visualizer_view(req)
        assert resp.status_code == 403
        assert b"Debug Mode Required" in resp.content

    # 2. When DEBUG is True -> renders template with 200 OK
    with override_settings(DEBUG=True):
        with patch("kalai.models.Broker.objects") as mock_broker_qs, \
             patch("kalai.models.AlgoInfo.objects") as mock_algoinfo_qs:
            mock_broker_qs.all.return_value.order_by.return_value = []
            # Returns mixed tables - only _all tables should be included
            mock_algoinfo_qs.filter.return_value.values_list.return_value.distinct.return_value = [
                "fwd_10_all", "fwd_30_all", "stop_loss_info", "strike_entry_info", "HS6525_inst_tokens"
            ]

            req = rf.get("/admin/kalai/broker/candle-visualizer/")
            req.user = MagicMock(is_active=True, is_staff=True)
            resp = admin_inst.candle_visualizer_view(req)
            assert resp.status_code == 200
            assert b"Candle Visualizer" in resp.content


def test_candle_visualizer_api_debug_permission():
    site = AdminSite()
    admin_inst = AccountAdmin(Broker, site)
    rf = RequestFactory()

    # When DEBUG is False -> API must return 403
    with override_settings(DEBUG=False):
        req = rf.get("/admin/kalai/broker/candle-visualizer/api/data/")
        resp = admin_inst.candle_visualizer_api_view(req)
        assert resp.status_code == 403
        data = json.loads(resp.content)
        assert data["success"] is False
        assert "Debug mode required" in data["error"]


def test_candle_visualizer_api_data_parsing_and_sorting():
    site = AdminSite()
    admin_inst = AccountAdmin(Broker, site)
    rf = RequestFactory()

    # Mock Broker & Master Data
    mock_broker = MagicMock()
    mock_broker.account_id = "HS6525"
    mock_master = MagicMock()
    mock_master.tabledata = [
        {"instrument_token": 256265, "tradingsymbol": "NIFTY 50", "name": "NIFTY"},
        {"instrument_token": 408065, "tradingsymbol": "INFY", "name": "INFOSYS"},
    ]
    mock_broker.get_master_data.return_value = mock_master
    mock_broker.get_subscribed_tokens.return_value = [256265, 408065]

    # Mock AlgoInfo candle records (intentionally out of chronological order)
    mock_record = MagicMock()
    mock_record.timestamp = datetime(2026, 9, 1, 9, 30, tzinfo=timezone.utc)
    mock_record.tabledata = [
        {"instrument_token": 256265, "date_time": "2026-09-01 09:30:00", "open": 24530.0, "high": 24570.0, "low": 24520.0, "close": 24560.0},
        {"instrument_token": 256265, "date_time": "2026-09-01 09:15:00", "open": 24500.0, "high": 24550.0, "low": 24480.0, "close": 24520.0},
        {"instrument_token": 256265, "date_time": "2026-09-01 09:20:00", "open": 24520.0, "high": 24530.0, "low": 24490.0, "close": 24510.0},
        {"instrument_token": 408065, "date_time": "2026-09-01 09:15:00", "open": 1800.0, "high": 1810.0, "low": 1795.0, "close": 1805.0},
    ]

    with override_settings(DEBUG=True):
        with patch("kalai.models.Broker.resolve", return_value=mock_broker), \
             patch("kalai.models.AlgoInfo.objects.filter") as mock_filter:
            mock_filter.return_value.values_list.return_value.distinct.return_value = ["fwd_10_all", "fwd_30_all", "day_cdl_all", "stop_loss_info"]
            mock_filter.return_value.filter.return_value.first.return_value = mock_record

            req = rf.get("/admin/kalai/broker/candle-visualizer/api/data/?account_id=HS6525&tablename=fwd_10_all&token=256265")
            resp = admin_inst.candle_visualizer_api_view(req)
            assert resp.status_code == 200
            data = json.loads(resp.content)

            assert data["success"] is True
            assert data["active_token"] == "256265"
            assert data["active_symbol"] == "NIFTY 50"
            assert data["candle_count"] == 3

            # Verify available_tables only contains tables ending with _all
            assert "available_tables" in data
            for t in data["available_tables"]:
                assert t.endswith("_all")
            assert "stop_loss_info" not in data["available_tables"]

            # Verify strictly ascending chronological order (09:15, 09:20, 09:30)
            candles = data["candles"]
            assert candles[0]["open"] == 24500.0
            assert candles[1]["open"] == 24520.0
            assert candles[2]["open"] == 24530.0
            assert candles[0]["time"] < candles[1]["time"] < candles[2]["time"]

            # Verify available tokens list with symbol mapping (pure symbol names)
            tok_map = {t["token"]: t for t in data["available_tokens"]}
            assert "256265" in tok_map and tok_map["256265"]["symbol"] == "NIFTY 50"
            assert tok_map["256265"]["label"] == "NIFTY 50"
            assert "408065" in tok_map and tok_map["408065"]["symbol"] == "INFY"
            assert tok_map["408065"]["label"] == "INFY"


def test_candle_visualizer_empty_symbols_filtered_out():
    """Verify that subscribed tokens with 0 candle data and invalid/empty records are omitted from dropdown."""
    site = AdminSite()
    admin_inst = AccountAdmin(Broker, site)
    rf = RequestFactory()

    mock_broker = MagicMock()
    mock_broker.account_id = "COIN_SWITCH"
    mock_master = MagicMock()
    mock_master.tabledata = [
        {"instrument_token": 1001, "tradingsymbol": "BTC/USDT"},
        {"instrument_token": 1002, "tradingsymbol": "ETH/USDT"},
        {"instrument_token": 1003, "tradingsymbol": "SOL/USDT"},  # Subscribed, but no candles
    ]
    mock_broker.get_master_data.return_value = mock_master
    mock_broker.get_subscribed_tokens.return_value = [1001, 1002, 1003]

    mock_record = MagicMock()
    mock_record.timestamp = datetime(2026, 9, 1, 9, 30, tzinfo=timezone.utc)
    mock_record.tabledata = [
        {"instrument_token": 1001, "date_time": "2026-09-01 09:30:00", "open": 65000.0, "high": 65100.0, "low": 64900.0, "close": 65050.0},
        {"instrument_token": 1002, "date_time": "2026-09-01 09:30:00", "open": 3400.0, "high": 3450.0, "low": 3390.0, "close": 3420.0},
        {"instrument_token": 9999, "date_time": "2026-09-01 09:30:00", "open": None, "high": None, "low": None, "close": None},  # Corrupt/empty candle
    ]

    with override_settings(DEBUG=True):
        with patch("kalai.models.Broker.resolve", return_value=mock_broker), \
             patch("kalai.models.AlgoInfo.objects.filter") as mock_filter:
            mock_filter.return_value.values_list.return_value.distinct.return_value = ["fwd_10_all"]
            mock_filter.return_value.filter.return_value.first.return_value = mock_record

            req = rf.get("/admin/kalai/broker/candle-visualizer/api/data/?account_id=COIN_SWITCH&tablename=fwd_10_all")
            resp = admin_inst.candle_visualizer_api_view(req)
            assert resp.status_code == 200
            data = json.loads(resp.content)

            assert data["success"] is True
            tokens_in_dropdown = [t["token"] for t in data["available_tokens"]]
            assert "1001" in tokens_in_dropdown
            assert "1002" in tokens_in_dropdown
            # 1003 (subscribed but 0 candles) must NOT be present
            assert "1003" not in tokens_in_dropdown
            # 9999 (null open/close) must NOT be present
            assert "9999" not in tokens_in_dropdown


def test_candle_visualizer_numeric_unmapped_filtered_out():
    """Verify that tokens with only numeric IDs and no resolved symbol name are filtered out from dropdown."""
    site = AdminSite()
    admin_inst = AccountAdmin(Broker, site)
    rf = RequestFactory()

    mock_broker = MagicMock()
    mock_broker.account_id = "ZERODHA"
    mock_master = MagicMock()
    mock_master.tabledata = [
        {"instrument_token": 256265, "tradingsymbol": "NIFTY"},
    ]
    mock_broker.get_master_data.return_value = mock_master

    mock_record = MagicMock()
    mock_record.timestamp = datetime(2026, 9, 1, 9, 30, tzinfo=timezone.utc)
    mock_record.tabledata = [
        {"instrument_token": 256265, "date_time": "2026-09-01 09:30:00", "open": 24000.0, "high": 24100.0, "low": 23900.0, "close": 24050.0},
        {"instrument_token": 987654321, "date_time": "2026-09-01 09:30:00", "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0},  # Numeric only, no symbol name
    ]

    with override_settings(DEBUG=True):
        with patch("kalai.models.Broker.resolve", return_value=mock_broker), \
             patch("kalai.models.ExchangeMasterData.objects.filter") as mock_ex_filter, \
             patch("kalai.models.AlgoInfo.objects.filter") as mock_filter:
            mock_ex_filter.return_value = [mock_master]
            mock_filter.return_value.values_list.return_value.distinct.return_value = ["fwd_10_all"]
            mock_filter.return_value.filter.return_value.first.return_value = mock_record

            req = rf.get("/admin/kalai/broker/candle-visualizer/api/data/?account_id=ZERODHA&tablename=fwd_10_all")
            resp = admin_inst.candle_visualizer_api_view(req)
            assert resp.status_code == 200
            data = json.loads(resp.content)

            assert data["success"] is True
            tokens_in_dropdown = [t["token"] for t in data["available_tokens"]]
            assert "256265" in tokens_in_dropdown
            # 987654321 has no symbol name and must be filtered out
            assert "987654321" not in tokens_in_dropdown


def test_candle_visualizer_timezone_ist_for_indian_broker():
    """Verify that naive timestamps for Indian brokers are interpreted as Asia/Kolkata (IST), yielding exact epoch seconds."""
    from zoneinfo import ZoneInfo
    site = AdminSite()
    admin_inst = AccountAdmin(Broker, site)
    rf = RequestFactory()

    mock_broker = MagicMock()
    mock_broker.account_id = "W1NPY"
    mock_broker.broker_name = "Kotak"
    mock_broker.is_crypto = False
    mock_master = MagicMock()
    mock_master.tabledata = [{"instrument_token": 256265, "tradingsymbol": "NATGASMINI"}]
    mock_broker.get_master_data.return_value = mock_master
    mock_broker.get_subscribed_tokens.return_value = [256265]

    # Date time: 2026-09-09 18:00:00 IST
    # 18:00 IST is 12:30 UTC -> epoch = 1788957000
    mock_record = MagicMock()
    mock_record.timestamp = datetime(2026, 9, 9, 12, 30, tzinfo=timezone.utc)
    mock_record.tabledata = [
        {"instrument_token": 256265, "date_time": "2026-09-09 18:00:00", "open": 250.0, "high": 251.0, "low": 249.0, "close": 250.5},
    ]

    with override_settings(DEBUG=True):
        with patch("kalai.models.Broker.resolve", return_value=mock_broker), \
             patch("kalai.models.AlgoInfo.objects.filter") as mock_filter:
            mock_filter.return_value.values_list.return_value.distinct.return_value = ["fwd_10_all"]
            mock_filter.return_value.filter.return_value.first.return_value = mock_record

            req = rf.get("/admin/kalai/broker/candle-visualizer/api/data/?account_id=W1NPY&tablename=fwd_10_all&token=256265")
            resp = admin_inst.candle_visualizer_api_view(req)
            assert resp.status_code == 200
            data = json.loads(resp.content)
            assert data["success"] is True
            assert len(data["candles"]) == 1
            candle = data["candles"][0]
            # Expected timestamp: datetime(2026, 9, 9, 18, 0, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp() == 1788957000
            expected_epoch = int(datetime(2026, 9, 9, 18, 0, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp())
            assert candle["time"] == expected_epoch


def test_candle_visualizer_isolated_account_no_cross_fallback():
    """Verify that if an account_id is requested and has no record, it does NOT fall back to another broker's record."""
    site = AdminSite()
    admin_inst = AccountAdmin(Broker, site)
    rf = RequestFactory()

    mock_broker = MagicMock()
    mock_broker.account_id = "NON_EXISTENT_ACC"
    mock_broker.broker_name = "Kotak"
    mock_broker.is_crypto = False

    with override_settings(DEBUG=True):
        with patch("kalai.models.Broker.resolve", return_value=mock_broker), \
             patch("kalai.models.AlgoInfo.objects.filter") as mock_filter:
            mock_filter.return_value.values_list.return_value.distinct.return_value = ["fwd_10_all"]
            # account filter returns None
            mock_filter.return_value.filter.return_value.first.return_value = None

            req = rf.get("/admin/kalai/broker/candle-visualizer/api/data/?account_id=NON_EXISTENT_ACC&tablename=fwd_10_all")
            resp = admin_inst.candle_visualizer_api_view(req)
            assert resp.status_code == 200
            data = json.loads(resp.content)
            assert data["success"] is True
            # Must be empty and not contain candles from another account
            assert data["candles"] == []
            assert data["candle_count"] == 0



