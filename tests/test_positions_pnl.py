"""
tests/test_positions_pnl.py
───────────────────────────
Comprehensive test suite for Multi-Broker Positions, Trade Records,
Daily P&L Snapshots, Background Synchronizer, and On-Demand P&L Analytics.
"""

from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

import polars as pl
import pytest
from django.contrib.auth.models import User
from django.test import Client
from django.urls import reverse
from django.utils import timezone as dj_timezone

from kalai.models import (
    ApiProvider,
    Broker,
    BrokerPosition,
    BrokerType,
    DailyPnLSnapshot,
    TradeRecord,
    AlgoLog,
)
from kalai.positions import (
    compute_pnl_analytics,
    fetch_and_normalize_positions,
    sync_account_positions,
    sync_all_broker_positions,
    sync_positions_from_algo_frames,
)
from kalai.tasks import create_daily_pnl_snapshots, sync_all_broker_positions as task_sync_positions


@pytest.fixture
def sample_brokers(db):
    """Fixture providing sample broker accounts across multiple providers."""
    b_type_z, _ = BrokerType.objects.get_or_create(code="zerodha", defaults={"name": "Zerodha"})
    b_type_k, _ = BrokerType.objects.get_or_create(code="kotak", defaults={"name": "Kotak Neo"})
    b_type_c, _ = BrokerType.objects.get_or_create(code="coinswitch", defaults={"name": "CoinSwitch PRO"})

    api_prov_z, _ = ApiProvider.objects.get_or_create(code="zerodha", defaults={"name": "Zerodha API"})
    api_prov_k, _ = ApiProvider.objects.get_or_create(code="kotak_neo", defaults={"name": "Kotak Neo API"})
    api_prov_c, _ = ApiProvider.objects.get_or_create(code="coinswitch", defaults={"name": "CoinSwitch PRO API"})

    b_zerodha, _ = Broker.objects.get_or_create(
        name="test_pnl_zerodha",
        defaults={
            "account_id": "HS6525_TEST",
            "broker_name": b_type_z,
            "api_provider": api_prov_z,
            "api_key": "test_z_key",
            "api_secret": "test_z_secret",
            "enable_trade": True,
            "access_token": "test_z_token",
        }
    )

    b_kotak, _ = Broker.objects.get_or_create(
        name="test_pnl_kotak",
        defaults={
            "account_id": "W1NPY_TEST",
            "broker_name": b_type_k,
            "api_provider": api_prov_k,
            "api_key": "test_k_key",
            "api_secret": "123456",
            "enable_trade": True,
            "access_token": "test_k_token:::sid_123",
        }
    )

    b_coinswitch, _ = Broker.objects.get_or_create(
        name="test_pnl_cs",
        defaults={
            "account_id": "CS_PRO_TEST",
            "broker_name": b_type_c,
            "api_provider": api_prov_c,
            "enable_trade": True,
            "api_key": "cs_key",
            "api_secret": "cs_secret",
        }
    )

    b_type_d, _ = BrokerType.objects.get_or_create(code="delta", defaults={"name": "Delta Exchange"})
    api_prov_d, _ = ApiProvider.objects.get_or_create(code="delta", defaults={"name": "Delta API"})
    b_delta, _ = Broker.objects.get_or_create(
        name="test_pnl_delta",
        defaults={
            "account_id": "73270496_TEST",
            "broker_name": b_type_d,
            "api_provider": api_prov_d,
            "api_key": "test_d_key",
            "api_secret": "test_d_secret",
            "enable_trade": True,
        }
    )

    yield {
        "zerodha": b_zerodha,
        "kotak": b_kotak,
        "coinswitch": b_coinswitch,
        "delta": b_delta,
    }

    try:
        from algo_trading.algos.logger import algo_logger
        algo_logger._log_buffer.clear()
    except Exception:
        pass
    test_broker_ids = [b.id for b in [b_zerodha, b_kotak, b_coinswitch, b_delta]]
    BrokerPosition.objects.filter(account_id__in=test_broker_ids).delete()
    TradeRecord.objects.filter(account_id__in=test_broker_ids).delete()
    DailyPnLSnapshot.objects.filter(account_id__in=test_broker_ids).delete()
    AlgoLog.objects.filter(account_id__in=test_broker_ids).delete()



@pytest.mark.django_db
def test_models_creation_and_constraints(sample_brokers):
    """Verify BrokerPosition, TradeRecord, and DailyPnLSnapshot model operations."""
    b_z = sample_brokers["zerodha"]

    # 1. BrokerPosition
    pos = BrokerPosition.objects.create(
        account=b_z,
        tradingsymbol="NIFTY26AUG24500CE",
        instrument_token=123456,
        product="NRML",
        quantity=Decimal("50"),
        buy_quantity=Decimal("50"),
        buy_price=Decimal("120.50"),
        buy_value=Decimal("6025.00"),
        last_price=Decimal("145.00"),
        unrealized_pnl=Decimal("1225.00"),
        realized_pnl=Decimal("0.00"),
        total_pnl=Decimal("1225.00"),
        is_open=True,
    )
    assert pos.is_open is True
    assert "HS6525_TEST" in str(pos) or "test_pnl_zerodha" in str(pos)
    assert "1225.00" in str(pos)

    # 2. TradeRecord
    tr = TradeRecord.objects.create(
        account=b_z,
        order_id="ORD-9988",
        tradingsymbol="NIFTY26AUG24500CE",
        product="NRML",
        action_type="BUY",
        quantity=Decimal("50"),
        price=Decimal("120.50"),
        value=Decimal("6025.00"),
        realized_pnl=Decimal("0.00"),
    )
    assert tr.order_id == "ORD-9988"
    assert "BUY" in str(tr)

    # 3. DailyPnLSnapshot
    today = dj_timezone.localdate()
    snap = DailyPnLSnapshot.objects.create(
        account=b_z,
        date=today,
        realized_pnl=Decimal("500.00"),
        unrealized_pnl=Decimal("1225.00"),
        net_pnl=Decimal("1725.00"),
        total_trades=4,
        winning_trades=3,
        losing_trades=1,
        turnover=Decimal("25000.00"),
    )
    assert snap.net_pnl == Decimal("1725.00")
    assert "1725.00" in str(snap)


@pytest.mark.django_db
def test_fetch_and_normalize_zerodha_positions(sample_brokers):
    """Verify position normalization from Zerodha Kite payload."""
    b_z = sample_brokers["zerodha"]

    mock_df = pl.DataFrame([
        {
            "tradingsymbol": "NIFTY26AUG24500CE",
            "instrument_token": 123456,
            "product": "NRML",
            "quantity": 50,
            "buy_quantity": 50,
            "buy_price": 120.5,
            "buy_value": 6025.0,
            "sell_quantity": 0,
            "sell_price": 0.0,
            "sell_value": 0.0,
            "last_price": 140.0,
            "unrealised": 975.0,
            "realised": 0.0,
            "pnl": 975.0,
        }
    ])

    with patch("kiteconnect.KiteConnect"):
        with patch("algo_trading.algos.zerodha_utils.ZerodhaUtility.pos_data", return_value=(None, mock_df)):
            positions = fetch_and_normalize_positions(b_z)
            assert len(positions) == 1
            p = positions[0]
            assert p["tradingsymbol"] == "NIFTY26AUG24500CE"
            assert p["quantity"] == Decimal("50.0")
            assert p["unrealized_pnl"] == Decimal("975.0")
            assert p["is_open"] is True


@pytest.mark.django_db
def test_fetch_and_normalize_kotak_positions(sample_brokers):
    """Verify position normalization from Kotak Neo payload."""
    b_k = sample_brokers["kotak"]

    mock_df = pl.DataFrame([
        {
            "tradingsymbol": "BANKNIFTY26AUG51000PE",
            "tok": 987654,
            "prod": "NRML",
            "quantity": -15,
            "buy_quantity": 0,
            "buy_price": 0.0,
            "buy_value": 0.0,
            "sell_quantity": 15,
            "sell_price": 310.0,
            "sell_value": 4650.0,
            "last_price": 280.0,
            "unrealised": 450.0,
            "realised": 0.0,
            "pnl": 450.0,
        }
    ])

    with patch("algo_trading.algos.kotak_utils.KotakNeoUtility.pos_data", return_value=(None, mock_df)):
        positions = fetch_and_normalize_positions(b_k)
        assert len(positions) == 1
        p = positions[0]
        assert p["tradingsymbol"] == "BANKNIFTY26AUG51000PE"
        assert p["quantity"] == Decimal("-15.0")
        assert p["unrealized_pnl"] == Decimal("450.0")
        assert p["is_open"] is True


@pytest.mark.django_db
def test_fetch_and_normalize_coinswitch_positions(sample_brokers):
    """Verify position normalization from CoinSwitch PRO perpetual futures payload."""
    b_c = sample_brokers["coinswitch"]

    mock_df = pl.DataFrame([
        {
            "pair": "BTCUSDT",
            "active_pos": 0.05,
            "avg_price": 62000.0,
            "mark_price": 63500.0,
            "unrealized_pnl": 75.0,
            "realized_pnl": 12.5,
        }
    ])

    with patch("algo_trading.algos.coinswitch_utils.CoinSwitchUtility.pos_data", return_value=(None, mock_df)):
        positions = fetch_and_normalize_positions(b_c)
        assert len(positions) == 1
        p = positions[0]
        assert p["tradingsymbol"] == "BTCUSDT"
        assert p["quantity"] == Decimal("0.05")
        assert p["total_pnl"] == Decimal("87.5")
        assert p["is_open"] is True


@pytest.mark.django_db
def test_fetch_and_normalize_delta_positions(sample_brokers):
    """Verify Delta Exchange positions fetch and normalization."""
    b_d = sample_brokers["delta"]

    mock_df = pl.DataFrame([
        {
            "symbol": "BTCUSD",
            "product_id": 27,
            "size": 2.0,
            "entry_price": 90000.0,
            "mark_price": 92000.0,
            "liquidation_price": 60000.0,
            "margin": 200.0,
            "unrealized_pnl": 4000.0,
            "realized_pnl": 500.0,
        }
    ])

    with patch("algo_trading.algos.delta_utils.DeltaExchangeUtility.pos_data", return_value=(None, mock_df)):
        positions = fetch_and_normalize_positions(b_d)
        assert len(positions) == 1
        p = positions[0]
        assert p["tradingsymbol"] == "BTCUSD"
        assert p["quantity"] == Decimal("2.0")
        assert p["total_pnl"] == Decimal("4500.0")
        assert p["is_open"] is True
        assert p["product"] == "DERIVATIVE"


@pytest.mark.django_db
def test_sync_account_positions_and_closure(sample_brokers):
    """Verify position syncing and automatic closure of stale positions."""
    b_z = sample_brokers["zerodha"]

    # Pre-create an open position that will not be returned by broker (should be closed)
    stale_pos = BrokerPosition.objects.create(
        account=b_z,
        tradingsymbol="OLD_CLOSED_STRIKE",
        product="NRML",
        quantity=Decimal("25"),
        is_open=True,
    )

    mock_df = pl.DataFrame([
        {
            "tradingsymbol": "NEW_ACTIVE_STRIKE",
            "instrument_token": 554433,
            "product": "NRML",
            "quantity": 50,
            "buy_quantity": 50,
            "buy_price": 100.0,
            "buy_value": 5000.0,
            "sell_quantity": 0,
            "sell_price": 0.0,
            "sell_value": 0.0,
            "last_price": 110.0,
            "unrealised": 500.0,
            "realised": 0.0,
            "pnl": 500.0,
        }
    ])

    with patch("kiteconnect.KiteConnect"):
        with patch("algo_trading.algos.zerodha_utils.ZerodhaUtility.pos_data", return_value=(None, mock_df)):
            res = sync_account_positions(b_z, force_api=True)
            assert res["synced_positions"] == 1
            assert res["open_positions"] == 1

            # Check newly synced position
            new_p = BrokerPosition.objects.get(account=b_z, tradingsymbol="NEW_ACTIVE_STRIKE")
            assert new_p.is_open is True
            assert new_p.quantity == Decimal("50.0")

            # Stale position must now be marked closed
            stale_pos.refresh_from_db()
            assert stale_pos.is_open is False
            assert stale_pos.quantity == Decimal("0")

            # Daily snapshot updated for today
            today = dj_timezone.localdate()
            snap = DailyPnLSnapshot.objects.get(account=b_z, date=today)
            assert snap.unrealized_pnl == Decimal("500.0")


@pytest.mark.django_db
def test_sync_positions_from_algo_frames_in_production(sample_brokers, settings):
    """Verify in-memory position synchronization directly from strategy engine Polars frames with 1-minute throttling."""
    import polars as pl
    b_z = sample_brokers["zerodha"]
    settings.DEBUG = False

    polars_df = pl.DataFrame([
        {
            "tradingsymbol": "NIFTY26AUG24500CE",
            "instrument_token": 123456,
            "product": "NRML",
            "quantity": 50.0,
            "buy_quantity": 50.0,
            "buy_price": 120.5,
            "buy_value": 6025.0,
            "sell_quantity": 0.0,
            "sell_price": 0.0,
            "sell_value": 0.0,
            "last_price": 145.0,
            "unrealised": 1225.0,
            "realised": 0.0,
            "pnl": 1225.0,
        }
    ])

    # 1. First sync: executes write
    res1 = sync_positions_from_algo_frames(b_z, polars_df, force=True)
    assert res1["synced_positions"] == 1
    assert res1["open_positions"] == 1
    assert res1["unrealized_pnl"] == 1225.0

    pos = BrokerPosition.objects.get(account=b_z, tradingsymbol="NIFTY26AUG24500CE")
    assert pos.is_open is True
    assert pos.quantity == Decimal("50.0")
    assert pos.unrealized_pnl == Decimal("1225.0")

    # 2. Second sync within 60s without force: throttled (0 DB write queries)
    res2 = sync_positions_from_algo_frames(b_z, polars_df, min_interval_seconds=60.0, force=False)
    assert res2.get("status") in ("throttled", "unchanged")


@pytest.mark.django_db(transaction=True)
def test_sync_all_broker_positions_parallel(sample_brokers):
    """Verify parallel synchronization across all configured broker accounts."""
    with patch("kalai.positions.fetch_and_normalize_positions", return_value=[]):
        results = sync_all_broker_positions(force_api=True)
        assert len(results) >= 1


@pytest.mark.django_db
def test_compute_pnl_analytics_multi_period(sample_brokers):
    """Verify on-demand P&L analytics calculation across multi-day snapshots."""
    b_z = sample_brokers["zerodha"]
    today = dj_timezone.localdate()

    # Create 5 days of snapshots
    # Day 4: +1000
    # Day 3: -400
    # Day 2: +2000
    # Day 1: +500
    # Today: +800
    DailyPnLSnapshot.objects.create(account=b_z, date=today - timedelta(days=4), net_pnl=Decimal("1000"), realized_pnl=Decimal("1000"), total_trades=2)
    DailyPnLSnapshot.objects.create(account=b_z, date=today - timedelta(days=3), net_pnl=Decimal("-400"), realized_pnl=Decimal("-400"), total_trades=1)
    DailyPnLSnapshot.objects.create(account=b_z, date=today - timedelta(days=2), net_pnl=Decimal("2000"), realized_pnl=Decimal("2000"), total_trades=3)
    DailyPnLSnapshot.objects.create(account=b_z, date=today - timedelta(days=1), net_pnl=Decimal("500"), realized_pnl=Decimal("500"), total_trades=2)
    DailyPnLSnapshot.objects.create(account=b_z, date=today, net_pnl=Decimal("800"), realized_pnl=Decimal("800"), total_trades=2)

    # 1. Test Last 7 Days
    res_7d = compute_pnl_analytics(account_id=b_z.account_id, period_code="last_7_days")
    assert res_7d["total_net_pnl"] == 3900.0  # 1000 - 400 + 2000 + 500 + 800
    assert res_7d["winning_days"] == 4
    assert res_7d["losing_days"] == 1
    assert res_7d["win_rate_percent"] == 80.0
    assert res_7d["best_day_pnl"] == 2000.0
    assert res_7d["worst_day_pnl"] == -400.0
    assert res_7d["total_trades"] == 10

    # 2. Test Custom Range (Day 3 to Day 1)
    res_custom = compute_pnl_analytics(
        account_id=b_z.account_id,
        period_code="custom",
        start_date=today - timedelta(days=3),
        end_date=today - timedelta(days=1),
    )
    assert res_custom["total_net_pnl"] == 2100.0  # -400 + 2000 + 500
    assert res_custom["winning_days"] == 2
    assert res_custom["losing_days"] == 1


@pytest.mark.django_db
def test_positions_pnl_admin_view_permissions(sample_brokers):
    """Verify positions_pnl_view in Django Admin is staff-protected and renders cleanly."""
    client = Client()
    admin_user = User.objects.create_superuser(username="pnl_admin", email="pnl@example.com", password="password123")
    client.force_login(admin_user)

    url = reverse("admin:positions_pnl")
    response = client.get(url, {"period": "last_30_days"})
    assert response.status_code == 200
    content = response.content.decode()

    assert "Multi-Broker Positions & P&L Analytics" in content
    assert "Live Intraday & Overnight Positions" in content
    assert "Daily Performance & P&L Progression Timeline" in content


@pytest.mark.django_db
def test_background_tasks_execution(settings):
    """Verify background tasks invoke synchronizers without error and guard against production API calls."""
    # 1. Production Mode: skips REST calls, returns managed message
    settings.DEBUG = False
    res_prod = task_sync_positions()
    assert "positions managed by live trading algorithms" in res_prod

    # 2. Debug Mode: executes sync
    settings.DEBUG = True
    with patch("kalai.positions.sync_all_broker_positions", return_value=[{"account": "TEST"}]):
        res_dbg = task_sync_positions()
        assert "Debug sync" in res_dbg

        res_snap = create_daily_pnl_snapshots()
        assert "Finalized" in res_snap
