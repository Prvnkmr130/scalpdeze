from unittest.mock import patch
import pytest
from datetime import timedelta
from django.utils import timezone
from django.contrib.auth.models import User
from django.test import Client
from django.urls import reverse
from kalai.models import (
    Broker,
    BrokerType,
    ApiProvider,
    AlgoLog,
    ProcessedTickStore,
    BrokerPosition,
    DailyPnLSnapshot,
    TradeRecord,
)
from kalai.admin import evaluate_all_algos_status


TEST_ACCOUNT_IDS = {"Z1", "K1", "C1", "Z_MAIN", "Z_HEDGE", "Z_PROD", "DELTA_1", "UPSTOX_1"}


def _clean_db():
    AlgoLog.objects.filter(account__account_id__in=TEST_ACCOUNT_IDS).delete()
    Broker.objects.filter(account_id__in=TEST_ACCOUNT_IDS).delete()


@pytest.mark.django_db
def test_evaluate_all_algos_status_states():
    """Verify evaluate_all_algos_status accurately computes RUNNING, STOPPED, IDLE, and ERROR states."""
    _clean_db()

    bt_z, _ = BrokerType.objects.get_or_create(code="zerodha", defaults={"name": "Zerodha"})
    bt_k, _ = BrokerType.objects.get_or_create(code="kotak_neo", defaults={"name": "Kotak Neo"})
    bt_c, _ = BrokerType.objects.get_or_create(code="coinswitch", defaults={"name": "CoinSwitch PRO"})

    ap_z, _ = ApiProvider.objects.get_or_create(code="zerodha", defaults={"name": "Zerodha API"})
    ap_k, _ = ApiProvider.objects.get_or_create(code="kotak_neo", defaults={"name": "Kotak Neo API"})
    ap_c, _ = ApiProvider.objects.get_or_create(code="coinswitch", defaults={"name": "CoinSwitch API"})

    # 1. Zerodha: Enabled + Recent Log -> RUNNING
    b_z = Broker.objects.create(account_id="Z1", name="Zerodha Acc", broker_name=bt_z, api_provider=ap_z, enable_trade=True)
    AlgoLog.objects.create(
        account=b_z,
        algo_name="indian_opt_trde_polars",
        tag="TRADE",
        level="INFO",
        message="[INDIAN OPT POLARS CYCLE] Executed strategy iteration successfully",
        timestamp=timezone.now() - timedelta(seconds=20)
    )

    # 2. Kotak: Enabled + Old Log (> 5m) -> IDLE
    b_k = Broker.objects.create(account_id="K1", name="Kotak Acc", broker_name=bt_k, api_provider=ap_k, enable_trade=True)
    AlgoLog.objects.create(
        account=b_k,
        algo_name="indian_opt_trde_polars",
        tag="GENERAL",
        level="INFO",
        message="Standby check",
        timestamp=timezone.now() - timedelta(minutes=10)
    )

    # 3. CoinSwitch: Disabled -> STOPPED
    b_c = Broker.objects.create(account_id="C1", name="CoinSwitch Acc", broker_name=bt_c, api_provider=ap_c, enable_trade=False)

    test_brokers = [b_c, b_k, b_z]  # sorted order
    with patch("kalai.models.Broker.objects.select_related") as mock_sr:
        mock_sr.return_value.all.return_value.order_by.return_value = test_brokers

        algos, summary = evaluate_all_algos_status()
        algo_map = {a["key"]: a for a in algos}

        assert algo_map["zerodha"]["status"] == "RUNNING"
        assert algo_map["kotak_neo"]["status"] == "IDLE"
        assert algo_map["coinswitch"]["status"] == "STOPPED"

        assert summary["running"] == 1
        assert summary["idle"] == 1
        assert summary["stopped"] == 1
        assert summary["errors"] == 0

        # 4. Add an error to Zerodha -> ERROR
        AlgoLog.objects.create(
            account=b_z,
            algo_name="indian_opt_trde_polars",
            tag="EXCEPTION",
            level="ERROR",
            message="[INDIAN OPT POLARS CYCLE ERROR] Margin breach failure",
            timestamp=timezone.now()
        )

        algos, summary = evaluate_all_algos_status()
        algo_map = {a["key"]: a for a in algos}

        assert algo_map["zerodha"]["status"] == "ERROR"
        assert summary["errors"] == 1

    _clean_db()


@pytest.mark.django_db
def test_algo_status_admin_view_permissions():
    """Verify algo_status_view is accessible to staff members and renders cleanly."""
    client = Client()
    User.objects.filter(username="admin_test").delete()
    admin_user = User.objects.create_superuser(username="admin_test", email="admin@example.com", password="password123")
    client.force_login(admin_user)

    url = reverse("admin:algo_status")
    response = client.get(url)
    assert response.status_code == 200
    assert "Algorithm Status & Execution Monitor" in response.content.decode()
    assert "Running Strategies" in response.content.decode()
    User.objects.filter(username="admin_test").delete()


@pytest.mark.django_db
def test_admin_index_displays_monitoring_and_documentation():
    """Verify both Algo Monitoring and Documentation sections render on the admin index dashboard in proper sequence."""
    client = Client()
    User.objects.filter(username="admin_test").delete()
    admin_user = User.objects.create_superuser(username="admin_test", email="admin@example.com", password="password123")
    client.force_login(admin_user)

    url = reverse("admin:index")
    response = client.get(url)
    assert response.status_code == 200
    content = response.content.decode()
    assert "⚡ Algo Monitoring" in content
    assert "Algorithm Status & Execution Monitor" in content
    assert "📘 Documentation" in content
    assert "Multi-Broker Setup & Credential Mapping Guide" in content

    # Verify sequential visual hierarchy: Algo Monitoring -> Kalai -> Documentation -> Authentication
    pos_algo_monitor = content.find("app-algo-monitoring")
    pos_kalai = content.find("app-kalai")
    pos_doc = content.find("app-documentation")
    pos_auth = content.find("app-auth")

    assert pos_algo_monitor != -1
    assert pos_kalai != -1
    assert pos_doc != -1
    assert pos_auth != -1
    assert pos_algo_monitor < pos_kalai < pos_doc < pos_auth
    User.objects.filter(username="admin_test").delete()


@pytest.mark.django_db
def test_admin_nav_sidebar_ordering_sequence():
    """Verify left collapsible nav sidebar renders sections matching index sequence: Algo -> Kalai -> Docs -> Auth."""
    client = Client()
    User.objects.filter(username="admin_nav").delete()
    admin_user = User.objects.create_superuser(username="admin_nav", email="admin_nav@example.com", password="password123")
    client.force_login(admin_user)

    # Fetch a model changelist view which renders nav_sidebar
    url = reverse("admin:kalai_broker_changelist")
    response = client.get(url)
    assert response.status_code == 200
    content = response.content.decode()

    sidebar_idx = content.find('id="nav-sidebar"')
    assert sidebar_idx != -1
    sidebar_content = content[sidebar_idx:]

    pos_algo_monitor = sidebar_content.find("app-algo-monitoring")
    pos_kalai = sidebar_content.find("app-kalai")
    pos_doc = sidebar_content.find("app-documentation")
    pos_auth = sidebar_content.find("app-auth")

    assert pos_algo_monitor != -1
    assert pos_kalai != -1
    assert pos_doc != -1
    assert pos_auth != -1
    assert pos_algo_monitor < pos_kalai < pos_doc < pos_auth
    User.objects.filter(username="admin_nav").delete()


@pytest.mark.django_db
def test_evaluate_all_algos_status_dynamically_includes_added_brokers():
    """Verify that newly added broker accounts (e.g. Delta Exchange, Upstox) dynamically appear in Algorithm Status."""
    _clean_db()

    bt_delta, _ = BrokerType.objects.get_or_create(code="delta_exchange", defaults={"name": "Delta Exchange"})
    bt_upstox, _ = BrokerType.objects.get_or_create(code="upstox", defaults={"name": "Upstox"})

    ap_delta, _ = ApiProvider.objects.get_or_create(code="delta", defaults={"name": "Delta Exchange API"})
    ap_upstox, _ = ApiProvider.objects.get_or_create(code="upstox", defaults={"name": "Upstox API"})

    b_delta = Broker.objects.create(account_id="DELTA_1", name="Delta Acc", broker_name=bt_delta, api_provider=ap_delta, enable_trade=True)
    b_upstox = Broker.objects.create(account_id="UPSTOX_1", name="Upstox Acc", broker_name=bt_upstox, api_provider=ap_upstox, enable_trade=False)

    test_brokers = [b_delta, b_upstox]
    with patch("kalai.models.Broker.objects.select_related") as mock_sr:
        mock_sr.return_value.all.return_value.order_by.return_value = test_brokers

        algos, summary = evaluate_all_algos_status()
        algo_map = {a["key"]: a for a in algos}

        assert summary["total"] == 2
        assert "delta_exchange" in algo_map
        assert "upstox" in algo_map
        assert algo_map["delta_exchange"]["broker_name"] == "Delta Exchange"
        assert algo_map["delta_exchange"]["status"] == "IDLE"  # Enabled with no logs yet
        assert algo_map["upstox"]["broker_name"] == "Upstox"
        assert algo_map["upstox"]["status"] == "STOPPED"  # Trade disabled

    _clean_db()


@pytest.mark.django_db
def test_evaluate_all_algos_status_multi_account_grouping():
    """Verify multiple accounts under the same broker are grouped together with individual pills."""
    _clean_db()

    bt_z, _ = BrokerType.objects.get_or_create(code="zerodha", defaults={"name": "Zerodha"})
    ap_z, _ = ApiProvider.objects.get_or_create(code="zerodha", defaults={"name": "Zerodha Kite API"})

    b1 = Broker.objects.create(account_id="Z_MAIN", name="Zerodha Main", broker_name=bt_z, api_provider=ap_z, enable_trade=True)
    b2 = Broker.objects.create(account_id="Z_HEDGE", name="Zerodha Hedge", broker_name=bt_z, api_provider=ap_z, enable_trade=False)

    test_brokers = [b2, b1]
    with patch("kalai.models.Broker.objects.select_related") as mock_sr:
        mock_sr.return_value.all.return_value.order_by.return_value = test_brokers

        algos, summary = evaluate_all_algos_status()
        assert summary["total"] == 1
        assert len(algos) == 1

        zerodha_algo = algos[0]
        assert zerodha_algo["key"] == "zerodha"
        assert len(zerodha_algo["linked_accounts"]) == 2
        acc_ids = {a["account_id"] for a in zerodha_algo["linked_accounts"]}
        assert acc_ids == {"Z_MAIN", "Z_HEDGE"}
        assert zerodha_algo["has_enabled_account"] is True

    _clean_db()


@pytest.mark.django_db
def test_evaluate_all_algos_status_master_engine_log_matching():
    """Verify unified production master engine logs (indian_opt_trde_polars / crypto_opt_trde_polars) match accounts."""
    _clean_db()

    bt_z, _ = BrokerType.objects.get_or_create(code="zerodha", defaults={"name": "Zerodha"})
    ap_z, _ = ApiProvider.objects.get_or_create(code="zerodha", defaults={"name": "Zerodha Kite API"})
    b_z = Broker.objects.create(account_id="Z_PROD", name="Zerodha Prod", broker_name=bt_z, api_provider=ap_z, enable_trade=True)

    # Master engine logs using 'indian_opt_trde_polars'
    AlgoLog.objects.create(
        account=b_z,
        algo_name="indian_opt_trde_polars",
        tag="TRADE",
        level="INFO",
        message="[INDIAN OPT POLARS CYCLE] Iteration complete",
        timestamp=timezone.now() - timedelta(seconds=15)
    )

    with patch("kalai.models.Broker.objects.select_related") as mock_sr:
        mock_sr.return_value.all.return_value.order_by.return_value = [b_z]

        algos, summary = evaluate_all_algos_status()
        assert summary["running"] == 1
        assert algos[0]["status"] == "RUNNING"
        assert algos[0]["latest_log"] is not None
        assert "[INDIAN OPT POLARS CYCLE]" in algos[0]["latest_log"].message

    _clean_db()


@pytest.mark.django_db
def test_evaluate_all_algos_status_empty_db():
    """Verify clean graceful return when no broker accounts exist in the database."""
    with patch("kalai.models.Broker.objects.select_related") as mock_sr:
        mock_sr.return_value.all.return_value.order_by.return_value = []

        algos, summary = evaluate_all_algos_status()
        assert algos == []
        assert summary == {"total": 0, "running": 0, "errors": 0, "idle": 0, "stopped": 0}


