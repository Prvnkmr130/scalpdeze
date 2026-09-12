from unittest.mock import patch
import pytest
from kalai.models import Broker, BrokerType, ApiProvider
from algo_trading.algos import load_all_algos, run_algos_sequential


def test_algo_registry_filters_by_enabled_trade():
    """Verify load_all_algos only loads algos whose broker has enable_trade=True."""
    # 1. Only Zerodha enabled
    with patch("algo_trading.algos._get_enabled_broker_identifiers", return_value={"zerodha"}):
        algos = load_all_algos()
        algo_names = [fn.__name__ for fn in algos]
        assert "indian_options_trading_algo_polars" in algo_names
        assert "crypto_options_trading_algo_polars" not in algo_names

    # 2. Enable CoinSwitch as well
    with patch("algo_trading.algos._get_enabled_broker_identifiers", return_value={"zerodha", "coinswitch"}):
        algos = load_all_algos()
        algo_names = [fn.__name__ for fn in algos]
        assert "indian_options_trading_algo_polars" in algo_names
        assert "crypto_options_trading_algo_polars" in algo_names

    # 3. Disable all
    with patch("algo_trading.algos._get_enabled_broker_identifiers", return_value=set()):
        algos = load_all_algos()
        assert len(algos) == 0

    # 4. Explicit filter_by_enabled=False returns all production algos
    all_algos = load_all_algos(filter_by_enabled=False)
    assert len(all_algos) == 2
    assert "indian_options_trading_algo_polars" in [fn.__name__ for fn in all_algos]
    assert "crypto_options_trading_algo_polars" in [fn.__name__ for fn in all_algos]


def test_run_algos_sequential_execution():
    """Verify run_algos_sequential executes registered algorithms in sequence."""
    calls = []

    def mock_algo(acc_id: str):
        calls.append(acc_id)

    result = run_algos_sequential(accounts=["ACC_SEQ_1", "ACC_SEQ_2"], algos=[mock_algo])
    assert result["executed_count"] == 2
    assert result["accounts_count"] == 2
    assert len(result["errors"]) == 0
    assert calls == ["ACC_SEQ_1", "ACC_SEQ_2"]


def test_run_algos_sequential_error_isolation():
    """Verify run_algos_sequential catches per-algo errors without halting execution."""
    calls = []

    def failing_algo(acc_id: str):
        raise ValueError(f"Failing on {acc_id}")

    def successful_algo(acc_id: str):
        calls.append(acc_id)

    result = run_algos_sequential(accounts=["ACC_TEST_1"], algos=[failing_algo, successful_algo])
    assert result["executed_count"] == 1
    assert len(result["errors"]) == 1
    assert result["errors"][0]["account_id"] == "ACC_TEST_1"
    assert calls == ["ACC_TEST_1"]



def test_active_algo_context_strict_thread_isolation():
    """Verify ActiveAlgoContext never bleeds context across threads or retains a global default."""
    import threading
    from unittest.mock import MagicMock
    from algo_trading.algos.logger import algo_logger

    algo_logger.clear_active_account()

    mock_broker_1 = MagicMock()
    mock_broker_1.account_id = "ACC_THREAD_1"

    algo_logger.set_active_account(mock_broker_1, algo_name="algo_1")
    assert algo_logger.get_active_account() == mock_broker_1
    assert algo_logger.context.algo_name == "algo_1"

    thread_results = {}

    def worker():
        thread_results["active_account"] = algo_logger.get_active_account()
        thread_results["algo_name"] = algo_logger.context.algo_name

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    # Worker thread must NOT inherit Thread 1's context
    assert thread_results["active_account"] is None
    assert thread_results["algo_name"] is None

    # Clearing on main thread resets cleanly
    algo_logger.clear_active_account()
    assert algo_logger.get_active_account() is None
    assert algo_logger.context.algo_name is None


@pytest.mark.django_db
def test_crypto_24_7_execution_and_schedule_respect():
    """Verify crypto accounts execute 24/7 and enable_schedule is respected."""
    from datetime import datetime, time
    from algo_trading.brokers.algo_engine import is_account_active, get_active_scheduled_accounts
    from kalai.models import Broker, BrokerType, ApiProvider

    bt_delta, _ = BrokerType.objects.get_or_create(code="delta_india", defaults={"name": "Delta Exchange (India)"})
    ap_delta, _ = ApiProvider.objects.get_or_create(code="delta_india", defaults={"name": "Delta Exchange API (India)"})

    bt_kotak, _ = BrokerType.objects.get_or_create(code="kotak_neo", defaults={"name": "Kotak Neo"})
    ap_kotak, _ = ApiProvider.objects.get_or_create(code="kotak_neo", defaults={"name": "Kotak Neo API"})

    try:
        b_crypto = Broker(
            account_id="DELTA_247",
            name="Delta Test",
            broker_name=bt_delta,
            api_provider=ap_delta,
            enable_trade=True,
        )
        b_crypto.save()

        # Verify Broker properties
        assert b_crypto.is_crypto is True
        # Auto-defaulted enable_schedule=False on creation
        assert b_crypto.enable_schedule is False

        b_equity = Broker.objects.create(
            account_id="KOTAK_SCHED",
            name="Kotak Test",
            broker_name=bt_kotak,
            api_provider=ap_kotak,
            enable_trade=True,
            enable_schedule=True,
            ws_start_time=time(9, 0, 0),
            ws_stop_time=time(23, 55, 0),
            ws_operating_days="WEEKDAYS",
        )
        assert b_equity.is_crypto is False

        # Simulate early morning 04:00 AM (outside 09:00 - 23:55)
        early_dt = datetime(2026, 9, 5, 4, 0, 0)

        # 1. Crypto broker MUST be active at 04:00 AM (24/7)
        assert is_account_active(
            now=early_dt,
            enable_trade=b_crypto.enable_trade,
            ws_start_time=b_crypto.ws_start_time,
            ws_stop_time=b_crypto.ws_stop_time,
            ws_operating_days=b_crypto.ws_operating_days,
            enable_schedule=b_crypto.enable_schedule,
            is_crypto=b_crypto.is_crypto,
        ) is True

        # 2. Equity broker with enable_schedule=True MUST be inactive at 04:00 AM
        assert is_account_active(
            now=early_dt,
            enable_trade=b_equity.enable_trade,
            ws_start_time=b_equity.ws_start_time,
            ws_stop_time=b_equity.ws_stop_time,
            ws_operating_days=b_equity.ws_operating_days,
            enable_schedule=b_equity.enable_schedule,
            is_crypto=b_equity.is_crypto,
        ) is False

        # 3. If enable_schedule is explicitly disabled on equity broker, it also runs
        b_equity.enable_schedule = False
        assert is_account_active(
            now=early_dt,
            enable_trade=b_equity.enable_trade,
            ws_start_time=b_equity.ws_start_time,
            ws_stop_time=b_equity.ws_stop_time,
            ws_operating_days=b_equity.ws_operating_days,
            enable_schedule=b_equity.enable_schedule,
            is_crypto=b_equity.is_crypto,
        ) is True
    finally:
        Broker.objects.filter(account_id__in=["DELTA_247", "KOTAK_SCHED"]).delete()




