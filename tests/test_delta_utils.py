"""
tests/test_delta_utils.py
─────────────────────────
Unit tests for DeltaExchangeUtility order management, balances, positions, and API signing.
"""

from unittest.mock import MagicMock, patch
import polars as pl
import pytest

from algo_trading.algos.delta_utils import DeltaExchangeUtility, retry


def test_retry_success():
    call_count = 0

    def mock_fn():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ConnectionError("Transient network failure")
        return "success"

    result = retry(mock_fn, max_retries=3, delay=0.01)
    assert result == "success"
    assert call_count == 2


def test_retry_exhausted_raises():
    def failing_fn():
        raise ValueError("Permanent failure")

    with pytest.raises(ValueError, match="Permanent failure"):
        retry(failing_fn, max_retries=2, delay=0.01)


def test_delta_utility_direct_keys():
    util = DeltaExchangeUtility(
        api_key="mock_key",
        api_secret="mock_secret",
        account_id="direct_test",
        base_url="https://api.india.delta.exchange",
    )
    assert util.account_id == "direct_test"
    assert util.api_key == "mock_key"
    assert util.api_secret == "mock_secret"
    assert util.base_url == "https://api.india.delta.exchange"


def test_delta_utility_db_lookup_mock():
    mock_broker = MagicMock()
    mock_broker.account_id = "delta_main"
    mock_broker.name = "delta_main"
    mock_broker.api_key = "test_api_key"
    mock_broker.api_secret = "test_api_secret"
    mock_broker.api_endpoint = "https://api.delta.exchange"

    with patch("kalai.models.Broker.objects.filter") as mock_filter:
        mock_filter.return_value.first.return_value = mock_broker

        util = DeltaExchangeUtility(account_id="delta_main")
        assert util.account_id == "delta_main"
        assert util.api_key == "test_api_key"
        assert util.api_secret == "test_api_secret"
        assert util.base_url == "https://api.delta.exchange"


def test_delta_utility_missing_credentials_raises():
    with patch("kalai.models.Broker.objects.filter") as mock_filter:
        mock_filter.return_value.order_by.return_value.first.return_value = None
        mock_filter.return_value.first.return_value = None

        with pytest.raises(ValueError, match="not found in database"):
            DeltaExchangeUtility(account_id="non_existent")


def test_delta_signature_generation():
    util = DeltaExchangeUtility(api_key="key123", api_secret="secret123")
    sig, ts = util._generate_signature("GET", "/v2/wallet/balances")

    assert len(sig) == 64  # SHA256 hex string length
    assert ts.isdigit()


def test_chk_live_bal_mock():
    util = DeltaExchangeUtility(api_key="key123", api_secret="secret123")

    mock_balances = [
        {"asset": {"symbol": "USDT"}, "balance": 15000.0, "available_balance": 12000.0},
        {"asset": {"symbol": "BTC"}, "balance": 0.5, "available_balance": 0.5},
        {"asset_symbol": "INR", "balance": 50000.0, "available_balance": 45000.0},
    ]

    with patch.object(util, "_auth_request", return_value=mock_balances):
        avail, total = util.chk_live_bal()
        # USDT avail: 12000, INR avail: 45000 => 57000
        # USDT total: 15000, BTC total: 0.5, INR total: 50000 => 65000.5
        assert avail == 57000.0
        assert total == 65000.5


def test_holdings_returns_polars_df():
    util = DeltaExchangeUtility(api_key="key123", api_secret="secret123")

    mock_balances = [
        {"asset": {"symbol": "USDT"}, "balance": 1000.0, "available_balance": 800.0},
        {"asset": {"symbol": "BTC"}, "balance": 0.1, "available_balance": 0.1},
    ]

    with patch.object(util, "_auth_request", return_value=mock_balances):
        df = util.holdings()
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (2, 5)
        assert "asset_symbol" in df.columns
        assert "available_balance" in df.columns
        assert df["balance"].to_list() == [1000.0, 0.1]


def test_pos_data_returns_polars_df():
    util = DeltaExchangeUtility(api_key="key123", api_secret="secret123")

    mock_positions = [
        {
            "product": {"symbol": "BTCUSD", "id": 27},
            "size": 5.0,
            "entry_price": 92000.0,
            "mark_price": 93500.0,
            "liquidation_price": 70000.0,
            "margin": 500.0,
            "unrealized_pnl": 7500.0,
            "realized_pnl": 0.0,
        }
    ]

    with patch.object(util, "_auth_request", return_value=mock_positions):
        day_df, net_df = util.pos_data(pair="BTCUSD")
        assert isinstance(net_df, pl.DataFrame)
        assert net_df.shape == (1, 9)
        assert net_df["symbol"][0] == "BTCUSD"
        assert net_df["unrealized_pnl"][0] == 7500.0


def test_mrk_ordr_success():
    util = DeltaExchangeUtility(api_key="key123", api_secret="secret123")

    with patch.object(util, "resolve_product_id", return_value=27), \
         patch.object(util, "_auth_request", return_value={"id": 998877}):
        order_id, msg = util.mrk_ordr(symbol="BTCUSD", quantity=1, buy_sell="BUY")
        assert order_id == "998877"
        assert "placed successfully" in msg


def test_lim_ordr_success():
    util = DeltaExchangeUtility(api_key="key123", api_secret="secret123")

    with patch.object(util, "resolve_product_id", return_value=27), \
         patch.object(util, "_auth_request", return_value={"id": 998878}):
        order_id, msg = util.lim_ordr(symbol="BTCUSD", quantity=1, buy_sell="BUY", price=90000.0)
        assert order_id == "998878"
        assert "placed successfully at 90000.0" in msg


def test_sl_ordr_success():
    util = DeltaExchangeUtility(api_key="key123", api_secret="secret123")

    with patch.object(util, "resolve_product_id", return_value=27), \
         patch.object(util, "_auth_request", return_value={"id": 998879}):
        order_id, msg = util.sl_ordr(symbol="BTCUSD", quantity=1, buy_sell="SELL", trig_price=88000.0, price=87900.0)
        assert order_id == "998879"
        assert "placed successfully at trigger 88000.0" in msg


def test_cancel_ordr_and_cancel_all():
    util = DeltaExchangeUtility(api_key="key123", api_secret="secret123")

    with patch.object(util, "_auth_request", return_value={}):
        ok1, msg1 = util.cancel_ordr(order_id="12345")
        assert ok1 is True
        assert "cancelled successfully" in msg1

        ok2, msg2 = util.cancel_all(product_id=27)
        assert ok2 is True
        assert "cancelled for product 27" in msg2


def test_delta_rest_client_aliases():
    util = DeltaExchangeUtility(api_key="key123", api_secret="secret123")

    with patch.object(util, "_auth_request", return_value={"orders": []}) as mock_auth, \
         patch.object(util, "_public_get", return_value={"tickers": []}) as mock_pub:
        util.get_wallet()
        mock_auth.assert_called_with("GET", "/wallet/balances")

        util.get_ticker("BTCUSD")
        mock_pub.assert_called_with("/tickers/BTCUSD")

        util.get_assets()
        mock_pub.assert_called_with("/assets")


def test_pos_data_missing_credentials_returns_empty_and_logs():
    util = DeltaExchangeUtility(api_key="key123", api_secret="")
    with patch("algo_trading.algos.logger.algo_logger.log_sync") as mock_log:
        day_df, net_df = util.pos_data()
        assert isinstance(day_df, pl.DataFrame)
        assert isinstance(net_df, pl.DataFrame)
        assert day_df.is_empty()
        assert net_df.is_empty()
        mock_log.assert_called()
        assert "missing" in mock_log.call_args[0][0].lower()


def test_log_sync_infers_broker_from_message_and_crypto_algo():
    from algo_trading.algos.logger import algo_logger
    from kalai.models import Broker

    mock_broker = MagicMock(spec=Broker)
    mock_broker.account_id = "73270496"
    mock_broker.name = "delta_crypto"
    mock_broker.broker_name = MagicMock(code="delta_india")
    mock_broker.api_provider = MagicMock(code="delta_exchange")
    mock_broker._state = MagicMock()
    mock_broker._meta = Broker._meta

    with patch.object(Broker.objects, "filter") as mock_filter, \
         patch.object(algo_logger, "flush_sync"):
        mock_filter.return_value.first.return_value = mock_broker
        algo_logger._sync_log_buffer.clear()

        # Simulate log with no broker passed, and algo_name passed as 'indian_opt_trde_polars' or None
        err_msg = "[delta_utils.py:461 in pos_data()] Failed to fetch Delta Exchange positions: API credentials (api_key/api_secret) are missing for account '73270496'. Authenticated action cannot proceed."
        algo_logger.log_sync(
            message=err_msg,
            tag="BROKER_API",
            level="ERROR",
            broker_obj=None,
            algo_name="indian_opt_trde_polars"  # Errant default from intercept handler
        )

        assert len(algo_logger._sync_log_buffer) == 1
        logged_entry = algo_logger._sync_log_buffer[0]
        # Account must be correctly resolved to the Delta broker
        assert logged_entry.account == mock_broker
        # Algo name must be corrected to crypto_opt_trde_polars because of delta_utils and delta broker
        assert logged_entry.algo_name == "crypto_opt_trde_polars"
        assert logged_entry.level == "ERROR"
        assert logged_entry.tag == "BROKER_API"
        algo_logger._sync_log_buffer.clear()


def test_intercept_handler_routes_crypto_utils_to_crypto_algo():
    import logging
    from algo_trading.algos.indian_opt_trde_polars import InterceptDBLogHandler

    handler = InterceptDBLogHandler()
    record = logging.LogRecord(
        name="algo_trading.algos.delta_utils",
        level=logging.ERROR,
        pathname="c:/algo_trading/algos/delta_utils.py",
        lineno=461,
        msg="API credentials (api_key/api_secret) are missing for account '73270496'",
        args=(),
        exc_info=None,
        func="pos_data"
    )

    with patch("algo_trading.algos.indian_opt_trde_polars._db_insert_single_log") as mock_insert:
        handler.emit(record)
        mock_insert.assert_called_once()
        _, kwargs = mock_insert.call_args
        assert kwargs.get("algo_name") == "crypto_opt_trde_polars"

