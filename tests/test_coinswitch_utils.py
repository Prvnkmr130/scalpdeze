"""
tests/test_coinswitch_utils.py
──────────────────────────────
Unit tests for CoinSwitchUtility order execution, portfolio inspection, Ed25519 signing, and market data.
"""

from unittest.mock import MagicMock, patch
import pytest

from algo_trading.algos.coinswitch_utils import CoinSwitchUtility, retry
from kalai.auth.coinswitch import CoinSwitchAuthAdapter
from kalai.auth.registry import get_auth_adapter

TEST_PRIV_HEX = "de471ae0a85c02d7a3c0b1f74e92675056bdd4aa10aa137f8af3641f8b4ce69b"
TEST_PUB_HEX = "7feee7f61d29ac71352925ba32cb0c586f9e687b6c6e97071ee9311e6465c33c"


def test_retry_success():
    call_count = 0

    def mock_fn():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ConnectionError("Transient network timeout")
        return "success"

    result = retry(mock_fn, max_retries=3, delay=0.01)
    assert result == "success"
    assert call_count == 2


def test_retry_exhausted_raises():
    def failing_fn():
        raise ValueError("Permanent failure")

    with pytest.raises(ValueError, match="Permanent failure"):
        retry(failing_fn, max_retries=2, delay=0.01)


def test_coinswitch_utility_direct_keys():
    util = CoinSwitchUtility(
        api_key=TEST_PUB_HEX,
        api_secret=TEST_PRIV_HEX,
        account_id="direct_test",
    )
    assert util.account_id == "direct_test"
    assert util.api_key == TEST_PUB_HEX
    assert util.api_secret == TEST_PRIV_HEX
    assert util._private_key is not None


def test_coinswitch_utility_db_lookup_mock():
    mock_broker = MagicMock()
    mock_broker.account_id = "coinswitch_pro_1"
    mock_broker.name = "coinswitch_pro_1"
    mock_broker.api_key = TEST_PUB_HEX
    mock_broker.api_secret = TEST_PRIV_HEX

    with patch("kalai.models.Broker.objects.filter") as mock_filter:
        mock_filter.return_value.first.return_value = mock_broker

        util = CoinSwitchUtility(account_id="coinswitch_pro_1")
        assert util.account_id == "coinswitch_pro_1"
        assert util.api_key == TEST_PUB_HEX
        assert util.api_secret == TEST_PRIV_HEX


def test_coinswitch_ed25519_signing():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    headers, signed_path = util._sign_request("POST", "/trade/api/v2/order", params={"exchange": "coinswitchx"})

    assert "X-AUTH-APIKEY" in headers
    assert headers["X-AUTH-APIKEY"] == TEST_PUB_HEX
    assert "X-AUTH-SIGNATURE" in headers
    assert len(headers["X-AUTH-SIGNATURE"]) == 128  # 64 bytes in hex
    assert "X-AUTH-EPOCH" in headers
    assert "exchange=coinswitchx" in signed_path


def test_coinswitch_chk_live_bal():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)

    mock_portfolio = {
        "data": {
            "total_portfolio_value_inr": 250000.0,
            "balances": [
                {"currency": "INR", "available_balance": "50000.0", "total_balance": "50000.0"},
                {"currency": "USDT", "available_balance": "1200.0", "total_balance": "1200.0"},
            ]
        }
    }
    mock_futures = {
        "data": {
            "available_balance": 800.0,
            "total_wallet_balance": 1500.0,
        }
    }

    with patch.object(util, "get_user_portfolio", return_value=mock_portfolio), \
         patch.object(util, "get_futures_wallet_balance", return_value=mock_futures):

        avail, net_cap = util.chk_live_bal()
        assert avail == 52000.0  # 50000 + 1200 + 800
        assert net_cap == 251500.0  # 250000 + 1500


def test_coinswitch_holdings():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)

    mock_portfolio = {
        "data": {
            "balances": [
                {"currency": "BTC", "available_balance": 0.05, "locked_balance": 0.01, "total_balance": 0.06},
                {"currency": "ETH", "available_balance": 1.5, "locked_balance": 0.0, "total_balance": 1.5},
                {"currency": "DOGE", "available_balance": 0.0, "locked_balance": 0.0, "total_balance": 0.0},
            ]
        }
    }

    with patch.object(util, "get_user_portfolio", return_value=mock_portfolio):
        df = util.holdings()
        assert df is not None
        assert len(df) == 2
        assert df["currency"].to_list() == ["BTC", "ETH"]
        btc_row = df.filter(df["currency"] == "BTC")
        assert btc_row["total"][0] == 0.06


def test_coinswitch_pos_data():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)

    mock_positions = {
        "data": [
            {
                "symbol": "BTC/USDT",
                "size": 0.1,
                "entry_price": 60000.0,
                "mark_price": 61000.0,
                "liquidation_price": 54000.0,
                "unrealized_pnl": 100.0,
                "realized_pnl": 25.0,
                "margin": 600.0,
                "leverage": 10,
            }
        ]
    }

    with patch.object(util, "_auth_request", return_value=mock_positions):
        day_df, net_df = util.pos_data(pair="BTC/USDT")
        assert day_df is not None and net_df is not None
        assert len(net_df) == 1
        assert net_df["symbol"][0] == "BTC/USDT"
        assert net_df["active_pos"][0] == 0.1
        assert net_df["unrealized_pnl"][0] == 100.0


def test_coinswitch_market_order_success():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    mock_resp = {"data": {"order_id": "CS_ORD_1001", "status": "FILLED"}}

    with patch.object(util, "_auth_request", return_value=mock_resp):
        order_id, msg = util.mrk_ordr(symbol="BTC/INR", quantity=0.005, buy_sell="BUY")
        assert order_id == "CS_ORD_1001"
        assert "successfully" in msg


def test_coinswitch_limit_order_success():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    mock_resp = {"data": {"order_id": "CS_LIM_2002"}}

    with patch.object(util, "_auth_request", return_value=mock_resp):
        order_id, msg = util.lim_ordr(symbol="BTC/USDT", quantity=0.01, buy_sell="BUY", price=59000.0)
        assert order_id == "CS_LIM_2002"
        assert "successfully" in msg


def test_coinswitch_limit_order_missing_price():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    order_id, msg = util.lim_ordr(symbol="BTC/USDT", quantity=0.01, price=0)
    assert order_id == -1
    assert "Price must be provided" in msg


def test_coinswitch_options_order():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    mock_resp = {"data": {"order_id": "CS_OPT_5005"}}

    with patch.object(util, "_auth_request", return_value=mock_resp) as mock_req:
        order_id, msg = util.lim_ordr(
            symbol="BTC-260826-60000-C",
            quantity=0.1,
            buy_sell="BUY",
            price=150.0,
            is_options=True,
        )
        assert order_id == "CS_OPT_5005"
        mock_req.assert_called_once()
        _, kwargs = mock_req.call_args
        assert kwargs.get("is_options") is True
        assert kwargs.get("json_body", {}).get("category") == "option"


def test_coinswitch_sl_order():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    mock_resp = {"data": {"order_id": "CS_SL_3003"}}

    with patch.object(util, "_auth_request", return_value=mock_resp):
        order_id, msg = util.sl_ordr(
            symbol="BTC/USDT",
            quantity=0.01,
            buy_sell="SELL",
            price=58000.0,
            trig_price=58200.0,
        )
        assert order_id == "CS_SL_3003"


def test_coinswitch_cancel_order():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    mock_resp = {"message": "Order cancelled"}

    with patch.object(util, "_auth_request", return_value=mock_resp):
        ok, msg = util.cancel_ordr(order_id="CS_ORD_1001")
        assert ok is True
        assert msg == "Order cancelled"


def test_coinswitch_cancel_all():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    mock_orders = [
        {"order_id": "ORD1", "symbol": "BTC/USDT"},
        {"order_id": "ORD2", "symbol": "BTC/USDT"},
    ]

    with patch.object(util, "orders", return_value=mock_orders), \
         patch.object(util, "cancel_ordr", return_value=(True, "Cancelled")):
        ok, msg = util.cancel_all(symbol="BTC/USDT")
        assert ok is True
        assert "Cancelled 2/2" in msg


def test_coinswitch_candles():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    mock_data = {
        "data": [
            [1700000000000, 58000.0, 58500.0, 57900.0, 58300.0, 10.5],
            [1700000060000, 58300.0, 58600.0, 58200.0, 58400.0, 8.2],
        ]
    }

    with patch.object(util, "_public_get", return_value=mock_data):
        df = util.candles("BTC/USDT", interval="1m", limit=2)
        assert df is not None
        assert len(df) == 2
        assert "open" in df.columns
        assert df["close"][1] == 58400.0


def test_coinswitch_order_book():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    mock_book = {
        "data": {
            "bids": [[58000.0, 1.2], [57990.0, 0.5]],
            "asks": [[58010.0, 0.8], [58020.0, 2.1]],
        }
    }

    with patch.object(util, "_public_get", return_value=mock_book):
        book = util.order_book("BTC/USDT")
        assert len(book["bids"]) == 2
        assert len(book["asks"]) == 2


def test_coinswitch_get_margin():
    util = CoinSwitchUtility(api_key=TEST_PUB_HEX, api_secret=TEST_PRIV_HEX)
    m = util.get_margin("BTC/USDT", quantity=0.1, price=60000.0, leverage=10)
    assert m["notional_value"] == 6000.0
    assert m["initial_margin"] == 600.0


def test_coinswitch_auth_adapter():
    adapter = CoinSwitchAuthAdapter()
    assert adapter.supports_direct_login() is True
    assert adapter.supports_oauth() is False

    mock_broker = MagicMock()
    mock_broker.account_id = "cs_acc_1"
    mock_broker.api_key = TEST_PUB_HEX
    mock_broker.api_secret = TEST_PRIV_HEX

    ok, msg = adapter.handle_direct_login(mock_broker)
    assert ok is True
    assert "verified" in msg

    # Test registry resolution
    mock_broker.name = "coinswitch"
    mock_broker.broker_name = None
    mock_broker.api_provider = None
    resolved = get_auth_adapter(mock_broker)
    assert isinstance(resolved, CoinSwitchAuthAdapter)
