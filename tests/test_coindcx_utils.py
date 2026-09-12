"""
tests/test_coindcx_utils.py
───────────────────────────
Unit tests for CoinDCXUtility order management, balances, and market data wrappers.
"""

from unittest.mock import MagicMock, patch
import polars as pl
import pytest

from algo_trading.algos.coindcx_utils import CoinDCXUtility, retry


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


def test_coindcx_utility_direct_keys():
    util = CoinDCXUtility(
        api_key="mock_key",
        api_secret="mock_secret",
        account_id="direct_test",
    )
    assert util.account_id == "direct_test"
    assert util.api_key == "mock_key"
    assert util.api_secret == "mock_secret"


def test_coindcx_utility_db_lookup_mock():
    mock_broker = MagicMock()
    mock_broker.account_id = "coindcx_main"
    mock_broker.name = "coindcx_main"
    mock_broker.api_key = "test_api_key"
    mock_broker.api_secret = "test_api_secret"

    with patch("kalai.models.Broker.objects.filter") as mock_filter:
        mock_filter.return_value.first.return_value = mock_broker

        util = CoinDCXUtility(account_id="coindcx_main")
        assert util.account_id == "coindcx_main"
        assert util.api_key == "test_api_key"
        assert util.api_secret == "test_api_secret"


def test_coindcx_utility_missing_credentials_raises():
    with patch("kalai.models.Broker.objects.filter") as mock_filter:
        mock_filter.return_value.first.return_value = None

        with pytest.raises(ValueError, match="not found in database"):
            CoinDCXUtility(account_id="non_existent")


def test_sign_payload():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")
    body = {"timestamp": 1600000000000, "market": "BTCUSDT"}
    json_str, signature = util._sign_payload(body)

    assert '"timestamp":1600000000000' in json_str
    assert len(signature) == 64  # SHA256 hex string length


def test_chk_live_bal_futures_and_spot():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    mock_futures_wallets = [
        {"currency_short_name": "USDT", "balance": 500.0, "locked_balance": 50.0, "cross_margin": 100.0}
    ]
    mock_spot_balances = [
        {"currency": "USDT", "balance": 200.0, "locked_balance": 20.0},
        {"currency": "BTC", "balance": 0.5, "locked_balance": 0.0},
    ]

    with patch.object(util, "get_futures_wallets", return_value=mock_futures_wallets), \
         patch.object(util, "get_spot_balances", return_value=mock_spot_balances):
        avail_cash, net_capital = util.chk_live_bal()
        # Futures avail: 500 - 50 - 100 = 350. Spot avail: 200 - 20 = 180. Total avail = 530
        # Futures net: 500. Spot net: 200. Total net = 700
        assert avail_cash == 530.0
        assert net_capital == 700.0


def test_holdings():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    mock_spot = [
        {"currency": "BTC", "balance": "0.15", "locked_balance": "0.05"},
        {"currency": "ETH", "balance": "2.0", "locked_balance": "0.0"},
        {"currency": "DOGE", "balance": "0.0", "locked_balance": "0.0"},
    ]

    with patch.object(util, "get_spot_balances", return_value=mock_spot):
        df = util.holdings()
        assert df is not None
        assert len(df) == 2  # DOGE should be filtered out (total = 0)
        assert df["tradingsymbol"].to_list() == ["BTC", "ETH"]
        btc_row = df.filter(pl.col("tradingsymbol") == "BTC")
        assert btc_row["total"][0] == 0.20


def test_pos_data():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    mock_positions = [
        {
            "id": "pos_001",
            "pair": "B-BTC_USDT",
            "active_pos": "0.05",
            "avg_price": "60000.0",
            "locked_margin": "300.0",
            "liquidation_price": "45000.0",
            "unrealized_pnl": "50.0",
        }
    ]

    with patch.object(util, "_post", return_value=mock_positions):
        day_df, net_df = util.pos_data(pair="B-BTC_USDT")
        assert day_df is not None
        assert net_df is not None
        assert len(net_df) == 1
        assert net_df["tradingsymbol"][0] == "B-BTC_USDT"
        assert net_df["active_pos"][0] == 0.05
        assert net_df["avg_price"][0] == 60000.0


def test_mrk_ordr_futures():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    with patch.object(util, "_post", return_value={"id": "fut_ord_12345"}) as mock_post:
        order_id, msg = util.mrk_ordr(
            symbol="B-BTC_USDT",
            quantity=0.01,
            buy_sell="BUY",
            leverage=10,
        )
        assert order_id == "fut_ord_12345"
        assert msg == "order placed"

        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "exchange/v1/derivatives/futures/orders/create"
        assert args[1]["order"]["pair"] == "B-BTC_USDT"
        assert args[1]["order"]["side"] == "buy"
        assert args[1]["order"]["order_type"] == "market_order"
        assert args[1]["order"]["leverage"] == 10
        assert args[1]["order"]["total_quantity"] == 0.01


def test_lim_ordr_spot():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    with patch.object(util, "_post", return_value={"orders": [{"id": "spot_ord_999"}]}) as mock_post:
        order_id, msg = util.lim_ordr(
            symbol="BTCUSDT",
            quantity=0.02,
            buy_sell="SELL",
            price=65000.0,
            is_futures=False,
        )
        assert order_id == "spot_ord_999"
        assert msg == "order placed"

        mock_post.assert_called_once()
        args, _ = mock_post.call_args
        assert args[0] == "exchange/v1/orders/create"
        assert args[1]["market"] == "BTCUSDT"
        assert args[1]["side"] == "sell"
        assert args[1]["order_type"] == "limit_order"
        assert args[1]["price_per_unit"] == 65000.0
        assert args[1]["total_quantity"] == 0.02


def test_sl_ordr_futures():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    with patch.object(util, "_post", return_value={"id": "sl_ord_555"}) as mock_post:
        order_id, msg = util.sl_ordr(
            symbol="B-ETH_USDT",
            quantity=1.0,
            buy_sell="SELL",
            price=3000.0,
            trig_price=3050.0,
            leverage=5,
        )
        assert order_id == "sl_ord_555"
        assert msg == "order placed"

        mock_post.assert_called_once()
        args, _ = mock_post.call_args
        assert args[0] == "exchange/v1/derivatives/futures/orders/create"
        assert args[1]["order"]["order_type"] == "stop_limit"
        assert args[1]["order"]["price"] == "3000.0"
        assert args[1]["order"]["stop_price"] == "3050.0"


def test_slmkt_ordr_futures():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    with patch.object(util, "_post", return_value={"id": "slm_ord_777"}) as mock_post:
        order_id, msg = util.slmkt_ordr(
            symbol="B-BTC_USDT",
            quantity=0.01,
            buy_sell="SELL",
            trig_price=59000.0,
            leverage=10,
        )
        assert order_id == "slm_ord_777"
        assert msg == "order placed"

        mock_post.assert_called_once()
        args, _ = mock_post.call_args
        assert args[0] == "exchange/v1/derivatives/futures/orders/create"
        assert args[1]["order"]["order_type"] == "take_profit"
        assert args[1]["order"]["stop_price"] == "59000.0"


def test_cancel_ordr_and_cancel_all():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    with patch.object(util, "_post", return_value={"status": "cancelled"}) as mock_post:
        res = util.cancel_ordr(order_id="fut_123", is_futures=True)
        assert res == {"status": "cancelled"}
        mock_post.assert_called_once_with(
            "exchange/v1/derivatives/futures/orders/cancel", {"id": "fut_123"}
        )

    with patch.object(util, "_post", return_value={"status": "all_cancelled"}) as mock_post:
        res = util.cancel_all(symbol="B-BTC_USDT", is_futures=True)
        assert res == {"status": "all_cancelled"}
        mock_post.assert_called_once_with(
            "exchange/v1/derivatives/futures/orders/cancel_all", {"pair": "B-BTC_USDT"}
        )


def test_exit_position():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    mock_pos_df = pl.DataFrame([
        {"pair": "B-BTC_USDT", "active_pos": 0.05}
    ])

    with patch.object(util, "pos_data", return_value=(mock_pos_df, mock_pos_df)), \
         patch.object(util, "mrk_ordr", return_value=("exit_ord_1", "order placed")) as mock_mrk:
        order_id, msg = util.exit_position("B-BTC_USDT")
        assert order_id == "exit_ord_1"
        assert msg == "order placed"
        # Since active_pos was +0.05 (LONG), opposite order should be SELL 0.05
        mock_mrk.assert_called_once_with(
            symbol="B-BTC_USDT",
            quantity=0.05,
            buy_sell="SELL",
            is_futures=True,
        )


def test_candles():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    mock_candle_data = [
        {
            "open": "50000.0",
            "high": "50200.0",
            "low": "49900.0",
            "close": "50150.0",
            "volume": "12.5",
            "time": 1622548800000,
        }
    ]

    with patch.object(util, "_get", return_value=mock_candle_data):
        df = util.candles("B-BTC_USDT", interval="5m", limit=100)
        assert df is not None
        assert len(df) == 1
        assert "datetime" in df.columns
        assert df["open"][0] == 50000.0
        assert df["close"][0] == 50150.0


def test_get_margin():
    util = CoinDCXUtility(api_key="key123", api_secret="secret123")

    orders = [
        {"symbol": "B-BTC_USDT", "quantity": 0.1, "price": 60000.0, "leverage": 10},
        {"symbol": "B-ETH_USDT", "quantity": 1.0, "price": 3000.0, "leverage": 5},
    ]
    margins = util.get_margin(orders)
    assert len(margins) == 2
    assert margins[0]["notional_value"] == 6000.0
    assert margins[0]["initial_margin"] == 600.0
    assert margins[1]["notional_value"] == 3000.0
    assert margins[1]["initial_margin"] == 600.0
