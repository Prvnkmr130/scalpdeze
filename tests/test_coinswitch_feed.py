"""
tests/test_coinswitch_feed.py
─────────────────────────────
Unit tests for CoinSwitch WebSocket Feed, tick normalization, and Broker Registry discovery.
"""

from algo_trading.brokers.feeds.coinswitch import CoinSwitchFeed
from algo_trading.brokers.registry import BrokerRegistry


def test_coinswitch_feed_instantiation():
    """Test CoinSwitchFeed instantiation and attributes."""
    feed = CoinSwitchFeed(
        account_id="coinswitch_user_1",
        broker_name="coinswitch",
        api_provider="coinswitch",
        api_key="test_api_key",
        api_secret="test_secret",
        instrument_tokens=["BTC,INR", "ETH/USDT", "SOL_USDT"],
    )

    assert feed.BROKER_NAME == "coinswitch"
    assert "coinswitch_pro" in feed.ALIASES
    assert feed.account_id == "coinswitch_user_1"
    assert feed.instrument_tokens == ["BTC,INR", "ETH/USDT", "SOL_USDT"]


def test_coinswitch_feed_normalize_tick():
    """Test tick normalization from raw CoinSwitch Socket.IO events."""
    feed = CoinSwitchFeed(
        account_id="cs_user",
        broker_name="coinswitch",
        api_key="test_key",
        api_secret="test_secret",
        instrument_tokens=[],
    )

    raw_payload = {
        "symbol": "BTC,INR",
        "last_price": 5850000.0,
        "open": 5800000.0,
        "high": 5900000.0,
        "low": 5780000.0,
        "volume": 15.2,
        "bids": [[5849000.0, 1.5], [5848000.0, 2.0]],
        "asks": [[5850000.0, 0.8], [5851000.0, 3.2]],
        "timestamp": 1700000000000,
    }

    tick = feed.normalize_tick(raw_payload, event_type="trades")

    assert isinstance(tick, dict)
    assert isinstance(tick["instrument_token"], int)
    assert tick["tradingsymbol"] == "BTC/INR"
    assert tick["last_price"] == 5850000.0
    assert tick["high"] == 5900000.0
    assert tick["buy_demand"] == 1.5
    assert tick["sell_demand"] == 0.8
    assert len(tick["depth"]["buy"]) == 2
    assert len(tick["depth"]["sell"]) == 2


def test_coinswitch_feed_normalize_depth_event():
    """Test normalization when price is inferred from top bid."""
    feed = CoinSwitchFeed(
        account_id="cs_user",
        broker_name="coinswitch",
        api_key="test_key",
        api_secret="test_secret",
        instrument_tokens=[],
    )

    raw_depth = {
        "s": "ETH/USDT",
        "b": [[3200.5, 5.0]],
        "a": [[3201.0, 4.2]],
        "q": 100.0,
    }

    tick = feed.normalize_tick(raw_depth, event_type="orderbook")
    assert tick["tradingsymbol"] == "ETH/USDT"
    assert tick["last_price"] == 3200.5
    assert tick["buy_demand"] == 5.0
    assert tick["sell_demand"] == 4.2


def test_broker_registry_discovers_coinswitch():
    """Test that BrokerRegistry discovers coinswitch feed."""
    reg = BrokerRegistry()
    assert "coinswitch" in reg.registered_brokers
    assert "coinswitch_pro" in reg.registered_brokers or "coinswitch" in reg._feed_classes
    assert reg._feed_classes.get("coinswitch") is CoinSwitchFeed
