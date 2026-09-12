"""
tests/test_delta_feed.py
────────────────────────
Unit tests for Delta Exchange WebSocket Feed, tick normalization, and Broker Registry discovery.
"""

from algo_trading.brokers.feeds.delta import DeltaExchangeFeed
from algo_trading.brokers.registry import BrokerRegistry


def test_delta_feed_instantiation():
    feed = DeltaExchangeFeed(
        account_id="delta_user_1",
        broker_name="delta_exchange",
        api_provider="delta_exchange",
        api_key="test_api_key",
        api_secret="test_secret",
        instrument_tokens=["BTCUSD", "ETHUSD", "SOLUSD"],
    )

    assert feed.BROKER_NAME == "delta_exchange"
    assert "delta" in feed.ALIASES
    assert "delta_india" in feed.ALIASES
    assert feed.account_id == "delta_user_1"
    assert feed.instrument_tokens == ["BTCUSD", "ETHUSD", "SOLUSD"]


def test_delta_feed_normalize_tick():
    feed = DeltaExchangeFeed(
        account_id="delta_user",
        broker_name="delta_exchange",
        api_key="test_key",
        api_secret="test_secret",
        instrument_tokens=[],
    )

    raw_payload = {
        "symbol": "BTCUSD",
        "close": 94500.0,
        "open": 93000.0,
        "high": 95000.0,
        "low": 92800.0,
        "volume": 320.5,
        "buy": [[94490.0, 10.0]],
        "sell": [[94510.0, 8.0]],
        "timestamp": 1740000000000,
    }

    tick = feed.normalize_tick(raw_payload)

    assert isinstance(tick, dict)
    assert tick["account_id"] == "delta_user"
    data = tick["data"]
    assert data["tradingsymbol"] == "BTCUSD"
    assert data["last_price"] == 94500.0
    assert data["high"] == 95000.0
    assert data["volume"] == 320.5
    assert len(data["depth"]["buy"]) == 1
    assert len(data["depth"]["sell"]) == 1


def test_broker_registry_discovers_delta():
    reg = BrokerRegistry()
    assert "delta_exchange" in reg.registered_brokers or "delta" in reg._feed_classes or "delta_exchange" in reg._feed_classes
    feed_cls = reg._feed_classes.get("delta_exchange") or reg._feed_classes.get("delta")
    assert feed_cls is DeltaExchangeFeed
