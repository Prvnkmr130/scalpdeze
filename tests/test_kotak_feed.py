"""
tests/test_kotak_feed.py
────────────────────────
Unit tests for Kotak Neo WebSocket Feed, Token Formatter, and Broker Registry.
"""

from algo_trading.brokers.feeds.kotak_neo import KotakNeoFeed, format_kotak_scrip
from algo_trading.brokers.registry import BrokerRegistry


def test_format_kotak_scrip():
    """Test scrip formatting for various token inputs."""
    assert format_kotak_scrip(11536) == "nse_cm|11536"
    assert format_kotak_scrip("260105") == "nse_fo|260105"
    assert format_kotak_scrip("260105", segment_map={"260105": "nse_cm"}) == "nse_cm|260105"
    assert format_kotak_scrip("nse_cm|11536") == "nse_cm|11536"
    assert format_kotak_scrip("NSE|11536") == "nse_cm|11536"
    assert format_kotak_scrip("NFO|54321") == "nse_fo|54321"
    assert format_kotak_scrip("BSE|500325") == "bse_cm|500325"
    assert format_kotak_scrip("MCX|426123") == "mcx_fo|426123"
    assert format_kotak_scrip("CDS|12345") == "cde_fo|12345"
    assert format_kotak_scrip({"segment": "nse_fo", "token": "54321"}) == "nse_fo|54321"
    assert format_kotak_scrip({"exchange": "BSE", "instrument_token": "500325"}) == "bse_cm|500325"



def test_kotak_feed_instantiation():
    """Test KotakNeoFeed instantiation and attributes."""
    feed = KotakNeoFeed(
        account_id="kotak_acc1",
        broker_name="kotak_neo",
        api_provider="kotak_neo",
        api_key="mock_ucc",
        api_secret="mock_secret",
        access_token="mock_token:::mock_sid:::wss://mlhsm.kotaksecurities.com:::mock_access",
        instrument_tokens=[11536, "nse_fo|54321"],
    )

    assert feed.BROKER_NAME == "kotak_neo"
    assert feed.account_id == "kotak_acc1"
    assert feed.instrument_tokens == [11536, "nse_fo|54321"]

    auth_token, sid = feed._parse_auth_credentials()
    assert auth_token == "mock_token"
    assert sid == "mock_sid"


def test_kotak_feed_normalize_tick_raw():
    """Test that normalize_tick wraps the raw payload directly into unified frame without modification."""
    feed = KotakNeoFeed(
        account_id="kotak_user",
        broker_name="kotak_neo",
        api_provider="kotak_neo",
        api_key="mock_ucc",
        api_secret="mock_secret",
        access_token="mock_token",
        instrument_tokens=[],
    )

    raw_tick_payload = {
        "tk": "11536",
        "e": "nse_cm",
        "ltp": 2540.25,
        "v": 1500000,
        "op": 2510.0,
        "h": 2555.0,
        "lo": 2505.0,
        "c": 2512.0,
        "cng": 28.25,
        "nc": "1.12",
        "ts": "TCS-EQ",
    }

    frame = feed.normalize_tick(raw_tick_payload)

    assert isinstance(frame, dict)
    assert frame["account_id"] == "kotak_user"
    assert frame["broker"] == "kotak_neo"
    assert frame["api_provider"] == "kotak_neo"
    assert "timestamp" in frame
    assert frame["data"] == raw_tick_payload
    assert frame["data"]["ltp"] == 2540.25
    assert frame["data"]["tk"] == "11536"


def test_broker_registry_discovery():
    """Test that BrokerRegistry discovers kotak_neo."""
    reg = BrokerRegistry()
    assert "kotak_neo" in reg.registered_brokers
    assert reg._feed_classes.get("kotak_neo") is KotakNeoFeed
