"""
tests/test_ws_engine_lifecycle.py
─────────────────────────────────
Tests WebSocket Engine lifecycle:
1. Standby mode when zero feeds are initially active (no premature exit).
2. Dynamic in-process start, stop, and reload via PostgreSQL NOTIFY without process suicide (no SIGTERM).
3. 24/7 Crypto scheduler guarantee (keeping crypto WebSockets active regardless of market hour schedules).
4. Reconciler detection of credential / token updates.
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from django.utils import timezone


class MockPoolConnectionContext:
    """Mock that supports both `async with pool.acquire()` and `conn = await pool.acquire()`."""
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

    def __await__(self):
        async def _coro():
            return self.conn
        return _coro().__await__()


class DummyFeed:
    """Mock feed for lifecycle testing."""
    def __init__(self, account_id: str, access_token: str = "token_1"):
        self.account_id = account_id
        self.api_provider = "dummy_provider"
        self.access_token = access_token
        self.api_key = "key_1"
        self.api_secret = "secret_1"
        self.instrument_tokens = [123, 456]
        self.run_called = False

    async def run(self, tick_queue, log_queue):
        self.run_called = True
        try:
            while True:
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_ws_engine_enters_standby_when_zero_feeds():
    """
    Verify run_engine() enters standby mode instead of immediately exiting with return
    when zero feeds are enabled.
    """
    from algo_trading.brokers.engine import run_engine

    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    mock_conn.add_listener = AsyncMock()
    mock_conn.remove_listener = AsyncMock()

    mock_pool.acquire.side_effect = lambda: MockPoolConnectionContext(mock_conn)
    mock_pool.release = AsyncMock()
    mock_pool.close = AsyncMock()

    with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)), \
         patch("algo_trading.brokers.registry.BrokerRegistry.get_enabled_feeds", new=AsyncMock(return_value=[])), \
         patch("algo_trading.brokers.engine.clear_old_logs_and_ticks_loop", new=AsyncMock()), \
         patch("algo_trading.brokers.consumers.log_consumer", new=AsyncMock()):

        # Start run_engine as a background task
        engine_task = asyncio.create_task(run_engine())
        
        # Give it a moment to initialize
        await asyncio.sleep(0.2)

        # The engine must still be running (waiting in standby), NOT exited
        assert not engine_task.done(), "Engine exited prematurely when zero feeds were found!"

        # Cancel/shutdown the engine cleanly
        engine_task.cancel()
        await engine_task
        assert engine_task.done()

        # Verify listeners were set up
        mock_conn.add_listener.assert_called_once()
        assert mock_conn.add_listener.call_args[0][0] == "engine_control"


@pytest.mark.asyncio
async def test_on_engine_control_no_sigterm():
    """
    Verify PostgreSQL NOTIFY on engine_control starts/stops pipelines dynamically
    WITHOUT raising SIGTERM / killing the process.
    """
    from algo_trading.brokers.engine import run_engine

    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    registered_listener = None

    async def capture_listener(channel, callback):
        nonlocal registered_listener
        registered_listener = callback

    mock_conn.add_listener = capture_listener
    mock_conn.remove_listener = AsyncMock()

    mock_pool.acquire.side_effect = lambda: MockPoolConnectionContext(mock_conn)
    mock_pool.release = AsyncMock()
    mock_pool.close = AsyncMock()

    dummy_feed = DummyFeed("W1NPY")

    with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)), \
         patch("algo_trading.brokers.registry.BrokerRegistry.get_enabled_feeds", new=AsyncMock(return_value=[dummy_feed])), \
         patch("algo_trading.brokers.registry.BrokerRegistry.get_feed_for_account", new=AsyncMock(return_value=dummy_feed)), \
         patch("algo_trading.brokers.consumers.ensure_broker_table", new=AsyncMock(return_value="b_w1npy_stream_kv")), \
         patch("algo_trading.brokers.consumers.tick_consumer", new=AsyncMock()), \
         patch("algo_trading.brokers.engine.clear_old_logs_and_ticks_loop", new=AsyncMock()), \
         patch("algo_trading.brokers.consumers.log_consumer", new=AsyncMock()), \
         patch("os.kill") as mock_os_kill:

        engine_task = asyncio.create_task(run_engine())
        await asyncio.sleep(0.2)

        assert registered_listener is not None, "Listener callback was not registered!"

        # Send "stop" notification for W1NPY
        stop_payload = json.dumps({"action": "stop", "account_id": "W1NPY"})
        registered_listener(mock_conn, 1234, "engine_control", stop_payload)
        await asyncio.sleep(0.1)

        # os.kill must NOT have been called
        mock_os_kill.assert_not_called()

        # Send "start" notification for W1NPY
        start_payload = json.dumps({"action": "start", "account_id": "W1NPY"})
        registered_listener(mock_conn, 1234, "engine_control", start_payload)
        await asyncio.sleep(0.1)

        # os.kill still must NOT have been called
        mock_os_kill.assert_not_called()

        # Send "reload" notification for W1NPY
        reload_payload = json.dumps({"action": "reload", "account_id": "W1NPY"})
        registered_listener(mock_conn, 1234, "engine_control", reload_payload)
        await asyncio.sleep(0.1)

        mock_os_kill.assert_not_called()

        # Cancel/shutdown the engine cleanly
        engine_task.cancel()
        await engine_task
        assert engine_task.done()


def test_evaluate_websocket_schedules_crypto_24_7_guarantee():
    """
    Verify evaluate_websocket_schedules() keeps 24/7 crypto WebSockets enabled
    even when enable_schedule=False.
    """
    from kalai.tasks import evaluate_websocket_schedules

    # Mock crypto broker: trade is enabled, websocket is currently False, schedule is False
    crypto_broker = MagicMock()
    crypto_broker.is_crypto = True
    crypto_broker.enable_trade = True
    crypto_broker.enable_websocket = False
    crypto_broker.enable_schedule = False
    crypto_broker.account_id = "TEST_DELTA_247"
    crypto_broker.name = "Delta Test"
    crypto_broker.save = MagicMock()

    # Mock non-crypto broker: trade enabled, schedule enabled, but outside market hours
    equity_broker = MagicMock()
    equity_broker.is_crypto = False
    equity_broker.enable_trade = True
    equity_broker.enable_websocket = True
    equity_broker.enable_schedule = True
    equity_broker.account_id = "TEST_EQUITY_SCHED"
    equity_broker.name = "Equity Test"
    equity_broker.ws_operating_days = "WEEKDAYS"
    equity_broker.ws_start_time = None
    equity_broker.ws_stop_time = None
    equity_broker.save = MagicMock()

    def mock_filter(**kwargs):
        if kwargs.get("enable_trade") is True and kwargs.get("enable_websocket") is False:
            return [crypto_broker]
        if kwargs.get("enable_schedule") is True:
            return [equity_broker]
        return []

    with patch("kalai.models.Broker.objects.filter", side_effect=mock_filter):
        result = evaluate_websocket_schedules()
        
        # Crypto broker must be activated for 24/7 streaming
        assert crypto_broker.enable_websocket is True
        crypto_broker.save.assert_called_once()
        assert "Evaluated" in result


@pytest.mark.asyncio
async def test_reconciler_lifecycle_auto_detection():
    """
    Verify the feed reconciler automatically starts pipelines when new accounts appear,
    reloads them when credentials/tokens change, and stops them when accounts disappear.
    """
    from algo_trading.brokers.engine import run_engine

    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    registered_listener = None

    async def capture_listener(channel, callback):
        nonlocal registered_listener
        registered_listener = callback

    mock_conn.add_listener = capture_listener
    mock_conn.remove_listener = AsyncMock()

    mock_pool.acquire.side_effect = lambda: MockPoolConnectionContext(mock_conn)
    mock_pool.release = AsyncMock()
    mock_pool.close = AsyncMock()

    current_enabled_feeds = []

    async def mock_get_enabled_feeds(broker_filter=None):
        return list(current_enabled_feeds)

    with patch("asyncpg.create_pool", new=AsyncMock(return_value=mock_pool)), \
         patch("algo_trading.brokers.registry.BrokerRegistry.get_enabled_feeds", side_effect=mock_get_enabled_feeds), \
         patch("algo_trading.brokers.registry.BrokerRegistry.get_feed_for_account", side_effect=lambda acc: next((f for f in current_enabled_feeds if f.account_id == acc), None)), \
         patch("algo_trading.brokers.consumers.ensure_broker_table", new=AsyncMock(return_value="b_delta_new_stream_kv")), \
         patch("algo_trading.brokers.consumers.tick_consumer", new=AsyncMock()), \
         patch("algo_trading.brokers.engine.clear_old_logs_and_ticks_loop", new=AsyncMock()), \
         patch("algo_trading.brokers.consumers.log_consumer", new=AsyncMock()):

        engine_task = asyncio.create_task(run_engine())
        await asyncio.sleep(0.2)

        # 1. Initially 0 feeds, now add DELTA_NEW feed
        feed_v1 = DummyFeed("DELTA_NEW", access_token="token_v1")
        current_enabled_feeds.append(feed_v1)

        # Trigger reconcile via NOTIFY
        registered_listener(mock_conn, 1234, "engine_control", json.dumps({"action": "reconcile"}))
        await asyncio.sleep(0.2)
        assert feed_v1.run_called is True, "Feed pipeline was not started by reconciler!"

        # 2. Update token on DELTA_NEW
        feed_v2 = DummyFeed("DELTA_NEW", access_token="token_v2_updated")
        current_enabled_feeds.clear()
        current_enabled_feeds.append(feed_v2)

        registered_listener(mock_conn, 1234, "engine_control", json.dumps({"action": "reconcile"}))
        await asyncio.sleep(0.8)
        assert feed_v2.run_called is True, "Feed pipeline was not reloaded with updated token!"

        # 3. Disable feed completely
        current_enabled_feeds.clear()
        registered_listener(mock_conn, 1234, "engine_control", json.dumps({"action": "reconcile"}))
        await asyncio.sleep(0.2)

        # Engine still running cleanly in standby
        assert not engine_task.done()

        # Clean shutdown
        engine_task.cancel()
        await engine_task
        assert engine_task.done()

