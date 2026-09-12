# -*- coding: utf-8 -*-
"""
tests/test_account_intelligent_sync.py
───────────────────────────────────────
Validates intelligent tiered TTLs, event-driven dirty flags, selective API dispatch,
and optimistic local balance updating for IndianUserAccount and CryptoUserAccount.
"""

import time
import pytest
from unittest.mock import MagicMock
import polars as pl

from algo_trading.algos.indian_user_account import IndianUserAccount
from algo_trading.algos.crypto_opt_trde_polars import CryptoUserAccount


class MockClient:
    def __init__(self):
        self.bal_calls = 0
        self.hold_calls = 0
        self.pos_calls = 0
        self.order_calls = 0

    def chk_live_bal(self):
        self.bal_calls += 1
        return (100000.0, 100000.0)

    def holdings(self):
        self.hold_calls += 1
        return []

    def pos_data(self):
        self.pos_calls += 1
        return ([], [])

    def orders(self):
        self.order_calls += 1
        return []


def test_indian_user_account_tiered_ttl_caching():
    """Verify that calling sync_account_state within TTL windows avoids redundant API calls."""
    client = MockClient()
    acc = IndianUserAccount(user_id="ACC_TEST_TTL", client=client)

    # First call must invoke all endpoints
    acc.sync_account_state()
    assert client.bal_calls == 1
    assert client.hold_calls == 1
    assert client.pos_calls == 1
    assert client.order_calls == 1

    # Immediate second call should make ZERO additional API calls (cached)
    acc.sync_account_state()
    assert client.bal_calls == 1
    assert client.hold_calls == 1
    assert client.pos_calls == 1
    assert client.order_calls == 1


def test_indian_user_account_dirty_flags_selective_sync():
    """Verify that dirty flags force only the dirty endpoints to update."""
    client = MockClient()
    acc = IndianUserAccount(user_id="ACC_TEST_DIRTY", client=client)

    acc.sync_account_state()
    assert client.bal_calls == 1

    # Mark balance dirty
    acc.dirty_balance = True
    acc.sync_account_state()
    assert client.bal_calls == 2
    assert client.hold_calls == 1   # Holdings still cached!
    assert client.pos_calls == 1    # Positions still cached!


def test_indian_user_account_mark_order_placed_optimistic_balance():
    """Verify mark_order_placed updates balance optimistically and flags pending orders."""
    client = MockClient()
    acc = IndianUserAccount(user_id="ACC_TEST_ORDER", client=client)

    acc.live_balance = 50000.0
    acc.avail_cash = 50000.0

    acc.mark_order_placed(order_id="ORD123", symbol="NIFTY26SEP24500CE", qty=50, price=100.0, transaction_type="BUY")

    # 50 qty * 100 Rs = 5000 Rs cost
    assert acc.live_balance == 45000.0
    assert acc.avail_cash == 45000.0
    assert acc.has_pending_orders is True
    assert acc.dirty_balance is True
    assert acc.dirty_positions is True


def test_crypto_user_account_intelligent_sync():
    """Verify CryptoUserAccount also employs tiered TTLs and selective API execution."""
    client = MockClient()
    acc = CryptoUserAccount(user_id="ACC_CRYPTO_TEST", client=client)

    acc.sync_account_state()
    assert client.bal_calls == 1
    assert client.hold_calls == 1
    assert client.pos_calls == 1
    assert client.order_calls == 1

    # Cached call
    acc.sync_account_state()
    assert client.bal_calls == 1
    assert client.order_calls == 1

    # Optimistic order placement
    acc.live_balance = 1000.0
    acc.mark_order_placed(order_id="ORD_C1", symbol="BTCUSD", qty=1, price=200.0, transaction_type="BUY")
    assert acc.live_balance == 800.0
    assert acc.has_pending_orders is True

    # Next sync should update balance and orders
    acc.sync_account_state()
    assert client.bal_calls == 2
    assert client.order_calls == 2
    assert client.hold_calls == 1  # Holdings remain cached


def test_crypto_user_account_kwargs_and_parse_df():
    """Verify CryptoUserAccount accepts keyword arguments and robustly parses dataframes."""
    client = MockClient()
    acc = CryptoUserAccount(user_id="ACC_CRYPTO_KWARGS", client=client)

    # Calling with force_positions and force_balance directly
    acc.sync_account_state(force_positions=True, force_balance=True, debug_mode=True)
    assert client.bal_calls == 1
    assert client.pos_calls == 1

    # Verify _parse_df on diverse formats
    assert acc._parse_df(None).is_empty()
    assert len(acc._parse_df('[{"symbol": "BTCUSD", "price": 95000.0}]')) == 1
    assert len(acc._parse_df({"symbol": "BTCUSD", "price": 95000.0})) == 1
    assert len(acc._parse_df([{"symbol": "BTCUSD", "price": 95000.0}])) == 1


def test_delta_and_coinswitch_user_account_sync_kwargs():
    """Verify CryptoUserAccount accepts keyword arguments in sync_account_state without TypeError."""
    from algo_trading.algos.crypto_user_account import CryptoUserAccount

    client = MockClient()
    crypto_acc = CryptoUserAccount(user_id="CRYPTO_TEST", client=client)

    # Calling with force_positions and force_balance must not raise TypeError
    crypto_acc.sync_account_state(force_positions=True, force_balance=True, debug_mode=True)


def test_crypto_user_account_standalone_import():
    """Verify CryptoUserAccount can be imported from crypto_user_account directly and is identical to crypto_opt_trde_polars."""
    from algo_trading.algos.crypto_user_account import CryptoUserAccount as StandaloneCryptoUserAccount, UserAccount as StandaloneUserAccount
    from algo_trading.algos.crypto_opt_trde_polars import CryptoUserAccount as EngineCryptoUserAccount

    assert StandaloneCryptoUserAccount is EngineCryptoUserAccount
    assert StandaloneUserAccount is StandaloneCryptoUserAccount


def test_crypto_feeds_and_registry_resolution():
    """Verify BrokerRegistry discovers all crypto feeds (Delta, CoinSwitch, CoinDCX) and Kotak Neo with aliases."""
    from algo_trading.brokers.registry import BrokerRegistry

    registry = BrokerRegistry()
    classes = registry._feed_classes

    # Verify all crypto providers and aliases map properly
    assert "coinswitch" in classes
    assert "coinswitch_pro" in classes
    assert "delta" in classes
    assert "delta_exchange" in classes
    assert "delta_india" in classes
    assert "coindcx" in classes
    assert "coindcx_pro" in classes
    assert "kotak_neo" in classes
    assert "kotak" in classes
    assert "zerodha" in classes
    assert "kite" in classes


def test_sync_account_state_binds_thread_context():
    """Verify that ThreadPool worker threads in CryptoUserAccount and IndianUserAccount bind thread-local context."""
    from algo_trading.algos.logger import algo_logger

    crypto_seen_account = None
    crypto_seen_algo = None

    class ContextCheckCryptoClient(MockClient):
        def pos_data(self):
            nonlocal crypto_seen_account, crypto_seen_algo
            crypto_seen_account = algo_logger.context.active_account
            crypto_seen_algo = algo_logger.context.algo_name
            return ([], [])

    mock_broker = MagicMock()
    mock_broker.account_id = "73270496"
    crypto_acc = CryptoUserAccount(
        user_id="73270496",
        client=ContextCheckCryptoClient(),
        broker_obj=mock_broker,
        enabled=True
    )
    crypto_acc.sync_account_state(force_positions=True)
    assert crypto_seen_account == mock_broker
    assert crypto_seen_algo == "crypto_opt_trde_polars"
    assert algo_logger.context.active_account is None

    indian_seen_account = None
    indian_seen_algo = None

    class ContextCheckIndianClient(MockClient):
        def pos_data(self):
            nonlocal indian_seen_account, indian_seen_algo
            indian_seen_account = algo_logger.context.active_account
            indian_seen_algo = algo_logger.context.algo_name
            return ([], [])

    indian_broker = MagicMock()
    indian_broker.account_id = "W1NPY"
    indian_acc = IndianUserAccount(
        user_id="W1NPY",
        client=ContextCheckIndianClient(),
        broker_obj=indian_broker,
        enabled=True
    )
    indian_acc.sync_account_state(force_positions=True)
    assert indian_seen_account == indian_broker
    assert indian_seen_algo == "indian_opt_trde_polars"
    assert algo_logger.context.active_account is None

