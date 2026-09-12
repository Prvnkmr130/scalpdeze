# -*- coding: utf-8 -*-
"""
tests/test_indian_master_engine.py
───────────────────────────────────
Validates initialization, decoupled Tier 1 (market calculation) / Tier 2 (account execution),
and Signal-Driven JIT sync in TradeAlgo.
"""

import os
import pytest
from unittest.mock import MagicMock, patch
import polars as pl

from algo_trading.algos.indian_user_account import IndianUserAccount
from algo_trading.algos.indian_opt_trde_polars import TradeAlgo, discover_indian_broker_accounts


class MockBrokerClient:
    def __init__(self):
        self.chk_live_bal = MagicMock(return_value=(150000.0, 150000.0))
        self.holdings = MagicMock(return_value=[])
        self.pos_data = MagicMock(return_value=([], []))
        self.orders = MagicMock(return_value=[])
        self.lim_ordr = MagicMock(return_value=(123456, "COMPLETE"))
        self.cancel_ordr = MagicMock(return_value=(123456, "CANCELLED"))


def test_trade_algo_init_order_and_discovery():
    """Verify TradeAlgo initializes without AttributeError and correctly sets up accounts."""
    client1 = MockBrokerClient()
    client2 = MockBrokerClient()

    acc1 = IndianUserAccount(user_id="ACC_USER_1", client=client1, capital_allowed=200000.0)
    acc2 = IndianUserAccount(user_id="ACC_USER_2", client=client2, capital_allowed=100000.0)

    # Initialize TradeAlgo with two accounts in debug mode
    engine = TradeAlgo(accounts=[acc1, acc2], debug_mode=True)

    # Verify session and holiday calendar exist
    assert hasattr(engine, 'nse_holiday_info')
    assert hasattr(engine, 'session')
    assert engine.session is not None

    # Verify multi-account lookup
    assert engine.get_account("ACC_USER_1") is acc1
    assert engine.get_account("ACC_USER_2") is acc2
    assert engine.get_account("NON_EXISTENT") is None


def test_trade_algo_tier_1_and_tier_2_decoupling():
    """Verify compute_market_signals and execute_account_trade execute cleanly."""
    client1 = MockBrokerClient()
    acc1 = IndianUserAccount(user_id="ACC_USER_1", client=client1)
    engine = TradeAlgo(accounts=[acc1], debug_mode=True)

    # Mock empty tick data to verify compute_market_signals doesn't crash
    engine.compute_market_signals(tick_data=pl.DataFrame())

    # Execute trade for account
    engine.execute_account_trade(acc1)

    # Verify acc1 was synced and accessed
    assert client1.chk_live_bal.called


def test_signal_driven_jit_sync():
    """Verify that when buy or sell signals exist, force_positions & force_balance are triggered."""
    client = MockBrokerClient()
    acc = IndianUserAccount(user_id="ACC_JIT_TEST", client=client)
    engine = TradeAlgo(accounts=[acc], debug_mode=True)

    # Seed an active buy signal
    engine.buy_stock_cap = pl.DataFrame([{
        "tradingsymbol": "NIFTY26SEP24500CE",
        "instrument_token": 12345,
        "exchange": "NFO",
        "tradable": 1,
        "last_price": 105.0
    }])

    # Spy on sync_account_state
    original_sync = acc.sync_account_state
    sync_args = []

    def spy_sync(*args, **kwargs):
        sync_args.append(kwargs)
        return original_sync(*args, **kwargs)

    acc.sync_account_state = spy_sync

    engine.execute_account_trade(acc)

    # Must have triggered force_positions=True, force_balance=True due to active signal
    assert any(call.get("force_positions") is True for call in sync_args)
    assert any(call.get("force_balance") is True for call in sync_args)


def test_indian_opt_trde_polars_account_sync_keyword_args():
    """Verify IndianUserAccount imported from indian_opt_trde_polars accepts force_positions and force_balance."""
    from algo_trading.algos.indian_opt_trde_polars import IndianUserAccount as PolarsIndianUserAccount
    from algo_trading.algos.indian_user_account import IndianUserAccount as CanonicalIndianUserAccount

    assert PolarsIndianUserAccount is CanonicalIndianUserAccount

    client = MockBrokerClient()
    acc = PolarsIndianUserAccount(user_id="POLARS_KWARGS_TEST", client=client)

    # Calling with force_positions and force_balance directly must not raise TypeError
    acc.sync_account_state(force_positions=True, force_balance=True, debug_mode=True)
    assert client.chk_live_bal.called
    assert client.pos_data.called

    # Calling via TradeAlgo execution with active signals
    engine = TradeAlgo(accounts=[acc], debug_mode=True)
    engine.buy_stock_cap = pl.DataFrame([{
        "tradingsymbol": "NIFTY26SEP24500CE",
        "instrument_token": 12345,
        "exchange": "NFO",
        "tradable": 1,
        "last_price": 105.0
    }])
    # Should execute without TypeError: sync_account_state() got an unexpected keyword argument 'force_positions'
    engine.execute_account_trade(acc)


def test_build_aug_table_zero_capital_share_and_tradable_stock_independence():
    """Verify build_aug_table filters strictly by Capital_share > 0 and Max_lots_per_order > 0, ignoring Tradable_stock."""
    from algo_trading.algos.polars_excel import build_aug_table

    sample_df = pl.DataFrame([
        {"Symbol": "NATGASMINI", "Capital_share": 1.0, "Max_lots_per_order": 1, "Tradable_stock": "Yes"},
        {"Symbol": "NIFTY", "Capital_share": 0.0, "Max_lots_per_order": 1, "Tradable_stock": "Yes"},
        {"Symbol": "BANKNIFTY", "Capital_share": 0.0, "Max_lots_per_order": 2, "Tradable_stock": "Yes"},
        {"Symbol": "CRUDEOILM", "Capital_share": None, "Max_lots_per_order": 1, "Tradable_stock": "Yes"},
        {"Symbol": "GOLDM", "Capital_share": 1.0, "Max_lots_per_order": 0, "Tradable_stock": "Yes"},  # Zero lots
        {"Symbol": "SILVERM", "Capital_share": 1.0, "Max_lots_per_order": 1, "Tradable_stock": "No"}, # Tradable_stock='No' must NOT exclude it
    ])

    aug = build_aug_table(sample_df)
    assert len(aug) == 2
    assert set(aug["Symbol"].to_list()) == {"NATGASMINI", "SILVERM"}

    # Verify when no symbols have Capital_share > 0, it returns empty DataFrame without resurrecting all
    zero_cap_df = pl.DataFrame([
        {"Symbol": "NIFTY", "Capital_share": 0.0, "Max_lots_per_order": 1},
        {"Symbol": "BANKNIFTY", "Capital_share": 0.0, "Max_lots_per_order": 1},
    ])
    empty_aug = build_aug_table(zero_cap_df)
    assert empty_aug.is_empty()

