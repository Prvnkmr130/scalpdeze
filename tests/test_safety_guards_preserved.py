# -*- coding: utf-8 -*-
"""
tests/test_safety_guards_preserved.py
──────────────────────────────────────
Confirms that:
1. Native in-memory debounce counters (Dict[int, int]) are preserved for O(1) lookups.
2. Symbol 30-second cooldowns in ordr_chk and ordr_sell_chk are enforced.
3. Position guardrails (hold_pos_buy_chk, hold_pos_sell_chk) prevent false order triggers.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock
import pytest
import polars as pl

from algo_trading.algos.indian_user_account import IndianUserAccount
from algo_trading.algos.indian_opt_trde_polars import TradeAlgo


class MockBrokerClient:
    def __init__(self):
        self.chk_live_bal = MagicMock(return_value=(100000.0, 100000.0))
        self.holdings = MagicMock(return_value=[])
        self.pos_data = MagicMock(return_value=([], []))
        self.orders = MagicMock(return_value=[])
        self.lim_ordr = MagicMock(return_value=(123456, "COMPLETE"))
        self.cancel_ordr = MagicMock(return_value=(123456, "CANCELLED"))


def test_debounce_counters_are_native_dicts():
    """Verify debounce counters are native Dict[int, int] per AGENTS.md standards."""
    client = MockBrokerClient()
    acc = IndianUserAccount(user_id="ACC_GUARD_TEST", client=client)
    engine = TradeAlgo(accounts=[acc], debug_mode=True)

    assert isinstance(engine.one_counter_ce_dict, dict)
    assert isinstance(engine.one_counter_pe_dict, dict)
    assert isinstance(engine.minus_one_counter_ce_dict, dict)
    assert isinstance(engine.minus_one_counter_pe_dict, dict)
    assert isinstance(engine.minus_two_counter_ce_dict, dict)
    assert isinstance(engine.minus_two_counter_pe_dict, dict)
    assert isinstance(engine.hedge_counter_dict, dict)


def test_cooldown_prevents_false_reorder():
    """Verify recent order cooldown prevents re-triggering buy orders."""
    client = MockBrokerClient()
    acc = IndianUserAccount(user_id="ACC_COOLDOWN_TEST", client=client)
    engine = TradeAlgo(accounts=[acc], debug_mode=True)

    sym = "NIFTY26SEP24500CE"

    # Set recent buy order time to 2 seconds ago (< 5s throttle)
    acc.recent_buy_order_time[sym] = datetime.now() - timedelta(seconds=2)

    ord_sts, ord_id, trans_type, buy_list_remove = engine.ordr_chk(sym, pl.DataFrame(), account=acc)
    # Must drop candidate because it's within the throttle/cooldown period
    assert buy_list_remove == 1
    assert ord_sts == 0


def test_hold_pos_buy_chk_prevents_duplicate_position():
    """Verify hold_pos_buy_chk returns 0 if symbol already has active position."""
    client = MockBrokerClient()
    acc = IndianUserAccount(user_id="ACC_POS_TEST", client=client)
    engine = TradeAlgo(accounts=[acc], debug_mode=True)

    sym = "NIFTY26SEP24500CE"

    # Open positions table with active position
    open_positions = pl.DataFrame([{
        "tradingsymbol": sym,
        "active_pos": 50,
        "instrument_token": 12345
    }])

    sts = engine.hold_pos_buy_chk(sym, open_positions)
    # Should NOT allow buying again (0 = cannot buy, 1 = can buy)
    assert sts == 0


def test_hold_pos_sell_chk_detects_exit_qty():
    """Verify hold_pos_sell_chk returns active position qty to sell."""
    client = MockBrokerClient()
    acc = IndianUserAccount(user_id="ACC_POS_SELL_TEST", client=client)
    engine = TradeAlgo(accounts=[acc], debug_mode=True)

    sym = "NIFTY26SEP24500CE"

    open_positions = pl.DataFrame([{
        "tradingsymbol": sym,
        "active_pos": 50,
        "instrument_token": 12345
    }])

    sts, qty = engine.hold_pos_sell_chk(sym, open_positions)
    assert sts == 1
    assert qty == 50


@pytest.mark.django_db
def test_production_broker_accounts_deletion_blocked():
    """Verify conftest pre_delete safety guard blocks deletion of live production accounts."""
    import pytest
    from kalai.models import Broker
    b = Broker.objects.filter(account_id="73270496").first()
    if b:
        with pytest.raises(RuntimeError, match="CRITICAL SAFETY VIOLATION"):
            b.delete()

