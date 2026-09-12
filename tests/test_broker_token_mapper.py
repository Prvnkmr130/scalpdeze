# -*- coding: utf-8 -*-
"""
tests/test_broker_token_mapper.py
──────────────────────────────────
Unit tests for BrokerTokenMapper, canonical instrument keys,
index symbol translation across brokers, and active subscription filtering.
"""

import pytest
import polars as pl
from algo_trading.algos.broker_token_mapper import (
    BrokerTokenMapper,
    CanonicalInstrumentKey,
    token_mapper,
)


def test_index_symbol_normalization():
    """Verify index symbol aliases across brokers."""
    mapper = BrokerTokenMapper()
    assert mapper.canonical_index_root("NIFTY 50") == "NIFTY"
    assert mapper.canonical_index_root("Nifty 50") == "NIFTY"
    assert mapper.canonical_index_root("NIFTY BANK") == "BANKNIFTY"
    assert mapper.canonical_index_root("Nifty Bank") == "BANKNIFTY"
    assert mapper.canonical_index_root("NIFTY FIN SERVICE") == "FINNIFTY"

    # Broker-specific targets
    assert mapper.normalize_index_symbol("NIFTY 50", "kotak_neo") == "NIFTY"
    assert mapper.normalize_index_symbol("NIFTY", "zerodha") == "NIFTY 50"
    assert mapper.normalize_index_symbol("NIFTY BANK", "kotak_neo") == "BANKNIFTY"
    assert mapper.normalize_index_symbol("BANKNIFTY", "zerodha") == "NIFTY BANK"
    assert mapper.normalize_index_symbol("FINNIFTY", "kotak_neo") == "FINNIFTY"


def test_canonical_instrument_key_equality_and_hashing():
    """Verify CanonicalInstrumentKey is frozen, hashable, and supports O(1) dict lookups."""
    k1 = CanonicalInstrumentKey(
        inst_type="OPT",
        root="NIFTY",
        expiry="2026-09-24",
        strike=24000.0,
        option_type="CE",
    )
    k2 = CanonicalInstrumentKey(
        inst_type="OPT",
        root="NIFTY",
        expiry="2026-09-24",
        strike=24000.0,
        option_type="CE",
    )
    k3 = CanonicalInstrumentKey(
        inst_type="FUT",
        root="NATGASMINI",
        expiry="2026-09-25",
    )

    assert k1 == k2
    assert hash(k1) == hash(k2)
    assert k1 != k3

    test_dict = {k1: 100}
    assert test_dict[k2] == 100


def test_broker_token_mapper_bidirectional_registration():
    """Verify bidirectional registration and resolution across brokers."""
    mapper = BrokerTokenMapper()
    opt_key = CanonicalInstrumentKey(
        inst_type="OPT",
        root="NIFTY",
        expiry="2026-09-24",
        strike=24000.0,
        option_type="CE",
    )

    mapper.register_instrument("kotak_neo", 123456, "NIFTY2692424000CE", opt_key)
    mapper.register_instrument("zerodha", 654321, "NIFTY26SEP24000CE", opt_key)

    # Token lookup
    assert mapper.to_broker_token(opt_key, "kotak_neo") == 123456
    assert mapper.to_broker_token(opt_key, "zerodha") == 654321

    # Tradingsymbol lookup
    assert mapper.to_broker_symbol(opt_key, "kotak_neo") == "NIFTY2692424000CE"
    assert mapper.to_broker_symbol(opt_key, "zerodha") == "NIFTY26SEP24000CE"

    # Reverse lookup from token
    assert mapper.get_canonical_from_token(123456, "kotak_neo") == opt_key
    assert mapper.get_canonical_from_token(654321, "zerodha") == opt_key

    # Reverse lookup from tradingsymbol
    assert mapper.get_canonical_from_symbol("NIFTY2692424000CE", "kotak_neo") == opt_key
    assert mapper.get_canonical_from_symbol("NIFTY26SEP24000CE", "zerodha") == opt_key


def test_crypto_symbol_normalization_and_aliases():
    """Verify crypto root extraction and index aliases across Delta and CoinSwitch."""
    from algo_trading.algos.broker_token_mapper import clean_crypto_root

    assert clean_crypto_root("BTC/USDT") == "BTC"
    assert clean_crypto_root("BTCUSD") == "BTC"
    assert clean_crypto_root("C-BTC-54000-250926") == "BTC"
    assert clean_crypto_root("P-ETH-2500-250926") == "ETH"
    assert clean_crypto_root("BTC-25SEP26-50000-P-USDT") == "BTC"
    assert clean_crypto_root("SOL/INR") == "SOL"

    mapper = BrokerTokenMapper()
    assert mapper.canonical_index_root("BTC") == "BTC"
    assert mapper.canonical_index_root("ETH") == "ETH"
    assert mapper.normalize_index_symbol("BTC", "delta") == "BTCUSD"
    assert mapper.normalize_index_symbol("BTC", "coinswitch") == "BTCUSDT"
    assert mapper.normalize_index_symbol("ETH", "delta") == "ETHUSD"
    assert mapper.normalize_index_symbol("ETH", "coinswitch") == "ETHUSDT"


def test_crypto_cross_broker_derivative_mapping():
    """Verify mapping between Delta Exchange and CoinSwitch PRO for identical crypto options."""
    mapper = BrokerTokenMapper()

    # Canonical option key for BTC 100000 CE expiring 2026-11-27
    opt_key = CanonicalInstrumentKey(
        inst_type="OPT",
        root="BTC",
        expiry="2026-11-27",
        strike=100000.0,
        option_type="CE",
    )

    # Delta Product ID (e.g. 151159)
    mapper.register_instrument("delta", 151159, "C-BTC-100000-271126", opt_key)

    # CoinSwitch Instrument Token (e.g. 385526)
    mapper.register_instrument("coinswitch", 385526, "BTC-27NOV26-100000-C-USDT", opt_key)

    # Cross-broker lookup by key
    assert mapper.to_broker_token(opt_key, "delta") == 151159
    assert mapper.to_broker_token(opt_key, "coinswitch") == 385526

    # Reverse resolution
    assert mapper.get_canonical_from_token(151159, "delta") == opt_key
    assert mapper.get_canonical_from_token(385526, "coinswitch") == opt_key
    assert mapper.get_canonical_from_symbol("C-BTC-100000-271126", "delta") == opt_key
    assert mapper.get_canonical_from_symbol("BTC-27NOV26-100000-C-USDT", "coinswitch") == opt_key

