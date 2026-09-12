# -*- coding: utf-8 -*-
"""
algo_trading/algos/broker_token_mapper.py
──────────────────────────────────────────
Cross-Broker Instrument & Token Mapping Layer.
Normalizes tradingsymbols, tokens, and cash indices across Indian brokers
(Zerodha Kite, Kotak Neo, Upstox, Angel) and crypto brokers (Delta Exchange, CoinSwitch).

Provides O(1) in-memory bidirectional translation between Canonical Instrument Keys
and broker-specific tokens / tradingsymbols.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import polars as pl

logger = logging.getLogger("algo_trading.algos.broker_token_mapper")


@dataclass(frozen=True, slots=True)
class CanonicalInstrumentKey:
    """
    Broker-agnostic identifier for any traded asset.
    - Cash Index: ("INDEX", "NIFTY", None, None, None)
    - Equity: ("EQ", "RELIANCE", None, None, None)
    - Future: ("FUT", "NIFTY", "2026-09-24", None, None)
    - Option: ("OPT", "NIFTY", "2026-09-24", 24000.0, "CE")
    """
    inst_type: str        # 'INDEX', 'EQ', 'FUT', 'OPT'
    root: str             # 'NIFTY', 'BANKNIFTY', 'NATGASMINI', 'RELIANCE', etc.
    expiry: Optional[str] = None      # 'YYYY-MM-DD'
    strike: Optional[float] = None    # strike price (e.g. 24000.0)
    option_type: Optional[str] = None # 'CE', 'PE', or None

    def to_string(self) -> str:
        if self.inst_type == "INDEX":
            return f"INDEX:{self.root}"
        elif self.inst_type == "EQ":
            return f"EQ:{self.root}"
        elif self.inst_type == "FUT":
            return f"FUT:{self.root}:{self.expiry or ''}"
        elif self.inst_type == "OPT":
            strike_str = f"{self.strike:.2f}".rstrip("0").rstrip(".") if self.strike is not None else ""
            return f"OPT:{self.root}:{self.expiry or ''}:{strike_str}:{self.option_type or ''}"
        return f"{self.inst_type}:{self.root}"

    def __str__(self) -> str:
        return self.to_string()


@dataclass(slots=True)
class BrokerMeta:
    """Metadata mapped to a broker's native instrument representation."""
    broker: str
    token: int
    tradingsymbol: str
    exchange: str
    segment: str
    lot_size: int = 1
    tick_size: float = 0.05


# ── Known Index Name Variations Across Indian Brokers ─────────────────────────

INDEX_ALIASES: Dict[str, Dict[str, str]] = {
    "NIFTY": {
        "canonical": "NIFTY",
        "zerodha": "NIFTY 50",
        "kotak": "NIFTY",
        "kotak_neo": "NIFTY",
        "upstox": "Nifty 50",
        "angel": "Nifty 50",
    },
    "BANKNIFTY": {
        "canonical": "BANKNIFTY",
        "zerodha": "NIFTY BANK",
        "kotak": "BANKNIFTY",
        "kotak_neo": "BANKNIFTY",
        "upstox": "Nifty Bank",
        "angel": "Nifty Bank",
    },
    "FINNIFTY": {
        "canonical": "FINNIFTY",
        "zerodha": "NIFTY FIN SERVICE",
        "kotak": "FINNIFTY",
        "kotak_neo": "FINNIFTY",
        "upstox": "Nifty Fin Service",
        "angel": "Nifty Fin Service",
    },
    "MIDCPNIFTY": {
        "canonical": "MIDCPNIFTY",
        "zerodha": "NIFTY MID SELECT",
        "kotak": "MIDCPNIFTY",
        "kotak_neo": "MIDCPNIFTY",
        "upstox": "NIFTY MID SELECT",
        "angel": "NIFTY MID SELECT",
    },
    "SENSEX": {
        "canonical": "SENSEX",
        "zerodha": "SENSEX",
        "kotak": "SENSEX",
        "kotak_neo": "SENSEX",
        "upstox": "SENSEX",
        "angel": "SENSEX",
    },
    "BANKEX": {
        "canonical": "BANKEX",
        "zerodha": "BANKEX",
        "kotak": "BANKEX",
        "kotak_neo": "BANKEX",
        "upstox": "BANKEX",
        "angel": "BANKEX",
    },
    # ── Crypto Index & Perpetual Reference Aliases ──────────────────────────
    "BTC": {
        "canonical": "BTC",
        "delta": "BTCUSD",
        "delta_exchange": "BTCUSD",
        "coinswitch": "BTCUSDT",
        "coinswitch_pro": "BTCUSDT",
    },
    "ETH": {
        "canonical": "ETH",
        "delta": "ETHUSD",
        "delta_exchange": "ETHUSD",
        "coinswitch": "ETHUSDT",
        "coinswitch_pro": "ETHUSDT",
    },
    "SOL": {
        "canonical": "SOL",
        "delta": "SOLUSD",
        "delta_exchange": "SOLUSD",
        "coinswitch": "SOLUSDT",
        "coinswitch_pro": "SOLUSDT",
    },
    "XRP": {
        "canonical": "XRP",
        "delta": "XRPUSD",
        "delta_exchange": "XRPUSD",
        "coinswitch": "XRPUSDT",
        "coinswitch_pro": "XRPUSDT",
    },
    "DOGE": {
        "canonical": "DOGE",
        "delta": "DOGEUSD",
        "delta_exchange": "DOGEUSD",
        "coinswitch": "DOGEUSDT",
        "coinswitch_pro": "DOGEUSDT",
    },
    "BNB": {
        "canonical": "BNB",
        "delta": "BNBUSD",
        "delta_exchange": "BNBUSD",
        "coinswitch": "BNBUSDT",
        "coinswitch_pro": "BNBUSDT",
    },
    "AVAX": {
        "canonical": "AVAX",
        "delta": "AVAXUSD",
        "delta_exchange": "AVAXUSD",
        "coinswitch": "AVAXUSDT",
        "coinswitch_pro": "AVAXUSDT",
    },
}

def clean_crypto_root(symbol: str) -> str:
    """
    Extracts clean base crypto symbol (e.g. 'BTC', 'ETH', 'SOL')
    from spot pair, futures, or option tradingsymbols.
    Examples:
        'BTC/USDT' -> 'BTC'
        'BTCUSD' -> 'BTC'
        'BTC_USDT' -> 'BTC'
        'C-BTC-54000-250926' -> 'BTC'
        'BTC-25SEP26-50000-P-USDT' -> 'BTC'
    """
    if not symbol:
        return ""
    clean = str(symbol).strip().upper()
    if clean.startswith(("C-", "P-")):
        parts = clean.split("-")
        if len(parts) >= 2:
            return parts[1]
    if "-C-" in clean or "-P-" in clean or clean.endswith(("-C-USDT", "-P-USDT", "-CE", "-PE")):
        return clean.split("-")[0]
    for suffix in ["/USDT", "/INR", "_USDT", "USDT", "USD"]:
        if clean.endswith(suffix) and len(clean) > len(suffix):
            return clean[:-len(suffix)]
    return clean


# Reverse lookup: from any broker's index symbol string to canonical root name
_RAW_TO_CANONICAL_INDEX: Dict[str, str] = {}
for root, broker_dict in INDEX_ALIASES.items():
    _RAW_TO_CANONICAL_INDEX[root.upper().strip()] = root
    for broker, sym in broker_dict.items():
        _RAW_TO_CANONICAL_INDEX[sym.upper().strip()] = root


class BrokerTokenMapper:
    """
    Central cross-broker mapping engine.
    Maintains fast in-memory bidirectional dictionaries:
      canonical_key <-> (broker, token, tradingsymbol)
    """

    def __init__(self) -> None:
        # broker_name -> canonical_key -> BrokerMeta
        self._canon_to_broker: Dict[str, Dict[CanonicalInstrumentKey, BrokerMeta]] = {}
        # broker_name -> token -> CanonicalInstrumentKey
        self._broker_token_to_canon: Dict[str, Dict[int, CanonicalInstrumentKey]] = {}
        # broker_name -> tradingsymbol -> CanonicalInstrumentKey
        self._broker_sym_to_canon: Dict[str, Dict[str, CanonicalInstrumentKey]] = {}
        # broker_name -> root_symbol -> index_token
        self._broker_index_tokens: Dict[str, Dict[str, int]] = {}

    @staticmethod
    def canonical_index_root(symbol: str) -> str:
        """
        Normalizes any index string (e.g. 'NIFTY 50', 'NIFTY BANK', 'Nifty Bank')
        to its canonical index root ('NIFTY', 'BANKNIFTY').
        If not an index alias, returns stripped uppercase symbol.
        """
        clean = (symbol or "").upper().strip()
        return _RAW_TO_CANONICAL_INDEX.get(clean, clean)

    @classmethod
    def normalize_index_symbol(cls, raw_symbol: str, target_broker: str = "zerodha") -> str:
        """
        Translates an index symbol to the target broker's native naming convention.
        Example:
            normalize_index_symbol("NIFTY 50", "kotak_neo") -> "NIFTY"
            normalize_index_symbol("NIFTY", "zerodha")      -> "NIFTY 50"
            normalize_index_symbol("NIFTY BANK", "kotak")   -> "BANKNIFTY"
        """
        clean = (raw_symbol or "").strip()
        canon_root = cls.canonical_index_root(clean)
        b_key = (target_broker or "").lower().strip()
        if "kotak" in b_key:
            b_key = "kotak_neo"

        if canon_root in INDEX_ALIASES:
            return INDEX_ALIASES[canon_root].get(b_key, INDEX_ALIASES[canon_root].get("canonical", canon_root))
        return clean

    def register_broker_instruments(
        self,
        broker_name: str,
        instruments_df: pl.DataFrame,
    ) -> int:
        """
        Ingests a broker's normalized instruments DataFrame into the mapping registry.
        Expected schema in instruments_df:
          - instrument_token: Int64
          - tradingsymbol: Utf8
          - name: Utf8
          - expiry: Utf8
          - instrument_type: Utf8 ('CE', 'PE', 'FUT', 'EQ')
          - segment: Utf8
          - exchange: Utf8
          - strike: Float64
          - lot_size: Int64
          - tick_size: Float64
        """
        if instruments_df.is_empty():
            return 0

        b_key = broker_name.lower().strip()
        if "kotak" in b_key:
            b_key = "kotak_neo"

        if b_key not in self._canon_to_broker:
            self._canon_to_broker[b_key] = {}
            self._broker_token_to_canon[b_key] = {}
            self._broker_sym_to_canon[b_key] = {}
            self._broker_index_tokens[b_key] = {}

        canon_map = self._canon_to_broker[b_key]
        token_map = self._broker_token_to_canon[b_key]
        sym_map = self._broker_sym_to_canon[b_key]
        index_tokens = self._broker_index_tokens[b_key]

        count = 0
        records = instruments_df.to_dicts()
        for r in records:
            tkn = r.get("instrument_token")
            tsym = r.get("tradingsymbol") or ""
            if not tkn or not tsym:
                continue

            tkn_int = int(tkn)
            name = (r.get("name") or tsym).upper().strip()
            inst_type = (r.get("instrument_type") or "EQ").upper().strip()
            segment = (r.get("segment") or "").upper().strip()
            exchange = (r.get("exchange") or "").upper().strip()
            expiry = str(r.get("expiry") or "").strip() or None
            strike = float(r.get("strike") or 0.0)
            lot_size = int(r.get("lot_size") or 1)
            tick_size = float(r.get("tick_size") or 0.05)

            # Determine canonical classification
            canon_key: Optional[CanonicalInstrumentKey] = None
            is_crypto = b_key in ("delta", "delta_exchange", "coinswitch", "coinswitch_pro")
            c_root = clean_crypto_root(name or tsym) if is_crypto else name

            if inst_type in ("CE", "PE"):
                canon_key = CanonicalInstrumentKey(
                    inst_type="OPT",
                    root=c_root,
                    expiry=expiry,
                    strike=strike,
                    option_type=inst_type,
                )
            elif inst_type == "FUT":
                canon_key = CanonicalInstrumentKey(
                    inst_type="FUT",
                    root=c_root,
                    expiry=expiry or ("PERP" if is_crypto else None),
                )
            elif "INDEX" in segment or name in INDEX_ALIASES or tsym in _RAW_TO_CANONICAL_INDEX or (is_crypto and c_root in INDEX_ALIASES):
                canon_root = self.canonical_index_root(tsym if tsym in _RAW_TO_CANONICAL_INDEX else (c_root if is_crypto else name))
                canon_key = CanonicalInstrumentKey(
                    inst_type="INDEX",
                    root=canon_root,
                )
                index_tokens[canon_root] = tkn_int
            else:
                canon_key = CanonicalInstrumentKey(
                    inst_type="EQ",
                    root=c_root if is_crypto else tsym,
                )

            meta = BrokerMeta(
                broker=b_key,
                token=tkn_int,
                tradingsymbol=tsym,
                exchange=exchange,
                segment=segment,
                lot_size=lot_size,
                tick_size=tick_size,
            )

            canon_map[canon_key] = meta
            token_map[tkn_int] = canon_key
            sym_map[tsym] = canon_key
            count += 1

        logger.info("BrokerTokenMapper: Registered %d instruments for broker '%s'", count, b_key)
        return count

    def register_instrument(
        self,
        broker_name: str,
        token: int,
        tradingsymbol: str,
        canonical_key: CanonicalInstrumentKey,
        exchange: str = "",
        segment: str = "",
        lot_size: int = 1,
        tick_size: float = 0.05,
    ) -> None:
        """Register a single instrument mapping directly into in-memory dictionaries."""
        b_key = broker_name.lower().strip()
        if "kotak" in b_key:
            b_key = "kotak_neo"
        canon_map = self._canon_to_broker.setdefault(b_key, {})
        token_map = self._broker_token_to_canon.setdefault(b_key, {})
        sym_map = self._broker_sym_to_canon.setdefault(b_key, {})

        meta = BrokerMeta(
            broker=b_key,
            token=int(token),
            tradingsymbol=tradingsymbol,
            exchange=exchange,
            segment=segment,
            lot_size=lot_size,
            tick_size=tick_size,
        )
        canon_map[canonical_key] = meta
        token_map[int(token)] = canonical_key
        sym_map[tradingsymbol] = canonical_key
        if canonical_key.inst_type == "INDEX":
            self._broker_index_tokens.setdefault(b_key, {})[canonical_key.root] = int(token)

    def get_index_token(self, index_name: str, broker_name: str = "zerodha") -> int:
        """
        Direct O(1) resolution for cash index tokens across brokers.
        Accepts any index string ('NIFTY', 'NIFTY 50', 'Nifty Bank') and broker name.
        """
        b_key = broker_name.lower().strip()
        if "kotak" in b_key:
            b_key = "kotak_neo"

        canon_root = self.canonical_index_root(index_name)
        broker_indices = self._broker_index_tokens.get(b_key, {})
        if canon_root in broker_indices:
            return broker_indices[canon_root]

        # Fallback: check canonical map
        canon_key = CanonicalInstrumentKey(inst_type="INDEX", root=canon_root)
        meta = self._canon_to_broker.get(b_key, {}).get(canon_key)
        if meta:
            return meta.token

        return -1

    def to_broker_token(self, canonical_key: CanonicalInstrumentKey, broker_name: str) -> int:
        """Returns broker's numeric instrument_token for a CanonicalInstrumentKey, or -1 if unmapped."""
        b_key = broker_name.lower().strip()
        if "kotak" in b_key:
            b_key = "kotak_neo"
        meta = self._canon_to_broker.get(b_key, {}).get(canonical_key)
        return meta.token if meta else -1

    def to_broker_symbol(self, canonical_key: CanonicalInstrumentKey, broker_name: str) -> str:
        """Returns broker's native tradingsymbol for a CanonicalInstrumentKey, or empty string."""
        b_key = broker_name.lower().strip()
        if "kotak" in b_key:
            b_key = "kotak_neo"
        meta = self._canon_to_broker.get(b_key, {}).get(canonical_key)
        return meta.tradingsymbol if meta else ""

    def get_canonical_from_token(self, token: int, broker_name: str) -> Optional[CanonicalInstrumentKey]:
        """Reverse lookup: returns CanonicalInstrumentKey from broker's numeric token."""
        b_key = broker_name.lower().strip()
        if "kotak" in b_key:
            b_key = "kotak_neo"
        return self._broker_token_to_canon.get(b_key, {}).get(int(token))

    def get_canonical_from_symbol(self, tradingsymbol: str, broker_name: str) -> Optional[CanonicalInstrumentKey]:
        """Reverse lookup: returns CanonicalInstrumentKey from broker's native tradingsymbol."""
        b_key = broker_name.lower().strip()
        if "kotak" in b_key:
            b_key = "kotak_neo"
        return self._broker_sym_to_canon.get(b_key, {}).get(tradingsymbol)


# Global singleton instance
token_mapper = BrokerTokenMapper()
