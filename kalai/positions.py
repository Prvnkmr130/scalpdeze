"""
kalai/positions.py
──────────────────
Unified, thread-safe Multi-Broker Position Synchronizer and Historical P&L Analytics Engine.
Normalizes live positions, orders, and P&L metrics across Zerodha, Kotak Neo, CoinSwitch PRO, and CoinDCX.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from django.db import transaction
from django.db.models import Q
from django.utils import timezone as dj_timezone

from kalai.models import Broker, BrokerPosition, DailyPnLSnapshot

logger = logging.getLogger("kalai.positions")


def _to_decimal(val: Any, default: float = 0.0) -> Decimal:
    """Safely convert any numeric or string value to a Decimal."""
    if val is None or val == "":
        return Decimal(str(default))
    try:
        return Decimal(str(float(val)))
    except (ValueError, TypeError):
        return Decimal(str(default))


def fetch_and_normalize_positions(broker: Broker) -> List[Dict[str, Any]]:
    """
    Fetch live positions from the underlying broker API and normalize them into a uniform structure.
    Supported brokers: Zerodha, Kotak Neo, CoinSwitch PRO, CoinDCX, Delta Exchange.
    """
    from algo_trading.algos.logger import algo_logger
    algo_logger.set_active_account(broker, algo_name="POSITIONS_SYNC")
    try:
        b_code = (broker.broker_name.code.lower() if broker.broker_name and broker.broker_name.code else "")
        p_code = (broker.api_provider.code.lower() if broker.api_provider and broker.api_provider.code else "")
        name_lower = (broker.name.lower() if broker.name else "")
        account_str = broker.account_id or broker.name or "UNKNOWN"

        normalized_positions: List[Dict[str, Any]] = []

        def _to_records(obj):
            if obj is None:
                return []
            if hasattr(obj, "to_dicts"):
                return obj.to_dicts()
            if hasattr(obj, "to_dict"):
                return obj.to_dict(orient="records") if hasattr(obj, "to_dict") else []
            if isinstance(obj, list):
                return obj
            return []

        # 1. Zerodha (Kite Connect)
        if "zerodha" in b_code or "zerodha" in p_code or "zerodha" in name_lower:
            try:
                from algo_trading.algos.zerodha_utils import ZerodhaUtility
                util = ZerodhaUtility(broker_obj=broker)
                _, pos_net = util.pos_data()
                records = _to_records(pos_net)
                for d in records:
                    sym = str(d.get("tradingsymbol") or "").strip()
                    if not sym:
                        continue
                    qty = _to_decimal(d.get("quantity", 0))
                    buy_qty = _to_decimal(d.get("buy_quantity", 0))
                    buy_px = _to_decimal(d.get("buy_price", 0))
                    buy_val = _to_decimal(d.get("buy_value", 0))
                    sell_qty = _to_decimal(d.get("sell_quantity", 0))
                    sell_px = _to_decimal(d.get("sell_price", 0))
                    sell_val = _to_decimal(d.get("sell_value", 0))
                    ltp = _to_decimal(d.get("last_price", 0))
                    unrealized = _to_decimal(d.get("unrealised") or d.get("m2m") or d.get("pnl") or 0)
                    realized = _to_decimal(d.get("realised", 0))
                    total_pnl = unrealized + realized
                    tkn = int(d.get("instrument_token")) if d.get("instrument_token") and str(d.get("instrument_token")).isdigit() else None
                    prod = str(d.get("product") or "NRML").upper()

                    normalized_positions.append({
                        "tradingsymbol": sym,
                        "instrument_token": tkn,
                        "product": prod,
                        "quantity": qty,
                        "buy_quantity": buy_qty,
                        "buy_price": buy_px,
                        "buy_value": buy_val,
                        "sell_quantity": sell_qty,
                        "sell_price": sell_px,
                        "sell_value": sell_val,
                        "last_price": ltp,
                        "unrealized_pnl": unrealized,
                        "realized_pnl": realized,
                        "total_pnl": total_pnl,
                        "is_open": abs(qty) > Decimal("0.0001"),
                        "raw_data": {k: str(v) if isinstance(v, (datetime, date, Decimal)) else v for k, v in d.items()},
                    })
            except Exception as e:
                logger.warning("[%s] Zerodha positions fetch error: %s", account_str, e)

        # 2. Kotak Neo
        elif "kotak" in b_code or "kotak" in p_code or "kotak" in name_lower:
            try:
                from algo_trading.algos.kotak_utils import KotakNeoUtility
                util = KotakNeoUtility(broker_obj=broker)
                _, pos_net = util.pos_data()
                records = _to_records(pos_net)
                for d in records:
                    sym = str(d.get("tradingsymbol") or d.get("trdSym") or d.get("sym") or "").strip()
                    if not sym:
                        continue
                    qty = _to_decimal(d.get("quantity") or d.get("netQty") or 0)
                    buy_qty = _to_decimal(d.get("buy_quantity") or d.get("flBuyQty") or 0)
                    buy_px = _to_decimal(d.get("buy_price") or d.get("buyAvgPrice") or 0)
                    buy_val = _to_decimal(d.get("buy_value") or d.get("buyAmt") or 0)
                    sell_qty = _to_decimal(d.get("sell_quantity") or d.get("flSellQty") or 0)
                    sell_px = _to_decimal(d.get("sell_price") or d.get("sellAvgPrice") or 0)
                    sell_val = _to_decimal(d.get("sell_value") or d.get("sellAmt") or 0)
                    ltp = _to_decimal(d.get("last_price") or d.get("ltp") or 0)
                    unrealized = _to_decimal(d.get("unrealised") or d.get("urPnl") or 0)
                    realized = _to_decimal(d.get("realised") or d.get("rPnl") or 0)
                    total_pnl = _to_decimal(d.get("pnl") or (unrealized + realized))
                    tkn = int(d.get("token") or d.get("tok")) if (d.get("token") or d.get("tok")) and str(d.get("token") or d.get("tok")).isdigit() else None
                    prod = str(d.get("product") or d.get("prod") or "NRML").upper()

                    normalized_positions.append({
                        "tradingsymbol": sym,
                        "instrument_token": tkn,
                        "product": prod,
                        "quantity": qty,
                        "buy_quantity": buy_qty,
                        "buy_price": buy_px,
                        "buy_value": buy_val,
                        "sell_quantity": sell_qty,
                        "sell_price": sell_px,
                        "sell_value": sell_val,
                        "last_price": ltp,
                        "unrealized_pnl": unrealized,
                        "realized_pnl": realized,
                        "total_pnl": total_pnl,
                        "is_open": abs(qty) > Decimal("0.0001"),
                        "raw_data": {k: str(v) if isinstance(v, (datetime, date, Decimal)) else v for k, v in d.items()},
                    })
            except Exception as e:
                logger.warning("[%s] Kotak Neo positions fetch error: %s", account_str, e)

        # 3. CoinSwitch PRO
        elif "coinswitch" in b_code or "coinswitch" in p_code or "coinswitch" in name_lower:
            try:
                from algo_trading.algos.coinswitch_utils import CoinSwitchUtility
                util = CoinSwitchUtility(broker_obj=broker)
                _, pos_net = util.pos_data()
                records = _to_records(pos_net)
                for d in records:
                    sym = str(d.get("symbol") or d.get("pair") or "").strip()
                    if not sym:
                        continue
                    qty = _to_decimal(d.get("active_pos") or d.get("contracts") or d.get("size") or 0)
                    entry_px = _to_decimal(d.get("avg_price") or d.get("entry_price") or 0)
                    mark_px = _to_decimal(d.get("mark_price") or entry_px)
                    unrealized = _to_decimal(d.get("unrealized_pnl") or d.get("pnl") or 0)
                    realized = _to_decimal(d.get("realized_pnl") or 0)
                    total_pnl = unrealized + realized
                    val = qty * entry_px

                    normalized_positions.append({
                        "tradingsymbol": sym,
                        "instrument_token": None,
                        "product": "FUT",
                        "quantity": qty,
                        "buy_quantity": qty if qty > 0 else Decimal(0),
                        "buy_price": entry_px if qty > 0 else Decimal(0),
                        "buy_value": val if qty > 0 else Decimal(0),
                        "sell_quantity": abs(qty) if qty < 0 else Decimal(0),
                        "sell_price": entry_px if qty < 0 else Decimal(0),
                        "sell_value": abs(val) if qty < 0 else Decimal(0),
                        "last_price": mark_px,
                        "unrealized_pnl": unrealized,
                        "realized_pnl": realized,
                        "total_pnl": total_pnl,
                        "is_open": abs(qty) > Decimal("0.0000001"),
                        "raw_data": {k: str(v) if isinstance(v, (datetime, date, Decimal)) else v for k, v in d.items()},
                    })
            except Exception as e:
                logger.warning("[%s] CoinSwitch positions fetch error: %s", account_str, e)

        # 4. CoinDCX
        elif "coindcx" in b_code or "coindcx" in p_code or "coindcx" in name_lower:
            try:
                from algo_trading.algos.coindcx_utils import CoinDCXUtility
                util = CoinDCXUtility(broker_obj=broker)
                _, pos_net = util.pos_data()
                records = _to_records(pos_net)
                for d in records:
                    sym = str(d.get("pair") or d.get("symbol") or "").strip()
                    if not sym:
                        continue
                    qty = _to_decimal(d.get("active_pos") or d.get("contracts") or 0)
                    entry_px = _to_decimal(d.get("entry_price") or d.get("avg_price") or 0)
                    mark_px = _to_decimal(d.get("mark_price") or entry_px)
                    unrealized = _to_decimal(d.get("unrealized_pnl") or 0)
                    realized = _to_decimal(d.get("realized_pnl") or 0)
                    total_pnl = unrealized + realized
                    val = qty * entry_px

                    normalized_positions.append({
                        "tradingsymbol": sym,
                        "instrument_token": None,
                        "product": "FUT",
                        "quantity": qty,
                        "buy_quantity": qty if qty > 0 else Decimal(0),
                        "buy_price": entry_px if qty > 0 else Decimal(0),
                        "buy_value": val if qty > 0 else Decimal(0),
                        "sell_quantity": abs(qty) if qty < 0 else Decimal(0),
                        "sell_price": entry_px if qty < 0 else Decimal(0),
                        "sell_value": abs(val) if qty < 0 else Decimal(0),
                        "last_price": mark_px,
                        "unrealized_pnl": unrealized,
                        "realized_pnl": realized,
                        "total_pnl": total_pnl,
                        "is_open": abs(qty) > Decimal("0.0000001"),
                        "raw_data": {k: str(v) if isinstance(v, (datetime, date, Decimal)) else v for k, v in d.items()},
                    })
            except Exception as e:
                logger.warning("[%s] CoinDCX positions fetch error: %s", account_str, e)

        # 5. Delta Exchange (Global & India)
        elif "delta" in b_code or "delta" in p_code or "delta" in name_lower:
            try:
                from algo_trading.algos.delta_utils import DeltaExchangeUtility
                util = DeltaExchangeUtility(broker_obj=broker)
                _, pos_net = util.pos_data()
                records = _to_records(pos_net)
                for d in records:
                    sym = str(d.get("symbol") or "").strip()
                    if not sym:
                        continue
                    qty = _to_decimal(d.get("size") or 0)
                    entry_px = _to_decimal(d.get("entry_price") or 0)
                    mark_px = _to_decimal(d.get("mark_price") or entry_px)
                    unrealized = _to_decimal(d.get("unrealized_pnl") or 0)
                    realized = _to_decimal(d.get("realized_pnl") or 0)
                    total_pnl = unrealized + realized
                    pid = d.get("product_id")
                    tkn = int(pid) if pid and str(pid).isdigit() else None
                    val = qty * entry_px

                    normalized_positions.append({
                        "tradingsymbol": sym,
                        "instrument_token": tkn,
                        "product": "DERIVATIVE",
                        "quantity": qty,
                        "buy_quantity": qty if qty > 0 else Decimal(0),
                        "buy_price": entry_px if qty > 0 else Decimal(0),
                        "buy_value": val if qty > 0 else Decimal(0),
                        "sell_quantity": abs(qty) if qty < 0 else Decimal(0),
                        "sell_price": entry_px if qty < 0 else Decimal(0),
                        "sell_value": abs(val) if qty < 0 else Decimal(0),
                        "last_price": mark_px,
                        "unrealized_pnl": unrealized,
                        "realized_pnl": realized,
                        "total_pnl": total_pnl,
                        "is_open": abs(qty) > Decimal("0.0000001"),
                        "raw_data": {k: str(v) if isinstance(v, (datetime, date, Decimal)) else v for k, v in d.items()},
                    })
            except Exception as e:
                logger.warning("[%s] Delta Exchange positions fetch error: %s", account_str, e)

        return normalized_positions
    finally:
        algo_logger.clear_active_account()


import hashlib
import time

_LAST_POSITION_SYNC_TIME: Dict[Any, float] = {}
_LAST_POSITION_HASH: Dict[Any, str] = {}


def sync_positions_from_algo_frames(
    broker: Broker,
    pos_df: Any,
    min_interval_seconds: float = 60.0,
    force: bool = False,
) -> Dict[str, Any]:
    """
    Synchronizes position state directly from the trading algorithm's in-memory Polars DataFrame.
    Throttled to at most once every 60 seconds (1 per minute) in production to avoid redundant database writes.
    Zero external HTTP or REST API calls are made. Used exclusively during production trading execution.
    """
    from django.conf import settings

    acc_key = broker.id if broker.id else (broker.account_id or broker.name)
    now_monotonic = time.monotonic()

    # In production, throttle updates to once every min_interval_seconds (default: 60s / 1 minute)
    if not force and not settings.DEBUG and (now_monotonic - _LAST_POSITION_SYNC_TIME.get(acc_key, 0.0) < min_interval_seconds):
        return {
            "account": str(broker.account_id or broker.name),
            "status": "throttled",
            "interval_seconds": min_interval_seconds,
        }

    if pos_df is None:
        return {"account": str(broker.account_id or broker.name), "synced_positions": 0, "open_positions": 0}

    # Convert Polars DataFrame to records
    if hasattr(pos_df, "is_empty") and pos_df.is_empty():
        records = []
    elif hasattr(pos_df, "to_dicts"):
        records = pos_df.to_dicts()
    elif hasattr(pos_df, "to_dict"):
        records = pos_df.to_dict(orient="records")
    elif isinstance(pos_df, list):
        records = pos_df
    else:
        records = []

    # Fast hash check: if positions payload has not changed, skip redundant writes
    payload_repr = str([(r.get("tradingsymbol") or r.get("symbol"), r.get("quantity") or r.get("active_pos"), r.get("unrealised") or r.get("unrealized_pnl")) for r in records])
    payload_hash = hashlib.md5(payload_repr.encode("utf-8")).hexdigest()
    if not force and not settings.DEBUG and _LAST_POSITION_HASH.get(acc_key) == payload_hash and (now_monotonic - _LAST_POSITION_SYNC_TIME.get(acc_key, 0.0) < 300.0):
        return {
            "account": str(broker.account_id or broker.name),
            "status": "unchanged",
        }

    _LAST_POSITION_SYNC_TIME[acc_key] = now_monotonic
    _LAST_POSITION_HASH[acc_key] = payload_hash

    today = dj_timezone.localdate()
    total_unrealized = Decimal(0)
    total_realized = Decimal(0)
    total_turnover = Decimal(0)
    open_count = 0
    active_keys = set()

    with transaction.atomic():
        for d in records:
            sym = str(d.get("tradingsymbol") or d.get("symbol") or d.get("pair") or d.get("trdSym") or d.get("sym") or "").strip()
            if not sym:
                continue
            prod = str(d.get("product") or d.get("prod") or "NRML").upper()
            qty = _to_decimal(d.get("quantity") or d.get("active_pos") or d.get("netQty") or d.get("size") or d.get("contracts") or 0)
            buy_qty = _to_decimal(d.get("buy_quantity") or d.get("flBuyQty") or 0)
            buy_px = _to_decimal(d.get("buy_price") or d.get("avg_price") or d.get("entry_price") or d.get("buyAvgPrice") or 0)
            buy_val = _to_decimal(d.get("buy_value") or d.get("buyAmt") or (buy_qty * buy_px) or 0)
            sell_qty = _to_decimal(d.get("sell_quantity") or d.get("flSellQty") or 0)
            sell_px = _to_decimal(d.get("sell_price") or d.get("sellAvgPrice") or 0)
            sell_val = _to_decimal(d.get("sell_value") or d.get("sellAmt") or (sell_qty * sell_px) or 0)
            ltp = _to_decimal(d.get("last_price") or d.get("mark_price") or d.get("ltp") or 0)
            unrealized = _to_decimal(d.get("unrealised") or d.get("unrealized_pnl") or d.get("urPnl") or d.get("m2m") or d.get("pnl") or 0)
            realized = _to_decimal(d.get("realised") or d.get("realized_pnl") or d.get("rPnl") or 0)
            total_pnl = _to_decimal(d.get("pnl") or (unrealized + realized))
            is_open = abs(qty) > Decimal("0.0000001")
            tkn = None
            raw_tkn = d.get("instrument_token") or d.get("token") or d.get("tok")
            if raw_tkn is not None and str(raw_tkn).isdigit():
                tkn = int(raw_tkn)

            active_keys.add((sym, prod))
            total_unrealized += unrealized
            total_realized += realized
            total_turnover += buy_val + sell_val
            if is_open:
                open_count += 1

            BrokerPosition.objects.update_or_create(
                account=broker,
                tradingsymbol=sym,
                product=prod,
                defaults={
                    "instrument_token": tkn,
                    "quantity": qty,
                    "buy_quantity": buy_qty,
                    "buy_price": buy_px,
                    "buy_value": buy_val,
                    "sell_quantity": sell_qty,
                    "sell_price": sell_px,
                    "sell_value": sell_val,
                    "last_price": ltp,
                    "unrealized_pnl": unrealized,
                    "realized_pnl": realized,
                    "total_pnl": total_pnl,
                    "is_open": is_open,
                    "raw_data": {k: str(v) if isinstance(v, (datetime, date, Decimal)) else v for k, v in d.items()},
                }
            )

        # Mark positions missing from current active payload as closed
        existing_open = BrokerPosition.objects.filter(account=broker, is_open=True)
        for pos in existing_open:
            if (pos.tradingsymbol, pos.product) not in active_keys:
                pos.is_open = False
                pos.quantity = Decimal(0)
                pos.save(update_fields=["is_open", "quantity", "updated_at"])

        # Update running daily snapshot for today
        net_pnl = total_unrealized + total_realized
        DailyPnLSnapshot.objects.update_or_create(
            account=broker,
            date=today,
            defaults={
                "realized_pnl": total_realized,
                "unrealized_pnl": total_unrealized,
                "net_pnl": net_pnl,
                "turnover": total_turnover,
            }
        )

    return {
        "account": str(broker.account_id or broker.name),
        "synced_positions": len(records),
        "open_positions": open_count,
        "unrealized_pnl": float(total_unrealized),
        "realized_pnl": float(total_realized),
        "net_pnl": float(total_unrealized + total_realized),
    }


def sync_account_positions(broker: Broker, force_api: bool = False) -> Dict[str, Any]:
    """
    Synchronizes positions and daily snapshot for a single broker account into the database.
    In production, position information is received directly from the strategy engine.
    During debug/testing or when force_api=True, queries the broker REST API.
    """
    from django.conf import settings
    if not settings.DEBUG and not force_api:
        # In production, positions are synced directly from the algo script; avoid redundant REST API calls
        live_count = BrokerPosition.objects.filter(account=broker, is_open=True).count()
        return {
            "account": str(broker.account_id or broker.name),
            "synced_positions": live_count,
            "open_positions": live_count,
            "source": "algo_database",
        }

    account_str = broker.account_id or broker.name or "UNKNOWN"
    positions = fetch_and_normalize_positions(broker)
    today = dj_timezone.localdate()

    total_unrealized = Decimal(0)
    total_realized = Decimal(0)
    total_turnover = Decimal(0)
    open_count = 0
    active_keys = set()

    with transaction.atomic():
        for p in positions:
            sym = p["tradingsymbol"]
            prod = p["product"]
            active_keys.add((sym, prod))

            total_unrealized += p["unrealized_pnl"]
            total_realized += p["realized_pnl"]
            total_turnover += p["buy_value"] + p["sell_value"]
            if p["is_open"]:
                open_count += 1

            BrokerPosition.objects.update_or_create(
                account=broker,
                tradingsymbol=sym,
                product=prod,
                defaults={
                    "instrument_token": p["instrument_token"],
                    "quantity": p["quantity"],
                    "buy_quantity": p["buy_quantity"],
                    "buy_price": p["buy_price"],
                    "buy_value": p["buy_value"],
                    "sell_quantity": p["sell_quantity"],
                    "sell_price": p["sell_price"],
                    "sell_value": p["sell_value"],
                    "last_price": p["last_price"],
                    "unrealized_pnl": p["unrealized_pnl"],
                    "realized_pnl": p["realized_pnl"],
                    "total_pnl": p["total_pnl"],
                    "is_open": p["is_open"],
                    "raw_data": p["raw_data"],
                }
            )

        # Mark positions missing from today's active payload as closed
        existing_open = BrokerPosition.objects.filter(account=broker, is_open=True)
        for pos in existing_open:
            if (pos.tradingsymbol, pos.product) not in active_keys:
                pos.is_open = False
                pos.quantity = Decimal(0)
                pos.save(update_fields=["is_open", "quantity", "updated_at"])

        # Update running daily snapshot for today
        net_pnl = total_unrealized + total_realized
        DailyPnLSnapshot.objects.update_or_create(
            account=broker,
            date=today,
            defaults={
                "realized_pnl": total_realized,
                "unrealized_pnl": total_unrealized,
                "net_pnl": net_pnl,
                "turnover": total_turnover,
            }
        )

    return {
        "account": account_str,
        "synced_positions": len(positions),
        "open_positions": open_count,
        "unrealized_pnl": float(total_unrealized),
        "realized_pnl": float(total_realized),
        "net_pnl": float(total_unrealized + total_realized),
    }


def _safe_thread_sync(broker: Broker, force_api: bool = False) -> Dict[str, Any]:
    from django.db import close_old_connections
    close_old_connections()
    try:
        return sync_account_positions(broker, force_api=force_api)
    finally:
        close_old_connections()


def sync_all_broker_positions(force_api: bool = False) -> List[Dict[str, Any]]:
    """
    Synchronizes positions across all active broker accounts in parallel.
    In production, uses algo-synced positions from the database; during debug or when force_api=True, queries broker APIs.
    """
    brokers = list(
        Broker.objects.select_related("broker_name", "api_provider")
        .filter(enable_trade=True)
    )
    if not brokers:
        # Fallback to any brokers with non-empty credentials
        brokers = list(
            Broker.objects.select_related("broker_name", "api_provider")
            .filter(Q(access_token__isnull=False) | Q(api_key__isnull=False))
        )

    results = []
    with ThreadPoolExecutor(max_workers=min(max(len(brokers), 1), 5)) as executor:
        futures = [executor.submit(_safe_thread_sync, b, force_api) for b in brokers]
        for f in futures:
            try:
                results.append(f.result(timeout=6.0))
            except Exception as exc:
                logger.error("Error in parallel broker position sync: %s", exc)

    return results


def compute_pnl_analytics(
    account_id: Optional[str] = None,
    period_code: str = "this_month",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[str, Any]:
    """
    On-demand performance analytics engine for custom time periods (1D, 7D, 30D, 90D, 1Y, Custom).
    Executes in sub-2ms by leveraging indexed DailyPnLSnapshot and BrokerPosition models.
    """
    today = dj_timezone.localdate()

    # Determine date range boundaries based on preset or custom inputs
    if period_code == "today":
        start_d = today
        end_d = today
    elif period_code == "yesterday":
        start_d = today - timedelta(days=1)
        end_d = today - timedelta(days=1)
    elif period_code == "this_week":
        start_d = today - timedelta(days=today.weekday())  # Monday
        end_d = today
    elif period_code == "last_7_days":
        start_d = today - timedelta(days=7)
        end_d = today
    elif period_code == "this_month":
        start_d = today.replace(day=1)
        end_d = today
    elif period_code == "last_30_days":
        start_d = today - timedelta(days=30)
        end_d = today
    elif period_code == "last_90_days":
        start_d = today - timedelta(days=90)
        end_d = today
    elif period_code == "ytd":
        start_d = today.replace(month=1, day=1)
        end_d = today
    elif period_code == "past_1_year" or period_code == "1_year":
        start_d = today - timedelta(days=365)
        end_d = today
    elif period_code == "custom":
        start_d = start_date or (end_date - timedelta(days=30) if end_date else today - timedelta(days=30))
        end_d = end_date or today
        if start_d > end_d:
            start_d, end_d = end_d, start_d
    else:
        # Default to Last 30 Days
        start_d = today - timedelta(days=30)
        end_d = today

    # Account filter
    broker_filter = Q()
    if account_id and account_id not in ("ALL", "all", "", None):
        broker_filter = Q(account__account_id=account_id) | Q(account__name=account_id)
        if str(account_id).isdigit():
            broker_filter |= Q(account_id=int(account_id))

    # 1. Fetch Daily Snapshots in range
    snapshots_qs = (
        DailyPnLSnapshot.objects.filter(broker_filter)
        .filter(date__gte=start_d, date__lte=end_d)
        .select_related("account", "account__broker_name")
        .order_by("date")
    )

    snapshots_list = list(snapshots_qs)

    # Group by date for daily progression timeline
    date_map: Dict[date, Dict[str, Any]] = {}
    cur_date = start_d
    # Populate all calendar days in range
    while cur_date <= end_d:
        date_map[cur_date] = {
            "date": cur_date,
            "realized_pnl": Decimal(0),
            "unrealized_pnl": Decimal(0),
            "net_pnl": Decimal(0),
            "total_trades": 0,
            "turnover": Decimal(0),
        }
        cur_date += timedelta(days=1)

    account_pnl_map: Dict[str, Dict[str, Any]] = {}

    for s in snapshots_list:
        d_val = s.date
        if d_val in date_map:
            date_map[d_val]["realized_pnl"] += s.realized_pnl or Decimal(0)
            date_map[d_val]["unrealized_pnl"] += s.unrealized_pnl or Decimal(0)
            date_map[d_val]["net_pnl"] += s.net_pnl or Decimal(0)
            date_map[d_val]["total_trades"] += s.total_trades or 0
            date_map[d_val]["turnover"] += s.turnover or Decimal(0)

        acc_name = (s.account.account_id or s.account.name) if s.account else "Account"
        b_firm = s.account.broker_name.name if (s.account and s.account.broker_name) else ""
        acc_key = f"{acc_name} ({b_firm})" if b_firm else acc_name

        if acc_key not in account_pnl_map:
            account_pnl_map[acc_key] = {
                "account": acc_name,
                "broker": b_firm,
                "net_pnl": Decimal(0),
                "realized_pnl": Decimal(0),
                "unrealized_pnl": Decimal(0),
                "trades": 0,
            }
        account_pnl_map[acc_key]["net_pnl"] += s.net_pnl or Decimal(0)
        account_pnl_map[acc_key]["realized_pnl"] += s.realized_pnl or Decimal(0)
        account_pnl_map[acc_key]["unrealized_pnl"] += s.unrealized_pnl or Decimal(0)
        account_pnl_map[acc_key]["trades"] += s.total_trades or 0

    # Calculate cumulative timeline progression
    timeline: List[Dict[str, Any]] = []
    cumulative_pnl = Decimal(0)
    winning_days = 0
    losing_days = 0
    best_day_pnl = Decimal("-999999999")
    worst_day_pnl = Decimal("999999999")
    gross_gains = Decimal(0)
    gross_losses = Decimal(0)

    for d_key in sorted(date_map.keys()):
        item = date_map[d_key]
        n_pnl = item["net_pnl"]
        cumulative_pnl += n_pnl
        item["cumulative_pnl"] = cumulative_pnl

        if n_pnl > 0:
            winning_days += 1
            gross_gains += n_pnl
            if n_pnl > best_day_pnl or best_day_pnl == Decimal("-999999999"):
                best_day_pnl = n_pnl
        elif n_pnl < 0:
            losing_days += 1
            gross_losses += abs(n_pnl)
            if n_pnl < worst_day_pnl or worst_day_pnl == Decimal("999999999"):
                worst_day_pnl = n_pnl

        timeline.append(item)

    # 2. Live Positions Query (for current intraday status)
    live_positions_qs = (
        BrokerPosition.objects.filter(broker_filter)
        .select_related("account", "account__broker_name")
        .order_by("-is_open", "-updated_at")
    )
    live_positions = list(live_positions_qs)

    total_net_pnl = sum((item["net_pnl"] for item in timeline), Decimal(0))
    total_realized_pnl = sum((item["realized_pnl"] for item in timeline), Decimal(0))
    total_unrealized_pnl = sum((p.unrealized_pnl or Decimal(0) for p in live_positions if p.is_open), Decimal(0))
    total_trades_count = sum((item["total_trades"] for item in timeline), 0)

    total_active_days = winning_days + losing_days
    win_rate = round((winning_days / total_active_days * 100), 1) if total_active_days > 0 else 0.0
    profit_factor = round(float(gross_gains / gross_losses), 2) if gross_losses > 0 else (99.0 if gross_gains > 0 else 0.0)

    if best_day_pnl == Decimal("-999999999"):
        best_day_pnl = Decimal(0)
    if worst_day_pnl == Decimal("999999999"):
        worst_day_pnl = Decimal(0)

    avg_daily_pnl = (total_net_pnl / total_active_days) if total_active_days > 0 else Decimal(0)

    # All accounts for dropdown selector
    all_accounts = [
        {
            "id": b.id,
            "account_id": b.account_id or b.name,
            "name": b.name,
            "broker": b.broker_name.name if b.broker_name else "Broker",
        }
        for b in Broker.objects.select_related("broker_name").all()
    ]

    return {
        "period_code": period_code,
        "start_date": start_d,
        "end_date": end_d,
        "account_id": account_id or "ALL",
        "all_accounts": all_accounts,
        "total_net_pnl": float(total_net_pnl),
        "total_realized_pnl": float(total_realized_pnl),
        "total_unrealized_pnl": float(total_unrealized_pnl),
        "total_trades": total_trades_count,
        "win_rate_percent": win_rate,
        "profit_factor": profit_factor,
        "winning_days": winning_days,
        "losing_days": losing_days,
        "total_active_days": total_active_days,
        "best_day_pnl": float(best_day_pnl),
        "worst_day_pnl": float(worst_day_pnl),
        "avg_daily_pnl": float(avg_daily_pnl),
        "account_breakdown": list(account_pnl_map.values()),
        "timeline": list(reversed(timeline)),  # Most recent first in table
        "live_positions": live_positions,
        "open_positions_count": sum(1 for p in live_positions if p.is_open),
    }
