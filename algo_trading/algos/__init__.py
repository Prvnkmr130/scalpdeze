"""
algo_trading/algos/__init__.py
───────────────────────────────
Central registry of active trading algorithms in DeltaZero26.
Dynamically filters production algorithms so only algorithms whose linked broker
has at least one active account with `enable_trade=True` are registered.
"""

from __future__ import annotations
import logging
import inspect
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

_logger = logging.getLogger("algo_trading.algos")

REGISTERED_ALGOS: List[Callable] = []


def algo(func: Callable) -> Callable:
    """
    Decorator to register a trading algorithm function into the engine.
    """
    if func not in REGISTERED_ALGOS:
        REGISTERED_ALGOS.append(func)
    return func


def _get_enabled_broker_identifiers() -> Optional[Set[str]]:
    """
    Queries active broker accounts with enable_trade=True from the database.
    Returns a set of lowercase broker code / provider / account identifiers.
    Returns None if database / Django models are not initialized or query fails.
    """
    try:
        from django.apps import apps
        if not apps.ready:
            return None
        from kalai.models import Broker
        # If no broker accounts exist in the DB at all (fresh install/test), return None to load candidates
        if not Broker.objects.exists():
            return None

        active_brokers = Broker.objects.filter(enable_trade=True).select_related('broker_name', 'api_provider')
        enabled_ids: Set[str] = set()
        for b in active_brokers:
            if b.broker_name and b.broker_name.code:
                enabled_ids.add(b.broker_name.code.strip().lower())
            if b.api_provider and b.api_provider.code:
                enabled_ids.add(b.api_provider.code.strip().lower())
            if b.name:
                enabled_ids.add(b.name.strip().lower())
            if b.account_id:
                enabled_ids.add(b.account_id.strip().lower())
            if getattr(b, "is_crypto", False):
                enabled_ids.add("crypto")
            else:
                enabled_ids.add("indian")
        return enabled_ids
    except Exception as e:
        _logger.debug(f"Could not query enabled broker accounts from DB: {e}")
        return None


def load_all_algos(
    filter_by_enabled: bool = True,
    target_strategy: Optional[str] = None,
) -> List[Callable]:
    """
    Loads and returns active algorithms in DeltaZero26.
    When filter_by_enabled=True, only registers algorithms whose linked broker
    has at least one account with enable_trade=True.
    When target_strategy is provided (e.g. 'indian' or 'crypto'), restricts discovery to that domain.
    """
    from algo_trading.algos.logger import algo_logger
    algo_logger.clear_active_account()

    # Explicit import of active production trading strategies (lazy to avoid circular imports)
    from algo_trading.algos.indian_opt_trde_polars import indian_options_trading_algo_polars
    from algo_trading.algos.crypto_opt_trde_polars import crypto_options_trading_algo_polars

    # Strategy-to-Broker Identifier Mapping
    # (Matches broker_name code, api_provider code, or account keywords)
    strategy_candidates: List[Tuple[Callable, List[str]]] = [
        # Unified Multi-Account Engines (Master Production Architecture)
        (indian_options_trading_algo_polars, ["zerodha", "kotak_neo", "kotak", "upstox", "angel", "groww", "shoonya", "finvasia", "indian"]),
        (crypto_options_trading_algo_polars, ["coinswitch", "delta", "delta_exchange", "delta_india", "coindcx", "crypto", "bitcoin"]),
    ]

    if target_strategy:
        target_lower = target_strategy.strip().lower()
        strategy_candidates = [
            (fn, keys) for fn, keys in strategy_candidates
            if target_lower in fn.__name__.lower() or any(target_lower in k for k in keys)
        ]

    enabled_broker_ids = _get_enabled_broker_identifiers() if filter_by_enabled else None

    loaded_algos: List[Callable] = []
    for fn, broker_keys in strategy_candidates:
        if enabled_broker_ids is None:
            # No filtering (DB empty / disabled / filter_by_enabled=False)
            loaded_algos.append(fn)
        else:
            # Check if any associated broker keyword matches an active enabled account
            is_enabled = any(
                any(key in b_id or b_id in key for b_id in enabled_broker_ids)
                for key in broker_keys
            )
            if is_enabled:
                loaded_algos.append(fn)
            else:
                _logger.debug(
                    f"Skipping algo '{fn.__name__}': No active account with enable_trade=True found for broker domain ({', '.join(broker_keys[:2])})."
                )

    current_names = [fn.__name__ for fn in REGISTERED_ALGOS]
    new_names = [fn.__name__ for fn in loaded_algos]
    if current_names != new_names:
        _logger.info(f"Loaded {len(loaded_algos)} active trading algorithm(s): {new_names}")

    REGISTERED_ALGOS.clear()
    for fn in loaded_algos:
        if fn not in REGISTERED_ALGOS:
            REGISTERED_ALGOS.append(fn)

    return REGISTERED_ALGOS


def run_algos_sequential(
    accounts: Optional[List[str]] = None,
    filter_by_enabled: bool = True,
    algos: Optional[List[Callable]] = None,
) -> Dict[str, Any]:
    """
    Executes all registered trading algorithms in strict sequential order across active accounts.

    Args:
        accounts: Optional explicit list of account IDs. If None, queries all active accounts with enable_trade=True.
        filter_by_enabled: If True, filters REGISTERED_ALGOS based on active enabled accounts.
        algos: Optional explicit list of algorithm callables to execute.

    Returns:
        Summary dict containing execution metrics and per-algo status.
    """
    from algo_trading.algos.logger import algo_logger

    if algos is not None:
        active_algos = algos
    elif REGISTERED_ALGOS and not filter_by_enabled:
        active_algos = REGISTERED_ALGOS
    else:
        active_algos = load_all_algos(filter_by_enabled=filter_by_enabled)

    if not active_algos:
        _logger.debug("No active algorithms registered to execute.")
        return {"executed_count": 0, "errors": [], "duration_seconds": 0.0}

    # Discover target accounts
    target_accounts: List[str] = []
    if accounts is not None:
        target_accounts = list(accounts)
    else:
        try:
            from kalai.models import Broker
            target_accounts = list(
                Broker.objects.filter(enable_trade=True)
                .values_list("account_id", flat=True)
            )
        except Exception as e:
            _logger.error(f"Failed to fetch active broker accounts: {e}")
            return {"executed_count": 0, "errors": [str(e)], "duration_seconds": 0.0}

    if not target_accounts:
        _logger.debug("No active broker accounts found for sequential algo run.")
        return {"executed_count": 0, "errors": [], "duration_seconds": 0.0}

    start_ts = time.time()
    executed_count = 0
    errors: List[Dict[str, str]] = []

    for acc_id in target_accounts:
        for algo_fn in active_algos:
            fn_name = getattr(algo_fn, "__name__", str(algo_fn))
            try:
                # Synchronous sequential execution
                if inspect.iscoroutinefunction(algo_fn):
                    # Fallback for coroutine functions if any
                    import asyncio
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            future = asyncio.run_coroutine_threadsafe(algo_fn(acc_id), loop)
                            future.result()
                        else:
                            loop.run_until_complete(algo_fn(acc_id))
                    except RuntimeError:
                        asyncio.run(algo_fn(acc_id))
                else:
                    algo_fn(acc_id)
                executed_count += 1
            except Exception as e:
                err_msg = f"[{datetime.now().isoformat()}] [SEQUENTIAL ENGINE ERROR] Error executing {fn_name} for account {acc_id}: {e}"
                _logger.error(err_msg, exc_info=True)
                algo_logger.log_sync(err_msg, algo_name=fn_name, level="ERROR")
                errors.append({"account_id": acc_id, "algo": fn_name, "error": str(e)})
            finally:
                algo_logger.clear_active_account()

    # Flush all buffered logs once per sequential sweep and ensure thread context is clean
    algo_logger.flush_sync(force=True)
    algo_logger.clear_active_account()

    duration = time.time() - start_ts
    return {
        "executed_count": executed_count,
        "accounts_count": len(target_accounts),
        "algos_count": len(active_algos),
        "errors": errors,
        "duration_seconds": duration
    }
