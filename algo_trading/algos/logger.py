"""
algo_trading/algos/logger.py
────────────────────────────
Unified, thread-safe, and async-safe buffered logging infrastructure for all trading algorithms.
Maintains in-memory log batching and periodically flushes to the kalai_algolog database table
based on engine_config (LOG_BATCH_SIZE_LIMIT and LOG_FLUSH_INTERVAL).

Usage:
    # Synchronous multi-threaded strategy engine:
    from algo_trading.algos.logger import algo_logger
    algo_logger.set_active_account(broker_obj, algo_name="zerodha_opt_trde_polars")
    algo_logger.log_sync("Signal generated", tag="TRADE", level="INFO")
    algo_logger.flush_sync(force=False)

    # Asynchronous worker engine:
    await algo_logger.add_log(account_id, "Worker iteration completed", tablename="zerodha_opt_trde_polars")
    await algo_logger.flush_if_needed(account_id, broker, tablename="zerodha_opt_trde_polars", force=True)
"""

from __future__ import annotations

import asyncio
import threading
import time
import os
import sys
from datetime import datetime
import logging
from typing import Any, Dict, List, Optional

from asgiref.sync import sync_to_async
from algo_trading.brokers.config import engine_config

logger = logging.getLogger("algo_trading.algos.logger")


class ActiveAlgoContext:
    """Thread-safe context tracking active account and algorithm name."""
    def __init__(self):
        self._local = threading.local()

    @property
    def active_account(self) -> Any:
        return getattr(self._local, "active_account", None)

    @active_account.setter
    def active_account(self, value: Any):
        self._local.active_account = value

    @property
    def algo_name(self) -> Optional[str]:
        return getattr(self._local, "algo_name", None)

    @algo_name.setter
    def algo_name(self, value: Optional[str]):
        self._local.algo_name = value


class SharedAlgoLogger:
    """
    Unified logging utility that buffers logs in-memory and batch-inserts into
    the AlgoLog database table periodically, supporting both synchronous and asynchronous callers.
    """

    def __init__(self):
        self.context = ActiveAlgoContext()

        # Synchronous in-memory log buffer
        self._sync_log_buffer: List[Any] = []
        self._sync_buffer_lock = threading.Lock()
        self._last_sync_flush = time.time()

        # Asynchronous in-memory log buffer: maps account_id -> { tablename -> list of logs }
        self._async_logs: Dict[str, Dict[str, List[str]]] = {}
        self._async_last_flush: Dict[str, Dict[str, float]] = {}
        self._async_buffer_lock = asyncio.Lock()
        self._async_db_locks: Dict[str, asyncio.Lock] = {}

    # ── Context Management ──────────────────────────────────────────
    def set_active_account(self, broker_obj: Any, algo_name: Optional[str] = None):
        """Set the active broker account and optional algorithm name for the current thread."""
        self.context.active_account = broker_obj
        if algo_name:
            self.context.algo_name = algo_name

    def get_active_account(self) -> Any:
        return self.context.active_account

    def clear_active_account(self):
        """Clear active broker account and algo name for the current thread."""
        self.context.active_account = None
        self.context.algo_name = None

    @staticmethod
    def _get_caller_location(skip_frames: int = 2) -> str:
        """Inspect caller stack to extract [filename:line in func_name()]."""
        try:
            frame = sys._getframe(skip_frames)
            while frame and (
                frame.f_code.co_name in (
                    'add_log', 'log_sync', '_db_insert_single_log',
                    '_get_caller_location', 'info', 'warning', 'error', 'debug', '_handle_log'
                )
                or 'logger.py' in frame.f_code.co_filename
            ):
                frame = frame.f_back
            if frame:
                filename = os.path.basename(frame.f_code.co_filename)
                func_name = frame.f_code.co_name
                lineno = frame.f_lineno
                return f"[{filename}:{lineno} in {func_name}()]"
        except Exception:
            pass
        return ""

    # ── Database Write Helper ─────────────────────────────────────────
    def _execute_bulk_create(self, batch: List[Any]):
        """Direct synchronous bulk_create on the AlgoLog table."""
        if not batch:
            return
        try:
            from kalai.models import AlgoLog, Broker
            valid_broker_ids = set(Broker.objects.values_list("id", flat=True))
            for item in batch:
                acc_id = getattr(item, "account_id", None)
                if acc_id and acc_id not in valid_broker_ids:
                    item.account = None
                    item.account_id = None
            AlgoLog.objects.bulk_create(batch, batch_size=200, ignore_conflicts=True)
        except Exception as e:
            logger.debug(f"Handled bulk_create AlgoLog entries exception: {e}")

    def _safe_bulk_create(self, batch: List[Any]):
        """Safely bulk-insert AlgoLog objects synchronously."""
        if not batch:
            return
        try:
            self._execute_bulk_create(batch)
        except Exception as e:
            logger.debug(f"Failed to safe bulk_create AlgoLog entries: {e}")

    # ── Synchronous Logging API (For TradeAlgo & Multi-threaded Engines) ──
    def log_sync(
        self,
        message: str,
        tag: str = "ALGO",
        level: str = "INFO",
        broker_obj: Any = None,
        algo_name: Optional[str] = None
    ):
        """Buffer a log entry synchronously for batch insertion into the AlgoLog table."""
        try:
            from django.utils import timezone
            from kalai.models import AlgoLog, Broker

            target_broker = broker_obj or self.context.active_account
            if target_broker is None and message:
                import re
                from django.db.models import Q
                m = re.search(r"(?:account|account_id|acc_id|broker)\s*(?:=|:|\s)\s*['\"]?([a-zA-Z0-9_-]+)['\"]?", message, re.IGNORECASE)
                if not m:
                    m = re.search(r"\[([a-zA-Z0-9_-]{4,30})\]", message)
                if m:
                    cand_id = m.group(1).strip()
                    if cand_id.upper() not in (
                        "ORDER", "TRADE", "STRIKE", "APP", "GENERAL", "BROKER_API",
                        "SYSTEM", "INFO", "ERROR", "WARNING", "EXCEPTION", "DEBUG", "ALGO", "CRITICAL"
                    ):
                        try:
                            found = Broker.objects.filter(Q(account_id__iexact=cand_id) | Q(name__iexact=cand_id)).first()
                            if found:
                                target_broker = found
                        except Exception:
                            pass

            target_algo = algo_name or self.context.algo_name
            if not target_algo:
                # Infer from message or caller location
                if "indian_opt_trde" in message or "indian_candle" in message or "indian_strike" in message:
                    target_algo = "indian_opt_trde_polars"
                elif "crypto_opt_trde" in message or "crypto_options_trading" in message or "crypto_master" in message:
                    target_algo = "crypto_opt_trde_polars"
                elif "kotak_utils" in message or "zerodha_utils" in message or "upstox_utils" in message:
                    target_algo = "indian_opt_trde_polars"
                elif "coinswitch_utils" in message or "delta_utils" in message or "coindcx_utils" in message:
                    target_algo = "crypto_opt_trde_polars"
                else:
                    caller_str = self._get_caller_location()
                    if "load_all_algos" in caller_str or "__init__.py" in caller_str:
                        target_algo = "SYSTEM"
                    elif "indian" in caller_str or "kotak" in caller_str or "zerodha" in caller_str:
                        target_algo = "indian_opt_trde_polars"
                    elif "crypto" in caller_str or "coinswitch" in caller_str or "delta" in caller_str:
                        target_algo = "crypto_opt_trde_polars"
                    elif target_broker:
                        b_code = str(getattr(getattr(target_broker, "broker_name", None), "code", "") or "").lower()
                        b_name = str(getattr(target_broker, "name", "") or "").lower()
                        p_code = str(getattr(getattr(target_broker, "api_provider", None), "code", "") or "").lower()
                        if any(k in b_code or k in b_name or k in p_code for k in ("coinswitch", "delta", "crypto", "coindcx")):
                            target_algo = "crypto_opt_trde_polars"
                        else:
                            target_algo = "indian_opt_trde_polars"
                    else:
                        target_algo = "SYSTEM"

            # Strict guard against misattributing crypto utilities/brokers to Indian strategies or vice-versa
            if any(k in message for k in ("delta_utils", "coinswitch_utils", "coindcx_utils", "crypto_opt_trde")):
                if target_algo in ("indian_opt_trde_polars", "SYSTEM", None, ""):
                    target_algo = "crypto_opt_trde_polars"
            elif any(k in message for k in ("kotak_utils", "zerodha_utils", "indian_opt_trde")):
                if target_algo in ("crypto_opt_trde_polars", "SYSTEM", None, ""):
                    target_algo = "indian_opt_trde_polars"

            if target_broker:
                b_code = str(getattr(getattr(target_broker, "broker_name", None), "code", "") or "").lower()
                b_name = str(getattr(target_broker, "name", "") or "").lower()
                p_code = str(getattr(getattr(target_broker, "api_provider", None), "code", "") or "").lower()
                is_crypto = any(k in b_code or k in b_name or k in p_code for k in ("coinswitch", "delta", "crypto", "coindcx"))
                if is_crypto and target_algo in ("indian_opt_trde_polars", "SYSTEM", None, ""):
                    target_algo = "crypto_opt_trde_polars"
                elif not is_crypto and target_algo in ("crypto_opt_trde_polars", "SYSTEM", None, ""):
                    target_algo = "indian_opt_trde_polars"
            now_ts = timezone.now()

            # Prevent double formatting if message is already structured
            if not message.startswith("[20") and not (message.startswith("[") and "]" in message[:20]):
                caller_str = self._get_caller_location()
                caller_prefix = f" {caller_str}" if caller_str and caller_str not in message else ""
                iso_msg = f"[{datetime.now().isoformat()}]{caller_prefix} {message}"
            else:
                iso_msg = message

            log_obj = AlgoLog(
                account=target_broker,
                algo_name=target_algo,
                tag=tag,
                level=level.upper(),
                message=iso_msg,
                timestamp=now_ts
            )

            with self._sync_buffer_lock:
                self._sync_log_buffer.append(log_obj)

            self.flush_sync(force=False)
        except Exception:
            pass

    def flush_sync(self, force: bool = False):
        """Batch-flush in-memory buffered logs into AlgoLog table safely."""
        with self._sync_buffer_lock:
            if not self._sync_log_buffer:
                return
            now = time.time()
            if not force and len(self._sync_log_buffer) < engine_config.LOG_BATCH_SIZE_LIMIT and (now - self._last_sync_flush) < engine_config.LOG_FLUSH_INTERVAL:
                return

            batch = list(self._sync_log_buffer)
            self._sync_log_buffer.clear()
            self._last_sync_flush = now

        if batch:
            self._safe_bulk_create(batch)

    # ── Asynchronous Logging API (For Async Engine Workers) ───────────
    async def add_log(
        self,
        account_id: str,
        message: str,
        tablename: str = "logs",
        tag: str = "ALGO",
        level: str = "INFO"
    ):
        """Buffer a log message asynchronously for a specific account."""
        caller_str = self._get_caller_location()
        caller_prefix = f" {caller_str}" if caller_str and caller_str not in message else ""
        log_entry = f"[{datetime.now().isoformat()}]{caller_prefix} {message}"

        async with self._async_buffer_lock:
            if account_id not in self._async_logs:
                self._async_logs[account_id] = {}
                self._async_last_flush[account_id] = {}
                self._async_db_locks[account_id] = asyncio.Lock()

            if tablename not in self._async_logs[account_id]:
                self._async_logs[account_id][tablename] = []
                self._async_last_flush[account_id][tablename] = time.time()

            self._async_logs[account_id][tablename].append(log_entry)

        logger.info(log_entry)

    @sync_to_async
    def _write_async_batch_to_db(self, broker: Any, tablename: str, logs_to_write: List[str]):
        """Synchronous database call to bulk-insert logs into the AlgoLog table."""
        try:
            from django.utils import timezone
            from kalai.models import AlgoLog
            now_ts = timezone.now()
            entries = [
                AlgoLog(
                    account=broker,
                    algo_name=tablename,
                    tag="ALGO",
                    level="INFO",
                    message=log_entry,
                    timestamp=now_ts
                )
                for log_entry in logs_to_write
            ]
            AlgoLog.objects.bulk_create(entries, batch_size=200, ignore_conflicts=True)
        except Exception as e:
            logger.error(f"Failed to write logs to AlgoLog table for {broker}: {e}")

    async def flush_if_needed(
        self,
        account_id: str,
        broker: Any,
        tablename: str = "logs",
        force: bool = False
    ):
        """Flush both sync and async buffered logs to the DB."""
        # 1. Flush any synchronous buffer items in worker thread via sync_to_async
        sync_batch = None
        with self._sync_buffer_lock:
            if self._sync_log_buffer:
                now = time.time()
                if force or len(self._sync_log_buffer) >= engine_config.LOG_BATCH_SIZE_LIMIT or (now - self._last_sync_flush) >= engine_config.LOG_FLUSH_INTERVAL:
                    sync_batch = list(self._sync_log_buffer)
                    self._sync_log_buffer.clear()
                    self._last_sync_flush = now

        if sync_batch:
            await sync_to_async(self._execute_bulk_create, thread_sensitive=False)(sync_batch)

        # 2. Flush async logs for this account
        logs_copy = None
        async with self._async_buffer_lock:
            if account_id not in self._async_logs or tablename not in self._async_logs[account_id]:
                return

            current_logs = self._async_logs[account_id][tablename]
            if not current_logs:
                return

            elapsed = time.time() - self._async_last_flush[account_id][tablename]
            should_flush = force or (
                len(current_logs) >= engine_config.LOG_BATCH_SIZE_LIMIT
                or elapsed >= engine_config.LOG_FLUSH_INTERVAL
            )

            if should_flush:
                logs_copy = list(current_logs)
                self._async_logs[account_id][tablename].clear()
                self._async_last_flush[account_id][tablename] = time.time()

        if logs_copy:
            db_lock = self._async_db_locks.get(account_id)
            if db_lock:
                async with db_lock:
                    await self._write_async_batch_to_db(broker, tablename, logs_copy)


# Singleton instance shared across all algorithms
algo_logger = SharedAlgoLogger()
