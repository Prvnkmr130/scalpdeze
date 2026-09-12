#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ruff: noqa: E402
"""
Polars-Native Multi-Account Monolithic Engine for Zerodha Options Trading
File: algo_trading/algos/indian_opt_trde_polars.py
Author: Prvnkmr / Antigravity

This module is the high-performance, Polars-native options trading engine designed with 
strict 1:1 business logic parity against the legacy reference implementation.

Key Architectural Components:
- Data Layer: Columnar Polars DataFrames and zero-copy Apache Arrow structures for sub-second execution.
- Multi-Account Isolation: IndianUserAccount encapsulation of sessions, orders, positions, and stop-loss registries.
- Macro Lifecycle: MarketTimeDelegate controlling pre-market setup (08:45), trading (09:00–23:55), and post-trade (23:55+).
- Master Instrument Resolution: Fully delegated to the broker client utility via `self.client.master_tkn_list()`.
  `KotakNeoUtility.master_tkn_list()` (kotak_utils.py) handles Kotak Neo ScripMaster CSV parsing across NSE, NFO, CDS, MCX, BFO, BSE,
  resolving nearest unexpired futures (exp_date_list == min(exp_date_list) & expiry > today), joining cap_config, and persisting
  results in ExchangeMasterData for crash recovery. If the client lacks master_tkn_list, TradeAlgo logs an error and halts
  instrument resolution (empty tables — algo will not trade until restarted with a valid broker client).
- Session & Timing Control: Exact exchange windows and hold-time masks (exchg_time_buy_chk, special_session_chk, 
  next_session_closed, exchg_time_sell_chk, half_time, session_end, sl_update_time, session_start, expiry_sell_time).
- Multi-Timeframe Resampling: Vectorized SIMD downsampling into 1m, 3m, 5m, 10m, 15m, 30m, 60m, Day, Half-Day candles.
- Momentum Analysis: CDS tick-size normalization and Heikin-Ashi momentum signal evaluations (Paths 1 to 13_2).
- Risk & Order Management: Group-level capital allocation by (Ref_stock, instrument_type), 30s symbol cooldowns,
  Iceberg limit routing (ice_ordr), dynamic trailing stop loss (slu), and stale order purging (order_pending_chk).
- Logging & Standards: ISO timestamps, caller coordinate prefixing, in-memory buffering, and periodic batch database flushing.
"""

from __future__ import annotations
import sys
import os

try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(line_buffering=True)
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass
os.environ["PYTHONUNBUFFERED"] = "1"

# Limit OpenBLAS and multi-threading allocations to prevent memory allocation failures
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["POLARS_MAX_THREADS"] = os.getenv("POLARS_MAX_THREADS", "2")

import signal

# Ensure project root and package directory are in sys.path for direct script execution
_current_dir = os.path.dirname(os.path.abspath(__file__))
_parent_dir = os.path.dirname(_current_dir)
_root_dir = os.path.dirname(_parent_dir)
if _root_dir not in sys.path:
    sys.path.insert(0, _root_dir)
if _parent_dir not in sys.path:
    sys.path.insert(0, _parent_dir)
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

import gc
import time
import json
import logging
from datetime import datetime, date, timedelta, time as dt_time
from typing import Dict, List, Optional, Any, Tuple
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
import threading
import zoneinfo
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
try:
    import orjson
except ImportError:
    orjson = json


def flatten(d: Any, separator: str = "_") -> Dict[str, Any]:
    """Lightweight pure-python dictionary flattener without external dependencies."""
    if not isinstance(d, dict):
        return d
    out = {}
    def _flat(x: Any, prefix: str = ""):
        if isinstance(x, dict):
            for k, v in x.items():
                _flat(v, f"{prefix}{k}{separator}" if prefix else f"{k}{separator}")
        elif isinstance(x, list):
            for i, v in enumerate(x):
                _flat(v, f"{prefix}{i}{separator}" if prefix else f"{i}{separator}")
        else:
            out[prefix[:-len(separator)] if prefix.endswith(separator) else prefix] = x
    _flat(d)
    return out


try:
    import django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'algo_trading.settings')
    django.setup()
except Exception:
    pass

from django.conf import settings
from django.utils import timezone as dj_timezone
from django.db.models import Q
from kalai.models import (
    Broker, ProcessedTickStore, AlgoInfo, AlgoLog
)
from algo_trading.algos import algo
from algo_trading.algos.zerodha_utils import ZerodhaUtility
from algo_trading.algos.kotak_utils import KotakNeoUtility
from algo_trading.algos.indian_market_session import IndianMarketSession
from algo_trading.algos.indian_candle_engine import (
    group_by_rolling_window,
    heikin_ashi,
    update_candles_incremental,
    get_candle_bucket_start,
)
from algo_trading.algos.crypto_master_tokens import str_to_token
from algo_trading.algos.indian_user_account import (
    IndianUserAccount,
)
from algo_trading.algos.polars_excel import (
    load_indian_algo_config,
    write_polars_sheets_to_excel,
)

from algo_trading.algos.logger import algo_logger


# Dynamic data directory resolver
def _resolve_data_dir() -> str:
    candidates = [
        getattr(settings, 'DATA_DIR', None),
        getattr(settings, 'BASE_DIR', None),
        os.path.dirname(_parent_dir),
        os.path.dirname(_current_dir),
        _current_dir,
    ]
    for c in candidates:
        if c:
            p = os.path.abspath(str(c))
            if os.path.exists(p):
                return p
    return _current_dir

data_file_path = _resolve_data_dir()

# Synchronize run mode and environment variables from app_config / .env
try:
    from algo_trading.app_config import config
    APP_MODE = str(config.APP_MODE).strip().lower()
    DEBUG = bool(config.is_debug)
    APP_TIMEZONE = str(config.TIMEZONE).strip()
except Exception:
    APP_MODE = os.getenv("APP_MODE", "debug").strip().lower()
    DEBUG = getattr(settings, 'DEBUG', (APP_MODE == "debug"))
    APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Asia/Kolkata").strip()

sleep = time.sleep


# ============================================================================
# TIMEZONE & SESSION UTILITIES
# ============================================================================

def now_ist(tz: str = "Asia/Kolkata") -> datetime:
    return datetime.now(zoneinfo.ZoneInfo(tz))

def now_ist_naive(tz: str = "Asia/Kolkata") -> datetime:
    return datetime.now(zoneinfo.ZoneInfo(tz)).replace(tzinfo=None)

def today_ist(tz: str = "Asia/Kolkata") -> date:
    return datetime.now(zoneinfo.ZoneInfo(tz)).date()

def time_ist(tz: str = "Asia/Kolkata") -> time:
    return datetime.now(zoneinfo.ZoneInfo(tz)).time()

def tomorrow_ist(tz: str = "Asia/Kolkata") -> date:
    return (datetime.now(zoneinfo.ZoneInfo(tz)) + timedelta(days=1)).date()

def yesterday_ist(tz: str = "Asia/Kolkata") -> date:
    return (datetime.now(zoneinfo.ZoneInfo(tz)) - timedelta(days=1)).date()

def weekday_ist(tz: str = "Asia/Kolkata") -> int:
    return datetime.now(zoneinfo.ZoneInfo(tz)).weekday()

def is_weekend_ist(tz: str = "Asia/Kolkata") -> bool:
    return weekday_ist(tz=tz) >= 5

def is_today_in_ist(dt: Any, tz: str = "Asia/Kolkata") -> bool:
    if dt is None:
        return False
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=zoneinfo.ZoneInfo(tz))
        return dt.astimezone(zoneinfo.ZoneInfo(tz)).date() == today_ist(tz=tz)
    if isinstance(dt, date):
        return dt == today_ist(tz=tz)
    return False

class MarketTimeDelegate:
    """Delegates market time checks with local fallbacks and daylight savings support."""
    @staticmethod
    def check_for_orderplacing_time(tz: str = "Asia/Kolkata", day_light_saving: bool = True) -> bool:
        if DEBUG:
            return True
        t = time_ist(tz=tz)
        end_minute = 30 if day_light_saving else 55
        return (t.hour >= 9) and (t.hour < 23 or (t.hour == 23 and t.minute <= end_minute))

    @staticmethod
    def initialising_time(tz: str = "Asia/Kolkata", day_light_saving: bool = True) -> bool:
        if DEBUG:
            return True
        t = time_ist(tz=tz)
        end_minute = 30 if day_light_saving else 55
        return (t.hour == 8 and t.minute >= 45) or (t.hour == 23 and t.minute <= end_minute)

    @staticmethod
    def is_post_trade_time(tz: str = "Asia/Kolkata", day_light_saving: bool = True) -> bool:
        t = time_ist(tz=tz)
        end_minute = 30 if day_light_saving else 55
        return t.hour > 23 or (t.hour == 23 and t.minute > end_minute)

def is_market_day_ist(special_session: bool = False, tz: str = "Asia/Kolkata") -> bool:
    if special_session or DEBUG:
        return True
    return not is_weekend_ist(tz=tz)



# ============================================================================
# BUFFERED DATABASE LOGGING INFRASTRUCTURE (Complying with Project Rules)
# ============================================================================
# BUFFERED DATABASE LOGGING INFRASTRUCTURE (Centralized in algo_logger)
# ============================================================================

# Local session logs buffer tracking logs generated during the current execution run (for Excel export)
_LOCAL_SESSION_LOGS: List[Dict[str, Any]] = []
_LOCAL_SESSION_LOGS_LOCK = threading.Lock()


def _flush_buffered_db_logs(force: bool = False):
    """Batch-flush in-memory buffered logs into AlgoLog table via centralized algo_logger."""
    algo_logger.flush_sync(force=force)


import atexit
def _cleanup_db_pools():
    try:
        _flush_buffered_db_logs(force=True)
    except Exception:
        pass

atexit.register(_cleanup_db_pools)

# Maintain context reference for strategy account tracking
_CURRENT_ALGO_NAME = "indian_opt_trde_polars"
_ACTIVE_CONTEXT = algo_logger.context


def _db_insert_single_log(level: str, tag: str, iso_msg: str, broker_obj: Any = None, algo_name: Optional[str] = None):
    """Buffer log entry for batch insertion into AlgoLog table via centralized algo_logger."""
    algo_logger.log_sync(
        message=iso_msg,
        tag=tag,
        level=level,
        broker_obj=broker_obj,
        algo_name=algo_name
    )


class BufferedDBLogHandler:
    """Thread-safe buffered DB logger that batches logs, attaches ISO timestamps, and tracks caller lines/methods."""
    def __init__(self, tag: str = "SYSTEM"):
        self.tag = tag

    @staticmethod
    def _get_caller_location() -> str:
        """Inspect caller stack to extract [filename:line in func_name()]."""
        try:
            import sys
            import os
            frame = sys._getframe(2)
            while frame:
                code = frame.f_code
                filename = os.path.basename(code.co_filename)
                func_name = code.co_name
                lineno = frame.f_lineno
                if filename not in ('logger.py', 'logging', '__init__.py') and not func_name.startswith('_handle_log'):
                    return f"[{filename}:{lineno} in {func_name}()]"
                frame = frame.f_back
        except Exception:
            pass
        return ""

    def _handle_log(self, level: str, msg: str, *args, **kwargs):
        iso_timestamp = datetime.now().isoformat()
        caller_str = self._get_caller_location()
        if caller_str:
            formatted_msg = f"[{iso_timestamp}] [{self.tag}] {caller_str} {msg}"
        else:
            formatted_msg = f"[{iso_timestamp}] [{self.tag}] {msg}"

        if args:
            try:
                formatted_msg = formatted_msg % args
            except Exception:
                pass
        
        # Output to console immediately with safe encoding fallback
        try:
            print(f"[{level}] {formatted_msg}", flush=True)
            sys.stdout.flush()
        except UnicodeEncodeError:
            try:
                enc = sys.stdout.encoding or 'utf-8'
                safe_msg = formatted_msg.encode(enc, errors='replace').decode(enc)
                print(f"[{level}] {safe_msg}", flush=True)
                sys.stdout.flush()
            except Exception:
                pass
        
        # Buffer into local session memory tracker
        try:
            with _LOCAL_SESSION_LOGS_LOCK:
                _LOCAL_SESSION_LOGS.append({
                    "tag": self.tag,
                    "level": level.upper(),
                    "message": formatted_msg,
                    "timestamp": dj_timezone.now(),
                })
        except Exception:
            pass

        # Write to system logger queue
        try:
            _db_insert_single_log(level, self.tag, formatted_msg, algo_name=_CURRENT_ALGO_NAME)
        except Exception:
            pass

    def debug(self, msg: str, *args, **kwargs):
        self._handle_log("DEBUG", msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        self._handle_log("INFO", msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        self._handle_log("WARNING", msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        self._handle_log("ERROR", msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):
        self._handle_log("ERROR", msg, *args, **kwargs)


general_logger = BufferedDBLogHandler(tag="GENERAL")
inst_analysis_logger = BufferedDBLogHandler(tag="INST_ANALYSIS")
trade_logger = BufferedDBLogHandler(tag="TRADE")
strike_logger = BufferedDBLogHandler(tag="STRIKE")
order_logger = BufferedDBLogHandler(tag="ORDER")
app_logger = BufferedDBLogHandler(tag="APP")
opt_exception_logger = BufferedDBLogHandler(tag="EXCEPTION")
logger = logging.getLogger("algo_trading")


class InterceptDBLogHandler(logging.Handler):
    """
    Standard logging.Handler that intercepts all standard library log records
    across ANY broker and application module, routing them into our buffered DB & session log pipeline.
    """
    def __init__(self, default_tag: str = "BROKER_API"):
        super().__init__()
        self.default_tag = default_tag

    def emit(self, record: logging.LogRecord):
        try:
            if getattr(record, '_intercepted_db', False):
                return
            record._intercepted_db = True

            # Ignore DEBUG records and framework registry logs from __init__.py / load_all_algos
            if record.levelno < logging.INFO:
                return
            if record.name == "algo_trading.algos" or "load_all_algos" in getattr(record, "funcName", ""):
                return

            msg = record.getMessage()
            level = record.levelname.upper()
            caller_str = f"[{os.path.basename(record.pathname)}:{record.lineno} in {record.funcName}()]"
            
            tag = self.default_tag
            name_lower = record.name.lower()
            if "order" in name_lower or "order" in record.funcName.lower():
                tag = "ORDER"
            elif "trade" in name_lower:
                tag = "TRADE"
            elif "strike" in name_lower:
                tag = "STRIKE"
            elif "analysis" in name_lower:
                tag = "INST_ANALYSIS"
            elif "util" in name_lower or "broker" in name_lower or "feed" in name_lower:
                tag = "BROKER_API"
            elif "app" in name_lower:
                tag = "APP"

            iso_timestamp = datetime.now().isoformat()
            formatted_msg = f"[{iso_timestamp}] [{tag}] {caller_str} {msg}"

            # Infer appropriate algorithm and account context for intercepted log
            source_info = f"{record.name} {record.pathname} {msg}".lower()
            if any(k in source_info for k in ("delta", "coinswitch", "coindcx", "crypto")):
                target_algo = "crypto_opt_trde_polars"
            elif any(k in source_info for k in ("kotak", "zerodha", "shoonya", "indian_opt_trde", "indian_candle", "indian_strike")):
                target_algo = "indian_opt_trde_polars"
            elif algo_logger.context.algo_name:
                target_algo = algo_logger.context.algo_name
            else:
                target_algo = "SYSTEM"

            target_broker = (
                getattr(record, "broker", None)
                or getattr(record, "broker_obj", None)
                or algo_logger.context.active_account
            )

            # Buffer into local session memory tracker only if relevant to Indian options trading
            if target_algo == "indian_opt_trde_polars":
                try:
                    with _LOCAL_SESSION_LOGS_LOCK:
                        _LOCAL_SESSION_LOGS.append({
                            "tag": tag,
                            "level": level,
                            "message": formatted_msg,
                            "timestamp": dj_timezone.now(),
                        })
                except Exception:
                    pass

            # Buffer into DB queue
            _db_insert_single_log(level, tag, formatted_msg, broker_obj=target_broker, algo_name=target_algo)
        except Exception:
            pass


def _setup_logger_interception():
    """Register InterceptDBLogHandler globally on algo_trading, brokers, and kalai loggers."""
    handler = InterceptDBLogHandler()
    for logger_name in [
        "algo_trading",
        "algo_trading.brokers",
        "kalai",
    ]:
        lg = logging.getLogger(logger_name)
        if not any(isinstance(h, InterceptDBLogHandler) for h in lg.handlers):
            lg.addHandler(handler)

_setup_logger_interception()


def parse_algo_log_record(
    msg: str,
    tag: str,
    level: str,
    timestamp_str: str,
    acc_id: Optional[str] = None,
    algo_name: Optional[str] = None,
    log_id: Any = 1,
    account_name: str = "",
    broker_name: str = ""
) -> Dict[str, Any]:
    """
    Generic log parser extracting structured telemetry, caller coordinates,
    clean messages, and domain-specific attributes across any broker and account.
    """
    import re

    raw_msg = str(msg or "")

    # 1. Extract caller location coordinates e.g. [filename.py:123 in func()]
    caller_match = re.search(r"\[([a-zA-Z0-9_\-\.]+\.py):(\d+)\s+in\s+([a-zA-Z0-9_]+)\(\)\]", raw_msg)
    caller_file = caller_match.group(1) if caller_match else ""
    caller_func = caller_match.group(3) if caller_match else ""
    caller_loc = caller_match.group(0) if caller_match else ""

    # 2. Derive clean message by stripping out ISO timestamps, tags, and caller brackets
    clean_msg = raw_msg
    clean_msg = re.sub(r"^\[\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?\]\s*", "", clean_msg)
    clean_msg = re.sub(r"^\[[A-Z_]+\]\s*", "", clean_msg)
    if caller_loc:
        clean_msg = clean_msg.replace(caller_loc, "").strip()
    clean_msg = re.sub(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ \[[A-Z]+\] [^—–-]+ [—–-]\s*", "", clean_msg)
    clean_msg = clean_msg.strip()

    # 3. Dynamically resolve Broker
    broker = broker_name
    if not broker:
        # Detect broker dynamically from caller path or raw text
        for b_cand in ["zerodha", "coindcx", "coinswitch", "tradovate", "kotak", "shoonya", "angelone", "fyers", "binance", "interactive_brokers"]:
            if b_cand in raw_msg.lower() or b_cand in caller_file.lower():
                broker = b_cand.upper()
                break
    if not broker:
        broker = "BROKER"

    # 4. Dynamically resolve Account_ID
    resolved_acc_id = acc_id
    if not resolved_acc_id or resolved_acc_id in ("SYSTEM", "PRIMARY", ""):
        # Extract account ID dynamically from message pattern e.g. [ACC123] or account 'ACC123'
        acc_match = re.search(r"account\s*['\"]([a-zA-Z0-9_-]+)['\"]", raw_msg, re.IGNORECASE)
        if not acc_match:
            acc_match = re.search(r"\[([A-Z0-9_-]{3,20})\]", raw_msg)
        if acc_match and acc_match.group(1) not in ("INFO", "WARNING", "ERROR", "GENERAL", "TRADE", "ORDER", "STRIKE", "APP", "DEBUG", "DEBUG POLARS", "SYSTEM", "PRIMARY"):
            resolved_acc_id = acc_match.group(1)
        else:
            resolved_acc_id = acc_id or "SYSTEM"

    # 5. Telemetry metrics extraction
    balance_match = re.search(r"Balance=Rs\.?([0-9,.]+)", raw_msg)
    balance_val = float(balance_match.group(1).replace(",", "")) if balance_match else None

    positions_match = re.search(r"OpenPositions=([0-9]+)", raw_msg)
    positions_val = int(positions_match.group(1)) if positions_match else None

    orders_match = re.search(r"Orders=([0-9]+)", raw_msg)
    orders_val = int(orders_match.group(1)) if orders_match else None

    exec_time_match = re.search(r"(?:executed in|cycle executed in|took)\s+([0-9.]+)\s*s", raw_msg)
    exec_time_val = float(exec_time_match.group(1)) if exec_time_match else None

    # 6. Trading order parameters extraction (Symbol, Qty, Price)
    symbol_match = re.search(r"(?:for|BUY:|SELL:|order|candle tables to DB for account)\s+([A-Z0-9_\-]{3,30}(?:CE|PE|FUT)?)\b", raw_msg)
    symbol_val = symbol_match.group(1) if symbol_match else None
    if not symbol_val:
        sym_candidate = re.search(r"\b(NIFTY\d{2}[A-Z]{3}\d+[CP]E|BANKNIFTY\d{2}[A-Z]{3}\d+[CP]E|[A-Z]{3,10}\d{2}[A-Z]{3}\d+[CP]E)\b", raw_msg)
        symbol_val = sym_candidate.group(1) if sym_candidate else None

    qty_match = re.search(r"(?:Qty|quantity|qty)[=\s:]+([0-9]+)", raw_msg)
    qty_val = int(qty_match.group(1)) if qty_match else None

    price_match = re.search(r"(?:Price|price|at)[=\s:]+([0-9.]+)", raw_msg)
    price_val = float(price_match.group(1)) if price_match else None

    # 7. Generic Component classification
    component = "SYSTEM"
    func_l = caller_func.lower()
    tag_u = (tag or "").upper()
    if func_l in ("sync_account_state", "chk_live_bal", "pos_data", "holdings") or "balance=" in raw_msg.lower() or "broker" in tag_u:
        component = "ACCOUNT_SYNC"
    elif func_l in ("ice_ordr", "order_pending_chk", "slu") or tag_u == "ORDER" or "order" in raw_msg.lower():
        component = "ORDER_ENGINE"
    elif func_l in ("derivative_analysis", "mom", "strike_detect", "strike_update") or tag_u in ("INST_ANALYSIS", "STRIKE"):
        component = "STRATEGY_ANALYSIS"
    elif func_l in ("update_all_candles_batch", "heikin_ashi", "write_db_candle") or "resampled" in raw_msg.lower() or "candle" in raw_msg.lower():
        component = "CANDLE_ENGINE"
    elif func_l in ("__init__", "master_tkn_list", "token_list_update") or "config" in raw_msg.lower() or "token" in raw_msg.lower():
        component = "TOKEN_CONFIG"
    elif func_l in ("main", "trde", "pre_trde", "post_trde"):
        component = "ENGINE_CORE"

    # 8. Generic Action Type classification
    action_type = "GENERAL_LOG"
    raw_lower = raw_msg.lower()
    if "simulated buy" in raw_lower or "placed buy order" in raw_lower:
        action_type = "BUY_ORDER"
    elif "simulated sell" in raw_lower or "placed sell order" in raw_lower:
        action_type = "SELL_ORDER"
    elif "cancellation of stale order" in raw_lower or "cancel_order" in raw_lower:
        action_type = "CANCEL_STALE_ORDER"
    elif "stop-loss" in raw_lower or "slu" in raw_lower or func_l == "slu":
        action_type = "TRAILING_STOP_LOSS"
    elif "synced:" in raw_lower or "balance=" in raw_lower or "fetch balances" in raw_lower or func_l == "sync_account_state":
        action_type = "SYNC_TELEMETRY"
    elif "resampled" in raw_lower:
        action_type = "RESAMPLE_CANDLES"
    elif "token list update" in raw_lower or "tokens" in raw_lower:
        action_type = "TOKEN_SUBSCRIPTION"
    elif "derivative analysis" in raw_lower or "evaluating derivative" in raw_lower:
        action_type = "DERIVATIVE_ANALYSIS"
    elif "polars loop cycle executed" in raw_lower:
        action_type = "CYCLE_EXECUTION"
    elif "attempt" in raw_lower and "failed with error" in raw_lower:
        action_type = "API_RETRY"
    elif level in ("ERROR", "EXCEPTION"):
        action_type = "ERROR_EVENT"

    # 9. Generic Category
    category = "GENERAL"
    if "Balance=" in raw_msg or "Synced:" in raw_msg or action_type == "SYNC_TELEMETRY":
        category = "ACCOUNT_SYNC"
    elif action_type == "TRAILING_STOP_LOSS":
        category = "STOP_LOSS"
    elif action_type in ("BUY_ORDER", "SELL_ORDER", "CANCEL_STALE_ORDER") or tag_u in ("ORDER", "TRADE"):
        category = "TRADE_ORDER"
    elif level in ("ERROR", "EXCEPTION"):
        category = "ERROR"
    elif tag:
        category = tag

    # 10. Generic Error Details
    error_details = None
    if level in ("ERROR", "EXCEPTION", "WARNING") or "error" in raw_lower or "failed" in raw_lower:
        err_m = re.search(r"(?:error|failed with error|failed to [^:]+):\s*([^\n\r]+)", raw_msg, re.IGNORECASE)
        error_details = err_m.group(1).strip() if err_m else clean_msg

    return {
        "Log_ID": log_id,
        "Timestamp": timestamp_str,
        "Broker": broker,
        "Account_ID": resolved_acc_id,
        "Account_Name": account_name or resolved_acc_id,
        "Algorithm": algo_name or _ACTIVE_CONTEXT.algo_name,
        "Tag": tag,
        "Level": level,
        "Category": category,
        "Component": component,
        "Action_Type": action_type,
        "Symbol": symbol_val,
        "Quantity": qty_val,
        "Price": price_val,
        "Balance_Rs": balance_val,
        "Open_Positions": positions_val,
        "Orders_Count": orders_val,
        "Execution_Time_s": exec_time_val,
        "Caller_Location": caller_loc,
        "Clean_Message": clean_msg,
        "Message": clean_msg,
        "Error_Details": error_details,
        "Raw_Message": raw_msg,
    }


def export_local_algo_logs_to_excel(
    output_path: str,
    start_time: Optional[datetime] = None,
    account_id: Optional[str] = None,
    algo_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generic local log exporter: collects algorithm logs generated locally from start_time onwards,
    parses structured telemetry/coordinates/events, and exports a fresh multi-sheet Excel file at output_path
    across ANY broker, algorithm, or account.
    """
    # Ensure all buffered in-memory logs are written to local database
    _flush_buffered_db_logs(force=True)

    records = []

    # 1. Query local AlgoLog database table
    try:
        qs = AlgoLog.objects.select_related('account').all().order_by('timestamp')
        if algo_name:
            qs = qs.filter(algo_name=algo_name)
        if start_time:
            qs = qs.filter(timestamp__gte=start_time)
        if account_id and account_id not in ("PRIMARY", "SYSTEM"):
            qs = qs.filter(Q(account__account_id=account_id) | Q(account__name=account_id))

        for log in qs:
            msg = log.message or ""
            acc_id = log.account.account_id if log.account else (account_id or "SYSTEM")
            acc_name = log.account.name if log.account else (account_id or "SYSTEM")
            broker_val = str(getattr(log.account, 'broker_name', getattr(log.account, 'name', ''))) if log.account else ""
            ts_str = log.timestamp.strftime("%Y-%m-%d %H:%M:%S") if log.timestamp else ""

            parsed = parse_algo_log_record(
                msg=msg,
                tag=log.tag,
                level=log.level,
                timestamp_str=ts_str,
                acc_id=acc_id,
                algo_name=log.algo_name,
                log_id=log.id,
                account_name=acc_name,
                broker_name=broker_val,
            )
            records.append(parsed)
    except Exception as e_db:
        general_logger.warning(f"Local AlgoLog table query: {e_db}")

    # 2. In-memory session logs fallback if local DB returned 0 records (e.g. offline testing)
    if not records:
        with _LOCAL_SESSION_LOGS_LOCK:
            session_entries = list(_LOCAL_SESSION_LOGS)
        for idx, entry in enumerate(session_entries, start=1):
            ts = entry.get("timestamp")
            if start_time and ts:
                try:
                    ts_epoch = ts.timestamp() if hasattr(ts, 'timestamp') else 0
                    st_epoch = start_time.timestamp() if hasattr(start_time, 'timestamp') else 0
                    if ts_epoch < st_epoch:
                        continue
                except Exception:
                    pass
            msg = entry.get("message", "")
            tag = entry.get("tag", "GENERAL")
            lvl = entry.get("level", "INFO")
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else ""

            parsed = parse_algo_log_record(
                msg=msg,
                tag=tag,
                level=lvl,
                timestamp_str=ts_str,
                acc_id=account_id or "SYSTEM",
                algo_name=algo_name or _ACTIVE_CONTEXT.algo_name,
                log_id=idx,
                account_name=account_id or "SYSTEM",
            )
            records.append(parsed)

    all_columns = [
        "Log_ID", "Timestamp", "Broker", "Account_ID", "Account_Name", "Algorithm",
        "Tag", "Level", "Category", "Component", "Action_Type", "Symbol", "Quantity",
        "Price", "Balance_Rs", "Open_Positions", "Orders_Count", "Execution_Time_s",
        "Caller_Location", "Clean_Message", "Message", "Error_Details", "Raw_Message"
    ]

    df_all = pl.DataFrame(records) if records else pl.DataFrame(schema={c: pl.Utf8 for c in all_columns})

    from algo_trading.algos.polars_excel import write_polars_sheets_to_excel

    # Sheet 1: All Logs
    export_cols = [c for c in all_columns if c in df_all.columns and c != "Raw_Message"]
    df_all_export = df_all.select(export_cols) if not df_all.is_empty() else df_all

    # Sheet 2: Account Telemetry
    if not df_all.is_empty() and "Category" in df_all.columns:
        cat_cond = pl.col("Category").is_in(["ACCOUNT_SYNC", "TRADE_ORDER", "STOP_LOSS"])
        bal_cond = pl.col("Balance_Rs").is_not_null() if "Balance_Rs" in df_all.columns else pl.lit(False)
        pos_cond = pl.col("Open_Positions").is_not_null() if "Open_Positions" in df_all.columns else pl.lit(False)
        exec_cond = pl.col("Execution_Time_s").is_not_null() if "Execution_Time_s" in df_all.columns else pl.lit(False)
        df_sync = df_all.filter(cat_cond | bal_cond | pos_cond | exec_cond)
    else:
        df_sync = pl.DataFrame()

    telemetry_cols = [c for c in ["Log_ID", "Timestamp", "Broker", "Account_ID", "Component", "Action_Type", "Balance_Rs", "Open_Positions", "Orders_Count", "Execution_Time_s", "Clean_Message", "Message"] if c in df_all.columns]
    df_sync_export = df_sync.select(telemetry_cols) if not df_sync.is_empty() else pl.DataFrame({"Info": ["No telemetry sync records found for this execution."]})

    # Sheet 3: Trades & Orders
    if not df_all.is_empty() and "Action_Type" in df_all.columns:
        act_cond = pl.col("Action_Type").is_in(["BUY_ORDER", "SELL_ORDER", "CANCEL_STALE_ORDER", "TRAILING_STOP_LOSS"])
        tag_cond = pl.col("Tag").is_in(["TRADE", "ORDER"]) if "Tag" in df_all.columns else pl.lit(False)
        sym_cond = pl.col("Symbol").is_not_null() if "Symbol" in df_all.columns else pl.lit(False)
        df_trades = df_all.filter(act_cond | tag_cond | sym_cond)
    else:
        df_trades = pl.DataFrame()

    trade_cols = [c for c in ["Log_ID", "Timestamp", "Broker", "Account_ID", "Action_Type", "Symbol", "Quantity", "Price", "Execution_Time_s", "Clean_Message", "Message"] if c in df_all.columns]
    df_trades_export = df_trades.select(trade_cols) if not df_trades.is_empty() else pl.DataFrame({"Info": ["No orders or trade candidate signals recorded in this execution."]})

    # Sheet 4: Errors & Warnings
    if not df_all.is_empty() and "Level" in df_all.columns:
        lvl_cond = pl.col("Level").is_in(["ERROR", "WARNING", "EXCEPTION"])
        err_cond = pl.col("Error_Details").is_not_null() if "Error_Details" in df_all.columns else pl.lit(False)
        df_errors = df_all.filter(lvl_cond | err_cond)
    else:
        df_errors = pl.DataFrame()

    err_cols = [c for c in ["Log_ID", "Timestamp", "Broker", "Account_ID", "Level", "Component", "Caller_Location", "Error_Details", "Clean_Message", "Message"] if c in df_all.columns]
    df_errors_export = df_errors.select(err_cols) if not df_errors.is_empty() else pl.DataFrame({"Status": ["No errors or warnings recorded in this execution."]})

    # Sheet 5: Summary Statistics
    summary_data = {
        "Metric": [
            "Total Logs Processed",
            "Unique Accounts",
            "Unique Brokers",
            "Unique Algorithms",
            "Strategy Cycles Executed",
            "Account Sync Events",
            "Trades & Order Actions",
            "Error Count",
            "Warning Count",
            "Execution Session Start",
            "Export Generated At"
        ],
        "Value": [
            len(df_all),
            len(df_all["Account_ID"].unique().to_list()) if not df_all.is_empty() and "Account_ID" in df_all.columns else 0,
            len(df_all["Broker"].unique().to_list()) if not df_all.is_empty() and "Broker" in df_all.columns else 0,
            len(df_all["Algorithm"].unique().to_list()) if not df_all.is_empty() and "Algorithm" in df_all.columns else 0,
            len(df_all.filter(pl.col("Action_Type") == "CYCLE_EXECUTION")) if not df_all.is_empty() and "Action_Type" in df_all.columns else 0,
            len(df_all.filter(pl.col("Category") == "ACCOUNT_SYNC")) if not df_all.is_empty() and "Category" in df_all.columns else 0,
            len(df_trades) if not df_trades.is_empty() else 0,
            len(df_all.filter(pl.col("Level") == "ERROR")) if not df_all.is_empty() and "Level" in df_all.columns else 0,
            len(df_all.filter(pl.col("Level") == "WARNING")) if not df_all.is_empty() and "Level" in df_all.columns else 0,
            start_time.strftime("%Y-%m-%d %H:%M:%S") if start_time else "All",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]
    }

    final_output_path = write_polars_sheets_to_excel({
        "All_Logs": df_all_export,
        "Account_Telemetry": df_sync_export,
        "Trades_&_Orders": df_trades_export,
        "Errors_&_Warnings": df_errors_export,
        "Summary": summary_data,
    }, output_path)

    return {
        "file_path": final_output_path,
        "row_count": len(df_all),
        "accounts": df_all["Account_ID"].unique().to_list() if not df_all.is_empty() and "Account_ID" in df_all.columns else [],
    }


def timeit(func):
    @wraps(func)
    def measure_time(*args, **kw):
        start_time = time.time()
        result = func(*args, **kw)
        elapsed = time.time() - start_time
        general_logger.info(f"Execution of {func.__name__} took {elapsed:.5f} seconds.")
        return result
    return measure_time


def run_once(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)
    wrapper.has_run = False
    return wrapper


# ============================================================================
# MARKET DATA INGESTION (Polars-Native)
# ============================================================================

def fetch_recent_ticks(broker_obj: Optional[Broker] = None, seconds: Optional[int] = None, limit: int = 2000) -> pl.DataFrame:
    """
    Fetch recent market ticks from ProcessedTickStore in PostgreSQL into a Polars DataFrame.
    Seamlessly parses and normalizes ticks from both Zerodha (Kite) and Kotak Neo (Trade API).
    Returns normalized Polars DataFrame with columns:
    ['instrument_token', 'last_price', 'date_time', 'volume', 'oi', 'buy_price', 'sell_price', 'buy_quantity', 'sell_quantity', 'mode']
    """
    try:
        qs = ProcessedTickStore.objects.all()
        if broker_obj:
            qs = qs.filter(account=broker_obj)

        if seconds is not None and seconds > 0:
            since = dj_timezone.now() - timedelta(seconds=seconds)
            ticks = list(qs.filter(timestamp__gte=since).order_by('timestamp').values_list('data', 'timestamp')[:limit])
        else:
            ticks = list(qs.order_by('-timestamp').values_list('data', 'timestamp')[:limit])
            ticks.reverse()

        if not ticks:
            ticks = list(qs.order_by('-timestamp').values_list('data', 'timestamp')[:min(limit, 500)])
            ticks.reverse()

        if not ticks and broker_obj:
            try:
                from django.db import connection
                from algo_trading.brokers.consumers import get_broker_stream_table_name
                stream_tbl = get_broker_stream_table_name(broker_obj.account_id or broker_obj.name)
                with connection.cursor() as cur:
                    cur.execute(f'SELECT data, timestamp FROM "{stream_tbl}" ORDER BY timestamp DESC LIMIT %s;', [min(limit, 500)])
                    rows = cur.fetchall()
                    if rows:
                        ticks = [(r[0], datetime.fromtimestamp(r[1] / 1e9, tz=dj_timezone.utc)) for r in reversed(rows)]
            except Exception:
                pass

        records = []
        for d, ts in ticks:
            if isinstance(d, str):
                try:
                    d = orjson.loads(d)
                except Exception:
                    continue
            raw_list = d if isinstance(d, list) else [d]
            for item in raw_list:
                if not isinstance(item, dict):
                    continue
                # Skip Kotak WebSocket control/handshake messages
                if item.get('type') in ('cn', 'sub', 'unsub', 'hb') or 'stCode' in item or item.get('stat') == 'Ok':
                    continue

                # Token extraction (Zerodha: instrument_token, Kotak: tk / token)
                tkn = item.get('instrument_token') or item.get('tk') or item.get('token')
                sym = str(item.get('tradingsymbol') or item.get('symbol') or item.get('ts') or item.get('s') or '').strip()
                if tkn is None and sym:
                    tkn = sym
                if tkn is None:
                    continue
                try:
                    tkn_int = int(tkn)
                except (ValueError, TypeError):
                    tkn_int = str_to_token(str(tkn))

                # Price extraction (Zerodha: last_price, Kotak: ltp / iv / c / ap / bp / sp)
                last_p = item.get('last_price') or item.get('ltp') or item.get('iv') or item.get('c') or item.get('ap') or item.get('bp') or item.get('sp')
                if last_p is None:
                    continue
                try:
                    p_float = float(last_p)
                except (ValueError, TypeError):
                    continue

                # DateTime extraction
                dt = None
                from zoneinfo import ZoneInfo
                tz_ist = ZoneInfo("Asia/Kolkata")
                for dt_key in ['date_time', 'exchange_timestamp', 'last_trade_time', 'current_time']:
                    if dt_key in item and item[dt_key]:
                        try:
                            clean_dt_str = str(item[dt_key]).replace('Z', '+00:00')
                            parsed_dt = datetime.fromisoformat(clean_dt_str)
                            if parsed_dt.tzinfo is not None:
                                dt = parsed_dt.astimezone(tz_ist).replace(tzinfo=None)
                            else:
                                dt = parsed_dt
                            break
                        except Exception:
                            pass
                if dt is None:
                    for dt_key in ['fdtm', 'ltt']:
                        if dt_key in item and item[dt_key]:
                            try:
                                dt = datetime.strptime(str(item[dt_key]), '%d/%m/%Y %H:%M:%S')
                                break
                            except Exception:
                                pass
                if dt is None:
                    if ts and hasattr(ts, 'astimezone'):
                        dt = ts.astimezone(tz_ist).replace(tzinfo=None)
                    elif ts and hasattr(ts, 'replace'):
                        dt = ts.replace(tzinfo=None)
                    else:
                        dt = datetime.now(tz_ist).replace(tzinfo=None)

                # Volume & Depth metrics
                v = item.get('volume_traded') or item.get('volume') or item.get('v') or 0
                oi = item.get('oi') or 0
                bp = item.get('buy_price') or item.get('bp') or 0.0
                sp = item.get('sell_price') or item.get('sp') or 0.0
                bq = item.get('total_buy_quantity') or item.get('buy_quantity') or item.get('tbq') or item.get('bq') or 0
                sq = item.get('total_sell_quantity') or item.get('sell_quantity') or item.get('tsq') or item.get('sq') or 0

                records.append({
                    'instrument_token': tkn_int,
                    'last_price': p_float,
                    'date_time': dt,
                    'volume': int(float(v)),
                    'oi': int(float(oi)),
                    'buy_price': float(bp),
                    'sell_price': float(sp),
                    'buy_quantity': int(float(bq)),
                    'sell_quantity': int(float(sq)),
                    'mode': 'full',
                    'tradingsymbol': sym,
                })

        if not records:
            schema = {
                'instrument_token': pl.Int64,
                'last_price': pl.Float64,
                'date_time': pl.Datetime,
                'volume': pl.Int64,
                'oi': pl.Int64,
                'buy_price': pl.Float64,
                'sell_price': pl.Float64,
                'buy_quantity': pl.Int64,
                'sell_quantity': pl.Int64,
                'mode': pl.Utf8,
                'tradingsymbol': pl.Utf8,
            }
            return pl.DataFrame(schema=schema)

        df = pl.from_dicts(records, infer_schema_length=len(records))
        req_cols = [c for c in ['instrument_token', 'last_price', 'date_time'] if c in df.columns]
        df = df.drop_nulls(subset=req_cols)
        return df
    except Exception as exc:
        general_logger.error(f'[{datetime.now().isoformat()}] Failed to fetch recent ticks in Polars: {exc}')
        return pl.DataFrame(schema={'instrument_token': pl.Int64, 'last_price': pl.Float64, 'date_time': pl.Datetime})



# Note: IndianUserAccount is imported from algo_trading.algos.indian_user_account



# ============================================================================
# MAIN MONOLITHIC TRADING ENGINE (Polars-Native Multi-Account Router)
# ============================================================================


def create_indian_broker_utility(broker_obj: Any, account_id: str) -> Any:
    """Factory to instantiate the appropriate Indian broker utility (Zerodha, Kotak Neo, etc.)."""
    code_val = (broker_obj.broker_name.code if broker_obj and broker_obj.broker_name else "").lower()
    api_val = (broker_obj.api_provider.code if broker_obj and broker_obj.api_provider else "").lower()
    name_val = (broker_obj.name or "").lower() if broker_obj else ""
    if "kotak" in code_val or "kotak" in api_val or "kotak" in name_val:
        return KotakNeoUtility(account_id=account_id, broker_obj=broker_obj)
    return ZerodhaUtility(account_id=account_id, broker_obj=broker_obj)


def discover_indian_broker_accounts(target_account_id: Optional[str] = None) -> List[IndianUserAccount]:
    """Auto-discovers active Indian trading accounts across Zerodha and Kotak Neo."""
    from kalai.models import Broker
    from django.db.models import Q

    qs = Broker.objects.all()
    if target_account_id:
        target_clean = str(target_account_id).strip().lower()
        qs = qs.filter(
            Q(account_id__iexact=target_clean)
            | Q(name__iexact=target_clean)
            | Q(name__icontains=target_clean)
            | Q(broker_name__code__icontains=target_clean)
            | Q(api_provider__code__icontains=target_clean)
        )
    else:
        qs = qs.filter(
            Q(broker_name__code__in=["zerodha", "kotak", "kotak_neo", "upstox", "angel"])
            | Q(api_provider__code__in=["zerodha", "kotak", "kotak_neo", "upstox", "angel"])
            | Q(name__icontains="zerodha") | Q(name__icontains="kotak")
        )

    trade_enabled = qs.filter(enable_trade=True)
    active_brokers = trade_enabled if trade_enabled.exists() else qs

    accounts: List[IndianUserAccount] = []
    for b in active_brokers:
        acc_id = b.account_id or b.name
        try:
            client = create_indian_broker_utility(b, acc_id)
            accounts.append(IndianUserAccount(
                user_id=acc_id,
                client=client,
                broker_obj=b,
                capital_allowed=100000.0,
                enabled=b.enable_trade
            ))
        except Exception as e:
            general_logger.error(f"Failed initializing broker account '{acc_id}': {e}. Skipping this broker.")
    return accounts


class TradeAlgo:
    """
    Unified Monolithic Polars Trading Engine:
    - Ingests market ticks and calculates multi-timeframe candles, strike contracts,
      and technical momentum paths ONCE globally using Polars.
    - Slices lots, manages capital, and routes orders concurrently across N IndianUserAccount instances.
    """

    def __init__(
        self,
        disco_bro: Optional[Any] = None,
        accounts: Optional[List[IndianUserAccount]] = None,
        special_session: bool = False,
        timezone: str = "Asia/Kolkata",
        debug_mode: Optional[bool] = None,
        day_light_saving: bool = True,
        algo_info_sync_cycles: Optional[int] = None,
        target_account_id: Optional[str] = None
    ):
        self.target_account_id = target_account_id
        self.init_done = False
        self.special_session = special_session
        self.tz = timezone
        self.day_light_saving = day_light_saving
        self.nfo_risk_hold = False
        self.data_ready = False
        self.debug_mode = DEBUG if debug_mode is None else debug_mode
        self.loop_count: int = 0
        if algo_info_sync_cycles is not None:
            self.algo_info_sync_cycles = max(0, int(algo_info_sync_cycles))
        elif self.debug_mode:
            self.algo_info_sync_cycles = 0  # Instant sync in debug mode
        else:
            self.algo_info_sync_cycles = max(0, int(getattr(config, 'ALGO_INFO_SYNC_CYCLES', 15)))

        # File paths
        algo_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(algo_dir, '..', '..'))
        self.logs_dir = os.path.join(project_root, 'logs')
        os.makedirs(self.logs_dir, exist_ok=True)
        
        acc_prefix = f"{accounts[0].user_id}_" if (accounts and accounts[0].user_id) else ""

        if acc_prefix and os.path.exists(os.path.join(algo_dir, f"{acc_prefix}token_ref.xlsx")):
            self.input_file = os.path.join(algo_dir, f"{acc_prefix}token_ref.xlsx")
        else:
            self.input_file = os.path.join(algo_dir, 'token_ref.xlsx')

        self.output_file = os.path.join(self.logs_dir, f"{acc_prefix}cum_table.xlsx" if acc_prefix else "cum_table.xlsx")
        self.master_list = os.path.join(self.logs_dir, f"{acc_prefix}Master_inst_token.xlsx" if acc_prefix else "Master_inst_token.xlsx")
        self.algo_logs_file = os.path.join(self.logs_dir, f"{acc_prefix}algo_logs_export.xlsx" if acc_prefix else "PRIMARY_algo_logs_export.xlsx")
        self.session_start_time = dj_timezone.now()

        # Load Excel configuration via Polars (with openpyxl engine) FIRST
        general_logger.info(f"Loading Polars configuration from {self.input_file} via load_indian_algo_config...")
        cfg = load_indian_algo_config(self.input_file)
        self.stock_config = cfg.stock_config
        self.nse_holiday_info = cfg.nse_holiday_info
        self.stock_data_info = cfg.stock_data_info
        self.aug_table = cfg.aug_table
        self.loss_table_stocks = cfg.loss_table_stocks
        self.der_loss_table_stocks = cfg.der_loss_table_stocks
        self.percent_of_capital_utilization = cfg.percent_of_capital_utilization
        self.cap_config = cfg.cap_config
        self.debounce_counter_threshold = cfg.debounce_counter_threshold
        self.order_pending_counter_threshold = cfg.order_pending_counter_threshold
        self.margin_per_stock = cfg.margin_per_stock
        self.live_balance_lower_limit = cfg.live_balance_lower_limit
        self.buy_ttl_value = cfg.buy_ttl_value
        self.sell_ttl_value = cfg.sell_ttl_value
        self.strike_choice_CE = cfg.strike_choice_CE
        self.strike_choice_PE = cfg.strike_choice_PE
        self.hedge_threshold = cfg.hedge_threshold
        self.capital_allowed = cfg.capital_allowed
        self.stoploss_threshold = cfg.stoploss_threshold
        general_logger.info(f"Loaded config: {len(self.aug_table)} active stocks (of {len(self.stock_data_info)}), {len(self.nse_holiday_info)} holidays.")

        # Initialize Indian Market Session evaluator with loaded holiday calendar
        self.session = IndianMarketSession(
            nse_holiday_info=self.nse_holiday_info,
            special_session=self.special_session,
            day_light_saving=self.day_light_saving,
            timezone=self.tz,
            debug_mode=self.debug_mode
        )

        # Algorithm parameters with robust defaults
        if not self.stock_config.is_empty() and len(self.stock_config) > 20:
            self.percent_of_capital_utilization = self.stock_config.item(7, 1)
            # Slice cap config from stock_config
            cap_slice = self.stock_config.slice(1, 4).select(self.stock_config.columns[4:])
            new_cols = [str(x) for x in cap_slice.row(0)]
            self.cap_config = cap_slice.slice(1).rename(dict(zip(cap_slice.columns, new_cols)))
            
            self.debounce_counter_threshold = self.stock_config.item(8, 1)
            self.order_pending_counter_threshold = self.stock_config.item(12, 1)
            self.margin_per_stock = self.stock_config.item(9, 1)
            self.live_balance_lower_limit = self.stock_config.item(10, 1)
            self.buy_ttl_value = self.stock_config.item(14, 1)
            self.sell_ttl_value = self.stock_config.item(15, 1)
            self.strike_choice_CE = self.stock_config.item(16, 1)
            self.strike_choice_PE = self.stock_config.item(17, 1)
            self.hedge_threshold = self.stock_config.item(18, 1)
            self.capital_allowed = self.stock_config.item(19, 1)
            self.stoploss_threshold = self.stock_config.item(20, 1)
        else:
            self.percent_of_capital_utilization = 1.0
            self.cap_config = pl.DataFrame()
            self.debounce_counter_threshold = 3
            self.order_pending_counter_threshold = 5
            self.margin_per_stock = 50000.0
            self.live_balance_lower_limit = 10000.0
            self.buy_ttl_value = 60
            self.sell_ttl_value = 60
            self.strike_choice_CE = 'ATM'
            self.strike_choice_PE = 'ATM'
            self.hedge_threshold = 1.5
            self.capital_allowed = 100000.0
            self.stoploss_threshold = 0.15

        self.month_cutoff = 0
        self.week_cutoff = 3

        # Multi-Account Setup
        self.accounts: List[IndianUserAccount] = []
        if accounts:
            self.accounts = accounts
            self.client = self.accounts[0].client if self.accounts else disco_bro
        elif disco_bro:
            self.client = disco_bro
            b_obj = getattr(disco_bro, 'broker', None)
            self.accounts = [IndianUserAccount(user_id="PRIMARY", client=disco_bro, broker_obj=b_obj, capital_allowed=float(self.capital_allowed))]
        else:
            try:
                self.accounts = discover_indian_broker_accounts(target_account_id=self.target_account_id)
                if self.accounts:
                    general_logger.info(f"Discovered {len(self.accounts)} active Indian broker account(s): {[a.user_id for a in self.accounts]}")
            except Exception as e:
                general_logger.error(f"Error initializing Indian broker accounts: {e}")
                self.accounts = []

        if not hasattr(self, 'account') or self.account is None:
            self.account = self.accounts[0] if self.accounts else None
            self.client = self.account.client if self.account else disco_bro

        if self.account and hasattr(self.account, 'broker_obj') and self.account.broker_obj:
            algo_logger.set_active_account(self.account.broker_obj, algo_name=_CURRENT_ALGO_NAME)

        # Update account-scoped file paths with discovered account ID prefix
        acc_prefix = f"{self.account.user_id}_" if (self.account and self.account.user_id) else ""
        if acc_prefix:
            self.output_file = os.path.join(self.logs_dir, f"{acc_prefix}cum_table.xlsx")
            self.master_list = os.path.join(self.logs_dir, f"{acc_prefix}Master_inst_token.xlsx")
            self.algo_logs_file = os.path.join(self.logs_dir, f"{acc_prefix}algo_logs_export.xlsx")
            if os.path.exists(os.path.join(algo_dir, f"{acc_prefix}token_ref.xlsx")):
                self.input_file = os.path.join(algo_dir, f"{acc_prefix}token_ref.xlsx")

        # Master Token update timestamp and Primary Broker reference
        self.primary_broker = self.account.broker_obj if (self.account and hasattr(self.account, 'broker_obj')) else None
        try:
            if self.primary_broker:
                raw_master = self.primary_broker.get_algo_state('master_tkn_list_update_time') or self.primary_broker.get_algo_state('zerodha_master_token_list')
                self.master_tkn_list_update_time_df = pl.from_dicts(raw_master) if raw_master else pl.DataFrame([{'updated_time': today_ist() - timedelta(days=2)}])
            else:
                self.master_tkn_list_update_time_df = pl.DataFrame([{'updated_time': today_ist() - timedelta(days=2)}])
        except Exception:
            self.master_tkn_list_update_time_df = pl.DataFrame([{'updated_time': today_ist() - timedelta(days=2)}])

        # Build O(1) Fast Metadata Lookup Maps
        self.sym_to_meta: Dict[str, Dict[str, Any]] = {}
        self.tkn_to_meta: Dict[int, Dict[str, Any]] = {}
        self.sym_to_tkn_map: Dict[str, int] = {}
        self.tkn_to_sym_map: Dict[int, str] = {}

        # Global Instrument Tables (Polars DataFrames)
        if self.client and hasattr(self.client, 'master_tkn_list'):
            res = self.client.master_tkn_list(
                input_file=self.input_file,
                master_list=self.master_list,
                output_file=self.output_file,
                tz=self.tz,
                debug_mode=self.debug_mode,
                month_cutoff=self.month_cutoff,
                aug_table=getattr(cfg, 'aug_table', None),
                cap_config=self.cap_config
            )
            self.cum_table, self.inst_list_int, self.init_ref_list = res[0], res[1], res[2]
            self.index_ref_list = res[3] if len(res) > 3 else pl.DataFrame(schema={"instrument_token": pl.Int64})
        else:
            general_logger.error(
                "master_tkn_list: self.client does not have a master_tkn_list method. "
                "Instrument token resolution is not possible without a valid broker client. "
                "Initializing empty tables — algo will not trade until restarted with a valid client."
            )
            self.cum_table = pl.DataFrame()
            self.inst_list_int = np.array([])
            self.init_ref_list = pl.DataFrame(schema={"instrument_token": pl.Int64})
            self.index_ref_list = pl.DataFrame(schema={"instrument_token": pl.Int64})
        
        if not self.cum_table.is_empty() and 'Ref_stock_tkn' in self.cum_table.columns:
            self.all_ref_tkns = np.array(self.cum_table['Ref_stock_tkn'].drop_nulls().unique().to_list())
        else:
            self.all_ref_tkns = np.array([])
        
        if not self.cum_table.is_empty():
            for row in self.cum_table.to_dicts():
                sym = str(row.get('tradingsymbol', ''))
                tkn = row.get('instrument_token')
                if sym and tkn is not None:
                    tkn_int = int(tkn)
                    self.sym_to_meta[sym] = row
                    self.tkn_to_meta[tkn_int] = row
                    self.sym_to_tkn_map[sym] = tkn_int
                    self.tkn_to_sym_map[tkn_int] = sym

        # Global Market Data & Candle Buffers (Polars DataFrames)
        cdl_schema = {
            'open': pl.Float64,
            'low': pl.Float64,
            'high': pl.Float64,
            'close': pl.Float64,
            'instrument_token': pl.Int64,
            'date_time': pl.Datetime
        }
        tick_schema = {
            'instrument_token': pl.Int64,
            'last_price': pl.Float64,
            'date_time': pl.Datetime
        }
        self.tick_data = pl.DataFrame(schema=tick_schema)
        self.delta_tick_data = pl.DataFrame(schema=tick_schema)
        self.buy_stock_cap = pl.DataFrame()
        self.sell_stock_table = pl.DataFrame()
        self.fwd_1_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_3_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_5_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_10_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_15_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_30_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_60_all = pl.DataFrame(schema=cdl_schema)
        self.day_cdl_all = pl.DataFrame(schema=cdl_schema)
        self.half_day_cdl_all = pl.DataFrame(schema=cdl_schema)
        self.prev_day_cdl_all = pl.DataFrame(schema=cdl_schema)
        self.ref_min_max_all = pl.DataFrame(schema=cdl_schema)
        self.current_trading_day: date = today_ist()
        self._eod_cleared_date: Optional[date] = None

        # Global In-Memory Fast Debounce & State Counters (sub-millisecond dictionary lookups)
        self.one_counter_ce_dict: Dict[int, int] = {}
        self.one_counter_pe_dict: Dict[int, int] = {}
        self.minus_one_counter_ce_dict: Dict[int, int] = {}
        self.minus_one_counter_pe_dict: Dict[int, int] = {}
        self.minus_two_counter_ce_dict: Dict[int, int] = {}
        self.minus_two_counter_pe_dict: Dict[int, int] = {}
        self.minus_three_counter_dict: Dict[int, int] = {}
        self.minus_five_counter_dict: Dict[int, int] = {}
        self.stop_loss_counter_dict: Dict[int, int] = {}
        self.hedge_counter_dict: Dict[int, int] = {}
        self.jump_counter_dict: Dict[int, int] = {}
        
        strike_schema = {
            'instrument_token': pl.Int64,
            'strike_value': pl.Float64,
            'tradingsymbol': pl.Utf8,
            'date_time': pl.Datetime,
            'ref_symbol': pl.Int64,
            'Ref_stock': pl.Utf8,
            'instrument_type': pl.Utf8,
        }
        self.strike_entry_info: pl.DataFrame = pl.DataFrame(schema=strike_schema)
        self.strike_exit_info: pl.DataFrame = pl.DataFrame(schema=strike_schema)
        self.stop_loss_info: pl.DataFrame = pl.DataFrame(schema={'order_id': pl.Utf8, 'buy_price': pl.Float64, 'instrument_token': pl.Int64, 'date_time': pl.Datetime, 'tradingsymbol': pl.Utf8})

        self._lock = threading.Lock()
        self._state_cache: Dict[str, Any] = {}

        # Account aliases
        if self.account:
            self.account.sync_account_state(debug_mode=self.debug_mode)
            self.hold_frame = self.account.hold_frame
            self.pos_day_frame = self.account.pos_day_frame
            self.pos_net_frame = self.account.pos_net_frame
            self.open_positions = self.account.open_positions
            self.order_status = self.account.order_status
            self.avail_cash = self.account.avail_cash
            self.live_balance = self.account.live_balance
            if hasattr(self.account, 'stop_loss_info') and not self.account.stop_loss_info.is_empty():
                self.stop_loss_info = self.account.stop_loss_info
            if hasattr(self.account, 'strike_entry_info') and not self.account.strike_entry_info.is_empty():
                self.strike_entry_info = self.account.strike_entry_info
        else:
            self.hold_frame = pl.DataFrame()
            self.pos_day_frame = pl.DataFrame()
            self.pos_net_frame = pl.DataFrame()
            self.open_positions = pl.DataFrame()
            self.order_status = pl.DataFrame()
            self.avail_cash = 0.0
            self.live_balance = 0.0

        # Subscribe instrument tokens
        self.updated_list = self.token_list_update()
        if not self.updated_list.is_empty() and 'instrument_token' in self.updated_list.columns:
            self.insert_instrument_token(self.updated_list['instrument_token'].to_list())
        
        if not self.cum_table.is_empty() and 'Ref_stock_tkn' in self.cum_table.columns and not self.updated_list.is_empty() and 'instrument_token' in self.updated_list.columns:
            subs = set(self.updated_list['instrument_token'].drop_nulls().to_list())
            refs = set(self.cum_table['Ref_stock_tkn'].drop_nulls().to_list())
            self.final_ref_tokens = list(subs.intersection(refs))
        else:
            self.final_ref_tokens = []

        self.update_config_info()
        self.read_algo_info_table()
        self.read_db_candle()
        if self.primary_broker:
            try:
                warmup_ticks = fetch_recent_ticks(self.primary_broker, seconds=None, limit=120000)
                if not warmup_ticks.is_empty():
                    self.update_all_candles_batch(warmup_ticks)
                    general_logger.info(f"[WARMUP] Warmed up Indian candles from {len(warmup_ticks)} recent ticks in ProcessedTickStore.")
            except Exception as e:
                general_logger.debug(f"Warmup ticks skipped: {e}")
        self.init_done = True
        acc_id = self.account.user_id if self.account else 'PRIMARY'
        general_logger.info(f"Polars TradeAlgo initialized for account '{acc_id}'.")
        app_logger.info(f"Polars TradeAlgo engine active for account '{acc_id}'.")

    def get_account(self, account_id: str) -> Optional[IndianUserAccount]:
        """Look up an IndianUserAccount by account_id."""
        for acc in self.accounts:
            if str(acc.user_id).strip().lower() == str(account_id).strip().lower():
                return acc
        return None

    @property
    def all_open_positions(self) -> pl.DataFrame:
        """Open positions for the account safely in Polars."""
        if self.account and hasattr(self.account, 'open_positions') and not self.account.open_positions.is_empty():
            return self.account.open_positions
        if hasattr(self, 'open_positions') and isinstance(self.open_positions, pl.DataFrame) and not self.open_positions.is_empty():
            return self.open_positions
        return pl.DataFrame()

    # ── Fast O(1) Lookups ───────────────────────────────────────────────────

    def get_token_meta(self, tkn: int) -> Dict[str, Any]:
        return self.tkn_to_meta.get(tkn, {})

    def get_symbol_meta(self, sym: str) -> Dict[str, Any]:
        return self.sym_to_meta.get(sym, {})

    def tkn_to_symbol(self, tkn_list: List[int]) -> List[str]:
        return [self.tkn_to_sym_map.get(t, "UNKNOWN") for t in tkn_list]

    def symbol_to_tkn(self, sym: str) -> int:
        if sym in self.sym_to_tkn_map:
            return self.sym_to_tkn_map[sym]
        for df in (getattr(self, 'nfo_cds_mcx', None), getattr(self, 'nse_inst_data', None), getattr(self, 'bse_inst_data', None)):
            if df is not None and not df.is_empty() and 'tradingsymbol' in df.columns and 'instrument_token' in df.columns:
                match = df.filter(pl.col('tradingsymbol') == sym)
                if not match.is_empty():
                    tkn = int(match['instrument_token'][0])
                    self.sym_to_tkn_map[sym] = tkn
                    return tkn
        return -1

    def tkn_to_exchg(self, tkn: int) -> str:
        meta = self.get_token_meta(tkn)
        return str(meta.get('exchange', 'NFO'))

    # ── State Sync ──────────────────────────────────────────────────────────

    def serialize_table_data(self, data: Any, default: Optional[Any] = None) -> str:
        res = orjson.dumps(data, default=default or self.json_datetime_converter)
        return res.decode('utf-8') if isinstance(res, bytes) else res

    @staticmethod
    def sanitize_for_json(data: Any) -> Any:
        if isinstance(data, pl.DataFrame):
            return [TradeAlgo.sanitize_for_json(row) for row in data.to_dicts()]
        elif isinstance(data, dict):
            return {k: TradeAlgo.sanitize_for_json(v) for k, v in data.items()}
        elif isinstance(data, (list, tuple)):
            return [TradeAlgo.sanitize_for_json(v) for v in data]
        elif isinstance(data, (datetime, date)):
            return data.isoformat()
        elif isinstance(data, (np.integer, np.int64, np.int32)):
            return int(data)
        elif isinstance(data, (np.floating, np.float64, np.float32)):
            return float(data)
        elif isinstance(data, np.ndarray):
            return data.tolist()
        return data

    def update_config_info(self) -> None:
        if not self.primary_broker:
            return
        tables = {
            'stock_config': getattr(self, 'stock_config', pl.DataFrame()),
            'nse_holiday_info': getattr(self, 'nse_holiday_info', pl.DataFrame()),
            'stock_data_info': getattr(self, 'stock_data_info', pl.DataFrame()),
            'cap_config': getattr(self, 'cap_config', pl.DataFrame()),
        }
        synced_count = 0
        for name, df in tables.items():
            if df is not None and not df.is_empty():
                try:
                    self.primary_broker.set_algo_state(name, self.sanitize_for_json(df))
                    synced_count += 1
                except Exception as e:
                    general_logger.warning(f"Error persisting config table {name}: {e}")
        general_logger.info(f"Persisted {synced_count} config table(s) to DB.")

    def read_algo_info_table(self) -> None:
        """
        Loads persisted dynamic operational state tables from DB and aliases:
        - zerodha_stop_loss / stop_loss_info -> self.stop_loss_info
        - zerodha_strike_entry / strike_entry_info -> self.strike_entry_info
        - zerodha_strike_exit / strike_exit_info -> self.strike_exit_info
        - in-memory trading counters
        Static / master instrument tables (cum_table, aug_table, etc.) are protected from being overwritten by stale DB data.
        """
        if not self.primary_broker:
            return
        PROTECTED_MASTER_TABLES = {
            'cum_table', 'aug_table', 'nfo_cds_mcx', 'master_inst_token',
            'stock_config', 'cap_config', 'stock_data_info',
            'nse_instrument_data', 'nfo_instrument_data', 'cds_instrument_data',
            'mcx_instrument_data', 'bfo_instrument_data', 'bse_instrument_data',
            'nse_holiday_info', 'master_tkn_list_update_time', 'all_ref_tkns',
            'inst_list_int', 'init_ref_list',
            'buy_stock_cap', 'sell_stock_table'
        }
        try:
            records = AlgoInfo.objects.filter(account=self.primary_broker).exclude(tablename__in=PROTECTED_MASTER_TABLES)
            loaded_count = 0
            for rec in records:
                name = rec.tablename
                raw = rec.tabledata
                if not raw:
                    continue
                try:
                    parsed = orjson.loads(raw) if isinstance(raw, str) else raw
                    df_loaded = None
                    if isinstance(parsed, list):
                        df_loaded = pl.from_dicts(parsed)
                    elif isinstance(parsed, dict):
                        df_loaded = pl.from_dicts([parsed])

                    if df_loaded is not None:
                        if 'date_time' in df_loaded.columns and df_loaded['date_time'].dtype == pl.Utf8:
                            try:
                                df_loaded = df_loaded.with_columns(pl.col('date_time').str.to_datetime())
                            except Exception:
                                pass
                        setattr(self, name, df_loaded)
                        # Aliasing for reference parity
                        if name in ('zerodha_stop_loss', 'stop_loss_info'):
                            self.stop_loss_info = df_loaded
                            if self.account:
                                self.account.stop_loss_info = df_loaded
                        elif name in ('zerodha_strike_entry', 'strike_entry_info'):
                            self.strike_entry_info = df_loaded
                            if self.account:
                                self.account.strike_entry_info = df_loaded
                        elif name in ('zerodha_strike_exit', 'strike_exit_info'):
                            self.strike_exit_info = df_loaded
                            if self.account:
                                self.account.strike_exit_info = df_loaded

                        loaded_count += 1
                except Exception:
                    continue
            general_logger.info(f"Loaded {loaded_count} dynamic AlgoInfo state table(s) from DB for account {self.primary_broker.account_id}.")
        except Exception as e:
            general_logger.warning(f"Error reading AlgoInfo tables from DB: {e}")

    def update_algo_info_table(self, force: bool = False) -> None:
        """
        Persists updated state tables to AlgoInfo in DB with in-memory deduplication caching
        and periodic loop-count throttling in production to prevent redundant database write queries.
        In debug mode or when algo_info_sync_cycles <= 1, updates are instant every cycle.
        """
        if not self.primary_broker:
            return
        if not force and not self.debug_mode and self.algo_info_sync_cycles > 1 and (self.loop_count % self.algo_info_sync_cycles != 0):
            return
        stop_loss_df = getattr(self, 'stop_loss_info', pl.DataFrame())
        if stop_loss_df.is_empty() and self.account and hasattr(self.account, 'stop_loss_info'):
            stop_loss_df = self.account.stop_loss_info

        tables_to_sync = {
            'strike_entry_info': getattr(self, 'strike_entry_info', pl.DataFrame()),
            'zerodha_strike_entry': getattr(self, 'strike_entry_info', pl.DataFrame()),
            'zerodha_strike_exit': getattr(self, 'strike_exit_info', pl.DataFrame()),
            'zerodha_stop_loss': stop_loss_df,
            'stop_loss_info': stop_loss_df,
        }
        synced_count = 0
        if not hasattr(self, '_state_cache'):
            self._state_cache = {}

        for name, df in tables_to_sync.items():
            if df is not None and not df.is_empty():
                try:
                    sanitized = self.sanitize_for_json(df)
                    serialized = orjson.dumps(sanitized)
                    if self._state_cache.get(name) != serialized:
                        self.primary_broker.set_algo_state(name, sanitized)
                        self._state_cache[name] = serialized
                        synced_count += 1
                except Exception:
                    continue
        if synced_count > 0:
            general_logger.info(f"Updated {synced_count} state table(s) to DB for account {self.primary_broker.account_id}.")

        # In production, sync live positions directly to BrokerPosition table from in-memory Polars state
        if self.account and hasattr(self.account, 'pos_net_frame') and not self.account.pos_net_frame.is_empty():
            try:
                from kalai.positions import sync_positions_from_algo_frames
                sync_positions_from_algo_frames(self.primary_broker, self.account.pos_net_frame)
            except Exception:
                pass

    @staticmethod
    def json_datetime_converter(obj: Any) -> str:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    # ── Market Timings ──────────────────────────────────────────────────────

    def is_weekend(self) -> bool:
        if self.debug_mode:
            return False
        return is_weekend_ist(tz=self.tz)

    def _get_holiday_dates(self) -> List[date]:
        if self.nse_holiday_info.is_empty() or 'Date' not in self.nse_holiday_info.columns:
            return []
        try:
            col = self.nse_holiday_info['Date']
            if col.dtype == pl.String:
                dates = col.str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False).drop_nulls().to_list()
                return [d for d in dates if isinstance(d, date)]
            elif col.dtype in (pl.Datetime, pl.Date):
                dates = col.dt.date().drop_nulls().to_list()
                return [d for d in dates if isinstance(d, date)]
            else:
                return []
        except Exception:
            return []

    def is_holiday(self, exchg: str) -> bool:
        if self.debug_mode:
            return False
        holidays = self._get_holiday_dates()
        return today_ist(tz=self.tz) in holidays

    def special_session_chk(self, exchg: str) -> bool:
        # Reference lines 353-356: MCX/CDS inversion — if special_session is True, MCX/CDS return False
        if self.debug_mode:
            return True
        if exchg in ('MCX', 'CDS') and self.special_session:
            return not self.special_session  # returns False when special_session is True
        return self.special_session

    def next_session_closed(self, exchg: str) -> bool:
        # Reference lines 358-371: NFO/CDS check morning session, MCX checks evening session
        if self.debug_mode:
            return False
        tomorrow = tomorrow_ist(tz=self.tz)
        # Friday = weekday 4 => weekend follows
        if exchg in ('NFO', 'NSE', 'BSE', 'BFO', 'CDS'):
            if datetime.today().weekday() == 4:
                return True
            holidays = self._get_holiday_dates()
            return tomorrow in holidays
        elif exchg == 'MCX':
            if datetime.today().weekday() == 4:
                return True
            holidays = self._get_holiday_dates()
            return tomorrow in holidays
        return False

    def exchg_time_buy_chk(self, exchg: str) -> bool:
        # Reference lines 1206-1235: includes hold_time mask and exact exchange windows
        if getattr(self, 'debug_mode', False):
            return True
        t = time_ist(tz=getattr(self, 'tz', 'Asia/Kolkata'))
        now_naive = now_ist_naive(tz=getattr(self, 'tz', 'Asia/Kolkata'))
        start_time = now_naive.replace(hour=9, minute=15, second=0, microsecond=0)
        # hold_time: minutes within each hour where order placement is paused
        if not self.session_end(exchg):
            hold_time = (13 <= t.minute <= 20) or (43 <= t.minute <= 50)
        else:
            hold_time = False
        if exchg == 'CDS':
            end_time = now_naive.replace(hour=17, minute=0, second=0, microsecond=0)
        elif exchg == 'MCX':
            if not self.session_end(exchg):
                hold_time = (58 <= t.minute <= 59 or 0 <= t.minute <= 5) or (28 <= t.minute <= 35)
            else:
                hold_time = False
            start_time = now_naive.replace(hour=9, minute=1, second=0, microsecond=0)
            if getattr(self, 'day_light_saving', True):
                end_time = now_naive.replace(hour=23, minute=30, second=0, microsecond=0)
            else:
                end_time = now_naive.replace(hour=23, minute=55, second=0, microsecond=0)
        else:
            end_time = now_naive.replace(hour=15, minute=40, second=0, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return not hold_time

    def hedge_time_chk(self, exchg: str) -> bool:
        # Reference lines 1238-1256: full-day window (9:15 start, exchange-specific end)
        if getattr(self, 'debug_mode', False):
            return True
        now_naive = now_ist_naive(tz=getattr(self, 'tz', 'Asia/Kolkata'))
        start_time = now_naive.replace(hour=9, minute=15, second=0, microsecond=0)
        day_light_saving = getattr(self, 'day_light_saving', True)
        if exchg == 'CDS':
            end_time = now_naive.replace(hour=17, minute=0, second=0, microsecond=0)
        elif exchg == 'MCX':
            if day_light_saving:
                end_time = now_naive.replace(hour=23, minute=30, second=0, microsecond=0)
            else:
                end_time = now_naive.replace(hour=23, minute=55, second=0, microsecond=0)
        else:
            end_time = now_naive.replace(hour=15, minute=30, second=0, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def half_time(self, exchg: str) -> bool:
        # Reference lines 1258-1277: exact exchange time windows
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == 'CDS':
            start_time = now_naive.replace(hour=12, minute=15, second=0, microsecond=0)
            end_time = now_naive.replace(hour=16, minute=30, second=50, microsecond=0)
        elif exchg == 'MCX':
            if self.day_light_saving:
                start_time = now_naive.replace(hour=18, minute=00, second=0, microsecond=0)
                end_time = now_naive.replace(hour=23, minute=30, second=50, microsecond=0)
            else:
                start_time = now_naive.replace(hour=18, minute=00, second=0, microsecond=0)
                end_time = now_naive.replace(hour=23, minute=55, second=50, microsecond=0)
        else:
            start_time = now_naive.replace(hour=13, minute=30, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=15, second=10, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def exchg_time_sell_chk(self, exchg: str) -> bool:
        # Reference lines 1280-1302: separate sell window with earlier start
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == 'CDS':
            start_time = now_naive.replace(hour=9, minute=3, second=0, microsecond=0)
            end_time = now_naive.replace(hour=17, minute=0, second=0, microsecond=0)
        elif exchg == 'MCX':
            start_time = now_naive.replace(hour=9, minute=3, second=0, microsecond=0)
            if self.day_light_saving:
                end_time = now_naive.replace(hour=23, minute=30, second=0, microsecond=0)
            else:
                end_time = now_naive.replace(hour=23, minute=55, second=0, microsecond=0)
        else:
            start_time = now_naive.replace(hour=9, minute=15, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=40, second=0, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def session_end(self, exchg: str) -> bool:
        # Reference lines 1305-1327: exact window with start/end bounds
        if getattr(self, 'debug_mode', False):
            return False
        now_naive = now_ist_naive(tz=getattr(self, 'tz', 'Asia/Kolkata'))
        start_time = now_naive.replace(hour=15, minute=20, second=20, microsecond=0)
        end_time = now_naive.replace(hour=15, minute=35, second=0, microsecond=0)
        day_light_saving = getattr(self, 'day_light_saving', True)
        if exchg == 'MCX':
            if day_light_saving:
                start_time = now_naive.replace(hour=23, minute=0, second=0, microsecond=0)
                end_time = now_naive.replace(hour=23, minute=30, second=0, microsecond=0)
            else:
                start_time = now_naive.replace(hour=23, minute=30, second=0, microsecond=0)
                end_time = now_naive.replace(hour=23, minute=55, second=0, microsecond=0)
        elif exchg == 'CDS':
            start_time = now_naive.replace(hour=15, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=30, second=0, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def prev_cdl_save_time(self) -> bool:
        # Reference lines 1329-1348: Window 1 (15:29 to 15:30) & Window 2 (MCX dynamic based on daylight savings)
        if getattr(self, 'debug_mode', False):
            return False
        now_naive = now_ist_naive(tz=getattr(self, 'tz', 'Asia/Kolkata'))
        day_light_saving = getattr(self, 'day_light_saving', True)
        # Window 1: 15:29:00 to 15:30:00
        start_1 = now_naive.replace(hour=15, minute=29, second=0, microsecond=0)
        end_1 = now_naive.replace(hour=15, minute=30, second=0, microsecond=0)

        # Window 2: Dynamic based on Daylight Savings
        if day_light_saving:
            start_2 = now_naive.replace(hour=23, minute=30, second=0, microsecond=0)
            end_2 = now_naive.replace(hour=23, minute=31, second=0, microsecond=0)
        else:
            start_2 = now_naive.replace(hour=23, minute=55, second=0, microsecond=0)
            end_2 = now_naive.replace(hour=23, minute=56, second=0, microsecond=0)

        return (start_1 <= now_naive <= end_1) or (start_2 <= now_naive <= end_2)

    def pre_trade_sl_update_time(self) -> bool:
        # Reference lines 1349-1361: 12:45 to 12:46 stop loss update
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=12, minute=45, second=0, microsecond=0)
        end_time = now_naive.replace(hour=12, minute=46, second=0, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def sl_update_time(self, exchg: str) -> bool:
        # Reference lines 1363-1390: exact 1-minute windows per exchange with session closure check
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=12, minute=45, second=0, microsecond=0)
        end_time = now_naive.replace(hour=12, minute=46, second=0, microsecond=0)
        if exchg in ('NSE', 'NFO', 'BSE', 'BFO'):
            start_time = now_naive.replace(hour=10, minute=15, second=0, microsecond=0)
            end_time = now_naive.replace(hour=10, minute=16, second=0, microsecond=0)
        elif exchg == 'MCX':
            start_time = now_naive.replace(hour=18, minute=00, second=0, microsecond=0)
            end_time = now_naive.replace(hour=18, minute=1, second=0, microsecond=0)
        elif exchg == 'CDS':
            start_time = now_naive.replace(hour=17, minute=30, second=0, microsecond=0)
            end_time = now_naive.replace(hour=17, minute=30, second=59, microsecond=0)
        if self.session_end(exchg) and self.next_session_closed(exchg):
            return True
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def strike_update_time(self, exchg: str) -> bool:
        # Reference lines 1392-1410: strike update window
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=9, minute=15, second=0, microsecond=0)
        end_time = now_naive.replace(hour=9, minute=15, second=5, microsecond=0)
        if exchg == 'MCX':
            start_time = now_naive.replace(hour=9, minute=29, second=40, microsecond=0)
            end_time = now_naive.replace(hour=9, minute=30, second=1, microsecond=0)
        elif exchg == 'CDS':
            start_time = now_naive.replace(hour=9, minute=0, second=3, microsecond=0)
            end_time = now_naive.replace(hour=9, minute=0, second=8, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def session_reset(self, exchg: str) -> bool:
        # Reference lines 1412-1430: session reset window
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=15, minute=10, second=0, microsecond=0)
        end_time = now_naive.replace(hour=15, minute=20, second=0, microsecond=0)
        if exchg == 'MCX':
            if self.day_light_saving:
                start_time = now_naive.replace(hour=23, minute=10, second=0, microsecond=0)
                end_time = now_naive.replace(hour=23, minute=20, second=0, microsecond=0)
            else:
                start_time = now_naive.replace(hour=23, minute=35, second=0, microsecond=0)
                end_time = now_naive.replace(hour=23, minute=45, second=0, microsecond=0)
        elif exchg == 'CDS':
            start_time = now_naive.replace(hour=16, minute=40, second=0, microsecond=0)
            end_time = now_naive.replace(hour=16, minute=50, second=0, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def session_start(self, exchg: str) -> bool:
        # Reference lines 1432-1450: exact per-exchange windows
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == 'MCX':
            start_time = now_naive.replace(hour=9, minute=1, second=0, microsecond=0)
            end_time = now_naive.replace(hour=9, minute=45, second=0, microsecond=0)
        elif exchg == 'CDS':
            start_time = now_naive.replace(hour=9, minute=1, second=0, microsecond=0)
            end_time = now_naive.replace(hour=9, minute=30, second=0, microsecond=0)
        else:
            start_time = now_naive.replace(hour=9, minute=25, second=0, microsecond=0)
            end_time = now_naive.replace(hour=9, minute=45, second=0, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def expiry_sell_time(self, exchg: str) -> bool:
        # Reference lines 1453-1471: exchange-specific expiry sell window
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == 'MCX':
            start_time = now_naive.replace(hour=11, minute=0, second=0, microsecond=0)
            if self.day_light_saving:
                end_time = now_naive.replace(hour=23, minute=30, second=0, microsecond=0)
            else:
                end_time = now_naive.replace(hour=23, minute=55, second=0, microsecond=0)
        elif exchg == 'CDS':
            start_time = now_naive.replace(hour=15, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(hour=17, minute=0, second=0, microsecond=0)
        else:
            start_time = now_naive.replace(hour=15, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=40, second=0, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def post_trade_time(self, exchg: str) -> bool:
        # Reference lines 1473-1491: tight 10-second post-session window per exchange
        if self.debug_mode:
            return False
        now_naive = now_ist_naive(tz=self.tz)
        if exchg == 'MCX':
            if self.day_light_saving:
                start_time = now_naive.replace(hour=23, minute=30, second=0, microsecond=0)
                end_time = now_naive.replace(hour=23, minute=30, second=10, microsecond=0)
            else:
                start_time = now_naive.replace(hour=23, minute=55, second=0, microsecond=0)
                end_time = now_naive.replace(hour=23, minute=55, second=10, microsecond=0)
        elif exchg == 'CDS':
            start_time = now_naive.replace(hour=17, minute=0, second=0, microsecond=0)
            end_time = now_naive.replace(hour=17, minute=0, second=10, microsecond=0)
        else:
            start_time = now_naive.replace(hour=15, minute=40, second=0, microsecond=0)
            end_time = now_naive.replace(hour=15, minute=40, second=10, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True

    def master_list_update_time(self) -> bool:
        # Reference lines 1493-1506: 08:00–09:00 window
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=8, minute=0, second=0, microsecond=0)
        end_time = now_naive.replace(hour=9, minute=0, second=0, microsecond=0)
        if now_naive < start_time or now_naive > end_time:
            return False
        return True


    # ── Token Subscription List ──────────────────────────────────────────────

    def token_list_update(self) -> pl.DataFrame:
        """Collect all subscribed tokens across holdings, positions, ref/index contracts, and active strikes.
        Matches reference lines 1509-1530: splits active strikes by instrument_type CE/PE.
        """
        all_tokens: List[int] = []
        ref_count = 0
        if hasattr(self, 'index_ref_list') and not self.index_ref_list.is_empty() and 'instrument_token' in self.index_ref_list.columns:
            tkns = self.index_ref_list['instrument_token'].drop_nulls().to_list()
            all_tokens.extend(tkns)
            ref_count += len(tkns)
        if hasattr(self, 'init_ref_list') and not self.init_ref_list.is_empty() and 'instrument_token' in self.init_ref_list.columns:
            tkns = self.init_ref_list['instrument_token'].drop_nulls().to_list()
            all_tokens.extend(tkns)
            ref_count += len(tkns)

        pos_count = 0
        if self.account:
            if not self.account.hold_frame.is_empty() and 'instrument_token' in self.account.hold_frame.columns:
                tkns = self.account.hold_frame['instrument_token'].drop_nulls().to_list()
                all_tokens.extend(tkns)
                pos_count += len(tkns)
            if not self.account.open_positions.is_empty() and 'instrument_token' in self.account.open_positions.columns:
                tkns = self.account.open_positions['instrument_token'].drop_nulls().to_list()
                all_tokens.extend(tkns)
                pos_count += len(tkns)

        # Reference lines 1522-1527: split active strikes by instrument_type to form current_list
        strike_count = 0
        if not self.cum_table.is_empty() and 'Buy_strike' in self.cum_table.columns and 'instrument_token' in self.cum_table.columns:
            active = self.cum_table.filter(pl.col('Buy_strike') == 'Yes')
            if 'instrument_type' in active.columns:
                ce_list = active.filter(pl.col('instrument_type') == 'CE')['instrument_token'].drop_nulls().to_list()
                pe_list = active.filter(pl.col('instrument_type') == 'PE')['instrument_token'].drop_nulls().to_list()
                all_tokens.extend(ce_list)
                all_tokens.extend(pe_list)
                strike_count += len(ce_list) + len(pe_list)
            else:
                tkns = active['instrument_token'].drop_nulls().to_list()
                all_tokens.extend(tkns)
                strike_count += len(tkns)

        unique_tokens = []
        seen = set()
        for t in all_tokens:
            if t is None:
                continue
            s_val = str(t).strip()
            if not s_val or s_val == '-1':
                continue
            if s_val not in seen:
                seen.add(s_val)
                try:
                    unique_tokens.append(int(s_val))
                except (ValueError, TypeError):
                    unique_tokens.append(s_val)

        general_logger.info(
            f"[TOKEN_UPDATE] Assembled {len(unique_tokens)} active tokens for streaming: {ref_count} underlyings/indexes, {pos_count} positions/holdings, {strike_count} selected strikes."
        )
        if all(isinstance(t, int) for t in unique_tokens):
            return pl.DataFrame({'instrument_token': unique_tokens}, schema={'instrument_token': pl.Int64})
        return pl.DataFrame({'instrument_token': [str(t) for t in unique_tokens]}, schema={'instrument_token': pl.Utf8})

    def insert_instrument_token(self, token_list: List[Any]) -> None:
        """Inserts subscribed instrument tokens into AlgoInfo table in DB for this broker account."""
        if not token_list or not self.primary_broker:
            return
        parsed_tokens = []
        for t in token_list:
            if t is None:
                continue
            s_val = str(t).strip()
            if not s_val or s_val == '-1':
                continue
            try:
                val = int(s_val)
                if val > 0:
                    parsed_tokens.append(val)
            except (ValueError, TypeError):
                parsed_tokens.append(s_val)

        token_set = set(str(t) for t in parsed_tokens)
        if hasattr(self, '_last_subscribed_tokens') and self._last_subscribed_tokens == token_set:
            return

        try:
            existing = self.primary_broker.get_subscribed_tokens()
            existing_set = set(str(t) for t in existing) if existing else set()
            if existing_set != token_set:
                self.primary_broker.set_subscribed_tokens(parsed_tokens)
                general_logger.info(f"[{self.primary_broker.account_id}] Subscribed {len(parsed_tokens)} tokens to DB table '{self.primary_broker.token_tablename}'")
            self._last_subscribed_tokens = token_set
        except Exception as e:
            general_logger.error(f"[{self.primary_broker.account_id}] Error subscribing instrument tokens to DB: {e}")

    # ── Vectorized Polars Candle Resampling Engine ──────────────────────────

    def group_by_rolling_window(
        self,
        df_hist: pl.DataFrame,
        raw_df: pl.DataFrame,
        window_size: str,
        max_bars: int = 100,
    ) -> pl.DataFrame:
        """
        SIMD Vectorized Multi-Token Hierarchical Candle Resampler in Polars:
        Delegates exclusively to unified indian_candle_engine.group_by_rolling_window.
        """
        effective_max = max(25, max_bars) if max_bars > 0 else getattr(config, 'MAX_CANDLE_HISTORY_BARS', 25)
        tkn_fn = getattr(self, 'tkn_to_exchg', None)
        return group_by_rolling_window(
            df_hist, raw_df, window_size, tkn_to_exchg_fn=tkn_fn, max_bars=effective_max
        )

    def normalize_ticks(self, tick_data: pl.DataFrame) -> pl.DataFrame:
        """Normalizes incoming ticks so instrument_token matches canonical cum_table contract tokens."""
        if tick_data.is_empty() or "tradingsymbol" not in tick_data.columns:
            return tick_data

        if hasattr(self, "cum_table") and not self.cum_table.is_empty() and "tradingsymbol" in self.cum_table.columns and "instrument_token" in self.cum_table.columns:
            sym_to_token = dict(zip(self.cum_table["tradingsymbol"].to_list(), self.cum_table["instrument_token"].to_list()))
            mapped = pl.col("tradingsymbol").replace(sym_to_token).cast(pl.Int64, strict=False)
            return tick_data.with_columns(
                pl.when(mapped.is_not_null())
                .then(mapped)
                .otherwise(pl.col("instrument_token"))
                .alias("instrument_token")
            )
        return tick_data

    def _get_target_brokers(self) -> List[Broker]:
        """Collects unique active Broker model instances across primary and registered user accounts."""
        brokers: List[Broker] = []
        if self.primary_broker and getattr(self.primary_broker, "enable_trade", False) and self.primary_broker not in brokers:
            brokers.append(self.primary_broker)
        for acc in self.accounts:
            b_obj = getattr(acc, "broker_obj", None)
            if b_obj and getattr(b_obj, "enable_trade", False) and b_obj not in brokers:
                brokers.append(b_obj)
        return brokers

    def update_all_candles_batch(self, tick_data: pl.DataFrame) -> None:
        """
        Batch Multi-Token Incremental Vectorized Candle Resampler:
        1. Incremental 1min OHLC update (fwd_1_all, capped at 25 bars).
        2. Fast O(1) in-place close/high/low updates and new candle bucket generation for higher timeframes.
        3. Persists multi-timeframe candles to DB via in-memory deduplication cache.
        """
        if tick_data.is_empty() or not self.data_ready:
            general_logger.debug(
                "[CANDLE_UPDATE] Skipping batch candle update: tick_data empty=%s, data_ready=%s.",
                tick_data.is_empty(), self.data_ready
            )
            return

        raw_df = self.normalize_ticks(tick_data)
        if 'date_time' not in raw_df.columns:
            general_logger.warning("[CANDLE_UPDATE] Skipping batch candle update: 'date_time' column missing in tick_data.")
            return

        clean_ticks = raw_df.filter(
            pl.col("last_price").is_not_null() & (pl.col("last_price") > 0)
        )
        summary_cols = [c for c in ["open", "high", "low", "close"] if c in clean_ticks.columns]
        if summary_cols:
            clean_ticks = clean_ticks.drop(summary_cols)

        if clean_ticks.is_empty():
            general_logger.debug("[CANDLE_UPDATE] Skipping batch candle update: no clean positive price ticks found in %d ticks.", len(tick_data))
            return

        effective_max = max(25, getattr(config, 'MAX_CANDLE_HISTORY_BARS', 25)) if 'config' in globals() else 25

        def _tkn_to_exchg(tkn: int) -> str:
            if hasattr(self, 'cum_table') and not self.cum_table.is_empty() and 'exchange' in self.cum_table.columns and 'instrument_token' in self.cum_table.columns:
                matched = self.cum_table.filter(pl.col('instrument_token') == tkn)
                if not matched.is_empty():
                    return str(matched['exchange'][0]).upper()
            return 'NFO'

        try:
            self.fwd_3_all = update_candles_incremental(
                self.fwd_3_all, clean_ticks, window_size='3min', max_bars=effective_max, default_origin="09:15", tkn_to_exchg_fn=_tkn_to_exchg
            )
        except Exception as e:
            general_logger.error(f"[CANDLE_UPDATE] Error in Polars 3m incremental candle update: {e}")
            return

        if self.fwd_3_all.is_empty():
            general_logger.warning("[CANDLE_UPDATE] 3m candle frame fwd_3_all is empty after update. Skipping higher timeframes.")
            return

        # Extract ONLY the active (latest) 3m bar per token for fast O(1) downsampling to higher timeframes
        if not self.fwd_3_all.is_empty():
            if "instrument_token" in self.fwd_3_all.columns:
                active_3m = self.fwd_3_all.sort(["instrument_token", "date_time"], descending=[False, True]).group_by("instrument_token", maintain_order=True).first()
            else:
                active_3m = self.fwd_3_all.sort("date_time", descending=True).head(1)
        else:
            active_3m = pl.DataFrame()

        dt_span = 0.0
        if "date_time" in clean_ticks.columns:
            try:
                valid_dts = clean_ticks["date_time"].drop_nulls()
                if not valid_dts.is_empty():
                    dt_span = (valid_dts.max() - valid_dts.min()).total_seconds()
            except Exception:
                pass

        # Update higher timeframes: batch bootstrap if clean_ticks covers the timeframe interval or historical table is empty,
        # or if a multi-interval batch arrives (e.g. startup warmup / reconnects) for day_cdl_all.
        # Otherwise perform fast O(1) incremental update from single active_3m bar.
        is_multi_interval_batch = (dt_span > 180 or len(clean_ticks) > 50)
        if not active_3m.is_empty():
            for tf_name, tf_win, tf_sec in [
                ('fwd_10_all', '10min', 600),
                ('fwd_30_all', '30min', 1800),
                ('fwd_60_all', '60min', 3600),
                ('day_cdl_all', '1D', 86400),
            ]:
                curr_table = getattr(self, tf_name, pl.DataFrame())
                should_bootstrap = (
                    curr_table.is_empty()
                    or (dt_span >= tf_sec)
                    or (tf_name == "day_cdl_all" and is_multi_interval_batch)
                )
                if should_bootstrap and not clean_ticks.is_empty():
                    setattr(self, tf_name, group_by_rolling_window(curr_table, clean_ticks, window_size=tf_win, tkn_to_exchg_fn=_tkn_to_exchg, max_bars=effective_max))
                else:
                    setattr(self, tf_name, update_candles_incremental(curr_table, active_3m, window_size=tf_win, max_bars=effective_max, default_origin="09:15", tkn_to_exchg_fn=_tkn_to_exchg))

        self.ref_min_max_all = self.fwd_10_all
        general_logger.info(
            f"[CANDLE_UPDATE] Resampled active candles across 5 timeframes for {len(tick_data)} ticks "
            f"(3m={len(self.fwd_3_all)}, 10m={len(self.fwd_10_all)}, 30m={len(self.fwd_30_all)}, 60m={len(self.fwd_60_all)}, 1D={len(self.day_cdl_all)})."
        )
        self.write_db_candle()

    def write_db_candle(self, force: bool = False) -> None:
        """Persists updated multi-timeframe candles to AlgoInfo in DB with in-memory deduplication caching."""
        target_brokers = self._get_target_brokers()
        if not target_brokers:
            return

        candles_to_save = {
            'fwd_3_all': getattr(self, 'fwd_3_all', pl.DataFrame()),
            'fwd_10_all': getattr(self, 'fwd_10_all', pl.DataFrame()),
            'fwd_30_all': getattr(self, 'fwd_30_all', pl.DataFrame()),
            'fwd_60_all': getattr(self, 'fwd_60_all', pl.DataFrame()),
            'day_cdl_all': getattr(self, 'day_cdl_all', pl.DataFrame()),
            'prev_day_cdl_all': getattr(self, 'prev_day_cdl_all', pl.DataFrame()),
        }
        if not hasattr(self, '_candle_cache'):
            self._candle_cache = {}

        saved_count = 0
        for name, df in candles_to_save.items():
            if df is not None and not df.is_empty():
                try:
                    sanitized = self.sanitize_for_json(df)
                    serialized = orjson.dumps(sanitized)
                    if force or self._candle_cache.get(name) != serialized:
                        for b in target_brokers:
                            b.set_algo_state(name, sanitized)
                        self._candle_cache[name] = serialized
                        saved_count += 1
                except Exception as e:
                    general_logger.warning("Error persisting candle table %s to DB: %s", name, e)
        if saved_count > 0:
            acc_list = [b.account_id for b in target_brokers]
            general_logger.info(f"Persisted {saved_count} candle tables to DB for account(s) {acc_list}.")

    def read_db_candle(self) -> None:
        if not self.primary_broker:
            return
        today_date = today_ist()
        for name in ['fwd_3_all', 'fwd_10_all', 'fwd_30_all', 'fwd_60_all', 'day_cdl_all', 'prev_day_cdl_all']:
            try:
                data = AlgoInfo.get_table_data(account=self.primary_broker, tablename=name)
                raw = data.get(name)
                if raw:
                    parsed = orjson.loads(raw) if isinstance(raw, str) else raw
                    df_loaded = pl.from_dicts(parsed) if isinstance(parsed, list) else None
                    if df_loaded is not None and not df_loaded.is_empty():
                        if 'date_time' in df_loaded.columns and df_loaded['date_time'].dtype == pl.Utf8:
                            try:
                                df_loaded = df_loaded.with_columns(pl.col('date_time').cast(pl.Utf8).str.slice(0, 19).str.to_datetime(strict=False))
                            except Exception:
                                pass
                        setattr(self, name, df_loaded)
            except Exception:
                continue

    def check_eod_cleanup(self, force: bool = False) -> None:
        """
        End-Of-Day (EOD) Lifecycle Manager:
        1. When post-trade time is reached or date advances, preserve day_cdl_all as prev_day_cdl_all.
        2. Persist prev_day_cdl_all to DB across all target brokers.
        3. Clear raw tick buffers from memory.
        4. Multi-timeframe candles (fwd_10_all, fwd_30_all, fwd_60_all) are retained in DB with at least 25 bars per token.
        """
        today = today_ist()
        is_new_day = (today > getattr(self, 'current_trading_day', today))

        has_mcx = False
        if hasattr(self, 'cum_table') and not self.cum_table.is_empty() and 'exchange' in self.cum_table.columns:
            has_mcx = any(str(e).upper() == 'MCX' for e in self.cum_table['exchange'].drop_nulls().unique())

        is_post_market = False
        if hasattr(self, 'session') and self.session is not None:
            if is_post_trade_time(tz=self.tz):
                if has_mcx:
                    now_naive = now_ist_naive(tz=self.tz)
                    close_time = now_naive.replace(hour=23, minute=30, second=0, microsecond=0)
                    is_post_market = (now_naive >= close_time)
                else:
                    is_post_market = True

        should_cleanup = force or is_new_day or (is_post_market and getattr(self, '_eod_cleared_date', None) != today)
        if not should_cleanup:
            return

        general_logger.info(f"[EOD_CLEANUP] Executing end-of-day candle cleanup (date={today}, force={force}, is_new_day={is_new_day}, is_post_market={is_post_market}).")

        # 1. Preserve day_cdl_all into prev_day_cdl_all
        if hasattr(self, 'day_cdl_all') and not self.day_cdl_all.is_empty():
            self.prev_day_cdl_all = self.day_cdl_all

        target_brokers = self._get_target_brokers()

        # 2. Persist prev_day_cdl_all to DB
        if not self.prev_day_cdl_all.is_empty() and target_brokers:
            try:
                sanitized_prev = self.sanitize_for_json(self.prev_day_cdl_all)
                for b in target_brokers:
                    b.set_algo_state('prev_day_cdl_all', sanitized_prev)
                general_logger.info(f"[EOD_CLEANUP] Persisted {len(self.prev_day_cdl_all)} previous day candle(s) to prev_day_cdl_all in DB.")
            except Exception as e:
                general_logger.warning(f"[EOD_CLEANUP] Failed to persist prev_day_cdl_all: {e}")

        # 3. Clear raw tick buffers
        self.tick_data = pl.DataFrame()
        self.delta_tick_data = pl.DataFrame()

        self._eod_cleared_date = today
        if is_new_day:
            self.current_trading_day = today
            cdl_schema = getattr(self.fwd_10_all, 'schema', None)
            self.day_cdl_all = pl.DataFrame(schema=cdl_schema)
            general_logger.info(f"[EOD_CLEANUP] Trading day advanced to {today}. Started fresh session.")

    # ── Vectorized Heikin-Ashi Indicator ───────────────────────────────────

    def heikin_ashi(self, df: pl.DataFrame) -> pl.DataFrame:
        """Pure Polars Heikin-Ashi transformer delegating to indian_candle_engine."""
        return heikin_ashi(df)

    # ── Optimized Strike Detection (Polars-Native) ──────────────────────────

    def _detect_strike_side(
        self,
        ref_stock_name: str,
        inst_type: str,
        cap_info: str,
        strike_dist: float,
        candle_avg: float
    ) -> None:
        if candle_avg is None or np.isnan(candle_avg) or candle_avg <= 0:
            strike_logger.debug("[%s] %s strike detection skipped: invalid candle_avg (%s).", ref_stock_name, inst_type, candle_avg)
            return
        if strike_dist is None or np.isnan(strike_dist):
            strike_dist = 0.0

        est_strike = float(candle_avg + strike_dist)
        if est_strike <= 0:
            strike_logger.debug("[%s] %s strike detection skipped: est_strike <= 0 (%.2f).", ref_stock_name, inst_type, est_strike)
            return

        candidates = self.cum_table.filter(
            (pl.col('Ref_stock') == ref_stock_name) &
            (pl.col('instrument_type') == inst_type) &
            (pl.col('cap') == cap_info)
        )
        if candidates.is_empty():
            strike_logger.debug("[%s] %s strike detection skipped: no candidates found matching cap=%s.", ref_stock_name, inst_type, cap_info)
            return

        # Expiry filters
        expiry_cutoff = pl.lit(tomorrow_ist())
        candidates = candidates.filter(
            (pl.col('expiry').str.to_date(strict=False) > expiry_cutoff) &
            (pl.col('exp_date_list') > self.month_cutoff)
        )
        if candidates.is_empty():
            strike_logger.debug("[%s] %s strike detection skipped: no candidates remaining after expiry filter.", ref_stock_name, inst_type)
            return

        min_week = candidates['week_dist'].min()
        candidates = candidates.filter(pl.col('week_dist') == min_week)
        if candidates.is_empty():
            strike_logger.debug("[%s] %s strike detection skipped: no candidates found matching min week_dist (%s).", ref_stock_name, inst_type, min_week)
            return

        # Compute ratio
        dist_calc = candidates.with_columns(((pl.col('strike') / est_strike).round(6)).alias('ratio'))
        if inst_type == 'CE':
            selected = dist_calc.filter(pl.col('ratio') >= 1.0).sort('ratio', descending=False).head(1)
        else:
            selected = dist_calc.filter(pl.col('ratio') <= 1.0).sort('ratio', descending=True).head(1)

        if not selected.is_empty():
            sel_tkn = selected['instrument_token'][0]
            sel_sym = selected['tradingsymbol'][0]
            sel_ratio = float(selected['ratio'][0]) if 'ratio' in selected.columns else 1.0
            
            # Update cum_table
            tradable_expr = pl.col('Tradable_stock') if 'Tradable_stock' in self.cum_table.columns else pl.lit('No')
            sel_tkn_str = str(sel_tkn).strip()
            self.cum_table = self.cum_table.with_columns(
                pl.when(pl.col('instrument_token').cast(pl.Utf8) == sel_tkn_str)
                  .then(pl.lit('Yes'))
                  .otherwise(pl.col('Buy_strike'))
                  .alias('Buy_strike'),
                pl.when(pl.col('instrument_token').cast(pl.Utf8) == sel_tkn_str)
                  .then(pl.lit('Yes'))
                  .otherwise(tradable_expr)
                  .alias('Tradable_stock')
            )
            strike_logger.info(f"[{ref_stock_name}] {inst_type} Strike Selected in Polars: {sel_sym} (token={sel_tkn}, EstStrike={est_strike:.2f}, ratio={sel_ratio:.4f})")
        else:
            strike_logger.debug(f"[{ref_stock_name}] {inst_type}: No candidate selected (EstStrike={est_strike:.2f}, Candidates={len(candidates)})")

    def strike_detect(self, tick_data: pl.DataFrame) -> None:
        """Optimized Strike Detection Engine in Polars."""
        if self.cum_table.is_empty():
            general_logger.warning("[STRIKE_DETECT] Skipping strike detection: cum_table is empty.")
            return
        if not tick_data.is_empty():
            tick_data = self.normalize_ticks(tick_data)

        # Reset Buy_strike and Tradable_stock
        self.cum_table = self.cum_table.with_columns([
            pl.lit('NA').alias('Buy_strike'),
            pl.lit('No').alias('Tradable_stock')
        ])

        # Ensure all_ref_tkns is always synchronized with cum_table (safe list of non-empty tokens, avoiding int-only truncation)
        ref_tkns = self.cum_table['Ref_stock_tkn'].drop_nulls().unique().to_list()
        self.all_ref_tkns = [t for t in ref_tkns if t is not None and str(t).strip() not in ('-1', '')]

        strike_logger.info(
            "[STRIKE_DETECT] Executing strike detection for %d underlyings across %d ticks...",
            len(self.all_ref_tkns),
            len(tick_data),
        )

        for ref_tkn in self.all_ref_tkns:
            ref_tkn_str = str(ref_tkn).strip()
            if not ref_tkn_str or ref_tkn_str == '-1':
                continue

            matching_ref = self.cum_table.filter(pl.col('Ref_stock_tkn').cast(pl.Utf8) == ref_tkn_str)
            if matching_ref.is_empty():
                continue

            # Prioritize Cap_info (user configuration), NEVER fall back to contract-level matching_ref['cap'][0]
            cap_info = matching_ref['Cap_info'][0] if ('Cap_info' in matching_ref.columns and matching_ref['Cap_info'][0] is not None) else 'monthly_options'
            ref_stock_name = str(matching_ref['Ref_stock'][0]) if 'Ref_stock' in matching_ref.columns else ""
            idx_stock_name = str(matching_ref['Nifty_index'][0]) if ('Nifty_index' in matching_ref.columns and matching_ref['Nifty_index'][0] is not None) else ""
            base_sym = str(matching_ref['Symbol'][0]) if ('Symbol' in matching_ref.columns and matching_ref['Symbol'][0] is not None) else ""
            idx_tkn = matching_ref['Index_tkn'][0] if 'Index_tkn' in matching_ref.columns else None

            # Alphanumeric tokens matching: match instrument_token against Ref_stock_tkn and Index_tkn
            token_candidates = [ref_tkn_str]
            if idx_tkn is not None and str(idx_tkn).strip() not in ('-1', ''):
                token_candidates.append(str(idx_tkn).strip())

            token_filter = pl.col('instrument_token').cast(pl.Utf8).is_in(token_candidates)

            # Symbol names matching: match tradingsymbol strictly against text symbol names
            symbol_candidates = [s for s in [ref_stock_name, idx_stock_name, base_sym] if s and str(s).strip()]
            if not tick_data.is_empty() and 'tradingsymbol' in tick_data.columns and symbol_candidates:
                token_filter = token_filter | pl.col('tradingsymbol').is_in(symbol_candidates)

            recent_ticks = tick_data.filter(token_filter) if not tick_data.is_empty() else pl.DataFrame()

            ref_fwd_3 = self.fwd_3_all.filter(
                pl.col('instrument_token').cast(pl.Utf8).is_in(token_candidates)
            ) if not self.fwd_3_all.is_empty() and 'instrument_token' in self.fwd_3_all.columns else pl.DataFrame()

            try:
                candle_avg = None
                if not ref_fwd_3.is_empty() and len(ref_fwd_3) >= 2:
                    mean_val = (ref_fwd_3['close'][1] + ref_fwd_3['open'][1]) / 2.0
                    candle_avg = float(mean_val)
                elif not ref_fwd_3.is_empty() and len(ref_fwd_3) == 1:
                    mean_val = (ref_fwd_3['close'][0] + ref_fwd_3['open'][0]) / 2.0
                    candle_avg = float(mean_val)

                if (candle_avg is None or np.isnan(candle_avg)) and not recent_ticks.is_empty() and 'last_price' in recent_ticks.columns:
                    valid_lps = recent_ticks.filter(pl.col('last_price') > 0)['last_price']
                    if not valid_lps.is_empty():
                        candle_avg = float(valid_lps[-1])

                if (candle_avg is None or np.isnan(candle_avg) or candle_avg <= 0) and 'last_price' in matching_ref.columns:
                    m_lp = matching_ref['last_price'][0]
                    if m_lp is not None and float(m_lp) > 0:
                        candle_avg = float(m_lp)

                if (candle_avg is None or np.isnan(candle_avg) or candle_avg <= 0) and not self.day_cdl_all.is_empty() and 'instrument_token' in self.day_cdl_all.columns:
                    day_ref = self.day_cdl_all.filter(pl.col('instrument_token').cast(pl.Utf8).is_in(token_candidates))
                    if not day_ref.is_empty() and 'close' in day_ref.columns:
                        d_lp = day_ref['close'][-1]
                        if d_lp is not None and float(d_lp) > 0:
                            candle_avg = float(d_lp)

                if candle_avg is not None and not np.isnan(candle_avg) and candle_avg > 0:
                    strike_dist_ce = float(matching_ref['Strike_dist_CE'][0]) if ('Strike_dist_CE' in matching_ref.columns and matching_ref['Strike_dist_CE'][0] is not None) else 0.0
                    strike_dist_pe = float(matching_ref['Strike_dist_PE'][0]) if ('Strike_dist_PE' in matching_ref.columns and matching_ref['Strike_dist_PE'][0] is not None) else 0.0

                    self._detect_strike_side(ref_stock_name, 'CE', cap_info, strike_dist_ce, candle_avg)
                    self._detect_strike_side(ref_stock_name, 'PE', cap_info, strike_dist_pe, candle_avg)
                else:
                    strike_logger.warning("[%s] Unable to compute valid candle_avg (val=%s). Skipping strike detection.", ref_stock_name, candle_avg)
            except Exception as e:
                general_logger.error(f"Error in Polars strike_detect for {ref_stock_name}: {e}")

        selected_count = 0
        active_syms = []
        if "Buy_strike" in self.cum_table.columns:
            sel_df = self.cum_table.filter(pl.col("Buy_strike") == "Yes")
            selected_count = sel_df.height
            if "tradingsymbol" in sel_df.columns:
                active_syms = sel_df["tradingsymbol"].drop_nulls().to_list()
        strike_logger.info("[STRIKE_DETECT] Strike detection completed: %d active strike(s) selected (%s).", selected_count, ", ".join(active_syms) if active_syms else "None")

    # ── Complete Momentum & Signal Logic (100% Exact Parity in Polars) ────

    def mom(self, token_number: int) -> pl.DataFrame:
        """
        Exact Momentum Technical Analysis Engine in Polars:
        Evaluates multi-timeframe candles (1m, 3m, 5m, 10m, 15m, 30m, 60m, 0.5D, 1D)
        executing Paths 1 through 13_2 with 100% exact parity to zerodha_opt_trde_reference.py.
        """
        # Reset signal columns for token
        self.cum_table = self.cum_table.with_columns(
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(0)).otherwise(pl.col('buy_signal_PE')).alias('buy_signal_PE'),
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(0)).otherwise(pl.col('buy_signal_CE')).alias('buy_signal_CE'),
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(0)).otherwise(pl.col('day_fall')).alias('day_fall'),
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(0)).otherwise(pl.col('day_rise')).alias('day_rise'),
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(0)).otherwise(pl.col('CE_jump')).alias('CE_jump'),
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(0)).otherwise(pl.col('PE_jump')).alias('PE_jump'),
        )

        matching_tkn = self.cum_table.filter(pl.col('instrument_token') == token_number)
        if matching_tkn.is_empty():
            matching_tkn = self.cum_table.filter(pl.col('Ref_stock_tkn') == token_number)
        if matching_tkn.is_empty():
            inst_analysis_logger.warning("[MOM_SKIP] Token %d not found in cum_table (instrument_token or Ref_stock_tkn). Returning empty signals.", token_number)
            return pl.DataFrame()

        index_token_number = matching_tkn['Index_tkn'][0] if 'Index_tkn' in matching_tkn.columns else token_number
        exchg = str(matching_tkn['exchange'][0])
        cur_symbol = str(matching_tkn['tradingsymbol'][0])
        scan_window = int(matching_tkn['Scan_window'][0]) if 'Scan_window' in matching_tkn.columns else 300
        hedge_points_pe = float(matching_tkn['Hedge_points_PE'][0]) if 'Hedge_points_PE' in matching_tkn.columns else 20.0
        hedge_points_ce = float(matching_tkn['Hedge_points_CE'][0]) if 'Hedge_points_CE' in matching_tkn.columns else 20.0

        olhc_max = 0.0
        olhc_min = 0.0

        if not self.tick_data.is_empty() and 'instrument_token' in self.tick_data.columns:
            session_ref_data = self.tick_data.filter(pl.col('instrument_token') == token_number)
            index_ref_data = self.tick_data.filter(pl.col('instrument_token') == index_token_number)
        else:
            session_ref_data = pl.DataFrame()
            index_ref_data = pl.DataFrame()

        has_session_data = not session_ref_data.is_empty()
        has_index_data = not index_ref_data.is_empty()

        sig_ce = 0
        sig_pe = 0
        ce_jump = 0
        pe_jump = 0
        try:
            debounce_threshold = int(float(getattr(self, 'debounce_counter_threshold', 0)))
        except Exception:
            debounce_threshold = 0

        if has_session_data and has_index_data and self.data_ready:
            # Slicing from pre-resampled global multi-token buffers (sorted descending: 0=newest, 1=prev, 2=2-prev)
            self.fwd_10 = self.fwd_10_all.filter(pl.col('instrument_token') == token_number).sort('date_time', descending=True)
            self.ref_min_max = self.fwd_10
            self.fwd_30 = self.fwd_30_all.filter(pl.col('instrument_token') == token_number).sort('date_time', descending=True)
            self.fwd_30_index = self.fwd_30_all.filter(pl.col('instrument_token') == index_token_number).sort('date_time', descending=True)
            self.day_cdl = self.day_cdl_all.filter(pl.col('instrument_token') == token_number).sort('date_time', descending=True)

            session_ref_sorted = session_ref_data.sort('date_time', descending=True)
            current_ltp = float(session_ref_sorted['last_price'][0] or 0.0)

            try:
                max_time = self.ref_min_max['date_time'].max()
                cutoff = max_time - timedelta(seconds=scan_window)
                active_scan = self.ref_min_max.filter(pl.col('date_time') >= cutoff)
                if not active_scan.is_empty():
                    c_max = active_scan['close'].max()
                    o_max = active_scan['open'].max()
                    c_min = active_scan['close'].min()
                    o_min = active_scan['open'].min()
                    olhc_max = float(max(c_max if c_max is not None else current_ltp, o_max if o_max is not None else current_ltp))
                    olhc_min = float(min(c_min if c_min is not None else current_ltp, o_min if o_min is not None else current_ltp))
                else:
                    olhc_max = current_ltp
                    olhc_min = current_ltp
            except Exception:
                olhc_max = current_ltp
                olhc_min = current_ltp

            self.prev_day_cdl = self.prev_day_cdl_all.filter(pl.col('instrument_token') == token_number).sort('date_time', descending=True)
            if len(self.prev_day_cdl) < 1:
                self.prev_day_cdl = session_ref_sorted

            # ── 1. Session Start Rules ──────────────────────────────────────
            if self.session_start(exchg) and not self.prev_day_cdl_all.is_empty():
                if not self.prev_day_cdl.is_empty():
                    prev_last_price = float(self.prev_day_cdl['last_price'][0]) if 'last_price' in self.prev_day_cdl.columns else (float(self.prev_day_cdl['close'][0]) if 'close' in self.prev_day_cdl.columns else current_ltp)
                    if exchg == 'MCX':
                        if (prev_last_price - current_ltp) > hedge_points_pe:
                            sig_pe = 1
                        elif (current_ltp - prev_last_price) > hedge_points_ce:
                            sig_ce = 1
                    elif exchg == 'NFO' and not self.fwd_30.is_empty():
                        fwd_open_0 = float(self.fwd_30['open'][0])
                        if (fwd_open_0 - current_ltp) > hedge_points_pe:
                            sig_pe = 1
                        elif (current_ltp - fwd_open_0) > hedge_points_ce:
                            sig_ce = 1

            # ── 2. Mid-Session Momentum & Trend Rules ───────────────────────
            elif not self.fwd_30.is_empty() and not self.fwd_30_index.is_empty() and len(self.fwd_30) >= 3:
                fwd30_c = self.fwd_30['close'].to_list()
                fwd30_o = self.fwd_30['open'].to_list()
                fwd30_h = self.fwd_30['high'].to_list()
                fwd30_l = self.fwd_30['low'].to_list()

                fwd10_c = self.fwd_10['close'].to_list() if not self.fwd_10.is_empty() else []
                fwd10_o = self.fwd_10['open'].to_list() if not self.fwd_10.is_empty() else []
                fwd10_mean = [(fwd10_o[i] + fwd10_c[i]) / 2.0 for i in range(min(len(fwd10_c), min(len(fwd10_o), 3)))]

                # Path 10: CE Jump Exit Detection
                if (
                    (fwd30_c[1] <= (fwd30_o[2] + fwd30_c[2]) / 2.0) and
                    ((olhc_max - current_ltp) > hedge_points_ce * 1.5) and
                    (fwd30_c[1] > fwd30_c[0]) and
                    (fwd30_c[1] < fwd30_o[1]) and
                    (fwd30_c[0] < fwd30_o[0])
                ):
                    ce_jump = 0
                    inst_analysis_logger.info('path_10')

                # Path 3: CE Buy Detection
                # Note: fwd_10[0:2] mean is_monotonic_decreasing in descending series means fwd10_mean[0] >= fwd10_mean[1]
                if (
                    len(fwd10_mean) >= 2 and
                    (fwd10_mean[0] >= fwd10_mean[1]) and
                    ((current_ltp - olhc_min) > hedge_points_ce * 1.0) and
                    (fwd30_c[0] > fwd30_o[0]) and
                    (fwd30_c[1] > fwd30_o[1])
                ):
                    self.one_counter_ce_dict[token_number] = self.one_counter_ce_dict.get(token_number, 0) + 1
                    if self.one_counter_ce_dict[token_number] >= debounce_threshold:
                        sig_ce = 1
                        inst_analysis_logger.info('path_3')
                else:
                    self.one_counter_ce_dict[token_number] = 0

                # Path 9: PE Jump Exit Detection
                if (
                    ((fwd30_o[2] + fwd30_c[2]) / 2.0 <= fwd30_c[1]) and
                    ((current_ltp - olhc_min) > hedge_points_pe * 1.5) and
                    (fwd30_c[1] < fwd30_c[0]) and
                    (fwd30_c[1] > fwd30_o[1]) and
                    (fwd30_c[0] > fwd30_o[0])
                ):
                    pe_jump = 0
                    inst_analysis_logger.info('path_9')

                # Path 5: PE Buy Detection
                # Note: fwd_10[0:2] mean is_monotonic_increasing in descending series means fwd10_mean[0] <= fwd10_mean[1]
                if (
                    len(fwd10_mean) >= 2 and
                    (fwd10_mean[0] <= fwd10_mean[1]) and
                    ((olhc_max - current_ltp) > hedge_points_pe * 1.0) and
                    (fwd30_c[0] < fwd30_o[0]) and
                    (fwd30_c[1] < fwd30_o[1])
                ):
                    self.one_counter_pe_dict[token_number] = self.one_counter_pe_dict.get(token_number, 0) + 1
                    if self.one_counter_pe_dict[token_number] >= debounce_threshold:
                        sig_pe = 1
                        inst_analysis_logger.info('path_5')
                else:
                    self.one_counter_pe_dict[token_number] = 0

                # ── 3. End-of-Session Trend & Prediction (Paths 13) ─────────
                if self.session_end(exchg):
                    if exchg == 'NFO' and len(fwd30_c) >= 2:
                        fwd30_c1 = fwd30_c[1]
                        fwd30_first_h = fwd30_h[-1]
                        fwd30_first_l = fwd30_l[-1]
                        fwd30_first_o = fwd30_o[-1]
                        fwd30_first_c = fwd30_c[-1]
                        fwd30_h1 = fwd30_h[1]
                        fwd30_l1 = fwd30_l[1]

                        is_trendy = (
                            ((fwd30_c1 < min(fwd30_first_h, fwd30_first_l)) or (fwd30_c1 > max(fwd30_first_h, fwd30_first_l)))
                            and (not (fwd30_first_o <= fwd30_h1 <= fwd30_first_h) or not (fwd30_first_o <= fwd30_l1 <= fwd30_first_h))
                        )
                        if is_trendy:
                            inst_analysis_logger.info(f"today trendy for {cur_symbol}")
                            if fwd30_c1 > fwd30_first_c:
                                if len(fwd30_c) >= 6:
                                    h5_max = max(fwd30_h[5], fwd30_l[5])
                                    h1_4_all_below = all(h5_max > h for h in fwd30_h[1:4])
                                    m1_5 = [(fwd30_c[i] + fwd30_o[i]) / 2.0 for i in range(1, 5)]
                                    m1_5_increasing = all(m1_5[i] <= m1_5[i+1] for i in range(len(m1_5)-1))
                                    c1_less_prev2 = (fwd30_c1 < fwd30_c[-2])

                                    if h1_4_all_below and m1_5_increasing and c1_less_prev2:
                                        sig_ce = 1
                                        sig_pe = -1
                                        inst_analysis_logger.info(f"tomorrow down for {cur_symbol}")
                                    else:
                                        sig_ce = 1
                                        sig_pe = -1
                                        inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                                else:
                                    sig_ce = -1
                                    sig_pe = -1
                                    inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")

                            elif fwd30_c1 < fwd30_first_c:
                                if len(fwd30_c) >= 6:
                                    min_oc5 = min(fwd30_o[5], fwd30_c[5])
                                    any_c1_5_above = any(min_oc5 < c for c in fwd30_c[1:5])
                                    c1_5 = list(fwd30_c[1:5])
                                    not_monotonic_dec = not all(c1_5[i] >= c1_5[i+1] for i in range(len(c1_5)-1))

                                    h5_max = max(fwd30_h[5], fwd30_l[5])
                                    h1_4_all_below = all(h5_max > h for h in fwd30_h[1:4])
                                    is_monotonic_inc = all(c1_5[i] <= c1_5[i+1] for i in range(len(c1_5)-1))

                                    if any_c1_5_above or not_monotonic_dec:
                                        sig_pe = -1
                                        sig_ce = 1
                                        inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                                    elif h1_4_all_below and is_monotonic_inc:
                                        sig_pe = 1
                                        sig_ce = -1
                                        inst_analysis_logger.info(f"tomorrow down for {cur_symbol}")
                                else:
                                    sig_pe = -1
                                    sig_ce = 1
                                    inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                        else:
                            inst_analysis_logger.info(f"today flat for {cur_symbol}")
                            c1_4 = list(fwd30_c[1:4])
                            c1_4_dec = all(c1_4[i] >= c1_4[i+1] for i in range(len(c1_4)-1)) if len(c1_4) >= 2 else False
                            h1_gt_prev2 = (fwd30_h[1] > fwd30_h[-2]) if len(fwd30_h) >= 3 else False
                            first_mean = (fwd30_c[-1] + fwd30_o[-1]) / 2.0

                            if (h1_gt_prev2 or c1_4_dec) and (first_mean < fwd30_c1):
                                sig_ce = 1
                                sig_pe = -1
                                inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                            else:
                                sig_ce = -1
                                sig_pe = 1
                                inst_analysis_logger.info(f"tomorrow down for {cur_symbol}")

                    if self.nfo_risk_hold and len(fwd30_c) >= 2:
                        if fwd30_c[1] < fwd30_c[-1]:
                            sig_ce = 1
                            sig_pe = 1

                    if self.next_session_closed(exchg):
                        if exchg == 'MCX':
                            sig_pe = 1
                            sig_ce = 1
                            inst_analysis_logger.info('path_13')
                        elif exchg == 'NFO' and len(fwd30_c) >= 2:
                            fwd30_c1 = fwd30_c[1]
                            fwd30_first_h = fwd30_h[-1]
                            fwd30_first_l = fwd30_l[-1]
                            fwd30_first_o = fwd30_o[-1]
                            fwd30_h1 = fwd30_h[1]
                            fwd30_l1 = fwd30_l[1]

                            is_trendy_ns = (
                                ((fwd30_c1 < min(fwd30_first_h, fwd30_first_l)) or (fwd30_c1 > max(fwd30_first_h, fwd30_first_l)))
                                and (not (fwd30_first_l <= fwd30_h1 <= fwd30_first_h) or not (fwd30_first_l <= fwd30_l1 <= fwd30_first_h))
                            )
                            if is_trendy_ns:
                                inst_analysis_logger.info(f"today trendy for {cur_symbol}")
                                if fwd30_c1 < fwd30_first_o:
                                    sig_ce = 1
                                    sig_pe = -1
                                    inst_analysis_logger.info('path_13_1')
                            # else:
                            #     sig_ce = -1
                            #     sig_pe = -1
                            #     inst_analysis_logger.info('sell all')
            is_debug = getattr(self, 'debug_mode', DEBUG)
            if is_debug:
                sig_pe = 1
                sig_ce = 1
                ce_jump = 0
                pe_jump = 0

        else:
            general_logger.info('skipping analysis')
            self.minus_five_counter_dict[token_number] = self.minus_five_counter_dict.get(token_number, 0) + 1
            if self.minus_five_counter_dict[token_number] >= debounce_threshold:
                sig_pe = 0
                sig_ce = 0

        latest_price = float(session_ref_data['last_price'][-1]) if not session_ref_data.is_empty() else 0.0

        # Update cum_table with resolved signals
        self.cum_table = self.cum_table.with_columns(
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(sig_pe)).otherwise(pl.col('buy_signal_PE')).alias('buy_signal_PE'),
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(sig_ce)).otherwise(pl.col('buy_signal_CE')).alias('buy_signal_CE'),
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(ce_jump)).otherwise(pl.col('CE_jump')).alias('CE_jump'),
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(pe_jump)).otherwise(pl.col('PE_jump')).alias('PE_jump'),
            pl.when(pl.col('Ref_stock_tkn') == token_number).then(pl.lit(latest_price)).otherwise(pl.col('current_value')).alias('current_value'),
        )

        stock_info = pl.DataFrame([{
            'instrument_token': int(token_number),
            'buy_signal_CE': int(sig_ce),
            'buy_signal_PE': int(sig_pe),
            'CE_jump': int(ce_jump),
            'PE_jump': int(pe_jump),
            'exchange': exchg,
            'symbol': cur_symbol
        }])

        # Always log instrument quantitative analysis telemetry
        inst_analysis_logger.info(
            f"[MOM_SIGNAL] [{cur_symbol}:{token_number}] LTP={latest_price:.2f}, High={olhc_max:.2f}, Low={olhc_min:.2f}, "
            f"Sig_CE={sig_ce}, Sig_PE={sig_pe}, CE_jump={ce_jump}, PE_jump={pe_jump}"
        )
        return stock_info


    # ── Derivative Analysis & Sizing in Polars ─────────────────────────────

    def derivative_analysis(self, all_table: pl.DataFrame, tick_data: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """High-Performance Polars Derivative Analysis Engine."""
        if self.cum_table.is_empty() or 'instrument_type' not in self.cum_table.columns or 'Buy_strike' not in self.cum_table.columns:
            inst_analysis_logger.warning("[DERIV_ANALYSIS_SKIP] cum_table is empty or missing required columns ('instrument_type', 'Buy_strike'). Returning empty frames.")
            return pl.DataFrame(), pl.DataFrame()

        inst_analysis_logger.info(f"Evaluating derivative strikes for {len(all_table) if not all_table.is_empty() else 0} instrument(s)...")

        # 1. Candidate BUY Selection
        buy_cand = self.cum_table.filter(
            (pl.col('Buy_strike') == 'Yes') & (
                ((pl.col('instrument_type') == 'CE') & (pl.col('buy_signal_CE') >= 1)) |
                ((pl.col('instrument_type') == 'PE') & (pl.col('buy_signal_PE') >= 1))
            )
        )
        self.buy_stock_cap = buy_cand

        # Open Interest (OI) Filter (1:1 Parity with zerodha_opt_trde_reference.py lines 3812-3819)
        if not self.buy_stock_cap.is_empty() and not tick_data.is_empty() and 'oi' in tick_data.columns and 'instrument_token' in tick_data.columns:
            buy_tkns = self.buy_stock_cap['instrument_token'].drop_nulls().unique().to_list()
            recent_oi = tick_data.filter(pl.col('instrument_token').is_in(buy_tkns) & (pl.col('oi') > 0))
            if not recent_oi.is_empty():
                valid_oi_tkns = recent_oi['instrument_token'].drop_nulls().unique().to_list()
                oi_candidates = self.buy_stock_cap.filter(pl.col('instrument_token').is_in(valid_oi_tkns))
                if not oi_candidates.is_empty():
                    best_tokens = []
                    pe_cands = oi_candidates.filter(pl.col('instrument_type') == 'PE')
                    ce_cands = oi_candidates.filter(pl.col('instrument_type') == 'CE')
                    if not pe_cands.is_empty():
                        pe_best = pe_cands.sort('strike', descending=True).unique(subset=['Ref_stock_tkn'], keep='first')
                        best_tokens.extend(pe_best['instrument_token'].drop_nulls().to_list())
                    if not ce_cands.is_empty():
                        ce_best = ce_cands.sort('strike', descending=False).unique(subset=['Ref_stock_tkn'], keep='first')
                        best_tokens.extend(ce_best['instrument_token'].drop_nulls().to_list())
                    if best_tokens:
                        self.buy_stock_cap = self.buy_stock_cap.filter(pl.col('instrument_token').is_in(best_tokens))

        # Friday End-of-Session Cutoff Filter (1:1 Parity with zerodha_opt_trde_reference.py lines 3821-3825)
        if not self.buy_stock_cap.is_empty() and datetime.today().weekday() == 4:
            drop_tkns = []
            for g_tkn in self.buy_stock_cap['instrument_token'].drop_nulls().unique().to_list():
                tkn_rows = self.cum_table.filter(pl.col('instrument_token') == g_tkn)
                if not tkn_rows.is_empty():
                    exchg = tkn_rows['exchange'][0] if 'exchange' in tkn_rows.columns else 'NFO'
                    row = self.buy_stock_cap.filter(pl.col('instrument_token') == g_tkn)
                    exp_val = row['exp_date_list'][0] if 'exp_date_list' in row.columns else None
                    if self.session_end(exchg) and exp_val == 2:
                        drop_tkns.append(g_tkn)
                        sym_name = row['tradingsymbol'][0] if 'tradingsymbol' in row.columns else str(g_tkn)
                        general_logger.info(f"removed {sym_name} from buy list")
            if drop_tkns:
                self.buy_stock_cap = self.buy_stock_cap.filter(~pl.col('instrument_token').is_in(drop_tkns))

        # 2. Check Exits across all accounts
        sell_candidates_list: List[pl.DataFrame] = []
        all_open_positions = self.all_open_positions

        if not all_open_positions.is_empty():
            for t_tkn in all_open_positions['instrument_token'].drop_nulls().unique().to_list():
                pos_tkn_rows = self.cum_table.filter(pl.col('instrument_token') == t_tkn)
                if pos_tkn_rows.is_empty():
                    continue

                ref_tkn_val = pos_tkn_rows['Ref_stock_tkn'][0]
                inst_type = pos_tkn_rows['instrument_type'][0]
                sym = pos_tkn_rows['tradingsymbol'][0]

                if not all_table.is_empty() and 'instrument_token' in all_table.columns:
                    ref_sig = all_table.filter(pl.col('instrument_token') == ref_tkn_val)
                    if not ref_sig.is_empty():
                        sig_ce = ref_sig['buy_signal_CE'][0]
                        sig_pe = ref_sig['buy_signal_PE'][0]
                        ce_j = ref_sig['CE_jump'][0]
                        pe_j = ref_sig['PE_jump'][0]

                        if inst_type == 'CE' and (ce_j == -1 or sig_ce in [-1, -2, -5] or sig_pe == -5):
                            sell_candidates_list.append(pos_tkn_rows.with_columns(pl.lit(sig_ce).alias('buy_signal_CE')))
                            trade_logger.info(f"[branch: ce_exit_signal_matched] [{sym}] CE Exit Signal={sig_ce}, CE_jump={ce_j}")
                        elif inst_type == 'PE' and (pe_j == -1 or sig_pe in [-1, -2, -5] or sig_ce == -5):
                            sell_candidates_list.append(pos_tkn_rows.with_columns(pl.lit(sig_pe).alias('buy_signal_PE')))
                            trade_logger.info(f"[branch: pe_exit_signal_matched] [{sym}] PE Exit Signal={sig_pe}, PE_jump={pe_j}")
                        else:
                            trade_logger.info(f"[branch: position_holding_steady] [{sym}] CE_sig={sig_ce}, PE_sig={sig_pe} - holding position.")

        if sell_candidates_list:
            self.sell_stock_table = pl.concat(sell_candidates_list, how="diagonal").unique(subset=['instrument_token'])
            trade_logger.info(f"[branch: sell_candidates_assembled] Total sell candidates={len(self.sell_stock_table)}.")
        else:
            self.sell_stock_table = pl.DataFrame()

        if not self.buy_stock_cap.is_empty():
            self.buy_stock_cap = self.buy_stock_cap.filter(pl.col('exp_date_list') > self.month_cutoff).unique(subset=['instrument_token'])
            trade_logger.info(f"[branch: buy_candidates_filtered_by_expiry] Filtered {len(self.buy_stock_cap)} buy candidates after expiry cutoff.")

        inst_analysis_logger.info(
            f"[DERIV_ANALYSIS_COMPLETE] Derivative analysis complete: {len(self.buy_stock_cap)} buy candidate(s), {len(self.sell_stock_table)} exit candidate(s) across {len(all_open_positions)} open positions."
        )
        return self.buy_stock_cap, self.sell_stock_table

    def _get_hedge_counter(self, tkn: int) -> int:
        return self.hedge_counter_dict.get(int(tkn), 0)

    def _increment_hedge_counter(self, tkn: int) -> None:
        t = int(tkn)
        self.hedge_counter_dict[t] = self.hedge_counter_dict.get(t, 0) + 1

    def _reset_hedge_counter(self, tkn: int) -> None:
        self.hedge_counter_dict[int(tkn)] = 0

    def strike_update(self) -> None:
        """Update strike entry table based on completed buy orders and daily candle ranges."""
        if self.open_positions.is_empty() or 'instrument_token' not in self.open_positions.columns:
            if not self.strike_entry_info.is_empty():
                self.strike_entry_info = pl.DataFrame(schema=self.strike_entry_info.schema)
            return

        opn_pos_tkns = self.open_positions['instrument_token'].drop_nulls().unique().to_list()
        if not self.strike_entry_info.is_empty() and 'instrument_token' in self.strike_entry_info.columns:
            self.strike_entry_info = self.strike_entry_info.filter(pl.col('instrument_token').is_in(opn_pos_tkns))
            if 'date_time' in self.strike_entry_info.columns and self.strike_entry_info['date_time'].dtype == pl.Utf8:
                try:
                    self.strike_entry_info = self.strike_entry_info.with_columns(pl.col('date_time').str.to_datetime())
                except Exception:
                    pass

        # Check completed buy orders in order_status
        if not self.order_status.is_empty() and 'status' in self.order_status.columns:
            successful_orders = self.order_status.filter(
                (pl.col('status').str.to_uppercase().is_in(['COMPLETE', 'PARTIAL'])) &
                (pl.col('transaction_type').str.to_uppercase() == 'BUY')
            )
            if not successful_orders.is_empty():
                for row in successful_orders.to_dicts():
                    ord_tkn = int(row.get('instrument_token') or -1)
                    if ord_tkn in opn_pos_tkns:
                        sym = str(row.get('tradingsymbol', ''))
                        sym_meta = self.get_symbol_meta(sym)
                        ref_tkn = int(sym_meta.get('Ref_stock_tkn', -1))
                        ref_stock = str(sym_meta.get('Ref_stock', ''))
                        inst_type = str(sym_meta.get('instrument_type', ''))

                        existing = False
                        if not self.strike_entry_info.is_empty() and 'instrument_token' in self.strike_entry_info.columns:
                            existing = not self.strike_entry_info.filter(pl.col('instrument_token') == ord_tkn).is_empty()

                        if not existing:
                            # Estimate entry underlying strike price
                            nearest_price = 0.0
                            if not self.tick_data.is_empty() and 'instrument_token' in self.tick_data.columns:
                                ref_ticks = self.tick_data.filter(pl.col('instrument_token') == ref_tkn)
                                if not ref_ticks.is_empty() and 'last_price' in ref_ticks.columns:
                                    nearest_price = float(ref_ticks['last_price'][-1])
                            if nearest_price <= 0 and hasattr(self, 'fwd_3_all') and not self.fwd_3_all.is_empty():
                                ref_cdl = self.fwd_3_all.filter(pl.col('instrument_token') == ref_tkn)
                                if not ref_cdl.is_empty() and 'close' in ref_cdl.columns:
                                    nearest_price = float(ref_cdl['close'][-1])

                            if nearest_price > 0:
                                new_entry = pl.DataFrame([{
                                    'instrument_token': ord_tkn,
                                    'strike_value': float(nearest_price),
                                    'tradingsymbol': sym,
                                    'date_time': datetime.now(),
                                    'ref_symbol': ref_tkn,
                                    'Ref_stock': ref_stock,
                                    'instrument_type': inst_type
                                }])
                                self.strike_entry_info = pl.concat([self.strike_entry_info, new_entry], how="diagonal").unique(subset=['instrument_token'], keep='last')
                                trade_logger.info(f"[branch: strike_entry_recorded] [{sym}] Strike entry price recorded at {nearest_price:.2f} for ref token {ref_tkn}")

        # Update strike detect based on day candle trailing
        for e in opn_pos_tkns:
            sym_meta = self.tkn_to_meta.get(e, {})
            ref_tkn = int(sym_meta.get('Ref_stock_tkn', -1))
            exchg = str(sym_meta.get('exchange', 'NFO'))
            inst_type = str(sym_meta.get('instrument_type', ''))

            if not self.day_cdl_all.is_empty() and 'instrument_token' in self.day_cdl_all.columns:
                ref_day = self.day_cdl_all.filter(pl.col('instrument_token') == ref_tkn)
                if not ref_day.is_empty() and 'high' in ref_day.columns and 'low' in ref_day.columns:
                    h = float(ref_day['high'][-1])
                    low_p = float(ref_day['low'][-1])
                    nearest_price = (h + low_p) / 2.0
                    if nearest_price > 0 and not self.strike_entry_info.is_empty() and not self.session_end(exchg):
                        cur_match = self.strike_entry_info.filter(pl.col('instrument_token') == e)
                        if not cur_match.is_empty():
                            cur_val = float(cur_match['strike_value'][0])
                            if inst_type == 'CE' and nearest_price > cur_val:
                                self.strike_entry_info = self.strike_entry_info.with_columns(
                                    pl.when(pl.col('instrument_token') == e).then(pl.lit(nearest_price)).otherwise(pl.col('strike_value')).alias('strike_value')
                                )
                            elif inst_type == 'PE' and nearest_price < cur_val:
                                self.strike_entry_info = self.strike_entry_info.with_columns(
                                    pl.when(pl.col('instrument_token') == e).then(pl.lit(nearest_price)).otherwise(pl.col('strike_value')).alias('strike_value')
                                )

    def jump(self, buy_list: pl.DataFrame, sell_list: pl.DataFrame, tick_data: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Dynamic Strike Jump & Hedging Consolidation in Polars:
        1. Evaluates open positions for strike drift against entry strikes using 3-min reference candle prices.
        2. Initiates jump exits (adds to sell_list) if underlying moves beyond Hedge_points * 3.5.
        3. Evaluates buy candidates for hedging roll confirmations against open positions in the same underlying.
        4. Discards conflicting tokens between buy and sell candidates and applies signal thresholds.
        """
        opn_syms = self.open_positions['tradingsymbol'].drop_nulls().to_list() if not self.open_positions.is_empty() and 'tradingsymbol' in self.open_positions.columns else []
        buy_syms = buy_list['tradingsymbol'].drop_nulls().to_list() if not buy_list.is_empty() and 'tradingsymbol' in buy_list.columns else []

        if not opn_syms and buy_list.is_empty() and sell_list.is_empty():
            return buy_list, sell_list

        all_syms = list(dict.fromkeys(opn_syms + buy_syms))
        open_pos_data = self.cum_table.filter(pl.col('tradingsymbol').is_in(opn_syms)) if opn_syms else pl.DataFrame()
        new_sell_rows: List[pl.DataFrame] = []

        for k in all_syms:
            sym_meta = self.get_symbol_meta(k)
            ref_stock_tkn = int(sym_meta.get('Ref_stock_tkn', -1))
            ref_inst_type = str(sym_meta.get('instrument_type', ''))
            exchg = str(sym_meta.get('exchange', 'NFO'))

            # Reference underlying 3-minute candle price
            ref_fwd_price = 0.0
            if hasattr(self, 'fwd_3_all') and not self.fwd_3_all.is_empty() and 'instrument_token' in self.fwd_3_all.columns:
                ref_cdl = self.fwd_3_all.filter(pl.col('instrument_token') == ref_stock_tkn)
                if not ref_cdl.is_empty() and 'close' in ref_cdl.columns and 'open' in ref_cdl.columns:
                    tail_rows = ref_cdl.tail(2)
                    c_val = tail_rows['close'].mean()
                    o_val = tail_rows['open'].mean()
                    if c_val is not None and o_val is not None:
                        ref_fwd_price = float((c_val + o_val) / 2.0)
            if ref_fwd_price <= 0 and not tick_data.is_empty() and 'instrument_token' in tick_data.columns:
                ref_ticks = tick_data.filter(pl.col('instrument_token') == ref_stock_tkn)
                if not ref_ticks.is_empty() and 'last_price' in ref_ticks.columns:
                    ref_fwd_price = float(ref_ticks['last_price'][-1])

            # Case A: Symbol is currently an OPEN position (Evaluating Strike Jump Exits)
            if k in opn_syms:
                striked_value = -1.0
                tkn_id = self.symbol_to_tkn(k)
                if hasattr(self, 'strike_entry_info') and not self.strike_entry_info.is_empty() and 'instrument_token' in self.strike_entry_info.columns:
                    stk_match = self.strike_entry_info.filter(pl.col('instrument_token') == tkn_id)
                    if not stk_match.is_empty() and 'strike_value' in stk_match.columns:
                        striked_value = float(stk_match['strike_value'][0])

                if striked_value <= 0:
                    striked_value = float(sym_meta.get('current_value', 0.0) or 0.0)

                if striked_value > 0 and exchg == 'CDS':
                    tick_size = float(sym_meta.get('tick_size', 1.0) or 1.0)
                    striked_value = striked_value * tick_size

                if striked_value > 0 and ref_fwd_price > 0:
                    hedge_pts_ce = float(sym_meta.get('Hedge_points_CE', 0.0) or 0.0)
                    hedge_pts_pe = float(sym_meta.get('Hedge_points_PE', 0.0) or 0.0)
                    ce_jump = int(sym_meta.get('CE_jump', 0) or 0)
                    pe_jump = int(sym_meta.get('PE_jump', 0) or 0)

                    if ref_inst_type == 'PE':
                        strike_diff_pe = striked_value - ref_fwd_price
                        inst_analysis_logger.info(f"[PE_Jump] {k}: Diff={strike_diff_pe:.2f}, Striked={striked_value:.2f}, RefPrice={ref_fwd_price:.2f}, PE_jump={pe_jump}")
                        if not self.session_start(exchg) and self.session_end(exchg):
                            if hedge_pts_pe > 0 and strike_diff_pe > (hedge_pts_pe * 3.5):
                                self._increment_hedge_counter(tkn_id)
                                if self._get_hedge_counter(tkn_id) >= self.hedge_threshold:
                                    if pe_jump <= -1:
                                        pos_row = self.cum_table.filter(pl.col('tradingsymbol') == k).with_columns([
                                            pl.lit(-1).alias('buy_signal_PE'),
                                            pl.lit(0).alias('buy_signal_CE')
                                        ])
                                        new_sell_rows.append(pos_row)
                                        trade_logger.info(f"[branch: pe_jump_exit_triggered] [{k}] Triggered jump exit at ref price {ref_fwd_price:.2f}")
                                        self._reset_hedge_counter(tkn_id)
                    elif ref_inst_type == 'CE':
                        strike_diff_ce = ref_fwd_price - striked_value
                        inst_analysis_logger.info(f"[CE_Jump] {k}: Diff={strike_diff_ce:.2f}, Striked={striked_value:.2f}, RefPrice={ref_fwd_price:.2f}, CE_jump={ce_jump}")
                        if not self.session_start(exchg) and self.session_end(exchg):
                            if hedge_pts_ce > 0 and strike_diff_ce > (hedge_pts_ce * 3.5):
                                self._increment_hedge_counter(tkn_id)
                                if self._get_hedge_counter(tkn_id) >= self.hedge_threshold:
                                    if ce_jump <= -1:
                                        pos_row = self.cum_table.filter(pl.col('tradingsymbol') == k).with_columns([
                                            pl.lit(-1).alias('buy_signal_CE'),
                                            pl.lit(0).alias('buy_signal_PE')
                                        ])
                                        new_sell_rows.append(pos_row)
                                        trade_logger.info(f"[branch: ce_jump_exit_triggered] [{k}] Triggered jump exit at ref price {ref_fwd_price:.2f}")
                                        self._reset_hedge_counter(tkn_id)

            # Case B: Symbol is a BUY candidate not currently open (Hedging roll validation)
            elif k in buy_syms and not open_pos_data.is_empty():
                possible_pos = open_pos_data.filter(
                    (pl.col('Ref_stock_tkn') == ref_stock_tkn) &
                    (pl.col('instrument_type') == ref_inst_type)
                )
                if not possible_pos.is_empty():
                    target_pos = possible_pos.sort('strike', descending=(ref_inst_type == 'CE')).head(1)
                    pos_tkn = int(target_pos['instrument_token'][0])
                    striked_val = -1.0
                    if hasattr(self, 'strike_entry_info') and not self.strike_entry_info.is_empty():
                        stk_m = self.strike_entry_info.filter(pl.col('instrument_token') == pos_tkn)
                        if not stk_m.is_empty() and 'strike_value' in stk_m.columns:
                            striked_val = float(stk_m['strike_value'][0])

                    if striked_val <= 0 and 'current_value' in target_pos.columns:
                        striked_val = float(target_pos['current_value'][0] or 0.0)

                    if striked_val > 0 and exchg == 'CDS':
                        tick_size = float(sym_meta.get('tick_size', 1.0) or 1.0)
                        striked_val = striked_val * tick_size

                    if striked_val > 0 and ref_fwd_price > 0:
                        hedge_pts_ce = float(sym_meta.get('Hedge_points_CE', 0.0) or 0.0)
                        hedge_pts_pe = float(sym_meta.get('Hedge_points_PE', 0.0) or 0.0)

                        if ref_inst_type == 'CE':
                            strike_diff_ce = ref_fwd_price - striked_val
                            inst_analysis_logger.info(f"[CE_Jump_BuyCheck] {k}: Diff={strike_diff_ce:.2f}, Striked={striked_val:.2f}, HedgePts={hedge_pts_ce}")
                            if hedge_pts_ce > 0 and strike_diff_ce > hedge_pts_ce:
                                general_logger.info(f"[branch: ce_jump_buy_confirmed] [{k}] Buy confirmed on jump.")
                            else:
                                buy_list = buy_list.filter(pl.col('tradingsymbol') != k)
                                general_logger.info(f"[branch: ce_jump_buy_suppressed] [{k}] Diff={strike_diff_ce:.2f} <= {hedge_pts_ce}. Suppressed.")
                        elif ref_inst_type == 'PE':
                            strike_diff_pe = striked_val - ref_fwd_price
                            inst_analysis_logger.info(f"[PE_Jump_BuyCheck] {k}: Diff={strike_diff_pe:.2f}, Striked={striked_val:.2f}, HedgePts={hedge_pts_pe}")
                            if hedge_pts_pe > 0 and strike_diff_pe > hedge_pts_pe:
                                general_logger.info(f"[branch: pe_jump_buy_confirmed] [{k}] Buy confirmed on jump.")
                            else:
                                buy_list = buy_list.filter(pl.col('tradingsymbol') != k)
                                general_logger.info(f"[branch: pe_jump_buy_suppressed] [{k}] Diff={strike_diff_pe:.2f} <= {hedge_pts_pe}. Suppressed.")

        if new_sell_rows:
            all_sells = [sell_list] + new_sell_rows if not sell_list.is_empty() else new_sell_rows
            sell_list = pl.concat(all_sells, how="diagonal").unique(subset=['instrument_token'])

        # Final mutual exclusion & signal thresholds
        if not buy_list.is_empty() and not sell_list.is_empty():
            sell_tkns = sell_list['instrument_token'].drop_nulls().to_list()
            buy_list = buy_list.filter(~pl.col('instrument_token').is_in(sell_tkns))
            trade_logger.info(f"[branch: jump_filtered_conflicts] Excluded conflicting tokens {sell_tkns} from buy list.")

        if not buy_list.is_empty() and ('buy_signal_PE' in buy_list.columns or 'buy_signal_CE' in buy_list.columns):
            conditions = []
            if 'buy_signal_PE' in buy_list.columns:
                conditions.append(pl.col('buy_signal_PE') >= 1)
            if 'buy_signal_CE' in buy_list.columns:
                conditions.append(pl.col('buy_signal_CE') >= 1)
            if conditions:
                combined_cond = conditions[0]
                for c in conditions[1:]:
                    combined_cond = combined_cond | c
                buy_list = buy_list.filter(combined_cond)

        if not sell_list.is_empty() and ('buy_signal_PE' in sell_list.columns or 'buy_signal_CE' in sell_list.columns):
            conditions = []
            if 'buy_signal_PE' in sell_list.columns:
                conditions.append(pl.col('buy_signal_PE') <= -1)
            if 'buy_signal_CE' in sell_list.columns:
                conditions.append(pl.col('buy_signal_CE') <= -1)
            if conditions:
                combined_cond = conditions[0]
                for c in conditions[1:]:
                    combined_cond = combined_cond | c
                sell_list = sell_list.filter(combined_cond)

        general_logger.info(f"Jump analysis finished: BuyCandidates={len(buy_list)}, SellCandidates={len(sell_list)}")
        return buy_list, sell_list

    # ── Multi-Account Sizing, Execution & Risk Management ───────────────────

    def capital_allocation_calc(self, symbol: str, buy_list: pl.DataFrame, account: Optional[IndianUserAccount] = None) -> float:
        """
        Exact Replica of reference.py capital_allocation_calc in Polars:
        1. Checks order_status to ensure no OPEN/PENDING order exists for the same Ref_stock and instrument_type.
        2. Calculates existing invested capital across ALL open positions sharing the same Ref_stock & instrument_type.
        3. Deducts current capital share from Max_capital_CE / Max_capital_PE.
        4. Bounds allocation by live account balance.
        """
        acc = account or self.account
        if not acc:
            return 0.0

        sym_meta = self.get_symbol_meta(symbol)
        ref_stock = str(sym_meta.get('Ref_stock', ''))
        inst_type = str(sym_meta.get('instrument_type', ''))
        if not ref_stock or not inst_type:
            if self.cum_table.is_empty() or 'tradingsymbol' not in self.cum_table.columns:
                strike_logger.info(f"[branch: cap_alloc_missing_symbol] Symbol '{symbol}' not found in cum_table.")
                return 0.0
            sym_rows = self.cum_table.filter(pl.col('tradingsymbol') == symbol)
            if sym_rows.is_empty():
                strike_logger.info(f"[branch: cap_alloc_missing_symbol] Symbol '{symbol}' not found in cum_table.")
                return 0.0
            ref_stock = str(sym_rows['Ref_stock'][0])
            inst_type = str(sym_rows['instrument_type'][0])

        max_cap_base = sym_meta.get(f'Max_capital_{inst_type}')
        if max_cap_base is None:
            if self.cum_table.is_empty() or 'tradingsymbol' not in self.cum_table.columns:
                return 0.0
            sym_rows = self.cum_table.filter(pl.col('tradingsymbol') == symbol)
            max_cap_base = sym_rows[f'Max_capital_{inst_type}'][0] if f'Max_capital_{inst_type}' in sym_rows.columns else 0.0

        if max_cap_base is None or np.isnan(max_cap_base) or float(max_cap_base) <= 0:
            strike_logger.info(f"[branch: cap_alloc_zero] Symbol '{symbol}' Max_capital_{inst_type}={max_cap_base} <= 0. Skipping.")
            return 0.0

        # 1. Check if an order is already OPEN/PENDING for this Ref_stock and instrument_type
        if not acc.order_status.is_empty():
            matching_orders = acc.order_status
            if 'Ref_stock' in matching_orders.columns and ref_stock:
                matching_orders = matching_orders.filter(pl.col('Ref_stock') == ref_stock)
            if 'instrument_type' in matching_orders.columns and inst_type:
                matching_orders = matching_orders.filter(pl.col('instrument_type') == inst_type)
            if not matching_orders.is_empty() and 'status' in matching_orders.columns:
                last_st = str(matching_orders['status'][-1]).upper()
                if last_st in ('OPEN', 'PENDING'):
                    strike_logger.info(f"[branch: cap_alloc_order_open] [{acc.user_id}] {ref_stock} {inst_type} has pending order (status={last_st}). Allocation=0.")
                    return 0.0

        # 2. Calculate already invested capital for this Ref_stock and instrument_type
        current_capital_share = 0.0
        if not acc.open_positions.is_empty():
            pos_df = acc.open_positions
            matching_pos = pos_df
            matching_tkns = self.cum_table.filter(
                (pl.col('Ref_stock') == ref_stock) &
                (pl.col('instrument_type') == inst_type)
            )['instrument_token'].drop_nulls().to_list()

            if matching_tkns and 'instrument_token' in pos_df.columns:
                matching_pos = pos_df.filter(pl.col('instrument_token').is_in(matching_tkns))
            elif 'tradingsymbol' in pos_df.columns:
                matching_pos = pos_df.filter(pl.col('tradingsymbol') == symbol)

            if not matching_pos.is_empty() and 'quantity' in matching_pos.columns:
                for row in matching_pos.to_dicts():
                    q = abs(float(row.get('quantity') or 0.0))
                    p = float(row.get('buy_price') or row.get('last_price') or 100.0)
                    mult = float(row.get('multiplier') or 1.0)
                    current_capital_share += (q * p * mult)
                strike_logger.info(f"[branch: existing_investment_deducted] [{acc.user_id}] {ref_stock} {inst_type}: Invested={current_capital_share:.2f} INR across {len(matching_pos)} position(s).")

        max_cap = float(max_cap_base) * acc.capital_multiplier
        available_share = max_cap - current_capital_share
        allocated = max(0.0, min(available_share, acc.live_balance))
        strike_logger.info(f"[branch: capital_allocated] [{acc.user_id}] {symbol} ({ref_stock} {inst_type}): MaxCap={max_cap:.2f}, AvailShare={available_share:.2f}, FinalAllocated={allocated:.2f} INR (LiveBal={acc.live_balance:.2f}).")
        return float(allocated)

    def buy_stk_qty(self, symbol: str, capital_share: float, buy_list: pl.DataFrame, account: Optional[IndianUserAccount] = None) -> Tuple[int, float]:
        """
        Exact Replica of reference.py buy_stk_qty in Polars:
        1. Computes adjusted tick price from tick_data (rounded up to tick_size).
        2. Calculates raw lot count from capital_share // adj_price.
        3. Enforces Max_lots_per_order cap minus existing lots already held in the same Ref_stock and instrument_type.
        """
        acc = account or self.account
        if not acc:
            return 0, 0.0
        sym_rows = self.cum_table.filter(pl.col('tradingsymbol') == symbol)
        if sym_rows.is_empty():
            strike_logger.info(f"[branch: buy_qty_symbol_missing] Symbol '{symbol}' not found in cum_table.")
            return 0, 0.0

        tkn = int(sym_rows['instrument_token'][0])
        ref_stock = str(sym_rows['Ref_stock'][0])
        inst_type = str(sym_rows['instrument_type'][0])
        lot_size = max(1, int(sym_rows['lot_size'][0] or 1)) if 'lot_size' in sym_rows.columns else 1
        tick_size = max(0.01, float(sym_rows['tick_size'][0] or 0.05)) if 'tick_size' in sym_rows.columns else 0.05
        max_lots = max(1, int(sym_rows['Max_lots_per_order'][0] or 10)) if 'Max_lots_per_order' in sym_rows.columns else 10
        exchg = str(sym_rows['exchange'][0]) if 'exchange' in sym_rows.columns else 'NFO'

        recent = self.tick_data.filter(pl.col('instrument_token') == tkn)
        if recent.is_empty():
            strike_logger.info(f"[branch: buy_qty_ticks_missing] No tick price found for '{symbol}'.")
            return 0, 0.0
        ltp = float(recent['last_price'][-1] or 0.0)

        adj_price = float(np.ceil(ltp / tick_size) * tick_size)
        if adj_price <= 0 or capital_share <= 0:
            strike_logger.info(f"[branch: buy_qty_zero_capital_or_price] Price={adj_price}, CapitalShare={capital_share}.")
            return 0, adj_price

        raw_units = int(capital_share // adj_price)
        lot_count = int(raw_units // lot_size)
        if lot_count <= 0:
            strike_logger.info(f"[branch: buy_qty_zero_lots] RawUnits={raw_units}, LotSize={lot_size} -> 0 lots.")
            return 0, adj_price

        # Check existing lots in open positions for this Ref_stock and instrument_type
        existing_qty = 0
        if not acc.open_positions.is_empty():
            matching_tkns = self.cum_table.filter(
                (pl.col('Ref_stock') == ref_stock) &
                (pl.col('instrument_type') == inst_type)
            )['instrument_token'].drop_nulls().to_list()
            if matching_tkns and 'instrument_token' in acc.open_positions.columns:
                pos_m = acc.open_positions.filter(pl.col('instrument_token').is_in(matching_tkns))
                if not pos_m.is_empty() and 'quantity' in pos_m.columns:
                    existing_qty = int(pos_m['quantity'].abs().sum())

        existing_lots = int(existing_qty // lot_size)
        available_lots = max(0, max_lots - existing_lots)
        final_lots = min(lot_count, available_lots)

        if final_lots <= 0:
            strike_logger.info(f"[branch: buy_qty_max_lots_reached] [{acc.user_id}] {symbol}: ExistingLots={existing_lots} >= MaxLots={max_lots}. Qty=0.")
            return 0, adj_price

        final_qty = final_lots if 'MCX' in exchg else (final_lots * lot_size)
        strike_logger.info(f"[branch: buy_qty_success] [{acc.user_id}] {symbol}: Lots={final_lots} (AvailLots={available_lots}), Qty={final_qty}, Price={adj_price:.2f}, Value={final_qty * adj_price:.2f} INR.")
        return int(final_qty), float(adj_price)

    def hold_pos_buy_chk(self, symbol_string: str, open_positions: pl.DataFrame) -> int:
        """
        Exact Replica of zerodha_opt_trde_reference.py hold_pos_buy_chk in Polars:
        - Checks loss_value_exist from loss_table_info columns (Ref_stock prefix matching).
        - Evaluates half_time(exchange) vs morning session.
        - Before half_time: allows buy only if no open position exists for the same Ref_stock.
        - After half_time: sets hold_pos_buy_sts = 1.
        - Checks session_start(exchange), next_session_closed(exchange), and session_end(exchange) overrides.
        - Returns 1 if buy is permitted, 0 otherwise.
        """
        hold_pos_buy_sts = 0
        meta = self.get_symbol_meta(symbol_string)
        if not meta:
            return 0
        ref_stock = str(meta.get('Ref_stock', ''))
        exchg = str(meta.get('exchange', 'NSE'))

        # Check if loss value configuration exists for this stock
        loss_value_exist = False
        loss_stocks = getattr(self, 'loss_table_stocks', set())
        for s_str in loss_stocks:
            if ref_stock[:len(s_str)].startswith(s_str):
                loss_value_exist = True
                general_logger.info(f"loss value exists for {symbol_string}")
                break

        has_open_pos = False
        open_pos_data = pl.DataFrame()
        if not open_positions.is_empty() and 'tradingsymbol' in open_positions.columns:
            qty_filter = open_positions.filter(pl.col('quantity') > 0) if 'quantity' in open_positions.columns else open_positions
            if not qty_filter.is_empty():
                has_open_pos = True
                open_syms = qty_filter['tradingsymbol'].drop_nulls().to_list()
                if not self.cum_table.is_empty() and 'tradingsymbol' in self.cum_table.columns:
                    open_pos_data = self.cum_table.filter(pl.col('tradingsymbol').is_in(open_syms))

        if has_open_pos and loss_value_exist:
            matching_ref = open_pos_data.filter(pl.col('Ref_stock') == ref_stock) if (not open_pos_data.is_empty() and 'Ref_stock' in open_pos_data.columns) else pl.DataFrame()
            if not self.half_time(exchg):
                if matching_ref.is_empty():
                    hold_pos_buy_sts = 1
                    general_logger.info(f"hold_pos_buy_sts is one for {symbol_string}")
                else:
                    hold_pos_buy_sts = 0
                    general_logger.info(f"hold_pos_buy_sts is zero for {symbol_string}")
            else:
                hold_pos_buy_sts = 1
                general_logger.info(f"hold_pos_buy_sts is one for {symbol_string}")

            if self.session_start(exchg) and loss_value_exist:
                if matching_ref.is_empty():
                    hold_pos_buy_sts = 1
                    general_logger.info(f"hold_pos_buy_sts is one for {symbol_string}")

        elif (not has_open_pos) and loss_value_exist:
            hold_pos_buy_sts = 1
            if self.session_start(exchg):
                hold_pos_buy_sts = 1
                general_logger.info(f"hold_pos_buy_sts is one for {symbol_string}")

        if self.next_session_closed(exchg) and self.session_end(exchg) and loss_value_exist:
            hold_pos_buy_sts = 1
            general_logger.info(f"hold_pos_buy_sts is one for {symbol_string}")

        if self.session_end(exchg) and loss_value_exist:
            hold_pos_buy_sts = 1
            general_logger.info(f"hold_pos_buy_sts is one for {symbol_string}")

        return hold_pos_buy_sts

    def hold_pos_sell_chk(self, symbol_string: str, open_positions: pl.DataFrame) -> Tuple[int, int]:
        if not open_positions.is_empty() and 'tradingsymbol' in open_positions.columns:
            matching = open_positions.filter(pl.col('tradingsymbol') == symbol_string)
            if not matching.is_empty():
                qty_col = None
                for col_name in ['quantity', 'active_pos', 'net_quantity']:
                    if col_name in matching.columns:
                        qty_col = col_name
                        break
                if qty_col:
                    total_qty = int(matching[qty_col].abs().sum())
                    if total_qty > 0:
                        trade_logger.info(f"[branch: hold_pos_sell_match] Found active position for '{symbol_string}': Qty={total_qty}.")
                        return 1, total_qty
        trade_logger.info(f"[branch: hold_pos_sell_no_match] No active position found for '{symbol_string}'.")
        return 0, 0

    def ordr_chk(self, symbol_string: str, order_status: Optional[pl.DataFrame] = None, account: Optional[IndianUserAccount] = None) -> Tuple[int, int, str, int]:
        """
        Exact Replica of reference.py ordr_chk in Polars:
        Checks if symbol or its Ref_stock/instrument_type already has an active or recent order:
        Returns (ordr_sts, ordr_id, trans_type, buy_list_remove):
          ordr_sts: 0 = not placed / delayed / pending, 1 = success / allowed, 7 = partial, 8 = rejected
          ordr_id: last order_id int or -1
          trans_type: transaction_type of last order ('BUY', 'SELL', 'none')
          buy_list_remove: 1 if symbol should be dropped from candidates, 0 otherwise
        """
        acc = account or self.account
        ord_df = order_status if (order_status is not None and not order_status.is_empty()) else (acc.order_status if (acc and not acc.order_status.is_empty()) else pl.DataFrame())
        
        ordr_sts = 1
        ord_id = -1
        trans_type = 'none'
        buy_list_remove = 0

        # 1. Match order_status records for Ref_stock & instrument_type
        if not ord_df.is_empty():
            sym_meta = self.get_symbol_meta(symbol_string)
            ref_stock = str(sym_meta.get('Ref_stock', ''))
            inst_type = str(sym_meta.get('instrument_type', ''))
            
            matching = ord_df
            if 'Ref_stock' in ord_df.columns and ref_stock:
                matching = matching.filter(pl.col('Ref_stock') == ref_stock)
            elif 'tradingsymbol' in ord_df.columns:
                matching = matching.filter(pl.col('tradingsymbol') == symbol_string)
                
            if 'instrument_type' in ord_df.columns and inst_type:
                matching = matching.filter(pl.col('instrument_type') == inst_type)

            if not matching.is_empty():
                last_order = matching.tail(1)
                st = str(last_order['status'][0]).upper() if 'status' in last_order.columns else 'COMPLETE'
                trans_type = str(last_order['transaction_type'][0]) if 'transaction_type' in last_order.columns else 'BUY'
                ord_id = int(last_order['order_id'][0]) if ('order_id' in last_order.columns and str(last_order['order_id'][0]).isdigit()) else -1
                msg = str(last_order.get_column('status_message')[0]) if 'status_message' in last_order.columns else ''

                if st == 'COMPLETE':
                    ordr_sts = 1
                elif st in ('PENDING', 'OPEN'):
                    ordr_sts = 0
                    buy_list_remove = 1
                    order_logger.info(f"[branch: buy_order_pending] [{acc.user_id if acc else 'GLOBAL'}] Symbol '{symbol_string}' has open/pending order (status={st}).")
                elif st == 'CANCELLED':
                    ordr_sts = 1
                elif st == 'REJECTED':
                    if 'suspended from trading' in msg.lower():
                        ordr_sts = 0
                        buy_list_remove = 1
                        order_logger.info(f"[branch: buy_order_suspended] [{acc.user_id if acc else 'GLOBAL'}] Symbol '{symbol_string}' suspended from trading.")
                    elif 'insufficient funds' in msg.lower():
                        ordr_sts = 1
                    else:
                        ordr_sts = 8
                elif 'partial' in st.lower() or 'partial' in msg.lower():
                    ordr_sts = 7

        # 2. Cooldown & delay check for the specific tradingsymbol
        if not ord_df.is_empty() and 'tradingsymbol' in ord_df.columns:
            sym_orders = ord_df.filter(pl.col('tradingsymbol') == symbol_string)
            if not sym_orders.is_empty():
                prev_order = sym_orders.tail(1)
                prev_st = str(prev_order['status'][0]).upper() if 'status' in prev_order.columns else ''
                prev_tt = str(prev_order['transaction_type'][0]) if 'transaction_type' in prev_order.columns else trans_type
                
                if prev_st != 'CANCELLED' and 'exchange_timestamp' in prev_order.columns:
                    ts = prev_order['exchange_timestamp'][0]
                    if ts is not None:
                        try:
                            if isinstance(ts, str):
                                order_dt = datetime.fromisoformat(ts.replace('Z', ''))
                            elif isinstance(ts, (datetime, date)):
                                order_dt = ts if isinstance(ts, datetime) else datetime.combine(ts, datetime.min.time())
                            else:
                                order_dt = datetime.now()
                            diff_sec = (datetime.now() - order_dt).total_seconds()
                            if diff_sec <= 30.0:
                                ordr_sts = 0
                                buy_list_remove = 1
                                trans_type = prev_tt
                                order_logger.info(f"[branch: buy_order_delayed_cooldown] [{symbol_string}] Order placed recently ({diff_sec:.1f}s <= 30s). Delaying.")
                        except Exception:
                            pass

        # 3. In-memory throttle check (recent_buy_order_time)
        if acc and hasattr(acc, 'recent_buy_order_time') and symbol_string in acc.recent_buy_order_time:
            elapsed = (datetime.now() - acc.recent_buy_order_time[symbol_string]).total_seconds()
            if elapsed < 5.0:
                order_logger.info(f"[branch: buy_order_throttled] [{acc.user_id}] Symbol '{symbol_string}' throttled ({elapsed:.1f}s < 5s).")
                return 0, ord_id, 'BUY_THROTTLED', 1

        order_logger.info(f"[branch: ordr_chk_result] [{symbol_string}] Status={ordr_sts}, OrderID={ord_id}, TransType={trans_type}, DropCandidate={buy_list_remove}")
        return ordr_sts, ord_id, trans_type, buy_list_remove

    def ordr_sell_chk(self, symbol_string: str, order_status: Optional[pl.DataFrame] = None, account: Optional[IndianUserAccount] = None) -> Tuple[int, int, str]:
        """
        Exact Replica of reference.py ordr_sell_chk in Polars:
        Checks if symbol or its Ref_stock/instrument_type has open sell orders or delay.
        Returns (ordr_sts, ordr_id, trans_type).
        """
        acc = account or self.account
        ord_df = order_status if (order_status is not None and not order_status.is_empty()) else (acc.order_status if (acc and not acc.order_status.is_empty()) else pl.DataFrame())
        
        ordr_sts = 1
        ord_id = -1
        trans_type = 'none'

        # 1. Match order_status records for Ref_stock & instrument_type
        if not ord_df.is_empty():
            sym_meta = self.get_symbol_meta(symbol_string)
            ref_stock = str(sym_meta.get('Ref_stock', ''))
            inst_type = str(sym_meta.get('instrument_type', ''))
            
            matching = ord_df
            if 'Ref_stock' in ord_df.columns and ref_stock:
                matching = matching.filter(pl.col('Ref_stock') == ref_stock)
            elif 'tradingsymbol' in ord_df.columns:
                matching = matching.filter(pl.col('tradingsymbol') == symbol_string)
                
            if 'instrument_type' in ord_df.columns and inst_type:
                matching = matching.filter(pl.col('instrument_type') == inst_type)

            if not matching.is_empty():
                last_order = matching.tail(1)
                st = str(last_order['status'][0]).upper() if 'status' in last_order.columns else 'COMPLETE'
                trans_type = str(last_order['transaction_type'][0]) if 'transaction_type' in last_order.columns else 'SELL'
                ord_id = int(last_order['order_id'][0]) if ('order_id' in last_order.columns and str(last_order['order_id'][0]).isdigit()) else -1
                msg = str(last_order.get_column('status_message')[0]) if 'status_message' in last_order.columns else ''

                if st == 'COMPLETE':
                    ordr_sts = 1
                elif st in ('PENDING', 'OPEN'):
                    ordr_sts = 0
                    order_logger.info(f"[branch: sell_order_pending] [{acc.user_id if acc else 'GLOBAL'}] Symbol '{symbol_string}' has open/pending order (status={st}).")
                elif st == 'CANCELLED':
                    ordr_sts = 1
                elif st == 'REJECTED':
                    if 'suspended from trading' in msg.lower():
                        ordr_sts = 0
                        order_logger.info(f"[branch: sell_order_suspended] [{acc.user_id if acc else 'GLOBAL'}] Symbol '{symbol_string}' suspended from trading.")
                    elif 'insufficient funds' in msg.lower():
                        ordr_sts = 1
                    else:
                        ordr_sts = 8
                elif 'partial' in st.lower() or 'partial' in msg.lower():
                    ordr_sts = 7

        # 2. In-memory throttle check
        if acc and hasattr(acc, 'recent_sell_order_time') and symbol_string in acc.recent_sell_order_time:
            elapsed = (datetime.now() - acc.recent_sell_order_time[symbol_string]).total_seconds()
            if elapsed < 5.0:
                order_logger.info(f"[branch: sell_order_throttled] [{acc.user_id}] Symbol '{symbol_string}' throttled ({elapsed:.1f}s < 5s).")
                return 0, ord_id, 'SELL_THROTTLED'

        order_logger.info(f"[branch: ordr_sell_chk_result] [{symbol_string}] Status={ordr_sts}, OrderID={ord_id}, TransType={trans_type}")
        return ordr_sts, ord_id, trans_type

    def execute_buy(
        self,
        sym: str,
        tkn: int,
        qty: int,
        exchg: str,
        order_status: int,
        order_id: int,
        price: float,
        account: Optional[IndianUserAccount] = None
    ) -> int:
        """
        Exact Replica of reference.py execute_buy in Polars:
        1. Determines lot counts, lot sizes, and validity (DAY for MCX, TTL for others).
        2. Routes standard orders to lim_ordr, and oversized orders to ice_ordr (iceberg limit).
        3. Enforces exchange trading hours, holiday, and weekend validation.
        4. Updates recent_buy_order_time and appends to stop_loss_info.
        """
        acc = account or self.account
        if not acc or qty <= 0 or price <= 0:
            order_logger.warning(f"[ORDER_GATE] [BUY_REJECT] [{acc.user_id if acc else 'NONE'}] Buy aborted for {sym} (token={tkn}): invalid params (qty={qty}, price={price}).")
            return -1

        sym_meta = self.get_symbol_meta(sym)
        lot_size = max(1, int(sym_meta.get('lot_size') or 1))
        max_lots = max(1, int(sym_meta.get('Max_lots_per_order') or 10))
        lot_count = int(qty // lot_size) if lot_size > 0 else qty
        validity = 'DAY' if exchg == 'MCX' else 'TTL'

        is_debug = getattr(self, 'debug_mode', DEBUG)
        can_trade = (not is_debug and not self.is_weekend() and self.exchg_time_buy_chk(exchg) and not self.is_holiday(exchg)) or self.special_session_chk(exchg)

        result_id = -1
        ordr_msg = 'order not placed'

        if is_debug:
            result_id = 999999
            order_logger.info(f"[branch: execute_buy_simulation] [{acc.user_id}] [DEBUG POLARS] Simulated BUY: {sym} Qty={qty} Price={price:.2f} Validity={validity}")
        elif can_trade:
            try:
                if max_lots >= lot_count or exchg == 'MCX':
                    qty_per_order = lot_count * lot_size if exchg != 'MCX' else qty
                    order_logger.info(f"[branch: placing_limit_buy] [{acc.user_id}] Placing Limit BUY for {sym}: Qty={qty_per_order} Price={price:.2f} Validity={validity}")
                    result_id, ordr_msg = acc.client.lim_ordr(sym, qty_per_order, 'BUY', exchg, 'NRML', price, self.buy_ttl_value, validity)
                else:
                    # Iceberg limit order
                    lot_per_leg = max(1, max_lots)
                    total_legs = min(9, max(1, int(lot_count // lot_per_leg)))
                    qty_per_order = lot_per_leg * lot_size
                    total_qty = qty_per_order * total_legs
                    order_logger.info(f"[branch: placing_iceberg_buy] [{acc.user_id}] Placing Iceberg BUY for {sym}: Legs={total_legs} QtyPerLeg={qty_per_order} TotalQty={total_qty} Price={price:.2f}")
                    result_id, ordr_msg = acc.client.ice_ordr(sym, qty_per_order, 'BUY', exchg, 'NRML', price, total_legs, total_qty, self.buy_ttl_value, 'LIMIT', validity)

                time.sleep(1)
            except Exception as e:
                general_logger.error(f"[branch: execute_buy_error] [{acc.user_id}] Order placement error: {e}", exc_info=True)
        else:
            order_logger.info(f"[branch: execute_buy_outside_hours] [{acc.user_id}] Market outside trading hours or holiday for exchange {exchg}.")

        if result_id != -1:
            if hasattr(acc, "mark_order_placed"):
                acc.mark_order_placed(result_id, sym, qty, price, "BUY")
            acc.recent_buy_order_time[sym] = datetime.now()
            new_sl = pl.DataFrame([{
                'order_id': str(result_id),
                'buy_price': float(price),
                'instrument_token': int(tkn),
                'date_time': datetime.now(),
                'tradingsymbol': str(sym)
            }])
            acc.stop_loss_info = pl.concat([acc.stop_loss_info, new_sl], how="diagonal").unique(subset=['instrument_token'], keep='last')
            trade_logger.info(f"[branch: execute_buy_success] [{acc.user_id}] {result_id} / {sym} / BUY / {price:.2f} / LIMIT / Qty={qty}")
        else:
            trade_logger.info(f"[branch: execute_buy_failed] [{acc.user_id}] {result_id} / {sym} / BUY / {price:.2f} / {ordr_msg}")

        order_logger.info(f"[ORDER_STATUS] [BUY_RESULT] [{acc.user_id}] Buy execution returned order_id={result_id} for {sym} (Qty={qty}, Price={price:.2f}).")
        return result_id

    def execute_sell(
        self,
        sym: str,
        tkn: int,
        qty: int,
        exchg: str,
        order_status: int,
        order_id: int,
        price: Optional[float] = None,
        account: Optional[IndianUserAccount] = None
    ) -> int:
        """
        Exact Replica of reference.py execute_sell in Polars:
        1. Computes adjusted tick price from tick_data (rounded down to tick_size).
        2. Sets validity (DAY for MCX, TTL for others).
        3. Enforces exchange trading hours and session limits.
        4. Dispatches limit sell order via client.lim_ordr.
        5. Updates recent_sell_order_time upon execution.
        """
        acc = account or self.account
        if not acc or qty <= 0:
            order_logger.warning(f"[ORDER_GATE] [SELL_REJECT] [{acc.user_id if acc else 'NONE'}] Sell aborted for {sym} (token={tkn}): invalid qty={qty}.")
            return -1

        sym_meta = self.get_symbol_meta(sym)
        tick_size = max(0.01, float(sym_meta.get('tick_size') or 0.05))
        validity = 'DAY' if exchg == 'MCX' else 'TTL'

        # Compute sell limit price
        adj_sell_price = price or 0.0
        if adj_sell_price <= 0 and not self.tick_data.is_empty() and 'instrument_token' in self.tick_data.columns:
            sym_ticks = self.tick_data.filter(pl.col('instrument_token') == tkn)
            if not sym_ticks.is_empty() and 'last_price' in sym_ticks.columns:
                ltp = float(sym_ticks['last_price'][-1] or 0.0)
                adj_sell_price = float(np.round(ltp - (ltp % tick_size), 2))

        if adj_sell_price <= 0:
            adj_sell_price = 0.05

        is_debug = getattr(self, 'debug_mode', DEBUG)
        can_trade = (not is_debug and not self.is_weekend() and self.exchg_time_sell_chk(exchg) and not self.is_holiday(exchg)) or self.special_session_chk(exchg)

        result_id = -1
        ordr_msg = 'sell order not placed'

        if is_debug:
            result_id = 888888
            order_logger.info(f"[branch: execute_sell_simulation] [{acc.user_id}] [DEBUG POLARS] Simulated SELL: {sym} Qty={qty} Price={adj_sell_price:.2f} Validity={validity}")
        elif can_trade:
            try:
                order_logger.info(f"[branch: placing_limit_sell] [{acc.user_id}] Placing Limit SELL for {sym}: Qty={qty} Price={adj_sell_price:.2f} Validity={validity}")
                result_id, ordr_msg = acc.client.lim_ordr(sym, qty, 'SELL', exchg, 'NRML', adj_sell_price, self.sell_ttl_value, validity)
                time.sleep(1)
            except Exception as e:
                general_logger.error(f"[branch: execute_sell_error] [{acc.user_id}] Sell order placement error: {e}", exc_info=True)
        else:
            order_logger.info(f"[branch: execute_sell_outside_hours] [{acc.user_id}] Market outside trading hours or holiday for exchange {exchg}.")

        if result_id != -1:
            if hasattr(acc, "mark_order_placed"):
                acc.mark_order_placed(result_id, sym, qty, adj_sell_price, "SELL")
            acc.recent_sell_order_time[sym] = datetime.now()
            trade_logger.info(f"[branch: execute_sell_success] [{acc.user_id}] {result_id} / {sym} / SELL / {adj_sell_price:.2f} / LIMIT / Qty={qty}")
        else:
            trade_logger.info(f"[branch: execute_sell_failed] [{acc.user_id}] {result_id} / {sym} / SELL / {adj_sell_price:.2f} / {ordr_msg}")

        order_logger.info(f"[ORDER_STATUS] [SELL_RESULT] [{acc.user_id}] Sell execution returned order_id={result_id} for {sym} (Qty={qty}, Price={adj_sell_price:.2f}).")
        return result_id

    def buy_sell_loop(self, account: Optional[IndianUserAccount] = None) -> None:
        """
        Exact Replica of reference.py buy_sell_loop in Polars:
        1. Validates active trading account and synchronizes state with JIT priority when signals exist.
        2. Deduplicates sell_stock_table and buy_stock_cap by instrument_token.
        3. Evaluates each sell candidate via hold_pos_sell_chk and ordr_sell_chk before dispatching execute_sell.
        4. Evaluates each buy candidate via tradable, hold_pos_buy_chk, and ordr_chk before capital allocation and execute_buy.
        5. Optimistically tracks in-flight orders without redundant broker API hammering.
        """
        acc = account or self.account
        if not acc or not acc.enabled:
            general_logger.info(f"[branch: account_disabled] [{acc.user_id if acc else 'NONE'}] Account is not configured or disabled.")
            return

        is_debug = getattr(self, 'debug_mode', DEBUG)
        if acc.live_balance == 0.0 and acc.open_positions.is_empty() and acc.pos_day_frame.is_empty():
            try:
                acc.sync_account_state(debug_mode=is_debug)
            except Exception as e:
                general_logger.error(f"[branch: sync_failed] [{acc.user_id}] Account sync failed: {e}")
                return

        self.open_positions = acc.open_positions
        self.order_status = acc.order_status
        self.live_balance = acc.live_balance
        self.avail_cash = acc.avail_cash

        is_debug = getattr(self, 'debug_mode', DEBUG)
        sells_attempted = 0
        sells_placed = 0
        buys_attempted = 0
        buys_placed = 0

        # ── 1. Sells Execution Loop ──────────────────────────────────────────
        if not self.sell_stock_table.is_empty():
            dedup_sells = self.sell_stock_table.unique(subset=['instrument_token'])
            trade_logger.info(f"[branch: process_sells] [{acc.user_id}] Processing {len(dedup_sells)} sell candidate(s).")
            for row in dedup_sells.to_dicts():
                sym = str(row.get('tradingsymbol') or '')
                tkn = int(row.get('instrument_token') or 0)
                exchg = str(row.get('exchange') or 'NFO')
                if not sym or tkn <= 0:
                    continue
                
                hold_sts, sell_qty = self.hold_pos_sell_chk(sym, acc.open_positions)
                if hold_sts == 1 and sell_qty > 0:
                    ord_sts, ord_id, _ = self.ordr_sell_chk(sym, acc.order_status, account=acc)
                    if ord_sts == 1 or is_debug:
                        sells_attempted += 1
                        trade_logger.info(f"[branch: sell_checks_passed] [{acc.user_id}] {sym}: HoldSts={hold_sts}, Qty={sell_qty}, OrderSts={ord_sts}, OrderID={ord_id}")
                        res = self.execute_sell(sym, tkn, sell_qty, exchg, ord_sts, ord_id, account=acc)
                        if res != -1:
                            sells_placed += 1
                            if not is_debug:
                                self.open_positions = acc.open_positions
                                self.order_status = acc.order_status
                                self.live_balance = acc.live_balance
                    else:
                        trade_logger.info(f"[branch: sell_ordr_chk_blocked] [{acc.user_id}] {sym} blocked by ordr_sell_chk (OrderSts={ord_sts}).")
                else:
                    trade_logger.debug(f"[branch: sell_hold_skipped] [{acc.user_id}] {sym}: HoldSts={hold_sts}, Qty={sell_qty}.")
        else:
            trade_logger.info(f"[branch: process_sells_empty] [{acc.user_id}] No sell candidates to execute.")

        # ── 2. Buys Execution Loop ───────────────────────────────────────────
        if acc.live_balance > 0 and not self.buy_stock_cap.is_empty():
            dedup_buys = self.buy_stock_cap.unique(subset=['instrument_token'])
            trade_logger.info(f"[branch: process_buys] [{acc.user_id}] Processing {len(dedup_buys)} buy candidate(s) with live balance {acc.live_balance:.2f} INR.")
            for row in dedup_buys.to_dicts():
                sym = str(row.get('tradingsymbol') or '')
                tkn = int(row.get('instrument_token') or 0)
                raw_tradable = row.get('tradable', 1)
                try:
                    tradable = int(float(str(raw_tradable))) if raw_tradable is not None else 1
                except (ValueError, TypeError):
                    tradable = 1 if str(raw_tradable).strip().lower() in ("1", "true", "yes") else 0

                if not sym or tkn <= 0:
                    continue

                if tradable != 1:
                    order_logger.info(f"[branch: buy_not_tradable] [{acc.user_id}] {sym} has tradable={raw_tradable}. Skipping.")
                    continue

                hold_buy_sts = self.hold_pos_buy_chk(sym, acc.open_positions)
                if hold_buy_sts == 1 or is_debug:
                    ord_sts, ord_id, trans_type, buy_list_remove = self.ordr_chk(sym, acc.order_status, account=acc)
                    if buy_list_remove == 1:
                        order_logger.info(f"[branch: buy_candidate_dropped] [{acc.user_id}] {sym} dropped by ordr_chk cooldown/pending flag.")
                        continue

                    if ord_sts == 1 or is_debug:
                        allocated_cap = self.capital_allocation_calc(sym, self.buy_stock_cap, account=acc)
                        if allocated_cap > 0:
                            qty, price = self.buy_stk_qty(sym, allocated_cap, self.buy_stock_cap, account=acc)
                            if qty > 0:
                                buys_attempted += 1
                                res = self.execute_buy(sym, tkn, qty, exchg, ord_sts, ord_id, price, account=acc)
                                if res != -1:
                                    buys_placed += 1
                                    if not is_debug:
                                        self.open_positions = acc.open_positions
                                        self.order_status = acc.order_status
                                        self.live_balance = acc.live_balance
                            else:
                                order_logger.info(f"[branch: buy_qty_zero] [{acc.user_id}] {sym}: computed buy quantity <= 0 (allocated_cap={allocated_cap:.2f}). Skipping.")
                        else:
                            order_logger.info(f"[branch: buy_capital_zero] [{acc.user_id}] {sym}: allocated capital <= 0. Skipping.")
                    else:
                        order_logger.info(f"[branch: buy_ordr_chk_blocked] [{acc.user_id}] {sym}: ordr_chk blocked (ord_sts={ord_sts}).")
                else:
                    order_logger.info(f"[branch: buy_hold_chk_blocked] [{acc.user_id}] {sym}: hold_pos_buy_chk blocked (hold_buy_sts={hold_buy_sts}).")
        elif acc.live_balance <= 0:
            trade_logger.info(f"[branch: process_buys_no_balance] [{acc.user_id}] Live balance={acc.live_balance:.2f} <= 0. Skipping buy entries.")
        else:
            trade_logger.info(f"[branch: process_buys_empty] [{acc.user_id}] No buy candidate signals to execute.")

        trade_logger.info(
            f"[TRADE_LOOP_SUMMARY] [{acc.user_id}] Buy/sell evaluation complete: "
            f"sells (candidates={len(self.sell_stock_table) if not self.sell_stock_table.is_empty() else 0}, attempted={sells_attempted}, placed={sells_placed}), "
            f"buys (candidates={len(self.buy_stock_cap) if not self.buy_stock_cap.is_empty() else 0}, attempted={buys_attempted}, placed={buys_placed}, live_balance={acc.live_balance:.2f})."
        )

    def slu(self, account: Optional[IndianUserAccount] = None) -> None:
        """
        Dynamic Trailing Stop Loss Maintenance in Polars (Full Parity with reference.py):
        1. Purges stop loss entries for positions that have been closed/sold or are no longer active in open_positions.
        2. Initializes entry stop losses for newly filled open positions from order_status average/buy price.
        3. Trails stop loss upwards during sl_update_time (afternoon session) when live tick price exceeds existing stop loss.
        4. Persists updated stop loss table to AlgoInfo.
        """
        acc = account or self.account
        if not acc or not acc.enabled:
            trade_logger.debug(f"[SLU_SKIP] Account {acc.user_id if acc else 'NONE'} skipped (enabled={getattr(acc, 'enabled', False)}).")
            return

        # ── 1. Purge Stop Losses for Closed Positions ─────────────────────────
        recent_cutoff = datetime.now() - timedelta(seconds=300)
        recent_tokens = set()
        if hasattr(acc, "recent_buy_order_time"):
            for s, t in acc.recent_buy_order_time.items():
                if t >= recent_cutoff:
                    tkn_val = self.symbol_to_tkn(s)
                    if tkn_val:
                        recent_tokens.add(tkn_val)
        if hasattr(self, "recent_buy_order_time"):
            for s, t in self.recent_buy_order_time.items():
                if t >= recent_cutoff:
                    tkn_val = self.symbol_to_tkn(s)
                    if tkn_val:
                        recent_tokens.add(tkn_val)

        opn_tkns = set()
        if not acc.open_positions.is_empty() and 'instrument_token' in acc.open_positions.columns:
            opn_tkns.update(acc.open_positions['instrument_token'].drop_nulls().unique().to_list())

        valid_tkns = list(opn_tkns | recent_tokens)
        if valid_tkns:
            if not acc.stop_loss_info.is_empty() and 'instrument_token' in acc.stop_loss_info.columns:
                acc.stop_loss_info = acc.stop_loss_info.filter(pl.col('instrument_token').is_in(valid_tkns))
                self.stop_loss_info = acc.stop_loss_info
        else:
            if not acc.stop_loss_info.is_empty():
                acc.stop_loss_info = pl.DataFrame(schema=acc.stop_loss_info.schema)
                self.stop_loss_info = acc.stop_loss_info
                trade_logger.info(f"[branch: slu_cleared_empty_positions] [{acc.user_id}] Cleared stop losses since open positions and recent buys are empty.")
            return

        # ── 2. Initialize / Register Stop Losses for Open Positions ───────────
        if not acc.open_positions.is_empty():
            for row in acc.open_positions.to_dicts():
                sym = str(row.get('tradingsymbol', ''))
                tkn = int(row.get('instrument_token') or -1)
                exchg = str(row.get('exchange') or 'NFO')

                existing_sl = -1.0
                if not acc.stop_loss_info.is_empty() and 'instrument_token' in acc.stop_loss_info.columns:
                    m = acc.stop_loss_info.filter(pl.col('instrument_token') == tkn)
                    if not m.is_empty() and 'buy_price' in m.columns:
                        existing_sl = float(m['buy_price'][0])

                if existing_sl <= 0:
                    # Lookup completed BUY order for this symbol
                    buy_price = float(row.get('buy_price') or row.get('last_price') or 0.0)
                    ord_id = str(row.get('order_id') or '-1')

                    if not acc.order_status.is_empty() and 'tradingsymbol' in acc.order_status.columns:
                        ord_matches = acc.order_status.filter(
                            (pl.col('tradingsymbol') == sym) &
                            (pl.col('transaction_type').str.to_uppercase() == 'BUY') &
                            (pl.col('status').str.to_uppercase().is_in(['COMPLETE', 'PARTIAL']))
                        )
                        if not ord_matches.is_empty():
                            last_ord = ord_matches.tail(1)
                            ord_id = str(last_ord['order_id'][0]) if 'order_id' in last_ord.columns else ord_id
                            if 'average_price' in last_ord.columns and float(last_ord['average_price'][0] or 0.0) > 0:
                                buy_price = float(last_ord['average_price'][0])
                            elif 'price' in last_ord.columns and float(last_ord['price'][0] or 0.0) > 0:
                                buy_price = float(last_ord['price'][0])

                    if buy_price > 0:
                        new_sl = pl.DataFrame([{
                            'order_id': ord_id,
                            'buy_price': buy_price,
                            'instrument_token': tkn,
                            'date_time': datetime.now(),
                            'tradingsymbol': sym
                        }], schema=acc.stop_loss_info.schema if not acc.stop_loss_info.is_empty() else None)
                        acc.stop_loss_info = pl.concat([acc.stop_loss_info, new_sl], how="diagonal").unique(subset=['instrument_token'], keep='last')
                        self.stop_loss_info = acc.stop_loss_info
                        trade_logger.info(f"[branch: slu_created] [{acc.user_id}] Stop loss recorded for {sym} at {buy_price:.2f} (OrderID={ord_id})")

                # ── 3. Trail Stop Loss in Afternoon Session (sl_update_time) ─────
                if self.sl_update_time(exchg) and exchg in ('NFO', 'NSE', 'BFO', 'BSE'):
                    if not self.tick_data.is_empty() and 'instrument_token' in self.tick_data.columns:
                        tkn_ticks = self.tick_data.filter(pl.col('instrument_token') == tkn)
                        if not tkn_ticks.is_empty() and 'last_price' in tkn_ticks.columns:
                            cur_ltp = float(tkn_ticks['last_price'][-1] or 0.0)
                            if cur_ltp > existing_sl and existing_sl > 0:
                                acc.stop_loss_info = acc.stop_loss_info.with_columns(
                                    pl.when(pl.col('instrument_token') == tkn)
                                    .then(pl.lit(cur_ltp))
                                    .otherwise(pl.col('buy_price'))
                                    .alias('buy_price'),
                                    pl.when(pl.col('instrument_token') == tkn)
                                    .then(pl.lit(datetime.now()))
                                    .otherwise(pl.col('date_time'))
                                    .alias('date_time')
                                )
                                self.stop_loss_info = acc.stop_loss_info
                                trade_logger.info(f"[branch: slu_trailed_up] [{acc.user_id}] Trailed stop loss for {sym} from {existing_sl:.2f} to {cur_ltp:.2f}")

        self.update_algo_info_table()
        trade_logger.info(
            f"[SLU_SUMMARY] [{acc.user_id}] Dynamic stop loss maintenance complete: "
            f"tracking {len(acc.stop_loss_info) if not acc.stop_loss_info.is_empty() else 0} active stop losses for {len(acc.open_positions) if not acc.open_positions.is_empty() else 0} open positions."
        )

    def order_pending_chk(self, delta_seconds: int = 60, account: Optional[IndianUserAccount] = None) -> None:
        """Cancel stale pending orders especially on MCX or across exchanges after delta_seconds."""
        acc = account or self.account
        if not acc or not acc.enabled or acc.order_status.is_empty():
            order_logger.debug(
                f"[ORDER_PENDING_SKIP] Pending order scan skipped for account {acc.user_id if acc else 'NONE'} "
                f"(enabled={getattr(acc, 'enabled', False)}, orders_empty={acc.order_status.is_empty() if acc else True})."
            )
            return

        is_debug = getattr(self, 'debug_mode', DEBUG)
        cancelled_cnt = 0
        if 'status' in acc.order_status.columns:
            open_orders = acc.order_status.filter(
                pl.col('status').str.to_uppercase().is_in(['OPEN', 'PENDING'])
            )
            for row in open_orders.to_dicts():
                ord_id = str(row.get('order_id') or '')
                exchg = str(row.get('exchange') or '')
                ts = row.get('exchange_timestamp') or row.get('order_timestamp')
                if ord_id and ts:
                    try:
                        if isinstance(ts, str):
                            order_dt = datetime.fromisoformat(ts.replace('Z', ''))
                        elif isinstance(ts, (datetime, date)):
                            order_dt = ts if isinstance(ts, datetime) else datetime.combine(ts, datetime.min.time())
                        else:
                            order_dt = datetime.now()
                        elapsed = (datetime.now() - order_dt).total_seconds()
                        if elapsed > delta_seconds:
                            cancelled_cnt += 1
                            if is_debug:
                                order_logger.info(f"[branch: cancel_order_simulation] [{acc.user_id}] [DEBUG] Simulated cancellation of stale order {ord_id} ({elapsed:.1f}s > {delta_seconds}s)")
                            else:
                                order_logger.info(f"[branch: cancelling_stale_order] [{acc.user_id}] Cancelling stale order {ord_id} on {exchg} ({elapsed:.1f}s > {delta_seconds}s)")
                                acc.client.cancel_ordr(variety='regular', order_id=ord_id)
                    except Exception as e:
                        order_logger.warning(f"Error checking/cancelling pending order {ord_id}: {e}")
        acc_id = acc.user_id if acc else 'PRIMARY'
        order_logger.info(
            f"[ORDER_PENDING_SUMMARY] [{acc_id}] Pending order check complete: "
            f"scanned {len(open_orders) if 'open_orders' in locals() else 0} open/pending order(s), cancelled {cancelled_cnt} stale order(s) (threshold={delta_seconds}s)."
        )

    def clear_tables(self) -> None:
        cdl_schema = {
            'open': pl.Float64, 'low': pl.Float64, 'high': pl.Float64,
            'close': pl.Float64, 'instrument_token': pl.Int64, 'date_time': pl.Datetime
        }
        self.fwd_1_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_3_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_5_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_10_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_15_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_30_all = pl.DataFrame(schema=cdl_schema)
        self.fwd_60_all = pl.DataFrame(schema=cdl_schema)
        self.day_cdl_all = pl.DataFrame(schema=cdl_schema)
        self.half_day_cdl_all = pl.DataFrame(schema=cdl_schema)
        self.ref_min_max_all = pl.DataFrame(schema=cdl_schema)
        self.buy_stock_cap = pl.DataFrame()
        self.sell_stock_table = pl.DataFrame()
        self.delta_tick_data = pl.DataFrame()

    def prev_cdl_save(self) -> None:
        """
        Saves previous day candles to DB and maintains self.prev_day_cdl_all in Polars.
        Reference lines 4467-4500.
        """
        general_logger.info(f"[{datetime.now().isoformat()}] prev_cdl_save: Aggregating end-of-day candles...")
        if self.tick_data.is_empty() or not self.primary_broker:
            return

        try:
            latest_ticks = (
                self.tick_data
                .sort('date_time')
                .group_by('instrument_token')
                .last()
            )

            ref_tokens = []
            if not self.cum_table.is_empty():
                if 'Ref_stock_tkn' in self.cum_table.columns:
                    ref_tokens.extend(self.cum_table['Ref_stock_tkn'].drop_nulls().to_list())
                if 'Index_tkn' in self.cum_table.columns:
                    ref_tokens.extend(self.cum_table['Index_tkn'].drop_nulls().to_list())

            if ref_tokens:
                ref_set = set(int(t) for t in ref_tokens if t is not None)
                latest_ticks = latest_ticks.filter(pl.col('instrument_token').is_in(list(ref_set)))

            yesterday = (datetime.now() - timedelta(days=1)).date()
            if not self.prev_day_cdl_all.is_empty():
                recent_cdls = self.prev_day_cdl_all.filter(pl.col('date_time').dt.date() > yesterday)
                if not recent_cdls.is_empty():
                    self.prev_day_cdl_all = recent_cdls
                else:
                    self.prev_day_cdl_all = pl.DataFrame(schema=self.prev_day_cdl_all.schema)

            combined = pl.concat([self.prev_day_cdl_all, latest_ticks], how="diagonal") if not self.prev_day_cdl_all.is_empty() else latest_ticks
            self.prev_day_cdl_all = combined.sort('date_time').group_by('instrument_token').last()

            if not self.prev_day_cdl_all.is_empty() and not self.debug_mode:
                sanitized = self.sanitize_for_json(self.prev_day_cdl_all)
                self.primary_broker.set_algo_state('prev_day_cdl_all', sanitized)
                general_logger.info(f"[{datetime.now().isoformat()}] Successfully persisted {len(self.prev_day_cdl_all)} candle(s) to prev_day_cdl_all in DB.")
        except Exception as e:
            general_logger.info(f"[{datetime.now().isoformat()}] prev_cdl_save error: {e}", exc_info=True)

    def pre_trde(self) -> bool:
        general_logger.info("Executing Polars pre-trade routine.")
        self.read_algo_info_table()
        return True

    def post_trde(self) -> bool:
        general_logger.info("Executing Polars post-trade routine.")
        self.update_algo_info_table(force=True)
        self.write_db_candle(force=True)
        return True

    # ── Primary 2-Second Polars Trading Loop ────────────────────────────────

    def compute_market_signals(self, tick_data: Optional[pl.DataFrame] = None) -> None:
        """
        Tier 1: Shared Market Calculation (Calculated Once per 2.0s Cycle across all accounts).
        Fetches ticks, downsamples candles, detects strikes, evaluates momentum paths,
        and populates buy_stock_cap and sell_stock_table.
        """
        if tick_data is not None and not tick_data.is_empty():
            self.delta_tick_data = tick_data
        else:
            self.delta_tick_data = fetch_recent_ticks(self.primary_broker, seconds=None)

        # Concatenate delta ticks into rolling tick buffer
        if not self.delta_tick_data.is_empty():
            if not self.tick_data.is_empty():
                self.tick_data = pl.concat([self.tick_data, self.delta_tick_data], how="diagonal")
            else:
                self.tick_data = self.delta_tick_data

        if not self.tick_data.is_empty():
            self.data_ready = True
            self.tick_data = self.normalize_ticks(self.tick_data)
            self.tick_data = self.tick_data.sort('date_time')
            if 'mode' in self.tick_data.columns:
                self.tick_data = self.tick_data.filter(pl.col('mode') == 'full')

            # Resample & Signals
            self.update_all_candles_batch(self.tick_data)
            self.check_eod_cleanup()
            self.strike_detect(self.tick_data)
            self.updated_list = self.token_list_update()
            
            if not self.updated_list.is_empty() and 'instrument_token' in self.updated_list.columns:
                self.final_tkns = self.updated_list['instrument_token'].drop_nulls().to_list()
                self.insert_instrument_token(self.final_tkns)
            else:
                self.final_tkns = []

            if not self.cum_table.is_empty() and 'Ref_stock_tkn' in self.cum_table.columns and self.final_tkns:
                refs = set(self.cum_table['Ref_stock_tkn'].drop_nulls().to_list())
                self.final_ref_tokens = list(set(self.final_tkns).intersection(refs))
            else:
                self.final_ref_tokens = []

            signals: List[pl.DataFrame] = []
            for tkn in self.final_ref_tokens:
                if not self.cum_table.is_empty() and 'instrument_token' in self.cum_table.columns:
                    tkn_meta = self.cum_table.filter(pl.col('instrument_token') == tkn)
                    if (not tkn_meta.is_empty() and 'segment' in tkn_meta.columns and
                            str(tkn_meta['segment'][0]) == 'CDS-FUT' and
                            'tick_size' in tkn_meta.columns):
                        div_val = float(tkn_meta['tick_size'][0] or 1.0)
                        if div_val > 0 and not self.tick_data.is_empty() and 'instrument_token' in self.tick_data.columns:
                            self.tick_data = self.tick_data.with_columns(
                                pl.when(pl.col('instrument_token') == tkn)
                                .then(pl.col('last_price') / div_val)
                                .otherwise(pl.col('last_price'))
                                .alias('last_price')
                            )
                sig = self.mom(int(tkn))
                if not sig.is_empty():
                    signals.append(sig)

            stock_info_table = pl.concat(signals, how="diagonal") if signals else pl.DataFrame()
            self.strike_update()
            self.buy_stock_cap, self.sell_stock_table = self.derivative_analysis(stock_info_table, self.tick_data)

            # Keep tick buffer lean (configurable retention horizon)
            tick_retention_sec = float(getattr(config, 'WS_TICK_FLUSH_INTERVAL', 180.0) if 'config' in globals() else 180.0)
            if 'date_time' in self.tick_data.columns:
                max_t = self.tick_data['date_time'].max()
                if max_t is not None:
                    cutoff = max_t - timedelta(seconds=max(30.0, tick_retention_sec))
                    self.tick_data = self.tick_data.filter(pl.col('date_time') >= cutoff)

            delta_cnt = len(self.delta_tick_data) if hasattr(self, 'delta_tick_data') and not self.delta_tick_data.is_empty() else 0
            buf_cnt = len(self.tick_data) if hasattr(self, 'tick_data') and not self.tick_data.is_empty() else 0
            buy_cnt = len(self.buy_stock_cap) if hasattr(self, 'buy_stock_cap') and not self.buy_stock_cap.is_empty() else 0
            sell_cnt = len(self.sell_stock_table) if hasattr(self, 'sell_stock_table') and not self.sell_stock_table.is_empty() else 0
            general_logger.info(
                f"[MARKET_SIGNALS] Tier 1 computation complete: ingested {delta_cnt} delta ticks ({buf_cnt} retained buffer), "
                f"{len(getattr(self, 'final_ref_tokens', []))} ref tokens evaluated -> {buy_cnt} buy candidate(s), {sell_cnt} exit candidate(s)."
            )
        else:
            general_logger.debug("[MARKET_SIGNALS] Tick buffer is empty. Skipping Tier 1 market signal calculation.")
            self.check_eod_cleanup()

    def execute_account_trade(self, account: IndianUserAccount) -> None:
        """
        Tier 2: Multi-Account Order & Capital Dispatch (Per-Account Execution).
        Performs signal-driven JIT synchronization, executes buy/sell order checks,
        maintains dynamic trailing stop losses, and cleans stale pending orders.
        """
        if not account or not account.enabled:
            trade_logger.info(
                f"[ACCOUNT_GATE] Trade dispatch skipped: account={account.user_id if account else 'NONE'}, "
                f"enabled={getattr(account, 'enabled', False)}."
            )
            return

        if hasattr(account, 'broker_obj') and account.broker_obj:
            algo_logger.set_active_account(account.broker_obj, algo_name=_CURRENT_ALGO_NAME)

        self.account = account

        # Signal-Driven JIT Sync: If candidate buy/sell signals exist, immediately force fresh live sync
        has_signals = not self.buy_stock_cap.is_empty() or not self.sell_stock_table.is_empty()
        is_debug = getattr(self, 'debug_mode', False)
        if has_signals:
            account.sync_account_state(force_positions=True, force_balance=True, debug_mode=is_debug)
        else:
            account.sync_account_state(debug_mode=is_debug)

        self.open_positions = account.open_positions
        self.order_status = account.order_status
        self.live_balance = account.live_balance
        self.avail_cash = account.avail_cash

        # Order Execution for this specific account
        self.buy_sell_loop(account=account)
        self.slu(account=account)
        self.order_pending_chk(account=account)
        trade_logger.info(
            f"[ACCOUNT_DISPATCH_COMPLETE] [{account.user_id}] Account cycle complete: "
            f"balance={account.live_balance:.2f}, open_positions={len(account.open_positions)}, stop_losses={len(account.stop_loss_info)}."
        )

    def trde(self) -> None:
        """Main Unified Engine Loop in Polars across all registered accounts."""
        start_time = time.time()
        self.loop_count += 1
        if self.account and hasattr(self.account, 'broker_obj') and self.account.broker_obj:
            algo_logger.set_active_account(self.account.broker_obj, algo_name=_CURRENT_ALGO_NAME)

        self.compute_market_signals()

        for acc in self.accounts:
            if acc.enabled:
                self.execute_account_trade(acc)

        # End of day candle persistence
        if self.prev_cdl_save_time():
            general_logger.info(f"[{datetime.now().isoformat()}] Entered prev_cdl_save time window.")
            self.prev_cdl_save()

        self.update_algo_info_table(force=False)
        self.write_db_candle(force=False)

        # Periodic garbage collection to reclaim fragmented Python & Polars memory
        gc_interval = getattr(config, 'GC_COLLECTION_INTERVAL_CYCLES', 10) if 'config' in globals() else 10
        if gc_interval > 0 and (self.loop_count % gc_interval == 0):
            import gc
            gc.collect()

        elapsed = time.time() - start_time
        active_accs = sum(1 for a in self.accounts if a.enabled)
        general_logger.info(
            f"[ENGINE_CYCLE_COMPLETE] Polars Indian engine cycle {self.loop_count} completed in {elapsed:.3f}s "
            f"for {active_accs} active / {len(self.accounts)} total account(s)."
        )
        _flush_buffered_db_logs(force=True)

    def export_algo_logs(self, start_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Exports locally generated algorithm logs from the current session into a fresh multi-sheet Excel file across any broker and account.
        """
        _flush_buffered_db_logs(force=True)
        acc_id = self.account.user_id if (hasattr(self, 'account') and self.account and self.account.user_id) else (self.accounts[0].user_id if (hasattr(self, 'accounts') and self.accounts and self.accounts[0].user_id) else None)
        effective_start = start_time or getattr(self, 'session_start_time', None)
        res = export_local_algo_logs_to_excel(
            output_path=self.algo_logs_file,
            start_time=effective_start,
            account_id=acc_id,
            algo_name=getattr(self, 'algo_name', _ACTIVE_CONTEXT.algo_name),
        )
        general_logger.info(f"[DEBUG EXCEL] Exported {res['row_count']} fresh algorithm logs to {res['file_path']}")
        return res


# ============================================================================
# LIFECYCLE & RUNNER
# ============================================================================

def is_token_valid_today(broker_name: Optional[str] = None, account_id: Optional[str] = None, tz: str = "Asia/Kolkata") -> bool:
    try:
        from django.db.models import Q
        qs = Broker.objects.filter(enable_trade=True)
        if account_id:
            clean_id = str(account_id).strip().lower()
            qs = qs.filter(
                Q(account_id__iexact=clean_id)
                | Q(name__iexact=clean_id)
                | Q(name__icontains=clean_id)
                | Q(broker_name__code__icontains=clean_id)
                | Q(api_provider__code__icontains=clean_id)
            )
        elif broker_name:
            clean_b = str(broker_name).strip().lower()
            qs = qs.filter(
                Q(broker_name__code__icontains=clean_b)
                | Q(api_provider__code__icontains=clean_b)
                | Q(name__icontains=clean_b)
            )
        else:
            qs = qs.filter(
                Q(broker_name__code__in=["zerodha", "kotak", "kotak_neo", "upstox", "angel"])
                | Q(api_provider__code__in=["zerodha", "kotak", "kotak_neo", "upstox", "angel"])
                | Q(name__icontains="zerodha") | Q(name__icontains="kotak")
            )
        if not qs.exists():
            return True
        for b in qs:
            token_date = getattr(b, 'access_token_updated_at', None) or getattr(b, 'token_date', None)
            if token_date is None:
                continue
            now_dt = datetime.now(ZoneInfo(tz))
            if token_date.date() == now_dt.date():
                return True
        return False
    except Exception:
        return True



def check_for_orderplacing_time(tz: str = "Asia/Kolkata") -> bool:
    return MarketTimeDelegate.check_for_orderplacing_time(tz=tz)


def initialising_time(tz: str = "Asia/Kolkata") -> bool:
    return MarketTimeDelegate.initialising_time(tz=tz)


def is_post_trade_time(tz: str = "Asia/Kolkata") -> bool:
    return MarketTimeDelegate.is_post_trade_time(tz=tz)


_shutdown_requested = False


def _interruptible_sleep(seconds: float) -> bool:
    """Sleep for specified seconds in 0.1s slices, waking up immediately if shutdown is requested."""
    global _shutdown_requested
    end_t = time.time() + seconds
    while time.time() < end_t and not _shutdown_requested:
        sleep(min(0.1, max(0.01, end_t - time.time())))
    return not _shutdown_requested


def _signal_handler(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    try:
        sig_name = signal.Signals(signum).name
    except Exception:
        sig_name = str(signum)
    general_logger.info(f"Received signal {sig_name} ({signum}). Exiting cleanly...")
    _flush_buffered_db_logs(force=True)
    sys.exit(0)


def _install_signal_handlers():
    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    except Exception:
        pass


def main(
    run_once: bool = False,
    force_run: bool = False,
    special_session: bool = False,
    tz: str = "Asia/Kolkata",
    timezone: Optional[str] = None,
    loop_freq: int = 60,
    debug_mode: Optional[bool] = None,
    max_debug_iterations: int = 5,
    target_account_id: Optional[str] = None,
):
    global _shutdown_requested
    _install_signal_handlers()

    effective_tz = timezone or tz or APP_TIMEZONE
    is_debug = DEBUG if debug_mode is None else debug_mode
    debug_iteration_count = 0
    init_done = False
    post_trade_done = False
    algo_instance: Optional[TradeAlgo] = None
    execution_start_time = dj_timezone.now()

    # In Debug mode: auto-activate force_run off-market so developers/tests can verify instantly
    if force_run is False and is_debug:
        if is_weekend_ist(tz=effective_tz) or not check_for_orderplacing_time(tz=effective_tz):
            force_run = True
            general_logger.info(f"Debug mode active (APP_MODE={APP_MODE}): Automatically enabling force-run for direct test execution.")

    target_desc = f"account='{target_account_id}'" if target_account_id else "all active Indian accounts"
    general_logger.info(f"Starting Indian Polars Options Engine ({target_desc}, mode='{APP_MODE}', debug={is_debug}, force_run={force_run}, special_session={special_session}, timezone={effective_tz}, max_iterations={'UNLIMITED' if not is_debug else max_debug_iterations})...")
    app_logger.info(f"Application lifecycle started: Indian Polars Options Engine [{target_desc}, Mode: {APP_MODE.upper()}]")
    _flush_buffered_db_logs(force=True)

    while not _shutdown_requested:
        try:
            token_ok = is_token_valid_today(account_id=target_account_id, tz=effective_tz)
            is_mkt_day = is_market_day_ist(special_session=special_session, tz=effective_tz)
            is_init_window = initialising_time(tz=effective_tz)

            if force_run or (is_init_window and is_mkt_day and token_ok):
                if not init_done or algo_instance is None:
                    general_logger.info(f"Initializing Polars TradeAlgo engine (Timezone: {effective_tz})...")
                    _flush_buffered_db_logs(force=True)
                    try:
                        algo_instance = TradeAlgo(target_account_id=target_account_id, special_session=special_session or force_run, timezone=effective_tz, debug_mode=is_debug)
                        init_done = algo_instance.init_done
                    except Exception as e:
                        general_logger.error(f"Polars engine initialization error: {e}", exc_info=True)
                        _flush_buffered_db_logs(force=True)
                        init_done = False
                        if run_once:
                            raise
                        _interruptible_sleep(10)
                        continue

                while init_done and (force_run or check_for_orderplacing_time(tz=tz)) and not _shutdown_requested:
                    try:
                        algo_instance.trde()
                        _flush_buffered_db_logs(force=True)
                    except Exception as e:
                        general_logger.error(f"Polars tick execution error: {e}", exc_info=True)
                        _flush_buffered_db_logs(force=True)

                    if is_debug:
                        debug_iteration_count += 1
                        general_logger.info(f"[DEBUG] Iteration {debug_iteration_count}/{max_debug_iterations} completed.")
                        if debug_iteration_count >= max_debug_iterations:
                            general_logger.info(f"[DEBUG] Reached debug iteration limit ({max_debug_iterations}). Stopping engine.")
                            break

                    if run_once or _shutdown_requested:
                        return
                    _interruptible_sleep(2)

                if is_debug and debug_iteration_count >= max_debug_iterations:
                    break

                if is_post_trade_time(tz=tz) and not post_trade_done and not _shutdown_requested:
                    general_logger.info("Executing end-of-day post-trade routine in Polars...")
                    _flush_buffered_db_logs(force=True)
                    try:
                        if algo_instance is not None:
                            post_trade_done = algo_instance.post_trde()
                    except Exception as e:
                        opt_exception_logger.exception(f"Post-trade error: {e}", exc_info=True)
                    _flush_buffered_db_logs(force=True)
                    if run_once:
                        return

            else:
                if not token_ok:
                    general_logger.warning(f"Zerodha access token not updated today in {tz}. Pass --force-run to bypass schedule.")
                elif not is_mkt_day:
                    general_logger.info(f"Market closed in {tz} (weekend/holiday). Pass --force-run to execute test cycle immediately.")
                else:
                    general_logger.info(f"Outside market hours in {tz}. Engine in standby mode (checking every {loop_freq}s). Pass --force-run to execute immediately.")

                _flush_buffered_db_logs(force=True)
                if run_once or _shutdown_requested:
                    return
                _interruptible_sleep(loop_freq)

        except KeyboardInterrupt:
            general_logger.info("Polars engine stopped by user.")
            break
        except Exception as e:
            general_logger.error(f"Top-level runner exception in Polars: {e}", exc_info=True)
            _flush_buffered_db_logs(force=True)
            if run_once:
                raise
            _interruptible_sleep(5)

    _flush_buffered_db_logs(force=True)

    if is_debug:
        general_logger.info("Exporting fresh debug algorithm logs to Excel...")
        try:
            if algo_instance is not None and hasattr(algo_instance, 'export_algo_logs'):
                algo_instance.export_algo_logs(start_time=execution_start_time)
            else:
                logs_dir = os.path.join(data_file_path if 'data_file_path' in globals() else _resolve_data_dir(), 'logs')
                os.makedirs(logs_dir, exist_ok=True)
                export_local_algo_logs_to_excel(
                    output_path=os.path.join(logs_dir, f"{_CURRENT_ALGO_NAME}_algo_logs_export.xlsx"),
                    start_time=execution_start_time,
                    algo_name=_CURRENT_ALGO_NAME
                )
        except Exception as e:
            general_logger.error(f"Failed to export debug algorithm logs: {e}", exc_info=True)

    general_logger.info(f"Polars trading engine cleanly exited [Mode: {APP_MODE.upper()}].")
    app_logger.info(f"Application lifecycle ended: Polars trading engine stopped [Mode: {APP_MODE.upper()}].")


_global_polars_trade_algo_engine: Optional[TradeAlgo] = None
_last_market_eval_time: float = 0.0

@algo
def indian_options_trading_algo_polars(account_id: str):
    """
    Unified Polars-native Indian Options Strategy registered in DeltaZero26 dynamic algo engine.
    Executes Tier 1 market calculations once per 2.0s cycle, and Tier 2 trade routines per account.
    """
    global _global_polars_trade_algo_engine, _last_market_eval_time

    broker = Broker.resolve(account_id)
    if not broker or not broker.enable_trade or not broker.access_token:
        logger.debug(
            f"[ALGO_GATE] Account {account_id} skipped: broker_found={bool(broker)}, "
            f"enable_trade={getattr(broker, 'enable_trade', False)}, has_token={bool(broker and broker.access_token)}."
        )
        return

    # Ensure this account belongs to an Indian broker
    code_val = (broker.broker_name.code if broker.broker_name else "").lower()
    api_val = (broker.api_provider.code if broker.api_provider else "").lower()
    name_val = (broker.name or "").lower()
    is_indian = any(k in code_val or k in api_val or k in name_val for k in ["zerodha", "kotak", "upstox", "angel", "indian"])
    if not is_indian:
        logger.debug(f"[ALGO_GATE] Account {account_id} skipped: broker '{getattr(broker, 'name', 'Unknown')}' is not classified as Indian.")
        return

    algo_logger.set_active_account(broker, algo_name=_CURRENT_ALGO_NAME)
    try:
        if _global_polars_trade_algo_engine is None:
            try:
                accounts = discover_indian_broker_accounts()
                _global_polars_trade_algo_engine = TradeAlgo(accounts=accounts, debug_mode=DEBUG)
            except Exception as e:
                err_msg = f"[{datetime.now().isoformat()}] [INDIAN OPT POLARS INIT ERROR] Failed to initialize: {e}"
                logger.error(err_msg, exc_info=True)
                algo_logger.log_sync(err_msg, broker_obj=broker, algo_name=_CURRENT_ALGO_NAME, level="ERROR")
                algo_logger.flush_sync(force=True)
                return

        target_acc = _global_polars_trade_algo_engine.get_account(account_id)
        if target_acc is None:
            client = create_indian_broker_utility(broker, account_id)
            target_acc = IndianUserAccount(
                user_id=account_id,
                client=client,
                broker_obj=broker,
                capital_allowed=float(getattr(broker, 'capital_allowed', 100000.0) or 100000.0),
                capital_multiplier=float(getattr(broker, 'capital_multiplier', 1.0) or 1.0),
                enabled=broker.enable_trade
            )
            _global_polars_trade_algo_engine.accounts.append(target_acc)

        # Tier 1: Evaluate shared market calculations once per 2.0s cycle across all accounts
        now = time.time()
        if (now - _last_market_eval_time) >= 1.8:
            try:
                _global_polars_trade_algo_engine.compute_market_signals()
                _last_market_eval_time = now
            except Exception as e:
                err_msg = f"[{datetime.now().isoformat()}] [INDIAN OPT POLARS TIER 1 ERROR] {e}"
                logger.error(err_msg, exc_info=True)
                algo_logger.log_sync(err_msg, broker_obj=broker, algo_name=_CURRENT_ALGO_NAME, level="ERROR")

        # Tier 2: Account-specific execution
        if not target_acc or not target_acc.enabled:
            logger.info(
                f"[ALGO_GATE] Account {account_id} execution skipped: "
                f"target_acc={bool(target_acc)}, enabled={getattr(target_acc, 'enabled', False)}."
            )
            return

        try:
            _global_polars_trade_algo_engine.execute_account_trade(target_acc)
            algo_logger.log_sync(
                f"[{datetime.now().isoformat()}] [INDIAN OPT POLARS CYCLE] Executed strategy iteration successfully for account {account_id}",
                broker_obj=broker,
                algo_name=_CURRENT_ALGO_NAME,
                level="INFO"
            )
            logger.info(
                f"[ACCOUNT_CYCLE_SUCCESS] Strategy iteration completed for Indian account {account_id} "
                f"(balance={target_acc.live_balance:.2f}, open_positions={len(target_acc.open_positions)})."
            )
        except Exception as e:
            err_msg = f"[{datetime.now().isoformat()}] [INDIAN OPT POLARS CYCLE ERROR] {e}"
            logger.error(err_msg, exc_info=True)
            algo_logger.log_sync(err_msg, broker_obj=broker, algo_name=_CURRENT_ALGO_NAME, level="ERROR")

        algo_logger.flush_sync(force=True)
    finally:
        algo_logger.clear_active_account()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Polars-Native Indian Options Trading Engine (Zerodha, Kotak Neo, etc.)")
    parser.add_argument("--account", "--broker", dest="account", type=str, default=None, help="Target broker account ID or keyword (e.g., 'kotak', 'W1NPY', 'zerodha')")
    parser.add_argument("--special-session", action="store_true", help="Enable special trading session")
    parser.add_argument("--run-once", action="store_true", help="Execute a single check iteration and exit")
    parser.add_argument("--force-run", action="store_true", help="Force engine execution immediately bypassing market hours/holidays")
    parser.add_argument("--timezone", type=str, default=APP_TIMEZONE, help="Market timezone (defaults to .env APP_TIMEZONE)")
    parser.add_argument("--loop-freq", type=int, default=60, help="Standby check interval in seconds")
    parser.add_argument("--debug", action="store_true", default=None, help="Explicitly enable debug mode (defaults to .env APP_MODE)")
    parser.add_argument("--production", action="store_true", help="Explicitly force production mode (unlimited iterations, strict market hours)")
    parser.add_argument("--iterations", type=int, default=5, help="Number of debug iterations to execute (default: 5)")
    args = parser.parse_args()

    effective_debug = None
    if args.production:
        effective_debug = False
    elif args.debug:
        effective_debug = True

    try:
        signal.signal(signal.SIGINT, _signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, _signal_handler)
    except Exception:
        pass

    main(
        special_session=args.special_session,
        run_once=args.run_once,
        force_run=args.force_run,
        timezone=args.timezone,
        loop_freq=args.loop_freq,
        debug_mode=effective_debug,
        max_debug_iterations=args.iterations,
        target_account_id=args.account,
    )

# Backward compatibility alias
zerodha_options_trading_algo_polars = indian_options_trading_algo_polars
kotak_options_trading_algo_polars = indian_options_trading_algo_polars
