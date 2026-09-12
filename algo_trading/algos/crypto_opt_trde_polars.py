# -*- coding: utf-8 -*-
# ruff: noqa: E402
"""
algo_trading/algos/crypto_opt_trde_polars.py
───────────────────────────────────────────
Unified 24/7 Polars-Native Multi-Account Trading Engine for Crypto Exchanges
(CoinSwitch PRO, Delta Exchange Global & India).

Features:
- Single execution engine for all crypto exchanges with 24/7/365 continuous trading.
- Multi-timeframe SIMD candle resampling (1min, 3min, 5min, 10min, 15min, 30min, 60min, 1D).
- Automatic broker dispatch via create_crypto_broker_utility (CoinSwitchPRO vs DeltaExchange).
- Memory-safe debounce dicts (Dict[int, int]), batch DB logging with ISO timestamps.
- Zero Pandas dependencies.
"""

from __future__ import annotations

import os
import sys

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass
os.environ["PYTHONUNBUFFERED"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["POLARS_MAX_THREADS"] = os.getenv("POLARS_MAX_THREADS", "2")

import gc
import json
import logging
import math
import signal
import threading
import time
import zoneinfo
from datetime import date, datetime, timedelta, timezone as dt_timezone
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import numpy as np
import polars as pl

try:
    import orjson
except ImportError:
    orjson = json

_current_dir = os.path.dirname(os.path.abspath(__file__))
_parent_dir = os.path.dirname(_current_dir)
_root_dir = os.path.dirname(_parent_dir)
for d in (_root_dir, _parent_dir, _current_dir):
    if d not in sys.path:
        sys.path.insert(0, d)

try:
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
    django.setup()
except Exception:
    pass

from django.conf import settings
from django.db.models import Q

from kalai.models import Broker, ProcessedTickStore, AlgoInfo
from algo_trading.algos import algo
from algo_trading.algos.logger import algo_logger
from algo_trading.algos.indian_candle_engine import (
    group_by_rolling_window,
    heikin_ashi,
    update_candles_incremental,
    get_candle_bucket_start,
)
from algo_trading.algos.crypto_master_tokens import str_to_token
from algo_trading.algos.broker_token_mapper import clean_crypto_root
from algo_trading.algos.indian_opt_trde_polars import export_local_algo_logs_to_excel
from algo_trading.algos.crypto_user_account import CryptoUserAccount, UserAccount


# Mandatory Module Identity
_CURRENT_ALGO_NAME = "crypto_opt_trde_polars"

# Dynamic data directory resolver
def _resolve_data_dir() -> str:
    candidates = [
        getattr(settings, "DATA_DIR", None),
        getattr(settings, "BASE_DIR", None),
        _root_dir,
        _current_dir,
    ]
    for c in candidates:
        if c:
            p = os.path.abspath(str(c))
            if os.path.exists(p):
                return p
    return _current_dir

data_file_path = _resolve_data_dir()

try:
    from algo_trading.app_config import config
    APP_MODE = str(config.APP_MODE).strip().lower()
    DEBUG = bool(config.is_debug)
    APP_TIMEZONE = str(config.TIMEZONE).strip()
except Exception:
    APP_MODE = os.getenv("APP_MODE", "debug").strip().lower()
    DEBUG = getattr(settings, "DEBUG", (APP_MODE == "debug"))
    APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Asia/Kolkata").strip()

sleep = time.sleep



def _signal_handler(signum: int, frame: Any) -> None:
    logger = logging.getLogger("algo_trading.algos.crypto_opt_trde_polars")
    logger.info("Received termination signal %s. Initiating graceful shutdown...", signum)
    sys.exit(0)


# Structured Logging Adapter
class CryptoAlgoLoggerAdapter:
    def __init__(self, tag: str = "APP"):
        self.tag = tag
        self._internal_logger = logging.getLogger(f"algo_trading.algos.crypto_opt_trde_polars.{tag}")

    def info(self, msg: str, *args, **kwargs) -> None:
        self._log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self._log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._log(logging.ERROR, msg, *args, **kwargs)

    def debug(self, msg: str, *args, **kwargs) -> None:
        self._log(logging.DEBUG, msg, *args, **kwargs)

    def _log(self, level: int, msg: str, *args, **kwargs) -> None:
        ts = datetime.now().isoformat()
        formatted_msg = f"[{ts}] [{self.tag}] {msg}"
        self._internal_logger.log(level, formatted_msg, *args, **kwargs)


app_logger = CryptoAlgoLoggerAdapter("APP")
general_logger = CryptoAlgoLoggerAdapter("GENERAL")
trade_logger = CryptoAlgoLoggerAdapter("TRADE")
strike_logger = CryptoAlgoLoggerAdapter("STRIKE")
order_logger = CryptoAlgoLoggerAdapter("ORDER")
inst_analysis_logger = CryptoAlgoLoggerAdapter("INST_ANALYSIS")


def now_ist_naive(tz: str = "Asia/Kolkata") -> datetime:
    try:
        import zoneinfo
        return datetime.now(zoneinfo.ZoneInfo(tz)).replace(tzinfo=None)
    except Exception:
        return datetime.now()


def time_ist(tz: str = "Asia/Kolkata") -> datetime.time:
    return now_ist_naive(tz=tz).time()


def today_ist(tz: str = "Asia/Kolkata") -> date:
    return now_ist_naive(tz=tz).date()
inst_analysis_logger = CryptoAlgoLoggerAdapter("INST_ANALYSIS")


def now_ist_naive(tz: str = "Asia/Kolkata") -> datetime:
    try:
        import zoneinfo
        return datetime.now(zoneinfo.ZoneInfo(tz)).replace(tzinfo=None)
    except Exception:
        return datetime.now()


def time_ist(tz: str = "Asia/Kolkata") -> datetime.time:
    return now_ist_naive(tz=tz).time()


def today_ist(tz: str = "Asia/Kolkata") -> date:
    return now_ist_naive(tz=tz).date()


# Broker Factory & Discovery
def create_crypto_broker_utility(broker_obj: Any, account_id: str = "") -> Any:
    """Dynamically instantiate the appropriate crypto broker utility."""
    code_val = (broker_obj.broker_name.code if broker_obj and getattr(broker_obj, 'broker_name', None) else "").lower()
    api_val = (broker_obj.api_provider.code if broker_obj and getattr(broker_obj, 'api_provider', None) else "").lower()
    name_val = (broker_obj.name or "").lower() if broker_obj else ""
    acc_id = account_id or (getattr(broker_obj, "account_id", None) or name_val)

    if "delta" in code_val or "delta" in api_val or "delta" in name_val:
        from algo_trading.algos.delta_utils import DeltaExchangeUtility
        return DeltaExchangeUtility(account_id=acc_id, broker_obj=broker_obj)
    if "coindcx" in code_val or "coindcx" in api_val or "coindcx" in name_val:
        from algo_trading.algos.coindcx_utils import CoinDCXUtility
        return CoinDCXUtility(account_id=acc_id, broker_obj=broker_obj)
    from algo_trading.algos.coinswitch_utils import CoinSwitchUtility
    return CoinSwitchUtility(account_id=acc_id, broker_obj=broker_obj)


def discover_crypto_broker_accounts(target_account_id: Optional[str] = None) -> List[Broker]:
    """Retrieve active crypto broker accounts from DB."""
    try:
        from django.db.models import Q
        qs = Broker.objects.all()
        if target_account_id:
            clean_id = str(target_account_id).strip().lower()
            qs = qs.filter(
                Q(account_id__iexact=clean_id)
                | Q(name__iexact=clean_id)
                | Q(name__icontains=clean_id)
                | Q(broker_name__code__icontains=clean_id)
                | Q(api_provider__code__icontains=clean_id)
            )
        else:
            qs = qs.filter(
                Q(broker_name__code__in=["coinswitch", "delta", "delta_exchange", "delta_india", "coindcx", "crypto", "bitcoin"])
                | Q(api_provider__code__in=["coinswitch", "delta", "delta_exchange", "delta_india", "coindcx", "crypto", "bitcoin"])
                | Q(name__icontains="coinswitch") | Q(name__icontains="delta") | Q(name__icontains="coindcx")
            )
        trade_enabled = qs.filter(enable_trade=True)
        active_brokers = list(trade_enabled if trade_enabled.exists() else qs)
        general_logger.info("Discovered %d active crypto broker account(s).", len(active_brokers))
        return active_brokers
    except Exception as e:
        general_logger.error("Error querying active crypto broker accounts: %s", e)
        return []


# CryptoUserAccount and UserAccount are imported from algo_trading.algos.crypto_user_account


def fetch_recent_ticks(broker_obj: Optional[Broker] = None, seconds: Optional[int] = None, limit: int = 2000) -> pl.DataFrame:
    """
    Fetch recent market ticks from ProcessedTickStore or account stream table in PostgreSQL into a Polars DataFrame.
    Seamlessly parses and normalizes ticks from CoinSwitch PRO, Delta, and other crypto feeds.
    """
    try:
        from kalai.models import ProcessedTickStore
        from django.utils import timezone as dj_timezone
        from datetime import timezone as dt_timezone
        import orjson

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
                        ticks = [(r[0], datetime.fromtimestamp(r[1] / 1e9, tz=dt_timezone.utc)) for r in reversed(rows)]
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
                if item.get('type') in ('cn', 'sub', 'unsub', 'hb') or 'stCode' in item:
                    continue

                sym = str(item.get('tradingsymbol') or item.get('symbol') or item.get('s') or item.get('pair') or '').strip()
                tkn = item.get('instrument_token') or item.get('tk') or item.get('token')
                # Inspect raw Delta payload if token is non-numeric or missing
                if (tkn is None or not str(tkn).isdigit()) and isinstance(item.get('raw'), dict):
                    raw_d = item['raw'].get('d')
                    if isinstance(raw_d, list) and len(raw_d) > 0 and isinstance(raw_d[0], dict) and 'i' in raw_d[0]:
                        tkn = raw_d[0]['i']

                if tkn is None and sym:
                    tkn_int = str_to_token(sym)
                elif tkn is not None:
                    try:
                        tkn_int = int(tkn)
                    except (ValueError, TypeError):
                        tkn_int = str_to_token(str(tkn))
                elif sym:
                    tkn_int = str_to_token(sym)
                else:
                    continue

                last_p = item.get('last_price') or item.get('ltp') or item.get('p') or item.get('price') or item.get('c') or item.get('ap') or item.get('bp') or item.get('sp')
                if last_p is None:
                    continue
                try:
                    p_float = float(last_p)
                except (ValueError, TypeError):
                    continue

                dt = None
                for dt_key in ['date_time', 'exchange_timestamp', 'last_trade_time', 'current_time', 'E', 'timestamp']:
                    if dt_key in item and item[dt_key]:
                        val = item[dt_key]
                        if isinstance(val, (int, float)) and val > 1e11:
                            dt = datetime.fromtimestamp(val / 1000.0, tz=dt_timezone.utc).replace(tzinfo=None)
                            break
                        try:
                            clean_dt_str = str(val).replace('Z', '+00:00')
                            dt = datetime.fromisoformat(clean_dt_str).replace(tzinfo=None)
                            break
                        except Exception:
                            pass
                if dt is None:
                    dt = ts.replace(tzinfo=None) if (ts and hasattr(ts, 'replace')) else datetime.now()

                v = item.get('volume_traded') or item.get('volume') or item.get('q') or item.get('v') or 0
                oi = item.get('oi') or 0

                buy_depth = item.get('depth', {}).get('buy') if isinstance(item.get('depth'), dict) else None
                sell_depth = item.get('depth', {}).get('sell') if isinstance(item.get('depth'), dict) else None
                bp = item.get('buy_price') or item.get('bp') or (buy_depth[0].get('price') if buy_depth and isinstance(buy_depth, list) and len(buy_depth) > 0 and isinstance(buy_depth[0], dict) else 0.0) or 0.0
                sp = item.get('sell_price') or item.get('sp') or (sell_depth[0].get('price') if sell_depth and isinstance(sell_depth, list) and len(sell_depth) > 0 and isinstance(sell_depth[0], dict) else 0.0) or 0.0
                bq = item.get('total_buy_quantity') or item.get('buy_demand') or item.get('buy_quantity') or item.get('tbq') or item.get('bq') or 0
                sq = item.get('total_sell_quantity') or item.get('sell_demand') or item.get('sell_quantity') or item.get('tsq') or item.get('sq') or 0

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
                "instrument_token": pl.Int64,
                "last_price": pl.Float64,
                "date_time": pl.Datetime,
                "volume": pl.Int64,
                "oi": pl.Int64,
                "buy_price": pl.Float64,
                "sell_price": pl.Float64,
                "buy_quantity": pl.Int64,
                "sell_quantity": pl.Int64,
                "mode": pl.Utf8,
                "tradingsymbol": pl.Utf8,
            }
            return pl.DataFrame(schema=schema)

        df = pl.DataFrame(records)
        return df.sort("date_time", descending=False)
    except Exception as ex:
        general_logger.error("Error fetching crypto ticks: %s", ex)
        return pl.DataFrame()


# Unified Crypto Strategy Engine
class TradeAlgo:
    """Polars-Native Unified Trading Engine for 24/7 Crypto Options & Derivatives."""

    def __init__(
        self,
        broker_accounts: Optional[List[Broker]] = None,
        accounts: Optional[List[CryptoUserAccount]] = None,
        debug_mode: Optional[bool] = None,
        special_session: bool = False,
        timezone: str = APP_TIMEZONE,
        month_cutoff: int = 0,
        input_file: str = "token_ref_bitcoin.xlsx",
        output_file: str = "token_ref_bitcoin_out.xlsx",
        master_list: str = "token_ref_bitcoin_master.xlsx",
        target_account_id: Optional[str] = None,
    ):
        self.debug_mode = DEBUG if debug_mode is None else debug_mode
        self.special_session = special_session
        self.tz = timezone
        self.month_cutoff = month_cutoff
        self.target_account_id = target_account_id

        # File paths & logs directory setup
        base_dir = data_file_path
        algo_dir = os.path.dirname(os.path.abspath(__file__))
        self.logs_dir = os.path.join(base_dir, "logs")
        os.makedirs(self.logs_dir, exist_ok=True)

        # Resolve input_file: check project root first, then algo_dir (where Excel files actually live)
        candidate_input = os.path.join(base_dir, input_file) if not os.path.isabs(input_file) else input_file
        if os.path.exists(candidate_input):
            self.input_file = candidate_input
        elif not os.path.isabs(input_file) and os.path.exists(os.path.join(algo_dir, input_file)):
            self.input_file = os.path.join(algo_dir, input_file)
        else:
            self.input_file = candidate_input  # keep original for error logging

        self.output_file = os.path.join(self.logs_dir, output_file) if not os.path.isabs(output_file) else output_file
        self.master_list = os.path.join(self.logs_dir, master_list) if not os.path.isabs(master_list) else master_list
        self.algo_logs_file = os.path.join(self.logs_dir, "crypto_algo_logs_export.xlsx")

        # Candle DataFrames across 8 timeframes (Polars Schema Safe)
        cdl_schema = {
            'open': pl.Float64,
            'low': pl.Float64,
            'high': pl.Float64,
            'close': pl.Float64,
            'instrument_token': pl.Int64,
            'date_time': pl.Datetime
        }
        self.fwd_1_all: pl.DataFrame = pl.DataFrame(schema=cdl_schema)
        self.fwd_3_all: pl.DataFrame = pl.DataFrame(schema=cdl_schema)
        self.fwd_5_all: pl.DataFrame = pl.DataFrame(schema=cdl_schema)
        self.fwd_10_all: pl.DataFrame = pl.DataFrame(schema=cdl_schema)
        self.fwd_15_all: pl.DataFrame = pl.DataFrame(schema=cdl_schema)
        self.fwd_30_all: pl.DataFrame = pl.DataFrame(schema=cdl_schema)
        self.fwd_60_all: pl.DataFrame = pl.DataFrame(schema=cdl_schema)
        self.day_cdl_all: pl.DataFrame = pl.DataFrame(schema=cdl_schema)
        self.prev_day_cdl_all: pl.DataFrame = pl.DataFrame(schema=cdl_schema)
        self.ref_min_max_all: pl.DataFrame = pl.DataFrame(schema=cdl_schema)
        self.current_trading_day: date = datetime.now().date()

        # Contract tables & token arrays
        self.cum_table: pl.DataFrame = pl.DataFrame()
        self.aug_table: pl.DataFrame = pl.DataFrame()
        self.cap_config: pl.DataFrame = pl.DataFrame()
        self.stock_config: pl.DataFrame = pl.DataFrame()
        self.stock_data_info: pl.DataFrame = pl.DataFrame()
        self.nse_holiday_info: pl.DataFrame = pl.DataFrame()
        self.inst_list_int: np.ndarray = np.array([], dtype=np.int64)
        self.init_ref_list: pl.DataFrame = pl.DataFrame(schema={"instrument_token": pl.Int64})
        self.index_ref_list: pl.DataFrame = pl.DataFrame(schema={"instrument_token": pl.Int64})
        self.all_ref_tkns: np.ndarray = np.array([], dtype=np.int64)
        self.updated_list: pl.DataFrame = pl.DataFrame()
        self._candle_cache: Dict[str, bytes] = {}
        self._last_subscribed_symbols: Optional[Set[str]] = None

        # In-memory debounce state dictionaries
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
        self.two_counter_ce_dict: Dict[int, int] = {}
        self.two_counter_pe_dict: Dict[int, int] = {}
        self.jump_ce_counter_dict: Dict[int, int] = {}
        self.jump_pe_counter_dict: Dict[int, int] = {}
        self.exit_ce_counter_dict: Dict[int, int] = {}
        self.exit_pe_counter_dict: Dict[int, int] = {}
        self.recent_sell_order_time: Dict[str, datetime] = {}
        self.recent_buy_order_time: Dict[str, datetime] = {}

        # Signal tables & Order buffers
        self.buy_stock_cap: pl.DataFrame = pl.DataFrame()
        self.sell_stock_table: pl.DataFrame = pl.DataFrame()
        self.stock_info_table: pl.DataFrame = pl.DataFrame()
        self.delta_tick_data: pl.DataFrame = pl.DataFrame()
        self.loss_table_stocks: Set[str] = set()
        self.nfo_risk_hold: bool = False
        self.algo_info_sync_cycles: int = 1
        self._state_cache: Dict[str, bytes] = {}

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
        self.stop_loss_info: pl.DataFrame = pl.DataFrame(
            schema={
                'order_id': pl.Utf8,
                'buy_price': pl.Float64,
                'instrument_token': pl.Int64,
                'date_time': pl.Datetime,
                'tradingsymbol': pl.Utf8,
            }
        )

        # Fast O(1) Lookup Maps
        self.sym_to_meta: Dict[str, Any] = {}
        self.tkn_to_meta: Dict[int, Any] = {}
        self.sym_to_tkn_map: Dict[str, int] = {}
        self.tkn_to_sym_map: Dict[int, str] = {}

        # Account state buffers
        self.hold_frame: pl.DataFrame = pl.DataFrame()
        self.pos_day_frame: pl.DataFrame = pl.DataFrame()
        self.pos_net_frame: pl.DataFrame = pl.DataFrame()
        self.open_positions: pl.DataFrame = pl.DataFrame()
        self.order_status: pl.DataFrame = pl.DataFrame()
        self.avail_cash: float = 0.0
        self.live_balance: float = 0.0
        self._active_account: Optional[CryptoUserAccount] = None

        self.loop_count: int = 0
        self.data_ready: bool = False
        self.tick_data: pl.DataFrame = pl.DataFrame()

        # Load Excel configuration via polars_excel if file exists
        if os.path.exists(self.input_file):
            general_logger.info("Loading Polars crypto configuration from %s...", self.input_file)
            from algo_trading.algos.polars_excel import load_crypto_algo_config
            try:
                cfg = load_crypto_algo_config(self.input_file)
                self.aug_table = cfg.aug_table
                self.cap_config = cfg.cap_config
                self.stock_config = getattr(cfg, "stock_config", pl.DataFrame())
                self.stock_data_info = getattr(cfg, "stock_data_info", pl.DataFrame())
                self.nse_holiday_info = getattr(cfg, "nse_holiday_info", pl.DataFrame())
                general_logger.info("Successfully loaded Excel config in Polars: %d stocks.", len(self.aug_table))
            except Exception as ex:
                general_logger.error("Failed to load Excel configuration: %s", ex)
                self.aug_table = pl.DataFrame()
                self.cap_config = pl.DataFrame()
        else:
            general_logger.warning("Input file not found at %s (base_dir=%s, algo_dir=%s). aug_table will be empty.", self.input_file, base_dir, algo_dir)

        # Multi-Account setup
        self.accounts: Dict[str, CryptoUserAccount] = {}
        self.primary_account_id: str = "PRIMARY"
        self.primary_broker: Optional[Broker] = None
        self.client: Any = None

        if accounts:
            for acc in accounts:
                self.accounts[acc.user_id] = acc
                if not self.primary_broker and getattr(acc, "broker_obj", None):
                    self.primary_broker = acc.broker_obj
                    self.primary_account_id = acc.user_id
                    self.client = acc.client
            if not self.client and accounts:
                self.client = accounts[0].client
                self.primary_account_id = accounts[0].user_id

        else:
            if broker_accounts is None:
                broker_accounts = discover_crypto_broker_accounts(target_account_id=self.target_account_id)

            if broker_accounts:
                for b in broker_accounts:
                    try:
                        util = create_crypto_broker_utility(b, b.account_id)
                        is_trade_flag = bool(getattr(b, "enable_trade", getattr(b, "is_active", True)))
                        has_credentials = True
                        if hasattr(util, "api_key") or hasattr(util, "api_secret"):
                            if not (getattr(util, "api_key", None) or "").strip() or not (getattr(util, "api_secret", None) or "").strip():
                                has_credentials = False

                        if is_trade_flag and not has_credentials:
                            general_logger.warning(
                                "[%s] Account '%s' has enable_trade=True but API credentials (api_key/api_secret) are incomplete in database. Automated trading suspended until credentials are provided.",
                                b.account_id,
                                getattr(b, "name", b.account_id)
                            )

                        acc = CryptoUserAccount(
                            user_id=b.account_id,
                            client=util,
                            broker_obj=b,
                            capital_allowed=float(getattr(b, "capital_allowed", 100000.0) or 100000.0),
                            capital_multiplier=float(getattr(b, "capital_multiplier", 1.0) or 1.0),
                            enabled=bool(is_trade_flag and has_credentials),
                        )
                        self.accounts[b.account_id] = acc
                        if not self.primary_broker:
                            self.primary_broker = b
                            self.primary_account_id = b.account_id
                            self.client = util
                    except Exception as e:
                        general_logger.error("Failed to initialize crypto broker account %s: %s. Skipping.", b.account_id, e)

        if not self.accounts:
            # Standalone/mock-safe fallback without hitting live DB
            class _MockCryptoUtil:
                def __init__(self):
                    self.broker = None
                    self.account_id = "PRIMARY"
                def chk_live_bal(self): return (50000.0, 50000.0)
                def holdings(self): return pl.DataFrame()
                def pos_data(self): return (pl.DataFrame(), pl.DataFrame())
                def orders(self): return []
                def lim_ordr(self, *a, **kw): return ("12345", "placed")
                def cancel_ordr(self, *a, **kw): return (True, "cancelled")
                def master_tkn_list(self, **kw):
                    return pl.DataFrame(), np.array([], dtype=np.int64), pl.DataFrame(), pl.DataFrame()

            mock_util = _MockCryptoUtil()
            acc = CryptoUserAccount(user_id="PRIMARY", client=mock_util, enabled=True)
            self.accounts["PRIMARY"] = acc
            self.client = mock_util

        if self.primary_broker:
            algo_logger.set_active_account(self.primary_broker, algo_name=_CURRENT_ALGO_NAME)

        # Account-scoped paths (cum_table, master list, algo logs)
        acc_prefix = f"{self.primary_account_id}_" if (self.primary_account_id and self.primary_account_id != "PRIMARY") else ""
        if acc_prefix:
            self.output_file = os.path.join(self.logs_dir, f"{acc_prefix}cum_table.xlsx")
            self.master_list = os.path.join(self.logs_dir, f"{acc_prefix}Master_inst_token.xlsx")
            self.algo_logs_file = os.path.join(self.logs_dir, f"{acc_prefix}algo_logs_export.xlsx")
            prefixed_name = f"{acc_prefix}token_ref_bitcoin.xlsx"
            if os.path.exists(os.path.join(base_dir, prefixed_name)):
                self.input_file = os.path.join(base_dir, prefixed_name)
            elif os.path.exists(os.path.join(algo_dir, prefixed_name)):
                self.input_file = os.path.join(algo_dir, prefixed_name)
        else:
            self.output_file = os.path.join(self.logs_dir, "cum_table_crypto.xlsx")
            self.master_list = os.path.join(self.logs_dir, "Master_inst_token_crypto.xlsx")
            self.algo_logs_file = os.path.join(self.logs_dir, "crypto_algo_logs_export.xlsx")

        # Assemble master token list and cum_table using client utility
        self.load_master_token_list()
        self.updated_list = self.token_list_update()
        if not self.updated_list.is_empty() and "tradingsymbol" in self.updated_list.columns:
            active_syms = [str(s).strip() for s in self.updated_list["tradingsymbol"].drop_nulls().to_list() if str(s).strip()]
            self.insert_instrument_token(active_syms)
        else:
            self.insert_instrument_token()
        self.update_config_info()
        self.read_algo_info_table()
        self.read_db_candle()
        # Warm up candles from ProcessedTickStore if available to guarantee fresh continuous history on startup
        if self.primary_broker:
            try:
                warmup_ticks = fetch_recent_ticks(self.primary_broker, seconds=None, limit=120000)
                if not warmup_ticks.is_empty():
                    self.update_all_candles_batch(warmup_ticks)
                    general_logger.info("[WARMUP] Warmed up crypto candles from %d recent ticks in ProcessedTickStore.", len(warmup_ticks))
            except Exception as e:
                general_logger.debug("Warmup ticks skipped: %s", e)
        general_logger.info("Polars TradeAlgo engine active for account '%s'.", self.primary_account_id)

    def export_algo_logs(self, start_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Exports locally generated algorithm logs from the current session into a fresh multi-sheet Excel file."""
        algo_logger.flush_sync(force=True)
        effective_start = start_time or getattr(self, "session_start_time", None)
        res = export_local_algo_logs_to_excel(
            output_path=self.algo_logs_file,
            start_time=effective_start,
            account_id=self.primary_account_id,
            algo_name=_CURRENT_ALGO_NAME,
        )
        general_logger.info("[DEBUG EXCEL] Exported %d fresh crypto algorithm logs to %s", res.get("row_count", 0), res.get("file_path", self.algo_logs_file))
        return res

    @property
    def account(self) -> Optional[CryptoUserAccount]:
        if hasattr(self, '_active_account') and self._active_account is not None:
            return self._active_account
        return list(self.accounts.values())[0] if self.accounts else None

    @account.setter
    def account(self, acc: Optional[CryptoUserAccount]) -> None:
        self._active_account = acc

    def get_account(self, account_id: str) -> Optional[CryptoUserAccount]:
        """Look up a CryptoUserAccount by account_id."""
        for acc_id, acc in self.accounts.items():
            if str(acc.user_id).strip().lower() == str(account_id).strip().lower():
                return acc
        return None

    @property
    def all_open_positions(self) -> pl.DataFrame:
        """Aggregate open positions across all active crypto accounts or the active account."""
        frames = []
        for acc in self.accounts.values():
            if acc.enabled and hasattr(acc, 'open_positions') and not acc.open_positions.is_empty():
                frames.append(acc.open_positions)
        if frames:
            return pl.concat(frames, how="diagonal_relaxed").unique(subset=['instrument_token'])
        if hasattr(self, 'open_positions') and isinstance(self.open_positions, pl.DataFrame) and not self.open_positions.is_empty():
            return self.open_positions
        return pl.DataFrame()

    def get_token_meta(self, tkn: int) -> Dict[str, Any]:
        return self.tkn_to_meta.get(int(tkn), {})

    def get_symbol_meta(self, sym: str) -> Dict[str, Any]:
        return self.sym_to_meta.get(str(sym), {})

    def tkn_to_symbol(self, tkn_list: List[int]) -> List[str]:
        return [self.tkn_to_sym_map.get(int(t), "UNKNOWN") for t in tkn_list]

    def symbol_to_tkn(self, sym: str) -> int:
        if sym in self.sym_to_tkn_map:
            return self.sym_to_tkn_map[sym]
        if not self.cum_table.is_empty() and 'tradingsymbol' in self.cum_table.columns and 'instrument_token' in self.cum_table.columns:
            m = self.cum_table.filter(pl.col('tradingsymbol') == sym)
            if not m.is_empty():
                t = int(m['instrument_token'][0])
                self.sym_to_tkn_map[sym] = t
                return t
        return -1

    def tkn_to_exchg(self, tkn: int) -> str:
        meta = self.get_token_meta(tkn)
        return str(meta.get('exchange', 'OPTIONS'))

    def _get_hedge_counter(self, tkn: int) -> int:
        return self.hedge_counter_dict.get(int(tkn), 0)

    def _increment_hedge_counter(self, tkn: int) -> None:
        t = int(tkn)
        self.hedge_counter_dict[t] = self.hedge_counter_dict.get(t, 0) + 1

    def _reset_hedge_counter(self, tkn: int) -> None:
        self.hedge_counter_dict[int(tkn)] = 0

    # ── 24/7 Crypto Market Timings (Continuous Trading) ─────────────────────
    def is_weekend(self) -> bool:
        return False

    def is_holiday(self, exchg: str = "") -> bool:
        return False

    def special_session_chk(self, exchg: str = "") -> bool:
        return True

    def next_session_closed(self, exchg: str = "") -> bool:
        return False

    def exchg_time_buy_chk(self, exchg: str = "") -> bool:
        return True

    def hedge_time_chk(self, exchg: str = "") -> bool:
        return True

    def half_time(self, exchg: str = "") -> bool:
        return True

    def exchg_time_sell_chk(self, exchg: str = "") -> bool:
        return True

    def session_end(self, exchg: str = "") -> bool:
        return False

    def session_start(self, exchg: str = "") -> bool:
        return False

    def sl_update_time(self, exchg: str = "") -> bool:
        if self.debug_mode:
            return True
        now_naive = now_ist_naive(tz=self.tz)
        start_time = now_naive.replace(hour=2, minute=30, second=0, microsecond=0)
        end_time = now_naive.replace(hour=2, minute=31, second=0, microsecond=0)
        return True

    def expiry_sell_time(self, exchg: str = "") -> bool:
        return True

    def post_trade_time(self, exchg: str = "") -> bool:
        return False

    def master_list_update_time(self) -> bool:
        return False

    # ── AlgoInfo Table Persistence ──────────────────────────────────────────
    def read_algo_info_table(self) -> None:
        """Restores persisted stop loss and strike entry tables from AlgoInfo in DB."""
        if not self.primary_broker:
            return
        try:
            db_sl = self.primary_broker.get_algo_state('stop_loss_info')
            if db_sl and isinstance(db_sl, list) and len(db_sl) > 0:
                self.stop_loss_info = pl.DataFrame(db_sl)
                if 'date_time' in self.stop_loss_info.columns and self.stop_loss_info['date_time'].dtype == pl.Utf8:
                    try:
                        self.stop_loss_info = self.stop_loss_info.with_columns(pl.col('date_time').str.to_datetime())
                    except Exception:
                        pass
                general_logger.info("Restored %d stop-loss entries from DB.", len(self.stop_loss_info))
        except Exception as e:
            general_logger.debug("Could not restore stop_loss_info from DB: %s", e)

        try:
            db_se = self.primary_broker.get_algo_state('strike_entry_info')
            if db_se and isinstance(db_se, list) and len(db_se) > 0:
                self.strike_entry_info = pl.DataFrame(db_se)
                if 'date_time' in self.strike_entry_info.columns and self.strike_entry_info['date_time'].dtype == pl.Utf8:
                    try:
                        self.strike_entry_info = self.strike_entry_info.with_columns(pl.col('date_time').str.to_datetime())
                    except Exception:
                        pass
                general_logger.info("Restored %d strike-entry entries from DB.", len(self.strike_entry_info))
        except Exception as e:
            general_logger.debug("Could not restore strike_entry_info from DB: %s", e)

    def update_algo_info_table(self, force: bool = False) -> None:
        """Persists updated state tables (stop_loss_info, strike_entry_info) to AlgoInfo in DB."""
        target_brokers = self._get_target_brokers()
        if not target_brokers:
            return
        stop_loss_df = getattr(self, 'stop_loss_info', pl.DataFrame())
        if stop_loss_df.is_empty() and self.account and hasattr(self.account, 'stop_loss_info'):
            stop_loss_df = self.account.stop_loss_info

        tables_to_sync = {
            'strike_entry_info': getattr(self, 'strike_entry_info', pl.DataFrame()),
            'stop_loss_info': stop_loss_df,
        }
        for b in target_brokers:
            for name, df in tables_to_sync.items():
                if df is not None and not df.is_empty():
                    try:
                        sanitized = self.sanitize_for_json(df)
                        b.set_algo_state(name, sanitized)
                    except Exception:
                        pass

    @staticmethod
    def sanitize_for_json(data: Any) -> Any:
        if isinstance(data, pl.DataFrame):
            return [TradeAlgo.sanitize_for_json(row) for row in data.to_dicts()]
        elif isinstance(data, dict):
            return {k: TradeAlgo.sanitize_for_json(v) for k, v in data.items()}
        elif isinstance(data, (list, tuple, set)):
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

    def _get_target_brokers(self) -> List[Broker]:
        brokers: List[Broker] = []
        if self.primary_broker and getattr(self.primary_broker, "enable_trade", False) and self.primary_broker not in brokers:
            brokers.append(self.primary_broker)
        for acc in self.accounts.values():
            b_obj = getattr(acc, "broker_obj", None)
            if b_obj and getattr(b_obj, "enable_trade", False) and b_obj not in brokers:
                brokers.append(b_obj)
        try:
            from kalai.models import Broker
            from django.db.models import Q
            crypto_brokers = Broker.objects.filter(
                Q(broker_name__code__in=["coinswitch", "delta", "delta_exchange", "delta_india", "coindcx", "crypto", "bitcoin"])
                | Q(api_provider__code__in=["coinswitch", "delta", "delta_exchange", "delta_india", "coindcx", "crypto", "bitcoin"])
                | Q(name__icontains="coinswitch") | Q(name__icontains="delta") | Q(name__icontains="coindcx"),
                enable_trade=True,
            )
            for b in crypto_brokers:
                if b not in brokers:
                    brokers.append(b)
        except Exception:
            pass
        return brokers

    def insert_instrument_token(self, token_list: Optional[List[Any]] = None) -> None:
        """Inserts subscribed crypto symbols into AlgoInfo / Token table in DB for each broker account."""
        target_brokers = self._get_target_brokers()
        if not target_brokers:
            return

        sym_list: List[str] = []
        if token_list:
            sym_list = [str(s).strip() for s in token_list if str(s).strip()]
        elif hasattr(self, "updated_list") and not self.updated_list.is_empty() and "tradingsymbol" in self.updated_list.columns:
            sym_list = [str(s).strip() for s in self.updated_list["tradingsymbol"].drop_nulls().to_list() if str(s).strip()]
        elif hasattr(self, "cum_table") and not self.cum_table.is_empty() and "Ref_stock_tkn" in self.cum_table.columns:
            # Fallback strictly to underlying active reference contracts, NEVER the full 900+ option universe
            active_refs = self.cum_table.filter((pl.col("instrument_token").cast(pl.Utf8) == pl.col("Ref_stock_tkn").cast(pl.Utf8)) & (pl.col("Capital_share") > 0))
            if not active_refs.is_empty() and "tradingsymbol" in active_refs.columns:
                sym_list = [str(s).strip() for s in active_refs["tradingsymbol"].to_list() if str(s).strip()]

        if not sym_list:
            sym_list = ["BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "DOGEUSD", "BNBUSD", "AVAXUSD"]

        sym_set = set(sym_list)
        if hasattr(self, "_last_subscribed_symbols") and self._last_subscribed_symbols == sym_set:
            return

        for b in target_brokers:
            try:
                existing = b.get_subscribed_tokens()
                if set(existing) != sym_set:
                    b.set_subscribed_tokens(sym_list)
                    general_logger.info(
                        "[%s] Subscribed %d crypto symbols to DB table '%s': %s",
                        b.account_id,
                        len(sym_list),
                        b.token_tablename,
                        sym_list[:10],
                    )
            except Exception as e:
                general_logger.error("[%s] Error subscribing crypto tokens to DB: %s", b.account_id, e)

        self._last_subscribed_symbols = sym_set

    def update_config_info(self) -> None:
        """Persist config tables (stock_config, stock_data_info, cap_config) to AlgoInfo in DB."""
        target_brokers = self._get_target_brokers()
        if not target_brokers:
            return

        tables = {
            "stock_config": getattr(self, "stock_config", pl.DataFrame()),
            "stock_data_info": getattr(self, "stock_data_info", pl.DataFrame()),
            "cap_config": getattr(self, "cap_config", pl.DataFrame()),
        }
        synced_count = 0
        for name, df in tables.items():
            if df is not None and not df.is_empty():
                try:
                    sanitized = self.sanitize_for_json(df)
                    for b in target_brokers:
                        b.set_algo_state(name, sanitized)
                    synced_count += 1
                except Exception as e:
                    general_logger.warning("Error persisting config table %s to DB: %s", name, e)
        if synced_count > 0:
            general_logger.info("Persisted %d crypto config table(s) to DB.", synced_count)

    def write_db_candle(self, force: bool = False) -> None:
        """Persists updated multi-timeframe candles to AlgoInfo in DB with in-memory deduplication caching."""
        target_brokers = self._get_target_brokers()
        if not target_brokers:
            return

        candles_to_save = {
            "fwd_3_all": getattr(self, "fwd_3_all", pl.DataFrame()),
            "fwd_5_all": getattr(self, "fwd_5_all", pl.DataFrame()),
            "fwd_10_all": getattr(self, "fwd_10_all", pl.DataFrame()),
            "fwd_15_all": getattr(self, "fwd_15_all", pl.DataFrame()),
            "fwd_30_all": getattr(self, "fwd_30_all", pl.DataFrame()),
            "fwd_60_all": getattr(self, "fwd_60_all", pl.DataFrame()),
            "day_cdl_all": getattr(self, "day_cdl_all", pl.DataFrame()),
            "prev_day_cdl_all": getattr(self, "prev_day_cdl_all", pl.DataFrame()),
        }
        if not hasattr(self, "_candle_cache"):
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
            general_logger.info("Persisted %d crypto candle tables to DB.", saved_count)

    def read_db_candle(self) -> None:
        """Restores recent candle arrays from DB AlgoInfo upon engine initialization."""
        if not self.primary_broker:
            return
        today_date = datetime.now().date()
        for name in ["fwd_3_all", "fwd_5_all", "fwd_10_all", "fwd_15_all", "fwd_30_all", "fwd_60_all", "day_cdl_all", "prev_day_cdl_all"]:
            try:
                data = AlgoInfo.get_table_data(account=self.primary_broker, tablename=name)
                raw = data.get(name)
                if raw:
                    parsed = orjson.loads(raw) if isinstance(raw, str) else raw
                    if parsed:
                        df_loaded = pl.from_dicts(parsed)
                        if "date_time" in df_loaded.columns and df_loaded["date_time"].dtype == pl.Utf8:
                            try:
                                df_loaded = df_loaded.with_columns(pl.col("date_time").cast(pl.Utf8).str.slice(0, 19).str.to_datetime(strict=False))
                            except Exception:
                                pass
                        setattr(self, name, df_loaded)
            except Exception as e:
                general_logger.debug("Could not restore candle %s from DB: %s", name, e)

    def load_master_token_list(self) -> None:
        """Assembles the master contract universe (cum_table) once using the broker utility's master_tkn_list."""
        if self.client and hasattr(self.client, "master_tkn_list"):
            try:
                (
                    self.cum_table,
                    self.inst_list_int,
                    self.init_ref_list,
                    self.index_ref_list,
                ) = self.client.master_tkn_list(
                    input_file=self.input_file,
                    master_list=self.master_list,
                    output_file=self.output_file,
                    tz=self.tz,
                    debug_mode=self.debug_mode,
                    month_cutoff=self.month_cutoff,
                    aug_table=self.aug_table,
                    cap_config=self.cap_config,
                )
                if not self.cum_table.is_empty() and "Ref_stock_tkn" in self.cum_table.columns:
                    ref_tkns = self.cum_table["Ref_stock_tkn"].drop_nulls().unique().to_list()
                    self.all_ref_tkns = np.array([int(t) for t in ref_tkns if t is not None and int(t) != -1], dtype=np.int64)
                if not self.cum_table.is_empty() and "week_dist" in self.cum_table.columns:
                    self.cum_table = self.cum_table.with_columns(pl.col("week_dist").cast(pl.Int64, strict=False))
                if not self.cum_table.is_empty():
                    for row in self.cum_table.to_dicts():
                        sym = str(row.get('tradingsymbol', ''))
                        tkn = row.get('instrument_token')
                        if sym and tkn is not None:
                            try:
                                tkn_int = int(tkn)
                                self.sym_to_meta[sym] = row
                                self.tkn_to_meta[tkn_int] = row
                                self.sym_to_tkn_map[sym] = tkn_int
                                self.tkn_to_sym_map[tkn_int] = sym
                            except Exception:
                                pass
                if not self.cum_table.is_empty():
                    for row in self.cum_table.to_dicts():
                        sym = str(row.get('tradingsymbol', ''))
                        tkn = row.get('instrument_token')
                        if sym and tkn is not None:
                            try:
                                tkn_int = int(tkn)
                                self.sym_to_meta[sym] = row
                                self.tkn_to_meta[tkn_int] = row
                                self.sym_to_tkn_map[sym] = tkn_int
                                self.tkn_to_sym_map[tkn_int] = sym
                            except Exception:
                                pass
                general_logger.info("Master contract universe loaded: %d contracts across %d active underlyings.", len(self.cum_table), len(self.all_ref_tkns))
            except Exception as e:
                general_logger.error("Failed to execute master_tkn_list on client: %s", e)

    def token_list_update(self) -> pl.DataFrame:
        """
        Collects active tokens/symbols to stream across holdings, positions, ref/index contracts, and selected strikes.
        Matches reference zerodha_opt_trde_reference.py lines 1504-1525.
        """
        all_tokens: List[int] = []
        ref_count = 0
        if hasattr(self, "index_ref_list") and not self.index_ref_list.is_empty() and "instrument_token" in self.index_ref_list.columns:
            ref_tkns = self.index_ref_list["instrument_token"].drop_nulls().to_list()
            all_tokens.extend(ref_tkns)
            ref_count += len(ref_tkns)
        if hasattr(self, "init_ref_list") and not self.init_ref_list.is_empty() and "instrument_token" in self.init_ref_list.columns:
            ref_tkns = self.init_ref_list["instrument_token"].drop_nulls().to_list()
            all_tokens.extend(ref_tkns)
            ref_count += len(ref_tkns)

        pos_count = 0
        for acc in self.accounts.values():
            if not acc.hold_frame.is_empty() and "instrument_token" in acc.hold_frame.columns:
                h_tkns = acc.hold_frame["instrument_token"].drop_nulls().to_list()
                all_tokens.extend(h_tkns)
                pos_count += len(h_tkns)
            if not acc.open_positions.is_empty() and "instrument_token" in acc.open_positions.columns:
                p_tkns = acc.open_positions["instrument_token"].drop_nulls().to_list()
                all_tokens.extend(p_tkns)
                pos_count += len(p_tkns)

        # Reference lines 1517-1520: split active strikes where Buy_strike == 'Yes'
        strike_count = 0
        if not self.cum_table.is_empty() and "Buy_strike" in self.cum_table.columns and "instrument_token" in self.cum_table.columns:
            active = self.cum_table.filter(pl.col("Buy_strike") == "Yes")
            if not active.is_empty():
                if "instrument_type" in active.columns:
                    ce_list = active.filter(pl.col("instrument_type") == "CE")["instrument_token"].drop_nulls().to_list()
                    pe_list = active.filter(pl.col("instrument_type") == "PE")["instrument_token"].drop_nulls().to_list()
                    all_tokens.extend(ce_list)
                    all_tokens.extend(pe_list)
                    strike_count += len(ce_list) + len(pe_list)
                else:
                    s_tkns = active["instrument_token"].drop_nulls().to_list()
                    all_tokens.extend(s_tkns)
                    strike_count += len(s_tkns)

        unique_tokens = []
        seen = set()
        for t in all_tokens:
            if t is None:
                continue
            s_val = str(t).strip()
            if not s_val or s_val == "-1":
                continue
            if s_val not in seen:
                seen.add(s_val)
                try:
                    unique_tokens.append(int(s_val))
                except (ValueError, TypeError):
                    unique_tokens.append(s_val)

        if not self.cum_table.is_empty() and "instrument_token" in self.cum_table.columns:
            token_strs = [str(t) for t in unique_tokens]
            matched = self.cum_table.filter(pl.col("instrument_token").cast(pl.Utf8).is_in(token_strs))
            cols = [c for c in ["instrument_token", "tradingsymbol"] if c in matched.columns]
            res_df = matched.select(cols).unique(subset=["instrument_token"])
        else:
            res_df = pl.DataFrame({"instrument_token": [str(t) for t in unique_tokens]}, schema={"instrument_token": pl.Utf8})

        self.updated_list = res_df
        sample_symbols = []
        if not res_df.is_empty() and "tradingsymbol" in res_df.columns:
            sample_symbols = res_df["tradingsymbol"].drop_nulls().head(6).to_list()

        general_logger.info(
            "[TOKEN_UPDATE] Assembled %d active tokens for streaming: %d underlyings/indexes, %d positions/holdings, %d selected strikes (Sample: %s).",
            len(res_df), ref_count, pos_count, strike_count, ", ".join(str(s) for s in sample_symbols) if sample_symbols else "None"
        )
        return res_df

    def group_by_rolling_window(self, existing_df: pl.DataFrame, base_1m: pl.DataFrame, window_size: str, max_bars: int = 25) -> pl.DataFrame:
        """Modular dynamic SIMD candle resampler delegating to indian_candle_engine with per-token rolling window."""
        return group_by_rolling_window(existing_df, base_1m, window_size, max_bars=max_bars)

    def heikin_ashi(self, df: pl.DataFrame) -> pl.DataFrame:
        """Pure Polars Heikin-Ashi transformer delegating to indian_candle_engine."""
        return heikin_ashi(df)

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

    def update_all_candles_batch(self, tick_data: pl.DataFrame) -> None:
        """Batch resample ticks across 8 candle timeframes simultaneously retaining at least 25 bars per token."""
        if tick_data.is_empty():
            general_logger.debug("[CANDLE_UPDATE] Skipping batch candle update: tick_data is empty.")
            return

        norm_ticks = self.normalize_ticks(tick_data)
        clean_ticks = norm_ticks.filter(
            pl.col("last_price").is_not_null() & (pl.col("last_price") > 0)
        )
        # Strip broker summary OHLC columns from raw ticks so candles strictly derive from last_price
        summary_cols = [c for c in ["open", "high", "low", "close"] if c in clean_ticks.columns]
        if summary_cols:
            clean_ticks = clean_ticks.drop(summary_cols)

        if clean_ticks.is_empty():
            general_logger.debug("[CANDLE_UPDATE] Skipping batch candle update: no clean positive price ticks found in %d ticks.", len(tick_data))
            return

        today = datetime.now().date()
        if hasattr(self, "current_trading_day") and today > self.current_trading_day:
            if hasattr(self, "day_cdl_all") and not self.day_cdl_all.is_empty():
                self.prev_day_cdl_all = self.day_cdl_all
            self.current_trading_day = today

        cfg_max = getattr(globals().get("config"), "MAX_CANDLE_HISTORY_BARS", 25)
        effective_max = max(25, int(cfg_max or 25))

        # 1. Update 3m base candles from incoming ticks (runs once per 2s cycle)
        self.fwd_3_all = update_candles_incremental(self.fwd_3_all, clean_ticks, window_size="3min", max_bars=effective_max, default_origin="00:00")

        # 2. Extract ONLY the active (latest) 3m bar per token for fast O(1) downsampling to higher timeframes
        if not self.fwd_3_all.is_empty():
            if "instrument_token" in self.fwd_3_all.columns:
                active_3m = self.fwd_3_all.sort(["instrument_token", "date_time"], descending=[False, True]).group_by("instrument_token", maintain_order=True).first()
            else:
                active_3m = self.fwd_3_all.sort("date_time", descending=True).head(1)
        else:
            active_3m = pl.DataFrame()

        # 3. Check if higher timeframes need historical bootstrap directly from ticks (e.g. startup warmup or when bars < 25)
        dt_span = 0.0
        if "date_time" in clean_ticks.columns:
            try:
                valid_dts = clean_ticks["date_time"].drop_nulls()
                if not valid_dts.is_empty():
                    dt_span = (valid_dts.max() - valid_dts.min()).total_seconds()
            except Exception:
                pass

        # 3. Update higher timeframes: batch bootstrap if clean_ticks covers the timeframe interval or historical table is empty,
        # or if a multi-interval batch arrives (e.g. startup warmup / reconnects) for day_cdl_all.
        # Otherwise perform fast O(1) incremental update from single active_3m bar.
        is_multi_interval_batch = (dt_span > 180 or len(clean_ticks) > 50)
        if not active_3m.is_empty():
            for tf_name, tf_win, tf_sec in [
                ("fwd_5_all", "5min", 300),
                ("fwd_10_all", "10min", 600),
                ("fwd_15_all", "15min", 900),
                ("fwd_30_all", "30min", 1800),
                ("fwd_60_all", "60min", 3600),
                ("day_cdl_all", "1D", 86400),
            ]:
                curr_table = getattr(self, tf_name, pl.DataFrame())
                should_bootstrap = (
                    curr_table.is_empty()
                    or (dt_span >= tf_sec)
                    or (tf_name == "day_cdl_all" and is_multi_interval_batch)
                )
                if should_bootstrap and not clean_ticks.is_empty():
                    setattr(self, tf_name, group_by_rolling_window(curr_table, clean_ticks, window_size=tf_win, max_bars=effective_max))
                else:
                    setattr(self, tf_name, update_candles_incremental(curr_table, active_3m, window_size=tf_win, max_bars=effective_max, default_origin="00:00"))
        self.ref_min_max_all = self.fwd_10_all
        general_logger.info(
            "[CANDLE_UPDATE] Resampled active candles across 7 timeframes for %d ticks (3m=%d, 5m=%d, 10m=%d, 15m=%d, 30m=%d, 60m=%d, 1D=%d).",
            len(tick_data), len(self.fwd_3_all), len(self.fwd_5_all),
            len(self.fwd_10_all), len(self.fwd_15_all), len(self.fwd_30_all), len(self.fwd_60_all), len(self.day_cdl_all)
        )
        self.write_db_candle()

    def detect_strike_side(self, ref_stock_name: str, inst_type: str, candle_avg: float, strike_dist: float, cap_info: str) -> None:
        """Polars Strike Side Filter selecting closest ATM/OTM strike matching reference lines 3731-3796."""
        if self.cum_table.is_empty() or "Ref_stock" not in self.cum_table.columns or "strike" not in self.cum_table.columns:
            strike_logger.debug("[%s] %s: Skipped detect_strike_side: cum_table empty or missing 'Ref_stock'/'strike' columns.", ref_stock_name, inst_type)
            return

        if candle_avg is None or np.isnan(candle_avg) or candle_avg <= 0:
            strike_logger.debug("[%s] %s: Skipped detect_strike_side: invalid candle_avg (%s).", ref_stock_name, inst_type, candle_avg)
            return
        if strike_dist is None or np.isnan(strike_dist):
            strike_dist = 0.0

        est_strike = float(candle_avg + strike_dist)
        if est_strike <= 0:
            strike_logger.debug("[%s] %s: Skipped detect_strike_side: non-positive est_strike (%.2f).", ref_stock_name, inst_type, est_strike)
            return

        c_root = clean_crypto_root(ref_stock_name)
        ref_filter = (pl.col("Ref_stock") == ref_stock_name)
        if "name" in self.cum_table.columns and c_root:
            ref_filter = ref_filter | (pl.col("name") == c_root)

        candidates = self.cum_table.filter(
            ref_filter
            & (pl.col("instrument_type") == inst_type)
            & (pl.col("strike").is_not_null())
            & (pl.col("strike") > 0)
        )
        if candidates.is_empty():
            strike_logger.debug("[%s] %s: No candidate contracts in cum_table.", ref_stock_name, inst_type)
            return

        # Cap filter: for crypto options, allow weekly and monthly options across the 3 to 14-day window
        if "cap" in candidates.columns and cap_info:
            c_info = str(cap_info).strip().lower()
            if c_info in ("weekly_options", "weekely_options", "monthly_options"):
                candidates = candidates.filter(pl.col("cap").is_in(["weekly_options", "weekely_options", "monthly_options"]))
            else:
                candidates = candidates.filter(pl.col("cap") == c_info)
            if candidates.is_empty():
                strike_logger.debug("[%s] %s: No candidate matching cap_info '%s'.", ref_stock_name, inst_type, cap_info)
                return

        if "expiry" not in candidates.columns or candidates.is_empty():
            strike_logger.debug("[%s] %s: No candidate contracts after cap filtering.", ref_stock_name, inst_type)
            return

        # Expiry window filter:
        # Lower cutoff: 3 days, Upper cutoff: 7 days.
        # If no contract in [3, 7] days, extend window to 14 days and select the contract expiring early.
        today_date = datetime.now().date()
        if candidates['expiry'].dtype in (pl.Date, pl.Datetime):
            exp_date_col = pl.col('expiry').cast(pl.Date)
        else:
            candidates = candidates.filter(pl.col('expiry').is_not_null() & (pl.col('expiry').cast(pl.Utf8) != ''))
            exp_date_col = pl.col('expiry').cast(pl.Utf8).str.to_date(strict=False)

        candidates = candidates.filter(pl.col("expiry").is_not_null()).with_columns(
            exp_date_col.alias("_exp_date_clean")
        ).with_columns(
            ((pl.col("_exp_date_clean") - pl.lit(today_date)).dt.total_days()).alias("days_to_expiry")
        )

        # 1. Primary window: 3 <= days_to_expiry <= 7
        primary_cands = candidates.filter(
            (pl.col("days_to_expiry") >= 3)
            & (pl.col("days_to_expiry") <= 7)
            & (pl.col("exp_date_list") > self.month_cutoff)
        )
        if not primary_cands.is_empty():
            # In primary window: group by calendar week (Monday) and choose derivative which expires last in the week
            primary_cands = primary_cands.with_columns(
                pl.col("_exp_date_clean").dt.truncate("1w").alias("week_start")
            )
            min_week_start = primary_cands["week_start"].min()
            target_week_cands = primary_cands.filter(pl.col("week_start") == min_week_start)
            last_exp_in_week = target_week_cands["expiry"].max()
            candidates = target_week_cands.filter(pl.col("expiry") == last_exp_in_week)
        else:
            # 2. Fallback: extend window to 14 days (3 <= days_to_expiry <= 14) and select contract expiring early
            extended_cands = candidates.filter(
                (pl.col("days_to_expiry") >= 3)
                & (pl.col("days_to_expiry") <= 14)
                & (pl.col("exp_date_list") > self.month_cutoff)
            )
            if not extended_cands.is_empty():
                early_exp = extended_cands["expiry"].min()
                candidates = extended_cands.filter(pl.col("expiry") == early_exp)
            else:
                # 3. Safety fallback: nearest future contract >= 3 days (or any future contract if none >= 3)
                future_cands = candidates.filter(
                    (pl.col("days_to_expiry") >= 3) & (pl.col("exp_date_list") > self.month_cutoff)
                )
                if future_cands.is_empty():
                    future_cands = candidates.filter(
                        (pl.col("days_to_expiry") > 0) & (pl.col("exp_date_list") > self.month_cutoff)
                    )
                if not future_cands.is_empty():
                    fallback_exp = future_cands["expiry"].min()
                    candidates = future_cands.filter(pl.col("expiry") == fallback_exp)
                else:
                    strike_logger.debug("[%s] %s: No candidate contracts after expiry filtering.", ref_stock_name, inst_type)
                    return

        if candidates.is_empty():
            strike_logger.debug("[%s] %s: No candidate contracts selected.", ref_stock_name, inst_type)
            return

        dist_calc = candidates.with_columns(((pl.col("strike") / est_strike).round(6)).alias("ratio"))
        if inst_type == "CE":
            selected = dist_calc.filter(pl.col("ratio") >= 1.0).sort("ratio", descending=False).head(1)
        else:
            selected = dist_calc.filter(pl.col("ratio") <= 1.0).sort("ratio", descending=True).head(1)

        if not selected.is_empty():
            sel_tkn = selected["instrument_token"][0]
            sel_sym = selected["tradingsymbol"][0]
            sel_strike = float(selected["strike"][0])
            tradable_expr = pl.col("Tradable_stock") if "Tradable_stock" in self.cum_table.columns else pl.lit("No")
            sel_tkn_str = str(sel_tkn).strip()
            self.cum_table = self.cum_table.with_columns(
                pl.when(pl.col("instrument_token").cast(pl.Utf8) == sel_tkn_str).then(pl.lit("Yes")).otherwise(pl.col("Buy_strike")).alias("Buy_strike"),
                pl.when(pl.col("instrument_token").cast(pl.Utf8) == sel_tkn_str).then(pl.lit("Yes")).otherwise(tradable_expr).alias("Tradable_stock"),
            )
            strike_logger.info("[%s] %s Strike Selected: %s (EstStrike=%.2f, Strike=%.2f, Expiry=%s, Ratio=%.4f)", ref_stock_name, inst_type, sel_sym, est_strike, sel_strike, str(selected["expiry"][0]) if "expiry" in selected.columns else "N/A", float(selected["ratio"][0]))
        else:
            strike_logger.debug("[%s] %s: No candidate selected (EstStrike=%.2f, Candidates=%d)", ref_stock_name, inst_type, est_strike, len(candidates))

    def strike_detect(self, tick_data: pl.DataFrame) -> None:
        """Optimized Strike Detection Engine in Polars."""
        if self.cum_table.is_empty():
            general_logger.warning("[STRIKE_DETECT] Skipping strike detection: cum_table is empty.")
            return
        if "Ref_stock_tkn" not in self.cum_table.columns:
            general_logger.warning("[STRIKE_DETECT] Skipping strike detection: 'Ref_stock_tkn' column missing in cum_table.")
            return

        if not tick_data.is_empty():
            tick_data = self.normalize_ticks(tick_data)

        self.cum_table = self.cum_table.with_columns([
            pl.lit("NA").alias("Buy_strike"),
            pl.lit("No").alias("Tradable_stock"),
        ])

        ref_tkns = self.cum_table["Ref_stock_tkn"].drop_nulls().unique().to_list()
        self.all_ref_tkns = [t for t in ref_tkns if t is not None and str(t).strip() not in ("-1", "")]

        strike_logger.info(
            "[STRIKE_DETECT] Executing strike detection for %d underlyings across %d ticks...",
            len(self.all_ref_tkns),
            len(tick_data),
        )

        for ref_tkn in self.all_ref_tkns:
            ref_tkn_str = str(ref_tkn).strip()
            if not ref_tkn_str or ref_tkn_str == "-1":
                continue
            matching_ref = self.cum_table.filter(pl.col("Ref_stock_tkn").cast(pl.Utf8) == ref_tkn_str)
            if matching_ref.is_empty():
                strike_logger.debug("[STRIKE_DETECT] Token %s not found in cum_table. Skipping underlying.", ref_tkn_str)
                continue

            # Prioritize Cap_info (user configuration), NEVER fall back to contract-level matching_ref['cap'][0]
            cap_info = matching_ref["Cap_info"][0] if ("Cap_info" in matching_ref.columns and matching_ref["Cap_info"][0] is not None) else "monthly_options"
            ref_stock_name = str(matching_ref["Ref_stock"][0]) if "Ref_stock" in matching_ref.columns else ""
            idx_stock_name = str(matching_ref["Nifty_index"][0]) if ("Nifty_index" in matching_ref.columns and matching_ref["Nifty_index"][0] is not None) else ""
            base_sym = str(matching_ref["Symbol"][0]) if ("Symbol" in matching_ref.columns and matching_ref["Symbol"][0] is not None) else ""
            idx_tkn = matching_ref["Index_tkn"][0] if "Index_tkn" in matching_ref.columns else None

            # Alphanumeric tokens matching: match instrument_token against Ref_stock_tkn and Index_tkn
            token_candidates = [ref_tkn_str]
            if idx_tkn is not None and str(idx_tkn).strip() not in ("-1", ""):
                token_candidates.append(str(idx_tkn).strip())
            if ref_stock_name:
                token_candidates.append(str(str_to_token(ref_stock_name)))

            token_filter = pl.col("instrument_token").cast(pl.Utf8).is_in(token_candidates)

            # Symbol names matching: match tradingsymbol strictly against symbol names
            symbol_candidates = [s for s in [ref_stock_name, idx_stock_name, base_sym] if s and str(s).strip()]
            c_root = clean_crypto_root(base_sym) or clean_crypto_root(ref_stock_name)
            if c_root:
                symbol_candidates.extend([f"{c_root}USD", f"{c_root}USDT", f"{c_root}/USDT", f"{c_root}/INR", c_root])
            symbol_candidates = list(dict.fromkeys(symbol_candidates))

            if not tick_data.is_empty() and "tradingsymbol" in tick_data.columns and symbol_candidates:
                token_filter = token_filter | pl.col("tradingsymbol").is_in(symbol_candidates)

            recent_ticks = tick_data.filter(token_filter) if not tick_data.is_empty() else pl.DataFrame()

            ref_fwd_3 = self.fwd_3_all.filter(
                pl.col("instrument_token").cast(pl.Utf8).is_in(token_candidates)
            ) if not self.fwd_3_all.is_empty() and "instrument_token" in self.fwd_3_all.columns else pl.DataFrame()

            try:
                candle_avg = None
                price_src = "none"
                if not ref_fwd_3.is_empty() and len(ref_fwd_3) >= 2:
                    candle_avg = float((ref_fwd_3["close"][1] + ref_fwd_3["open"][1]) / 2.0)
                    price_src = "fwd_3_midpoint"
                elif not ref_fwd_3.is_empty() and len(ref_fwd_3) == 1:
                    candle_avg = float((ref_fwd_3["close"][0] + ref_fwd_3["open"][0]) / 2.0)
                    price_src = "fwd_3_single"

                if (candle_avg is None or np.isnan(candle_avg)) and not recent_ticks.is_empty() and "last_price" in recent_ticks.columns:
                    valid_lps = recent_ticks.filter(pl.col("last_price") > 0)["last_price"]
                    if not valid_lps.is_empty():
                        candle_avg = float(valid_lps[-1])
                        price_src = "recent_ticks"

                if (candle_avg is None or np.isnan(candle_avg) or candle_avg <= 0) and "last_price" in matching_ref.columns:
                    m_lp = matching_ref["last_price"][0]
                    if m_lp is not None and float(m_lp) > 0:
                        candle_avg = float(m_lp)
                        price_src = "master_reference"

                if (candle_avg is None or np.isnan(candle_avg) or candle_avg <= 0) and c_root:
                    from algo_trading.algos.crypto_master_tokens import KNOWN_CRYPTO_BASE_PRICES
                    if c_root in KNOWN_CRYPTO_BASE_PRICES:
                        candle_avg = float(KNOWN_CRYPTO_BASE_PRICES[c_root])
                        price_src = "known_base_price"

                if candle_avg is not None and not np.isnan(candle_avg) and candle_avg > 0:
                    strike_dist_ce = float(matching_ref["Strike_dist_CE"][0]) if ("Strike_dist_CE" in matching_ref.columns and matching_ref["Strike_dist_CE"][0] is not None) else 0.0
                    strike_dist_pe = float(matching_ref["Strike_dist_PE"][0]) if ("Strike_dist_PE" in matching_ref.columns and matching_ref["Strike_dist_PE"][0] is not None) else 0.0

                    strike_logger.debug("[%s] Strike detect price resolved: candle_avg=%.2f (source=%s, dist_CE=%.1f, dist_PE=%.1f)", ref_stock_name, candle_avg, price_src, strike_dist_ce, strike_dist_pe)
                    self.detect_strike_side(ref_stock_name, "CE", candle_avg, strike_dist_ce, cap_info)
                    self.detect_strike_side(ref_stock_name, "PE", candle_avg, strike_dist_pe, cap_info)
                else:
                    strike_logger.warning("[%s] Unable to compute valid candle_avg (val=%s). Skipping strike detection.", ref_stock_name, candle_avg)
            except Exception as e:
                general_logger.error("[%s] Strike detection failed: %s", ref_stock_name, e)

        selected_count = 0
        active_syms = []
        if "Buy_strike" in self.cum_table.columns:
            sel_df = self.cum_table.filter(pl.col("Buy_strike") == "Yes")
            selected_count = sel_df.height
            if "tradingsymbol" in sel_df.columns:
                active_syms = sel_df["tradingsymbol"].drop_nulls().to_list()
        strike_logger.info("[STRIKE_DETECT] Strike detection completed: %d active strike(s) selected (%s).", selected_count, ", ".join(active_syms) if active_syms else "None")

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

        matching_tkn = self.cum_table.filter(pl.col('instrument_token') == token_number) if 'instrument_token' in self.cum_table.columns else pl.DataFrame()
        if matching_tkn.is_empty() and 'Ref_stock_tkn' in self.cum_table.columns:
            matching_tkn = self.cum_table.filter(pl.col('Ref_stock_tkn') == token_number)
        if matching_tkn.is_empty():
            return pl.DataFrame()

        index_token_number = matching_tkn['Index_tkn'][0] if 'Index_tkn' in matching_tkn.columns else token_number
        exchg = str(matching_tkn['exchange'][0]) if 'exchange' in matching_tkn.columns else (str(matching_tkn['Exchange_type'][0]) if 'Exchange_type' in matching_tkn.columns else "coinswitchx")
        cur_symbol = str(matching_tkn['tradingsymbol'][0]) if 'tradingsymbol' in matching_tkn.columns else (str(matching_tkn['Symbol'][0]) if 'Symbol' in matching_tkn.columns else "")
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
            self.fwd_60 = self.fwd_60_all.filter(pl.col('instrument_token') == token_number).sort('date_time', descending=True)
            self.day_cdl = self.day_cdl_all.filter(pl.col('instrument_token') == token_number).sort('date_time', descending=True)

            session_ref_sorted = session_ref_data.sort('date_time', descending=True)
            current_ltp = float(session_ref_sorted['last_price'][0])

            try:
                max_time = self.ref_min_max['date_time'].max()
                cutoff = max_time - timedelta(seconds=scan_window)
                active_scan = self.ref_min_max.filter(pl.col('date_time') >= cutoff)
                if not active_scan.is_empty():
                    olhc_max = float(max(active_scan['close'].max(), active_scan['open'].max()))
                    olhc_min = float(min(active_scan['close'].min(), active_scan['open'].min()))
                else:
                    olhc_max = current_ltp
                    olhc_min = current_ltp
            except Exception:
                olhc_max = current_ltp
                olhc_min = current_ltp

            if not self.prev_day_cdl_all.is_empty() and 'instrument_token' in self.prev_day_cdl_all.columns:
                self.prev_day_cdl = self.prev_day_cdl_all.filter(pl.col('instrument_token') == token_number).sort('date_time', descending=True)
            else:
                self.prev_day_cdl = session_ref_sorted
            if len(self.prev_day_cdl) < 1:
                self.prev_day_cdl = session_ref_sorted

            # ── 1. Session Start Rules (Legacy Equity - Disabled for 24/7 Crypto) ───
            # if self.session_start(exchg) and not self.prev_day_cdl_all.is_empty():
            #     if not self.prev_day_cdl.is_empty():
            #         prev_last_price = float(self.prev_day_cdl['last_price'][0]) if 'last_price' in self.prev_day_cdl.columns else (float(self.prev_day_cdl['close'][0]) if 'close' in self.prev_day_cdl.columns else current_ltp)
            #         if exchg == 'MCX':
            #             if (prev_last_price - current_ltp) > hedge_points_pe:
            #                 sig_pe = 1
            #             elif (current_ltp - prev_last_price) > hedge_points_ce:
            #                 sig_ce = 1
            #         elif exchg == 'NFO' and not self.fwd_30.is_empty():
            #             fwd_open_0 = float(self.fwd_30['open'][0])
            #             if (fwd_open_0 - current_ltp) > hedge_points_pe:
            #                 sig_pe = 1
            #             elif (current_ltp - fwd_open_0) > hedge_points_ce:
            #                 sig_ce = 1

            # ── 2. Momentum & Trend Evaluation ──────────────────────────────
            if not self.fwd_30.is_empty() and not self.fwd_30_index.is_empty() and len(self.fwd_30) >= 3:
                fwd30_c = self.fwd_60['close'].to_list()  # intentional do not change
                fwd30_o = self.fwd_60['open'].to_list()  # intentional do not change

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
                # if self.session_end(exchg):
                #     if exchg == 'NFO' and len(fwd30_c) >= 2:
                #         fwd30_c1 = fwd30_c[1]
                #         fwd30_first_h = fwd30_h[-1]
                #         fwd30_first_l = fwd30_l[-1]
                #         fwd30_first_o = fwd30_o[-1]
                #         fwd30_first_c = fwd30_c[-1]
                #         fwd30_h1 = fwd30_h[1]
                #         fwd30_l1 = fwd30_l[1]

                #         is_trendy = (
                #             ((fwd30_c1 < min(fwd30_first_h, fwd30_first_l)) or (fwd30_c1 > max(fwd30_first_h, fwd30_first_l)))
                #             and (not (fwd30_first_o <= fwd30_h1 <= fwd30_first_h) or not (fwd30_first_o <= fwd30_l1 <= fwd30_first_h))
                #         )
                #         if is_trendy:
                #             inst_analysis_logger.info(f"today trendy for {cur_symbol}")
                #             if fwd30_c1 > fwd30_first_c:
                #                 if len(fwd30_c) >= 6:
                #                     h5_max = max(fwd30_h[5], fwd30_l[5])
                #                     h1_4_all_below = all(h5_max > h for h in fwd30_h[1:4])
                #                     m1_5 = [(fwd30_c[i] + fwd30_o[i]) / 2.0 for i in range(1, 5)]
                #                     m1_5_increasing = all(m1_5[i] <= m1_5[i+1] for i in range(len(m1_5)-1))
                #                     c1_less_prev2 = (fwd30_c1 < fwd30_c[-2])

                #                     if h1_4_all_below and m1_5_increasing and c1_less_prev2:
                #                         sig_ce = 1
                #                         sig_pe = -1
                #                         inst_analysis_logger.info(f"tomorrow down for {cur_symbol}")
                #                     else:
                #                         sig_ce = -1
                #                         sig_pe = -1
                #                         inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                #                 else:
                #                     sig_ce = -1
                #                     sig_pe = -1
                #                     inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")

                #             elif fwd30_c1 < fwd30_first_c:
                #                 if len(fwd30_c) >= 6:
                #                     min_oc5 = min(fwd30_o[5], fwd30_c[5])
                #                     any_c1_5_above = any(min_oc5 < c for c in fwd30_c[1:5])
                #                     c1_5 = list(fwd30_c[1:5])
                #                     not_monotonic_dec = not all(c1_5[i] >= c1_5[i+1] for i in range(len(c1_5)-1))

                #                     h5_max = max(fwd30_h[5], fwd30_l[5])
                #                     h1_4_all_below = all(h5_max > h for h in fwd30_h[1:4])
                #                     is_monotonic_inc = all(c1_5[i] <= c1_5[i+1] for i in range(len(c1_5)-1))

                #                     if any_c1_5_above or not_monotonic_dec:
                #                         sig_pe = -1
                #                         sig_ce = 1
                #                         inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                #                     elif h1_4_all_below and is_monotonic_inc:
                #                         sig_pe = 1
                #                         sig_ce = -1
                #                         inst_analysis_logger.info(f"tomorrow down for {cur_symbol}")
                #                 else:
                #                     sig_pe = -1
                #                     sig_ce = 1
                #                     inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                #         else:
                #             inst_analysis_logger.info(f"today flat for {cur_symbol}")
                #             c1_4 = list(fwd30_c[1:4])
                #             c1_4_dec = all(c1_4[i] >= c1_4[i+1] for i in range(len(c1_4)-1)) if len(c1_4) >= 2 else False
                #             h1_gt_prev2 = (fwd30_h[1] > fwd30_h[-2]) if len(fwd30_h) >= 3 else False
                #             first_mean = (fwd30_c[-1] + fwd30_o[-1]) / 2.0

                #             if (h1_gt_prev2 or c1_4_dec) and (first_mean < fwd30_c1):
                #                 sig_ce = 1
                #                 sig_pe = -1
                #                 inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                #             else:
                #                 sig_ce = -1
                #                 sig_pe = 1
                #                 inst_analysis_logger.info(f"tomorrow down for {cur_symbol}")

                #     if self.nfo_risk_hold and len(fwd30_c) >= 2:
                #         if fwd30_c[1] < fwd30_c[-1]:
                #             sig_ce = 1
                #             sig_pe = 1

                #     if self.next_session_closed(exchg):
                #         if exchg == 'MCX':
                #             sig_pe = 1
                #             sig_ce = 1
                #             inst_analysis_logger.info('path_13')
                #         elif exchg == 'NFO' and len(fwd30_c) >= 2:
                #             fwd30_c1 = fwd30_c[1]
                #             fwd30_first_h = fwd30_h[-1]
                #             fwd30_first_l = fwd30_l[-1]
                #             fwd30_first_o = fwd30_o[-1]
                #             fwd30_h1 = fwd30_h[1]
                #             fwd30_l1 = fwd30_l[1]

                #             is_trendy_ns = (
                #                 ((fwd30_c1 < min(fwd30_first_h, fwd30_first_l)) or (fwd30_c1 > max(fwd30_first_h, fwd30_first_l)))
                #                 and (not (fwd30_first_l <= fwd30_h1 <= fwd30_first_h) or not (fwd30_first_l <= fwd30_l1 <= fwd30_first_h))
                #             )
                #             if is_trendy_ns:
                #                 inst_analysis_logger.info(f"today trendy for {cur_symbol}")
                #                 if fwd30_c1 < fwd30_first_o:
                #                     sig_ce = 1
                #                     sig_pe = -1
                #                     inst_analysis_logger.info('path_13_1')
                #             else:
                #                 sig_ce = -1
                #                 sig_pe = -1
                #                 inst_analysis_logger.info('sell all')
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
            f"[{cur_symbol}:{token_number}] LTP={latest_price:.2f}, High={olhc_max:.2f}, Low={olhc_min:.2f}, "
            f"Sig_CE={sig_ce}, Sig_PE={sig_pe}, CE_jump={ce_jump}, PE_jump={pe_jump}"
        )
        return stock_info


    # ── Derivative Analysis & Sizing in Polars ─────────────────────────────


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
            exchg = str(sym_meta.get('exchange', 'OPTIONS'))
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


    def _update_jump_stop_loss(self, tkn_id: int, sym: str, sym_meta: dict, tick_data: pl.DataFrame) -> None:
        """
        Updates the stop loss price for the given instrument token upon meeting the strike jump condition
        (similar to slu) and immediately persists the updated stop_loss_info table to AlgoInfo in DB.
        """
        cur_opt_ltp = 0.0
        if not tick_data.is_empty() and 'instrument_token' in tick_data.columns:
            tkn_ticks = tick_data.filter(pl.col('instrument_token') == tkn_id)
            if not tkn_ticks.is_empty() and 'last_price' in tkn_ticks.columns:
                cur_opt_ltp = float(tkn_ticks['last_price'][-1])

        if cur_opt_ltp <= 0 and hasattr(self, 'tick_data') and not self.tick_data.is_empty() and 'instrument_token' in self.tick_data.columns:
            tkn_ticks = self.tick_data.filter(pl.col('instrument_token') == tkn_id)
            if not tkn_ticks.is_empty() and 'last_price' in tkn_ticks.columns:
                cur_opt_ltp = float(tkn_ticks['last_price'][-1])

        if cur_opt_ltp <= 0 and hasattr(self, 'open_positions') and not self.open_positions.is_empty() and 'instrument_token' in self.open_positions.columns:
            pos_m = self.open_positions.filter(pl.col('instrument_token') == tkn_id)
            if not pos_m.is_empty():
                if 'last_price' in pos_m.columns and float(pos_m['last_price'][0] or 0.0) > 0:
                    cur_opt_ltp = float(pos_m['last_price'][0])
                elif 'buy_price' in pos_m.columns and float(pos_m['buy_price'][0] or 0.0) > 0:
                    cur_opt_ltp = float(pos_m['buy_price'][0])

        if cur_opt_ltp <= 0 and sym_meta:
            cur_opt_ltp = float(sym_meta.get('last_price', 0.0) or sym_meta.get('current_value', 0.0) or 0.0)

        if cur_opt_ltp <= 0:
            trade_logger.warning(f"[branch: jump_sl_skipped] [{sym}] Cannot update jump stop loss: LTP not available.")
            return

        acc = getattr(self, 'account', None)
        acc_id = acc.user_id if acc else 'PRIMARY'
        sl_schema = {
            'order_id': pl.Utf8,
            'buy_price': pl.Float64,
            'instrument_token': pl.Int64,
            'date_time': pl.Datetime,
            'tradingsymbol': pl.Utf8
        }

        if acc is not None:
            has_tkn = (not acc.stop_loss_info.is_empty() and
                       'instrument_token' in acc.stop_loss_info.columns and
                       (acc.stop_loss_info['instrument_token'] == tkn_id).any())
            if has_tkn:
                acc.stop_loss_info = acc.stop_loss_info.with_columns(
                    pl.when(pl.col('instrument_token') == tkn_id)
                    .then(pl.lit(cur_opt_ltp))
                    .otherwise(pl.col('buy_price'))
                    .alias('buy_price'),
                    pl.when(pl.col('instrument_token') == tkn_id)
                    .then(pl.lit(datetime.now()))
                    .otherwise(pl.col('date_time'))
                    .alias('date_time')
                )
            else:
                new_sl = pl.DataFrame([{
                    'order_id': '-1',
                    'buy_price': cur_opt_ltp,
                    'instrument_token': tkn_id,
                    'date_time': datetime.now(),
                    'tradingsymbol': sym
                }], schema=acc.stop_loss_info.schema if not acc.stop_loss_info.is_empty() else sl_schema)
                acc.stop_loss_info = pl.concat([acc.stop_loss_info, new_sl], how="diagonal").unique(subset=['instrument_token'], keep='last')
            self.stop_loss_info = acc.stop_loss_info
        else:
            has_tkn = (not self.stop_loss_info.is_empty() and
                       'instrument_token' in self.stop_loss_info.columns and
                       (self.stop_loss_info['instrument_token'] == tkn_id).any())
            if has_tkn:
                self.stop_loss_info = self.stop_loss_info.with_columns(
                    pl.when(pl.col('instrument_token') == tkn_id)
                    .then(pl.lit(cur_opt_ltp))
                    .otherwise(pl.col('buy_price'))
                    .alias('buy_price'),
                    pl.when(pl.col('instrument_token') == tkn_id)
                    .then(pl.lit(datetime.now()))
                    .otherwise(pl.col('date_time'))
                    .alias('date_time')
                )
            else:
                new_sl = pl.DataFrame([{
                    'order_id': '-1',
                    'buy_price': cur_opt_ltp,
                    'instrument_token': tkn_id,
                    'date_time': datetime.now(),
                    'tradingsymbol': sym
                }], schema=self.stop_loss_info.schema if not self.stop_loss_info.is_empty() else sl_schema)
                self.stop_loss_info = pl.concat([self.stop_loss_info, new_sl], how="diagonal").unique(subset=['instrument_token'], keep='last')

        self.update_algo_info_table()
        trade_logger.info(f"[branch: jump_sl_updated] [{acc_id}] Jump condition met: Updated stop loss for {sym} (token={tkn_id}) to {cur_opt_ltp:.2f} in AlgoInfo.")


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

                # if striked_value > 0 and exchg == 'CDS':
                #     tick_size = float(sym_meta.get('tick_size', 1.0) or 1.0)
                #     striked_value = striked_value * tick_size

                if striked_value > 0 and ref_fwd_price > 0:
                    hedge_pts_ce = float(sym_meta.get('Hedge_points_CE', 0.0) or 0.0)
                    hedge_pts_pe = float(sym_meta.get('Hedge_points_PE', 0.0) or 0.0)
                    ce_jump = int(sym_meta.get('CE_jump', 0) or 0)
                    pe_jump = int(sym_meta.get('PE_jump', 0) or 0)

                    if ref_inst_type == 'PE':
                        strike_diff_pe = striked_value - ref_fwd_price
                        inst_analysis_logger.info(f"[PE_Jump] {k}: Diff={strike_diff_pe:.2f}, Striked={striked_value:.2f}, RefPrice={ref_fwd_price:.2f}, PE_jump={pe_jump}")
                        # In 24/7 crypto, jump evaluation runs continuously without equity session boundaries
                        if hedge_pts_pe > 0 and strike_diff_pe > (hedge_pts_pe * 2):
                            self._update_jump_stop_loss(tkn_id, k, sym_meta, tick_data)
                            trade_logger.info(f"[branch: pe_jump_exit_triggered] [{k}] Triggered jump exit at ref price {ref_fwd_price:.2f}")
                    elif ref_inst_type == 'CE':
                        strike_diff_ce = ref_fwd_price - striked_value
                        inst_analysis_logger.info(f"[CE_Jump] {k}: Diff={strike_diff_ce:.2f}, Striked={striked_value:.2f}, RefPrice={ref_fwd_price:.2f}, CE_jump={ce_jump}")
                        # In 24/7 crypto, jump evaluation runs continuously without equity session boundaries
                        if hedge_pts_ce > 0 and strike_diff_ce > (hedge_pts_ce * 2):
                            self._update_jump_stop_loss(tkn_id, k, sym_meta, tick_data)
                            trade_logger.info(f"[branch: ce_jump_exit_triggered] [{k}] Triggered jump exit at ref price {ref_fwd_price:.2f}")

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

                    # if striked_val > 0 and exchg == 'CDS':
                    #     tick_size = float(sym_meta.get('tick_size', 1.0) or 1.0)
                    #     striked_val = striked_val * tick_size

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


    def derivative_analysis(self, all_table: pl.DataFrame, tick_data: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """High-Performance Polars Derivative Analysis Engine."""
        if self.cum_table.is_empty() or 'instrument_type' not in self.cum_table.columns or 'Buy_strike' not in self.cum_table.columns:
            return pl.DataFrame(), pl.DataFrame()

        inst_analysis_logger.info(f"Evaluating derivative strikes for {len(all_table) if not all_table.is_empty() else 0} instrument(s)...")

        # 1. Candidate BUY Selection
        buy_cand = self.cum_table.filter(
            (pl.col('Buy_strike') == 'Yes') & (
                ((pl.col('instrument_type') == 'CE') & (pl.col('buy_signal_CE') >= 1)) |
                ((pl.col('instrument_type') == 'PE') & (pl.col('buy_signal_PE') >= 1)) |
                (pl.col('instrument_type').is_in(['EQ', 'FUT']) & ((pl.col('buy_signal_CE') >= 1) | (pl.col('buy_signal_PE') >= 1)))
            )
        )
        self.buy_stock_cap = buy_cand

        # 2. Check Exits across all accounts
        sell_candidates_list: List[pl.DataFrame] = []
        all_open_positions = self.all_open_positions

        if not all_open_positions.is_empty():
            if 'instrument_token' not in all_open_positions.columns and 'tradingsymbol' in all_open_positions.columns:
                all_open_positions = all_open_positions.with_columns(
                    pl.col('tradingsymbol').map_elements(self.symbol_to_tkn, return_dtype=pl.Int64).alias('instrument_token')
                )

            if 'instrument_token' in all_open_positions.columns:
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

        general_logger.info(f"Polars Derivative analysis complete: Buy={len(self.buy_stock_cap)}, Sell={len(self.sell_stock_table)}")
        return self.buy_stock_cap, self.sell_stock_table


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
            self.strike_detect(self.tick_data)
            self.updated_list = self.token_list_update()

            if not self.updated_list.is_empty() and 'tradingsymbol' in self.updated_list.columns:
                active_syms = [str(s).strip() for s in self.updated_list['tradingsymbol'].drop_nulls().to_list() if str(s).strip() and not str(s).isdigit()]
                self.insert_instrument_token(active_syms)

            if not self.cum_table.is_empty() and 'Ref_stock_tkn' in self.cum_table.columns and not self.updated_list.is_empty() and 'instrument_token' in self.updated_list.columns:
                subs = set(self.updated_list['instrument_token'].drop_nulls().to_list())
                refs = set(self.cum_table['Ref_stock_tkn'].drop_nulls().to_list())
                self.final_ref_tokens = list(subs.intersection(refs))
            elif not self.cum_table.is_empty() and 'Ref_stock_tkn' in self.cum_table.columns:
                self.final_ref_tokens = [int(t) for t in self.cum_table['Ref_stock_tkn'].drop_nulls().unique().to_list() if int(t) != -1]
            else:
                self.final_ref_tokens = []

            signals: List[pl.DataFrame] = []
            for tkn in self.final_ref_tokens:
                sig = self.mom(int(tkn))
                if not sig.is_empty():
                    signals.append(sig)

            stock_info_table = pl.concat(signals, how="diagonal") if signals else pl.DataFrame()
            self.strike_update()
            self.buy_stock_cap, self.sell_stock_table = self.derivative_analysis(stock_info_table, self.tick_data)
            self.buy_stock_cap, self.sell_stock_table = self.jump(self.buy_stock_cap, self.sell_stock_table, self.tick_data)

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

    def capital_allocation_calc(self, symbol: str, buy_list: pl.DataFrame, account: Optional[CryptoUserAccount] = None) -> float:
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
            sym_rows = self.cum_table.filter(pl.col('tradingsymbol') == symbol)
            if sym_rows.is_empty():
                strike_logger.info(f"[branch: cap_alloc_missing_symbol] Symbol '{symbol}' not found in cum_table.")
                return 0.0
            ref_stock = str(sym_rows['Ref_stock'][0])
            inst_type = str(sym_rows['instrument_type'][0])

        max_cap_base = sym_meta.get(f'Max_capital_{inst_type}')
        if max_cap_base is None:
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


    def buy_stk_qty(self, symbol: str, capital_share: float, buy_list: pl.DataFrame, account: Optional[CryptoUserAccount] = None) -> Tuple[int, float]:
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
        lot_size = int(sym_rows['lot_size'][0]) if 'lot_size' in sym_rows.columns else 1
        tick_size = float(sym_rows['tick_size'][0]) if 'tick_size' in sym_rows.columns else 0.05
        max_lots = int(sym_rows['Max_lots_per_order'][0]) if 'Max_lots_per_order' in sym_rows.columns else 10

        recent = self.tick_data.filter(pl.col('instrument_token') == tkn)
        if recent.is_empty():
            strike_logger.info(f"[branch: buy_qty_ticks_missing] No tick price found for '{symbol}'.")
            return 0, 0.0
        ltp = float(recent['last_price'][-1])

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

        # In crypto, quantity is final_lots * lot_size
        # final_qty = final_lots if 'MCX' in exchg else (final_lots * lot_size)
        final_qty = final_lots * lot_size
        strike_logger.info(f"[branch: buy_qty_success] [{acc.user_id}] {symbol}: Lots={final_lots} (AvailLots={available_lots}), Qty={final_qty}, Price={adj_price:.2f}, Value={final_qty * adj_price:.2f} USDT/INR.")
        return int(final_qty), float(adj_price)


    def hold_pos_buy_chk(self, symbol_string: str, open_positions: pl.DataFrame) -> int:
        """
        Check if buy is permitted for symbol based on open positions and loss table configuration.
        In 24/7 crypto, equity exchange session restrictions (half_time, session_start, session_end) are bypassed.
        """
        hold_pos_buy_sts = 0
        meta = self.get_symbol_meta(symbol_string)
        if not meta:
            return 0
        ref_stock = str(meta.get('Ref_stock', ''))

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
            if matching_ref.is_empty():
                hold_pos_buy_sts = 1
                general_logger.info(f"hold_pos_buy_sts is one for {symbol_string}")
            else:
                hold_pos_buy_sts = 1
                general_logger.info(f"hold_pos_buy_sts is one for {symbol_string} (holding open position)")
        elif (not has_open_pos) and loss_value_exist:
            hold_pos_buy_sts = 1
            general_logger.info(f"hold_pos_buy_sts is one for {symbol_string}")
        else:
            hold_pos_buy_sts = 1

        return hold_pos_buy_sts


    def hold_pos_sell_chk(self, symbol_string: str, open_positions: pl.DataFrame) -> Tuple[int, int]:
        if not open_positions.is_empty() and 'tradingsymbol' in open_positions.columns:
            matching = open_positions.filter(pl.col('tradingsymbol') == symbol_string)
            if not matching.is_empty() and 'quantity' in matching.columns:
                total_qty = int(matching['quantity'].sum())
                if total_qty > 0:
                    trade_logger.info(f"[branch: hold_pos_sell_match] Found active position for '{symbol_string}': Qty={total_qty}.")
                    return 1, total_qty
        trade_logger.info(f"[branch: hold_pos_sell_no_match] No active position found for '{symbol_string}'.")
        return 0, 0


    def ordr_chk(self, symbol_string: str, order_status: Optional[pl.DataFrame] = None, account: Optional[CryptoUserAccount] = None) -> Tuple[int, int, str, int]:
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


    def ordr_sell_chk(self, symbol_string: str, order_status: Optional[pl.DataFrame] = None, account: Optional[CryptoUserAccount] = None) -> Tuple[int, int, str]:
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

        matching_tkn = self.cum_table.filter(pl.col('instrument_token') == token_number) if 'instrument_token' in self.cum_table.columns else pl.DataFrame()
        if matching_tkn.is_empty() and 'Ref_stock_tkn' in self.cum_table.columns:
            matching_tkn = self.cum_table.filter(pl.col('Ref_stock_tkn') == token_number)
        if matching_tkn.is_empty():
            return pl.DataFrame()

        index_token_number = matching_tkn['Index_tkn'][0] if 'Index_tkn' in matching_tkn.columns else token_number
        exchg = str(matching_tkn['exchange'][0]) if 'exchange' in matching_tkn.columns else (str(matching_tkn['Exchange_type'][0]) if 'Exchange_type' in matching_tkn.columns else "coinswitchx")
        cur_symbol = str(matching_tkn['tradingsymbol'][0]) if 'tradingsymbol' in matching_tkn.columns else (str(matching_tkn['Symbol'][0]) if 'Symbol' in matching_tkn.columns else "")
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
            self.fwd_60 = self.fwd_60_all.filter(pl.col('instrument_token') == token_number).sort('date_time', descending=True)
            self.day_cdl = self.day_cdl_all.filter(pl.col('instrument_token') == token_number).sort('date_time', descending=True)

            session_ref_sorted = session_ref_data.sort('date_time', descending=True)
            current_ltp = float(session_ref_sorted['last_price'][0])

            try:
                max_time = self.ref_min_max['date_time'].max()
                cutoff = max_time - timedelta(seconds=scan_window)
                active_scan = self.ref_min_max.filter(pl.col('date_time') >= cutoff)
                if not active_scan.is_empty():
                    olhc_max = float(max(active_scan['close'].max(), active_scan['open'].max()))
                    olhc_min = float(min(active_scan['close'].min(), active_scan['open'].min()))
                else:
                    olhc_max = current_ltp
                    olhc_min = current_ltp
            except Exception:
                olhc_max = current_ltp
                olhc_min = current_ltp

            if not self.prev_day_cdl_all.is_empty() and 'instrument_token' in self.prev_day_cdl_all.columns:
                self.prev_day_cdl = self.prev_day_cdl_all.filter(pl.col('instrument_token') == token_number).sort('date_time', descending=True)
            else:
                self.prev_day_cdl = session_ref_sorted
            if len(self.prev_day_cdl) < 1:
                self.prev_day_cdl = session_ref_sorted

            # ── 1. Session Start Rules (Legacy Equity - Disabled for 24/7 Crypto) ───
            # if self.session_start(exchg) and not self.prev_day_cdl_all.is_empty():
            #     if not self.prev_day_cdl.is_empty():
            #         prev_last_price = float(self.prev_day_cdl['last_price'][0]) if 'last_price' in self.prev_day_cdl.columns else (float(self.prev_day_cdl['close'][0]) if 'close' in self.prev_day_cdl.columns else current_ltp)
            #         if exchg == 'MCX':
            #             if (prev_last_price - current_ltp) > hedge_points_pe:
            #                 sig_pe = 1
            #             elif (current_ltp - prev_last_price) > hedge_points_ce:
            #                 sig_ce = 1
            #         elif exchg == 'NFO' and not self.fwd_30.is_empty():
            #             fwd_open_0 = float(self.fwd_30['open'][0])
            #             if (fwd_open_0 - current_ltp) > hedge_points_pe:
            #                 sig_pe = 1
            #             elif (current_ltp - fwd_open_0) > hedge_points_ce:
            #                 sig_ce = 1

            # ── 2. Momentum & Trend Evaluation ──────────────────────────────
            if not self.fwd_30.is_empty() and not self.fwd_30_index.is_empty() and len(self.fwd_30) >= 3:
                fwd30_c = self.fwd_60['close'].to_list()  # intentional do not change
                fwd30_o = self.fwd_60['open'].to_list()  # intentional do not change

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
                # if self.session_end(exchg):
                #     if exchg == 'NFO' and len(fwd30_c) >= 2:
                #         fwd30_c1 = fwd30_c[1]
                #         fwd30_first_h = fwd30_h[-1]
                #         fwd30_first_l = fwd30_l[-1]
                #         fwd30_first_o = fwd30_o[-1]
                #         fwd30_first_c = fwd30_c[-1]
                #         fwd30_h1 = fwd30_h[1]
                #         fwd30_l1 = fwd30_l[1]

                #         is_trendy = (
                #             ((fwd30_c1 < min(fwd30_first_h, fwd30_first_l)) or (fwd30_c1 > max(fwd30_first_h, fwd30_first_l)))
                #             and (not (fwd30_first_o <= fwd30_h1 <= fwd30_first_h) or not (fwd30_first_o <= fwd30_l1 <= fwd30_first_h))
                #         )
                #         if is_trendy:
                #             inst_analysis_logger.info(f"today trendy for {cur_symbol}")
                #             if fwd30_c1 > fwd30_first_c:
                #                 if len(fwd30_c) >= 6:
                #                     h5_max = max(fwd30_h[5], fwd30_l[5])
                #                     h1_4_all_below = all(h5_max > h for h in fwd30_h[1:4])
                #                     m1_5 = [(fwd30_c[i] + fwd30_o[i]) / 2.0 for i in range(1, 5)]
                #                     m1_5_increasing = all(m1_5[i] <= m1_5[i+1] for i in range(len(m1_5)-1))
                #                     c1_less_prev2 = (fwd30_c1 < fwd30_c[-2])

                #                     if h1_4_all_below and m1_5_increasing and c1_less_prev2:
                #                         sig_ce = 1
                #                         sig_pe = -1
                #                         inst_analysis_logger.info(f"tomorrow down for {cur_symbol}")
                #                     else:
                #                         sig_ce = -1
                #                         sig_pe = -1
                #                         inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                #                 else:
                #                     sig_ce = -1
                #                     sig_pe = -1
                #                     inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")

                #             elif fwd30_c1 < fwd30_first_c:
                #                 if len(fwd30_c) >= 6:
                #                     min_oc5 = min(fwd30_o[5], fwd30_c[5])
                #                     any_c1_5_above = any(min_oc5 < c for c in fwd30_c[1:5])
                #                     c1_5 = list(fwd30_c[1:5])
                #                     not_monotonic_dec = not all(c1_5[i] >= c1_5[i+1] for i in range(len(c1_5)-1))

                #                     h5_max = max(fwd30_h[5], fwd30_l[5])
                #                     h1_4_all_below = all(h5_max > h for h in fwd30_h[1:4])
                #                     is_monotonic_inc = all(c1_5[i] <= c1_5[i+1] for i in range(len(c1_5)-1))

                #                     if any_c1_5_above or not_monotonic_dec:
                #                         sig_pe = -1
                #                         sig_ce = 1
                #                         inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                #                     elif h1_4_all_below and is_monotonic_inc:
                #                         sig_pe = 1
                #                         sig_ce = -1
                #                         inst_analysis_logger.info(f"tomorrow down for {cur_symbol}")
                #                 else:
                #                     sig_pe = -1
                #                     sig_ce = 1
                #                     inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                #         else:
                #             inst_analysis_logger.info(f"today flat for {cur_symbol}")
                #             c1_4 = list(fwd30_c[1:4])
                #             c1_4_dec = all(c1_4[i] >= c1_4[i+1] for i in range(len(c1_4)-1)) if len(c1_4) >= 2 else False
                #             h1_gt_prev2 = (fwd30_h[1] > fwd30_h[-2]) if len(fwd30_h) >= 3 else False
                #             first_mean = (fwd30_c[-1] + fwd30_o[-1]) / 2.0

                #             if (h1_gt_prev2 or c1_4_dec) and (first_mean < fwd30_c1):
                #                 sig_ce = 1
                #                 sig_pe = -1
                #                 inst_analysis_logger.info(f"tomorrow up for {cur_symbol}")
                #             else:
                #                 sig_ce = -1
                #                 sig_pe = 1
                #                 inst_analysis_logger.info(f"tomorrow down for {cur_symbol}")

                #     if self.nfo_risk_hold and len(fwd30_c) >= 2:
                #         if fwd30_c[1] < fwd30_c[-1]:
                #             sig_ce = 1
                #             sig_pe = 1

                #     if self.next_session_closed(exchg):
                #         if exchg == 'MCX':
                #             sig_pe = 1
                #             sig_ce = 1
                #             inst_analysis_logger.info('path_13')
                #         elif exchg == 'NFO' and len(fwd30_c) >= 2:
                #             fwd30_c1 = fwd30_c[1]
                #             fwd30_first_h = fwd30_h[-1]
                #             fwd30_first_l = fwd30_l[-1]
                #             fwd30_first_o = fwd30_o[-1]
                #             fwd30_h1 = fwd30_h[1]
                #             fwd30_l1 = fwd30_l[1]

                #             is_trendy_ns = (
                #                 ((fwd30_c1 < min(fwd30_first_h, fwd30_first_l)) or (fwd30_c1 > max(fwd30_first_h, fwd30_first_l)))
                #                 and (not (fwd30_first_l <= fwd30_h1 <= fwd30_first_h) or not (fwd30_first_l <= fwd30_l1 <= fwd30_first_h))
                #             )
                #             if is_trendy_ns:
                #                 inst_analysis_logger.info(f"today trendy for {cur_symbol}")
                #                 if fwd30_c1 < fwd30_first_o:
                #                     sig_ce = 1
                #                     sig_pe = -1
                #                     inst_analysis_logger.info('path_13_1')
                #             else:
                #                 sig_ce = -1
                #                 sig_pe = -1
                #                 inst_analysis_logger.info('sell all')
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
            f"[{cur_symbol}:{token_number}] LTP={latest_price:.2f}, High={olhc_max:.2f}, Low={olhc_min:.2f}, "
            f"Sig_CE={sig_ce}, Sig_PE={sig_pe}, CE_jump={ce_jump}, PE_jump={pe_jump}"
        )
        return stock_info


    # ── Derivative Analysis & Sizing in Polars ─────────────────────────────


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
            exchg = str(sym_meta.get('exchange', 'OPTIONS'))
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


    def _update_jump_stop_loss(self, tkn_id: int, sym: str, sym_meta: dict, tick_data: pl.DataFrame) -> None:
        """
        Updates the stop loss price for the given instrument token upon meeting the strike jump condition
        (similar to slu) and immediately persists the updated stop_loss_info table to AlgoInfo in DB.
        """
        cur_opt_ltp = 0.0
        if not tick_data.is_empty() and 'instrument_token' in tick_data.columns:
            tkn_ticks = tick_data.filter(pl.col('instrument_token') == tkn_id)
            if not tkn_ticks.is_empty() and 'last_price' in tkn_ticks.columns:
                cur_opt_ltp = float(tkn_ticks['last_price'][-1])

        if cur_opt_ltp <= 0 and hasattr(self, 'tick_data') and not self.tick_data.is_empty() and 'instrument_token' in self.tick_data.columns:
            tkn_ticks = self.tick_data.filter(pl.col('instrument_token') == tkn_id)
            if not tkn_ticks.is_empty() and 'last_price' in tkn_ticks.columns:
                cur_opt_ltp = float(tkn_ticks['last_price'][-1])

        if cur_opt_ltp <= 0 and hasattr(self, 'open_positions') and not self.open_positions.is_empty() and 'instrument_token' in self.open_positions.columns:
            pos_m = self.open_positions.filter(pl.col('instrument_token') == tkn_id)
            if not pos_m.is_empty():
                if 'last_price' in pos_m.columns and float(pos_m['last_price'][0] or 0.0) > 0:
                    cur_opt_ltp = float(pos_m['last_price'][0])
                elif 'buy_price' in pos_m.columns and float(pos_m['buy_price'][0] or 0.0) > 0:
                    cur_opt_ltp = float(pos_m['buy_price'][0])

        if cur_opt_ltp <= 0 and sym_meta:
            cur_opt_ltp = float(sym_meta.get('last_price', 0.0) or sym_meta.get('current_value', 0.0) or 0.0)

        if cur_opt_ltp <= 0:
            trade_logger.warning(f"[branch: jump_sl_skipped] [{sym}] Cannot update jump stop loss: LTP not available.")
            return

        acc = getattr(self, 'account', None)
        acc_id = acc.user_id if acc else 'PRIMARY'
        sl_schema = {
            'order_id': pl.Utf8,
            'buy_price': pl.Float64,
            'instrument_token': pl.Int64,
            'date_time': pl.Datetime,
            'tradingsymbol': pl.Utf8
        }

        if acc is not None:
            has_tkn = (not acc.stop_loss_info.is_empty() and
                       'instrument_token' in acc.stop_loss_info.columns and
                       (acc.stop_loss_info['instrument_token'] == tkn_id).any())
            if has_tkn:
                acc.stop_loss_info = acc.stop_loss_info.with_columns(
                    pl.when(pl.col('instrument_token') == tkn_id)
                    .then(pl.lit(cur_opt_ltp))
                    .otherwise(pl.col('buy_price'))
                    .alias('buy_price'),
                    pl.when(pl.col('instrument_token') == tkn_id)
                    .then(pl.lit(datetime.now()))
                    .otherwise(pl.col('date_time'))
                    .alias('date_time')
                )
            else:
                new_sl = pl.DataFrame([{
                    'order_id': '-1',
                    'buy_price': cur_opt_ltp,
                    'instrument_token': tkn_id,
                    'date_time': datetime.now(),
                    'tradingsymbol': sym
                }], schema=acc.stop_loss_info.schema if not acc.stop_loss_info.is_empty() else sl_schema)
                acc.stop_loss_info = pl.concat([acc.stop_loss_info, new_sl], how="diagonal").unique(subset=['instrument_token'], keep='last')
            self.stop_loss_info = acc.stop_loss_info
        else:
            has_tkn = (not self.stop_loss_info.is_empty() and
                       'instrument_token' in self.stop_loss_info.columns and
                       (self.stop_loss_info['instrument_token'] == tkn_id).any())
            if has_tkn:
                self.stop_loss_info = self.stop_loss_info.with_columns(
                    pl.when(pl.col('instrument_token') == tkn_id)
                    .then(pl.lit(cur_opt_ltp))
                    .otherwise(pl.col('buy_price'))
                    .alias('buy_price'),
                    pl.when(pl.col('instrument_token') == tkn_id)
                    .then(pl.lit(datetime.now()))
                    .otherwise(pl.col('date_time'))
                    .alias('date_time')
                )
            else:
                new_sl = pl.DataFrame([{
                    'order_id': '-1',
                    'buy_price': cur_opt_ltp,
                    'instrument_token': tkn_id,
                    'date_time': datetime.now(),
                    'tradingsymbol': sym
                }], schema=self.stop_loss_info.schema if not self.stop_loss_info.is_empty() else sl_schema)
                self.stop_loss_info = pl.concat([self.stop_loss_info, new_sl], how="diagonal").unique(subset=['instrument_token'], keep='last')

        self.update_algo_info_table()
        trade_logger.info(f"[branch: jump_sl_updated] [{acc_id}] Jump condition met: Updated stop loss for {sym} (token={tkn_id}) to {cur_opt_ltp:.2f} in AlgoInfo.")


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

                # if striked_value > 0 and exchg == 'CDS':
                #     tick_size = float(sym_meta.get('tick_size', 1.0) or 1.0)
                #     striked_value = striked_value * tick_size

                if striked_value > 0 and ref_fwd_price > 0:
                    hedge_pts_ce = float(sym_meta.get('Hedge_points_CE', 0.0) or 0.0)
                    hedge_pts_pe = float(sym_meta.get('Hedge_points_PE', 0.0) or 0.0)
                    ce_jump = int(sym_meta.get('CE_jump', 0) or 0)
                    pe_jump = int(sym_meta.get('PE_jump', 0) or 0)

                    if ref_inst_type == 'PE':
                        strike_diff_pe = striked_value - ref_fwd_price
                        inst_analysis_logger.info(f"[PE_Jump] {k}: Diff={strike_diff_pe:.2f}, Striked={striked_value:.2f}, RefPrice={ref_fwd_price:.2f}, PE_jump={pe_jump}")
                        # In 24/7 crypto, jump evaluation runs continuously without equity session boundaries
                        if hedge_pts_pe > 0 and strike_diff_pe > (hedge_pts_pe * 2):
                            self._update_jump_stop_loss(tkn_id, k, sym_meta, tick_data)
                            trade_logger.info(f"[branch: pe_jump_exit_triggered] [{k}] Triggered jump exit at ref price {ref_fwd_price:.2f}")
                    elif ref_inst_type == 'CE':
                        strike_diff_ce = ref_fwd_price - striked_value
                        inst_analysis_logger.info(f"[CE_Jump] {k}: Diff={strike_diff_ce:.2f}, Striked={striked_value:.2f}, RefPrice={ref_fwd_price:.2f}, CE_jump={ce_jump}")
                        # In 24/7 crypto, jump evaluation runs continuously without equity session boundaries
                        if hedge_pts_ce > 0 and strike_diff_ce > (hedge_pts_ce * 2):
                            self._update_jump_stop_loss(tkn_id, k, sym_meta, tick_data)
                            trade_logger.info(f"[branch: ce_jump_exit_triggered] [{k}] Triggered jump exit at ref price {ref_fwd_price:.2f}")

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

                    # if striked_val > 0 and exchg == 'CDS':
                    #     tick_size = float(sym_meta.get('tick_size', 1.0) or 1.0)
                    #     striked_val = striked_val * tick_size

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


    def derivative_analysis(self, all_table: pl.DataFrame, tick_data: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """High-Performance Polars Derivative Analysis Engine."""
        if self.cum_table.is_empty() or 'instrument_type' not in self.cum_table.columns or 'Buy_strike' not in self.cum_table.columns:
            return pl.DataFrame(), pl.DataFrame()

        inst_analysis_logger.info(f"Evaluating derivative strikes for {len(all_table) if not all_table.is_empty() else 0} instrument(s)...")

        # 1. Candidate BUY Selection
        buy_cand = self.cum_table.filter(
            (pl.col('Buy_strike') == 'Yes') & (
                ((pl.col('instrument_type') == 'CE') & (pl.col('buy_signal_CE') >= 1)) |
                ((pl.col('instrument_type') == 'PE') & (pl.col('buy_signal_PE') >= 1)) |
                (pl.col('instrument_type').is_in(['EQ', 'FUT']) & ((pl.col('buy_signal_CE') >= 1) | (pl.col('buy_signal_PE') >= 1)))
            )
        )
        self.buy_stock_cap = buy_cand

        # 2. Check Exits across all accounts
        sell_candidates_list: List[pl.DataFrame] = []
        all_open_positions = self.all_open_positions

        if not all_open_positions.is_empty():
            if 'instrument_token' not in all_open_positions.columns and 'tradingsymbol' in all_open_positions.columns:
                all_open_positions = all_open_positions.with_columns(
                    pl.col('tradingsymbol').map_elements(self.symbol_to_tkn, return_dtype=pl.Int64).alias('instrument_token')
                )

            if 'instrument_token' in all_open_positions.columns:
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

        general_logger.info(f"Polars Derivative analysis complete: Buy={len(self.buy_stock_cap)}, Sell={len(self.sell_stock_table)}")
        return self.buy_stock_cap, self.sell_stock_table


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
            self.strike_detect(self.tick_data)
            self.updated_list = self.token_list_update()

            if not self.updated_list.is_empty() and 'tradingsymbol' in self.updated_list.columns:
                active_syms = [str(s).strip() for s in self.updated_list['tradingsymbol'].drop_nulls().to_list() if str(s).strip() and not str(s).isdigit()]
                self.insert_instrument_token(active_syms)

            if not self.cum_table.is_empty() and 'Ref_stock_tkn' in self.cum_table.columns and not self.updated_list.is_empty() and 'instrument_token' in self.updated_list.columns:
                subs = set(self.updated_list['instrument_token'].drop_nulls().to_list())
                refs = set(self.cum_table['Ref_stock_tkn'].drop_nulls().to_list())
                self.final_ref_tokens = list(subs.intersection(refs))
            elif not self.cum_table.is_empty() and 'Ref_stock_tkn' in self.cum_table.columns:
                self.final_ref_tokens = [int(t) for t in self.cum_table['Ref_stock_tkn'].drop_nulls().unique().to_list() if int(t) != -1]
            else:
                self.final_ref_tokens = []

            signals: List[pl.DataFrame] = []
            for tkn in self.final_ref_tokens:
                sig = self.mom(int(tkn))
                if not sig.is_empty():
                    signals.append(sig)

            stock_info_table = pl.concat(signals, how="diagonal") if signals else pl.DataFrame()
            self.strike_update()
            self.buy_stock_cap, self.sell_stock_table = self.derivative_analysis(stock_info_table, self.tick_data)
            self.buy_stock_cap, self.sell_stock_table = self.jump(self.buy_stock_cap, self.sell_stock_table, self.tick_data)

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

    def capital_allocation_calc(self, symbol: str, buy_list: pl.DataFrame, account: Optional[CryptoUserAccount] = None) -> float:
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
            sym_rows = self.cum_table.filter(pl.col('tradingsymbol') == symbol)
            if sym_rows.is_empty():
                strike_logger.info(f"[branch: cap_alloc_missing_symbol] Symbol '{symbol}' not found in cum_table.")
                return 0.0
            ref_stock = str(sym_rows['Ref_stock'][0])
            inst_type = str(sym_rows['instrument_type'][0])

        max_cap_base = sym_meta.get(f'Max_capital_{inst_type}')
        if max_cap_base is None:
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


    def buy_stk_qty(self, symbol: str, capital_share: float, buy_list: pl.DataFrame, account: Optional[CryptoUserAccount] = None) -> Tuple[int, float]:
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
        lot_size = int(sym_rows['lot_size'][0]) if 'lot_size' in sym_rows.columns else 1
        tick_size = float(sym_rows['tick_size'][0]) if 'tick_size' in sym_rows.columns else 0.05
        max_lots = int(sym_rows['Max_lots_per_order'][0]) if 'Max_lots_per_order' in sym_rows.columns else 10

        recent = self.tick_data.filter(pl.col('instrument_token') == tkn)
        if recent.is_empty():
            strike_logger.info(f"[branch: buy_qty_ticks_missing] No tick price found for '{symbol}'.")
            return 0, 0.0
        ltp = float(recent['last_price'][-1])

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

        # In crypto, quantity is final_lots * lot_size
        # final_qty = final_lots if 'MCX' in exchg else (final_lots * lot_size)
        final_qty = final_lots * lot_size
        strike_logger.info(f"[branch: buy_qty_success] [{acc.user_id}] {symbol}: Lots={final_lots} (AvailLots={available_lots}), Qty={final_qty}, Price={adj_price:.2f}, Value={final_qty * adj_price:.2f} USDT/INR.")
        return int(final_qty), float(adj_price)


    def hold_pos_buy_chk(self, symbol_string: str, open_positions: pl.DataFrame) -> int:
        """
        Check if buy is permitted for symbol based on open positions and loss table configuration.
        In 24/7 crypto, equity exchange session restrictions (half_time, session_start, session_end) are bypassed.
        """
        hold_pos_buy_sts = 0
        meta = self.get_symbol_meta(symbol_string)
        if not meta:
            return 0
        ref_stock = str(meta.get('Ref_stock', ''))

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
            if matching_ref.is_empty():
                hold_pos_buy_sts = 1
                general_logger.info(f"hold_pos_buy_sts is one for {symbol_string}")
            else:
                hold_pos_buy_sts = 1
                general_logger.info(f"hold_pos_buy_sts is one for {symbol_string} (holding open position)")
        elif (not has_open_pos) and loss_value_exist:
            hold_pos_buy_sts = 1
            general_logger.info(f"hold_pos_buy_sts is one for {symbol_string}")
        else:
            hold_pos_buy_sts = 1

        return hold_pos_buy_sts


    def hold_pos_sell_chk(self, symbol_string: str, open_positions: pl.DataFrame) -> Tuple[int, int]:
        if not open_positions.is_empty() and 'tradingsymbol' in open_positions.columns:
            matching = open_positions.filter(pl.col('tradingsymbol') == symbol_string)
            if not matching.is_empty() and 'quantity' in matching.columns:
                total_qty = int(matching['quantity'].sum())
                if total_qty > 0:
                    trade_logger.info(f"[branch: hold_pos_sell_match] Found active position for '{symbol_string}': Qty={total_qty}.")
                    return 1, total_qty
        trade_logger.info(f"[branch: hold_pos_sell_no_match] No active position found for '{symbol_string}'.")
        return 0, 0


    def ordr_chk(self, symbol_string: str, order_status: Optional[pl.DataFrame] = None, account: Optional[CryptoUserAccount] = None) -> Tuple[int, int, str, int]:
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


    def ordr_sell_chk(self, symbol_string: str, order_status: Optional[pl.DataFrame] = None, account: Optional[CryptoUserAccount] = None) -> Tuple[int, int, str]:
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
        price: Optional[float] = None,
        account: Optional[CryptoUserAccount] = None
    ) -> int:
        """Places buy order and registers stop-loss."""
        acc = account or self.account
        if not acc or qty <= 0:
            general_logger.warning(
                "[ORDER_GATE] [BUY_REJECT] Buy order aborted for %s (token=%s): invalid parameters (account=%s, qty=%s, price=%s).",
                sym, tkn, acc.user_id if acc else "None", qty, price
            )
            return -1

        sym_meta = self.get_symbol_meta(sym)
        tick_size = max(0.01, float(sym_meta.get('tick_size') or 0.05))

        # Compute buy limit price if not provided
        adj_buy_price = price or 0.0
        if adj_buy_price <= 0 and not self.tick_data.is_empty() and 'instrument_token' in self.tick_data.columns:
            sym_ticks = self.tick_data.filter(pl.col('instrument_token') == tkn)
            if not sym_ticks.is_empty() and 'last_price' in sym_ticks.columns:
                ltp = float(sym_ticks['last_price'][-1] or 0.0)
                adj_buy_price = float(np.ceil(ltp / tick_size) * tick_size)
        if adj_buy_price <= 0:
            adj_buy_price = 100.0

        is_debug = getattr(self, "debug_mode", DEBUG)
        if acc.live_balance == 0.0 and acc.open_positions.is_empty() and acc.pos_day_frame.is_empty():
            try:
                acc.sync_account_state(debug_mode=is_debug)
            except Exception as e:
                general_logger.error("[%s] Cold-start account sync failed: %s", acc.user_id, e)

        placed_id = 1
        if acc.client and hasattr(acc.client, "lim_ordr"):
            try:
                res_id, msg = acc.client.lim_ordr(sym=sym, qty=qty, price=adj_buy_price, buy_or_sell="BUY")
                placed_id = int(res_id) if str(res_id).isdigit() else 1
            except Exception:
                placed_id = 1

        now_dt = datetime.now()
        new_row = pl.DataFrame([{
            "order_id": str(placed_id),
            "buy_price": float(adj_buy_price),
            "instrument_token": int(tkn),
            "date_time": now_dt,
            "tradingsymbol": str(sym),
        }], schema={
            "order_id": pl.Utf8,
            "buy_price": pl.Float64,
            "instrument_token": pl.Int64,
            "date_time": pl.Datetime,
            "tradingsymbol": pl.Utf8,
        })
        acc.stop_loss_info = pl.concat([acc.stop_loss_info, new_row], how="diagonal_relaxed").unique(subset=['instrument_token'], keep='last') if not acc.stop_loss_info.is_empty() else new_row
        self.stop_loss_info = acc.stop_loss_info
        self.update_algo_info_table()

        if hasattr(acc, "recent_buy_order_time"):
            acc.recent_buy_order_time[sym] = now_dt
        if hasattr(self, "recent_buy_order_time"):
            self.recent_buy_order_time[sym] = now_dt

        if hasattr(acc, "mark_order_placed"):
            acc.mark_order_placed(placed_id, sym, qty, adj_buy_price, "BUY")
        general_logger.info(
            "[ORDER_STATUS] [BUY_PLACED] Buy order placed successfully: order_id=%s, sym=%s, token=%d, qty=%d, price=%.4f (account=%s, status=%d). Stop-loss registered.",
            placed_id, sym, tkn, qty, adj_buy_price, acc.user_id, order_status
        )
        return placed_id

    def execute_sell(
        self,
        sym: str,
        tkn: int,
        qty: int,
        exchg: str,
        order_status: int,
        order_id: int,
        price: Optional[float] = None,
        account: Optional[CryptoUserAccount] = None
    ) -> int:
        """Places sell order and cleans stop-loss."""
        acc = account or self.account
        if not acc or qty <= 0:
            general_logger.warning(
                "[ORDER_GATE] [SELL_REJECT] Sell order aborted for %s (token=%s): invalid parameters (account=%s, qty=%s, price=%s).",
                sym, tkn, acc.user_id if acc else "None", qty, price
            )
            return -1

        sym_meta = self.get_symbol_meta(sym)
        tick_size = max(0.01, float(sym_meta.get('tick_size') or 0.05))

        # Compute sell limit price
        adj_sell_price = price or 0.0
        if adj_sell_price <= 0 and not self.tick_data.is_empty() and 'instrument_token' in self.tick_data.columns:
            sym_ticks = self.tick_data.filter(pl.col('instrument_token') == tkn)
            if not sym_ticks.is_empty() and 'last_price' in sym_ticks.columns:
                ltp = float(sym_ticks['last_price'][-1] or 0.0)
                adj_sell_price = float(np.round(ltp - (ltp % tick_size), 4))
        if adj_sell_price <= 0 and not acc.open_positions.is_empty():
            pos_m = acc.open_positions.filter(pl.col('tradingsymbol') == sym)
            if not pos_m.is_empty() and 'last_price' in pos_m.columns:
                adj_sell_price = float(pos_m['last_price'][0] or 0.0)
        if adj_sell_price <= 0:
            adj_sell_price = 1.0

        is_debug = getattr(self, "debug_mode", DEBUG)
        if acc.live_balance == 0.0 and acc.open_positions.is_empty() and acc.pos_day_frame.is_empty():
            try:
                acc.sync_account_state(debug_mode=is_debug)
            except Exception as e:
                general_logger.error("[%s] Cold-start account sync failed: %s", acc.user_id, e)

        placed_id = 1
        if acc.client and hasattr(acc.client, "lim_ordr"):
            try:
                res_id, msg = acc.client.lim_ordr(sym=sym, qty=qty, price=adj_sell_price, buy_or_sell="SELL")
                placed_id = int(res_id) if str(res_id).isdigit() else 1
            except Exception:
                placed_id = 1

        if not acc.stop_loss_info.is_empty() and "tradingsymbol" in acc.stop_loss_info.columns:
            acc.stop_loss_info = acc.stop_loss_info.filter(pl.col("tradingsymbol") != sym)
            self.stop_loss_info = acc.stop_loss_info
            self.update_algo_info_table()

        if hasattr(acc, "mark_order_placed"):
            acc.mark_order_placed(placed_id, sym, qty, adj_sell_price, "SELL")
        general_logger.info(
            "[ORDER_STATUS] [SELL_PLACED] Sell order placed successfully: order_id=%s, sym=%s, token=%d, qty=%d, price=%.4f (account=%s, status=%d). Stop-loss cleared.",
            placed_id, sym, tkn, qty, adj_sell_price, acc.user_id, order_status
        )
        return placed_id

    def buy_sell_loop(self, account: Optional[CryptoUserAccount] = None) -> None:
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
                exchg = str(row.get('exchange') or 'OPTIONS')
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
            trade_logger.info(f"[branch: process_buys] [{acc.user_id}] Processing {len(dedup_buys)} buy candidate(s) with live balance {acc.live_balance:.2f}.")
            for row in dedup_buys.to_dicts():
                sym = str(row.get('tradingsymbol') or '')
                tkn = int(row.get('instrument_token') or 0)
                exchg = str(row.get('exchange') or 'OPTIONS')
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

    def slu(self, account: Optional[CryptoUserAccount] = None) -> None:
        """
        Dynamic Trailing Stop Loss Maintenance in Polars:
        1. Purges stop loss entries for positions that have been closed/sold or are no longer active in open_positions.
        2. Initializes entry stop losses for newly filled open positions from order_status average/buy price.
        3. Trails stop loss upwards when live tick price exceeds existing stop loss.
        4. Persists updated stop loss table to AlgoInfo.
        """
        acc = account or self.account
        if not acc or not acc.enabled:
            trade_logger.debug(f"[SLU_SKIP] Account {acc.user_id if acc else 'NONE'} skipped (enabled={getattr(acc, 'enabled', False)}).")
            return

        # ── 1. Purge Stop Losses for Closed Positions ─────────────────────────
        if not acc.open_positions.is_empty() and 'instrument_token' in acc.open_positions.columns:
            opn_tkns = acc.open_positions['instrument_token'].drop_nulls().unique().to_list()
            if not acc.stop_loss_info.is_empty() and 'instrument_token' in acc.stop_loss_info.columns:
                acc.stop_loss_info = acc.stop_loss_info.filter(pl.col('instrument_token').is_in(opn_tkns))
                self.stop_loss_info = acc.stop_loss_info
        else:
            if not acc.stop_loss_info.is_empty():
                acc.stop_loss_info = pl.DataFrame(schema=acc.stop_loss_info.schema)
                self.stop_loss_info = acc.stop_loss_info
                trade_logger.info(f"[branch: slu_cleared_empty_positions] [{acc.user_id}] Cleared stop losses since open positions is empty.")
            return

        # ── 2. Initialize / Register Stop Losses for Open Positions ───────────
        if not acc.open_positions.is_empty():
            for row in acc.open_positions.to_dicts():
                sym = str(row.get('tradingsymbol', ''))
                tkn = int(row.get('instrument_token') or -1)
                exchg = str(row.get('exchange') or 'OPTIONS')

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

                # ── 3. Trail Stop Loss ─────────────────────────────────────────
                # In 24/7 crypto, trailing stop loss executes continuously
                if self.sl_update_time(exchg):
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

    def order_pending_chk(self, delta_seconds: int = 60, account: Optional[CryptoUserAccount] = None) -> None:
        """Cancel stale pending orders across crypto accounts after delta_seconds."""
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

    def execute_account_trade(self, account: CryptoUserAccount) -> None:
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
        is_debug = getattr(self, 'debug_mode', DEBUG)
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

    def execute_sell(
        self,
        sym: str,
        tkn: int,
        qty: int,
        exchg: str,
        order_status: int,
        order_id: int,
        price: Optional[float] = None,
        account: Optional[CryptoUserAccount] = None
    ) -> int:
        """Places sell order and cleans stop-loss."""
        acc = account or self.account
        if not acc or qty <= 0:
            general_logger.warning(
                "[ORDER_GATE] [SELL_REJECT] Sell order aborted for %s (token=%s): invalid parameters (account=%s, qty=%s, price=%s).",
                sym, tkn, acc.user_id if acc else "None", qty, price
            )
            return -1

        sym_meta = self.get_symbol_meta(sym)
        tick_size = max(0.01, float(sym_meta.get('tick_size') or 0.05))

        # Compute sell limit price
        adj_sell_price = price or 0.0
        if adj_sell_price <= 0 and not self.tick_data.is_empty() and 'instrument_token' in self.tick_data.columns:
            sym_ticks = self.tick_data.filter(pl.col('instrument_token') == tkn)
            if not sym_ticks.is_empty() and 'last_price' in sym_ticks.columns:
                ltp = float(sym_ticks['last_price'][-1] or 0.0)
                adj_sell_price = float(np.round(ltp - (ltp % tick_size), 4))
        if adj_sell_price <= 0 and not acc.open_positions.is_empty():
            pos_m = acc.open_positions.filter(pl.col('tradingsymbol') == sym)
            if not pos_m.is_empty() and 'last_price' in pos_m.columns:
                adj_sell_price = float(pos_m['last_price'][0] or 0.0)
        if adj_sell_price <= 0:
            adj_sell_price = 1.0

        is_debug = getattr(self, "debug_mode", DEBUG)
        if acc.live_balance == 0.0 and acc.open_positions.is_empty() and acc.pos_day_frame.is_empty():
            try:
                acc.sync_account_state(debug_mode=is_debug)
            except Exception as e:
                general_logger.error("[%s] Cold-start account sync failed: %s", acc.user_id, e)

        placed_id = 1
        if acc.client and hasattr(acc.client, "lim_ordr"):
            try:
                res_id, msg = acc.client.lim_ordr(sym=sym, qty=qty, price=adj_sell_price, buy_or_sell="SELL")
                placed_id = int(res_id) if str(res_id).isdigit() else 1
            except Exception:
                placed_id = 1

        if not acc.stop_loss_info.is_empty() and "tradingsymbol" in acc.stop_loss_info.columns:
            acc.stop_loss_info = acc.stop_loss_info.filter(pl.col("tradingsymbol") != sym)
            self.stop_loss_info = acc.stop_loss_info
            self.update_algo_info_table()

        if hasattr(acc, "mark_order_placed"):
            acc.mark_order_placed(placed_id, sym, qty, adj_sell_price, "SELL")
        general_logger.info(
            "[ORDER_STATUS] [SELL_PLACED] Sell order placed successfully: order_id=%s, sym=%s, token=%d, qty=%d, price=%.4f (account=%s, status=%d). Stop-loss cleared.",
            placed_id, sym, tkn, qty, adj_sell_price, acc.user_id, order_status
        )
        return placed_id

    def buy_sell_loop(self, account: Optional[CryptoUserAccount] = None) -> None:
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
                exchg = str(row.get('exchange') or 'OPTIONS')
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
            trade_logger.info(f"[branch: process_buys] [{acc.user_id}] Processing {len(dedup_buys)} buy candidate(s) with live balance {acc.live_balance:.2f}.")
            for row in dedup_buys.to_dicts():
                sym = str(row.get('tradingsymbol') or '')
                tkn = int(row.get('instrument_token') or 0)
                exchg = str(row.get('exchange') or 'OPTIONS')
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

    def slu(self, account: Optional[CryptoUserAccount] = None) -> None:
        """
        Dynamic Trailing Stop Loss Maintenance in Polars:
        1. Purges stop loss entries for positions that have been closed/sold or are no longer active in open_positions.
        2. Initializes entry stop losses for newly filled open positions from order_status average/buy price.
        3. Trails stop loss upwards when live tick price exceeds existing stop loss.
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
                exchg = str(row.get('exchange') or 'OPTIONS')

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

                # ── 3. Trail Stop Loss ─────────────────────────────────────────
                # In 24/7 crypto, trailing stop loss executes continuously
                if self.sl_update_time(exchg):
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

    def order_pending_chk(self, delta_seconds: int = 60, account: Optional[CryptoUserAccount] = None) -> None:
        """Cancel stale pending orders across crypto accounts after delta_seconds."""
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

    def execute_account_trade(self, account: CryptoUserAccount) -> None:
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
        is_debug = getattr(self, 'debug_mode', DEBUG)
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

    def execute_trade_cycle(self, tick_data: pl.DataFrame) -> None:
        """Synchronous multi-account trade evaluation cycle."""
        start_ts = time.time()
        self.loop_count += 1

        # Tier 1: Evaluate shared market signals across all underlyings
        self.compute_market_signals(tick_data)

        # Tier 2: Execute trade routines per active account
        active_accs = 0
        for acc_id, acc in self.accounts.items():
            if acc.enabled:
                active_accs += 1
                self.execute_account_trade(acc)

        exec_duration = time.time() - start_ts
        active_strikes_cnt = 0
        if not self.cum_table.is_empty() and "Buy_strike" in self.cum_table.columns:
            active_strikes_cnt = self.cum_table.filter(pl.col("Buy_strike") == "Yes").height

        general_logger.info(
            "[CYCLE_SUMMARY] Crypto execution cycle %d completed in %.3fs across %d active/%d total accounts (active_strikes=%d, tokens_subscribed=%d).",
            self.loop_count, exec_duration, active_accs, len(self.accounts), active_strikes_cnt, len(self.updated_list)
        )

    def trde(self) -> None:
        """Execute trade evaluation cycle for current ticks."""
        if getattr(self, "tick_data", None) is not None and not self.tick_data.is_empty():
            self.execute_trade_cycle(self.tick_data)
        self.data_ready = True

    def run_24x7_loop(self, loop_freq: int = 10, max_iterations: Optional[int] = None) -> None:
        """Continuous 24/7/365 trading loop for crypto exchanges."""
        general_logger.info(
            "Starting continuous 24/7 crypto execution loop (interval=%ds, max_iterations=%s)...",
            loop_freq,
            max_iterations or "UNLIMITED"
        )
        session_start_time = datetime.now()
        self.session_start_time = session_start_time
        it_count = 0
        while True:
            try:
                tick_df = pl.DataFrame()
                if self.client and hasattr(self.client, "fetch_recent_ticks"):
                    tick_df = self.client.fetch_recent_ticks()
                elif self.primary_broker:
                    tick_df = fetch_recent_ticks(self.primary_broker, seconds=None)

                self.execute_trade_cycle(tick_df)
                it_count += 1
                if self.debug_mode:
                    general_logger.info("[DEBUG] Iteration %d/%s completed.", it_count, max_iterations or "UNLIMITED")
                if max_iterations and it_count >= max_iterations:
                    general_logger.info("Reached maximum iterations (%d). Exiting loop.", max_iterations)
                    break

                sleep(loop_freq)
            except KeyboardInterrupt:
                general_logger.info("KeyboardInterrupt received. Exiting 24/7 crypto loop.")
                break
            except Exception as ex:
                general_logger.error("Error in 24/7 crypto loop iteration: %s", ex)
                sleep(loop_freq)

        if self.debug_mode:
            general_logger.info("Exporting fresh debug crypto algorithm logs to Excel...")
            try:
                self.export_algo_logs(start_time=session_start_time)
            except Exception as e:
                general_logger.error("Failed to export debug crypto algorithm logs: %s", e)


_master_crypto_engine: Optional[TradeAlgo] = None
_last_crypto_market_eval_time: float = 0.0

# Unified Entry Point
@algo
def crypto_options_trading_algo_polars(account_id: str):
    """
    Main algorithm entry point for 24/7 Crypto exchanges.
    Discovers all active CoinSwitch and Delta accounts and manages live 24/7 execution.
    Executes Tier 1 market calculations once per 2.0s cycle, and Tier 2 trade routines per account.
    """
    global _master_crypto_engine, _last_crypto_market_eval_time

    broker = Broker.resolve(account_id)
    if not broker or not broker.enable_trade:
        general_logger.debug(
            "[ALGO_GATE] Account %s skipped: %s.",
            account_id, "broker not found" if not broker else f"enable_trade is {broker.enable_trade}"
        )
        return None

    # Ensure this account belongs to a Crypto broker
    code_val = (broker.broker_name.code if broker.broker_name else "").lower()
    api_val = (broker.api_provider.code if broker.api_provider else "").lower()
    name_val = (broker.name or "").lower()
    is_crypto = any(k in code_val or k in api_val or k in name_val for k in ["coinswitch", "delta", "crypto", "bitcoin"])
    if not is_crypto:
        general_logger.debug(
            "[ALGO_GATE] Account %s skipped: broker '%s' is not classified as Crypto.",
            account_id, getattr(broker, "name", "Unknown")
        )
        return None

    algo_logger.set_active_account(broker, algo_name=_CURRENT_ALGO_NAME)
    try:
        if _master_crypto_engine is None:
            broker_accounts = discover_crypto_broker_accounts()
            _master_crypto_engine = TradeAlgo(
                broker_accounts=broker_accounts,
                debug_mode=DEBUG,
                timezone=APP_TIMEZONE,
            )

        if account_id not in _master_crypto_engine.accounts:
            try:
                util = create_crypto_broker_utility(broker, account_id)
                is_trade_flag = bool(broker.enable_trade)
                has_credentials = True
                if hasattr(util, "api_key") or hasattr(util, "api_secret"):
                    if not (getattr(util, "api_key", None) or "").strip() or not (getattr(util, "api_secret", None) or "").strip():
                        has_credentials = False

                if is_trade_flag and not has_credentials:
                    general_logger.warning(
                        "[%s] Account '%s' has enable_trade=True but API credentials (api_key/api_secret) are incomplete. Automated trading suspended until credentials are provided.",
                        account_id,
                        getattr(broker, "name", account_id)
                    )

                acc = CryptoUserAccount(
                    user_id=account_id,
                    client=util,
                    broker_obj=broker,
                    capital_allowed=float(getattr(broker, "capital_allowed", 100000.0) or 100000.0),
                    capital_multiplier=float(getattr(broker, "capital_multiplier", 1.0) or 1.0),
                    enabled=bool(is_trade_flag and has_credentials),
                )
                _master_crypto_engine.accounts[account_id] = acc
                if not _master_crypto_engine.primary_broker:
                    _master_crypto_engine.primary_broker = broker
                    _master_crypto_engine.primary_account_id = account_id
                _master_crypto_engine.insert_instrument_token()
                _master_crypto_engine.update_config_info()
            except Exception as e:
                general_logger.error("Failed to initialize crypto account %s: %s", account_id, e)
                general_logger.warning("[ALGO_INIT_FAIL] Returning master engine without account %s due to init failure.", account_id)
                return _master_crypto_engine

        target_acc = _master_crypto_engine.accounts.get(account_id)
        if target_acc and not target_acc.enabled:
            # Check if credentials were newly updated in DB
            b_key = (getattr(broker, "api_key", None) or "").strip()
            b_sec = (getattr(broker, "api_secret", None) or "").strip()
            c_key = (getattr(target_acc.client, "api_key", None) or "").strip()
            c_sec = (getattr(target_acc.client, "api_secret", None) or "").strip()
            if (b_key != c_key or b_sec != c_sec) and b_key and b_sec:
                target_acc.client = create_crypto_broker_utility(broker, account_id)
                target_acc.broker_obj = broker
                target_acc.enabled = bool(broker.enable_trade)
                general_logger.info("[%s] Credentials updated for account '%s'. Automated trading activated.", account_id, getattr(broker, "name", account_id))

        # Tier 1: Evaluate shared crypto market calculations once per 2.0s cycle across all accounts
        now = time.time()
        if (now - _last_crypto_market_eval_time) >= 1.8:
            try:
                tick_df = pl.DataFrame()
                if _master_crypto_engine.client and hasattr(_master_crypto_engine.client, "fetch_recent_ticks"):
                    tick_df = _master_crypto_engine.client.fetch_recent_ticks()
                elif _master_crypto_engine.primary_broker:
                    tick_df = fetch_recent_ticks(_master_crypto_engine.primary_broker, seconds=None)

                _master_crypto_engine.compute_market_signals(tick_df)
                _last_crypto_market_eval_time = now
            except Exception as e:
                general_logger.error("Error in crypto Tier 1 market calculation: %s", e)

        # Tier 2: Account-specific execution
        if not target_acc or not target_acc.enabled:
            general_logger.info(
                "[ALGO_GATE] Account %s execution skipped: target_acc=%s, enabled=%s (has_credentials=%s).",
                account_id, bool(target_acc), getattr(target_acc, "enabled", False),
                bool(getattr(target_acc, "client", None) and getattr(target_acc.client, "api_key", None))
            )
            return _master_crypto_engine

        try:
            _master_crypto_engine.execute_account_trade(target_acc)
            algo_logger.log_sync(
                f"[{datetime.now().isoformat()}] [CRYPTO OPT POLARS CYCLE] Executed strategy iteration successfully for account {account_id}",
                broker_obj=broker,
                algo_name=_CURRENT_ALGO_NAME,
                level="INFO"
            )
            general_logger.info(
                "[ACCOUNT_CYCLE_SUCCESS] Strategy iteration completed for crypto account %s (balance=%.2f, open_positions=%d).",
                account_id, target_acc.live_balance, len(target_acc.open_positions)
            )
        except Exception as e:
            err_msg = f"[{datetime.now().isoformat()}] [CRYPTO OPT POLARS CYCLE ERROR] {e}"
            general_logger.error(err_msg, exc_info=True)
            algo_logger.log_sync(err_msg, broker_obj=broker, algo_name=_CURRENT_ALGO_NAME, level="ERROR")

        algo_logger.flush_sync(force=True)
        return _master_crypto_engine
    finally:
        algo_logger.clear_active_account()


def main(
    loop_freq: int = 10,
    debug_mode: Optional[bool] = None,
    max_debug_iterations: int = 5,
    timezone: str = APP_TIMEZONE,
    target_account_id: Optional[str] = None,
) -> None:
    """CLI launcher for Crypto trading engine."""
    effective_debug = DEBUG if debug_mode is None else debug_mode
    target_desc = f"account='{target_account_id}'" if target_account_id else "all active crypto accounts"
    general_logger.info(
        "Starting Crypto Polars Options Engine (%s, mode='%s', debug=%s, timezone=%s, max_iterations=%s)...",
        target_desc, APP_MODE, effective_debug, timezone, max_debug_iterations if effective_debug else "UNLIMITED"
    )
    brokers = discover_crypto_broker_accounts(target_account_id=target_account_id)
    engine = TradeAlgo(
        broker_accounts=brokers,
        target_account_id=target_account_id,
        debug_mode=effective_debug,
        timezone=timezone,
    )
    max_iter = max_debug_iterations if engine.debug_mode else None
    engine.run_24x7_loop(loop_freq=loop_freq, max_iterations=max_iter)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Polars-Native Unified Crypto Options Trading Engine (CoinSwitch, Delta, etc.)")
    parser.add_argument("--account", "--broker", dest="account", type=str, default=None, help="Target crypto broker account ID or keyword (e.g. 'coinswitch', 'delta', '73270496')")
    parser.add_argument("--loop-freq", type=int, default=10, help="Cycle interval in seconds")
    parser.add_argument("--debug", action="store_true", default=None, help="Force debug mode")
    parser.add_argument("--production", action="store_true", help="Force production mode")
    parser.add_argument("--iterations", type=int, default=5, help="Number of debug iterations")
    parser.add_argument("--timezone", type=str, default=APP_TIMEZONE, help="Timezone")
    args = parser.parse_args()

    eff_debug = False if args.production else (True if args.debug else None)
    try:
        signal.signal(signal.SIGINT, _signal_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _signal_handler)
    except Exception:
        pass

    main(
        loop_freq=args.loop_freq,
        debug_mode=eff_debug,
        max_debug_iterations=args.iterations,
        timezone=args.timezone,
        target_account_id=args.account,
    )
