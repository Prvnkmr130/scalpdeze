"""
algo_trading/app_config.py
──────────────────────────
Centralized application configuration.
All operator-tunable options live here. Django settings.py reads from this module.

Usage:
    from algo_trading.app_config import config
    if config.APP_MODE == "production":
        ...
"""

import os
from dataclasses import dataclass


def _clean_env(val: str) -> str:
    """Strip inline comments and whitespace from an env var string."""
    if not val:
        return ""
    # Only treat '#' as a comment delimiter if it is preceded by whitespace
    # to avoid stripping valid password characters like 'P@ss#123'
    parts = val.split(" #", 1)
    if len(parts) > 1:
        return parts[0].strip()
    return val.strip()


def _get_str(key: str, default: str) -> str:
    raw = os.getenv(key)
    if raw is None:
        return default
    return _clean_env(raw)


def _get_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return default
    cleaned = _clean_env(raw)
    try:
        return int(cleaned)
    except (ValueError, TypeError):
        return default


def _get_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    cleaned = _clean_env(raw).lower()
    return cleaned in ("true", "1", "yes")


@dataclass(frozen=True, slots=True)
class AppConfig:
    """Immutable application configuration loaded once at startup."""

    # ─── Core & Security ─────────────────────────────────────────
    APP_MODE: str = _get_str("APP_MODE", "debug")            # "debug" | "production"
    TIMEZONE: str = _get_str("APP_TIMEZONE", "Asia/Kolkata")  # IANA timezone string
    SECRET_KEY: str = os.getenv(
        "DJANGO_SECRET_KEY",
        "CHANGE-ME-in-production-use-python-c-import-secrets-secrets.token_urlsafe(64)"
    )
    M2M_SERVER_KEY: str = _get_str("M2M_SERVER_KEY", "")      # Dedicated Machine-to-Machine API token
    SSL_VERIFY: bool = _get_bool("SSL_VERIFY", True)          # Enforce SSL/TLS certificate verification

    # ─── Network / Host ──────────────────────────────────────────
    ALLOWED_HOSTS: str = _get_str("ALLOWED_HOSTS", "*")      # Comma-separated
    BIND_ADDRESS: str = _get_str("BIND_ADDRESS", "0.0.0.0")
    BIND_PORT: int = _get_int("BIND_PORT", 8000)

    # ─── Database ────────────────────────────────────────────────
    DB_NAME: str = _get_str("POSTGRES_DB", "algo_trading")
    DB_USER: str = _get_str("POSTGRES_USER", "appuser")
    DB_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
    DB_HOST: str = _get_str("POSTGRES_HOST", "127.0.0.1")    # localhost for single container
    DB_PORT: int = _get_int("POSTGRES_PORT", 5432)


    # ─── WebSocket Broker Toggles ─────────────────────────────────
    # Add a new line for each broker. Format: WS_BROKER_<NAME>_ENABLED
    WS_BROKER_A_ENABLED: bool = _get_bool("WS_BROKER_A_ENABLED", True)
    WS_BROKER_B_ENABLED: bool = _get_bool("WS_BROKER_B_ENABLED", True)
    WS_BROKER_C_ENABLED: bool = _get_bool("WS_BROKER_C_ENABLED", False)

    # ─── Performance & Memory Controls (Small Cloud Instance Optimized) ────────
    UVICORN_WORKERS: int = _get_int("UVICORN_WORKERS", 1)    # Keep 1 for 1-2 vCPU
    UVICORN_BACKLOG: int = _get_int("UVICORN_BACKLOG", 128)
    DB_CONN_POOL_SIZE: int = _get_int("DB_CONN_POOL_SIZE", 4) # Lean default for memory-constrained cloud environments
    DB_POOL_MIN: int = _get_int("DB_POOL_MIN", 0)             # Scale down to 0 idle connections to conserve RAM
    DB_POOL_MAX: int = _get_int("DB_POOL_MAX", 4)
    Q_CLUSTER_WORKERS: int = _get_int("Q_CLUSTER_WORKERS", 1) # 1 worker for low-memory footprint
    POLARS_MAX_THREADS: int = _get_int("POLARS_MAX_THREADS", 2) # Restrict Polars/Rayon thread pool size
    ALGO_WORKER_COUNT: int = _get_int("ALGO_WORKER_COUNT", 1)
    ALGO_LOOP_INTERVAL_SECONDS: float = float(_get_str("ALGO_LOOP_INTERVAL_SECONDS", "2.0"))  # Sequential execution interval
    ALGO_INFO_SYNC_CYCLES: int = _get_int("ALGO_INFO_SYNC_CYCLES", 15)  # Trading loop iterations between AlgoInfo state syncs

    # ─── WebSocket Engine Queues & Retention ──────────────────────
    WS_TICK_QUEUE_SIZE: int = _get_int("WS_TICK_QUEUE_SIZE", 10_000)
    WS_TICK_FLUSH_INTERVAL: float = float(_get_str("WS_TICK_FLUSH_INTERVAL", "30.0"))
    WS_TICK_BATCH_SIZE_LIMIT: int = _get_int("WS_TICK_BATCH_SIZE_LIMIT", 50_000)
    WS_LOG_QUEUE_SIZE: int = _get_int("WS_LOG_QUEUE_SIZE", 2_000)

    # ─── In-Memory Polars Buffer & Garbage Collection Caps ────────
    MAX_CANDLE_HISTORY_BARS: int = _get_int("MAX_CANDLE_HISTORY_BARS", 25) # Keep last 25 bars in RAM per token/timeframe (lean footprint)
    GC_COLLECTION_INTERVAL_CYCLES: int = _get_int("GC_COLLECTION_INTERVAL_CYCLES", 5) # Run gc.collect() every N cycles

    # ─── Logging ──────────────────────────────────────────────────
    LOG_LEVEL: str = _get_str("LOG_LEVEL", "INFO")           # DEBUG | INFO | WARNING | ERROR

    @property
    def is_debug(self) -> bool:
        return self.APP_MODE == "debug"

    @property
    def is_production(self) -> bool:
        return self.APP_MODE == "production"

    @property
    def allowed_hosts_list(self) -> list[str]:
        server_ip = _get_str("SERVER_IP", "").strip()
        raw_allowed = self.ALLOWED_HOSTS
        if server_ip:
            raw_allowed = raw_allowed.replace("${SERVER_IP}", server_ip).replace("$SERVER_IP", server_ip)

        if self.is_debug:
            return ["*"] if raw_allowed == "*" else [h.strip() for h in raw_allowed.split(",") if h.strip()]
        
        # Production Mode
        if raw_allowed == "*":
            return ["*"]

        hosts = [h.strip() for h in raw_allowed.split(",") if h.strip() and h.strip() != "*"]
        if server_ip and server_ip not in hosts:
            hosts.append(server_ip)

        defaults = ["prvntrde.net.in", "127.0.0.1", "localhost"]
        for d in defaults:
            if d not in hosts:
                hosts.append(d)
        return hosts

    @property
    def enabled_brokers(self) -> dict[str, bool]:
        """Returns a dict of broker_name → enabled status."""
        return {
            "broker_a": self.WS_BROKER_A_ENABLED,
            "broker_b": self.WS_BROKER_B_ENABLED,
            "broker_c": self.WS_BROKER_C_ENABLED,
        }


# Singleton instance — import this everywhere
config = AppConfig()
