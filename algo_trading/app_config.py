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
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Hierarchical .env loading based on APP_INSTANCE or ENV_FILE
_env_file = os.getenv("ENV_FILE")
_raw_instance = os.getenv("APP_INSTANCE")
if _env_file and os.path.exists(_env_file):
    load_dotenv(_env_file, override=True)
elif _raw_instance and os.path.exists(f".env.{_raw_instance}"):
    load_dotenv(f".env.{_raw_instance}", override=True)
elif os.path.exists(".env"):
    load_dotenv(".env")


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


def _get_default_instance() -> str:
    inst = os.getenv("APP_INSTANCE")
    if inst:
        return _clean_env(inst).lower()
    mode = os.getenv("APP_MODE", "prod").lower()
    if mode in ("production", "prod"):
        return "prod"
    if mode in ("debug", "dev"):
        return "debug"
    return "prod"


def _get_default_port(instance: str) -> int:
    # Check for instance-specific override first, e.g. BIND_PORT_DEBUG
    inst_upper = instance.upper()
    if os.getenv(f"BIND_PORT_{inst_upper}"):
        return _get_int(f"BIND_PORT_{inst_upper}", 8001)

    if instance in ("prod", "production", "default"):
        return _get_int("BIND_PORT", 8000)
    if instance in ("debug", "dev"):
        return 8001
    if instance.isdigit():
        return 8000 + int(instance)
    return 8002 + (abs(hash(instance)) % 24)


def _get_default_db(instance: str) -> str:
    inst_upper = instance.upper()
    if os.getenv(f"POSTGRES_DB_{inst_upper}"):
        return _clean_env(os.getenv(f"POSTGRES_DB_{inst_upper}"))

    if instance in ("prod", "production", "default"):
        return _get_str("POSTGRES_DB", "algo_trading")
    if instance in ("debug", "dev"):
        return "algo_trading_debug"
    return f"algo_trading_{instance}"


def _get_default_hotkey(instance: str) -> str:
    env_hotkey = os.getenv("HOTKEY_TOGGLE_BROWSER")
    if env_hotkey is not None:
        return _clean_env(env_hotkey)
    if instance in ("prod", "production", "default"):
        return "ctrl+alt+b"
    if instance in ("debug", "dev"):
        return "ctrl+shift+b"
    return ""


@dataclass(frozen=True, slots=True)
class AppConfig:
    """Immutable application configuration loaded once at startup."""

    # ─── Instance & Mode Identity ────────────────────────────────
    APP_INSTANCE: str = field(default_factory=_get_default_instance)
    APP_MODE: str = field(default_factory=lambda: _get_str(
        "APP_MODE",
        "production" if _get_default_instance() in ("prod", "production", "default") else "debug"
    ))
    TIMEZONE: str = _get_str("APP_TIMEZONE", "America/New_York")  # IANA timezone string
    MARKET_EXCHANGE: str = _get_str("MARKET_EXCHANGE", "U_EXCHANGE")
    ENABLE_PRE_MARKET: bool = _get_bool("ENABLE_PRE_MARKET", False)
    SECRET_KEY: str = os.getenv(
        "DJANGO_SECRET_KEY",
        "CHANGE-ME-in-production-use-python-c-import-secrets-secrets.token_urlsafe(64)"
    )
    M2M_SERVER_KEY: str = _get_str("M2M_SERVER_KEY", "")      # Dedicated Machine-to-Machine API token
    SSL_VERIFY: bool = _get_bool("SSL_VERIFY", True)          # Enforce SSL/TLS certificate verification

    # ─── Network / Host ──────────────────────────────────────────
    ALLOWED_HOSTS: str = _get_str("ALLOWED_HOSTS", "*")      # Comma-separated
    BIND_ADDRESS: str = _get_str("BIND_ADDRESS", "0.0.0.0")
    BIND_PORT: int = field(default_factory=lambda: _get_default_port(_get_default_instance()))

    # ─── Database (PostgreSQL 18 default / SQLite optional) ──────
    DB_ENGINE: str = _get_str("DB_ENGINE", "postgresql")     # "postgresql" | "sqlite"
    DB_NAME: str = field(default_factory=lambda: _get_default_db(_get_default_instance()))
    DB_USER: str = _get_str("POSTGRES_USER", "postgres")    # default postgres superuser or appuser
    DB_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    DB_HOST: str = _get_str("POSTGRES_HOST", "127.0.0.1")
    DB_PORT: int = _get_int("POSTGRES_PORT", 5432)

    # ─── Scrapling Stealth Browser Sniffer ─────────────────────────
    PORTAL_URL: str = _get_str("PORTAL_URL", "")
    PORTAL_USERNAME: str = _get_str("PORTAL_USERNAME", "")
    PORTAL_PASSWORD: str = os.getenv("PORTAL_PASSWORD", "")
    PORTAL_TOTP_SECRET: str = os.getenv("PORTAL_TOTP_SECRET", "")
    SCRAPLING_HEADLESS: bool = _get_bool("SCRAPLING_HEADLESS", True)
    SCRAPLING_MAX_RAM_MB: int = _get_int("SCRAPLING_MAX_RAM_MB", 800)
    PROFILES_DIR: str = field(default_factory=lambda: _get_str("PROFILES_DIR", f"profiles/{_get_default_instance()}"))
    HOTKEY_TOGGLE_BROWSER: str = field(default_factory=lambda: _get_default_hotkey(_get_default_instance()))

    # ─── Dual-NIC Network Failover ────────────────────────────────
    PRIMARY_NIC_ALIAS: str = _get_str("PRIMARY_NIC_ALIAS", "Ethernet")
    BACKUP_NIC_ALIAS: str = _get_str("BACKUP_NIC_ALIAS", "Wi-Fi")
    PING_PROBE_HOSTS: str = _get_str("PING_PROBE_HOSTS", "8.8.8.8,1.1.1.1")
    FAILOVER_CHECK_INTERVAL: float = float(_get_str("FAILOVER_CHECK_INTERVAL", "5.0"))
    FAILOVER_HYSTERESIS_SECONDS: float = float(_get_str("FAILOVER_HYSTERESIS_SECONDS", "60.0"))

    # ─── Hardware & Thermal Protection (Thin Client PC) ────────────
    THERMAL_WARNING_TEMP: float = float(_get_str("THERMAL_WARNING_TEMP", "75.0"))
    THERMAL_CRITICAL_TEMP: float = float(_get_str("THERMAL_CRITICAL_TEMP", "82.0"))
    THERMAL_CHECK_INTERVAL: float = float(_get_str("THERMAL_CHECK_INTERVAL", "30.0"))

    # ─── Power Lifecycle (Task Scheduler RTC Wake & Hibernate) ───
    ENABLE_AUTO_WAKE: bool = field(default_factory=lambda: _get_bool(
        "ENABLE_AUTO_WAKE",
        _get_default_instance() in ("prod", "production", "default")
    ))
    ENABLE_AUTO_HIBERNATE: bool = field(default_factory=lambda: _get_bool(
        "ENABLE_AUTO_HIBERNATE",
        _get_default_instance() in ("prod", "production", "default")
    ))
    WAKE_TIME_ET: str = _get_str("WAKE_TIME_ET", "09:15")
    HIBERNATE_TIME_ET: str = _get_str("HIBERNATE_TIME_ET", "16:15")

    # ─── WebSocket Broker Toggles ─────────────────────────────────
    # Add a new line for each broker. Format: WS_BROKER_<NAME>_ENABLED
    WS_BROKER_A_ENABLED: bool = _get_bool("WS_BROKER_A_ENABLED", True)
    WS_BROKER_B_ENABLED: bool = _get_bool("WS_BROKER_B_ENABLED", True)
    WS_BROKER_C_ENABLED: bool = _get_bool("WS_BROKER_C_ENABLED", False)

    # ─── Performance & Memory Controls (Thin Client / Local PC Optimized) ───
    UVICORN_WORKERS: int = _get_int("UVICORN_WORKERS", 1)    # Keep 1 for 1-2 vCPU
    UVICORN_BACKLOG: int = _get_int("UVICORN_BACKLOG", 128)
    DB_CONN_POOL_SIZE: int = _get_int("DB_CONN_POOL_SIZE", 4) # Lean default
    DB_POOL_MIN: int = _get_int("DB_POOL_MIN", 0)             # Scale down to 0 idle connections to conserve RAM
    DB_POOL_MAX: int = _get_int("DB_POOL_MAX", 4)
    Q_CLUSTER_WORKERS: int = _get_int("Q_CLUSTER_WORKERS", 1) # 1 worker for low-memory footprint
    POLARS_MAX_THREADS: int = _get_int("POLARS_MAX_THREADS", os.cpu_count() or 4) # Physical cores on thin client
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
        return self.APP_MODE.lower() in ("debug", "dev") or self.APP_INSTANCE not in ("prod", "production", "default")

    @property
    def is_production(self) -> bool:
        return not self.is_debug

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
