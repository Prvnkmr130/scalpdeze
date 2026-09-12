"""
algo_trading/brokers/config.py
──────────────────────────────
Engine-specific configuration for the async WebSocket engine.
Separate from Django's app_config.py — these control the asyncio
pipeline behaviour (queue sizes, flush intervals, reconnect policy).

Usage:
    from algo_trading.brokers.config import engine_config
"""

from dataclasses import dataclass, field

from algo_trading.app_config import config as app_config


@dataclass(frozen=True, slots=True)
class EngineConfig:
    """Immutable engine configuration loaded once at startup."""

    # ─── PostgreSQL (asyncpg) ────────────────────────────────────
    # Built from the central app_config values so there's one source of truth.
    POSTGRES_DSN: str = field(default_factory=lambda: (
        f"postgresql://{app_config.DB_USER}:{app_config.DB_PASSWORD}"
        f"@{app_config.DB_HOST}:{app_config.DB_PORT}/{app_config.DB_NAME}"
    ))

    # ─── Tick Ingestion (per-broker isolated pipeline) ───────────
    TICK_QUEUE_SIZE: int = app_config.WS_TICK_QUEUE_SIZE
    TICK_FLUSH_INTERVAL: float = app_config.WS_TICK_FLUSH_INTERVAL  # seconds between forced flushes
    TICK_BATCH_SIZE_LIMIT: int = app_config.WS_TICK_BATCH_SIZE_LIMIT # safety cap — flush early if reached
    RATE_LIMIT_INTERVAL: float = 0.1          # max 1 msg every 100 ms per broker

    # ─── Log Ingestion (shared pipeline) ─────────────────────────
    LOG_QUEUE_SIZE: int = app_config.WS_LOG_QUEUE_SIZE
    LOG_FLUSH_INTERVAL: float = 3.0
    LOG_BATCH_SIZE_LIMIT: int = 200

    # ─── Connection Management ───────────────────────────────────
    MAX_CONCURRENT_WRITES: int = 2            # semaphore cap per consumer
    DB_POOL_MIN: int = app_config.DB_POOL_MIN
    DB_POOL_MAX: int = app_config.DB_POOL_MAX

    # ─── Reconnect Policy ────────────────────────────────────────
    RECONNECT_BASE_DELAY: float = 2.0         # initial backoff (seconds)
    RECONNECT_MAX_DELAY: float = 60.0         # capped exponential backoff

    # ─── Dynamic Token Refresh & State Sync ──────────────────────
    TOKEN_REFRESH_INTERVAL: float = 30.0     # re-read instrument tokens from DB (was 5s — too aggressive)
    ALGO_INFO_SYNC_CYCLES: int = app_config.ALGO_INFO_SYNC_CYCLES  # cycles between AlgoInfo state syncs
    ALGO_LOOP_INTERVAL: float = app_config.ALGO_LOOP_INTERVAL_SECONDS  # execution frequency in seconds

    # ─── Heartbeat ───────────────────────────────────────────────
    WS_HEARTBEAT_INTERVAL: float = 30.0       # aiohttp / socketio heartbeat


# Singleton — import this everywhere inside the engine
engine_config = EngineConfig()
