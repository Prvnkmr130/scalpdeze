"""
algo_trading/brokers/consumers.py
──────────────────────────────────
Batch consumers for the async engine.

•  ``tick_consumer``       — drains a per-broker tick queue → PostgreSQL COPY
•  ``log_consumer``        — drains the shared log queue   → PostgreSQL COPY
•  ``ensure_broker_table`` — auto-creates ``{broker}_stream_kv`` on first boot
•  ``log_event``           — helper to push a log entry into the shared queue
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime, timezone

import re
import asyncpg
import orjson

from algo_trading.brokers.config import engine_config

logger = logging.getLogger("algo_trading.brokers")


# =====================================================================
# 1. PER-BROKER TABLE BOOTSTRAP
# =====================================================================

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table_name} (
    timestamp   BIGINT          NOT NULL,
    broker      VARCHAR(50)     NOT NULL,
    data        JSONB           NOT NULL,
    log_date    DATE            NOT NULL
) PARTITION BY RANGE (log_date);
"""

_PARTITION_FUNC_DDL = """
CREATE OR REPLACE FUNCTION auto_create_weekly_partition_{safe_name}()
RETURNS trigger AS $$
DECLARE
    partition_date DATE;
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
BEGIN
    partition_date := date_trunc('week', NEW.log_date)::date;
    partition_start := partition_date;
    partition_end := partition_start + INTERVAL '7 days';
    partition_name := '{table_name}_y' || to_char(partition_start, 'YYYY_"w"IW');

    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF {table_name} FOR VALUES FROM (%L) TO (%L);',
            partition_name, partition_start, partition_end
        );
        EXECUTE format(
            'CREATE INDEX %I ON %I (broker, timestamp DESC);',
            'idx_' || partition_name, partition_name
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_TRIGGER_DDL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE LOWER(tgname) = LOWER('trg_auto_partition_{safe_name}')
    ) THEN
        CREATE TRIGGER trg_auto_partition_{safe_name}
            BEFORE INSERT ON {table_name}
            FOR EACH ROW EXECUTE FUNCTION auto_create_weekly_partition_{safe_name}();
    END IF;
END
$$;
"""


_ENSURE_PARTITION_FUNC_DDL = """
CREATE OR REPLACE FUNCTION ensure_weekly_partition(tbl_name TEXT, target_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_date DATE;
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
BEGIN
    partition_date := date_trunc('week', target_date)::date;
    partition_start := partition_date;
    partition_end := partition_start + INTERVAL '7 days';
    partition_name := tbl_name || '_y' || to_char(partition_start, 'YYYY_"w"IW');

    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L);',
            partition_name, tbl_name, partition_start, partition_end
        );
        EXECUTE format(
            'CREATE INDEX %I ON %I (broker, timestamp DESC);',
            'idx_' || partition_name, partition_name
        );
    END IF;
END;
$$ LANGUAGE plpgsql;
"""


def get_broker_stream_table_name(account_id: str) -> str:
    """Deterministic, SQL-safe stream table name for an account or broker."""
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', str(account_id).lower().strip())
    safe_name = f"b_{cleaned}" if cleaned and cleaned[0].isdigit() else (cleaned or "stream")
    return f"{safe_name}_stream_kv"


async def ensure_broker_table(db_pool: asyncpg.Pool, broker_name: str) -> str:
    """
    Create the per-broker tick table if it doesn't already exist.
    Sets up weekly date-range partitioning with auto-create trigger.
    Returns the table name.
    """
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', str(broker_name).lower().strip())
    safe_name = f"b_{cleaned}" if cleaned and cleaned[0].isdigit() else (cleaned or "stream")
    table_name = f"{safe_name}_stream_kv"

    async with db_pool.acquire() as conn:
        await conn.execute(_ENSURE_PARTITION_FUNC_DDL)
        await conn.execute(_TABLE_DDL.format(table_name=table_name))
        await conn.execute(
            _PARTITION_FUNC_DDL.format(table_name=table_name, safe_name=safe_name)
        )
        await conn.execute(
            _TRIGGER_DDL.format(table_name=table_name, safe_name=safe_name)
        )

    logger.info("Ensured table '%s' exists with auto-partitioning.", table_name)
    return table_name


# =====================================================================
# 2. TICK CONSUMER (one instance per broker)
# =====================================================================

async def tick_consumer(
    db_pool: asyncpg.Pool,
    tick_queue: asyncio.Queue,
    table_name: str,
) -> None:
    """
    Drains ``tick_queue`` using a double-buffer pattern and bulk-inserts
    into ``table_name`` via ``asyncpg.copy_records_to_table``.

    Flush triggers:
        • ``TICK_FLUSH_INTERVAL`` seconds elapsed  OR
        • ``TICK_BATCH_SIZE_LIMIT`` items accumulated
    """
    cfg = engine_config
    db_semaphore = asyncio.Semaphore(cfg.MAX_CONCURRENT_WRITES)
    current_batch: list[dict] = []
    last_flush = time.time()

    logger.info(
        "Tick consumer started for table '%s' "
        "(flush every %.1fs or %d items).",
        table_name,
        cfg.TICK_FLUSH_INTERVAL,
        cfg.TICK_BATCH_SIZE_LIMIT,
    )

    try:
        while True:
            # Drain one item (with short timeout so we can check flush timers)
            try:
                item = await asyncio.wait_for(tick_queue.get(), timeout=0.05)
                current_batch.append(item)
                tick_queue.task_done()
            except asyncio.TimeoutError:
                pass

            elapsed = time.time() - last_flush
            should_flush = (
                len(current_batch) >= cfg.TICK_BATCH_SIZE_LIMIT
                or elapsed >= cfg.TICK_FLUSH_INTERVAL
            )

            if should_flush and current_batch:
                batch_to_flush = current_batch
                current_batch = []
                last_flush = time.time()
                asyncio.create_task(
                    _flush_ticks(db_pool, db_semaphore, batch_to_flush, table_name)
                )
    except asyncio.CancelledError:
        # Final flush on shutdown
        if current_batch:
            await _flush_ticks(db_pool, db_semaphore, current_batch, table_name)


_broker_ids: dict[str, int | None] = {}
_ensured_partitions: set[tuple[str, date]] = set()


async def _get_broker_id(conn, identifier: str) -> int | None:
    """Helper to fetch and cache broker/account ID by account_id, name, broker_name, or api_provider."""
    if not identifier:
        return None
    key = str(identifier).lower()
    if key not in _broker_ids:
        row = await conn.fetchrow(
            """
            SELECT id FROM kalai_broker 
            WHERE LOWER(account_id) = $1 
               OR LOWER(name) = $1 
               OR LOWER(broker_name) = $1 
               OR LOWER(api_provider) = $1 
            LIMIT 1;
            """,
            key
        )
        _broker_ids[key] = row["id"] if row else None
    return _broker_ids[key]



async def _flush_ticks(
    db_pool: asyncpg.Pool,
    db_semaphore: asyncio.Semaphore,
    batch: list[dict],
    table_name: str,
) -> None:
    """Binary COPY a batch of unified frames into both the account stream table and the Django Admin table."""
    async with db_semaphore:
        t0 = time.time()
        async with db_pool.acquire() as conn:
            try:
                records = []
                processed_tick_records = []
                for item in batch:
                    ts = item["timestamp"]
                    account_id = item.get("account_id", item.get("broker"))
                    broker = item["broker"]
                    data_jsonb = orjson.dumps(item["data"]).decode("utf-8")
                    log_date = datetime.fromtimestamp(ts / 1e9, tz=timezone.utc).date()
                    records.append((ts, broker, data_jsonb, log_date))

                    # Fetch account/broker ID and prepare record for Django ProcessedTickStore
                    b_id = await _get_broker_id(conn, account_id)
                    if b_id is None:
                        b_id = await _get_broker_id(conn, broker)
                    if b_id is not None:
                        dt_ts = datetime.fromtimestamp(ts / 1e9, tz=timezone.utc)
                        processed_tick_records.append((b_id, data_jsonb, dt_ts))

                # Ensure partitions exist before binary COPY (cached per table & date)
                unique_dates = {datetime.fromtimestamp(item["timestamp"] / 1e9, tz=timezone.utc).date() for item in batch}
                for log_date in unique_dates:
                    part_key = (table_name, log_date)
                    if part_key not in _ensured_partitions:
                        await conn.execute("SELECT ensure_weekly_partition($1, $2);", table_name, log_date)
                        _ensured_partitions.add(part_key)

                # COPY to account-specific stream table
                await conn.copy_records_to_table(
                    table_name,
                    records=records,
                    columns=["timestamp", "broker", "data", "log_date"],
                )

                # COPY to Django Admin ProcessedTickStore table
                if processed_tick_records:
                    await conn.copy_records_to_table(
                        "kalai_processedtickstore",
                        records=processed_tick_records,
                        columns=["account_id", "data", "timestamp"],
                    )
                logger.debug(
                    "[%s] Flushed %d ticks in %.3fs.",
                    table_name,
                    len(batch),
                    time.time() - t0,
                )
            except Exception as exc:
                logger.error(
                    "[%s] Tick flush FAILED (%d records): %s",
                    table_name,
                    len(batch),
                    exc,
                )
            finally:
                del batch


# =====================================================================
# 3. LOG CONSUMER (single shared instance)
# =====================================================================

async def log_consumer(
    db_pool: asyncpg.Pool,
    log_queue: asyncio.Queue,
) -> None:
    """
    Drains the shared ``log_queue`` and bulk-inserts into ``system_logs``.
    """
    cfg = engine_config
    db_semaphore = asyncio.Semaphore(cfg.MAX_CONCURRENT_WRITES)
    current_batch: list[dict] = []
    last_flush = time.time()

    logger.info(
        "Log consumer started (flush every %.1fs or %d items).",
        cfg.LOG_FLUSH_INTERVAL,
        cfg.LOG_BATCH_SIZE_LIMIT,
    )

    try:
        while True:
            try:
                item = await asyncio.wait_for(log_queue.get(), timeout=0.1)
                current_batch.append(item)
                log_queue.task_done()
            except asyncio.TimeoutError:
                pass

            elapsed = time.time() - last_flush
            should_flush = (
                len(current_batch) >= cfg.LOG_BATCH_SIZE_LIMIT
                or elapsed >= cfg.LOG_FLUSH_INTERVAL
            )

            if should_flush and current_batch:
                batch_to_flush = current_batch
                current_batch = []
                last_flush = time.time()
                asyncio.create_task(
                    _flush_logs(db_pool, db_semaphore, batch_to_flush)
                )
    except asyncio.CancelledError:
        if current_batch:
            await _flush_logs(db_pool, db_semaphore, current_batch)


async def _flush_logs(
    db_pool: asyncpg.Pool,
    db_semaphore: asyncio.Semaphore,
    batch: list[dict],
) -> None:
    """Binary COPY a batch of log entries into ``system_logs``."""
    async with db_semaphore:
        async with db_pool.acquire() as conn:
            try:
                records = [
                    (
                        log["timestamp"],
                        log["level"],
                        log["context"],
                        log["message"],
                        log["log_date"],
                    )
                    for log in batch
                ]
                await conn.copy_records_to_table(
                    "system_logs",
                    records=records,
                    columns=["timestamp", "level", "context", "message", "log_date"],
                )
            except Exception as exc:
                # Emergency fallback: print to stdout so logs aren't lost
                logger.error("Log flush FAILED (%d records): %s", len(batch), exc)
                for log in batch:
                    print(f"FAILED_LOG: [{log['level']}] [{log['context']}] {log['message']}")
            finally:
                del batch


# =====================================================================
# 4. LOG EVENT HELPER
# =====================================================================

def log_event(
    level: str,
    message: str,
    context: str = "SYSTEM",
    log_queue: asyncio.Queue | None = None,
) -> None:
    """
    Push a structured log entry into the shared log queue for database persistence.
    Also emits to the standard logger for uniform console and file output.
    """
    lvl_upper = level.upper()
    ctx_upper = context.upper()
    now = datetime.now(timezone.utc)
    payload = {
        "timestamp": now,
        "level": lvl_upper,
        "context": ctx_upper,
        "message": message,
        "log_date": now.date(),
    }

    if log_queue is not None:
        try:
            log_queue.put_nowait(payload)
            return
        except asyncio.QueueFull:
            logger.warning("[%s] Log queue full — dropping DB log: %s", ctx_upper, message)

    log_method = getattr(logger, level.lower(), logger.info)
    log_method("[%s] %s", ctx_upper, message)
