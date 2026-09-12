"""
algo_trading/brokers/engine.py
───────────────────────────────
Main orchestrator for the async WebSocket engine.

For each enabled broker it creates a fully isolated pipeline:
    Feed  →  tick_queue  →  tick_consumer  →  {broker}_stream_kv

All brokers share a single log pipeline:
    log_event  →  log_queue  →  log_consumer  →  system_logs

Usage:
    python manage.py run_ws_engine                            # all enabled
    python manage.py run_ws_engine --brokers zerodha coindcx  # specific
"""

from __future__ import annotations

import asyncio
import logging
import re
import signal
from datetime import timedelta, datetime, time

import asyncpg

logger = logging.getLogger("algo_trading.brokers")

async def clear_old_logs_and_ticks_loop(
    db_pool: asyncpg.Pool,
    log_retention_days: int = 7,
    tick_retention_days: int = 2,
    chunk_size: int = 5000,
    pause_seconds: float = 0.05,
) -> None:
    """
    Background task that deletes old DB logs and ticks in throttled batches.
    Prevents database CPU spikes, massive single WAL transactions, and table locks.
    Runs once immediately on startup, and then daily at midnight.
    """
    logger = logging.getLogger("algo_trading.brokers")

    def _fmt_bytes(b: int | float | None) -> str:
        if not b or b < 0:
            return "0 B"
        val = float(b)
        for u in ["B", "KB", "MB", "GB"]:
            if val < 1024.0:
                return f"{val:.2f} {u}" if u in ["MB", "GB"] else f"{int(val)} {u}"
            val /= 1024.0
        return f"{val:.2f} TB"

    async def _delete_in_chunks_async(
        conn,
        table_name: str,
        retention_interval: timedelta,
        id_col: str = "id",
    ) -> tuple[int, int, int]:
        """
        Asynchronously delete old rows in batches using asyncpg with pauses,
        tracking table sizes before and after deletion.
        Returns (total_deleted, initial_size_bytes, final_size_bytes).
        """
        if not re.match(r"^[a-zA-Z0-9_]+$", table_name) or not re.match(r"^[a-zA-Z0-9_]+$", id_col):
            logger.error("Invalid relation identifier: table=%s, id_col=%s", table_name, id_col)
            return 0, 0, 0

        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1);",
            table_name,
        )
        if not exists:
            logger.debug("Table '%s' does not exist; skipping async chunked delete.", table_name)
            return 0, 0, 0

        initial_size = await conn.fetchval(
            "SELECT pg_total_relation_size(quote_ident($1))::bigint;", table_name
        ) or 0

        total_deleted = 0
        query = f"""
            DELETE FROM {table_name}
            WHERE {id_col} IN (
                SELECT {id_col} FROM {table_name}
                WHERE timestamp < NOW() - $1::interval
                LIMIT $2
            );
        """
        while True:
            res = await conn.execute(query, retention_interval, chunk_size)
            count = 0
            if res and " " in res:
                try:
                    count = int(res.split(" ")[-1])
                except (ValueError, IndexError):
                    count = 0
            total_deleted += count
            if count < chunk_size:
                break
            if pause_seconds > 0:
                await asyncio.sleep(pause_seconds)

        final_size = await conn.fetchval(
            "SELECT pg_total_relation_size(quote_ident($1))::bigint;", table_name
        ) or 0
        freed = max(0, initial_size - final_size)

        logger.info(
            "Async chunked delete on '%s': %d rows deleted. Size: %s -> %s (freed: %s).",
            table_name, total_deleted, _fmt_bytes(initial_size), _fmt_bytes(final_size), _fmt_bytes(freed)
        )
        return total_deleted, initial_size, final_size

    # 1. Run once immediately on startup
    try:
        logger.info("Executing database startup logs and ticks cleanup in throttled chunks...")
        async with db_pool.acquire() as conn:
            del_sys, _, _ = await _delete_in_chunks_async(
                conn, "system_logs", timedelta(days=log_retention_days), id_col="ctid"
            )
            del_algo, _, _ = await _delete_in_chunks_async(
                conn, "kalai_algolog", timedelta(days=log_retention_days), id_col="id"
            )
            del_ticks, _, _ = await _delete_in_chunks_async(
                conn, "kalai_processedtickstore", timedelta(days=tick_retention_days), id_col="id"
            )
            logger.info(
                "Startup database cleanup completed: deleted %s system logs, %s algo logs, %s processed ticks "
                "(chunk_size=%d, pause=%ss, retaining %d days logs, %d days ticks).",
                del_sys,
                del_algo,
                del_ticks,
                chunk_size,
                pause_seconds,
                log_retention_days,
                tick_retention_days,
            )
    except Exception as e:
        logger.error("Error during startup database log cleanup: %s", e)

    # 2. Daily midnight loop
    while True:
        now = datetime.now()
        tomorrow = now + timedelta(days=1)
        midnight = datetime.combine(tomorrow.date(), time.min)
        seconds_to_midnight = (midnight - now).total_seconds()

        logger.info(f"Database logs and ticks pruner scheduled next for midnight ({midnight}). Sleeping for {seconds_to_midnight:.1f}s.")
        await asyncio.sleep(seconds_to_midnight)

        try:
            logger.info("Executing scheduled daily midnight database cleanup in throttled chunks...")
            async with db_pool.acquire() as conn:
                del_sys, _, _ = await _delete_in_chunks_async(
                    conn, "system_logs", timedelta(days=log_retention_days), id_col="ctid"
                )
                del_algo, _, _ = await _delete_in_chunks_async(
                    conn, "kalai_algolog", timedelta(days=log_retention_days), id_col="id"
                )
                del_ticks, _, _ = await _delete_in_chunks_async(
                    conn, "kalai_processedtickstore", timedelta(days=tick_retention_days), id_col="id"
                )
                logger.info(
                    "Daily database midnight cleanup completed: deleted %s system logs, %s algo logs, %s processed ticks.",
                    del_sys,
                    del_algo,
                    del_ticks,
                )
        except Exception as e:
            logger.error("Error during daily midnight database cleanup: %s", e)


async def run_engine(broker_filter: list[str] | None = None) -> None:
    """
    Entrypoint: discover feeds, create isolated pipelines, run forever.

    Parameters
    ----------
    broker_filter
        If provided, only these broker names are started
        (must also be enabled in the DB).
    """
    from algo_trading.brokers.base import BaseFeed
    from algo_trading.brokers.config import engine_config
    from algo_trading.brokers.consumers import (
        ensure_broker_table,
        log_consumer,
        log_event,
        tick_consumer,
    )
    from algo_trading.brokers.registry import BrokerRegistry

    cfg = engine_config

    # ── 1. Connect to PostgreSQL ────────────────────────────────
    logger.info("Connecting to PostgreSQL… (%s)", cfg.POSTGRES_DSN.split("@")[-1])
    db_pool = await asyncpg.create_pool(
        dsn=cfg.POSTGRES_DSN,
        min_size=cfg.DB_POOL_MIN,
        max_size=cfg.DB_POOL_MAX,
    )
    logger.info("PostgreSQL pool created (min=%d, max=%d).", cfg.DB_POOL_MIN, cfg.DB_POOL_MAX)

    # Ensure system_logs table exists
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS system_logs (
                timestamp   TIMESTAMP WITH TIME ZONE NOT NULL,
                level       VARCHAR(20)              NOT NULL,
                context     VARCHAR(50)              NOT NULL,
                message     TEXT                     NOT NULL,
                log_date    DATE                     NOT NULL
            );
        """)

    # ── 2. Shared log queue ─────────────────────────────────────
    log_queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.LOG_QUEUE_SIZE)
    log_event("INFO", "Engine starting…", "SYSTEM", log_queue)

    # ── 3. Discover and instantiate feeds ───────────────────────
    registry = BrokerRegistry()
    feeds = await registry.get_enabled_feeds(broker_filter)

    # ── 4. Create per-account isolated pipelines ─────────────────
    active_feeds: dict[str, BaseFeed] = {}
    active_pipelines: dict[str, list[asyncio.Task]] = {}

    async def start_account_pipeline(feed: BaseFeed) -> None:
        acc_id = feed.account_id
        matching_key = next((k for k in active_pipelines if k.lower() == acc_id.lower()), None)
        if matching_key is not None:
            return
        safe_acc_name = acc_id.lower().replace("-", "_").replace(" ", "_")
        table_name = await ensure_broker_table(db_pool, safe_acc_name)
        broker_tick_queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.TICK_QUEUE_SIZE)
        
        t1 = asyncio.create_task(feed.run(broker_tick_queue, log_queue), name=f"feed:{acc_id}")
        t2 = asyncio.create_task(tick_consumer(db_pool, broker_tick_queue, table_name), name=f"tick_consumer:{acc_id}")
        active_pipelines[acc_id] = [t1, t2]
        active_feeds[acc_id] = feed
        
        log_event("INFO", f"Pipeline created for account {acc_id} (API: {feed.api_provider}) -> {table_name}", "ENGINE", log_queue)
        logger.info("Pipeline created for account %s (API: %s) -> %s", acc_id, feed.api_provider, table_name)

    def stop_account_pipeline(account_id: str) -> None:
        target_keys = [k for k in active_pipelines if k.lower() == account_id.lower()]
        if not target_keys:
            return
        for key in target_keys:
            for t in active_pipelines[key]:
                t.cancel()
            del active_pipelines[key]
            active_feeds.pop(key, None)
            log_event("INFO", f"Pipeline stopped for account: {key}", "ENGINE", log_queue)
            logger.info("Pipeline stopped for account: %s", key)

    if feeds:
        for feed in feeds:
            await start_account_pipeline(feed)
    else:
        log_event("WARNING", "No enabled feeds found at startup. Entering standby mode.", "ENGINE", log_queue)
        logger.warning("No enabled feeds found at startup. Engine running in standby mode listening for events.")

    # ── 5. Setup PostgreSQL NOTIFY Listener ─────────────────────
    notify_conn = await db_pool.acquire()
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    async def _handle_start(account_id: str) -> None:
        try:
            # If already running, reload to pick up latest credentials / tokens
            matching_key = next((k for k in active_pipelines if k.lower() == account_id.lower()), None)
            if matching_key is not None:
                logger.info("Pipeline for %s is already running. Reloading to pick up latest config.", account_id)
                stop_account_pipeline(matching_key)
                await asyncio.sleep(0.5)

            feed = await registry.get_feed_for_account(account_id)
            if feed:
                await start_account_pipeline(feed)
            else:
                logger.warning("Could not start pipeline for %s: feed not available or not ready.", account_id)
        except Exception as exc:
            logger.error("Error starting pipeline for %s: %s", account_id, exc)

    async def _handle_reload(account_id: str) -> None:
        try:
            stop_account_pipeline(account_id)
            await asyncio.sleep(0.5)
            await _handle_start(account_id)
        except Exception as exc:
            logger.error("Error reloading pipeline for %s: %s", account_id, exc)

    def on_engine_control(con, pid, channel, payload):
        import json
        try:
            data = json.loads(payload)
            action = data.get("action")
            account_id = data.get("account_id") or data.get("broker")
            
            logger.info("Received engine_control event: action=%s, account=%s", action, account_id)
            if not account_id:
                if action in ("reload", "reconcile"):
                    loop.create_task(reconcile_once())
                return

            if action == "stop":
                stop_account_pipeline(account_id)
            elif action == "start":
                loop.create_task(_handle_start(account_id))
            elif action in ("restart", "reload"):
                loop.create_task(_handle_reload(account_id))
            elif action == "shutdown":
                shutdown_event.set()
        except Exception as exc:
            logger.error("Error processing notify payload: %s", exc)

    await notify_conn.add_listener('engine_control', on_engine_control)

    # ── 6. Shared log consumer ──────────────────────────────────
    log_consumer_task = asyncio.create_task(
        log_consumer(db_pool, log_queue),
        name="log_consumer",
    )

    # ── 7. Periodic DB logs/ticks cleanup ───────────────────────
    cleanup_task = asyncio.create_task(
        clear_old_logs_and_ticks_loop(db_pool),
        name="db_logs_cleanup",
    )

    # ── 8. Automatic Feed Reconciler Loop ───────────────────────
    async def reconcile_once() -> None:
        enabled_feeds = await registry.get_enabled_feeds(broker_filter)
        enabled_map = {f.account_id.lower(): f for f in enabled_feeds}

        # 1. Start new or reload updated feeds
        for f_id_lower, feed in enabled_map.items():
            matching_key = next((k for k in active_pipelines if k.lower() == f_id_lower), None)
            if matching_key is None:
                logger.info("Reconciler discovered active feed for %s. Starting pipeline.", feed.account_id)
                await start_account_pipeline(feed)
            else:
                existing_feed = active_feeds.get(matching_key)
                # Only restart on *credential* changes — token changes are handled
                # in-band by BaseFeed._token_refresh_loop + on_tokens_changed, so
                # comparing instrument_tokens here would cause spurious 30-second
                # pipeline restarts every time the strategy loop updates subscriptions.
                if existing_feed and (
                    existing_feed.access_token != feed.access_token
                    or existing_feed.api_key != feed.api_key
                    or existing_feed.api_secret != feed.api_secret
                ):
                    logger.info("Reconciler detected credential update for %s. Reloading pipeline.", feed.account_id)
                    stop_account_pipeline(matching_key)
                    await asyncio.sleep(0.5)
                    await start_account_pipeline(feed)

        # 2. Stop feeds that are no longer enabled
        for k in list(active_pipelines.keys()):
            if k.lower() not in enabled_map:
                logger.info("Reconciler stopping pipeline for disabled account: %s", k)
                stop_account_pipeline(k)

        # 3. Heal any tasks that died unexpectedly
        for k, tasks in list(active_pipelines.items()):
            dead_tasks = [t for t in tasks if t.done() and not t.cancelled()]
            if dead_tasks:
                for dt in dead_tasks:
                    exc = dt.exception() if not dt.cancelled() else None
                    if exc:
                        logger.error("Pipeline task for %s failed with exception: %s", k, exc)
                logger.info("Reconciler restarting dead pipeline for %s...", k)
                stop_account_pipeline(k)
                await asyncio.sleep(0.5)
                fresh_feed = await registry.get_feed_for_account(k)
                if fresh_feed:
                    await start_account_pipeline(fresh_feed)

    async def reconcile_feeds_loop() -> None:
        while True:
            try:
                await asyncio.sleep(30.0)
                await reconcile_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in reconcile_feeds_loop: %s", e)

    reconcile_task = asyncio.create_task(
        reconcile_feeds_loop(),
        name="reconcile_feeds",
    )

    if feeds:
        log_event("INFO", f"Engine running with {len(feeds)} feed(s).", "ENGINE", log_queue)
        logger.info(
            "Engine running — accounts: %s",
            ", ".join(f.account_id for f in feeds),
        )
    else:
        logger.info("Engine running in standby — waiting for active broker accounts.")

    # ── 9. Graceful shutdown ────────────────────────────────────
    def _signal_handler() -> None:
        logger.info("Shutdown signal received.")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for SIGTERM
            pass

    # Wait until shutdown is requested
    try:
        await shutdown_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        log_event("INFO", "Shutting down engine…", "ENGINE", log_queue)
        
        # Cleanup listener
        try:
            await notify_conn.remove_listener('engine_control', on_engine_control)
        except Exception:
            pass
        await db_pool.release(notify_conn)

        all_tasks = [t for tasks in active_pipelines.values() for t in tasks]
        all_tasks.append(log_consumer_task)
        all_tasks.append(cleanup_task)
        all_tasks.append(reconcile_task)
        logger.info("Cancelling %d tasks…", len(all_tasks))
        for t in all_tasks:
            t.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

        # Give log consumer a moment to flush
        await asyncio.sleep(0.5)

        await db_pool.close()
        logger.info("Engine stopped cleanly.")


def start_engine(broker_filter: list[str] | None = None) -> None:
    """
    Synchronous wrapper — sets up Django, then runs the async engine.
    Called by the management command.
    """
    import os

    import django

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
    django.setup()

    try:
        asyncio.run(run_engine(broker_filter))
    except KeyboardInterrupt:
        logger.info("Engine stopped via KeyboardInterrupt.")
