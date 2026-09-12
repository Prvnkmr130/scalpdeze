import time
import logging
import re
from datetime import timedelta, datetime
from django.db import connection
from django.utils import timezone

logger = logging.getLogger(__name__)

SAFE_IDENTIFIER_REGEX = re.compile(r"^[a-zA-Z0-9_]+$")
STREAM_PARTITION_REGEX = re.compile(r"^([a-zA-Z0-9_]+)_stream_kv_y(\d{4})_w(\d{2})$")


def format_bytes(bytes_count: int | float | None) -> str:
    """Format bytes count into human readable format (B, KB, MB, GB, TB)."""
    if bytes_count is None or bytes_count < 0:
        return "0 B"
    b = float(bytes_count)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if b < 1024.0:
            return f"{b:.2f} {unit}" if unit != "B" else f"{int(b)} {unit}"
        b /= 1024.0
    return f"{b:.2f} PB"



class DeletionResult(int):
    """
    Subclass of int representing the count of deleted records,
    enriched with table size metrics for disk space monitoring.
    Fully backwards compatible with any code or test expecting an integer.
    """
    def __new__(
        cls,
        count: int,
        initial_size: int = 0,
        final_size: int = 0,
        freed_bytes: int = 0,
        table_name: str = "",
        initial_pretty: str = "",
        final_pretty: str = "",
        freed_pretty: str = "",
    ):
        instance = super().__new__(cls, count)
        instance.count = count
        instance.table_name = table_name
        instance.initial_size_bytes = initial_size
        instance.final_size_bytes = final_size
        instance.freed_bytes = freed_bytes
        instance.initial_size_pretty = initial_pretty or format_bytes(initial_size)
        instance.final_size_pretty = final_pretty or format_bytes(final_size)
        instance.freed_pretty = freed_pretty or format_bytes(freed_bytes)
        return instance

    def __repr__(self):
        return (
            f"<DeletionResult: {self.count} rows deleted from '{self.table_name}', "
            f"size: {self.initial_size_pretty} -> {self.final_size_pretty} (freed: {self.freed_pretty})>"
        )


def get_table_size(table_name: str, cursor=None) -> dict:
    """
    Inspects table size using fast O(1) PostgreSQL catalog metadata.
    Does not scan table blocks and does not cause CPU throttling.
    Provides safe fallback on non-PostgreSQL databases (e.g. SQLite for testing).

    :param table_name: Database table name.
    :param cursor: Optional open database cursor.
    :return: Dict containing total_bytes, table_bytes, index_bytes, and formatted string.
    """
    if not SAFE_IDENTIFIER_REGEX.match(table_name):
        raise ValueError(f"Invalid table name: '{table_name}'. Only alphanumeric characters and underscores are allowed.")

    def _query(cur):
        if connection.vendor == "postgresql":
            try:
                cur.execute(
                    """
                    SELECT
                        pg_total_relation_size(quote_ident(%s)) AS total_bytes,
                        pg_relation_size(quote_ident(%s)) AS table_bytes,
                        pg_indexes_size(quote_ident(%s)) AS index_bytes
                    """,
                    [table_name, table_name, table_name],
                )
                row = cur.fetchone()
                if row:
                    total_b = int(row[0] or 0)
                    table_b = int(row[1] or 0)
                    idx_b = int(row[2] or 0)
                    return {
                        "total_bytes": total_b,
                        "table_bytes": table_b,
                        "index_bytes": idx_b,
                        "total_pretty": format_bytes(total_b),
                    }
            except Exception as e:
                logger.debug("Failed to query PostgreSQL table size for %s: %s", table_name, e)
        elif connection.vendor == "sqlite":
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cur.fetchone()[0]
                est_bytes = row_count * 256
                return {
                    "total_bytes": est_bytes,
                    "table_bytes": est_bytes,
                    "index_bytes": 0,
                    "total_pretty": format_bytes(est_bytes),
                }
            except Exception:
                pass

        return {
            "total_bytes": 0,
            "table_bytes": 0,
            "index_bytes": 0,
            "total_pretty": "0 B",
        }

    if cursor is not None:
        return _query(cursor)
    else:
        with connection.cursor() as cur:
            return _query(cur)


def delete_in_chunks(
    table_name: str,
    cutoff,
    id_col: str = "id",
    timestamp_col: str = "timestamp",
    chunk_size: int = 5000,
    sleep_interval: float = 0.05,
    min_size_bytes: int | None = None,
    min_size_mb: float | None = None,
    check_size: bool = True,
    vacuum: bool = False,
) -> DeletionResult:
    """
    Deletes records older than cutoff from table_name in chunks with a sleep pause.
    Throttles CPU utilization, minimizes lock hold times, avoids massive WAL transactions,
    and checks table sizes before and after deletion.

    :param table_name: Database table name to delete from.
    :param cutoff: Datetime cutoff threshold.
    :param id_col: Primary key or unique identifier column (e.g. 'id' or 'ctid').
    :param timestamp_col: Column containing timestamp/date (default: 'timestamp').
    :param chunk_size: Number of records to delete per chunk (default: 5000).
    :param sleep_interval: Seconds to pause between chunks to throttle CPU (default: 0.05s).
    :param min_size_bytes: Minimum table size in bytes to trigger deletion. If smaller, deletion is skipped.
    :param min_size_mb: Minimum table size in MB to trigger deletion.
    :param check_size: Whether to inspect and log table size before and after deletion (default: True).
    :param vacuum: If True, execute non-blocking VACUUM on PostgreSQL after chunked delete.
    :return: DeletionResult (int subclass) with deleted count and size metrics.
    """
    if not SAFE_IDENTIFIER_REGEX.match(table_name):
        raise ValueError(f"Invalid table name: '{table_name}'. Only alphanumeric characters and underscores are allowed.")
    if not SAFE_IDENTIFIER_REGEX.match(id_col):
        raise ValueError(f"Invalid id_col: '{id_col}'. Only alphanumeric characters and underscores are allowed.")
    if not SAFE_IDENTIFIER_REGEX.match(timestamp_col):
        raise ValueError(f"Invalid timestamp_col: '{timestamp_col}'. Only alphanumeric characters and underscores are allowed.")

    if min_size_mb is not None:
        min_size_bytes = int(min_size_mb * 1024 * 1024)

    total_deleted = 0
    query = f"""
        DELETE FROM {table_name}
        WHERE {id_col} IN (
            SELECT {id_col} FROM {table_name}
            WHERE {timestamp_col} < %s
            LIMIT %s
        )
    """

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);",
            [table_name],
        )
        if not cursor.fetchone()[0]:
            logger.debug("Table '%s' does not exist; skipping chunked delete.", table_name)
            return DeletionResult(0, 0, 0, 0, table_name)

        initial_size_info = get_table_size(table_name, cursor=cursor) if check_size else {"total_bytes": 0, "total_pretty": "0 B"}
        initial_bytes = initial_size_info["total_bytes"]

        if min_size_bytes is not None and initial_bytes > 0 and initial_bytes < min_size_bytes:
            logger.info(
                "Table '%s' size (%s) is below threshold (%s); skipping chunked deletion.",
                table_name,
                initial_size_info["total_pretty"],
                format_bytes(min_size_bytes),
            )
            return DeletionResult(
                0,
                initial_size=initial_bytes,
                final_size=initial_bytes,
                freed_bytes=0,
                table_name=table_name,
                initial_pretty=initial_size_info["total_pretty"],
                final_pretty=initial_size_info["total_pretty"],
                freed_pretty="0 B",
            )

        logger.info(
            "Starting chunked deletion on table '%s' (size: %s, cutoff: %s, chunk_size: %d, sleep: %ss)...",
            table_name,
            initial_size_info["total_pretty"],
            cutoff,
            chunk_size,
            sleep_interval,
        )

        chunk_count = 0
        while True:
            cursor.execute(query, [cutoff, chunk_size])
            deleted = cursor.rowcount
            total_deleted += deleted
            chunk_count += 1

            if deleted > 0 and chunk_count % 10 == 0:
                logger.debug(
                    "Table '%s' deletion progress: %d rows deleted so far (%d chunks)...",
                    table_name, total_deleted, chunk_count
                )

            if deleted < chunk_size:
                break

            if sleep_interval > 0:
                time.sleep(sleep_interval)

    # Optional PostgreSQL VACUUM to immediately reclaim disk space outside transactions
    if vacuum and connection.vendor == "postgresql" and total_deleted > 0:
        try:
            prev_autocommit = connection.connection.autocommit
            connection.connection.autocommit = True
            try:
                with connection.connection.cursor() as vac_cur:
                    logger.info("Running gentle non-blocking VACUUM on '%s'...", table_name)
                    vac_cur.execute(f"VACUUM {table_name};")
            finally:
                connection.connection.autocommit = prev_autocommit
        except Exception as e:
            logger.warning("VACUUM on '%s' failed or was skipped: %s", table_name, e)

    final_size_info = get_table_size(table_name) if check_size else {"total_bytes": 0, "total_pretty": "0 B"}
    final_bytes = final_size_info["total_bytes"]
    freed_bytes = max(0, initial_bytes - final_bytes)

    logger.info(
        "Completed chunked deletion on table '%s': %d rows deleted. Size: %s -> %s (freed: %s).",
        table_name,
        total_deleted,
        initial_size_info["total_pretty"],
        final_size_info["total_pretty"],
        format_bytes(freed_bytes),
    )

    return DeletionResult(
        total_deleted,
        initial_size=initial_bytes,
        final_size=final_bytes,
        freed_bytes=freed_bytes,
        table_name=table_name,
        initial_pretty=initial_size_info["total_pretty"],
        final_pretty=final_size_info["total_pretty"],
        freed_pretty=format_bytes(freed_bytes),
    )


def clear_old_logs(
    days=2,
    hours=None,
    chunk_size=5000,
    sleep_interval=0.05,
    min_size_mb=None,
    vacuum=False,
    include_django_q=True,
    django_q_days=7,
):
    """
    Background task to clear system logs, algorithm logs, and completed Django-Q tasks
    older than specified days or hours in chunks with CPU throttling and size monitoring.
    """
    if hours is not None and hours > 0:
        cutoff_logs = timezone.now() - timedelta(hours=hours)
    else:
        cutoff_logs = timezone.now() - timedelta(days=days)

    try:
        # 1. Clear raw system_logs in chunks using PostgreSQL physical tuple identifier ctid
        deleted_system_logs = delete_in_chunks(
            table_name="system_logs",
            cutoff=cutoff_logs,
            id_col="ctid",
            chunk_size=chunk_size,
            sleep_interval=sleep_interval,
            min_size_mb=min_size_mb,
            vacuum=vacuum,
        )

        # 2. Clear dedicated AlgoLog table in chunks using primary key id
        deleted_algo_logs = delete_in_chunks(
            table_name="kalai_algolog",
            cutoff=cutoff_logs,
            id_col="id",
            chunk_size=chunk_size,
            sleep_interval=sleep_interval,
            min_size_mb=min_size_mb,
            vacuum=vacuum,
        )

        deleted_q_tasks = 0
        if include_django_q:
            deleted_q_tasks = clear_old_django_q_tasks(
                days=django_q_days,
                chunk_size=chunk_size,
                sleep_interval=sleep_interval,
                min_size_mb=min_size_mb,
                vacuum=vacuum,
            )

        logger.info(
            "Successfully deleted %s old system logs (freed %s), %s old algorithm logs (freed %s), "
            "and %s Django-Q tasks (freed %s) in chunks (chunk_size=%s, sleep=%ss).",
            deleted_system_logs, deleted_system_logs.freed_pretty,
            deleted_algo_logs, deleted_algo_logs.freed_pretty,
            deleted_q_tasks, getattr(deleted_q_tasks, "freed_pretty", "0 B"),
            chunk_size, sleep_interval
        )
        return (
            f"Deleted {deleted_system_logs} system logs ({deleted_system_logs.freed_pretty} freed), "
            f"{deleted_algo_logs} algorithm logs ({deleted_algo_logs.freed_pretty} freed)"
            + (f", {deleted_q_tasks} Django-Q tasks ({getattr(deleted_q_tasks, 'freed_pretty', '0 B')} freed)" if include_django_q else "")
        )
    except Exception as e:
        logger.error("Error clearing old logs: %s", e)
        raise


def clear_old_ticks(days=2, chunk_size=5000, sleep_interval=0.05, min_size_mb=None, vacuum=False):
    """Background task to clear processed ticks older than specified days in chunks with CPU throttling and size monitoring."""
    cutoff_ticks = timezone.now() - timedelta(days=days)

    try:
        # Clear kalai_processedtickstore in chunks using primary key id
        deleted_ticks = delete_in_chunks(
            table_name="kalai_processedtickstore",
            cutoff=cutoff_ticks,
            id_col="id",
            chunk_size=chunk_size,
            sleep_interval=sleep_interval,
            min_size_mb=min_size_mb,
            vacuum=vacuum,
        )

        logger.info(
            "Successfully deleted %s old processed ticks in chunks (freed %s, chunk_size=%s, sleep=%ss).",
            deleted_ticks, deleted_ticks.freed_pretty, chunk_size, sleep_interval
        )
        return f"Deleted {deleted_ticks} old processed ticks ({deleted_ticks.freed_pretty} freed)"
    except Exception as e:
        logger.error("Error clearing old processed ticks: %s", e)
        raise


def clear_old_django_q_tasks(days=7, chunk_size=5000, sleep_interval=0.05, min_size_mb=None, vacuum=False):
    """Clear completed/failed Django-Q tasks older than specified days in chunks."""
    cutoff = timezone.now() - timedelta(days=days)
    return delete_in_chunks(
        table_name="django_q_task",
        cutoff=cutoff,
        id_col="id",
        timestamp_col="stopped",
        chunk_size=chunk_size,
        sleep_interval=sleep_interval,
        min_size_mb=min_size_mb,
        vacuum=vacuum,
    )


def clear_old_stream_partitions(keep_weeks=2, dry_run=False) -> list[dict]:
    """
    Identifies and safely drops obsolete weekly stream partition tables older than keep_weeks.
    In PostgreSQL, dropping a partition immediately reclaims 100% of disk space without CPU overhead.

    :param keep_weeks: Number of recent weeks to retain (default: 2, current week + previous week).
    :param dry_run: If True, only inspects and returns candidates without dropping them.
    :return: List of dicts with dropped table names and space reclaimed.
    """
    if connection.vendor != "postgresql":
        return []

    now = datetime.now()
    current_year, current_week, _ = now.isocalendar()

    # Calculate cutoff ISO year and week
    cutoff_date = now - timedelta(weeks=keep_weeks)
    cutoff_year, cutoff_week, _ = cutoff_date.isocalendar()

    results = []
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
              AND table_name LIKE '%_stream_kv_y%_w%'
            ORDER BY table_name;
        """)
        tables = [r[0] for r in cursor.fetchall()]

        for tbl in tables:
            m = STREAM_PARTITION_REGEX.match(tbl)
            if not m:
                continue
            tbl_year = int(m.group(2))
            tbl_week = int(m.group(3))

            is_older = (tbl_year < cutoff_year) or (tbl_year == cutoff_year and tbl_week < cutoff_week)
            if is_older:
                size_info = get_table_size(tbl, cursor=cursor)
                if not dry_run:
                    try:
                        logger.info("Dropping obsolete stream partition table '%s' (size: %s)...", tbl, size_info["total_pretty"])
                        cursor.execute(f"DROP TABLE IF EXISTS {tbl};")
                        results.append({
                            "table_name": tbl,
                            "year": tbl_year,
                            "week": tbl_week,
                            "size_bytes": size_info["total_bytes"],
                            "size_pretty": size_info["total_pretty"],
                            "dropped": True,
                        })
                    except Exception as e:
                        logger.error("Failed to drop partition '%s': %s", tbl, e)
                else:
                    results.append({
                        "table_name": tbl,
                        "year": tbl_year,
                        "week": tbl_week,
                        "size_bytes": size_info["total_bytes"],
                        "size_pretty": size_info["total_pretty"],
                        "dropped": False,
                    })

    return results


def clear_all_old_data(
    log_days=2,
    tick_days=2,
    django_q_days=7,
    keep_weeks=2,
    chunk_size=5000,
    sleep_interval=0.05,
    min_size_mb=None,
    vacuum=False,
    clean_partitions=False,
) -> dict:
    """
    Comprehensive cleanup routine for logs, ticks, Django-Q tasks, and stream partitions.
    Tracks table sizes and disk space reclaimed across all components.
    """
    summary = {
        "system_logs": delete_in_chunks(
            "system_logs",
            cutoff=timezone.now() - timedelta(days=log_days),
            id_col="ctid",
            chunk_size=chunk_size,
            sleep_interval=sleep_interval,
            min_size_mb=min_size_mb,
            vacuum=vacuum,
        ),
        "kalai_algolog": delete_in_chunks(
            "kalai_algolog",
            cutoff=timezone.now() - timedelta(days=log_days),
            id_col="id",
            chunk_size=chunk_size,
            sleep_interval=sleep_interval,
            min_size_mb=min_size_mb,
            vacuum=vacuum,
        ),
        "kalai_processedtickstore": delete_in_chunks(
            "kalai_processedtickstore",
            cutoff=timezone.now() - timedelta(days=tick_days),
            id_col="id",
            chunk_size=chunk_size,
            sleep_interval=sleep_interval,
            min_size_mb=min_size_mb,
            vacuum=vacuum,
        ),
        "django_q_task": clear_old_django_q_tasks(
            days=django_q_days,
            chunk_size=chunk_size,
            sleep_interval=sleep_interval,
            min_size_mb=min_size_mb,
            vacuum=vacuum,
        ),
    }

    if clean_partitions:
        summary["partitions"] = clear_old_stream_partitions(keep_weeks=keep_weeks)

    return summary


def evaluate_websocket_schedules():
    """
    Background task (runs every minute) to evaluate time-based schedules for broker accounts.
    Starts or stops WebSocket feeds dynamically based on configured market hours and operating days.
    """
    from kalai.models import Broker

    now_dt = timezone.localtime()
    current_time = now_dt.time()
    day_code = now_dt.strftime("%a").upper()[:3]  # MON, TUE, WED, THU, FRI, SAT, SUN

    updated_count = 0

    # Ensure all crypto brokers with trading enabled maintain 24/7 WebSockets active (even if enable_schedule=False)
    crypto_brokers = Broker.objects.filter(enable_trade=True, enable_websocket=False)
    for cb in crypto_brokers:
        if getattr(cb, "is_crypto", False):
            logger.info("Scheduler activating 24/7 WebSocket for crypto account %s", cb.account_id or cb.name)
            cb.enable_websocket = True
            cb.save()
            updated_count += 1

    scheduled_brokers = Broker.objects.filter(enable_schedule=True)

    for broker in scheduled_brokers:
        # Crypto brokers operate 24/7/365: their WebSockets must never be deactivated by market hours
        if getattr(broker, "is_crypto", False):
            if not broker.enable_websocket and broker.enable_trade:
                logger.info("Scheduler activating 24/7 WebSocket for crypto account %s", broker.account_id or broker.name)
                broker.enable_websocket = True
                broker.save()
                updated_count += 1
            continue

        op_days = broker.ws_operating_days or "ALL"
        if op_days == "ALL":
            is_operating_day = True
        elif op_days == "WEEKDAYS":
            is_operating_day = day_code in ["MON", "TUE", "WED", "THU", "FRI"]
        else:
            # Fallback for old comma-separated strings
            op_days_list = [d.strip().upper()[:3] for d in op_days.split(",")]
            is_operating_day = "ALL" in op_days_list or day_code in op_days_list

        start_t = broker.ws_start_time
        stop_t = broker.ws_stop_time

        should_be_active = False
        if is_operating_day and start_t and stop_t:
            if start_t <= stop_t:
                should_be_active = start_t <= current_time <= stop_t
            else:
                should_be_active = current_time >= start_t or current_time <= stop_t
        elif is_operating_day and not start_t and not stop_t:
            should_be_active = True

        if should_be_active and not broker.enable_websocket:
            logger.info("Scheduler activating WebSocket for account %s", broker.account_id or broker.name)
            broker.enable_websocket = True
            broker.save()
            updated_count += 1
        elif not should_be_active and broker.enable_websocket:
            logger.info("Scheduler deactivating WebSocket for account %s", broker.account_id or broker.name)
            broker.enable_websocket = False
            broker.save()
            updated_count += 1

    scheduled_count = len(scheduled_brokers) if isinstance(scheduled_brokers, (list, tuple)) else scheduled_brokers.count()
    return f"Evaluated {scheduled_count} scheduled brokers, updated {updated_count}"


def sync_all_broker_positions():
    """
    Background task: In production, live positions are maintained in the database
    directly by the active algorithm scripts. External REST API calls are skipped in production
    to prevent rate limits and session conflicts, and only executed when DEBUG=True.
    """
    from django.conf import settings
    if not settings.DEBUG:
        logger.debug("Production: positions are updated in-memory by running algorithm scripts.")
        return "Production mode: positions managed by live trading algorithms"
    try:
        from kalai.positions import sync_all_broker_positions as _sync
        results = _sync(force_api=True)
        return f"Debug sync: {len(results)} accounts"
    except Exception as e:
        logger.error("Error syncing broker positions: %s", e)
        raise


def create_daily_pnl_snapshots():
    """
    Background task (runs daily at midnight) to capture and finalize daily P&L snapshots
    for 1+ year historical performance analysis.
    """
    try:
        from kalai.positions import sync_all_broker_positions as _sync
        results = _sync()
        logger.info("Created daily P&L snapshots for %s accounts.", len(results))
        return f"Finalized daily snapshots for {len(results)} accounts"
    except Exception as e:
        logger.error("Error creating daily P&L snapshots: %s", e)
        raise


