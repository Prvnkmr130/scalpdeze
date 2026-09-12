# -*- coding: utf-8 -*-
"""
tests/test_chunked_cleanup.py
──────────────────────────────
Unit tests for chunked log and processed tick deletion with CPU throttling.
Verifies delete_in_chunks, clear_old_logs, clear_old_ticks, and clear_logs management command.
"""

from unittest.mock import patch
from datetime import timedelta
import pytest
from django.utils import timezone
from django.core.management import call_command
from django.db import connection

from kalai.models import AlgoLog, ProcessedTickStore
from kalai.tasks import (
    delete_in_chunks,
    clear_old_logs,
    clear_old_ticks,
    clear_old_django_q_tasks,
    clear_old_stream_partitions,
    clear_all_old_data,
    format_bytes,
    DeletionResult,
    get_table_size,
)



@pytest.mark.django_db
def test_delete_in_chunks_processed_ticks():
    """Verify that records older than cutoff are deleted in batches with sleep pauses."""
    now = timezone.now()
    old_time = now - timedelta(days=10)
    recent_time = now - timedelta(hours=1)

    # Create 15 old records and 5 recent records
    old_objs = [
        ProcessedTickStore(timestamp=old_time, data={"price": 100 + i})
        for i in range(15)
    ]
    recent_objs = [
        ProcessedTickStore(timestamp=recent_time, data={"price": 200 + i})
        for i in range(5)
    ]
    ProcessedTickStore.objects.bulk_create(old_objs + recent_objs)

    cutoff = now - timedelta(days=2)
    sleep_calls = []

    with patch("time.sleep", side_effect=lambda s: sleep_calls.append(s)):
        # Chunk size 5 means 15 records will be deleted in 3 chunks of 5 + 1 final check
        deleted = delete_in_chunks(
            table_name="kalai_processedtickstore",
            cutoff=cutoff,
            id_col="id",
            chunk_size=5,
            sleep_interval=0.01,
        )

    assert deleted == 15
    # Should have slept at least 3 times between chunks
    assert len(sleep_calls) >= 3
    assert sleep_calls[0] == 0.01

    # Remaining records should be only the 5 recent records
    remaining = ProcessedTickStore.objects.count()
    assert remaining == 5


@pytest.mark.django_db
def test_delete_in_chunks_algo_logs():
    """Verify chunked deletion on kalai_algolog table."""
    now = timezone.now()
    old_time = now - timedelta(days=10)
    recent_time = now - timedelta(hours=1)

    old_logs = [
        AlgoLog(timestamp=old_time, message=f"Old log {i}", tag="GENERAL")
        for i in range(12)
    ]
    recent_logs = [
        AlgoLog(timestamp=recent_time, message=f"Recent log {i}", tag="GENERAL")
        for i in range(3)
    ]
    AlgoLog.objects.bulk_create(old_logs + recent_logs)

    cutoff = now - timedelta(days=2)
    deleted = delete_in_chunks(
        table_name="kalai_algolog",
        cutoff=cutoff,
        id_col="id",
        chunk_size=5,
        sleep_interval=0.0,
    )

    assert deleted == 12
    assert AlgoLog.objects.filter(timestamp__lt=cutoff).count() == 0
    assert AlgoLog.objects.filter(message__startswith="Recent log").count() == 3


@pytest.mark.django_db
def test_delete_in_chunks_system_logs():
    """Verify chunked deletion on raw system_logs table using ctid."""
    now = timezone.now()
    old_time = now - timedelta(days=10)
    recent_time = now - timedelta(hours=1)

    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_logs (
                timestamp   TIMESTAMP WITH TIME ZONE NOT NULL,
                level       VARCHAR(20)              NOT NULL,
                context     VARCHAR(50)              NOT NULL,
                message     TEXT                     NOT NULL,
                log_date    DATE                     NOT NULL
            );
        """)
        # Insert 8 old and 2 recent rows
        for i in range(8):
            cursor.execute(
                "INSERT INTO system_logs (timestamp, level, context, message, log_date) VALUES (%s, %s, %s, %s, %s)",
                [old_time, "INFO", "SYS", f"old {i}", old_time.date()]
            )
        for i in range(2):
            cursor.execute(
                "INSERT INTO system_logs (timestamp, level, context, message, log_date) VALUES (%s, %s, %s, %s, %s)",
                [recent_time, "INFO", "SYS", f"recent {i}", recent_time.date()]
            )

    cutoff = now - timedelta(days=2)
    deleted = delete_in_chunks(
        table_name="system_logs",
        cutoff=cutoff,
        id_col="ctid",
        chunk_size=3,
        sleep_interval=0.0,
    )

    assert deleted == 8
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM system_logs WHERE timestamp < %s", [cutoff])
        assert cursor.fetchone()[0] == 0
        cursor.execute("SELECT count(*) FROM system_logs WHERE timestamp >= %s", [cutoff])
        assert cursor.fetchone()[0] == 2


@pytest.mark.django_db
def test_clear_old_logs_task():
    """Verify clear_old_logs wrapper deletes both system_logs and kalai_algolog."""
    now = timezone.now()
    old_time = now - timedelta(days=5)

    # Insert old AlgoLog
    AlgoLog.objects.create(timestamp=old_time, message="old algo log")

    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_logs (
                timestamp   TIMESTAMP WITH TIME ZONE NOT NULL,
                level       VARCHAR(20)              NOT NULL,
                context     VARCHAR(50)              NOT NULL,
                message     TEXT                     NOT NULL,
                log_date    DATE                     NOT NULL
            );
        """)
        cursor.execute(
            "INSERT INTO system_logs (timestamp, level, context, message, log_date) VALUES (%s, %s, %s, %s, %s)",
            [old_time, "INFO", "SYS", "old sys log", old_time.date()]
        )

    res = clear_old_logs(days=2, chunk_size=10, sleep_interval=0.0)
    assert "system logs" in res
    assert "algorithm logs" in res
    assert AlgoLog.objects.filter(timestamp=old_time).count() == 0


@pytest.mark.django_db
def test_clear_old_ticks_task():
    """Verify clear_old_ticks wrapper deletes old processed ticks."""
    now = timezone.now()
    old_time = now - timedelta(days=5)
    ProcessedTickStore.objects.create(timestamp=old_time, data={"t": 1})

    res = clear_old_ticks(days=2, chunk_size=10, sleep_interval=0.0)
    assert "processed ticks" in res
    assert ProcessedTickStore.objects.filter(timestamp=old_time).count() == 0


@pytest.mark.django_db
def test_clear_logs_management_command(capsys):
    """Verify clear_logs management command runs with chunking and sleep arguments."""
    now = timezone.now()
    old_time = now - timedelta(days=10)

    ProcessedTickStore.objects.create(timestamp=old_time, data={"tick": 1})
    AlgoLog.objects.create(timestamp=old_time, message="old log")

    call_command("clear_logs", days=5, tick_days=5, chunk_size=10, sleep=0.0)
    captured = capsys.readouterr()

    assert "Clearing logs older than 5 days" in captured.out
    assert "Clearing ticks older than 5 days" in captured.out
    assert "Successfully completed cleanup" in captured.out
    assert "kalai_algolog" in captured.out
    assert "kalai_processedtickstore" in captured.out


def test_format_bytes():
    """Verify human readable byte formatting."""
    assert format_bytes(0) == "0 B"
    assert format_bytes(500) == "500 B"
    assert format_bytes(1024) == "1.00 KB"
    assert format_bytes(1048576) == "1.00 MB"
    assert format_bytes(1073741824) == "1.00 GB"
    assert format_bytes(None) == "0 B"
    assert format_bytes(-10) == "0 B"


def test_deletion_result_compatibility():
    """Verify DeletionResult is a drop-in int replacement with size metadata."""
    res = DeletionResult(
        count=15,
        initial_size=2097152,
        final_size=1048576,
        freed_bytes=1048576,
        table_name="kalai_algolog",
    )
    # Must pass int assertions
    assert isinstance(res, int)
    assert res == 15
    assert res + 5 == 20
    assert int(res) == 15

    # Rich attributes
    assert res.table_name == "kalai_algolog"
    assert res.initial_size_bytes == 2097152
    assert res.final_size_bytes == 1048576
    assert res.freed_bytes == 1048576
    assert res.initial_size_pretty == "2.00 MB"
    assert res.final_size_pretty == "1.00 MB"
    assert res.freed_pretty == "1.00 MB"


@pytest.mark.django_db
def test_delete_in_chunks_security():
    """Verify SQL injection protection rejects illegal table, id, or timestamp column names."""
    now = timezone.now()
    cutoff = now - timedelta(days=2)

    with pytest.raises(ValueError, match="Invalid table name"):
        delete_in_chunks("kalai_algolog; DROP TABLE kalai_algolog;", cutoff)

    with pytest.raises(ValueError, match="Invalid id_col"):
        delete_in_chunks("kalai_algolog", cutoff, id_col="id; DROP TABLE users;")

    with pytest.raises(ValueError, match="Invalid timestamp_col"):
        delete_in_chunks("kalai_algolog", cutoff, timestamp_col="created_at; DROP TABLE users;")


@pytest.mark.django_db
def test_delete_in_chunks_min_size_threshold():
    """Verify delete_in_chunks skips table when size is below threshold."""
    now = timezone.now()
    old_time = now - timedelta(days=10)

    # Create 5 old records
    old_logs = [AlgoLog(timestamp=old_time, message=f"log {i}") for i in range(5)]
    AlgoLog.objects.bulk_create(old_logs)

    cutoff = now - timedelta(days=2)

    # Set min_size_mb to 500 MB (much larger than our 5 records)
    deleted = delete_in_chunks(
        table_name="kalai_algolog",
        cutoff=cutoff,
        id_col="id",
        min_size_mb=500.0,
    )

    # Deletion should be skipped, returning 0 deleted rows
    assert deleted == 0
    assert AlgoLog.objects.count() == 5


@pytest.mark.django_db
def test_clear_logs_command_with_options(capsys):
    """Verify clear_logs management command with --min-size-mb and --all options."""
    now = timezone.now()
    old_time = now - timedelta(days=10)

    AlgoLog.objects.create(timestamp=old_time, message="old")
    call_command("clear_logs", days=5, tick_days=5, min_size_mb=100.0, check_size=True, all=True)
    captured = capsys.readouterr()

    assert "Size threshold: skipping tables smaller than 100.0 MB" in captured.out
    assert "TOTAL" in captured.out

