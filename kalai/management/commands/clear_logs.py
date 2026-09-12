from datetime import timedelta
from django.core.management.base import BaseCommand
from django.utils import timezone
from kalai.tasks import (
    delete_in_chunks,
    clear_old_django_q_tasks,
    clear_old_stream_partitions,
    format_bytes,
)


class Command(BaseCommand):
    help = "Clear system logs, algo logs, processed ticks, and partitions in throttled chunks with size checking."

    def add_arguments(self, parser):
        parser.add_argument(
            "--days",
            type=int,
            default=7,
            help="Delete logs older than this number of days (default: 7).",
        )
        parser.add_argument(
            "--hours",
            type=int,
            default=None,
            help="Delete logs older than this number of hours (e.g. --hours 4). Overrides --days if specified.",
        )
        parser.add_argument(
            "--tick-days",
            type=int,
            default=2,
            help="Delete processed ticks older than this number of days (default: 2).",
        )
        parser.add_argument(
            "--django-q-days",
            type=int,
            default=7,
            help="Delete completed Django-Q tasks older than this number of days (default: 7).",
        )
        parser.add_argument(
            "--chunk-size",
            type=int,
            default=5000,
            help="Number of records to delete per chunk (default: 5000).",
        )
        parser.add_argument(
            "--sleep",
            type=float,
            default=0.05,
            help="Pause in seconds between chunks to throttle CPU utilization (default: 0.05s).",
        )
        parser.add_argument(
            "--min-size-mb",
            type=float,
            default=None,
            help="Skip tables whose total size is below this threshold in MB (e.g. --min-size-mb 10).",
        )
        parser.add_argument(
            "--check-size",
            action="store_true",
            default=True,
            help="Inspect and display table sizes before and after deletion (default: True).",
        )
        parser.add_argument(
            "--vacuum",
            action="store_true",
            default=False,
            help="Run gentle non-blocking PostgreSQL VACUUM after chunked deletion to reclaim disk space.",
        )
        parser.add_argument(
            "--clean-partitions",
            action="store_true",
            default=False,
            help="Drop obsolete weekly stream partition tables older than partition-keep-weeks.",
        )
        parser.add_argument(
            "--partition-keep-weeks",
            type=int,
            default=2,
            help="Number of recent weeks to retain when --clean-partitions is used (default: 2).",
        )
        parser.add_argument(
            "--all",
            action="store_true",
            default=False,
            help="Clean all old data including logs, ticks, Django-Q tasks, and obsolete partitions.",
        )

    def handle(self, *args, **options):
        days = options["days"]
        hours = options.get("hours")
        tick_days = options["tick_days"]
        django_q_days = options["django_q_days"]
        chunk_size = options["chunk_size"]
        sleep_interval = options["sleep"]
        min_size_mb = options["min_size_mb"]
        check_size = options["check_size"]
        vacuum = options["vacuum"]
        clean_partitions = options["clean_partitions"] or options["all"]
        clean_django_q = options["all"] or options["django_q_days"] is not None
        keep_weeks = options["partition_keep_weeks"]

        if hours is not None and hours > 0:
            cutoff_logs = timezone.now() - timedelta(hours=hours)
            log_cutoff_str = f"{hours} hours"
        else:
            cutoff_logs = timezone.now() - timedelta(days=days)
            log_cutoff_str = f"{days} days"
        cutoff_ticks = timezone.now() - timedelta(days=tick_days)

        self.stdout.write(
            f"Clearing logs older than {log_cutoff_str} (cutoff: {cutoff_logs}, chunk_size: {chunk_size}, sleep: {sleep_interval}s)"
        )
        self.stdout.write(
            f"Clearing ticks older than {tick_days} days (cutoff: {cutoff_ticks}, chunk_size: {chunk_size}, sleep: {sleep_interval}s)"
        )
        if min_size_mb:
            self.stdout.write(f"Size threshold: skipping tables smaller than {min_size_mb} MB")

        results = []

        # 1. Clear system_logs in chunks
        res_sys = delete_in_chunks(
            table_name="system_logs",
            cutoff=cutoff_logs,
            id_col="ctid",
            chunk_size=chunk_size,
            sleep_interval=sleep_interval,
            min_size_mb=min_size_mb,
            check_size=check_size,
            vacuum=vacuum,
        )
        results.append(res_sys)

        # 2. Clear kalai_algolog in chunks
        res_algo = delete_in_chunks(
            table_name="kalai_algolog",
            cutoff=cutoff_logs,
            id_col="id",
            chunk_size=chunk_size,
            sleep_interval=sleep_interval,
            min_size_mb=min_size_mb,
            check_size=check_size,
            vacuum=vacuum,
        )
        results.append(res_algo)

        # 3. Clear kalai_processedtickstore in chunks
        res_ticks = delete_in_chunks(
            table_name="kalai_processedtickstore",
            cutoff=cutoff_ticks,
            id_col="id",
            chunk_size=chunk_size,
            sleep_interval=sleep_interval,
            min_size_mb=min_size_mb,
            check_size=check_size,
            vacuum=vacuum,
        )
        results.append(res_ticks)

        # 4. Clear django_q_task if requested or under --all
        if clean_django_q:
            res_q = clear_old_django_q_tasks(
                days=django_q_days,
                chunk_size=chunk_size,
                sleep_interval=sleep_interval,
                min_size_mb=min_size_mb,
                vacuum=vacuum,
            )
            results.append(res_q)

        # Print Tabular Summary
        self.stdout.write("\n" + "=" * 80)
        self.stdout.write(f"{'Table':<26} | {'Initial Size':<12} | {'Rows Deleted':<12} | {'Final Size':<12} | {'Space Freed':<12}")
        self.stdout.write("-" * 80)
        total_freed_bytes = 0
        total_rows_deleted = 0
        for r in results:
            rows = int(r)
            total_rows_deleted += rows
            total_freed_bytes += getattr(r, "freed_bytes", 0)
            self.stdout.write(
                f"{getattr(r, 'table_name', 'unknown'):<26} | "
                f"{getattr(r, 'initial_size_pretty', 'N/A'):<12} | "
                f"{rows:<12,d} | "
                f"{getattr(r, 'final_size_pretty', 'N/A'):<12} | "
                f"{getattr(r, 'freed_pretty', 'N/A'):<12}"
            )
        self.stdout.write("-" * 80)
        self.stdout.write(
            f"{'TOTAL':<26} | {'':<12} | {total_rows_deleted:<12,d} | {'':<12} | {format_bytes(total_freed_bytes):<12}"
        )
        self.stdout.write("=" * 80 + "\n")

        # 5. Clean Partition Tables if requested
        if clean_partitions:
            self.stdout.write(f"Checking for stream partitions older than {keep_weeks} weeks...")
            partition_results = clear_old_stream_partitions(keep_weeks=keep_weeks)
            if partition_results:
                dropped_bytes = sum(p["size_bytes"] for p in partition_results if p.get("dropped"))
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Dropped {len(partition_results)} obsolete stream partitions, reclaiming {format_bytes(dropped_bytes)}."
                    )
                )
                for p in partition_results:
                    self.stdout.write(f"  - {p['table_name']}: {p['size_pretty']}")
            else:
                self.stdout.write("No obsolete stream partitions found to clean.")

        self.stdout.write(
            self.style.SUCCESS(
                f"Successfully completed cleanup: {total_rows_deleted:,d} rows deleted, "
                f"{format_bytes(total_freed_bytes)} reclaimed."
            )
        )


