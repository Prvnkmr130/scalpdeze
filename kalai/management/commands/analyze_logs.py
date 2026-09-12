# -*- coding: utf-8 -*-
"""
kalai/management/commands/analyze_logs.py
─────────────────────────────────────────
Django management command to run the Log Anomaly Analyzer from the command line.
Strictly enforced to execute only in DEBUG mode.

Usage:
    python manage.py analyze_logs --hours=24 --output=log_anomalies.xlsx
"""

from __future__ import annotations

import os
from datetime import datetime
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from algo_trading.tools.log_analyzer import LogAnomalyAnalyzer, rule_registry


class Command(BaseCommand):
    help = (
        "Inspect local database algorithm & system logs for operational anomalies, "
        "account-algo mismatches, auth loops, and context leaks. Exports a multi-sheet Excel report. "
        "Operates strictly in DEBUG mode."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--hours",
            type=float,
            default=24.0,
            help="Number of historical hours to inspect (default: 24.0). Set to 0 to use --start/--end.",
        )
        parser.add_argument(
            "--start",
            type=str,
            default=None,
            help="Optional start datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS).",
        )
        parser.add_argument(
            "--end",
            type=str,
            default=None,
            help="Optional end datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS).",
        )
        parser.add_argument(
            "--max-logs",
            type=int,
            default=50000,
            help="Maximum logs to ingest from database (default: 50000, set to 0 for all records).",
        )
        parser.add_argument(
            "--output",
            type=str,
            default=None,
            help="Output path for the generated Excel file (.xlsx). Defaults to logs/deltazero_anomalies_<timestamp>.xlsx.",
        )
        parser.add_argument(
            "--rules",
            type=str,
            default=None,
            help="Comma-separated list of rule IDs to execute (e.g. RULE_01,RULE_04).",
        )
        parser.add_argument(
            "--force-debug",
            action="store_true",
            default=False,
            help="Bypass the settings.DEBUG guardrail check.",
        )

    def handle(self, *args, **options):
        # 1. Strict Debug Check
        is_debug = getattr(settings, "DEBUG", False)
        force_debug = options.get("force_debug", False)

        if not is_debug and not force_debug:
            raise CommandError(
                "❌ Execution Rejected: Log Anomaly Analyzer is restricted to DEBUG mode (settings.DEBUG=True). "
                "This safeguards production database resources. Pass --force-debug if you explicitly intend to override."
            )

        if force_debug and not is_debug:
            self.stdout.write(
                self.style.WARNING("⚠️ Warning: Running Log Anomaly Analyzer with --force-debug override in non-debug mode!")
            )

        # Parse datetime bounds
        start_dt = None
        end_dt = None

        if options.get("start"):
            try:
                start_dt = datetime.fromisoformat(options["start"].strip())
            except ValueError:
                raise CommandError("Invalid --start format. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS.")

        if options.get("end"):
            try:
                end_dt = datetime.fromisoformat(options["end"].strip())
            except ValueError:
                raise CommandError("Invalid --end format. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS.")

        hours = options["hours"] if not start_dt else None
        rule_ids = [r.strip() for r in options["rules"].split(",")] if options.get("rules") else None
        raw_max_logs = options.get("max_logs", 50000)
        max_logs_val = None if (raw_max_logs is not None and raw_max_logs <= 0) else raw_max_logs

        self.stdout.write(self.style.MIGRATE_HEADING("\n[LOG ANALYZER] DeltaZero26 - Log Anomaly Analyzer (DEBUG Workstation)"))
        self.stdout.write("=" * 70)

        try:
            analyzer = LogAnomalyAnalyzer(
                hours=hours,
                start_time=start_dt,
                end_time=end_dt,
                max_logs=max_logs_val,
                enabled_rule_ids=rule_ids,
                force_debug=force_debug,
            )

            self.stdout.write(self.style.SUCCESS("[+] Ingesting logs from local database..."))
            entries = analyzer.fetch_logs()
            self.stdout.write(f"    Ingested {len(entries)} log rows across timeframe.\n")

            self.stdout.write(self.style.SUCCESS(f"[+] Running {len(rule_registry.get_active_rules())} active anomaly detection rules..."))
            summary = analyzer.run_analysis()

            # Display Summary Table
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.MIGRATE_LABEL("  EXECUTIVE ANOMALY SUMMARY"))
            self.stdout.write("=" * 70)
            self.stdout.write(f"  Total Logs Inspected:       {summary.total_logs_analyzed}")
            self.stdout.write(f"  Total Anomalies Detected:   {summary.total_anomalies}")
            self.stdout.write(f"    - CRITICAL Severity:      {summary.severity_counts.get('CRITICAL', 0)}")
            self.stdout.write(f"    - HIGH Severity:          {summary.severity_counts.get('HIGH', 0)}")
            self.stdout.write(f"    - MEDIUM Severity:        {summary.severity_counts.get('MEDIUM', 0)}")
            self.stdout.write(f"    - LOW Severity:           {summary.severity_counts.get('LOW', 0)}")
            self.stdout.write("-" * 70)

            self.stdout.write("  Breakdown by Category:")
            for cat, count in sorted(summary.category_counts.items(), key=lambda x: x[1], reverse=True):
                self.stdout.write(f"    * {cat:<35}: {count}")

            if summary.top_offending_accounts:
                self.stdout.write("-" * 70)
                self.stdout.write("  Top Offending Accounts:")
                for acc, count in summary.top_offending_accounts:
                    self.stdout.write(f"    * {acc:<20}: {count} anomalies")

            if analyzer.recurring_patterns:
                self.stdout.write("-" * 70)
                self.stdout.write("  Recurring Error Loops Detected:")
                for pat in analyzer.recurring_patterns[:5]:
                    self.stdout.write(f"    [!] [{pat.occurrences}x] {pat.pattern_signature[:60]} (Rate: {pat.frequency_per_min:.1f}/min)")

            # Generate Excel report
            output_file = options.get("output")
            if not output_file:
                logs_dir = os.path.join(settings.BASE_DIR, "logs")
                os.makedirs(logs_dir, exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = os.path.join(logs_dir, f"deltazero_anomalies_{ts}.xlsx")

            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("[+] Generating Multi-Sheet Excel Report..."))
            final_path = analyzer.generate_excel_report(output_file)
            self.stdout.write(self.style.SUCCESS(f"  Excel Report Saved: {final_path}"))
            self.stdout.write("=" * 70 + "\n")

        except Exception as e:
            raise CommandError(f"Log analysis failed: {e}")
        finally:
            from django.db import connections
            for c in connections.all():
                if hasattr(c, "pool") and c.pool:
                    try:
                        c.pool.close()
                    except Exception:
                        pass
            connections.close_all()
