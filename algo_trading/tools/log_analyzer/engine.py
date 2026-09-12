# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/engine.py
──────────────────────────────────────────
Core execution engine for Log Anomaly Analysis and Multi-Sheet Excel generation.
Strictly enforced to execute only in DEBUG mode.
"""

from __future__ import annotations

import collections
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import polars as pl
from django.conf import settings
from django.db import connection
from django.utils import timezone

from algo_trading.algos.polars_excel import write_polars_sheets_to_excel
from algo_trading.tools.log_analyzer.models import (
    AnalysisSummary,
    AnomalyRecord,
    LogEntry,
    RecurringPattern,
    Severity,
)
from algo_trading.tools.log_analyzer.registry import rule_registry
from algo_trading.tools.log_analyzer.rules.base import AnalysisContext

logger = logging.getLogger("algo_trading.tools.log_analyzer.engine")


class LogAnomalyAnalyzer:
    """
    High-performance, modular log analyzer that inspects local database logs
    for operational anomalies, account-algo mismatches, auth loops, and context leaks.
    """

    def __init__(
        self,
        hours: Optional[float] = 24.0,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        max_logs: Optional[int] = 50000,
        enabled_rule_ids: Optional[List[str]] = None,
        active_rules: Optional[List[str]] = None,
        force_debug: bool = False,
    ) -> None:
        # 1. Strict Debug Mode Guardrail
        is_debug = getattr(settings, "DEBUG", False)
        if not is_debug and not force_debug:
            raise PermissionError(
                "Security & Performance Guardrail: LogAnomalyAnalyzer is restricted to DEBUG mode "
                "(settings.DEBUG=True). It is disabled in production to protect server database performance."
            )

        self.hours = hours
        self.start_time = start_time
        self.end_time = end_time or timezone.now()
        if self.start_time is None and self.hours is not None:
            self.start_time = self.end_time - timedelta(hours=self.hours)

        self.max_logs = max_logs
        self.enabled_rule_ids = enabled_rule_ids or active_rules
        self.force_debug = force_debug

        # Results containers
        self.raw_entries: List[LogEntry] = []
        self.anomalies: List[AnomalyRecord] = []
        self.recurring_patterns: List[RecurringPattern] = []
        self.summary: AnalysisSummary = AnalysisSummary()
        self.context: AnalysisContext = AnalysisContext()

    def _build_context(self) -> AnalysisContext:
        """Loads live broker account topology from database to power fast O(1) checks."""
        try:
            from kalai.models import Broker
            brokers = list(Broker.objects.select_related("broker_name", "api_provider").all())
            ctx = AnalysisContext()

            for b in brokers:
                acc_id = (b.account_id or b.name or "").strip()
                if not acc_id:
                    continue

                ctx.known_accounts.add(acc_id)
                b_code = (b.broker_name.code if b.broker_name else "").lower()
                is_crypto = getattr(b, "is_crypto", False)

                if is_crypto:
                    ctx.crypto_accounts.add(acc_id)
                else:
                    ctx.indian_accounts.add(acc_id)

                if b.enable_trade:
                    ctx.trade_enabled_accounts.add(acc_id)

                ctx.account_metadata[acc_id] = {
                    "name": b.name,
                    "broker_code": b_code,
                    "is_crypto": is_crypto,
                    "enable_trade": b.enable_trade,
                }

            return ctx
        except Exception as e:
            logger.error("Failed to build AnalysisContext from database: %s", e)
            return AnalysisContext()

    def fetch_logs(self) -> List[LogEntry]:
        """Fetch log entries from kalai_algolog and system_logs within the configured timeframe."""
        from kalai.models import AlgoLog

        entries: List[LogEntry] = []
        qs = AlgoLog.objects.select_related("account", "account__broker_name").all()

        if self.start_time:
            qs = qs.filter(timestamp__gte=self.start_time)
        if self.end_time:
            qs = qs.filter(timestamp__lte=self.end_time)

        # Bounded query using values() to avoid heavy ORM model instantiation and OOM
        HARD_MAX_ANALYSIS_LOGS = 25000
        limit = min(self.max_logs, HARD_MAX_ANALYSIS_LOGS) if (self.max_logs is not None and self.max_logs > 0) else HARD_MAX_ANALYSIS_LOGS

        algo_logs_values = qs.order_by("-timestamp").values(
            "id", "timestamp", "algo_name", "tag", "level", "message",
            "account__account_id", "account__name", "account__broker_name__code",
            "account__api_provider__code", "account__enable_trade"
        )[:limit]

        for al in algo_logs_values:
            b_code = (al.get("account__broker_name__code") or "").lower()
            p_code = (al.get("account__api_provider__code") or "").lower()
            is_crypto = any(c in b_code or c in p_code for c in ("delta", "coinswitch", "coindcx", "crypto"))
            entries.append(
                LogEntry(
                    id=al["id"],
                    timestamp=al["timestamp"],
                    algo_name=al["algo_name"],
                    tag=al["tag"],
                    level=al["level"],
                    message=al["message"] or "",
                    account_id=al.get("account__account_id"),
                    account_name=al.get("account__name"),
                    broker_code=al.get("account__broker_name__code"),
                    is_crypto=is_crypto,
                    enable_trade=bool(al.get("account__enable_trade", False)),
                    source_table="kalai_algolog",
                )
            )

        # Optionally ingest matching system_logs if table exists
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'system_logs');"
                )
                if cursor.fetchone()[0]:
                    start_val = self.start_time or (timezone.now() - timedelta(hours=24))
                    sys_limit = min(limit, 10000)
                    sys_query = """
                        SELECT ctid::text, timestamp, level, context, message
                        FROM system_logs
                        WHERE timestamp >= %s AND timestamp <= %s
                        ORDER BY timestamp DESC
                        LIMIT %s;
                    """
                    params = [start_val, self.end_time, sys_limit]
                    cursor.execute(sys_query, params)
                    for row in cursor.fetchall():
                        ctid_str, ts, lvl, ctx, msg = row
                        # Hash numeric id from ctid for consistent model
                        simulated_id = abs(hash(ctid_str)) % (10 ** 8)
                        entries.append(
                            LogEntry(
                                id=simulated_id,
                                timestamp=ts,
                                algo_name=ctx or "SYSTEM",
                                tag="SYSTEM",
                                level=lvl or "INFO",
                                message=msg or "",
                                account_id=None,
                                source_table="system_logs",
                            )
                        )
        except Exception as ex:
            logger.debug("Could not ingest system_logs table: %s", ex)

        # Sort chronologically for timeline and recurrence evaluation
        entries.sort(key=lambda x: x.timestamp)
        self.raw_entries = entries
        return entries

    def run_analysis(self) -> AnalysisSummary:
        """Runs active anomaly rules against all fetched log entries."""
        self.context = self._build_context()
        if not self.raw_entries:
            self.raw_entries = self.fetch_logs()

        # Resolve rules to run
        all_rules = rule_registry.get_active_rules()
        if self.enabled_rule_ids:
            active_rules = [r for r in all_rules if r.rule_id in self.enabled_rule_ids]
        else:
            active_rules = all_rules

        anomalies: List[AnomalyRecord] = []
        severity_counts = {s.value: 0 for s in Severity}
        category_counts: Dict[str, int] = collections.defaultdict(int)
        account_counts: Dict[str, int] = collections.defaultdict(int)
        algo_counts: Dict[str, int] = collections.defaultdict(int)

        for entry in self.raw_entries:
            for rule in active_rules:
                anomaly = rule.evaluate(entry, self.context)
                if anomaly:
                    anomalies.append(anomaly)
                    severity_counts[anomaly.severity] = severity_counts.get(anomaly.severity, 0) + 1
                    category_counts[anomaly.category] += 1
                    if anomaly.account_id and anomaly.account_id != "UNASSIGNED":
                        account_counts[anomaly.account_id] += 1
                    if anomaly.algo_name:
                        algo_counts[anomaly.algo_name] += 1

        self.anomalies = anomalies

        # Evaluate recurring error patterns
        self.recurring_patterns = self._compute_recurring_patterns(anomalies)

        # Build summary
        top_accounts = sorted(account_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        top_algos = sorted(algo_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        self.summary = AnalysisSummary(
            total_logs_analyzed=len(self.raw_entries),
            total_anomalies=len(anomalies),
            severity_counts=severity_counts,
            category_counts=dict(category_counts),
            top_offending_accounts=top_accounts,
            top_offending_algos=top_algos,
            timeframe_start=self.start_time,
            timeframe_end=self.end_time,
            analyzed_at=datetime.now(),
            debug_mode=bool(getattr(settings, "DEBUG", False)),
        )

        return self.summary

    def _compute_recurring_patterns(self, anomalies: List[AnomalyRecord]) -> List[RecurringPattern]:
        """Groups repeating errors by normalized signature to detect burst loops."""
        pattern_buckets: Dict[str, List[AnomalyRecord]] = collections.defaultdict(list)

        for a in anomalies:
            # Normalize message (strip timestamps, numbers, and IDs) to detect root error signature
            sig = re.sub(r"\d+", "*", a.reason[:120])
            pattern_buckets[sig].append(a)

        patterns: List[RecurringPattern] = []
        for sig, records in pattern_buckets.items():
            if len(records) < 2:
                continue

            first_dt = min(r.timestamp for r in records)
            last_dt = max(r.timestamp for r in records)
            diff_mins = max(1.0, (last_dt - first_dt).total_seconds() / 60.0)
            rate = len(records) / diff_mins

            sample_accs = list({r.account_id for r in records if r.account_id != "UNASSIGNED"})[:5]
            top_record = records[0]

            patterns.append(
                RecurringPattern(
                    pattern_signature=sig,
                    category=top_record.category,
                    severity=top_record.severity,
                    occurrences=len(records),
                    first_seen=first_dt,
                    last_seen=last_dt,
                    frequency_per_min=rate,
                    sample_accounts=sample_accs or ["UNASSIGNED"],
                    sample_message=top_record.message,
                )
            )

        patterns.sort(key=lambda x: x.occurrences, reverse=True)
        return patterns

    def generate_excel_report(self, output_path: str) -> str:
        """
        Generates a comprehensive, formatted multi-sheet Excel report (.xlsx)
        using pure Polars & openpyxl with zero Pandas dependencies.
        """
        if not self.anomalies and not self.summary.total_logs_analyzed:
            self.run_analysis()

        sheets: Dict[str, Any] = {}

        # ─── Sheet 1: Executive Summary ──────────────────────────────
        summary_rows = [
            {"Metric": "Report Generated At", "Value": self.summary.analyzed_at.strftime("%Y-%m-%d %H:%M:%S")},
            {"Metric": "Execution Environment", "Value": "DEBUG Mode (Authorized)"},
            {"Metric": "Analysis Window Start", "Value": self.start_time.strftime("%Y-%m-%d %H:%M:%S") if self.start_time else "All Time"},
            {"Metric": "Analysis Window End", "Value": self.end_time.strftime("%Y-%m-%d %H:%M:%S") if self.end_time else "Now"},
            {"Metric": "Total Log Rows Inspected", "Value": str(self.summary.total_logs_analyzed)},
            {"Metric": "Total Operational Anomalies", "Value": str(self.summary.total_anomalies)},
            {"Metric": "── Severity Breakdown ──", "Value": "────────"},
            {"Metric": "CRITICAL Severity", "Value": str(self.summary.severity_counts.get(Severity.CRITICAL.value, 0))},
            {"Metric": "HIGH Severity", "Value": str(self.summary.severity_counts.get(Severity.HIGH.value, 0))},
            {"Metric": "MEDIUM Severity", "Value": str(self.summary.severity_counts.get(Severity.MEDIUM.value, 0))},
            {"Metric": "LOW Severity", "Value": str(self.summary.severity_counts.get(Severity.LOW.value, 0))},
            {"Metric": "── Category Breakdown ──", "Value": "────────"},
        ]
        for cat, cnt in sorted(self.summary.category_counts.items(), key=lambda x: x[1], reverse=True):
            pct = (cnt / max(1, self.summary.total_anomalies)) * 100.0
            summary_rows.append({"Metric": cat, "Value": f"{cnt} ({pct:.1f}%)"})

        sheets["Executive_Summary"] = pl.DataFrame(summary_rows)

        # Helper to convert anomaly records into Polars DataFrame
        def _to_df(records: List[AnomalyRecord]) -> pl.DataFrame:
            if not records:
                return pl.DataFrame(schema=[
                    "Timestamp", "Log_ID", "Severity", "Category", "Reason",
                    "Account_ID", "Broker", "Is_Crypto", "Algo_Name", "Tag", "Level", "Raw_Message"
                ])
            rows = []
            for r in records:
                rows.append({
                    "Timestamp": r.timestamp.strftime("%Y-%m-%d %H:%M:%S") if hasattr(r.timestamp, "strftime") else str(r.timestamp),
                    "Log_ID": r.log_id,
                    "Severity": r.severity,
                    "Category": r.category,
                    "Reason": r.reason,
                    "Account_ID": r.account_id,
                    "Broker": r.broker_code,
                    "Is_Crypto": "YES" if r.is_crypto else "NO",
                    "Algo_Name": r.algo_name,
                    "Tag": r.tag,
                    "Level": r.level,
                    "Raw_Message": r.message[:500],
                })
            return pl.DataFrame(rows)

        # ─── Sheet 2: Account & Algo Mismatches ───────────────────────
        mismatch_records = [
            a for a in self.anomalies if a.category == "Account & Algo Mismatch"
        ]
        sheets["Account_Algo_Mismatches"] = _to_df(mismatch_records)

        # ─── Sheet 3: Auth & API Errors ──────────────────────────────
        auth_records = [
            a for a in self.anomalies if a.category == "API & Authentication Errors"
        ]
        sheets["Auth_And_API_Errors"] = _to_df(auth_records)

        # ─── Sheet 4: System Misattributions ─────────────────────────
        system_records = [
            a for a in self.anomalies if a.category == "System & Framework Misattributions"
        ]
        sheets["System_Misattributions"] = _to_df(system_records)

        # ─── Sheet 5: Context & Thread Leaks ─────────────────────────
        context_records = [
            a for a in self.anomalies if a.category in {"Context & Formatting Violations", "Account Trading State Violations"}
        ]
        sheets["Context_Thread_Leaks"] = _to_df(context_records)

        # ─── Sheet 6: Recurring Error Loops ──────────────────────────
        pattern_rows = [p.to_dict() for p in self.recurring_patterns]
        if pattern_rows:
            sheets["Recurring_Error_Loops"] = pl.DataFrame(pattern_rows)
        else:
            sheets["Recurring_Error_Loops"] = pl.DataFrame(schema=[
                "pattern_signature", "category", "severity", "occurrences",
                "first_seen", "last_seen", "frequency_per_min", "sample_accounts", "sample_message"
            ])

        # ─── Sheet 7: Master Audit Log ───────────────────────────────
        sheets["All_Anomalies_Audit"] = _to_df(self.anomalies)

        final_file = write_polars_sheets_to_excel(sheets, output_path)
        logger.info("Successfully generated multi-sheet Log Anomaly Report at: %s", final_file)
        return final_file

    def get_dashboard_payload(self) -> Dict[str, Any]:
        """Formats analysis results for Django Admin template rendering."""
        if not self.anomalies and not self.summary.total_logs_analyzed:
            self.run_analysis()

        return {
            "summary": {
                "total_logs": self.summary.total_logs_analyzed,
                "total_anomalies": self.summary.total_anomalies,
                "critical_count": self.summary.severity_counts.get(Severity.CRITICAL.value, 0),
                "high_count": self.summary.severity_counts.get(Severity.HIGH.value, 0),
                "medium_count": self.summary.severity_counts.get(Severity.MEDIUM.value, 0),
                "low_count": self.summary.severity_counts.get(Severity.LOW.value, 0),
                "category_counts": self.summary.category_counts,
                "top_accounts": self.summary.top_offending_accounts,
                "top_algos": self.summary.top_offending_algos,
                "timeframe_start": self.start_time.strftime("%Y-%m-%d %H:%M:%S") if self.start_time else "",
                "timeframe_end": self.end_time.strftime("%Y-%m-%d %H:%M:%S") if self.end_time else "",
                "analyzed_at": self.summary.analyzed_at.strftime("%Y-%m-%d %H:%M:%S"),
            },
            "recent_anomalies": [a.to_dict() for a in self.anomalies[:100]],
            "recurring_patterns": [p.to_dict() for p in self.recurring_patterns[:20]],
            "rules": [
                {
                    "rule_id": r.rule_id,
                    "name": r.name,
                    "category": r.category,
                    "severity": r.severity.value,
                    "description": r.description,
                    "enabled": r.enabled,
                }
                for r in rule_registry.get_all_rules()
            ],
            "debug_mode": bool(getattr(settings, "DEBUG", False)),
        }
