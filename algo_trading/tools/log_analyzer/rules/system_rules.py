# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/rules/system_rules.py
──────────────────────────────────────────────────────
Rules detecting Framework, System, and Background task log misattributions.
"""

from __future__ import annotations

import re
from typing import Optional

from algo_trading.tools.log_analyzer.models import AnomalyRecord, LogEntry, Severity
from algo_trading.tools.log_analyzer.registry import register_rule
from algo_trading.tools.log_analyzer.rules.base import AnalysisContext, BaseAnomalyRule


@register_rule
class FrameworkLogMisattributionRule(BaseAnomalyRule):
    """
    RULE-03: Framework / Registry Log Misattribution.
    Guarantees that platform-level discovery, background tasks, database maintenance,
    and supervisor engine lifecycle logs are attributed to "SYSTEM" or "APP",
    never misattributed to specific trading algorithms.
    """
    rule_id = "RULE_03"
    name = "Framework Log Misattribution"
    category = "System & Framework Misattributions"
    severity = Severity.HIGH
    description = (
        "Detects framework discovery, maintenance, or lifecycle logs mistakenly "
        "attributed to a live trading algorithm."
    )

    FRAMEWORK_SIGNATURES = [
        "load_all_algos",
        "qcluster",
        "django_q",
        "evaluate_websocket_schedules",
        "clear_old_logs",
        "clear_old_ticks",
        "clear_old_logs_and_ticks_loop",
        "Starting Synchronous Sequential Algorithm Engine",
        "Sequential algorithm engine cleanly stopped",
        "supervisord",
        "ConnectionPool",
        "wait_for_postgres",
        "delete_in_chunks",
    ]

    def evaluate(self, entry: LogEntry, context: AnalysisContext) -> Optional[AnomalyRecord]:
        clean_algo = (entry.algo_name or "").strip().lower()
        if clean_algo in {"system", "app", "django_q", ""}:
            return None

        msg = entry.message or ""
        for sig in self.FRAMEWORK_SIGNATURES:
            if sig.lower() in msg.lower():
                return self.create_anomaly(
                    entry,
                    reason=(
                        f"Framework/system maintenance event ('{sig}') was recorded under algorithm "
                        f"identity '{entry.algo_name}' instead of 'SYSTEM'. Violates Framework Log Isolation."
                    ),
                )

        return None
