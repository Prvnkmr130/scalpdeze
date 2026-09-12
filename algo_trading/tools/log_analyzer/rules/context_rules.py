# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/rules/context_rules.py
──────────────────────────────────────────────────────
Rules detecting Thread Context Leaks, missing ISO timestamps, and formatting violations.
"""

from __future__ import annotations

import re
from typing import Optional

from algo_trading.tools.log_analyzer.models import AnomalyRecord, LogEntry, Severity
from algo_trading.tools.log_analyzer.registry import register_rule
from algo_trading.tools.log_analyzer.rules.base import AnalysisContext, BaseAnomalyRule


@register_rule
class IsoTimestampProtocolRule(BaseAnomalyRule):
    """
    RULE-05: Missing ISO Timestamp Prefix.
    Verifies adherence to Project Guideline Rule 2:
    'All log entries generated in algorithms and system components must attach
    an ISO timestamp ([{datetime.now().isoformat()}]) to each log entry upon creation.'
    """
    rule_id = "RULE_05"
    name = "Missing ISO Timestamp Prefix"
    category = "Context & Formatting Violations"
    severity = Severity.LOW
    description = (
        "Detects algorithm logs that omit the mandatory standardized ISO-8601 timestamp "
        "prefix '[YYYY-MM-DDTHH:MM:SS]' upon creation."
    )

    ISO_TIMESTAMP_PREFIX_REGEX = re.compile(r"^\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def evaluate(self, entry: LogEntry, context: AnalysisContext) -> Optional[AnomalyRecord]:
        # Only evaluate algorithm logs (system_logs use native DB timestamp columns)
        if entry.source_table != "kalai_algolog":
            return None

        # Exclude SYSTEM unassigned rows
        if (entry.algo_name or "").upper() in {"SYSTEM", "APP"}:
            return None

        msg = (entry.message or "").strip()
        if not msg:
            return None

        if not self.ISO_TIMESTAMP_PREFIX_REGEX.match(msg):
            return self.create_anomaly(
                entry,
                reason=(
                    "Log message violates Guideline Rule 2: Missing standardized ISO-8601 "
                    "timestamp prefix '[{datetime.now().isoformat()}]' at start of message."
                ),
                override_severity=Severity.LOW,
            )

        return None
