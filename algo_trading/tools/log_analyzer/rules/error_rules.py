# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/rules/error_rules.py
────────────────────────────────────────────────────
Rules detecting Unhandled Exceptions, Tracebacks, and Ingestion Safety warnings.
"""

from __future__ import annotations

import re
from typing import Optional

from algo_trading.tools.log_analyzer.models import AnomalyRecord, LogEntry, Severity
from algo_trading.tools.log_analyzer.registry import register_rule
from algo_trading.tools.log_analyzer.rules.base import AnalysisContext, BaseAnomalyRule


@register_rule
class UnhandledCrashTraceRule(BaseAnomalyRule):
    """
    RULE-07: Unhandled Exception & Traceback.
    Detects Python crash traces, unhandled exceptions, and fatal engine failures.
    """
    rule_id = "RULE_07"
    name = "Unhandled Exception or Traceback"
    category = "Engine Errors & Crash Traces"
    severity = Severity.HIGH
    description = (
        "Detects unhandled Python stack traces, unexpected runtime exceptions, "
        "and syntax/type errors in execution logs."
    )

    TRACEBACK_REGEX = re.compile(
        r"(?:Traceback \(most recent call last\)|Exception:|RuntimeError:|ZeroDivisionError:|KeyError:|TypeError:|NameError:)",
        re.IGNORECASE
    )

    def evaluate(self, entry: LogEntry, context: AnalysisContext) -> Optional[AnomalyRecord]:
        msg = entry.message or ""
        level = (entry.level or "").upper()

        if self.TRACEBACK_REGEX.search(msg) or level in {"ERROR", "CRITICAL"}:
            # If it's already caught by auth rule (401/403/whitelisting), let auth rule handle it to avoid duplicate categorization
            if "ip_not_whitelisted" in msg or "HTTP 401" in msg or "HTTP 403" in msg:
                return None

            match = self.TRACEBACK_REGEX.search(msg)
            err_type = match.group(0) if match else "Error record"
            return self.create_anomaly(
                entry,
                reason=f"Detected runtime failure ({err_type}). Level: {level}.",
                override_severity=Severity.HIGH if level != "CRITICAL" else Severity.CRITICAL,
            )

        return None


@register_rule
class IngestionSafetyRule(BaseAnomalyRule):
    """
    RULE-08: Ingestion & Dtype Safety Violations.
    Detects warnings related to numeric dtype parsing, openpyxl string comparison warnings,
    or zero-capital symbols that should have been filtered early.
    """
    rule_id = "RULE_08"
    name = "Ingestion & Dtype Safety Warning"
    category = "Configuration & Ingestion Warnings"
    severity = Severity.LOW
    description = (
        "Detects numeric dtype casting warnings, openpyxl string comparisons, "
        "or zero-capital symbols loaded into the engine."
    )

    INGESTION_PATTERNS = [
        ("could not cast", "Failed numeric type cast during spreadsheet config ingestion"),
        ("zero capital", "Zero capital underlying loaded into trading calculation space"),
        ("openpyxl", "Openpyxl raw string comparison or load issue"),
        ("dtype safety", "Dtype safety violation detected during execution pass"),
    ]

    def evaluate(self, entry: LogEntry, context: AnalysisContext) -> Optional[AnomalyRecord]:
        msg = (entry.message or "").lower()
        for kw, reason in self.INGESTION_PATTERNS:
            if kw in msg:
                return self.create_anomaly(entry, reason=reason, override_severity=Severity.LOW)

        return None
