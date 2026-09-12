# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/__init__.py
───────────────────────────────────────────
Log Anomaly Analysis Package.
Provides pluggable anomaly detection rules, Polars multi-sheet Excel generation,
and strict DEBUG mode verification.
"""

from __future__ import annotations

# 1. Ensure rules are loaded and registered
import algo_trading.tools.log_analyzer.rules  # noqa: F401

from algo_trading.tools.log_analyzer.engine import LogAnomalyAnalyzer
from algo_trading.tools.log_analyzer.models import (
    AnalysisSummary,
    AnomalyRecord,
    LogEntry,
    RecurringPattern,
    Severity,
)
from algo_trading.tools.log_analyzer.registry import (
    RuleRegistry,
    register_rule,
    rule_registry,
)
from algo_trading.tools.log_analyzer.rules.base import (
    AnalysisContext,
    BaseAnomalyRule,
)

__all__ = [
    "LogAnomalyAnalyzer",
    "rule_registry",
    "RuleRegistry",
    "register_rule",
    "BaseAnomalyRule",
    "AnalysisContext",
    "AnomalyRecord",
    "LogEntry",
    "RecurringPattern",
    "Severity",
    "AnalysisSummary",
]
