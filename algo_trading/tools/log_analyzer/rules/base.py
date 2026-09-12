# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/rules/base.py
─────────────────────────────────────────────
Abstract base class and analysis context for modular log anomaly rules.
Allows seamless addition, modification, and removal of rules in the future.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from algo_trading.tools.log_analyzer.models import AnomalyRecord, LogEntry, Severity


@dataclass
class AnalysisContext:
    """Pre-loaded platform state passed to rules for fast O(1) context checks."""
    known_accounts: Set[str] = field(default_factory=set)
    crypto_accounts: Set[str] = field(default_factory=set)
    indian_accounts: Set[str] = field(default_factory=set)
    trade_enabled_accounts: Set[str] = field(default_factory=set)
    active_production_algos: Set[str] = field(default_factory=lambda: {
        "indian_opt_trde_polars",
        "crypto_opt_trde_polars",
    })
    system_algo_names: Set[str] = field(default_factory=lambda: {
        "SYSTEM", "APP", "BROKER_API", "DJANGO_Q"
    })
    account_metadata: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class BaseAnomalyRule(ABC):
    """
    Abstract Base Class for Log Anomaly Detection Rules.
    Subclasses implement `evaluate` to check an individual log entry.
    """
    rule_id: str = "RULE_BASE"
    name: str = "Base Rule"
    category: str = "General"
    severity: Severity = Severity.MEDIUM
    description: str = "Base anomaly detection rule"
    enabled: bool = True

    @abstractmethod
    def evaluate(self, entry: LogEntry, context: AnalysisContext) -> Optional[AnomalyRecord]:
        """
        Evaluates a log entry against the rule criteria.
        Returns an AnomalyRecord if violated, or None if the log entry is valid.
        """
        raise NotImplementedError

    def create_anomaly(self, entry: LogEntry, reason: str, override_severity: Optional[Severity] = None) -> AnomalyRecord:
        """Helper to create a normalized AnomalyRecord for this rule."""
        return AnomalyRecord(
            log_id=entry.id,
            timestamp=entry.timestamp,
            rule_id=self.rule_id,
            rule_name=self.name,
            category=self.category,
            severity=(override_severity or self.severity).value,
            account_id=entry.account_id or "UNASSIGNED",
            broker_code=entry.broker_code or "NONE",
            is_crypto=entry.is_crypto,
            algo_name=entry.algo_name,
            tag=entry.tag,
            level=entry.level,
            reason=reason,
            message=entry.message,
            source_table=entry.source_table,
        )
