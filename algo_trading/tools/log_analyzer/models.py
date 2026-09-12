# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/models.py
──────────────────────────────────────────
Data models, enums, and container structures for the Log Anomaly Analyzer.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

    def __str__(self) -> str:
        return self.value


@dataclass
class LogEntry:
    """Normalized internal representation of a log row from kalai_algolog or system_logs."""
    id: int
    timestamp: datetime
    algo_name: str
    tag: str
    level: str
    message: str
    account_id: Optional[str] = None
    account_name: Optional[str] = None
    broker_code: Optional[str] = None
    is_crypto: bool = False
    enable_trade: bool = False
    source_table: str = "kalai_algolog"


@dataclass
class AnomalyRecord:
    """Structured record of a single detected anomaly."""
    log_id: int
    timestamp: datetime
    rule_id: str
    rule_name: str
    category: str
    severity: str
    account_id: str
    broker_code: str
    is_crypto: bool
    algo_name: str
    tag: str
    level: str
    reason: str
    message: str
    source_table: str = "kalai_algolog"

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat() if hasattr(self.timestamp, "isoformat") else str(self.timestamp)
        return d


@dataclass
class RecurringPattern:
    """Aggregated pattern of high-frequency or repeating errors."""
    pattern_signature: str
    category: str
    severity: str
    occurrences: int
    first_seen: datetime
    last_seen: datetime
    frequency_per_min: float
    sample_accounts: List[str]
    sample_message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pattern_signature": self.pattern_signature,
            "category": self.category,
            "severity": self.severity,
            "occurrences": self.occurrences,
            "first_seen": self.first_seen.isoformat() if hasattr(self.first_seen, "isoformat") else str(self.first_seen),
            "last_seen": self.last_seen.isoformat() if hasattr(self.last_seen, "isoformat") else str(self.last_seen),
            "frequency_per_min": round(self.frequency_per_min, 2),
            "sample_accounts": ", ".join(self.sample_accounts),
            "sample_message": self.sample_message[:200],
        }


@dataclass
class AnalysisSummary:
    """High-level metrics summary returned by the analyzer."""
    total_logs_analyzed: int = 0
    total_anomalies: int = 0
    severity_counts: Dict[str, int] = field(default_factory=lambda: {
        Severity.CRITICAL.value: 0,
        Severity.HIGH.value: 0,
        Severity.MEDIUM.value: 0,
        Severity.LOW.value: 0,
    })
    category_counts: Dict[str, int] = field(default_factory=dict)
    top_offending_accounts: List[Tuple[str, int]] = field(default_factory=list)
    top_offending_algos: List[Tuple[str, int]] = field(default_factory=list)
    timeframe_start: Optional[datetime] = None
    timeframe_end: Optional[datetime] = None
    analyzed_at: datetime = field(default_factory=datetime.now)
    debug_mode: bool = True
