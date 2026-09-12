# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/rules/__init__.py
─────────────────────────────────────────────────
Exposes base classes and ensures all default anomaly rules are imported and registered.
"""

from __future__ import annotations

from algo_trading.tools.log_analyzer.rules.base import AnalysisContext, BaseAnomalyRule
from algo_trading.tools.log_analyzer.rules.domain_rules import (
    AssetClassDomainMismatchRule,
    EmbeddedAccountMismatchRule,
)
from algo_trading.tools.log_analyzer.rules.system_rules import FrameworkLogMisattributionRule
from algo_trading.tools.log_analyzer.rules.auth_rules import AuthenticationApiErrorRule
from algo_trading.tools.log_analyzer.rules.context_rules import IsoTimestampProtocolRule
from algo_trading.tools.log_analyzer.rules.account_rules import InactiveAccountTradingRule
from algo_trading.tools.log_analyzer.rules.error_rules import (
    UnhandledCrashTraceRule,
    IngestionSafetyRule,
)

__all__ = [
    "BaseAnomalyRule",
    "AnalysisContext",
    "AssetClassDomainMismatchRule",
    "EmbeddedAccountMismatchRule",
    "FrameworkLogMisattributionRule",
    "AuthenticationApiErrorRule",
    "IsoTimestampProtocolRule",
    "InactiveAccountTradingRule",
    "UnhandledCrashTraceRule",
    "IngestionSafetyRule",
]
