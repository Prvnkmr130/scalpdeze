# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/rules/account_rules.py
───────────────────────────────────────────────────────
Rules detecting Inactive or Orphaned Account Trading executions.
"""

from __future__ import annotations

from typing import Optional

from algo_trading.tools.log_analyzer.models import AnomalyRecord, LogEntry, Severity
from algo_trading.tools.log_analyzer.registry import register_rule
from algo_trading.tools.log_analyzer.rules.base import AnalysisContext, BaseAnomalyRule


@register_rule
class InactiveAccountTradingRule(BaseAnomalyRule):
    """
    RULE-06: Inactive or Orphaned Account Execution.
    Detects trade execution or order placement attempts for broker accounts
    where enable_trade is False or the account is missing from the database.
    """
    rule_id = "RULE_06"
    name = "Inactive Account Execution"
    category = "Account Trading State Violations"
    severity = Severity.HIGH
    description = (
        "Detects trade or order signals executed for an account that is disabled "
        "(enable_trade=False) or orphaned in the database."
    )

    TRADE_TAGS = {"TRADE", "ORDER", "EXECUTION", "BUY", "SELL"}

    def evaluate(self, entry: LogEntry, context: AnalysisContext) -> Optional[AnomalyRecord]:
        if not entry.account_id or entry.account_id == "UNASSIGNED":
            return None

        clean_acc = entry.account_id.strip()
        is_known = clean_acc.lower() in {a.lower() for a in context.known_accounts}

        # 1. Orphaned account
        if not is_known:
            return self.create_anomaly(
                entry,
                reason=f"Account '{entry.account_id}' was logged but does not exist in the Broker database table.",
                override_severity=Severity.MEDIUM,
            )

        # 2. Inactive account placing orders / trades
        clean_tag = (entry.tag or "").upper()
        msg_upper = (entry.message or "").upper()
        is_trade_event = clean_tag in self.TRADE_TAGS or any(k in msg_upper for k in ["PLACED ORDER", "BUY ORDER", "SELL ORDER", "ORDER EXECUTED"])

        is_trade_enabled = clean_acc.lower() in {a.lower() for a in context.trade_enabled_accounts}
        if is_trade_event and not is_trade_enabled:
            return self.create_anomaly(
                entry,
                reason=(
                    f"Trade event detected for account '{entry.account_id}', but enable_trade is False "
                    f"in database configuration. Automated execution should be inhibited."
                ),
                override_severity=Severity.HIGH,
            )

        return None
