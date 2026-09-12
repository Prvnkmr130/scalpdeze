# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/rules/auth_rules.py
───────────────────────────────────────────────────
Rules detecting Broker API Authentication Failures and Recurring Rejections.
"""

from __future__ import annotations

import re
from typing import Optional

from algo_trading.tools.log_analyzer.models import AnomalyRecord, LogEntry, Severity
from algo_trading.tools.log_analyzer.registry import register_rule
from algo_trading.tools.log_analyzer.rules.base import AnalysisContext, BaseAnomalyRule


@register_rule
class AuthenticationApiErrorRule(BaseAnomalyRule):
    """
    RULE-04: Recurring API & Authentication Failures.
    Detects HTTP 401/403, IP whitelist rejections, invalid credentials,
    and expired OAuth tokens that halt live automated execution.
    """
    rule_id = "RULE_04"
    name = "Broker API Authentication Failure"
    category = "API & Authentication Errors"
    severity = Severity.CRITICAL
    description = (
        "Detects broker API authentication failures, IP whitelisting errors, "
        "and credentials expiration events."
    )

    AUTH_ERROR_PATTERNS = [
        (re.compile(r"ip_not_whitelisted_for_api_key|DELTA IP RESTRICTION ERROR", re.IGNORECASE), "Broker API Key rejected: IP address is not whitelisted in broker settings"),
        (re.compile(r"\[HTTP 401\]|HTTP 401|401 Unauthorized", re.IGNORECASE), "HTTP 401 Unauthorized response from broker API"),
        (re.compile(r"\[HTTP 403\]|HTTP 403|403 Forbidden", re.IGNORECASE), "HTTP 403 Forbidden access rejected by broker API"),
        (re.compile(r"expired_signature|token_expired|access token is not from today", re.IGNORECASE), "Expired authentication token or network clock drift"),
        (re.compile(r"credentials.*incomplete|api_key.*missing", re.IGNORECASE), "Incomplete or missing API credentials in database"),
        (re.compile(r"signature_verification_failed|invalid api signature|invalid api key", re.IGNORECASE), "Cryptographic signature verification or invalid API key failure"),
    ]

    def evaluate(self, entry: LogEntry, context: AnalysisContext) -> Optional[AnomalyRecord]:
        msg = entry.message or ""
        level = (entry.level or "").upper()

        for pattern, reason in self.AUTH_ERROR_PATTERNS:
            if pattern.search(msg):
                return self.create_anomaly(
                    entry,
                    reason=f"{reason}. Raw match details present in log trace.",
                    override_severity=Severity.CRITICAL if level in {"ERROR", "CRITICAL"} else Severity.HIGH,
                )

        return None
