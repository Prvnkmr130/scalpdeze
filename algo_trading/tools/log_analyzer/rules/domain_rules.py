# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/rules/domain_rules.py
──────────────────────────────────────────────────────
Rules detecting Asset-Class domain cross-talk and Account Foreign Key mismatches.
"""

from __future__ import annotations

import re
from typing import Optional

from algo_trading.tools.log_analyzer.models import AnomalyRecord, LogEntry, Severity
from algo_trading.tools.log_analyzer.registry import register_rule
from algo_trading.tools.log_analyzer.rules.base import AnalysisContext, BaseAnomalyRule


@register_rule
class AssetClassDomainMismatchRule(BaseAnomalyRule):
    """
    RULE-01: Asset-Class / Algorithm Domain Mismatch.
    Ensures cryptocurrency accounts trade exclusively under crypto engines,
    and Indian equity/derivative accounts trade exclusively under Indian engines.
    """
    rule_id = "RULE_01"
    name = "Asset-Class Domain Mismatch"
    category = "Account & Algo Mismatch"
    severity = Severity.CRITICAL
    description = (
        "Detects cryptocurrency accounts attributed to Indian equity algorithms, "
        "or Indian equity/derivatives accounts attributed to crypto algorithms."
    )

    CRYPTO_ALGOS = {"crypto_opt_trde_polars", "crypto_opt", "coinswitch_opt_trde_polars", "delta_opt_trde_polars"}
    INDIAN_ALGOS = {"indian_opt_trde_polars", "indian_opt", "zerodha_opt_trde_polars", "kotak_opt_trde_polars", "zerodha_opt"}

    def evaluate(self, entry: LogEntry, context: AnalysisContext) -> Optional[AnomalyRecord]:
        if not entry.account_id or entry.account_id == "UNASSIGNED":
            return None

        clean_acc = entry.account_id.strip().lower()
        clean_algo = entry.algo_name.strip().lower()

        is_crypto_acc = entry.is_crypto or clean_acc in {a.lower() for a in context.crypto_accounts}
        is_indian_acc = not is_crypto_acc and clean_acc in {a.lower() for a in context.indian_accounts}

        # Case A: Crypto account logged under Indian algorithm
        if is_crypto_acc and any(algo in clean_algo for algo in self.INDIAN_ALGOS):
            return self.create_anomaly(
                entry,
                reason=(
                    f"Cryptocurrency account '{entry.account_id}' (is_crypto=True) is mistakenly logged "
                    f"under Indian Equity algorithm '{entry.algo_name}'."
                ),
            )

        # Case B: Indian account logged under Crypto algorithm
        if is_indian_acc and any(algo in clean_algo for algo in self.CRYPTO_ALGOS):
            return self.create_anomaly(
                entry,
                reason=(
                    f"Indian Equity account '{entry.account_id}' (is_crypto=False) is mistakenly logged "
                    f"under Crypto algorithm '{entry.algo_name}'."
                ),
            )

        return None


@register_rule
class EmbeddedAccountMismatchRule(BaseAnomalyRule):
    """
    RULE-02: Embedded ID vs Foreign-Key Mismatch.
    Detects when a specific account identifier (e.g. [73270496] or Account 'W1NPY')
    is mentioned in the log message string, but the row's account foreign key points
    to a different broker account or is left unassigned (indicating thread context bleed).
    """
    rule_id = "RULE_02"
    name = "Embedded Account Context Mismatch"
    category = "Account & Algo Mismatch"
    severity = Severity.HIGH
    description = (
        "Detects when a log message contains an embedded account identifier that does not "
        "match the row's foreign key account_id."
    )

    ACCOUNT_REGEX = re.compile(
        r"(?:\[([a-zA-Z0-9_-]{4,30})\]|account\s*['\"]([a-zA-Z0-9_-]{4,30})['\"])",
        re.IGNORECASE
    )

    IGNORE_TAGS = {"SYSTEM", "GENERAL", "INFO", "WARNING", "ERROR", "DEBUG", "CRITICAL", "ALGO", "BROKER_API", "PERF"}

    def evaluate(self, entry: LogEntry, context: AnalysisContext) -> Optional[AnomalyRecord]:
        if not entry.message or len(entry.message) < 5:
            return None

        # Extract potential account mentions from message
        matches = self.ACCOUNT_REGEX.findall(entry.message)
        extracted_candidates = set()
        for m in matches:
            val = (m[0] or m[1] or "").strip()
            if val and val.upper() not in self.IGNORE_TAGS:
                extracted_candidates.add(val)

        if not extracted_candidates:
            return None

        known_lower = {acc.lower(): acc for acc in context.known_accounts}
        matched_known_accounts = {
            known_lower[cand.lower()] for cand in extracted_candidates if cand.lower() in known_lower
        }

        if not matched_known_accounts:
            return None

        row_acc = (entry.account_id or "").strip()
        for cand in matched_known_accounts:
            if not row_acc:
                return self.create_anomaly(
                    entry,
                    reason=(
                        f"Log message explicitly references account '{cand}', but the row has no linked "
                        f"account foreign key (account_id is NULL). Thread context may have been dropped."
                    ),
                )
            elif row_acc.lower() != cand.lower():
                return self.create_anomaly(
                    entry,
                    reason=(
                        f"Cross-account context bleed detected: Message explicitly references account '{cand}', "
                        f"but row is attributed to foreign key account '{row_acc}'."
                    ),
                )

        return None
