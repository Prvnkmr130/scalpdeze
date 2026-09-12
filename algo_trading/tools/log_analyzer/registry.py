# -*- coding: utf-8 -*-
"""
algo_trading/tools/log_analyzer/registry.py
────────────────────────────────────────────
Centralized pluggable registry for anomaly detection rules.
Enables adding, enabling, disabling, or removing rules dynamically.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Type

from algo_trading.tools.log_analyzer.rules.base import BaseAnomalyRule

logger = logging.getLogger("algo_trading.tools.log_analyzer.registry")


class RuleRegistry:
    """Registry that manages active and inactive log anomaly rules."""

    def __init__(self) -> None:
        self._rules: Dict[str, BaseAnomalyRule] = {}

    def register(self, rule: BaseAnomalyRule) -> None:
        """Register an instantiated rule."""
        if not isinstance(rule, BaseAnomalyRule):
            raise TypeError(f"Expected BaseAnomalyRule instance, got {type(rule)}")
        self._rules[rule.rule_id] = rule
        logger.debug("Registered log anomaly rule: %s [%s]", rule.name, rule.rule_id)

    def unregister(self, rule_id: str) -> Optional[BaseAnomalyRule]:
        """Remove a rule by ID."""
        return self._rules.pop(rule_id, None)

    def enable_rule(self, rule_id: str) -> bool:
        """Enable a rule by ID."""
        if rule_id in self._rules:
            self._rules[rule_id].enabled = True
            return True
        return False

    def disable_rule(self, rule_id: str) -> bool:
        """Disable a rule by ID without removing it."""
        if rule_id in self._rules:
            self._rules[rule_id].enabled = False
            return True
        return False

    def get_rule(self, rule_id: str) -> Optional[BaseAnomalyRule]:
        """Fetch a registered rule by ID."""
        return self._rules.get(rule_id)

    def get_active_rules(self) -> List[BaseAnomalyRule]:
        """Return all currently enabled rules in registration order."""
        return [rule for rule in self._rules.values() if rule.enabled]

    def get_all_rules(self) -> List[BaseAnomalyRule]:
        """Return all registered rules."""
        return list(self._rules.values())

    def clear(self) -> None:
        """Clear all registered rules."""
        self._rules.clear()


# Global registry singleton
rule_registry = RuleRegistry()


def register_rule(cls: Type[BaseAnomalyRule]) -> Type[BaseAnomalyRule]:
    """Decorator to register a BaseAnomalyRule class automatically."""
    instance = cls()
    rule_registry.register(instance)
    return cls
