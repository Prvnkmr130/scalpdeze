"""
kalai/auth/registry.py
──────────────────────
Registry for looking up the appropriate auth adapter for any broker account.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .angel import AngelOneAuthAdapter
from .base import BaseBrokerAuth
from .coinswitch import CoinSwitchAuthAdapter
from .delta import DeltaExchangeAuthAdapter
from .kotak import KotakNeoAuthAdapter
from .upstox import UpstoxAuthAdapter
from .zerodha import ZerodhaAuthAdapter

if TYPE_CHECKING:
    from kalai.models import Broker

logger = logging.getLogger("kalai.auth")


class GenericBrokerAuthAdapter(BaseBrokerAuth):
    """Fallback adapter for brokers without custom OAuth logic."""

    DISPLAY_NAME = "Generic Broker"

    def handle_callback(self, broker: Broker, request) -> tuple[bool, str]:
        token = (
            request.GET.get("access_token")
            or request.GET.get("token")
            or request.GET.get("request_token")
            or request.GET.get("auth_token")
            or request.GET.get("code")
        )
        if token:
            self.save_session_tokens(broker, token)
            return True, f"Updated token for account '{broker.account_id or broker.name}'."
        return False, f"No access token received in callback for {broker.name}."


class BrokerAuthRegistry:
    """Singleton registry holding all active broker auth adapters."""

    def __init__(self) -> None:
        self._adapters: dict[str, BaseBrokerAuth] = {}
        self._generic_adapter = GenericBrokerAuthAdapter()
        self._register_defaults()

    def register(self, adapter_cls: type[BaseBrokerAuth]) -> None:
        adapter = adapter_cls()
        for code in adapter.BROKER_CODES:
            self._adapters[code.lower()] = adapter

    def _register_defaults(self) -> None:
        for cls in (
            ZerodhaAuthAdapter,
            KotakNeoAuthAdapter,
            UpstoxAuthAdapter,
            AngelOneAuthAdapter,
            CoinSwitchAuthAdapter,
            DeltaExchangeAuthAdapter,
        ):
            self.register(cls)

    def get_adapter_by_code(self, code: str | None) -> BaseBrokerAuth:
        if not code:
            return self._generic_adapter
        return self._adapters.get(code.lower().strip(), self._generic_adapter)

    def get_adapter(self, broker: Broker) -> BaseBrokerAuth:
        """
        Resolve adapter using broker_name.code, api_provider.code, or broker.name.
        """
        # 1. Try broker_name (ForeignKey to BrokerType)
        if hasattr(broker, "broker_name") and broker.broker_name:
            code = getattr(broker.broker_name, "code", None) or getattr(broker.broker_name, "name", None)
            if code and code.lower() in self._adapters:
                return self._adapters[code.lower()]

        # 2. Try api_provider (ForeignKey to ApiProvider)
        if hasattr(broker, "api_provider") and broker.api_provider:
            code = getattr(broker.api_provider, "code", None) or getattr(broker.api_provider, "name", None)
            if code and code.lower() in self._adapters:
                return self._adapters[code.lower()]

        # 3. Try broker.name
        if broker.name and broker.name.lower() in self._adapters:
            return self._adapters[broker.name.lower()]

        return self._generic_adapter


# Global singleton instance
auth_registry = BrokerAuthRegistry()


def get_auth_adapter(broker: Broker) -> BaseBrokerAuth:
    """Convenience helper to retrieve the auth adapter for a broker."""
    return auth_registry.get_adapter(broker)
