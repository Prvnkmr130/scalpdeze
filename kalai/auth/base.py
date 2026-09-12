"""
kalai/auth/base.py
──────────────────
Abstract base class for all broker authentication adapters.
"""

from __future__ import annotations

import logging
from abc import ABC
from typing import TYPE_CHECKING, Any

from django.utils import timezone

if TYPE_CHECKING:
    from django.http import HttpRequest
    from kalai.models import Broker

logger = logging.getLogger("kalai.auth")


class BaseBrokerAuth(ABC):
    """
    Abstract Base Class defining the contract for broker login and token management.
    """

    BROKER_CODES: list[str] = []
    DISPLAY_NAME: str = "Broker"
    SUPPORTS_OAUTH: bool = False
    SUPPORTS_DIRECT_LOGIN: bool = False

    def supports_oauth(self) -> bool:
        """Returns True if this broker uses browser-based OAuth redirect flow."""
        return self.SUPPORTS_OAUTH

    def supports_direct_login(self) -> bool:
        """Returns True if this broker supports programmatic login (e.g. TOTP + MPIN / API key)."""
        return self.SUPPORTS_DIRECT_LOGIN

    def get_login_url(self, broker: Broker, request: HttpRequest, callback_url: str | None = None) -> str:
        """
        Generate the external OAuth login URL for the given broker account.
        """
        raise NotImplementedError(f"OAuth login is not supported for {self.DISPLAY_NAME}.")

    def handle_callback(self, broker: Broker, request: HttpRequest) -> tuple[bool, str]:
        """
        Process the incoming OAuth callback, exchange authorization codes for live tokens,
        and update the broker account record in the database.
        Returns: (success: bool, message: str)
        """
        raise NotImplementedError(f"OAuth callback handling is not supported for {self.DISPLAY_NAME}.")

    def handle_direct_login(self, broker: Broker, **kwargs: Any) -> tuple[bool, str]:
        """
        Execute programmatic direct login (e.g. TOTP + MPIN).
        Returns: (success: bool, message: str)
        """
        raise NotImplementedError(f"Direct programmatic login is not supported for {self.DISPLAY_NAME}.")

    @classmethod
    def save_session_tokens(
        cls,
        broker: Broker,
        access_token: str,
        refresh_token: str | None = None,
    ) -> None:
        """
        Standard helper to persist access token and timestamp to the Broker model.
        Saving triggers notify_engine_on_account_save to reload the WebSocket engine.
        """
        broker.access_token = access_token
        broker.access_token_updated_at = timezone.now()
        if refresh_token:
            broker.refresh_token = refresh_token
        broker.save()
        logger.info(
            "Updated session tokens for %s (%s) at %s",
            broker.account_id or broker.name,
            cls.DISPLAY_NAME,
            broker.access_token_updated_at,
        )
