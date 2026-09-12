"""
kalai/auth/angel.py
───────────────────
Angel One (SmartAPI) authentication adapter.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .base import BaseBrokerAuth

if TYPE_CHECKING:
    from django.http import HttpRequest
    from kalai.models import Broker

logger = logging.getLogger("kalai.auth.angel")


class AngelOneAuthAdapter(BaseBrokerAuth):
    """Authentication adapter for Angel One SmartAPI."""

    BROKER_CODES = ["angel", "angelone", "smartapi"]
    DISPLAY_NAME = "Angel One"
    SUPPORTS_OAUTH = False
    SUPPORTS_DIRECT_LOGIN = True

    def handle_callback(self, broker: Broker, request: HttpRequest) -> tuple[bool, str]:
        token = request.GET.get("token") or request.GET.get("access_token")
        if token:
            self.save_session_tokens(broker, token)
            return True, f"Updated Angel One token for '{broker.account_id or broker.name}'."
        return False, "No token parameter received for Angel One."

    def handle_direct_login(self, broker: Broker, **kwargs: Any) -> tuple[bool, str]:
        """
        Authenticate with SmartAPI using client_code, password/pin, totp, and api_key.
        """
        api_key = broker.api_key or kwargs.get("api_key")
        client_code = broker.account_id or kwargs.get("client_code")
        pin = kwargs.get("pin") or broker.api_secret
        totp = kwargs.get("totp") or (broker.get_totp() if hasattr(broker, "get_totp") else None)

        if not api_key or not client_code:
            return False, "Missing api_key or client_code for Angel One."

        try:
            from SmartApi import SmartConnect

            obj = SmartConnect(api_key=api_key)
            data = obj.generateSession(client_code, pin, totp)
            if data and data.get("status"):
                jwt_token = data["data"]["jwtToken"]
                refresh_token = data["data"].get("refreshToken")
                self.save_session_tokens(broker, jwt_token, refresh_token)
                return True, f"Angel One login successful for '{client_code}'!"
            return False, f"Angel One authentication failed: {data.get('message', 'Invalid response')}"
        except ImportError:
            return False, "SmartApi library is not installed."
        except Exception as exc:
            logger.error("Angel One login error: %s", exc)
            return False, f"Angel One error: {exc}"
