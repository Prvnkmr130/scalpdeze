"""
kalai/auth/zerodha.py
────────────────────
Zerodha Kite Connect OAuth authentication adapter.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import urlencode

from .base import BaseBrokerAuth

if TYPE_CHECKING:
    from django.http import HttpRequest
    from kalai.models import Broker

logger = logging.getLogger("kalai.auth.zerodha")


class ZerodhaAuthAdapter(BaseBrokerAuth):
    """Authentication adapter for Zerodha (Kite Connect)."""

    BROKER_CODES = ["zerodha", "kite"]
    DISPLAY_NAME = "Zerodha"
    SUPPORTS_OAUTH = True

    def get_login_url(
        self,
        broker: Broker,
        request: HttpRequest,
        callback_url: str | None = None,
    ) -> str:
        """
        Build the Kite Connect login URL.
        """
        if not broker.api_key or not broker.api_key.strip():
            raise ValueError(f"API key is missing for Zerodha account '{broker.account_id or broker.name}'.")

        params: dict[str, str] = {
            "api_key": broker.api_key.strip(),
            "v": "3",
        }
        
        # If a specific redirect_url is provided, attach it (some Kite apps support redirect_url override)
        if callback_url:
            params["redirect_url"] = callback_url
        elif broker.redirect_url:
            params["redirect_url"] = broker.redirect_url

        return f"https://kite.trade/connect/login?{urlencode(params)}"

    def handle_callback(self, broker: Broker, request: HttpRequest) -> tuple[bool, str]:
        """
        Exchange the Kite request_token for a session access_token.
        """
        request_token = request.GET.get("request_token")
        status = request.GET.get("status")

        if status == "cancelled" or request.GET.get("action") == "cancelled":
            return False, "Login was cancelled on Zerodha."

        if not request_token:
            return False, "No 'request_token' parameter received from Zerodha."

        if not broker.api_key or not broker.api_secret:
            return False, f"Missing API key or API secret for {broker.account_id or broker.name}."

        try:
            from kiteconnect import KiteConnect

            kite = KiteConnect(api_key=broker.api_key.strip())
            session = kite.generate_session(
                request_token=request_token.strip(),
                api_secret=broker.api_secret.strip(),
            )
            access_token = session.get("access_token")
            refresh_token = session.get("refresh_token")

            if not access_token:
                return False, "No access_token returned in Kite session response."

            self.save_session_tokens(broker, access_token, refresh_token)
            return True, f"Successfully authenticated Zerodha account '{broker.account_id or broker.name}'!"

        except Exception as exc:
            logger.error("KiteConnect token generation failed for %s: %s", broker.name, exc, exc_info=True)
            return False, f"Failed to generate Zerodha session: {exc}"
