"""
kalai/auth/upstox.py
───────────────────
Upstox API v2 OAuth authentication adapter.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import requests

from .base import BaseBrokerAuth

if TYPE_CHECKING:
    from django.http import HttpRequest
    from kalai.models import Broker

logger = logging.getLogger("kalai.auth.upstox")


class UpstoxAuthAdapter(BaseBrokerAuth):
    """Authentication adapter for Upstox (API v2)."""

    BROKER_CODES = ["upstox", "uplink"]
    DISPLAY_NAME = "Upstox"
    SUPPORTS_OAUTH = True

    def get_login_url(
        self,
        broker: Broker,
        request: HttpRequest,
        callback_url: str | None = None,
    ) -> str:
        if not broker.api_key:
            raise ValueError(f"API key is missing for Upstox account '{broker.account_id or broker.name}'.")

        redirect_uri = callback_url or broker.redirect_url or request.build_absolute_uri()
        params = {
            "client_id": broker.api_key.strip(),
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "state": broker.account_id or broker.name,
        }
        return f"https://api.upstox.com/v2/login/authorization/dialog?{urlencode(params)}"

    def handle_callback(self, broker: Broker, request: HttpRequest) -> tuple[bool, str]:
        code = request.GET.get("code")
        if not code:
            return False, "No authorization code received from Upstox."

        redirect_uri = broker.redirect_url or request.build_absolute_uri().split("?")[0]
        url = "https://api.upstox.com/v2/login/authorization/token"
        headers = {"accept": "application/json", "Api-Version": "2.0"}
        data = {
            "code": code,
            "client_id": broker.api_key,
            "client_secret": broker.api_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }

        try:
            res = requests.post(url, headers=headers, data=data, timeout=10)
            json_data = res.json()
            if res.status_code == 200 and "access_token" in json_data:
                self.save_session_tokens(broker, json_data["access_token"])
                return True, f"Upstox authentication successful for '{broker.account_id or broker.name}'!"
            return False, f"Upstox token exchange failed: {json_data.get('errors', json_data)}"
        except Exception as exc:
            logger.error("Upstox token exchange error for %s: %s", broker.name, exc)
            return False, f"Upstox error: {exc}"
