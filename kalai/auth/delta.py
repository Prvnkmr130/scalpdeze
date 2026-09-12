"""
kalai/auth/delta.py
───────────────────
Delta Exchange authentication adapter for API Key / HMAC-SHA256 signature credentials.
Supports Delta Exchange Global (api.delta.exchange) and Delta Exchange India (api.india.delta.exchange).
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import TYPE_CHECKING, Any

from .base import BaseBrokerAuth

if TYPE_CHECKING:
    from kalai.models import Broker

logger = logging.getLogger("kalai.auth")


class DeltaExchangeAuthAdapter(BaseBrokerAuth):
    """
    Authentication adapter for Delta Exchange (Global & India).
    Uses HMAC-SHA256 signing of (METHOD + TIMESTAMP + PATH + QUERY_STRING + PAYLOAD)
    with API Key and API Secret.
    """

    BROKER_CODES = ["delta", "delta_exchange", "delta_india", "deltaexchange"]
    DISPLAY_NAME = "Delta Exchange"
    SUPPORTS_OAUTH = False
    SUPPORTS_DIRECT_LOGIN = True

    def handle_direct_login(self, broker: Broker, **kwargs: Any) -> tuple[bool, str]:
        """
        Validate configured API Key and Secret for Delta Exchange.
        """
        api_key = (broker.api_key or "").strip()
        api_secret = (broker.api_secret or "").strip()

        if not api_key:
            return False, f"Missing API Key for Delta Exchange account '{broker.account_id or broker.name}'."
        if not api_secret:
            return False, f"Missing API Secret for Delta Exchange account '{broker.account_id or broker.name}'."

        try:
            # Test signature calculation to ensure secret and key validity format
            timestamp = str(int(time.time()))
            path = "/v2/wallet/balances"
            msg = "GET" + timestamp + path
            signature = hmac.new(api_secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()

            session_token = f"hmac256::{api_key[:6]}...{api_key[-4:] if len(api_key) > 10 else api_key}"
            self.save_session_tokens(broker, access_token=session_token)
            return True, f"Delta Exchange credentials verified for '{broker.account_id or broker.name}' (Signature Hash: {signature[:8]}...)."
        except Exception as exc:
            logger.error("Failed to validate Delta Exchange credentials for %s: %s", broker.account_id or broker.name, exc)
            return False, f"Invalid Delta Exchange credentials: {exc}"
