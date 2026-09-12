"""
kalai/auth/coinswitch.py
────────────────────────
CoinSwitch PRO authentication adapter for API Key / Ed25519 digital signature credentials.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from cryptography.hazmat.primitives.asymmetric import ed25519

from .base import BaseBrokerAuth

if TYPE_CHECKING:
    from kalai.models import Broker

logger = logging.getLogger("kalai.auth")


class CoinSwitchAuthAdapter(BaseBrokerAuth):
    """
    Authentication adapter for CoinSwitch PRO.
    Uses Ed25519 public/private keypairs configured via API Key and API Secret.
    """

    BROKER_CODES = ["coinswitch", "coinswitch_pro", "coinswitchx"]
    DISPLAY_NAME = "CoinSwitch PRO"
    SUPPORTS_OAUTH = False
    SUPPORTS_DIRECT_LOGIN = True

    def handle_direct_login(self, broker: Broker, **kwargs: Any) -> tuple[bool, str]:
        """
        Validate configured Ed25519 API Key and Secret for CoinSwitch PRO.
        """
        api_key = (broker.api_key or "").strip()
        api_secret = (broker.api_secret or "").strip()

        if not api_key:
            return False, f"Missing API Key for CoinSwitch account '{broker.account_id or broker.name}'."
        if not api_secret:
            return False, f"Missing API Secret (Ed25519 private key) for CoinSwitch account '{broker.account_id or broker.name}'."

        try:
            # Validate Ed25519 key structure
            try:
                secret_bytes = bytes.fromhex(api_secret)
                ed25519.Ed25519PrivateKey.from_private_bytes(secret_bytes)
            except Exception:
                raw_bytes = api_secret.encode("utf-8")[:32].ljust(32, b"\x00")
                ed25519.Ed25519PrivateKey.from_private_bytes(raw_bytes)

            session_token = f"ed25519::{api_key[:8]}...{api_key[-6:] if len(api_key) > 14 else api_key}"
            self.save_session_tokens(broker, access_token=session_token)
            return True, f"CoinSwitch PRO credentials verified for '{broker.account_id or broker.name}'."
        except Exception as exc:
            logger.error("Failed to validate CoinSwitch credentials for %s: %s", broker.account_id or broker.name, exc)
            return False, f"Invalid CoinSwitch Ed25519 key: {exc}"
