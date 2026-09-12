"""
kalai.auth
──────────
Pluggable authentication and OAuth adapter architecture for multi-broker and multi-account trading systems.
"""

from .base import BaseBrokerAuth
from .registry import BrokerAuthRegistry, get_auth_adapter
from .totp import clean_totp_secret, generate_totp_code, get_network_time_offset, get_totp_for_broker

__all__ = [
    "BaseBrokerAuth",
    "BrokerAuthRegistry",
    "get_auth_adapter",
    "clean_totp_secret",
    "generate_totp_code",
    "get_network_time_offset",
    "get_totp_for_broker",
]
