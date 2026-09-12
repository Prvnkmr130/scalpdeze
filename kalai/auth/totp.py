"""
kalai/auth/totp.py
──────────────────
Universal time-synchronized, Base32-sanitized TOTP generation utility for all broker accounts.
Supports Zerodha, Kotak Neo, Angel One, Upstox, CoinDCX, and custom multi-account 2FA setups.
"""

from __future__ import annotations

import logging
import re
import time
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

import pyotp
import requests

logger = logging.getLogger("kalai.auth.totp")

_TIME_OFFSET_CACHE = {"offset": 0.0, "last_synced": 0.0}
SYNC_INTERVAL_SECONDS = 300.0  # Resync every 5 minutes


def get_network_time_offset(force: bool = False) -> float:
    """
    Check and cache the clock drift offset between local system time and server/internet time
    (e.g., Zerodha Kite API / Google / Cloudflare) to guarantee valid TOTP codes even when
    the host computer's clock drifts.
    """
    now = time.time()
    if not force and (now - _TIME_OFFSET_CACHE["last_synced"] < SYNC_INTERVAL_SECONDS) and (_TIME_OFFSET_CACHE["last_synced"] > 0):
        return _TIME_OFFSET_CACHE["offset"]

    endpoints = [
        "https://kite.zerodha.com",
        "https://api.kite.trade",
        "https://www.google.com",
        "https://cloudflare.com",
    ]
    for endpoint in endpoints:
        try:
            resp = requests.head(endpoint, timeout=2.0)
            srv_date = resp.headers.get("Date")
            if srv_date:
                srv_dt = parsedate_to_datetime(srv_date)
                offset = srv_dt.timestamp() - time.time()
                _TIME_OFFSET_CACHE["offset"] = offset
                _TIME_OFFSET_CACHE["last_synced"] = now
                logger.debug("Network time offset synchronized: %.2f seconds from %s", offset, endpoint)
                return offset
        except Exception:
            continue

    return _TIME_OFFSET_CACHE["offset"]


def clean_totp_secret(raw_secret: str | None) -> str:
    """
    Sanitize and extract standard Base32 secret key from raw string, handling
    spaces, dashes, lowercase characters, and otpauth:// URI schemas.
    """
    if not raw_secret:
        return ""
    s = str(raw_secret).strip()
    if "otpauth://" in s:
        try:
            parsed = urlparse(s)
            qs = parse_qs(parsed.query)
            if "secret" in qs:
                s = qs["secret"][0]
        except Exception:
            pass
    # Strip any spaces, hyphens, and non-base32 characters
    cleaned = re.sub(r"[^A-Za-z2-7]", "", s).upper()
    return cleaned


def generate_totp_code(raw_secret: str | None, time_offset: float | None = None) -> str:
    """
    Generate a 6-digit zero-padded TOTP code for a given secret key,
    applying network clock drift compensation.
    """
    secret = clean_totp_secret(raw_secret)
    if not secret:
        return "000000"

    try:
        totp = pyotp.TOTP(secret)
        offset = get_network_time_offset() if time_offset is None else time_offset
        current_ts = time.time() + offset
        otp = totp.at(current_ts)
        return str(otp).zfill(6)
    except Exception as exc:
        logger.error("Error generating TOTP code: %s", exc)
        return "000000"


def get_totp_for_broker(broker: Any) -> str:
    """
    Generate the current valid 6-digit TOTP for any Broker instance or account.
    """
    if not broker:
        return "000000"
    secret = getattr(broker, "totp_secret", None)
    if not secret and isinstance(broker, dict):
        secret = broker.get("totp_secret")
    return generate_totp_code(secret)
