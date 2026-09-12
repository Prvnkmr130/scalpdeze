"""
kalai/auth/kotak.py
───────────────────
Kotak Neo 2FA (TOTP + MPIN) and token authentication adapter.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .base import BaseBrokerAuth

if TYPE_CHECKING:
    from django.http import HttpRequest
    from kalai.models import Broker

logger = logging.getLogger("kalai.auth.kotak")


class KotakNeoAuthAdapter(BaseBrokerAuth):
    """Authentication adapter for Kotak Neo API."""

    BROKER_CODES = ["kotak_neo", "kotak"]
    DISPLAY_NAME = "Kotak Neo"
    SUPPORTS_OAUTH = False
    SUPPORTS_DIRECT_LOGIN = True

    def handle_callback(self, broker: Broker, request: HttpRequest) -> tuple[bool, str]:
        """
        Handle direct token submission or callback tokens if routed through web.
        """
        token = request.GET.get("token") or request.GET.get("access_token") or request.GET.get("auth_token")
        sid = request.GET.get("sid") or broker.account_id or broker.name

        if token:
            compound_token = f"{token}:::{sid}" if sid else token
            self.save_session_tokens(broker, compound_token)
            return True, f"Updated Kotak Neo token for account '{broker.account_id or broker.name}'."

        return False, "No valid Kotak Neo authentication token provided."

    @staticmethod
    def _direct_http_login(consumer_key: str, ucc: str, totp: str, mpin: str, mobile: str | None = None) -> tuple[bool, str, str]:
        """
        Direct HTTP 2FA login against Kotak Neo API v2 without requiring external neo_api_client package.
        Returns (success, bearer_token, sid_or_error_message).
        """
        import requests

        headers_step1 = {
            "Authorization": consumer_key.strip(),
            "neo-fin-key": "neotradeapi",
            "Content-Type": "application/json",
        }

        mobiles_to_try = []
        if mobile:
            mob_clean = mobile.strip().replace(" ", "").replace("-", "")
            mobiles_to_try.append(mob_clean)
            if mob_clean.startswith("+91") and len(mob_clean) == 13:
                mobiles_to_try.append(mob_clean[3:])
            elif len(mob_clean) == 10 and mob_clean.isdigit():
                mobiles_to_try.append(f"+91{mob_clean}")
        else:
            mobiles_to_try = [None]

        endpoints = [
            ("https://mis.kotaksecurities.com/login/1.0/tradeApiLogin", "https://mis.kotaksecurities.com/login/1.0/tradeApiValidate"),
            ("https://napi.kotaksecurities.com/login/1.0/login/v6/totp/login", "https://napi.kotaksecurities.com/login/1.0/login/v6/totp/validate"),
            ("https://gw-napi.kotaksecurities.com/login/1.0/login/v6/totp/login", "https://gw-napi.kotaksecurities.com/login/1.0/login/v6/totp/validate"),
        ]

        last_err = ""
        for login_url, validate_url in endpoints:
            for mob in mobiles_to_try:
                try:
                    body_step1 = {
                        "mobileNumber": mob,
                        "ucc": ucc,
                        "totp": str(totp).strip()
                    }
                    resp1 = requests.post(login_url, headers=headers_step1, json=body_step1, timeout=15)
                    if not resp1.ok:
                        try:
                            err_data = resp1.json()
                            msg = err_data.get("message") or err_data.get("error") or err_data.get("data", {}).get("message") or resp1.text[:200]
                            if isinstance(msg, list) and len(msg) > 0 and isinstance(msg[0], dict):
                                msg = msg[0].get("message", str(msg))
                        except Exception:
                            msg = resp1.text[:200]
                        last_err = f"Step 1 (TOTP) failed ({resp1.status_code}): {msg}"
                        continue

                    resp1_data = resp1.json().get("data", {})
                    view_token = resp1_data.get("token") or resp1_data.get("viewToken")
                    sid = resp1_data.get("sid")

                    if not view_token or not sid:
                        last_err = f"Step 1 response missing token/sid: {resp1.text[:200]}"
                        continue

                    headers_step2 = {
                        "Authorization": consumer_key.strip(),
                        "sid": sid,
                        "Auth": view_token,
                        "neo-fin-key": "neotradeapi",
                        "Content-Type": "application/json",
                    }
                    body_step2 = {
                        "mpin": str(mpin).strip()
                    }

                    resp2 = requests.post(validate_url, headers=headers_step2, json=body_step2, timeout=15)
                    if not resp2.ok:
                        try:
                            err_data = resp2.json()
                            msg = err_data.get("message") or err_data.get("error") or err_data.get("data", {}).get("message") or resp2.text[:200]
                            if isinstance(msg, list) and len(msg) > 0 and isinstance(msg[0], dict):
                                msg = msg[0].get("message", str(msg))
                        except Exception:
                            msg = resp2.text[:200]
                        last_err = f"Step 2 (MPIN) failed ({resp2.status_code}): {msg}"
                        continue

                    resp2_data = resp2.json().get("data", {})
                    bearer_token = resp2_data.get("token") or resp2_data.get("editToken") or resp2_data.get("bearerToken")
                    edit_sid = resp2_data.get("sid") or resp2_data.get("editSid") or sid

                    if bearer_token:
                        return True, bearer_token, edit_sid
                    else:
                        last_err = f"Step 2 response missing bearer token: {resp2.text[:200]}"
                except Exception as exc:
                    last_err = f"Connection error: {exc}"

        return False, "", last_err or "Kotak Neo authentication failed on all endpoints."

    def handle_direct_login(self, broker: Broker, **kwargs: Any) -> tuple[bool, str]:
        """
        Execute 2FA TOTP login via Kotak Neo SDK or direct HTTP REST.
        """
        consumer_key = (broker.api_key or kwargs.get("consumer_key") or "").strip()
        
        # Intelligent resolution of Mobile Number, UCC, MPIN, and TOTP
        acc_raw = (broker.account_id or broker.name or "").strip()
        ref_raw = (broker.refresh_token or "").strip()

        mobile = kwargs.get("mobile_number")
        ucc = kwargs.get("ucc")

        # 1. Extract mobile number from refresh_token if present
        if not mobile and ref_raw:
            ref_digits = ref_raw.replace("+", "").replace(" ", "").replace("-", "")
            if ref_digits.isdigit() and len(ref_digits) >= 10:
                mobile = ref_raw

        # 2. Extract compound UCC:::MOBILE or MOBILE:::UCC from account_id
        if ":::" in acc_raw:
            parts = acc_raw.split(":::", 1)
            p1, p2 = parts[0].strip(), parts[1].strip()
            p1_digits = p1.replace("+", "").replace(" ", "").replace("-", "")
            p2_digits = p2.replace("+", "").replace(" ", "").replace("-", "")
            if p1_digits.isdigit() and len(p1_digits) >= 10:
                mobile = mobile or p1
                ucc = ucc or p2
            elif p2_digits.isdigit() and len(p2_digits) >= 10:
                mobile = mobile or p2
                ucc = ucc or p1
            else:
                ucc = ucc or p1
                mobile = mobile or p2
        elif not ucc:
            acc_digits = acc_raw.replace("+", "").replace(" ", "").replace("-", "")
            if acc_digits.isdigit() and len(acc_digits) >= 10:
                mobile = mobile or acc_raw
                ucc = broker.name if broker.name and not broker.name.isdigit() else acc_raw
            else:
                ucc = acc_raw

        # 3. Resolve MPIN
        mpin = kwargs.get("mpin")
        secret1 = (broker.api_secret or "").strip()
        secret2 = (broker.totp_secret or "").strip()

        if not mpin:
            if secret1:
                mpin = secret1
            elif secret2 and secret2.isdigit() and len(secret2) in (4, 6):
                mpin = secret2

        # 4. Resolve TOTP
        totp = kwargs.get("totp")
        if not totp:
            if hasattr(broker, "get_totp"):
                try:
                    totp = broker.get_totp()
                except Exception:
                    totp = None

        if not consumer_key:
            return False, "Missing Consumer Key (API Key) for Kotak Neo account. Please configure 'API Key' in Django Admin."

        if not mobile:
            return False, (
                f"Missing Registered Mobile Number for Kotak Neo account '{broker.account_id or broker.name}'. "
                "Please enter your 10-digit registered mobile number (e.g. +919876543210) in the 'Refresh Token' field in Django Admin."
            )

        if not mpin:
            return False, (
                f"Missing Neo MPIN for account '{broker.account_id or broker.name}'. "
                "Please enter your 6-digit Kotak Neo MPIN in the 'API Secret' field in Django Admin."
            )

        if not totp:
            return False, (
                f"Missing TOTP Secret for account '{broker.account_id or broker.name}'. "
                "Please enter your TOTP 2FA secret seed in the 'TOTP Secret' field in Django Admin."
            )

        # 1. Try official SDK if installed in the environment (dynamic import prevents static linter/type errors)
        try:
            import importlib
            neo_module = importlib.import_module("neo_api_client")
            NeoAPI = getattr(neo_module, "NeoAPI", None)

            if NeoAPI is not None:
                client = NeoAPI(environment="prod", consumer_key=consumer_key)
                client.totp_login(mobile_number=mobile, ucc=ucc, totp=totp)
                client.totp_validate(mpin=mpin)

                token = getattr(client.configuration, "bearer_token", "")
                sid = getattr(client.configuration, "edit_sid", None) or getattr(client.configuration, "sid", ucc)

                if token:
                    compound_token = f"{token}:::{sid}"
                    self.save_session_tokens(broker, compound_token)
                    return True, f"Kotak Neo login successful for account '{ucc}'!"

        except (ImportError, ModuleNotFoundError, AttributeError):
            logger.info("neo_api_client SDK not installed; using built-in high-performance REST authentication.")
        except Exception as exc:
            logger.warning("SDK login failed for %s, trying built-in REST fallback: %s", broker.name, exc)

        # 2. Seamless built-in Direct HTTP REST authentication
        success, token, sid_or_err = self._direct_http_login(
            consumer_key=consumer_key,
            ucc=ucc,
            totp=totp,
            mpin=mpin,
            mobile=mobile
        )
        if success:
            compound_token = f"{token}:::{sid_or_err}"
            self.save_session_tokens(broker, compound_token)
            return True, f"Kotak Neo login successful for account '{ucc}'!"
        else:
            logger.error("Kotak Neo direct HTTP login failed for %s: %s", broker.name, sid_or_err)
            return False, f"Kotak Neo authentication error: {sid_or_err}"


