# -*- coding: utf-8 -*-
"""
kalai/context_processors.py
───────────────────────────
High-performance, secure context processors for Django Admin and frontend views.
Injects:
- Debug mode flags
- CoinSwitch API secret key expiry alerts (cached 5h)
- Comprehensive Admin System Hub telemetry (logins, warnings, errors, upcoming events)
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime, timedelta
import zoneinfo
from typing import Any, Dict, List, Optional

from django.conf import settings
from django.core.cache import cache
from django.db.models import Q
from django.utils import timezone

logger = logging.getLogger(__name__)

CACHE_KEY_EXPIRY_ALERTS = "coinswitch_secret_expiry_alerts"
CACHE_TIMEOUT_5_HOURS = 5 * 60 * 60  # 18,000 seconds (5 hours)

CACHE_KEY_SYSTEM_HUB = "admin_system_hub_telemetry"
CACHE_TIMEOUT_SYSTEM_HUB = 30  # 30 seconds for near-realtime dashboard with 0 DB overhead


def debug_mode(request: Any) -> Dict[str, Any]:
    return {"DEBUG_MODE": settings.DEBUG}


def _mask_sensitive_data(text: str) -> str:
    """Mask credentials, API keys, and session tokens to prevent information disclosure."""
    if not text:
        return ""
    # Mask key=value patterns for secrets, tokens, passwords
    cleaned = re.sub(
        r"(api_secret|api_key|totp_secret|access_token|password|secret|token)=['\"][^'\"]+['\"]",
        r"\1='***MASKED***'",
        str(text),
        flags=re.IGNORECASE
    )
    # Mask raw hex/jwt tokens longer than 24 chars
    cleaned = re.sub(r"\b([a-zA-Z0-9_\-\.]{24,})\b", r"[\1...MASKED]", cleaned)
    return cleaned


def _format_relative_time(dt: Optional[datetime]) -> str:
    """Render human-friendly relative timestamps (e.g., '2m ago', '1h ago')."""
    if not dt:
        return "N/A"
    try:
        now = timezone.now()
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, timezone.get_current_timezone())
        diff = now - dt
        secs = int(diff.total_seconds())
        if secs < 0:
            return "Just now"
        if secs < 60:
            return f"{secs}s ago"
        mins = secs // 60
        if mins < 60:
            return f"{mins}m ago"
        hrs = mins // 60
        if hrs < 24:
            return f"{hrs}h ago"
        days = hrs // 24
        return f"{days}d ago"
    except Exception:
        return str(dt)[:19]


def get_coinswitch_secret_expiry_alerts(force: bool = False) -> List[Dict[str, Any]]:
    """
    Fetch all CoinSwitch accounts whose API secret key is expiring within their configured
    warning threshold (default 7 days) or already expired.
    Cached for 5 hours to eliminate redundant database queries.
    """
    if not force:
        cached_alerts = cache.get(CACHE_KEY_EXPIRY_ALERTS)
        if cached_alerts is not None:
            return cached_alerts

    alerts = []
    try:
        from kalai.models import Broker

        expiring_brokers = (
            Broker.objects.select_related("broker_name", "api_provider")
            .filter(enable_secret_key_expiry=True)
            .exclude(api_secret__isnull=True)
            .exclude(api_secret="")
        )

        for broker in expiring_brokers:
            status = broker.secret_key_expiry_status
            if status in ("EXPIRING_SOON", "EXPIRED"):
                days_left = broker.days_until_secret_key_expiry
                expires_at = broker.secret_key_expires_at
                added_at = broker.secret_key_added_at

                alerts.append({
                    "id": broker.pk,
                    "account_id": broker.account_id or broker.name,
                    "name": broker.name,
                    "broker_title": broker.broker_name.name if broker.broker_name else "CoinSwitch PRO",
                    "days_left": days_left if days_left is not None else 0,
                    "expires_at": expires_at.strftime("%b %d, %Y") if expires_at else "N/A",
                    "added_at": added_at.strftime("%b %d, %Y") if added_at else "N/A",
                    "validity_days": broker.secret_key_validity_days or 90,
                    "warn_days": broker.secret_key_warn_days or 7,
                    "status": status,
                    "is_expired": status == "EXPIRED",
                    "edit_url": f"/admin/kalai/broker/{broker.pk}/change/",
                })

        cache.set(CACHE_KEY_EXPIRY_ALERTS, alerts, timeout=CACHE_TIMEOUT_5_HOURS)
    except Exception as e:
        logger.warning("Error evaluating CoinSwitch secret key expiry: %s", e)

    return alerts


def coinswitch_expiry_alerts(request: Any) -> Dict[str, Any]:
    """
    Context processor injecting 5-hour evaluated CoinSwitch secret expiry alerts
    into all admin and application templates for authenticated staff users.
    """
    user = getattr(request, "user", None)
    if not user or not user.is_authenticated:
        return {
            "coinswitch_alerts": [],
            "has_coinswitch_alerts": False,
            "coinswitch_expired_count": 0,
            "coinswitch_expiring_count": 0,
        }

    alerts = get_coinswitch_secret_expiry_alerts()
    expired_count = sum(1 for a in alerts if a.get("is_expired"))
    expiring_count = len(alerts) - expired_count

    return {
        "coinswitch_alerts": alerts,
        "has_coinswitch_alerts": bool(alerts),
        "coinswitch_expired_count": expired_count,
        "coinswitch_expiring_count": expiring_count,
    }


def get_admin_system_hub_data(force: bool = False) -> Dict[str, Any]:
    """
    Aggregate all system telemetry, login events, secret key alerts, errors, and market schedules.
    Cached for 30 seconds to provide responsive live updates with zero database query spam.
    """
    if not force:
        cached = cache.get(CACHE_KEY_SYSTEM_HUB)
        if cached is not None:
            return cached

    now_tz = timezone.now()
    tz_ist = zoneinfo.ZoneInfo("Asia/Kolkata")
    now_ist = datetime.now(tz_ist)
    today_ist_date = now_ist.date()

    telemetry: Dict[str, Any] = {
        "timestamp": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        "date_ist": now_ist.strftime("%A, %b %d, %Y"),
        "time_ist": now_ist.strftime("%I:%M:%S %p"),
        "secret_alerts": [],
        "broker_logins": [],
        "recent_errors": [],
        "market_events": [],
        "summary": {
            "error_count": 0,
            "warning_count": 0,
            "secret_alert_count": 0,
            "login_pending_count": 0,
            "active_login_count": 0,
            "total_issues": 0,
            "health_status": "NORMAL",  # NORMAL, WARNING, CRITICAL
        }
    }

    try:
        from kalai.models import Broker, AlgoLog

        # 1. Secret Key Expiry Warnings
        secret_alerts = get_coinswitch_secret_expiry_alerts(force=force)
        telemetry["secret_alerts"] = secret_alerts
        expired_keys = sum(1 for a in secret_alerts if a.get("is_expired"))
        telemetry["summary"]["secret_alert_count"] = len(secret_alerts)

        # 2. Broker Logins & Token Validity
        active_brokers = (
            Broker.objects.select_related("broker_name", "api_provider")
            .all()
            .order_by("broker_name__name", "name")
        )

        broker_logins = []
        pending_logins = 0
        active_logins = 0

        for b in active_brokers:
            b_name = b.broker_name.name if b.broker_name else "Broker"
            acc_id = b.account_id or b.name
            has_token = bool(b.access_token and b.access_token.strip())
            token_date = None
            is_today_token = False

            if b.access_token_updated_at:
                try:
                    tok_dt = b.access_token_updated_at.astimezone(tz_ist) if timezone.is_aware(b.access_token_updated_at) else b.access_token_updated_at
                    token_date = tok_dt.date()
                    is_today_token = (token_date == today_ist_date)
                except Exception:
                    pass

            # Determine authentication state
            is_token_required = getattr(b, "access_token_required", False)
            b_code = (b.broker_name.code if b.broker_name else "").lower()

            if is_token_required or b_code in ("zerodha", "kotak", "kotak_neo", "coindcx"):
                if has_token and is_today_token:
                    auth_state = "SUCCESS"
                    auth_label = "Logged In Today"
                    badge_type = "badge-success"
                    active_logins += 1
                elif has_token and not is_today_token:
                    auth_state = "TOKEN_EXPIRED"
                    auth_label = "Token Expired (Requires Daily Login)"
                    badge_type = "badge-warning"
                    pending_logins += 1
                else:
                    auth_state = "LOGIN_REQUIRED"
                    auth_label = "No Active Token"
                    badge_type = "badge-danger"
                    pending_logins += 1
            else:
                # API Key / HMAC authenticated brokers (e.g. Delta Exchange)
                has_key = bool(b.api_key and b.api_secret)
                if has_key:
                    auth_state = "KEY_ACTIVE"
                    auth_label = "API Key Active (24/7)"
                    badge_type = "badge-info"
                    active_logins += 1
                else:
                    auth_state = "KEY_MISSING"
                    auth_label = "Missing Credentials"
                    badge_type = "badge-danger"
                    pending_logins += 1

            broker_logins.append({
                "id": b.pk,
                "account_id": acc_id,
                "name": b.name,
                "broker_name": b_name,
                "auth_state": auth_state,
                "auth_label": auth_label,
                "badge_type": badge_type,
                "has_token": has_token,
                "token_updated_at": b.access_token_updated_at.strftime("%b %d, %H:%M") if b.access_token_updated_at else "Never",
                "token_relative": _format_relative_time(b.access_token_updated_at),
                "enable_trade": b.enable_trade,
                "login_url": f"/broker-admin/broker-login/",
                "edit_url": f"/admin/kalai/broker/{b.pk}/change/",
            })

        telemetry["broker_logins"] = broker_logins
        telemetry["summary"]["login_pending_count"] = pending_logins
        telemetry["summary"]["active_login_count"] = active_logins

        # 3. Recent Errors & Warnings from AlgoLog (Last 48 Hours)
        since_48h = now_tz - timedelta(hours=48)
        recent_logs_qs = (
            AlgoLog.objects.filter(level__in=["ERROR", "WARNING"], timestamp__gte=since_48h)
            .select_related("account")
            .order_by("-timestamp")[:15]
        )

        formatted_logs = []
        err_count = 0
        warn_count = 0

        for log in recent_logs_qs:
            lvl = str(log.level).upper()
            if lvl == "ERROR":
                err_count += 1
            elif lvl == "WARNING":
                warn_count += 1

            acc_str = log.account.account_id if log.account else "SYSTEM"
            clean_msg = _mask_sensitive_data(log.message[:180])

            formatted_logs.append({
                "id": log.pk,
                "level": lvl,
                "tag": log.tag or "GENERAL",
                "account": acc_str,
                "algo_name": log.algo_name or "",
                "message": clean_msg,
                "timestamp_str": log.timestamp.astimezone(tz_ist).strftime("%b %d, %H:%M:%S") if timezone.is_aware(log.timestamp) else log.timestamp.strftime("%b %d, %H:%M:%S"),
                "relative_time": _format_relative_time(log.timestamp),
            })

        telemetry["recent_errors"] = formatted_logs
        telemetry["summary"]["error_count"] = err_count
        telemetry["summary"]["warning_count"] = warn_count

        # 4. Market Session Events & Upcoming Schedules
        hour = now_ist.hour
        minute = now_ist.minute
        weekday = now_ist.weekday()  # 0=Monday, 6=Sunday
        is_weekend = weekday >= 5

        # Indian Equities / Options (NSE / BSE / NFO)
        nse_open_min = 9 * 60 + 15
        nse_close_min = 15 * 60 + 30
        curr_min = hour * 60 + minute

        if is_weekend:
            nse_status = "CLOSED (Weekend)"
            nse_badge = "badge-muted"
        elif nse_open_min <= curr_min <= nse_close_min:
            nse_status = "LIVE TRADING (09:15 - 15:30)"
            nse_badge = "badge-success"
        elif curr_min < nse_open_min:
            nse_status = f"Pre-Market (Opens 09:15 IST in {nse_open_min - curr_min}m)"
            nse_badge = "badge-info"
        else:
            nse_status = "Closed for the day (Reopens tomorrow 09:15)"
            nse_badge = "badge-muted"

        # MCX Commodity Session
        mcx_open_min = 9 * 60
        mcx_close_min = 23 * 60 + 30
        if is_weekend:
            mcx_status = "CLOSED (Weekend)"
            mcx_badge = "badge-muted"
        elif mcx_open_min <= curr_min <= mcx_close_min:
            mcx_status = "LIVE TRADING (09:00 - 23:30)"
            mcx_badge = "badge-success"
        else:
            mcx_status = "Closed (Opens 09:00 IST)"
            mcx_badge = "badge-muted"

        market_events = [
            {
                "title": "Crypto Markets (CoinSwitch PRO & Delta Exchange)",
                "schedule": "24/7/365 Continuous Trading",
                "status": "ALWAYS ACTIVE",
                "badge": "badge-success",
                "icon": "⚡",
            },
            {
                "title": "Indian Equities & F&O (NSE / BSE / NFO)",
                "schedule": "09:15 - 15:30 IST (Mon - Fri)",
                "status": nse_status,
                "badge": nse_badge,
                "icon": "📈",
            },
            {
                "title": "MCX Commodities (Crude, Gold, Silver)",
                "schedule": "09:00 - 23:30 IST (Mon - Fri)",
                "status": mcx_status,
                "badge": mcx_badge,
                "icon": "🛢️",
            },
            {
                "title": "Automated Master Token List Synchronizer",
                "schedule": "Daily at 08:45:00 IST",
                "status": "Scheduled Daily Routine",
                "badge": "badge-info",
                "icon": "🔄",
            },
        ]
        telemetry["market_events"] = market_events

        # Overall Health Classification
        total_issues = err_count + expired_keys + pending_logins
        telemetry["summary"]["total_issues"] = total_issues
        if err_count > 0 or expired_keys > 0:
            telemetry["summary"]["health_status"] = "CRITICAL"
        elif warn_count > 0 or pending_logins > 0 or len(secret_alerts) > 0:
            telemetry["summary"]["health_status"] = "WARNING"
        else:
            telemetry["summary"]["health_status"] = "NORMAL"

        cache.set(CACHE_KEY_SYSTEM_HUB, telemetry, timeout=CACHE_TIMEOUT_SYSTEM_HUB)

    except Exception as ex:
        logger.warning("Error compiling admin system hub telemetry: %s", ex)

    return telemetry


def admin_system_hub(request: Any) -> Dict[str, Any]:
    """Context processor delivering the full System Hub telemetry to admin index templates."""
    user = getattr(request, "user", None)
    if not user or not user.is_authenticated or not user.is_staff:
        return {"system_hub": {}}

    data = get_admin_system_hub_data()
    return {"system_hub": data}
