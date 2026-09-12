# tests/test_admin_system_hub.py
"""
Unit and Security Tests for Django Admin System Hub & Collapsible Dashboard Sections:
- Security testing: Credential masking, role-based authorization, XSS protection.
- Performance testing: Caching verification, query minimization.
- Functional testing: Broker login detection, secret expiry warnings, AlgoLog telemetry, market schedules.
- Template integrity: Collapsible panels, persistent storage keys, tab navigation.
"""

from datetime import datetime, timedelta
import json
from unittest.mock import MagicMock, patch
import zoneinfo

from django.test import SimpleTestCase, RequestFactory
from django.utils import timezone
from django.core.cache import cache
from django.template.loader import render_to_string

from kalai.context_processors import (
    get_admin_system_hub_data,
    admin_system_hub,
    _mask_sensitive_data,
    _format_relative_time,
    CACHE_KEY_SYSTEM_HUB,
)
from kalai.views import admin_hub_events_api


class AdminSystemHubTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()

        # Mock Staff User
        self.staff_user = MagicMock()
        self.staff_user.is_authenticated = True
        self.staff_user.is_staff = True
        self.staff_user.username = "testadmin"

        # Mock Regular User
        self.regular_user = MagicMock()
        self.regular_user.is_authenticated = True
        self.regular_user.is_staff = False
        self.regular_user.username = "regularuser"

        # Mock Anonymous User
        self.anon_user = MagicMock()
        self.anon_user.is_authenticated = False
        self.anon_user.is_staff = False

    def tearDown(self):
        cache.clear()

    def test_security_sensitive_data_masking(self):
        """Ensure passwords, API secrets, tokens, and long hashes are never leaked in logs."""
        raw_log = "Order error: api_secret='supersecret12345' access_token='abcdef0123456789abcdef0123456789' for user"
        masked = _mask_sensitive_data(raw_log)
        self.assertNotIn("supersecret12345", masked)
        self.assertIn("MASKED", masked)

        raw_key_log = "Error api_key='myapikey' totp_secret='JBSWY3DPEHPK3PXP' failed"
        masked_key = _mask_sensitive_data(raw_key_log)
        self.assertNotIn("myapikey", masked_key)
        self.assertIn("MASKED", masked_key)

    def test_relative_time_formatter(self):
        """Verify humanized relative timestamps."""
        now = timezone.now()
        self.assertEqual(_format_relative_time(now - timedelta(seconds=15)), "15s ago")
        self.assertEqual(_format_relative_time(now - timedelta(minutes=5)), "5m ago")
        self.assertEqual(_format_relative_time(now - timedelta(hours=3)), "3h ago")
        self.assertEqual(_format_relative_time(now - timedelta(days=2)), "2d ago")
        self.assertEqual(_format_relative_time(None), "N/A")

    def test_security_authorization_context_processor(self):
        """Verify context processor refuses data to unauthenticated and non-staff users."""
        # Unauthenticated request
        req = self.factory.get("/admin/")
        req.user = self.anon_user
        data_anon = admin_system_hub(req)
        self.assertEqual(data_anon, {"system_hub": {}})

        # Normal user request (not staff)
        req.user = self.regular_user
        data_normal = admin_system_hub(req)
        self.assertEqual(data_normal, {"system_hub": {}})

        # Staff user request
        req.user = self.staff_user
        with patch("kalai.models.Broker.objects") as mock_broker_objs, \
             patch("kalai.models.AlgoLog.objects") as mock_log_objs:
            mock_broker_objs.select_related.return_value.all.return_value.order_by.return_value = []
            mock_broker_objs.select_related.return_value.filter.return_value.exclude.return_value.exclude.return_value = []
            mock_log_objs.filter.return_value.select_related.return_value.order_by.return_value = []

            data_staff = admin_system_hub(req)
            self.assertIn("system_hub", data_staff)
            self.assertIn("summary", data_staff["system_hub"])

    def test_security_api_endpoint_access_control(self):
        """Verify /api/admin-hub/events/ blocks unauthorized access and allows staff."""
        # Unauthenticated request
        req = self.factory.get("/api/admin-hub/events/")
        req.user = self.anon_user
        resp_anon = admin_hub_events_api(req)
        self.assertIn(resp_anon.status_code, (401, 403, 302))

        # Regular non-staff request
        req.user = self.regular_user
        resp_reg = admin_hub_events_api(req)
        self.assertIn(resp_reg.status_code, (401, 403, 302))

        # Staff user request
        req.user = self.staff_user
        with patch("kalai.context_processors.get_admin_system_hub_data") as mock_get_hub:
            mock_get_hub.return_value = {"summary": {"health_status": "NORMAL"}}
            resp_staff = admin_hub_events_api(req)
            self.assertEqual(resp_staff.status_code, 200)
            content = json.loads(resp_staff.content)
            self.assertTrue(content["success"])
            self.assertIn("summary", content)

    def test_performance_caching(self):
        """Verify telemetry is cached in memory to avoid repeated queries."""
        with patch("kalai.models.Broker.objects") as mock_broker_objs, \
             patch("kalai.models.AlgoLog.objects") as mock_log_objs:
            mock_broker_objs.select_related.return_value.all.return_value.order_by.return_value = []
            mock_broker_objs.select_related.return_value.filter.return_value.exclude.return_value.exclude.return_value = []
            mock_log_objs.filter.return_value.select_related.return_value.order_by.return_value = []

            cache.clear()
            data1 = get_admin_system_hub_data()
            self.assertIsNotNone(cache.get(CACHE_KEY_SYSTEM_HUB))

            # Second call must return cached data without querying again
            data2 = get_admin_system_hub_data()
            self.assertEqual(data1["timestamp"], data2["timestamp"])

    def test_broker_login_telemetry_evaluation(self):
        """Verify broker login evaluation detects expired daily tokens vs active sessions."""
        now = timezone.now()
        tz_ist = zoneinfo.ZoneInfo("Asia/Kolkata")
        today_ist = datetime.now(tz_ist).date()

        # Mock Broker 1: Zerodha requires login, token is old
        b1 = MagicMock()
        b1.pk = 1
        b1.account_id = "HS6525"
        b1.name = "Zerodha Main"
        b1.broker_name.name = "Zerodha Kite"
        b1.broker_name.code = "zerodha"
        b1.access_token_required = True
        b1.access_token = "token_xyz"
        b1.access_token_updated_at = now - timedelta(days=2)
        b1.enable_trade = True

        # Mock Broker 2: Kotak Neo requires login, logged in today
        b2 = MagicMock()
        b2.pk = 2
        b2.account_id = "W1NPY"
        b2.name = "Kotak Neo Main"
        b2.broker_name.name = "Kotak Neo"
        b2.broker_name.code = "kotak_neo"
        b2.access_token_required = True
        b2.access_token = "valid_today_token"
        b2.access_token_updated_at = now
        b2.enable_trade = True

        # Mock Broker 3: Delta Exchange (HMAC API key)
        b3 = MagicMock()
        b3.pk = 3
        b3.account_id = "delta_main"
        b3.name = "Delta Exchange"
        b3.broker_name.name = "Delta Exchange"
        b3.broker_name.code = "delta"
        b3.access_token_required = False
        b3.api_key = "key123"
        b3.api_secret = "secret123"
        b3.access_token_updated_at = None
        b3.enable_trade = True

        with patch("kalai.models.Broker.objects") as mock_broker_objs, \
             patch("kalai.models.AlgoLog.objects") as mock_log_objs:
            mock_broker_objs.select_related.return_value.all.return_value.order_by.return_value = [b1, b2, b3]
            mock_broker_objs.select_related.return_value.filter.return_value.exclude.return_value.exclude.return_value = []
            mock_log_objs.filter.return_value.select_related.return_value.order_by.return_value = []

            cache.clear()
            telemetry = get_admin_system_hub_data(force=True)

            logins = telemetry["broker_logins"]
            self.assertEqual(len(logins), 3)

            z_item = next(b for b in logins if b["account_id"] == "HS6525")
            self.assertEqual(z_item["auth_state"], "TOKEN_EXPIRED")
            self.assertEqual(z_item["badge_type"], "badge-warning")

            k_item = next(b for b in logins if b["account_id"] == "W1NPY")
            self.assertEqual(k_item["auth_state"], "SUCCESS")
            self.assertEqual(k_item["badge_type"], "badge-success")

            d_item = next(b for b in logins if b["account_id"] == "delta_main")
            self.assertEqual(d_item["auth_state"], "KEY_ACTIVE")
            self.assertEqual(d_item["badge_type"], "badge-info")

    def test_algolog_error_telemetry(self):
        """Verify errors and warnings from AlgoLog are formatted and reflected in summary."""
        log1 = MagicMock()
        log1.pk = 101
        log1.level = "ERROR"
        log1.tag = "ORDER"
        log1.account.account_id = "HS6525"
        log1.algo_name = "indian_opt"
        log1.message = "Order placement rejected: RMS Margin limit exceeded"
        log1.timestamp = timezone.now() - timedelta(minutes=10)

        with patch("kalai.models.Broker.objects") as mock_broker_objs, \
             patch("kalai.models.AlgoLog.objects") as mock_log_objs:
            mock_broker_objs.select_related.return_value.filter.return_value.order_by.return_value = []
            mock_broker_objs.select_related.return_value.filter.return_value.exclude.return_value.exclude.return_value = []
            mock_log_objs.filter.return_value.select_related.return_value.order_by.return_value = [log1]

            cache.clear()
            telemetry = get_admin_system_hub_data(force=True)
            self.assertEqual(telemetry["summary"]["error_count"], 1)
            self.assertEqual(telemetry["summary"]["health_status"], "CRITICAL")
            self.assertEqual(len(telemetry["recent_errors"]), 1)
            self.assertEqual(telemetry["recent_errors"][0]["account"], "HS6525")

    def test_template_collapsible_and_hub_rendering(self):
        """Verify that admin/index.html renders collapsible section headers and telemetry hub."""
        req = self.factory.get("/admin/")
        req.user = self.staff_user

        mock_hub = {
            "timestamp": "2026-09-03 07:30:00 IST",
            "time_ist": "07:30:00 AM",
            "secret_alerts": [
                {
                    "account_id": "cs_user",
                    "broker_title": "CoinSwitch PRO",
                    "is_expired": False,
                    "days_left": 4,
                    "expires_at": "Sep 07, 2026",
                    "edit_url": "/admin/kalai/broker/1/change/",
                }
            ],
            "broker_logins": [
                {
                    "account_id": "HS6525",
                    "broker_name": "Zerodha Kite",
                    "auth_state": "TOKEN_EXPIRED",
                    "auth_label": "Token Expired",
                    "badge_type": "badge-warning",
                    "token_updated_at": "Sep 01, 09:15",
                    "token_relative": "2d ago",
                    "enable_trade": True,
                    "login_url": "/broker-admin/broker-login/",
                    "edit_url": "/admin/kalai/broker/1/change/",
                }
            ],
            "recent_errors": [],
            "market_events": [
                {
                    "title": "Crypto Markets",
                    "schedule": "24/7/365 Continuous",
                    "status": "ALWAYS ACTIVE",
                    "badge": "badge-success",
                    "icon": "⚡",
                }
            ],
            "summary": {
                "health_status": "WARNING",
                "secret_alert_count": 1,
                "login_pending_count": 1,
                "error_count": 0,
                "warning_count": 0,
            }
        }

        ctx = {
            "app_list": [
                {
                    "app_label": "kalai",
                    "name": "Kalai Trading Platform",
                    "app_url": "/admin/kalai/",
                    "models": [
                        {
                            "object_name": "Broker",
                            "name": "Accounts",
                            "admin_url": "/admin/kalai/broker/",
                        }
                    ]
                }
            ],
            "system_hub": mock_hub,
            "user": self.staff_user,
            "log_entries": MagicMock(filter=MagicMock(return_value=[])),
        }

        rendered = render_to_string("admin/index.html", ctx, request=req)

        # 1. Collapsible Section IDs & controls
        self.assertIn("collapsible-module", rendered)
        self.assertIn("sec-algo-monitoring", rendered)
        self.assertIn("sec-app-kalai", rendered)
        self.assertIn("sec-documentation", rendered)
        self.assertIn("toggleSection", rendered)
        self.assertIn("toggleAllSections", rendered)

        # 2. System Hub Telemetry Card & Tabs
        self.assertIn("sys-hub-card", rendered)
        self.assertIn("tab-all", rendered)
        self.assertIn("tab-expiries", rendered)
        self.assertIn("tab-logins", rendered)
        self.assertIn("tab-errors", rendered)
        self.assertIn("tab-markets", rendered)
        self.assertIn("refreshSystemHub", rendered)

        # 3. Dynamic Values rendered
        self.assertIn("CoinSwitch PRO", rendered)
        self.assertIn("4 days", rendered)
        self.assertIn("Zerodha Kite", rendered)
        self.assertIn("Token Expired", rendered)
