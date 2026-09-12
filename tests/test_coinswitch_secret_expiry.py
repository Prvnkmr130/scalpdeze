# tests/test_coinswitch_secret_expiry.py
"""
Unit tests for CoinSwitch Secret Key 90-day expiry tracking, configurable validity
and warning thresholds, 5-hour cached context processor, and UI status badges.
"""

from datetime import timedelta
from django.test import TestCase, RequestFactory
from django.utils import timezone
from django.core.cache import cache
from django.contrib.auth.models import User

from kalai.models import Broker, BrokerType, ApiProvider
from kalai.context_processors import (
    get_coinswitch_secret_expiry_alerts,
    coinswitch_expiry_alerts,
    CACHE_KEY_EXPIRY_ALERTS,
)
from kalai.admin import AccountAdmin
from django.contrib.admin.sites import AdminSite


class MockAdminSite(AdminSite):
    pass


class CoinSwitchSecretExpiryTests(TestCase):
    def setUp(self):
        cache.clear()
        self.factory = RequestFactory()
        self.admin_user = User.objects.create_superuser(
            username="testadmin", email="admin@test.com", password="password123"
        )

        self.bt_coinswitch, _ = BrokerType.objects.get_or_create(
            code="coinswitch", defaults={"name": "CoinSwitch PRO"}
        )
        self.ap_coinswitch, _ = ApiProvider.objects.get_or_create(
            code="coinswitch", defaults={"name": "CoinSwitch API"}
        )

        self.bt_zerodha, _ = BrokerType.objects.get_or_create(
            code="zerodha", defaults={"name": "Zerodha Kite"}
        )
        self.ap_zerodha, _ = ApiProvider.objects.get_or_create(
            code="zerodha", defaults={"name": "Zerodha API"}
        )

    def tearDown(self):
        cache.clear()

    def test_auto_timestamp_on_creation(self):
        """When a CoinSwitch broker is created with api_secret, api_secret_updated_at and enable_secret_key_expiry are populated."""
        broker = Broker.objects.create(
            account_id="CS_TEST_01",
            name="coinswitch_acc_1",
            broker_name=self.bt_coinswitch,
            api_provider=self.ap_coinswitch,
            api_key="pub_key_123",
            api_secret="priv_secret_456",
        )
        self.assertIsNotNone(broker.api_secret_updated_at)
        self.assertTrue(broker.is_coinswitch)
        self.assertTrue(broker.enable_secret_key_expiry)
        self.assertEqual(broker.secret_key_validity_days, 90)
        self.assertEqual(broker.secret_key_warn_days, 7)

    def test_non_coinswitch_defaults_to_disabled(self):
        """Non-CoinSwitch brokers default to enable_secret_key_expiry=False."""
        broker = Broker.objects.create(
            account_id="ZD_DEFAULT",
            name="zd_default",
            broker_name=self.bt_zerodha,
            api_provider=self.ap_zerodha,
            api_key="pub_key_zd",
            api_secret="priv_secret_zd",
        )
        self.assertFalse(broker.enable_secret_key_expiry)
        self.assertEqual(broker.secret_key_expiry_status, "NOT_APPLICABLE")
        self.assertIsNone(broker.days_until_secret_key_expiry)
        self.assertIsNone(broker.secret_key_expires_at)

    def test_disabled_expiry_tracking_returns_not_applicable(self):
        """Disabling enable_secret_key_expiry suppresses expiry status and alerts."""
        broker = Broker.objects.create(
            account_id="CS_DISABLED",
            name="cs_disabled",
            broker_name=self.bt_coinswitch,
            api_provider=self.ap_coinswitch,
            api_key="pub_key_123",
            api_secret="priv_secret_456",
            enable_secret_key_expiry=False,
        )
        # Even if 100 days passed, disabled tracking returns NOT_APPLICABLE
        past_100d = timezone.now() - timedelta(days=100)
        Broker.objects.filter(pk=broker.pk).update(
            api_secret_updated_at=past_100d,
            enable_secret_key_expiry=False
        )
        broker.refresh_from_db()
        self.assertEqual(broker.secret_key_expiry_status, "NOT_APPLICABLE")
        self.assertFalse(broker.is_secret_key_expiring_soon)
        self.assertIsNone(broker.days_until_secret_key_expiry)

    def test_auto_timestamp_on_secret_update(self):
        """Updating api_secret updates api_secret_updated_at, while updating other fields retains it."""
        broker = Broker.objects.create(
            account_id="CS_TEST_02",
            name="coinswitch_acc_2",
            broker_name=self.bt_coinswitch,
            api_provider=self.ap_coinswitch,
            api_key="pub_key_123",
            api_secret="priv_secret_original",
        )
        initial_ts = broker.api_secret_updated_at

        # Updating enable_trade should not update api_secret_updated_at
        broker.enable_trade = True
        broker.save()
        broker.refresh_from_db()
        self.assertEqual(broker.api_secret_updated_at, initial_ts)

        # Updating api_secret should refresh api_secret_updated_at
        past_time = timezone.now() - timedelta(days=30)
        Broker.objects.filter(pk=broker.pk).update(api_secret_updated_at=past_time)
        broker.refresh_from_db()
        self.assertEqual(broker.api_secret_updated_at, past_time)

        broker.api_secret = "priv_secret_new"
        broker.save()
        broker.refresh_from_db()
        self.assertGreater(broker.api_secret_updated_at, past_time)

    def test_expiry_status_active(self):
        """Account added 10 days ago with 90-day validity has 80 days remaining -> ACTIVE."""
        broker = Broker.objects.create(
            account_id="CS_ACTIVE",
            name="cs_active",
            broker_name=self.bt_coinswitch,
            api_provider=self.ap_coinswitch,
            api_key="key",
            api_secret="secret",
            secret_key_validity_days=90,
            secret_key_warn_days=7,
        )
        past_10d = timezone.now() - timedelta(days=10)
        Broker.objects.filter(pk=broker.pk).update(api_secret_updated_at=past_10d)
        broker.refresh_from_db()

        self.assertEqual(broker.secret_key_expiry_status, "ACTIVE")
        self.assertFalse(broker.is_secret_key_expiring_soon)
        self.assertAlmostEqual(broker.days_until_secret_key_expiry, 80, delta=1)

    def test_expiry_status_expiring_soon(self):
        """Account added 85 days ago with 90-day validity has 5 days remaining (<= 7) -> EXPIRING_SOON."""
        broker = Broker.objects.create(
            account_id="CS_EXPIRING",
            name="cs_expiring",
            broker_name=self.bt_coinswitch,
            api_provider=self.ap_coinswitch,
            api_key="key",
            api_secret="secret",
            secret_key_validity_days=90,
            secret_key_warn_days=7,
        )
        past_85d = timezone.now() - timedelta(days=85)
        Broker.objects.filter(pk=broker.pk).update(api_secret_updated_at=past_85d)
        broker.refresh_from_db()

        self.assertEqual(broker.secret_key_expiry_status, "EXPIRING_SOON")
        self.assertTrue(broker.is_secret_key_expiring_soon)
        self.assertAlmostEqual(broker.days_until_secret_key_expiry, 5, delta=1)

    def test_expiry_status_expired(self):
        """Account added 92 days ago with 90-day validity has -2 days remaining -> EXPIRED."""
        broker = Broker.objects.create(
            account_id="CS_EXPIRED",
            name="cs_expired",
            broker_name=self.bt_coinswitch,
            api_provider=self.ap_coinswitch,
            api_key="key",
            api_secret="secret",
            secret_key_validity_days=90,
            secret_key_warn_days=7,
        )
        past_92d = timezone.now() - timedelta(days=92)
        Broker.objects.filter(pk=broker.pk).update(api_secret_updated_at=past_92d)
        broker.refresh_from_db()

        self.assertEqual(broker.secret_key_expiry_status, "EXPIRED")
        self.assertTrue(broker.is_secret_key_expiring_soon)
        self.assertLessEqual(broker.days_until_secret_key_expiry, 0)

    def test_configurable_validity_and_warning_thresholds(self):
        """Custom validity (e.g. 30 days) and custom warning lead time (e.g. 14 days)."""
        broker = Broker.objects.create(
            account_id="CS_CUSTOM",
            name="cs_custom",
            broker_name=self.bt_coinswitch,
            api_provider=self.ap_coinswitch,
            api_key="key",
            api_secret="secret",
            secret_key_validity_days=30,
            secret_key_warn_days=14,
        )
        past_20d = timezone.now() - timedelta(days=20)
        Broker.objects.filter(pk=broker.pk).update(api_secret_updated_at=past_20d)
        broker.refresh_from_db()

        # 30 - 20 = 10 days remaining. Since 10 <= 14, status should be EXPIRING_SOON
        self.assertEqual(broker.secret_key_expiry_status, "EXPIRING_SOON")
        self.assertTrue(broker.is_secret_key_expiring_soon)

    def test_context_processor_and_caching(self):
        """Context processor returns alerts for expiring/expired accounts and caches for 5 hours."""
        # 1. Expiring account
        b_expiring = Broker.objects.create(
            account_id="CS_ALERT_01",
            name="cs_alert_01",
            broker_name=self.bt_coinswitch,
            api_provider=self.ap_coinswitch,
            api_key="key1",
            api_secret="secret1",
            secret_key_validity_days=90,
            secret_key_warn_days=7,
        )
        Broker.objects.filter(pk=b_expiring.pk).update(
            api_secret_updated_at=timezone.now() - timedelta(days=86)
        )

        # 2. Active account
        Broker.objects.create(
            account_id="CS_SAFE",
            name="cs_safe",
            broker_name=self.bt_coinswitch,
            api_provider=self.ap_coinswitch,
            api_key="key2",
            api_secret="secret2",
            secret_key_validity_days=90,
            secret_key_warn_days=7,
        )

        # Fetch alerts
        alerts = get_coinswitch_secret_expiry_alerts(force=True)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["account_id"], "CS_ALERT_01")
        self.assertFalse(alerts[0]["is_expired"])
        self.assertEqual(alerts[0]["status"], "EXPIRING_SOON")

        # Test cached value
        cached = cache.get(CACHE_KEY_EXPIRY_ALERTS)
        self.assertIsNotNone(cached)
        self.assertEqual(len(cached), 1)

        # Test context processor output
        req = self.factory.get("/admin/")
        req.user = self.admin_user
        ctx = coinswitch_expiry_alerts(req)
        self.assertTrue(ctx["has_coinswitch_alerts"])
        self.assertEqual(len(ctx["coinswitch_alerts"]), 1)
        self.assertEqual(ctx["coinswitch_expiring_count"], 1)
        self.assertEqual(ctx["coinswitch_expired_count"], 0)

    def test_admin_badge_rendering(self):
        """Admin secret_key_status renders styled HTML badges."""
        admin_obj = AccountAdmin(Broker, MockAdminSite())

        # Expiring account
        b_expiring = Broker.objects.create(
            account_id="CS_BADGE_01",
            name="cs_badge_01",
            broker_name=self.bt_coinswitch,
            api_provider=self.ap_coinswitch,
            api_key="key",
            api_secret="secret",
        )
        Broker.objects.filter(pk=b_expiring.pk).update(
            api_secret_updated_at=timezone.now() - timedelta(days=86)
        )
        b_expiring.refresh_from_db()
        html = admin_obj.secret_key_status(b_expiring)
        self.assertIn("⚠️", html)
        self.assertIn("left", html)

        # Non-CoinSwitch broker
        b_zerodha = Broker.objects.create(
            account_id="ZD_01",
            name="zd_01",
            broker_name=self.bt_zerodha,
            api_provider=self.ap_zerodha,
            api_key="key",
            api_secret="secret",
        )
        zd_html = admin_obj.secret_key_status(b_zerodha)
        self.assertEqual(zd_html, '<span style="color:#64748b;">—</span>')
