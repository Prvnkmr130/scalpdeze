import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
django.setup()

from unittest.mock import patch
from django.test import TestCase, RequestFactory
from django.contrib.auth.models import User
from kalai.models import Broker, BrokerType, ApiProvider
from kalai.views import broker_login_view, default_totp_view

TEST_ACCOUNT_IDS = {"TEST_HS6525", "TEST_GUIDE_1"}


class BrokerLoginViewsTestCase(TestCase):
    def setUp(self):
        # Only clean up test accounts, NEVER live broker records
        User.objects.filter(username="staff_user").delete()
        Broker.objects.filter(account_id__in=TEST_ACCOUNT_IDS).delete()

        self.factory = RequestFactory()
        self.staff_user = User.objects.create_user(
            username="staff_user",
            password="password",
            is_staff=True,
            is_superuser=True,
        )
        self.bt_zerodha, _ = BrokerType.objects.get_or_create(code="zerodha", defaults={"name": "Zerodha"})
        self.ap_zerodha, _ = ApiProvider.objects.get_or_create(code="zerodha", defaults={"name": "Zerodha API"})

    def tearDown(self):
        Broker.objects.filter(account_id__in=TEST_ACCOUNT_IDS).delete()
        User.objects.filter(username="staff_user").delete()

    def test_broker_login_view_empty_state_does_not_seed(self):
        """When no accounts exist, broker_login_view must not create mock accounts and show empty state."""
        with patch("kalai.views.Broker.objects.all") as mock_all:
            mock_all.return_value = Broker.objects.none()

            req = self.factory.get("/broker-admin/broker-login/")
            req.user = self.staff_user
            req.session = {}

            response = broker_login_view(req)
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"No broker accounts found.", response.content)
            self.assertIn(b"+ Add Account in Admin", response.content)

    def test_default_totp_view_empty_state_does_not_seed(self):
        """When no accounts exist, default_totp_view must not create mock accounts and show empty state."""
        with patch("kalai.views.Broker.objects.exclude") as mock_exclude:
            mock_exclude.return_value = Broker.objects.none()

            req = self.factory.get("/broker-admin/default-totp/")
            req.user = self.staff_user
            req.session = {}

            response = default_totp_view(req)
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"No accounts found with a configured TOTP key", response.content)

    def test_broker_login_view_with_real_account(self):
        """When accounts exist, broker_login_view displays them without any mock accounts."""
        b = Broker.objects.create(
            name="test_live_zerodha",
            account_id="TEST_HS6525",
            broker_name=self.bt_zerodha,
            api_provider=self.ap_zerodha,
            api_key="real_api_key",
            api_secret="real_api_secret",
        )

        req = self.factory.get("/broker-admin/broker-login/")
        req.user = self.staff_user
        req.session = {}

        response = broker_login_view(req)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"TEST_HS6525", response.content)

    def test_credential_guide_routes_canonical_and_with_id(self):
        """Credential guide view should resolve both at canonical and instance-specific URLs."""
        from django.test import Client
        broker = Broker.objects.create(
            name="test_guide_zerodha",
            account_id="TEST_GUIDE_1",
            broker_name=self.bt_zerodha,
            api_provider=self.ap_zerodha,
            api_key="real_api_key",
            api_secret="real_api_secret",
        )
        client = Client()
        client.force_login(self.staff_user)

        from algo_trading.urls import ADMIN_PATH

        # Canonical path
        canonical_url = f"/{ADMIN_PATH}/kalai/broker/credential-guide/"
        resp_canonical = client.get(canonical_url, HTTP_HOST="localhost")
        self.assertEqual(resp_canonical.status_code, 200)
        self.assertIn(b"Multi-Broker Setup &amp; Credential Mapping Guide", resp_canonical.content)

        # Instance-specific path (e.g. clicked from broker change view)
        instance_url = f"/{ADMIN_PATH}/kalai/broker/{broker.pk}/credential-guide/"
        resp_instance = client.get(instance_url, HTTP_HOST="localhost")
        self.assertEqual(resp_instance.status_code, 200)
        self.assertIn(b"Multi-Broker Setup &amp; Credential Mapping Guide", resp_instance.content)
        self.assertIn(f"Back to {broker.name}".encode(), resp_instance.content)


