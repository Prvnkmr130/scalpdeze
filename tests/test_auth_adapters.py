"""
tests/test_auth_adapters.py
───────────────────────────
Unit tests for generalized multi-broker authentication registry, adapters, and callback routing.
"""

from unittest.mock import MagicMock
import pytest
from kalai.auth.registry import BrokerAuthRegistry, get_auth_adapter
from kalai.auth.zerodha import ZerodhaAuthAdapter
from kalai.auth.kotak import KotakNeoAuthAdapter
from kalai.auth.upstox import UpstoxAuthAdapter
from kalai.auth.angel import AngelOneAuthAdapter


def test_auth_registry_discovery():
    registry = BrokerAuthRegistry()
    
    assert isinstance(registry.get_adapter_by_code("zerodha"), ZerodhaAuthAdapter)
    assert isinstance(registry.get_adapter_by_code("kite"), ZerodhaAuthAdapter)
    assert isinstance(registry.get_adapter_by_code("kotak_neo"), KotakNeoAuthAdapter)
    assert isinstance(registry.get_adapter_by_code("kotak"), KotakNeoAuthAdapter)
    assert isinstance(registry.get_adapter_by_code("upstox"), UpstoxAuthAdapter)
    assert isinstance(registry.get_adapter_by_code("angel"), AngelOneAuthAdapter)


def test_auth_registry_mock_broker_resolution():
    mock_zerodha = MagicMock()
    mock_zerodha.broker_name.code = "zerodha"
    mock_zerodha.api_provider = None
    mock_zerodha.name = "my_zerodha_account"

    adapter = get_auth_adapter(mock_zerodha)
    assert isinstance(adapter, ZerodhaAuthAdapter)
    assert adapter.supports_oauth() is True

    mock_kotak = MagicMock()
    mock_kotak.broker_name = None
    mock_kotak.api_provider.code = "kotak_neo"
    mock_kotak.name = "kotak_acc"

    adapter_kotak = get_auth_adapter(mock_kotak)
    assert isinstance(adapter_kotak, KotakNeoAuthAdapter)
    assert adapter_kotak.supports_direct_login() is True


def test_zerodha_login_url_generation():
    adapter = ZerodhaAuthAdapter()
    mock_broker = MagicMock()
    mock_broker.api_key = "test_kite_api_key"
    mock_broker.account_id = "HS6525"
    mock_broker.name = "zerodha_1"
    mock_broker.redirect_url = None

    mock_request = MagicMock()
    callback = "https://mytrader.com/broker-admin/callback/HS6525/"

    url = adapter.get_login_url(mock_broker, mock_request, callback_url=callback)
    assert "https://kite.trade/connect/login" in url
    assert "api_key=test_kite_api_key" in url
    assert "redirect_url=https%3A%2F%2Fmytrader.com%2Fbroker-admin%2Fcallback%2FHS6525%2F" in url


def test_zerodha_login_missing_api_key_raises():
    adapter = ZerodhaAuthAdapter()
    mock_broker = MagicMock()
    mock_broker.api_key = ""
    mock_broker.account_id = "HS6525"
    mock_broker.name = "zerodha_1"

    mock_request = MagicMock()
    with pytest.raises(ValueError, match="API key is missing"):
        adapter.get_login_url(mock_broker, mock_request)


def test_kotak_neo_token_compound_callback():
    adapter = KotakNeoAuthAdapter()
    mock_broker = MagicMock()
    mock_broker.account_id = "KOTAK_USER_1"
    mock_broker.name = "kotak_account"

    mock_request = MagicMock()
    mock_request.GET = {"token": "live_jwt_token", "sid": "live_sid_123"}

    success, msg = adapter.handle_callback(mock_broker, mock_request)
    assert success is True
    assert mock_broker.access_token == "live_jwt_token:::live_sid_123"


def test_broker_auto_callback_url_generation():
    from kalai.models import Broker

    # Test auto population on instantiation
    b = Broker(account_id="HS6525", name="prvn_zerodha")
    b.auto_populate_redirect_url()
    assert b.base_redirect_url is not None
    assert "/broker-admin/callback/HS6525/" in b.redirect_url
    assert b.get_callback_url() == b.redirect_url

    # Test with custom base_redirect_url
    b2 = Broker(account_id="ACC_XYZ", name="acc_xyz", base_redirect_url="https://trading.deltazero.io")
    b2.auto_populate_redirect_url()
    assert b2.redirect_url == "https://trading.deltazero.io/broker-admin/callback/ACC_XYZ/"
    assert b2.get_callback_url() == "https://trading.deltazero.io/broker-admin/callback/ACC_XYZ/"


def test_broker_scoped_token_methods():
    from unittest.mock import patch, MagicMock
    from kalai.models import Broker

    b = Broker(account_id="HS6525", name="prvn_zerodha")
    assert b.token_tablename == "HS6525_inst_tokens"

    # Test set and get tokens strictly bound to broker
    mock_tokens = [256265, 408065]
    with patch("kalai.models.AlgoInfo.create_or_update") as mock_upsert:
        b.set_subscribed_tokens(mock_tokens)
        mock_upsert.assert_called_once_with(
            account=b,
            tablename="HS6525_inst_tokens",
            tabledata={"tokenid": mock_tokens}
        )

    with patch("kalai.models.AlgoInfo.objects.filter") as mock_filter:
        mock_record = MagicMock()
        mock_record.tabledata = {"tokenid": [256265, 408065]}
        mock_filter.return_value.first.return_value = mock_record

        tokens = b.get_subscribed_tokens()
        assert tokens == [256265, 408065]
        mock_filter.assert_called_once_with(account=b, tablename="HS6525_inst_tokens")


def test_algo_info_cross_account_validation_guard():
    import pytest
    from django.core.exceptions import ValidationError
    from kalai.models import Broker, AlgoInfo

    b = Broker(account_id="HS6525", name="prvn_zerodha")

    # Mismatched tablename for tokens should raise ValidationError
    mismatched = AlgoInfo(account=b, tablename="WRONG_ACC_inst_tokens", tabledata={"tokenid": [123]})
    with pytest.raises(ValidationError):
        mismatched.clean()

    # Correct tablename passes clean()
    correct = AlgoInfo(account=b, tablename="HS6525_inst_tokens", tabledata={"tokenid": [123]})
    correct.clean()
    assert correct.tablename == "HS6525_inst_tokens"


def test_clean_totp_secret():
    from kalai.views import clean_totp_secret

    assert clean_totp_secret("JBSW Y3DP EHPK 3PXP") == "JBSWY3DPEHPK3PXP"
    assert clean_totp_secret("jbsw-y3dp-ehpk-3pxp") == "JBSWY3DPEHPK3PXP"
    assert clean_totp_secret("otpauth://totp/Zerodha:user?secret=JBSWY3DPEHPK3PXP&issuer=Zerodha") == "JBSWY3DPEHPK3PXP"
    assert clean_totp_secret("") == ""
    assert clean_totp_secret(None) == ""


def test_get_totp_for_broker_robustness():
    from unittest.mock import patch, MagicMock
    from kalai.views import get_totp_for_broker

    mock_b = MagicMock()
    mock_b.totp_secret = "JBSW Y3DP EHPK 3PXP"
    mock_b.name = "test_zerodha"

    with patch("kalai.views.get_network_time_offset", return_value=0.0):
        totp = get_totp_for_broker(mock_b)
        assert len(totp) == 6
        assert totp.isdigit()

    # Empty secret returns 000000
    mock_b.totp_secret = ""
    assert get_totp_for_broker(mock_b) == "000000"

    mock_b.totp_secret = None
    assert get_totp_for_broker(mock_b) == "000000"


def test_kotak_neo_direct_login_missing_fields():
    adapter = KotakNeoAuthAdapter()
    mock_b = MagicMock()
    mock_b.account_id = "W1NPY"
    mock_b.name = "kotak_test"
    mock_b.api_key = ""
    mock_b.api_secret = "123456"
    mock_b.refresh_token = "9876543210"
    mock_b.get_totp.return_value = "112233"

    # Missing API Key (Consumer key)
    success, msg = adapter.handle_direct_login(mock_b)
    assert success is False
    assert "Missing Consumer Key" in msg

    # Missing Mobile Number
    mock_b.api_key = "test_consumer_key"
    mock_b.refresh_token = ""
    mock_b.account_id = "W1NPY"
    success, msg = adapter.handle_direct_login(mock_b)
    assert success is False
    assert "Missing Registered Mobile Number" in msg


def test_kotak_neo_direct_login_rest_success():
    from unittest.mock import patch
    adapter = KotakNeoAuthAdapter()
    mock_b = MagicMock()
    mock_b.account_id = "W1NPY"
    mock_b.name = "kotak_test"
    mock_b.api_key = "my_consumer_key"
    mock_b.api_secret = "654321"
    mock_b.refresh_token = "+919876543210"
    mock_b.get_totp.return_value = "123456"

    with patch.object(adapter, "_direct_http_login", return_value=(True, "mock_jwt_token", "mock_sid_xyz")):
        success, msg = adapter.handle_direct_login(mock_b)
        assert success is True
        assert "Kotak Neo login successful" in msg
        assert mock_b.access_token == "mock_jwt_token:::mock_sid_xyz"


def test_delta_exchange_adapter_and_direct_login():
    from kalai.auth.delta import DeltaExchangeAuthAdapter
    registry = BrokerAuthRegistry()
    adapter = registry.get_adapter_by_code("delta_exchange")
    assert isinstance(adapter, DeltaExchangeAuthAdapter)
    assert adapter.supports_direct_login() is True
    assert adapter.supports_oauth() is False

    mock_b = MagicMock()
    mock_b.account_id = "delta_1"
    mock_b.name = "delta_main"
    mock_b.api_key = "delta_api_key_12345"
    mock_b.api_secret = "delta_api_secret_67890"

    success, msg = adapter.handle_direct_login(mock_b)
    assert success is True
    assert "Delta Exchange credentials verified" in msg
    assert "hmac256::" in mock_b.access_token





