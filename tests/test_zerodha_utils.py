"""
tests/test_zerodha_utils.py
───────────────────────────
Unit tests for ZerodhaUtility order management and account wrappers.
"""

from unittest.mock import MagicMock, patch
import pytest
from algo_trading.algos.zerodha_utils import ZerodhaUtility, retry


def test_retry_success():
    call_count = 0
    def mock_fn():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ConnectionError("Transient network failure")
        return "success"

    result = retry(mock_fn, max_retries=3, delay=0.01)
    assert result == "success"
    assert call_count == 2


def test_retry_exhausted_raises():
    def failing_fn():
        raise ValueError("Permanent failure")

    with pytest.raises(ValueError, match="Permanent failure"):
        retry(failing_fn, max_retries=2, delay=0.01)


def test_zerodha_utility_instantiation_mock():
    mock_broker = MagicMock()
    mock_broker.account_id = "HS6525"
    mock_broker.api_key = "test_key"
    mock_broker.access_token = "test_token"

    with patch("kalai.models.Broker.objects.filter") as mock_filter:
        mock_filter.return_value.first.return_value = mock_broker

        util = ZerodhaUtility(account_id="HS6525")
        assert util.account_id == "HS6525"
        assert util.get_access_token() == "test_token"


def test_zerodha_utility_order_placement_mock():
    mock_broker = MagicMock()
    mock_broker.account_id = "HS6525"
    mock_broker.api_key = "test_key"
    mock_broker.access_token = "test_token"

    with patch("kalai.models.Broker.objects.filter") as mock_filter:
        mock_filter.return_value.first.return_value = mock_broker

        util = ZerodhaUtility(account_id="HS6525")
        util.client = MagicMock()
        util.client.place_order.return_value = "240821000123"

        order_id, msg = util.mrk_ordr("INFY", 10, "BUY", exchg="NSE", prod="MIS")
        assert order_id == "240821000123"
        assert msg == "order placed"

        util.client.place_order.assert_called_once_with(
            variety="regular",
            exchange="NSE",
            tradingsymbol="INFY",
            transaction_type="BUY",
            quantity=10,
            product="MIS",
            order_type="MARKET",
            price=None,
            validity="DAY",
        )
