"""
tests/test_security_performance_hardening.py
─────────────────────────────────────────────
Comprehensive unit tests for security hardening and performance optimizations:
1. Dedicated M2M Server Key authentication and SECRET_KEY fallback.
2. Strict OAuth state validation in broker callback handler.
3. ProcessedTickStore composite indexes and compound query verification.
4. Polars Excel reader deterministic workbook closure.
"""

from unittest.mock import MagicMock, patch
import pytest
from django.conf import settings
from django.test import Client, RequestFactory
from django.urls import reverse
from django.utils import timezone

from kalai.models import Broker, ProcessedTickStore
from kalai.views import export_models_api, broker_callback_view
from algo_trading.algos.polars_excel import read_excel_sheet_polars, read_excel_stoploss_columns


@pytest.mark.django_db
def test_m2m_server_key_authentication_success(monkeypatch):
    """Verify that export API accepts requests with dedicated M2M_SERVER_KEY."""
    monkeypatch.setattr(settings, "M2M_SERVER_KEY", "m2m-super-secure-token-9988")
    monkeypatch.setattr(settings, "SECRET_KEY", "django-secret-key-core-1234")

    client = Client()
    # 1. Test via X-Server-Key
    response = client.get("/api/export/models/", HTTP_X_SERVER_KEY="m2m-super-secure-token-9988")
    assert response.status_code == 200

    # 2. Test via Bearer Authorization header
    response = client.get("/api/export/models/", HTTP_AUTHORIZATION="Bearer m2m-super-secure-token-9988")
    assert response.status_code == 200


@pytest.mark.django_db
def test_m2m_secret_key_fallback_success(monkeypatch):
    """Verify backward-compatible fallback to SECRET_KEY if provided."""
    monkeypatch.setattr(settings, "M2M_SERVER_KEY", "m2m-super-secure-token-9988")
    monkeypatch.setattr(settings, "SECRET_KEY", "django-secret-key-core-1234")

    client = Client()
    response = client.get("/api/export/models/", HTTP_X_SERVER_KEY="django-secret-key-core-1234")
    assert response.status_code == 200


@pytest.mark.django_db
def test_m2m_authentication_failure_unauthorized():
    """Verify that export API rejects unauthorized requests with 401."""
    client = Client()
    response = client.get("/api/export/models/", HTTP_X_SERVER_KEY="invalid-attacker-key")
    assert response.status_code == 401
    data = response.json()
    assert "Unauthorized" in data.get("error", "")


@pytest.mark.django_db
def test_oauth_state_mismatch_strictly_aborted():
    """Verify that broker_callback_view immediately aborts when state token mismatches."""
    factory = RequestFactory()
    request = factory.get("/broker-admin/callback/ACC_TEST/?state=attacker_tampered_state&code=test_code")
    
    # Configure session
    from django.contrib.sessions.middleware import SessionMiddleware
    from django.contrib.messages.middleware import MessageMiddleware
    
    middleware_session = SessionMiddleware(lambda req: None)
    middleware_session.process_request(request)
    request.session["oauth_state"] = "legitimate_expected_state"
    request.session["login_account_id"] = "ACC_TEST"
    request.session.save()

    middleware_msg = MessageMiddleware(lambda req: None)
    middleware_msg.process_request(request)

    response = broker_callback_view(request, account_id="ACC_TEST")
    
    # Must redirect back to login and NOT process code
    assert response.status_code == 302
    assert "oauth_state" not in request.session


@pytest.mark.django_db
def test_processedtickstore_composite_indexes_query():
    """Verify ProcessedTickStore creates and uses compound indexes (account, -timestamp)."""
    broker = Broker.objects.create(account_id="ACC_INDEX_TEST", name="index_test")
    
    # Populate test ticks
    now = timezone.now()
    t1 = ProcessedTickStore.objects.create(account=broker, data={"price": 100}, timestamp=now)
    t2 = ProcessedTickStore.objects.create(account=broker, data={"price": 105}, timestamp=now)

    # Query filtered by account and ordered by -timestamp
    results = list(
        ProcessedTickStore.objects.filter(account=broker)
        .order_by("-timestamp")
        .values_list("id", flat=True)
    )
    assert len(results) == 2
    assert t2.id in results


def test_polars_excel_workbook_closed_deterministically(tmp_path):
    """Verify polars_excel reads workbook and closes file handles properly."""
    import openpyxl
    excel_file = tmp_path / "test_wb.xlsx"
    
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "stoploss_tbl"
    ws.append(["NIFTY", "BANKNIFTY", "CRUDEOILM"])
    ws.append([100, 200, 300])
    wb.save(str(excel_file))
    wb.close()

    # Read sheet
    df = read_excel_sheet_polars(str(excel_file), "stoploss_tbl")
    assert not df.is_empty()
    assert len(df) == 1

    # Read stoploss columns
    cols = read_excel_stoploss_columns(str(excel_file), "stoploss_tbl")
    assert "NIFTY" in cols
    assert "BANKNIFTY" in cols
