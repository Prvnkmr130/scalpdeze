import pytest
from django.contrib.admin.sites import AdminSite
from django.test import RequestFactory
from django.contrib.messages.storage.fallback import FallbackStorage
from kalai.models import AlgoInfo, Broker, BrokerType
from kalai.admin import AlgoInfoAdmin


@pytest.mark.django_db
def test_algoinfo_pinning_and_ordering():
    broker_type = BrokerType.objects.create(code="zerodha_test", name="Zerodha Test")
    broker = Broker.objects.create(account_id="ACC_PIN_TEST", name="Pin Test", broker_name=broker_type)

    # Create 3 tables:
    t1 = AlgoInfo.objects.create(account=broker, tablename="table_alpha", tabledata={"data": 1}, is_pinned=False)
    t2 = AlgoInfo.objects.create(account=broker, tablename="table_beta", tabledata={"data": 2}, is_pinned=False)
    t3 = AlgoInfo.objects.create(account=broker, tablename="table_gamma", tabledata={"data": 3}, is_pinned=True)

    # Query all AlgoInfo: t3 (pinned) should be first, followed by t2 (more recent than t1), then t1
    results = list(AlgoInfo.objects.filter(account=broker))
    assert len(results) == 3
    assert results[0].id == t3.id
    assert results[0].is_pinned is True
    assert results[1].id == t2.id
    assert results[2].id == t1.id

    # Test admin actions with RequestFactory
    site = AdminSite()
    admin_inst = AlgoInfoAdmin(AlgoInfo, site)
    factory = RequestFactory()
    req = factory.get("/admin/kalai/algoinfo/")
    setattr(req, "session", {})
    messages = FallbackStorage(req)
    setattr(req, "_messages", messages)

    # Unpin t3 via action
    admin_inst.unpin_selected(req, AlgoInfo.objects.filter(id=t3.id))
    t3.refresh_from_db()
    assert t3.is_pinned is False

    # Pin t1 via action
    admin_inst.pin_selected(req, AlgoInfo.objects.filter(id=t1.id))
    t1.refresh_from_db()
    assert t1.is_pinned is True

    # Now t1 should be first in queryset
    results_after = list(AlgoInfo.objects.filter(account=broker))
    assert results_after[0].id == t1.id

