import os
import tempfile
from datetime import datetime, timedelta
import pytest
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
django.setup()

from unittest.mock import MagicMock, patch
from django.test import RequestFactory, override_settings
from django.contrib.admin.sites import AdminSite

from kalai.models import Broker
from kalai.admin import AccountAdmin
from algo_trading.tools.log_analyzer.models import LogEntry, Severity
from algo_trading.tools.log_analyzer.registry import rule_registry
from algo_trading.tools.log_analyzer.rules.base import BaseAnomalyRule, AnalysisContext
from algo_trading.tools.log_analyzer.rules import (
    AssetClassDomainMismatchRule,
    EmbeddedAccountMismatchRule,
    FrameworkLogMisattributionRule,
    AuthenticationApiErrorRule,
    IsoTimestampProtocolRule,
    InactiveAccountTradingRule,
    UnhandledCrashTraceRule,
    IngestionSafetyRule,
)
from algo_trading.tools.log_analyzer.engine import LogAnomalyAnalyzer


# ─── 1. Registry & Extensibility Tests ────────────────────────────────────────

def test_rule_registry_builtins_and_extensibility():
    """Verify registry can register, lookup, enable/disable, and unregister rules dynamically."""
    assert len(rule_registry.get_all_rules()) >= 8

    # Define a new custom rule to demonstrate dynamic extensibility
    class CustomMaintenanceRule(BaseAnomalyRule):
        rule_id = "CUSTOM_TEST_RULE"
        name = "Custom Maintenance Rule"
        category = "Custom Category"
        severity = Severity.HIGH
        description = "Test rule for dynamic addition/removal"

        def evaluate(self, entry: LogEntry, context: AnalysisContext):
            if "CUSTOM_FAIL" in entry.message:
                return self.create_anomaly(entry, "Custom anomaly triggered")
            return None

    # Register dynamic rule
    test_rule = CustomMaintenanceRule()
    rule_registry.register(test_rule)
    assert rule_registry.get_rule("CUSTOM_TEST_RULE") is test_rule
    assert test_rule in rule_registry.get_all_rules()

    # Test rule evaluation
    ctx = AnalysisContext(known_accounts={"ACC1"})
    entry_bad = LogEntry(
        id=1,
        timestamp=datetime.now(),
        algo_name="test_algo",
        tag="TEST",
        level="ERROR",
        message="Failure: CUSTOM_FAIL encountered in worker",
        account_id="ACC1",
    )
    anomaly = test_rule.evaluate(entry_bad, ctx)
    assert anomaly is not None
    assert anomaly.rule_id == "CUSTOM_TEST_RULE"
    assert anomaly.severity == Severity.HIGH.value

    # Disable rule
    rule_registry.disable_rule("CUSTOM_TEST_RULE")
    assert rule_registry.get_rule("CUSTOM_TEST_RULE").enabled is False
    assert test_rule not in rule_registry.get_active_rules()

    # Re-enable rule
    rule_registry.enable_rule("CUSTOM_TEST_RULE")
    assert rule_registry.get_rule("CUSTOM_TEST_RULE").enabled is True

    # Unregister dynamic rule
    rule_registry.unregister("CUSTOM_TEST_RULE")
    assert rule_registry.get_rule("CUSTOM_TEST_RULE") is None


# ─── 2. Rule Detection Logic Tests ────────────────────────────────────────────

def test_rule_01_asset_class_mismatch():
    rule = AssetClassDomainMismatchRule()
    ctx = AnalysisContext(
        known_accounts={"73270496", "W1NPY"},
        crypto_accounts={"73270496"},
        indian_accounts={"W1NPY"},
    )

    # 1. Crypto account on Indian algo -> Anomaly
    entry1 = LogEntry(
        id=101,
        timestamp=datetime.now(),
        algo_name="indian_opt_trde_polars",
        tag="EXEC",
        level="INFO",
        message="[2026-09-05T08:00:00] Ingesting NIFTY contracts",
        account_id="73270496",
        is_crypto=True,
    )
    a1 = rule.evaluate(entry1, ctx)
    assert a1 is not None
    assert a1.rule_id == "RULE_01"
    assert a1.severity == Severity.CRITICAL.value

    # 2. Indian account on Crypto algo -> Anomaly
    entry2 = LogEntry(
        id=102,
        timestamp=datetime.now(),
        algo_name="crypto_opt_trde_polars",
        tag="EXEC",
        level="INFO",
        message="[2026-09-05T08:00:00] Ingesting BTC contracts",
        account_id="W1NPY",
        is_crypto=False,
    )
    a2 = rule.evaluate(entry2, ctx)
    assert a2 is not None
    assert a2.rule_id == "RULE_01"

    # 3. Legitimate pairing -> Clean
    entry3 = LogEntry(
        id=103,
        timestamp=datetime.now(),
        algo_name="crypto_opt_trde_polars",
        tag="EXEC",
        level="INFO",
        message="[2026-09-05T08:00:00] Subscribing to BTC-USDT orderbook",
        account_id="73270496",
        is_crypto=True,
    )
    assert rule.evaluate(entry3, ctx) is None


def test_rule_02_embedded_account_mismatch():
    rule = EmbeddedAccountMismatchRule()
    ctx = AnalysisContext(
        known_accounts={"73270496", "W1NPY"},
    )

    # Message references W1NPY, but log row account is 73270496 -> Anomaly
    entry = LogEntry(
        id=201,
        timestamp=datetime.now(),
        algo_name="crypto_opt_trde_polars",
        tag="ORDER",
        level="INFO",
        message="[2026-09-05T08:00:00] Order placed for account 'W1NPY' with strike 25000",
        account_id="73270496",
    )
    a = rule.evaluate(entry, ctx)
    assert a is not None
    assert a.rule_id == "RULE_02"
    assert "W1NPY" in a.reason


def test_rule_03_framework_misattribution():
    rule = FrameworkLogMisattributionRule()
    ctx = AnalysisContext()

    # Framework discovery log attributed to strategy algo -> Anomaly
    entry = LogEntry(
        id=301,
        timestamp=datetime.now(),
        algo_name="indian_opt_trde_polars",
        tag="DISCOVERY",
        level="INFO",
        message="[2026-09-05T08:00:00] load_all_algos() discovered 4 registered algorithms",
        account_id="UNASSIGNED",
    )
    a = rule.evaluate(entry, ctx)
    assert a is not None
    assert a.rule_id == "RULE_03"


def test_rule_04_auth_and_api_errors():
    rule = AuthenticationApiErrorRule()
    ctx = AnalysisContext()

    # 401 Unauthorized ip whitelist failure
    entry = LogEntry(
        id=401,
        timestamp=datetime.now(),
        algo_name="crypto_opt_trde_polars",
        tag="API_ERROR",
        level="ERROR",
        message="[2026-09-05T08:00:00] HTTP 401 Client Error: {'error': {'code': 'ip_not_whitelisted_for_api_key'}}",
        account_id="73270496",
    )
    a = rule.evaluate(entry, ctx)
    assert a is not None
    assert a.rule_id == "RULE_04"
    assert a.severity == Severity.CRITICAL.value


def test_rule_05_iso_timestamp_protocol():
    rule = IsoTimestampProtocolRule()
    ctx = AnalysisContext()

    # Missing ISO timestamp prefix
    entry_bad = LogEntry(
        id=501,
        timestamp=datetime.now(),
        algo_name="crypto_opt_trde_polars",
        tag="INFO",
        level="INFO",
        message="Raw unformatted log message without timestamp bracket",
        account_id="73270496",
    )
    a = rule.evaluate(entry_bad, ctx)
    assert a is not None
    assert a.rule_id == "RULE_05"

    # Valid ISO timestamp prefix -> Clean
    entry_good = LogEntry(
        id=502,
        timestamp=datetime.now(),
        algo_name="crypto_opt_trde_polars",
        tag="INFO",
        level="INFO",
        message="[2026-09-05T08:00:00.123456] Correctly formatted message",
        account_id="73270496",
    )
    assert rule.evaluate(entry_good, ctx) is None


def test_rule_06_inactive_account_trading():
    rule = InactiveAccountTradingRule()
    ctx = AnalysisContext(
        known_accounts={"DISABLED_ACC"},
        trade_enabled_accounts=set(),
    )

    # Order action on disabled account -> Anomaly
    entry = LogEntry(
        id=601,
        timestamp=datetime.now(),
        algo_name="indian_opt_trde_polars",
        tag="ORDER",
        level="INFO",
        message="[2026-09-05T08:00:00] Placed BUY order for NIFTY 25000 CE 2 lots",
        account_id="DISABLED_ACC",
        enable_trade=False,
    )
    a = rule.evaluate(entry, ctx)
    assert a is not None
    assert a.rule_id == "RULE_06"
    assert a.severity == Severity.HIGH.value


def test_rule_07_unhandled_tracebacks():
    rule = UnhandledCrashTraceRule()
    ctx = AnalysisContext()

    entry = LogEntry(
        id=701,
        timestamp=datetime.now(),
        algo_name="SYSTEM",
        tag="CRITICAL",
        level="CRITICAL",
        message="Traceback (most recent call last):\n  File 'engine.py', line 45, in execute\n    raise KeyError('token')",
        account_id="UNASSIGNED",
    )
    a = rule.evaluate(entry, ctx)
    assert a is not None
    assert a.rule_id == "RULE_07"


def test_rule_08_ingestion_dtype_safety():
    rule = IngestionSafetyRule()
    ctx = AnalysisContext()

    entry = LogEntry(
        id=801,
        timestamp=datetime.now(),
        algo_name="indian_opt_trde_polars",
        tag="CONFIG",
        level="WARNING",
        message="[2026-09-05T08:00:00] Warning: could not cast column tradable to integer in cap_config",
        account_id="W1NPY",
    )
    a = rule.evaluate(entry, ctx)
    assert a is not None
    assert a.rule_id == "RULE_08"


# ─── 3. Engine Guardrail & Execution Tests ────────────────────────────────────

def test_engine_debug_mode_guardrail():
    """Verify that analyzer strictly blocks execution in non-DEBUG environments."""
    with override_settings(DEBUG=False):
        # Must raise PermissionError if force_debug is False
        with pytest.raises(PermissionError, match="restricted to DEBUG mode"):
            LogAnomalyAnalyzer(hours=24, force_debug=False)

        # Allows execution if force_debug is True
        analyzer = LogAnomalyAnalyzer(hours=24, force_debug=True)
        assert analyzer.force_debug is True


def test_engine_analysis_and_excel_generation():
    """Verify analyzer evaluates synthetic logs and generates a multi-sheet Excel file."""
    with override_settings(DEBUG=True):
        analyzer = LogAnomalyAnalyzer(hours=24)

        synthetic_logs = [
            LogEntry(
                id=1,
                timestamp=datetime.now() - timedelta(minutes=10),
                algo_name="indian_opt_trde_polars",  # Mismatch for crypto account!
                tag="ORDER",
                level="INFO",
                message="[2026-09-05T08:10:00] Ingesting NIFTY order for 73270496",
                account_id="73270496",
                broker_code="DELTA",
                is_crypto=True,
            ),
            LogEntry(
                id=2,
                timestamp=datetime.now() - timedelta(minutes=8),
                algo_name="crypto_opt_trde_polars",
                tag="API",
                level="ERROR",
                message="[2026-09-05T08:12:00] HTTP 401: {'error': {'code': 'ip_not_whitelisted_for_api_key'}}",
                account_id="73270496",
                broker_code="DELTA",
                is_crypto=True,
            ),
            LogEntry(
                id=3,
                timestamp=datetime.now() - timedelta(minutes=5),
                algo_name="crypto_opt_trde_polars",
                tag="API",
                level="ERROR",
                message="[2026-09-05T08:15:00] HTTP 401: {'error': {'code': 'ip_not_whitelisted_for_api_key'}}",
                account_id="73270496",
                broker_code="DELTA",
                is_crypto=True,
            ),
        ]

        with patch.object(analyzer, "fetch_logs", return_value=synthetic_logs), \
             patch.object(analyzer, "_build_context", return_value=AnalysisContext(
                 known_accounts={"73270496"},
                 crypto_accounts={"73270496"},
                 indian_accounts=set(),
             )):
            summary = analyzer.run_analysis()
            assert summary.total_logs_analyzed == 3
            assert summary.total_anomalies >= 2
            assert summary.severity_counts[Severity.CRITICAL.value] >= 1

            # Verify dashboard payload format
            payload = analyzer.get_dashboard_payload()
            assert "summary" in payload
            assert "recent_anomalies" in payload
            assert "rules" in payload
            assert payload["summary"]["total_logs"] == 3

            # Verify Excel export
            with tempfile.TemporaryDirectory() as tmp_dir:
                out_file = os.path.join(tmp_dir, "test_log_anomalies.xlsx")
                res_path = analyzer.generate_excel_report(out_file)
                assert os.path.exists(res_path)
                assert os.path.getsize(res_path) > 1000


# ─── 4. Django Admin Views Tests ──────────────────────────────────────────────

def test_admin_log_analyzer_view_debug_permission():
    """Verify admin view enforces DEBUG mode permission."""
    site = AdminSite()
    admin_inst = AccountAdmin(Broker, site)
    rf = RequestFactory()

    # 1. When DEBUG=False -> 403 Forbidden
    with override_settings(DEBUG=False):
        req = rf.get("/admin/kalai/broker/algo-monitoring/log-analyzer/")
        req.user = MagicMock(is_active=True, is_staff=True)
        resp = admin_inst.log_analyzer_view(req)
        assert resp.status_code == 403
        assert b"Debug Mode Required" in resp.content

    # 2. When DEBUG=True without ?run=1 -> Instant load, does not invoke analyzer
    with override_settings(DEBUG=True):
        with patch("kalai.models.Broker.objects") as mock_broker_qs, \
             patch("algo_trading.tools.log_analyzer.engine.LogAnomalyAnalyzer") as mock_analyzer_cls:
            mock_broker_qs.values_list.return_value = ["73270496", "W1NPY"]

            req = rf.get("/admin/kalai/broker/algo-monitoring/log-analyzer/")
            req.user = MagicMock(is_active=True, is_staff=True)
            resp = admin_inst.log_analyzer_view(req)
            assert resp.status_code == 200
            assert b"On-Demand Log Anomaly Diagnostics" in resp.content
            assert b"Execute Log Analysis" in resp.content
            # Verify analyzer was NOT called on page load
            mock_analyzer_cls.assert_not_called()

    # 3. When DEBUG=True with ?run=1 -> Executes analyzer and renders results
    with override_settings(DEBUG=True):
        with patch("algo_trading.tools.log_analyzer.engine.LogAnomalyAnalyzer.get_dashboard_payload") as mock_payload, \
             patch("kalai.models.Broker.objects") as mock_broker_qs:
            mock_payload.return_value = {
                "summary": {
                    "total_logs": 100,
                    "total_anomalies": 2,
                    "critical_count": 1,
                    "high_count": 1,
                    "medium_count": 0,
                    "low_count": 0,
                    "category_counts": {},
                    "top_accounts": {},
                    "top_algos": {},
                    "timeframe_start": "",
                    "timeframe_end": "",
                    "analyzed_at": "2026-09-05 08:00:00",
                },
                "recent_anomalies": [],
                "recurring_patterns": [],
                "rules": [],
                "debug_mode": True,
            }
            mock_broker_qs.values_list.return_value = ["73270496", "W1NPY"]

            req = rf.get("/admin/kalai/broker/algo-monitoring/log-analyzer/?run=1")
            req.user = MagicMock(is_active=True, is_staff=True)
            resp = admin_inst.log_analyzer_view(req)
            assert resp.status_code == 200
            assert b"Logs Inspected" in resp.content
            assert b"Export Multi-Sheet Excel" in resp.content


def test_admin_log_analyzer_export_view():
    """Verify admin export view returns Excel file attachment."""
    site = AdminSite()
    admin_inst = AccountAdmin(Broker, site)
    rf = RequestFactory()

    # 1. When DEBUG=False -> 403 Forbidden
    with override_settings(DEBUG=False):
        req = rf.get("/admin/kalai/broker/algo-monitoring/log-analyzer/export/")
        req.user = MagicMock(is_active=True, is_staff=True)
        resp = admin_inst.log_analyzer_export_view(req)
        assert resp.status_code == 403

    # 2. When DEBUG=True -> FileResponse attachment
    with override_settings(DEBUG=True):
        with patch("algo_trading.tools.log_analyzer.engine.LogAnomalyAnalyzer.run_analysis"), \
             patch("algo_trading.tools.log_analyzer.engine.LogAnomalyAnalyzer.generate_excel_report") as mock_gen:
            with tempfile.NamedTemporaryFile(suffix=".xlsx", prefix="log_anomalies_", delete=False) as tmp:
                tmp.write(b"PK\x03\x04synthetic_excel_content")
                tmp_path = tmp.name

            try:
                mock_gen.return_value = tmp_path
                req = rf.get("/admin/kalai/broker/algo-monitoring/log-analyzer/export/")
                req.user = MagicMock(is_active=True, is_staff=True)
                resp = admin_inst.log_analyzer_export_view(req)
                assert resp.status_code == 200
                assert resp["Content-Type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                assert "log_anomalies_" in resp["Content-Disposition"]
                resp.close()
            finally:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass


def test_log_analyzer_all_records_option():
    """Verify that max_logs=None or max_logs='all' queries all records without slicing."""
    with override_settings(DEBUG=True):
        analyzer = LogAnomalyAnalyzer(hours=24, max_logs=None)
        assert analyzer.max_logs is None

        site = AdminSite()
        admin_inst = AccountAdmin(Broker, site)
        rf = RequestFactory()

        with patch("algo_trading.tools.log_analyzer.engine.LogAnomalyAnalyzer.get_dashboard_payload") as mock_payload, \
             patch("kalai.models.Broker.objects") as mock_broker_qs:
            mock_payload.return_value = {
                "summary": {
                    "total_logs": 100,
                    "total_anomalies": 0,
                    "critical_count": 0,
                    "high_count": 0,
                    "medium_count": 0,
                    "low_count": 0,
                    "anomaly_rate_pct": 0.0,
                    "timeframe_hours": 24.0,
                    "timeframe_start": "",
                    "timeframe_end": "",
                    "analyzed_at": "",
                    "top_categories": {},
                    "top_algos": {},
                },
                "recent_anomalies": [],
                "recurring_patterns": [],
                "rules": [],
                "debug_mode": True,
            }
            mock_broker_qs.values_list.return_value = ["73270496"]

            req = rf.get("/admin/kalai/broker/algo-monitoring/log-analyzer/?run=1&max_logs=all")
            req.user = MagicMock(is_active=True, is_staff=True)
            resp = admin_inst.log_analyzer_view(req)
            assert resp.status_code == 200
            assert b"All Records (No Limit)" in resp.content

