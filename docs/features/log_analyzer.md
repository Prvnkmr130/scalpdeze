# Log Anomaly Analyzer & Operational Diagnostics Workstation

The **Log Anomaly Analyzer** is a high-performance, rule-based forensic diagnostics system built directly into the DeltaZero26 platform. It operates strictly in **DEBUG mode** (`settings.DEBUG = True`) to inspect local database logs (`kalai_algolog` and `system_logs`), identify operational anomalies—such as account and algorithm domain mismatches, cross-account thread context leaks, auth failures, and unhandled exception loops—and generate formatted multi-sheet Excel reports.

---

## 1. Architectural Highlights

* **100% Pluggable & Extensible Rule Registry**: Built around `BaseAnomalyRule` and `RuleRegistry`. New anomaly rules can be added or existing rules removed or disabled at any time without altering core analyzer logic.
* **Strict DEBUG Mode Guardrail**: Execution is strictly restricted to `settings.DEBUG = True`. If invoked when `DEBUG = False` (in production), the analyzer raises `PermissionError` and admin endpoints return `403 Forbidden` to eliminate database load and lock contention on live trading servers.
* **Pure Polars & Zero-Pandas Excel Engine**: Multi-sheet workbook generation uses native `openpyxl` via `write_polars_sheets_to_excel`, adhering to the project's zero-Pandas standard.
* **Dual-Interface Access**: Available both as an interactive **Django Admin Workstation** under **`⚡ Algo Monitoring`** and as a **CLI Management Command** (`python manage.py analyze_logs`).
* **Fast In-Memory Recurring Pattern Detection**: Automatically clusters repetitive exceptions into signature buckets using normalized error regexes to calculate failure burst rates and pinpoint runaway loops.

---

## 2. Detection Rules Matrix

The analyzer ships with 8 built-in anomaly detection rules covering all critical operational boundaries:

| Rule ID | Rule Name | Category | Default Severity | Description & Anomaly Criteria |
| :--- | :--- | :--- | :--- | :--- |
| **`RULE_01`** | **Asset-Class Domain Mismatch** | `Account & Algo Mismatch` | **CRITICAL** | Flags cryptocurrency accounts (`is_crypto=True` or Delta/CoinSwitch/CoinDCX) logged under Indian equity engines (`indian_opt_trde_polars`), or Indian equity accounts logged under Crypto engines. |
| **`RULE_02`** | **Embedded Account Context Mismatch** | `Account & Algo Mismatch` | **HIGH** | Detects when a log message string explicitly mentions an account ID (e.g. `[W1NPY]` or `account '73270496'`) that does not match the database row's foreign key `account_id`, revealing thread-local context bleeding. |
| **`RULE_03`** | **Framework Log Misattribution** | `System & Framework Misattributions` | **HIGH** | Detects framework discovery (`load_all_algos`), background worker scheduling (`qcluster`), or database maintenance logs incorrectly attributed to live trading algorithms. |
| **`RULE_04`** | **Authentication & API Errors** | `API & Authentication Errors` | **CRITICAL** | Captures HTTP 401/403 errors, invalid API keys/secrets, expired tokens, and IP whitelist rejections (e.g. `ip_not_whitelisted_for_api_key`). |
| **`RULE_05`** | **ISO Timestamp Protocol Violation** | `Context & Formatting Violations` | **LOW** | Enforces the system logging standard requiring all log entries to begin with a bracketed ISO timestamp prefix (`[{datetime.now().isoformat()}]`). |
| **`RULE_06`** | **Inactive Account Execution** | `Account Trading State Violations` | **HIGH** | Flags order placement or trade execution events attributed to broker accounts with `enable_trade = False` or orphaned accounts not registered in the database. |
| **`RULE_07`** | **Unhandled Crash / Traceback** | `Engine Errors & Crash Traces` | **HIGH** | Detects unhandled Python stack traces (`Traceback (most recent call last)`), runtime fatal crashes, and unhandled syntax/type errors. |
| **`RULE_08`** | **Ingestion & Dtype Safety Warning** | `Configuration & Ingestion Warnings` | **LOW** | Catches numeric casting warnings (`could not cast`), openpyxl comparisons, or zero-capital underlying instruments loaded into calculation spaces. |

---

## 3. Extending or Modifying Rules

The analyzer is designed for future rule expansion. Developers can create and register custom rules in three simple steps:

### Defining a Custom Rule

```python
from algo_trading.tools.log_analyzer.models import AnomalyRecord, LogEntry, Severity
from algo_trading.tools.log_analyzer.registry import register_rule
from algo_trading.tools.log_analyzer.rules.base import AnalysisContext, BaseAnomalyRule

@register_rule
class CustomSpreadCheckRule(BaseAnomalyRule):
    rule_id = "RULE_09_CUSTOM_SPREAD"
    name = "Abnormal Option Spread Warning"
    category = "Market Data Anomalies"
    severity = Severity.MEDIUM
    description = "Detects bids and asks with bid-ask spread exceeding 50%."

    def evaluate(self, entry: LogEntry, context: AnalysisContext):
        if "spread exceeds" in (entry.message or "").lower():
            return self.create_anomaly(
                entry,
                reason="Bid-ask spread exceeds normal market threshold."
            )
        return None
```

Rules can also be dynamically enabled, disabled, or unregistered at runtime:

```python
from algo_trading.tools.log_analyzer.registry import rule_registry

# Disable a rule without deleting it
rule_registry.disable_rule("RULE_05")

# Re-enable a rule
rule_registry.enable_rule("RULE_05")

# Remove a rule completely
rule_registry.unregister("RULE_09_CUSTOM_SPREAD")
```

---

## 4. Multi-Sheet Excel Report Format

When exporting via the admin UI or CLI command, the analyzer generates a `.xlsx` workbook containing 7 formatted tabs:

1. **`Executive_Summary`**: Overall KPI summary, timeframe window, total logs inspected, operational anomaly count, and percentage breakdowns by severity and category.
2. **`Account_Algo_Mismatches`**: Specific records where crypto and Indian accounts cross-talked, or where thread-local context leaked another account ID.
3. **`Auth_And_API_Errors`**: Detailed audit of all 401/403 responses, IP whitelisting errors, and token expirations.
4. **`System_Misattributions`**: Registry discovery and maintenance logs wrongly assigned to strategy names.
5. **`Context_Thread_Leaks`**: Missing ISO timestamps, thread context leaks, and inactive trading attempts.
6. **`Recurring_Error_Loops`**: Frequency-grouped repeating error signatures with occurrence counts, first/last seen timestamps, and calculated frequency per minute.
7. **`All_Anomalies_Audit`**: Comprehensive chronological master audit log of every detected anomaly.

---

## 5. Django Admin Workstation

The Log Anomaly Analyzer is integrated into the Django Admin under Section 1 (**`⚡ Algo Monitoring`**):

* **Direct URL**: `/admin/kalai/broker/algo-monitoring/log-analyzer/`
* **Export URL**: `/admin/kalai/broker/algo-monitoring/log-analyzer/export/?hours=24`
* **Zero Initial Load Latency**: Opening the workstation loads instantly without running log ingestion or queries. An **On-Demand Launch Card** allows configuring the inspection timeframe (`1h`, `6h`, `12h`, `24h`, `48h`, `7d`), maximum logs limit (`10k`, `25k`, `50k`, `100k`, or `All Records [No Limit]`), and account scope before clicking the **▶ Execute Log Analysis** button.
* **Workstation Features (Post-Execution)**:
  * Real-time KPI summary cards (Logs Inspected, Anomalies Detected, Critical/High Counts, Recurring Error Loops).
  * Timeframe filter pills (`1h`, `6h`, `24h`, `48h`, `7d`) with preserved record limits.
  * Interactive record volume dropdown (`10k`, `25k`, `50k`, `100k`, `All Records [No Limit]`) to switch sample sizes on the fly.
  * Account, Category, and Severity dropdown filters.
  * Collapsible Active Detection Rules specification drawer.
  * Recurring error pattern summary table with rate-per-minute analysis.
  * Color-coded operational anomaly audit table with full message expansion.
  * One-click **📥 Export Multi-Sheet Excel** button (honoring current record volume and timeframe).
  * **🔄 Re-run Analysis** button to re-evaluate on demand.

---

## 6. CLI Management Command Reference

The analyzer can be executed from the terminal or scripts:

```bash
# Analyze past 24 hours and export to Excel (host)
uv run python manage.py analyze_logs --hours=24 --output=log_anomalies.xlsx

# Analyze ALL records without limit (--max-logs=0)
uv run python manage.py analyze_logs --hours=24 --max-logs=0 --output=all_records_audit.xlsx

# Filter specific rules (e.g. asset class mismatches and auth errors only)
uv run python manage.py analyze_logs --hours=6 --rules=RULE_01,RULE_04 --output=auth_anomalies.xlsx

# Run inside Docker container
docker exec algo_trading_monolith python manage.py analyze_logs --hours=24 --max-logs=0 --output=/app/logs/log_anomalies.xlsx

# Force run in non-DEBUG environments (CLI only)
uv run python manage.py analyze_logs --hours=24 --force-debug --output=forensic_audit.xlsx
```
