import json
import orjson
from datetime import datetime, date
from django.conf import settings
from django.contrib import admin
from django.db import models
from django.forms import widgets
import os
from django.http import HttpResponseForbidden, JsonResponse, FileResponse
from django.utils.html import format_html, mark_safe
from django.urls import path
from django.shortcuts import render
from django import forms
from kalai.models import (
    ProcessedTickStore,
    AlgoInfo,
    ExchangeMasterData,
    Broker,
    BrokerType,
    ApiProvider,
    AlgoLog,
    BrokerPosition,
    TradeRecord,
    DailyPnLSnapshot,
)

class AccountAdminForm(forms.ModelForm):
    class Meta:
        model = Broker
        fields = "__all__"
        widgets = {
            "account_id": forms.TextInput(attrs={"placeholder": "e.g. HS6525 (Zerodha) / W1NPY (Kotak Neo) / delta_user / PR45134584 (CoinDCX)"}),
            "name": forms.TextInput(attrs={"placeholder": "e.g. prvn_zerodha, w1npy_kotak, delta_main, coindcx_main"}),
            "api_key": forms.TextInput(attrs={"placeholder": "API Key (Delta Exchange/Zerodha/Crypto) / Consumer Key (Kotak Neo)"}),
            "api_secret": forms.TextInput(attrs={"placeholder": "API Secret (Delta Exchange HMAC/Zerodha) / 6-digit Neo MPIN"}),
            "totp_secret": forms.TextInput(attrs={"placeholder": "32-character TOTP 2FA Secret Key (Zerodha / Kotak Neo)"}),
            "access_token": forms.Textarea(attrs={"placeholder": "Active Session / Bearer / OAuth Token (auto-updated upon login)", "rows": 2}),
            "refresh_token": forms.TextInput(attrs={"placeholder": "Registered Mobile (+919876543210) for Kotak Neo / Refresh Token"}),
        }
        help_texts = {
            "account_id": "Primary account identifier: Zerodha Kite User ID (e.g. HS6525), Kotak Neo Client ID/UCC (e.g. W1NPY), or Crypto Account ID.",
            "api_key": "API Key for Zerodha/Crypto, or Consumer/Customer Key from Kotak Neo Developer Portal.",
            "api_secret": "API Secret for Zerodha/Crypto, or 6-digit login MPIN for Kotak Neo.",
            "totp_secret": "TOTP 2FA Secret Key (32-character base32 seed) for automated headless login in Zerodha & Kotak Neo.",
            "access_token": "Active daily session/bearer token (for Kotak Neo: <bearer_token>:::<sid>, auto-populated on login).",
            "refresh_token": "10-digit Registered Mobile Number with country code (+91...) for Kotak Neo 2FA, or Refresh Token for OAuth brokers.",
        }

class PrettyJSONWidget(widgets.Textarea):
    def format_value(self, value):
        try:
            if value is None:
                return ""
            if isinstance(value, str):
                if len(value) > 200000:
                    return value[:200000] + f"\n\n... [Truncated: {len(value):,} total characters in admin viewer] ..."
                value = json.loads(value)
            if isinstance(value, list) and len(value) > 200:
                summary = f"/* Large Dataset: {len(value):,} items. Showing first 100 items */\n"
                return summary + json.dumps(value[:100], indent=4) + "\n\n... [Remaining items truncated in admin viewer] ..."
            formatted = json.dumps(value, indent=4, sort_keys=True)
            if len(formatted) > 500000:
                return formatted[:500000] + f"\n\n... [Truncated: {len(formatted):,} total characters in admin viewer] ..."
            return formatted
        except Exception:
            return super().format_value(value)


# ─── Account / Broker Admin ───────────────────────────────────────────────────

@admin.register(Broker)
class AccountAdmin(admin.ModelAdmin):
    form = AccountAdminForm
    list_display = (
        "account_id",
        "name",
        "broker_name",
        "api_provider",
        "api_key_masked",
        "token_status",
        "secret_key_status",
        "enable_trade",
        "enable_websocket",
        "enable_schedule",
        "access_token_required",
        "access_token_updated_at",
        "api_secret_updated_at",
        "created_at",
    )
    list_filter = ("broker_name", "api_provider", "enable_trade", "enable_websocket", "enable_schedule", "enable_secret_key_expiry")
    search_fields = ("account_id", "name", "api_key")
    readonly_fields = ("created_at", "access_token_updated_at", "api_secret_updated_at", "callback_url_display")
    ordering = ("name",)

    fieldsets = (
        ("Account Identity", {
            "fields": ("account_id", "name", "broker_name", "api_provider"),
        }),
        ("API Credentials", {
            "fields": (
                "api_key",
                "api_secret",
                "totp_secret",
                "access_token",
                "refresh_token",
            ),
            "description": "Enter credentials according to your broker. Dynamic hints will appear below each field."
        }),
        ("Credential Validity & Expiry Tracking", {
            "fields": (
                "enable_secret_key_expiry",
                "secret_key_validity_days",
                "secret_key_warn_days",
                "api_secret_updated_at",
            ),
            "description": "Configure secret key validity lifecycle (default: 90 days for CoinSwitch) and warning notification lead time."
        }),
        ("OAuth Callback URL", {
            "fields": ("callback_url_display",),
            "description": "For OAuth brokers (Zerodha Kite, Upstox): copy and paste this URL into your developer portal as the Redirect/Callback URL. Not required for Kotak Neo Direct 2FA or Crypto APIs."
        }),
        ("Advanced URL Overrides", {
            "fields": ("api_endpoint", "redirect_url", "base_redirect_url"),
            "classes": ("collapse",),
            "description": "Optional: Override default API endpoints or base redirect domains."
        }),
        ("Settings", {
            "fields": ("enable_trade", "enable_websocket", "access_token_required"),
        }),
        ("WebSocket Schedule", {
            "fields": ("enable_schedule", "ws_start_time", "ws_stop_time", "ws_operating_days"),
        }),
        ("Timestamps", {
            "fields": ("access_token_updated_at", "created_at"),
        }),
    )

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "credential-guide/",
                self.admin_site.admin_view(self.credential_guide_view),
                name="broker_credential_guide"
            ),
            path(
                "<path:object_id>/credential-guide/",
                self.admin_site.admin_view(self.credential_guide_view),
                name="broker_object_credential_guide"
            ),
            path(
                "algo-status/",
                self.admin_site.admin_view(self.algo_status_view),
                name="algo_status"
            ),
            path(
                "positions-pnl/",
                self.admin_site.admin_view(self.positions_pnl_view),
                name="positions_pnl"
            ),
            path(
                "data-hub/",
                self.admin_site.admin_view(self.data_hub_view),
                name="data_hub"
            ),
            path(
                "algo-monitoring/",
                self.admin_site.admin_view(self.algo_monitoring_dashboard_view),
                name="algo_monitoring"
            ),
            path(
                "algo-monitoring/log-analyzer/",
                self.admin_site.admin_view(self.log_analyzer_view),
                name="algo_log_analyzer"
            ),
            path(
                "algo-monitoring/log-analyzer/export/",
                self.admin_site.admin_view(self.log_analyzer_export_view),
                name="algo_log_analyzer_export"
            ),
            path(
                "candle-visualizer/",
                self.admin_site.admin_view(self.candle_visualizer_view),
                name="candle_visualizer"
            ),
            path(
                "candle-visualizer/api/data/",
                self.admin_site.admin_view(self.candle_visualizer_api_view),
                name="candle_visualizer_api"
            ),
            path(
                "deployment-hub/",
                self.admin_site.admin_view(self.deployment_hub_view),
                name="deployment_hub"
            ),
        ]
        return custom_urls + urls

    def algo_monitoring_dashboard_view(self, request):
        algos_data, summary = evaluate_all_algos_status()
        from kalai.positions import compute_pnl_analytics
        pnl_analytics = compute_pnl_analytics(period_code="today")
        from kalai.models import ExchangeMasterData
        masters_count = ExchangeMasterData.objects.count()

        context = {
            **self.admin_site.each_context(request),
            "title": "Algo Monitoring & Operations Hub",
            "opts": self.model._meta,
            "algos": algos_data,
            "algo_summary": summary,
            "pnl_analytics": pnl_analytics,
            "masters_count": masters_count,
        }
        return render(request, "admin/kalai/algo_monitoring_hub.html", context)

    def credential_guide_view(self, request, object_id=None):
        broker = None
        if object_id:
            try:
                broker = self.model.objects.filter(pk=object_id).first()
            except Exception:
                broker = None

        context = {
            **self.admin_site.each_context(request),
            "title": "Multi-Broker Setup & Credential Mapping Guide",
            "opts": self.model._meta,
            "broker": broker,
        }
        return render(request, "admin/kalai/broker/credential_guide.html", context)

    def algo_status_view(self, request):
        algos_data, summary = evaluate_all_algos_status()
        context = {
            **self.admin_site.each_context(request),
            "title": "Algorithm Status & Execution Monitor",
            "opts": self.model._meta,
            "algos": algos_data,
            "summary": summary,
        }
        return render(request, "admin/kalai/algo_status.html", context)

    def positions_pnl_view(self, request):
        from kalai.positions import compute_pnl_analytics
        from datetime import datetime

        period = request.GET.get("period", "this_month")
        account_id = request.GET.get("account_id", "ALL")
        start_date_str = request.GET.get("start_date", "")
        end_date_str = request.GET.get("end_date", "")

        start_date = None
        end_date = None
        if start_date_str:
            try:
                start_date = datetime.strptime(start_date_str.strip(), "%Y-%m-%d").date()
            except ValueError:
                pass
        if end_date_str:
            try:
                end_date = datetime.strptime(end_date_str.strip(), "%Y-%m-%d").date()
            except ValueError:
                pass

        analytics = compute_pnl_analytics(
            account_id=account_id,
            period_code=period,
            start_date=start_date,
            end_date=end_date,
        )

        context = {
            **self.admin_site.each_context(request),
            "title": "Positions & P&L Analytics",
            "opts": self.model._meta,
            "analytics": analytics,
            "selected_period": period,
            "selected_account": account_id,
            "start_date_val": start_date_str,
            "end_date_val": end_date_str,
        }
        return render(request, "admin/kalai/positions_pnl.html", context)

    def data_hub_view(self, request):
        from kalai.views import _resolve_remote_host
        import re
        remote_host = _resolve_remote_host()
        ssl_verify = getattr(settings, "SSL_VERIFY", True)
        is_ip_host = bool(re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", (remote_host or "").strip()))
        context = {
            **self.admin_site.each_context(request),
            "title": "Data Hub & Cloud Synchronizer",
            "opts": self.model._meta,
            "brokers": Broker.objects.all().order_by("name"),
            "remote_db_host": remote_host or "",
            "ssl_verify": ssl_verify,
            "is_ip_host": is_ip_host,
        }
        return render(request, "admin/kalai/data_hub.html", context)

    def deployment_hub_view(self, request):
        from kalai.deployment import get_git_metadata, is_deployment_active
        from kalai.views import _resolve_remote_host

        git_info = get_git_metadata()
        remote_host = _resolve_remote_host()
        active_deploy = is_deployment_active()

        context = {
            **self.admin_site.each_context(request),
            "title": "🚀 Cloud Deployment & Git Operations Hub",
            "opts": self.model._meta,
            "git_info": git_info,
            "remote_db_host": remote_host or "",
            "is_deploy_active": active_deploy,
        }
        return render(request, "admin/kalai/deployment_hub.html", context)

    def log_analyzer_view(self, request):
        """Interactive Log Anomaly Analyzer Workstation in Django Admin (On-Demand Execution)."""
        if not getattr(settings, "DEBUG", False):
            return HttpResponseForbidden(
                "<h2>403 Forbidden — Debug Mode Required</h2>"
                "<p>Log Anomaly Analyzer is restricted strictly to DEBUG mode (<code>settings.DEBUG = True</code>).</p>"
            )

        from algo_trading.tools.log_analyzer.registry import rule_registry
        from algo_trading.tools.log_analyzer.engine import LogAnomalyAnalyzer

        hours_param = request.GET.get("hours", "24")
        try:
            hours = float(hours_param)
        except (ValueError, TypeError):
            hours = 24.0

        max_logs_param = request.GET.get("max_logs", "50000").strip().lower()
        if max_logs_param in ("all", "0", "none", "unlimited"):
            max_logs = None
            max_logs_display = "all"
        else:
            try:
                max_logs = int(max_logs_param)
                max_logs_display = str(max_logs)
            except (ValueError, TypeError):
                max_logs = 50000
                max_logs_display = "50000"

        rules_filter = request.GET.get("rules", "").strip()
        active_rules = [r.strip() for r in rules_filter.split(",") if r.strip()] if rules_filter else None

        # Analysis is executed strictly on demand via run=1 or POST
        should_run = request.GET.get("run") in {"1", "true", "yes"} or request.method == "POST"

        payload = None
        summary = None
        recent_anomalies = []
        recurring_patterns = []

        if should_run:
            analyzer = LogAnomalyAnalyzer(
                hours=hours,
                max_logs=max_logs,
                active_rules=active_rules,
            )
            payload = analyzer.get_dashboard_payload()
            summary = payload["summary"]
            recent_anomalies = payload["recent_anomalies"]
            recurring_patterns = payload["recurring_patterns"]

            filter_account = request.GET.get("account_id", "").strip()
            filter_category = request.GET.get("category", "").strip()
            filter_severity = request.GET.get("severity", "").strip()

            if filter_account:
                recent_anomalies = [a for a in recent_anomalies if a.get("account_id") == filter_account]
            if filter_category:
                recent_anomalies = [a for a in recent_anomalies if a.get("category") == filter_category]
            if filter_severity:
                recent_anomalies = [a for a in recent_anomalies if a.get("severity") == filter_severity]
        else:
            filter_account = ""
            filter_category = ""
            filter_severity = ""

        # Fetch registered rule metadata for the UI
        rules_list = [
            {
                "rule_id": r.rule_id,
                "name": r.name,
                "category": r.category,
                "severity": r.severity.value,
                "description": r.description,
                "enabled": r.enabled,
            }
            for r in rule_registry.get_all_rules()
        ]

        context = {
            **self.admin_site.each_context(request),
            "title": "🔍 Log Anomaly Analyzer & Operational Diagnostics",
            "opts": self.model._meta,
            "has_analyzed": should_run,
            "payload": payload,
            "summary": summary,
            "recent_anomalies": recent_anomalies,
            "recurring_patterns": recurring_patterns,
            "rules": rules_list,
            "selected_hours": hours_param,
            "max_logs": max_logs_display,
            "filter_account": filter_account,
            "filter_category": filter_category,
            "filter_severity": filter_severity,
            "all_accounts": list(Broker.objects.values_list("account_id", flat=True)),
        }
        return render(request, "admin/kalai/log_analyzer.html", context)

    def log_analyzer_export_view(self, request):
        """Export comprehensive log anomalies report as multi-sheet Excel file."""
        if not getattr(settings, "DEBUG", False):
            return HttpResponseForbidden(
                "<h2>403 Forbidden — Debug Mode Required</h2>"
                "<p>Log Anomaly Analyzer is restricted strictly to DEBUG mode (<code>settings.DEBUG = True</code>).</p>"
            )

        from algo_trading.tools.log_analyzer.engine import LogAnomalyAnalyzer

        hours_param = request.GET.get("hours", "24")
        try:
            hours = float(hours_param)
        except (ValueError, TypeError):
            hours = 24.0

        max_logs_param = request.GET.get("max_logs", "50000").strip().lower()
        if max_logs_param in ("all", "0", "none", "unlimited"):
            max_logs = None
        else:
            try:
                max_logs = int(max_logs_param)
            except (ValueError, TypeError):
                max_logs = 50000

        rules_filter = request.GET.get("rules", "").strip()
        active_rules = [r.strip() for r in rules_filter.split(",") if r.strip()] if rules_filter else None

        analyzer = LogAnomalyAnalyzer(hours=hours, max_logs=max_logs, active_rules=active_rules)
        analyzer.run_analysis()

        target_dir = os.path.join(settings.BASE_DIR, "logs", "exports")
        os.makedirs(target_dir, exist_ok=True)
        ts_label = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_file = os.path.join(target_dir, f"log_anomalies_{ts_label}.xlsx")

        final_path = analyzer.generate_excel_report(out_file)

        return FileResponse(
            open(final_path, "rb"),
            as_attachment=True,
            filename=os.path.basename(final_path),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def _get_symbol_map_for_broker(self, broker):
        """
        Extract comprehensive token-to-symbol mappings from ExchangeMasterData (cum_table, aug_table,
        nfo_cds_mcx, etc.) and auxiliary AlgoInfo master datasets for the given broker.
        Maps instrument_token (str) -> tradingsymbol (str).
        """
        symbol_map = {}
        if not broker:
            return symbol_map

        masters = []
        # 1. Primary: cum_table / aug_table from broker
        if hasattr(broker, "get_master_data"):
            for m_name in ["cum_table", "aug_table", "nfo_cds_mcx", "coindcx_market_details", "coindcx_instruments"]:
                try:
                    m_obj = broker.get_master_data(m_name)
                    if m_obj and m_obj not in masters:
                        masters.append(m_obj)
                except Exception:
                    pass

        # 2. Inspect ExchangeMasterData records for this broker and global
        try:
            if hasattr(broker, "pk") and isinstance(broker.pk, int):
                masters.extend(list(ExchangeMasterData.objects.filter(account=broker)))
            else:
                masters.extend(list(ExchangeMasterData.objects.filter(account=None)))
        except Exception:
            pass

        for master in masters:
            if not master:
                continue
            data = getattr(master, "tabledata", master)
            if not data:
                continue
            if isinstance(data, str):
                try:
                    data = orjson.loads(data)
                except Exception:
                    try:
                        data = json.loads(data)
                    except Exception:
                        data = []
            if isinstance(data, dict):
                if "cum_table" in data and isinstance(data["cum_table"], list):
                    data = data["cum_table"]
                elif "aug_table" in data and isinstance(data["aug_table"], list):
                    data = data["aug_table"]
                elif data:
                    first_val = next(iter(data.values()))
                    if isinstance(first_val, list):
                        length = len(first_val)
                        keys = list(data.keys())
                        data = [{k: data[k][i] for k in keys if i < len(data[k])} for i in range(length)]
                    else:
                        data = [data]
            if isinstance(data, list):
                for row in data:
                    if isinstance(row, dict):
                        # Direct token -> symbol mapping
                        tkn = (
                            row.get("instrument_token")
                            or row.get("Token")
                            or row.get("Token_No")
                            or row.get("token")
                            or row.get("tokenid")
                            or row.get("token_id")
                            or row.get("exchange_token")
                        )
                        sym = (
                            row.get("tradingsymbol")
                            or row.get("Tradingsymbol")
                            or row.get("Symbol")
                            or row.get("symbol")
                            or row.get("name")
                            or row.get("Stock")
                            or row.get("stock_name")
                            or row.get("opt_symbol")
                            or row.get("CompanyName")
                        )
                        if tkn is not None and sym and str(sym).strip():
                            symbol_map[str(tkn)] = str(sym).strip()

                        # Index_tkn -> Nifty_index / Symbol / name / CompanyName
                        idx_tkn = row.get("Index_tkn")
                        idx_sym = row.get("Nifty_index") or row.get("Symbol") or row.get("name") or row.get("CompanyName")
                        if idx_tkn is not None and idx_sym and str(idx_sym).strip() and str(idx_tkn) not in symbol_map:
                            symbol_map[str(idx_tkn)] = str(idx_sym).strip()

                        # Ref_stock_tkn -> Ref_stock
                        ref_tkn = row.get("Ref_stock_tkn")
                        ref_sym = row.get("Ref_stock") or row.get("Stock") or row.get("Symbol")
                        if ref_tkn is not None and ref_sym and str(ref_sym).strip() and str(ref_tkn) not in symbol_map:
                            symbol_map[str(ref_tkn)] = str(ref_sym).strip()

                        # Kotak / MCX / NSE master keys: pSymbol / pAssetCode -> pTrdSymbol / pSymbolName
                        p_sym = row.get("pSymbol") or row.get("lExchangeToken") or row.get("pAssetToken") or row.get("iToken") or row.get("pAssetCode")
                        p_trd = row.get("pTrdSymbol") or row.get("pSymbolName") or row.get("pInstName")
                        if p_sym is not None and p_trd and str(p_trd).strip() and str(p_sym) not in symbol_map:
                            symbol_map[str(p_sym)] = str(p_trd).strip()

        # 3. Inspect auxiliary master datasets in AlgoInfo (excluding _all candle tables)
        try:
            if hasattr(broker, "pk") and isinstance(broker.pk, int):
                algo_records = list(AlgoInfo.objects.filter(account=broker))
            else:
                algo_records = []
            for a in algo_records:
                if a.tablename.endswith("_all"):
                    continue
                td = a.tabledata
                if not td:
                    continue
                if isinstance(td, str):
                    try:
                        td = orjson.loads(td)
                    except Exception:
                        continue
                rows = td if isinstance(td, list) else (list(td.values())[0] if isinstance(td, dict) and isinstance(list(td.values())[0], list) else ([td] if isinstance(td, dict) else []))
                for r in rows:
                    if isinstance(r, dict):
                        tkn = r.get("instrument_token") or r.get("token") or r.get("pSymbol") or r.get("pAssetCode") or r.get("Token")
                        sym = r.get("tradingsymbol") or r.get("pTrdSymbol") or r.get("pSymbolName") or r.get("Symbol") or r.get("name") or r.get("symbol")
                        if tkn is not None and sym and str(sym).strip():
                            tkn_str = str(tkn)
                            if tkn_str not in symbol_map:
                                symbol_map[tkn_str] = str(sym).strip()
        except Exception:
            pass

        return symbol_map

    def candle_visualizer_view(self, request):
        """Interactive, lightweight financial candle chart visualization workstation."""
        if not getattr(settings, "DEBUG", False):
            return HttpResponseForbidden(
                "<h2>403 Forbidden — Debug Mode Required</h2>"
                "<p>Candle Chart Visualizer is restricted strictly to DEBUG mode (<code>settings.DEBUG = True</code>).</p>"
            )

        brokers = Broker.objects.all().order_by("name")
        req_account = request.GET.get("account_id", "").strip()
        selected_broker = None
        if req_account:
            selected_broker = Broker.resolve(req_account)
        if not selected_broker:
            if hasattr(brokers, "first"):
                selected_broker = brokers.first()
            elif brokers:
                selected_broker = list(brokers)[0]

        selected_account_id = selected_broker.account_id if selected_broker else ""

        # Query all distinct candle tables ending strictly with '_all' for this account
        available_tables = []
        if selected_broker:
            qs = AlgoInfo.objects.filter(account=selected_broker, tablename__endswith="_all").values_list("tablename", flat=True).distinct()
            standard_order = ["fwd_10_all", "fwd_30_all", "day_cdl_all", "fwd_1_all", "fwd_3_all", "fwd_5_all", "fwd_15_all", "fwd_60_all", "half_day_cdl_all", "prev_day_cdl_all"]
            found = {t for t in qs if t and t.endswith("_all")}
            for t in standard_order:
                if t in found:
                    available_tables.append(t)
            for t in sorted(found):
                if t not in available_tables:
                    available_tables.append(t)

        if not available_tables:
            available_tables = ["fwd_10_all", "fwd_30_all", "day_cdl_all"]

        selected_table = request.GET.get("tablename", "").strip()
        if not selected_table or selected_table not in available_tables:
            selected_table = available_tables[0]

        # Precompute initial tokens and symbols from cum_table for selected table
        initial_tokens = []
        if selected_broker:
            sym_map = self._get_symbol_map_for_broker(selected_broker)
            rec = AlgoInfo.objects.filter(account=selected_broker, tablename=selected_table).first()
            if not rec:
                rec = AlgoInfo.objects.filter(account=None, tablename=selected_table).first()
            if rec and rec.tabledata:
                td = rec.tabledata
                if isinstance(td, str):
                    try:
                        td = orjson.loads(td)
                    except Exception:
                        td = []
                r_list = td if isinstance(td, list) else (list(td.values())[0] if isinstance(td, dict) and td and isinstance(list(td.values())[0], list) else ([td] if isinstance(td, dict) else []))
                tok_counts = {}
                for r in r_list:
                    if isinstance(r, dict):
                        o, h, l, c_val = r.get("open"), r.get("high"), r.get("low"), r.get("close")
                        dt_raw = r.get("date_time") or r.get("datetime") or r.get("timestamp") or r.get("time") or r.get("Date")
                        if o is None or h is None or l is None or c_val is None or dt_raw is None:
                            continue
                        tkn = r.get("instrument_token") or r.get("token") or r.get("Token") or r.get("token_id") or r.get("tokenid") or r.get("tradingsymbol") or r.get("symbol")
                        if tkn is not None:
                            tok_counts[str(tkn)] = tok_counts.get(str(tkn), 0) + 1
                for tkn_str, cnt in sorted(tok_counts.items(), key=lambda x: (-x[1], x[0])):
                    if cnt > 0:
                        sym_name = sym_map.get(tkn_str)
                        # Filter out tokens where symbol name is missing or is purely numeric
                        if sym_name and not sym_name.isdigit() and not sym_name.replace(".", "").isdigit():
                            initial_tokens.append({
                                "token": tkn_str,
                                "label": sym_name,
                                "count": cnt,
                            })

        req_token = request.GET.get("token", "").strip()
        selected_token = req_token if any(str(t["token"]) == req_token for t in initial_tokens) else (initial_tokens[0]["token"] if initial_tokens else "")

        context = {
            **self.admin_site.each_context(request),
            "title": "📈 Candle Chart Visualizer (Debug Workstation)",
            "opts": self.model._meta,
            "brokers": brokers,
            "selected_account_id": selected_account_id,
            "selected_broker": selected_broker,
            "available_tables": available_tables,
            "selected_table": selected_table,
            "selected_token": selected_token,
            "initial_tokens": initial_tokens,
        }
        return render(request, "admin/kalai/candle_visualizer.html", context)

    def candle_visualizer_api_view(self, request):
        """JSON feed endpoint supplying sanitized, chronological OHLC candlestick data."""
        if not getattr(settings, "DEBUG", False):
            return JsonResponse({"success": False, "error": "Debug mode required (settings.DEBUG = True)"}, status=403)

        account_id = request.GET.get("account_id", "").strip()
        tablename = request.GET.get("tablename", "fwd_10_all").strip()
        target_token = request.GET.get("token", "").strip()

        broker = Broker.resolve(account_id) if account_id else None

        # Build list of available candle tables strictly ending with '_all'
        available_tables = []
        if broker:
            qs = AlgoInfo.objects.filter(account=broker, tablename__endswith="_all").values_list("tablename", flat=True).distinct()
            standard_order = ["fwd_10_all", "fwd_30_all", "day_cdl_all", "fwd_1_all", "fwd_3_all", "fwd_5_all", "fwd_15_all", "fwd_60_all", "half_day_cdl_all", "prev_day_cdl_all"]
            found = {t for t in qs if t and t.endswith("_all")}
            for t in standard_order:
                if t in found:
                    available_tables.append(t)
            for t in sorted(found):
                if t not in available_tables:
                    available_tables.append(t)
        if not available_tables:
            available_tables = ["fwd_10_all", "fwd_30_all", "day_cdl_all"]

        # Enforce that tablename strictly ends with '_all'
        if not tablename.endswith("_all") or tablename not in available_tables:
            tablename = available_tables[0]

        # Build symbol map from cum_table of the account
        symbol_map = self._get_symbol_map_for_broker(broker)

        # Retrieve AlgoInfo record
        query = AlgoInfo.objects.filter(tablename=tablename)
        record = None
        if broker:
            record = query.filter(account=broker).first()
            if not record:
                record = query.filter(account__account_id=broker.account_id).first()
            if not record:
                record = query.filter(account=None).first()
        if not record and not account_id:
            record = query.first()

        raw_tabledata = record.tabledata if record else None
        if isinstance(raw_tabledata, str):
            try:
                raw_tabledata = orjson.loads(raw_tabledata)
            except Exception:
                try:
                    raw_tabledata = json.loads(raw_tabledata)
                except Exception:
                    raw_tabledata = []

        rows = []
        if isinstance(raw_tabledata, list):
            rows = raw_tabledata
        elif isinstance(raw_tabledata, dict):
            first_val = next(iter(raw_tabledata.values())) if raw_tabledata else None
            if isinstance(first_val, list):
                length = len(first_val)
                keys = list(raw_tabledata.keys())
                rows = [{k: raw_tabledata[k][i] for k in keys if i < len(raw_tabledata[k])} for i in range(length)]
            else:
                if "open" in raw_tabledata or "close" in raw_tabledata:
                    rows = [raw_tabledata]
                else:
                    for v in raw_tabledata.values():
                        if isinstance(v, list):
                            rows.extend(v)
                        elif isinstance(v, dict):
                            rows.append(v)

        from zoneinfo import ZoneInfo
        tz_kolkata = ZoneInfo("Asia/Kolkata")
        tz_utc = ZoneInfo("UTC")

        is_crypto_account = False
        if broker:
            b_code = (getattr(broker.broker_name, "code", "") or "").lower()
            b_api = (getattr(broker.api_provider, "code", "") or "").lower()
            b_name = (broker.name or "").lower()
            is_crypto_account = getattr(broker, "is_crypto", False) or any(k in f"{b_code} {b_api} {b_name}" for k in ["coinswitch", "delta", "coindcx", "crypto", "bitcoin"])

        def _parse_ts(dt_val):
            if dt_val is None:
                return None
            if isinstance(dt_val, (int, float)):
                return int(dt_val / 1000) if dt_val > 1e11 else int(dt_val)
            if isinstance(dt_val, datetime):
                if dt_val.tzinfo is None:
                    dt_val = dt_val.replace(tzinfo=tz_kolkata if not is_crypto_account else tz_utc)
                return int(dt_val.timestamp())
            if isinstance(dt_val, date):
                dt_obj = datetime.combine(dt_val, datetime.min.time(), tzinfo=tz_kolkata)
                return int(dt_obj.timestamp())
            if isinstance(dt_val, str):
                val = dt_val.strip()
                try:
                    num = float(val)
                    return int(num / 1000) if num > 1e11 else int(num)
                except ValueError:
                    pass
                for fmt in (
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%d %H:%M",
                    "%Y-%m-%d",
                    "%d-%m-%Y %H:%M:%S",
                    "%d-%m-%Y",
                ):
                    try:
                        clean_str = val.split(".")[0]
                        dt = datetime.strptime(clean_str, fmt)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=tz_kolkata if not is_crypto_account else tz_utc)
                        return int(dt.timestamp())
                    except (ValueError, TypeError):
                        continue
                try:
                    dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=tz_kolkata if not is_crypto_account else tz_utc)
                    return int(dt.timestamp())
                except Exception:
                    return None
            return None

        # Discover all tokens present in the candle data with valid OHLC candles
        tokens_found = {}
        for r in rows:
            if not isinstance(r, dict):
                continue
            o, h, l, c_val = r.get("open"), r.get("high"), r.get("low"), r.get("close")
            dt_raw = r.get("date_time") or r.get("datetime") or r.get("timestamp") or r.get("time") or r.get("Date")
            if o is None or h is None or l is None or c_val is None or dt_raw is None:
                continue

            tkn = (
                r.get("instrument_token")
                or r.get("token")
                or r.get("Token")
                or r.get("token_id")
                or r.get("tokenid")
                or r.get("inst_token")
                or r.get("tkn")
                or r.get("instrument_tkn")
            )
            sym = r.get("tradingsymbol") or r.get("symbol") or r.get("Symbol") or r.get("name") or r.get("stock_name")
            if tkn is not None:
                tkn_str = str(tkn)
                tokens_found[tkn_str] = tokens_found.get(tkn_str, 0) + 1
                if sym and tkn_str not in symbol_map:
                    symbol_map[tkn_str] = str(sym)
            elif sym:
                # If no token id but symbol name exists, use symbol as identifier
                sym_str = str(sym)
                tokens_found[sym_str] = tokens_found.get(sym_str, 0) + 1
                symbol_map[sym_str] = sym_str

        # If table has rows but no token columns were detected:
        if not tokens_found and rows:
            has_ohlc = any(
                isinstance(r, dict)
                and r.get("open") is not None
                and r.get("close") is not None
                for r in rows
            )
            if has_ohlc:
                default_id = "main"
                default_lbl = (broker.name if broker else "Active") + " Series"
                tokens_found[default_id] = len(rows)
                symbol_map[default_id] = default_lbl

        # Build available tokens list with symbols from cum_table (strictly include tokens with valid candles and named symbols)
        available_tokens = []
        for tkn_str, cnt in sorted(tokens_found.items(), key=lambda x: (-x[1], x[0])):
            if cnt <= 0:
                continue
            sym = symbol_map.get(tkn_str, "").strip()
            # Filter out tokens where symbol name is missing or is purely numeric
            if not sym or sym.isdigit() or sym.replace(".", "").isdigit():
                continue
            available_tokens.append({
                "token": tkn_str,
                "symbol": sym,
                "label": sym,
                "count": cnt,
            })

        # Determine target token (ensure target token is valid and in available_tokens)
        active_token = ""
        if target_token and any(str(t["token"]) == str(target_token) for t in available_tokens):
            active_token = target_token
        elif available_tokens:
            active_token = available_tokens[0]["token"]

        # Filter and extract OHLC candles
        filtered_candles = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            r_tkn = r.get("instrument_token") or r.get("token") or r.get("Token") or r.get("token_id") or r.get("tokenid")
            if active_token and r_tkn is not None and str(r_tkn) != str(active_token):
                continue

            open_val = r.get("open")
            high_val = r.get("high")
            low_val = r.get("low")
            close_val = r.get("close")
            dt_raw = r.get("date_time") or r.get("datetime") or r.get("timestamp") or r.get("time") or r.get("Date")

            if open_val is None or high_val is None or low_val is None or close_val is None or dt_raw is None:
                continue

            ts = _parse_ts(dt_raw)
            if ts is None:
                continue

            try:
                filtered_candles.append({
                    "time": ts,
                    "open": round(float(open_val), 2),
                    "high": round(float(high_val), 2),
                    "low": round(float(low_val), 2),
                    "close": round(float(close_val), 2),
                    "raw_time": str(dt_raw),
                })
            except (ValueError, TypeError):
                continue

        # Sort ascending by time and deduplicate (strictly required for TradingView Lightweight Charts)
        filtered_candles.sort(key=lambda x: x["time"])
        deduped = {}
        for c in filtered_candles:
            deduped[c["time"]] = c
        final_candles = sorted(deduped.values(), key=lambda x: x["time"])

        active_symbol = symbol_map.get(str(active_token), "")
        last_sync = record.timestamp.isoformat() if record and record.timestamp else None

        return JsonResponse({
            "success": True,
            "account_id": account_id,
            "tablename": tablename,
            "active_token": active_token,
            "active_symbol": active_symbol,
            "last_sync": last_sync,
            "candle_count": len(final_candles),
            "available_tables": available_tables,
            "available_tokens": available_tokens,
            "candles": final_candles,
        })

    def callback_url_display(self, obj):
        if not obj or not obj.pk:
            return "Will be generated upon saving account."
        url = obj.get_callback_url()
        return format_html(
            '<div style="display:flex;align-items:center;gap:10px;">'
            '<code style="background:#f1f5f9;padding:4px 8px;border-radius:4px;color:#0f172a;font-weight:bold;">{}</code>'
            '<button type="button" onclick="navigator.clipboard.writeText(\'{}\'); this.innerText=\'Copied!\'; setTimeout(() => this.innerText=\'Copy\', 2000);" '
            'style="padding:4px 10px;background:#0077ff;color:#fff;border:none;border-radius:4px;cursor:pointer;font-size:12px;">Copy</button>'
            '</div>',
            url, url
        )
    callback_url_display.short_description = "Auto-Generated Callback URL"

    def save_model(self, request, obj, form, change):
        obj.auto_populate_redirect_url(request=request)
        super().save_model(request, obj, form, change)
        try:
            from django.core.cache import cache
            cache.delete("coinswitch_secret_expiry_alerts")
        except Exception:
            pass

    def api_key_masked(self, obj):
        if obj.api_key:
            masked = obj.api_key[:6] + "••••••" + obj.api_key[-4:]
            return format_html('<code>{}</code>', masked)
        return "—"
    api_key_masked.short_description = "API Key"

    def token_status(self, obj):
        from django.utils import timezone
        from datetime import timedelta

        if not obj.access_token:
            return mark_safe('<span style="color:#94a3b8;font-weight:bold;">⚪ Not Set</span>')
        
        if not obj.access_token_updated_at:
            return mark_safe('<span style="color:#dc2626;font-weight:bold;">🔴 Expired (No Date)</span>')

        now = timezone.now()
        if obj.access_token_updated_at.date() == now.date():
            return mark_safe('<span style="color:#16a34a;font-weight:bold;">🟢 Active Today</span>')
        elif (now - obj.access_token_updated_at) < timedelta(hours=24):
            return mark_safe('<span style="color:#d97706;font-weight:bold;">🟡 Active (&lt;24h)</span>')
        else:
            return mark_safe('<span style="color:#dc2626;font-weight:bold;">🔴 Expired (Old)</span>')
    token_status.short_description = "Token Status"

    def secret_key_status(self, obj):
        if not obj.enable_secret_key_expiry:
            return mark_safe('<span style="color:#64748b;">—</span>')

        if not obj.api_secret:
            return mark_safe('<span style="color:#94a3b8;font-weight:bold;">⚪ Not Set</span>')

        status = obj.secret_key_expiry_status
        days_left = obj.days_until_secret_key_expiry
        expires_at = obj.secret_key_expires_at
        exp_date_str = expires_at.strftime("%b %d") if expires_at else ""

        if status == "EXPIRED":
            label = f"🚨 Expired ({abs(days_left)}d ago)" if days_left is not None else "🚨 Expired"
            return format_html('<span style="color:#ef4444;font-weight:bold;background:#fee2e2;padding:2px 8px;border-radius:4px;display:inline-block;font-size:11px;">{}</span>', label)
        elif status == "EXPIRING_SOON":
            label = f"⚠️ {days_left}d left ({exp_date_str})"
            return format_html('<span style="color:#d97706;font-weight:bold;background:#fef3c7;padding:2px 8px;border-radius:4px;display:inline-block;font-size:11px;">{}</span>', label)
        else:
            label = f"🟢 {days_left}d left" if days_left is not None else "🟢 Active"
            return format_html('<span style="color:#16a34a;font-weight:bold;background:#dcfce7;padding:2px 8px;border-radius:4px;display:inline-block;font-size:11px;">{}</span>', label)
    secret_key_status.short_description = "Secret Key Expiry"


# ─── ProcessedTickStore Admin ─────────────────────────────────────────────────

@admin.register(ProcessedTickStore)
class ProcessedTickStoreAdmin(admin.ModelAdmin):
    list_display = ("id", "account", "timestamp", "data_preview")
    list_filter = ("account",)
    search_fields = ("account__name", "account__account_id")
    readonly_fields = ("timestamp",)
    raw_id_fields = ("account",)
    ordering = ("-timestamp",)
    date_hierarchy = "timestamp"
    list_select_related = ("account", "account__broker_name")
    list_per_page = 50
    show_full_result_count = False
    formfield_overrides = {
        models.JSONField: {"widget": PrettyJSONWidget(attrs={"rows": 20, "cols": 100})},
    }

    def data_preview(self, obj):
        if obj.data:
            if isinstance(obj.data, dict):
                ltp = obj.data.get('last_price') or obj.data.get('ltp')
                tkn = obj.data.get('instrument_token')
                mode = obj.data.get('mode')
                parts = []
                if tkn:
                    parts.append(f"Tkn: {tkn}")
                if ltp is not None:
                    parts.append(f"LTP: {ltp}")
                if mode:
                    parts.append(f"Mode: {mode}")
                summary = " | ".join(parts) if parts else str(obj.data)[:80]
            elif isinstance(obj.data, list):
                summary = f"List [{len(obj.data)} items]"
            else:
                summary = str(obj.data)[:80]
            return format_html('<code style="font-size:11px;color:#0369a1;">{}</code>', summary)
        return "—"
    data_preview.short_description = "Data Preview"


# ─── AlgoInfo Admin ───────────────────────────────────────────────────────────

@admin.register(AlgoInfo)
class AlgoInfoAdmin(admin.ModelAdmin):
    list_display = ("is_pinned", "id", "account", "tablename", "timestamp", "tabledata_summary")
    list_display_links = ("id", "tablename")
    list_editable = ("is_pinned",)
    list_filter = ("is_pinned", "account", "tablename")
    search_fields = ("account__name", "account__account_id", "tablename")
    readonly_fields = ("timestamp",)
    ordering = ("-is_pinned", "-timestamp")
    list_select_related = ("account", "account__broker_name")
    list_per_page = 25
    show_full_result_count = False
    actions = ["pin_selected", "unpin_selected"]
    formfield_overrides = {
        models.JSONField: {"widget": PrettyJSONWidget(attrs={"rows": 20, "cols": 100})},
    }

    @admin.action(description="📌 Pin selected tables to top")
    def pin_selected(self, request, queryset):
        updated = queryset.update(is_pinned=True)
        self.message_user(request, f"Successfully pinned {updated} table(s) to top.")

    @admin.action(description="Unpin selected tables")
    def unpin_selected(self, request, queryset):
        updated = queryset.update(is_pinned=False)
        self.message_user(request, f"Successfully unpinned {updated} table(s).")

    def get_readonly_fields(self, request, obj=None):
        if obj:  # editing an existing object
            return self.readonly_fields + ('account',)
        return self.readonly_fields

    def tabledata_summary(self, obj):
        if obj.tabledata is not None:
            raw = obj.tabledata
            if isinstance(raw, list):
                count = len(raw)
                sample_text = ""
                if count > 0:
                    first = raw[0]
                    if isinstance(first, dict):
                        if "open" in first and "close" in first:
                            o_val, h_val, l_val, c_val = first.get("open"), first.get("high"), first.get("low"), first.get("close")
                            sym = first.get("symbol") or first.get("tradingsymbol") or ""
                            sym_str = f"[{sym}] " if sym else ""
                            sample_text = f"{sym_str}O:{o_val} H:{h_val} L:{l_val} C:{c_val}"
                        elif "tradingsymbol" in first or "symbol" in first or "name" in first:
                            sym = first.get("tradingsymbol") or first.get("symbol") or first.get("name")
                            sample_text = f"Item: {sym}"
                        else:
                            # 2-3 key sample
                            sample_text = ", ".join(f"{k}: {v}" for k, v in list(first.items())[:3])
                    else:
                        sample_text = str(first)[:50]
                
                badge_html = f'<span style="background:#e0f2fe;color:#0369a1;padding:2px 8px;border-radius:4px;font-weight:600;font-size:11px;white-space:nowrap;">List ({count:,})</span>'
                if sample_text:
                    return format_html(
                        '{} <code style="color:#475569;font-size:11px;margin-left:6px;background:#f8fafc;padding:2px 6px;border-radius:3px;border:1px solid #e2e8f0;">{}</code>',
                        mark_safe(badge_html),
                        sample_text
                    )
                return mark_safe(badge_html)

            elif isinstance(raw, dict):
                count = len(raw)
                if "tokenid" in raw:
                    tokens = raw.get("tokenid", [])
                    t_count = len(tokens) if isinstance(tokens, list) else 0
                    badge_html = f'<span style="background:#fef3c7;color:#b45309;padding:2px 8px;border-radius:4px;font-weight:600;font-size:11px;white-space:nowrap;">Tokens ({t_count:,})</span>'
                    sample_tokens = ", ".join(str(t) for t in (tokens[:3] if isinstance(tokens, list) else []))
                    return format_html(
                        '{} <code style="color:#475569;font-size:11px;margin-left:6px;background:#f8fafc;padding:2px 6px;border-radius:3px;border:1px solid #e2e8f0;">[{}{}]</code>',
                        mark_safe(badge_html),
                        sample_tokens,
                        "..." if t_count > 3 else ""
                    )
                
                badge_html = f'<span style="background:#fef3c7;color:#b45309;padding:2px 8px;border-radius:4px;font-weight:600;font-size:11px;white-space:nowrap;">Dict ({count:,} keys)</span>'
                keys_sample = ", ".join(list(raw.keys())[:3])
                if keys_sample:
                    return format_html(
                        '{} <span style="color:#64748b;font-size:11px;margin-left:6px;">({}{})</span>',
                        mark_safe(badge_html),
                        keys_sample,
                        "..." if count > 3 else ""
                    )
                return mark_safe(badge_html)

            elif isinstance(raw, str):
                preview = raw[:70] + ("…" if len(raw) > 70 else "")
                return format_html('<code style="font-size:11px;color:#334155;background:#f8fafc;padding:2px 6px;border-radius:3px;border:1px solid #e2e8f0;">{}</code>', preview)
            else:
                return format_html('<code style="font-size:11px;">{}</code>', str(raw)[:70])
        return "—"
    tabledata_summary.short_description = "Table Data Summary"


@admin.register(BrokerType)
class BrokerTypeAdmin(admin.ModelAdmin):
    list_display = ("code", "name")
    search_fields = ("code", "name")

@admin.register(ApiProvider)
class ApiProviderAdmin(admin.ModelAdmin):
    list_display = ("code", "name")
    search_fields = ("code", "name")


@admin.register(ExchangeMasterData)
class ExchangeMasterDataAdmin(admin.ModelAdmin):
    list_display = ("master_name", "account_badge", "row_count_badge", "updated_at", "size_badge")
    list_filter = ("master_name", "account")
    search_fields = ("master_name", "account__account_id", "account__name")
    readonly_fields = ("updated_at", "row_count", "tabledata_summary_view", "direct_export_link")
    exclude = ("tabledata",)  # Strictly exclude heavy multi-megabyte JSON from standard textarea rendering
    ordering = ("-updated_at",)
    list_per_page = 20
    show_full_result_count = False

    def get_queryset(self, request):
        # Critical performance optimization: Defer heavy tabledata JSON column to ensure sub-millisecond changelist rendering
        return super().get_queryset(request).select_related("account", "account__broker_name").defer("tabledata")

    def account_badge(self, obj):
        return obj.account.account_id if obj.account else "Global"
    account_badge.short_description = "Account"

    def row_count_badge(self, obj):
        count_str = f"{obj.row_count or 0:,} rows"
        return format_html(
            '<span style="background:rgba(2,132,199,0.12);color:#0284c7;padding:3px 8px;border-radius:4px;font-weight:700;font-size:12px;">{}</span>',
            count_str
        )
    row_count_badge.short_description = "Contracts / Rows"

    def size_badge(self, obj):
        return mark_safe('<span style="color:#64748b;font-size:12px;">PostgreSQL JSONB</span>')
    size_badge.short_description = "Storage Mode"

    def direct_export_link(self, obj):
        from django.urls import reverse
        export_url = f"{reverse('admin:data_hub')}?table=exchangemaster"
        return format_html(
            '<a href="{}" class="button" style="background:#0284c7;color:#fff;padding:6px 12px;border-radius:6px;text-decoration:none;font-weight:600;">📦 Download via Data Hub</a>',
            export_url
        )
    direct_export_link.short_description = "Full Data Export"

    def tabledata_summary_view(self, obj):
        if not obj or not obj.pk:
            return "No data"
        # Fetch just this single record's tabledata without loading all other rows
        raw_val = ExchangeMasterData.objects.filter(pk=obj.pk).values_list("tabledata", flat=True).first()
        if not raw_val:
            return "Empty master table"
        if isinstance(raw_val, list):
            sample = raw_val[:3]
            count_str = f"{len(raw_val):,}"
            try:
                sample_json = orjson.dumps(sample, option=orjson.OPT_INDENT_2).decode("utf-8")
            except Exception:
                sample_json = str(sample)
            return format_html(
                '<div style="font-family:monospace;background:#f8fafc;padding:12px;border-radius:6px;border:1px solid #e2e8f0;max-height:280px;overflow:auto;">'
                '<strong>Total Universe Contracts:</strong> {}<br><br>'
                '<strong>Sample (First 3 items):</strong><br><pre style="margin:0;font-size:11px;">{}</pre>'
                '</div>',
                count_str,
                sample_json
            )
        elif isinstance(raw_val, dict):
            keys = list(raw_val.keys())
            return format_html(
                '<div style="font-family:monospace;background:#f8fafc;padding:12px;border-radius:6px;border:1px solid #e2e8f0;">'
                '<strong>Keys in Tabledata:</strong> {}'
                '</div>',
                ", ".join(keys[:10])
            )
        return str(type(raw_val))
    tabledata_summary_view.short_description = "Universe Sample"


@admin.register(AlgoLog)
class AlgoLogAdmin(admin.ModelAdmin):
    list_display = ("timestamp", "account", "algo_name", "tag", "level_badge", "message_preview")
    list_filter = ("level", "tag", "algo_name", "account")
    search_fields = ("message", "tag", "algo_name", "account__account_id", "account__name")
    readonly_fields = ("timestamp", "account", "algo_name", "tag", "level", "message")
    ordering = ("-timestamp",)
    list_per_page = 50

    def level_badge(self, obj):
        color_map = {
            "INFO": "#0284c7",
            "WARNING": "#d97706",
            "ERROR": "#dc2626",
            "EXCEPTION": "#991b1b",
        }
        color = color_map.get(obj.level.upper(), "#64748b")
        return format_html(
            '<span style="background:{};color:#fff;padding:2px 8px;border-radius:4px;font-weight:bold;font-size:11px;">{}</span>',
            color,
            obj.level.upper()
        )
    level_badge.short_description = "Level"

    def message_preview(self, obj):
        return obj.message[:120] + "..." if len(obj.message) > 120 else obj.message
    message_preview.short_description = "Message"


def evaluate_all_algos_status():
    """
    Evaluates operational status of all production trading algorithms dynamically based on
    linked Broker account settings (enable_trade) and recent AlgoLog telemetry events.
    """
    from django.utils import timezone
    from datetime import timedelta

    now = timezone.now()
    cutoff_running = now - timedelta(minutes=5)
    cutoff_error = now - timedelta(minutes=15)

    all_brokers = list(
        Broker.objects.select_related("broker_name", "api_provider")
        .all()
        .order_by("broker_name__name", "name")
    )

    def _resolve_strategy_spec(broker_obj):
        b_code = (broker_obj.broker_name.code if broker_obj.broker_name and broker_obj.broker_name.code else "").strip().lower()
        p_code = (broker_obj.api_provider.code if broker_obj.api_provider and broker_obj.api_provider.code else "").strip().lower()
        b_name = (broker_obj.broker_name.name if broker_obj.broker_name and broker_obj.broker_name.name else broker_obj.name or "Broker").strip()
        is_crypto = getattr(broker_obj, "is_crypto", False)

        slug = b_code or ("crypto" if is_crypto else "indian")
        if is_crypto:
            return {
                "key": slug,
                "name": f"{b_name} Crypto Engine",
                "engine_file": "crypto_opt_trde_polars.py",
                "broker_name": b_name,
                "broker_codes": [slug],
                "algo_names": ["crypto_opt_trde_polars"],
                "description": f"24/7 continuous crypto options and derivatives strategy via {b_name}.",
            }
        else:
            return {
                "key": slug,
                "name": f"{b_name} Options Engine",
                "engine_file": "indian_opt_trde_polars.py",
                "broker_name": b_name,
                "broker_codes": [slug],
                "algo_names": ["indian_opt_trde_polars"],
                "description": f"Automated Indian options trading strategy via {b_name} execution engine.",
            }

    candidates_dict = {}
    for b in all_brokers:
        spec = _resolve_strategy_spec(b)
        key = spec["key"]
        if key not in candidates_dict:
            candidates_dict[key] = {
                **spec,
                "linked_accounts": [],
            }
        candidates_dict[key]["linked_accounts"].append({
            "id": b.id,
            "account_id": b.account_id or b.name,
            "name": b.name,
            "enable_trade": b.enable_trade,
        })

    candidates = list(candidates_dict.values())

    algos_result = []
    summary = {
        "total": len(candidates),
        "running": 0,
        "errors": 0,
        "idle": 0,
        "stopped": 0,
    }

    for c in candidates:
        linked_accounts = c["linked_accounts"]
        linked_db_ids = [acc["id"] for acc in linked_accounts]
        has_enabled_account = any(acc["enable_trade"] for acc in linked_accounts)

        # Query strategy execution logs: strictly query account logs when accounts are linked
        latest_log = None
        has_recent_error = False

        if linked_db_ids:
            latest_log = (
                AlgoLog.objects.filter(account_id__in=linked_db_ids)
                .order_by("-timestamp")
                .first()
            )
            has_recent_error = (
                AlgoLog.objects.filter(
                    account_id__in=linked_db_ids,
                    level="ERROR",
                    timestamp__gte=cutoff_error,
                ).exists()
            )
        else:
            algo_names_to_match = c.get("algo_names", [c["key"]])
            fallback_q = (
                models.Q(account__isnull=True, algo_name__in=algo_names_to_match)
                if algo_names_to_match
                else models.Q(account__isnull=True, algo_name=c["key"])
            )
            latest_log = (
                AlgoLog.objects.filter(fallback_q)
                .order_by("-timestamp")
                .first()
            )
            has_recent_error = (
                AlgoLog.objects.filter(
                    fallback_q,
                    level="ERROR",
                    timestamp__gte=cutoff_error,
                ).exists()
            )

        # Status determination
        if not has_enabled_account or not linked_accounts:
            status = "STOPPED"
            status_display = "Stopped / Trade Disabled"
            status_badge = "badge-stopped"
            status_icon = "⏹️"
            summary["stopped"] += 1
        elif has_recent_error and (latest_log and latest_log.level in ["ERROR", "EXCEPTION"]):
            status = "ERROR"
            status_display = "Error Detected"
            status_badge = "badge-error"
            status_icon = "⚠️"
            summary["errors"] += 1
        elif latest_log and latest_log.timestamp >= cutoff_running:
            status = "RUNNING"
            status_display = "Running"
            status_badge = "badge-running"
            status_icon = "🟢"
            summary["running"] += 1
        else:
            status = "IDLE"
            status_display = "Idle / Standby"
            status_badge = "badge-idle"
            status_icon = "⏸️"
            summary["idle"] += 1

        last_active_str = "No logs recorded"
        last_active_relative = "Never"
        if latest_log:
            last_active_str = latest_log.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            diff_secs = max(0, int((now - latest_log.timestamp).total_seconds()))
            if diff_secs < 60:
                last_active_relative = f"{diff_secs}s ago"
            elif diff_secs < 3600:
                last_active_relative = f"{diff_secs // 60}m {diff_secs % 60}s ago"
            elif diff_secs < 86400:
                last_active_relative = f"{diff_secs // 3600}h {(diff_secs % 3600) // 60}m ago"
            else:
                last_active_relative = f"{diff_secs // 86400}d ago"

        algos_result.append({
            **c,
            "linked_accounts": linked_accounts,
            "has_enabled_account": has_enabled_account,
            "status": status,
            "status_display": status_display,
            "status_badge": status_badge,
            "status_icon": status_icon,
            "latest_log": latest_log,
            "last_active_str": last_active_str,
            "last_active_relative": last_active_relative,
        })

    return algos_result, summary


@admin.register(BrokerPosition)
class BrokerPositionAdmin(admin.ModelAdmin):
    list_display = (
        "account_badge",
        "tradingsymbol",
        "product",
        "quantity_badge",
        "buy_price",
        "sell_price",
        "last_price",
        "unrealized_pnl_badge",
        "realized_pnl_badge",
        "total_pnl_badge",
        "status_badge",
        "updated_at_relative",
    )
    list_filter = ("is_open", "account__broker_name", "product", "account")
    search_fields = ("tradingsymbol", "account__name", "account__account_id")
    readonly_fields = ("created_at", "updated_at", "raw_data")

    def account_badge(self, obj):
        b_name = obj.account.broker_name.name if obj.account.broker_name else ""
        return format_html(
            '<span style="font-weight:600;">{}</span> <small style="opacity:0.75;">({})</small>',
            obj.account.account_id or obj.account.name,
            b_name,
        )
    account_badge.short_description = "Account"

    def quantity_badge(self, obj):
        qty = obj.quantity
        if qty > 0:
            color = "#10b981"
            prefix = "+"
        elif qty < 0:
            color = "#f59e0b"
            prefix = ""
        else:
            color = "#64748b"
            prefix = ""
        return format_html(
            '<span style="font-weight:bold;color:{};">{}{}</span>',
            color,
            prefix,
            qty,
        )
    quantity_badge.short_description = "Net Qty"

    def unrealized_pnl_badge(self, obj):
        val = obj.unrealized_pnl
        color = "#10b981" if val > 0 else ("#ef4444" if val < 0 else "#94a3b8")
        val_str = f"{val:+,.2f}"
        return format_html('<span style="font-weight:600;color:{};">{}</span>', color, val_str)
    unrealized_pnl_badge.short_description = "Unrealized PnL"

    def realized_pnl_badge(self, obj):
        val = obj.realized_pnl
        color = "#10b981" if val > 0 else ("#ef4444" if val < 0 else "#94a3b8")
        val_str = f"{val:+,.2f}"
        return format_html('<span style="font-weight:600;color:{};">{}</span>', color, val_str)
    realized_pnl_badge.short_description = "Realized PnL"

    def total_pnl_badge(self, obj):
        val = obj.total_pnl
        color = "#10b981" if val > 0 else ("#ef4444" if val < 0 else "#94a3b8")
        val_str = f"{val:+,.2f}"
        return format_html('<span style="font-weight:bold;color:{};font-size:13px;">{}</span>', color, val_str)
    total_pnl_badge.short_description = "Total PnL"

    def status_badge(self, obj):
        if obj.is_open:
            return mark_safe('<span style="background:rgba(16,185,129,0.15);color:#10b981;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:bold;">🟢 OPEN</span>')
        return mark_safe('<span style="background:rgba(100,116,139,0.15);color:#94a3b8;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:bold;">⚪ CLOSED</span>')
    status_badge.short_description = "Status"

    def updated_at_relative(self, obj):
        from django.utils import timezone
        diff = int((timezone.now() - obj.updated_at).total_seconds())
        if diff < 60:
            return f"{diff}s ago"
        elif diff < 3600:
            return f"{diff // 60}m ago"
        return obj.updated_at.strftime("%Y-%m-%d %H:%M")
    updated_at_relative.short_description = "Updated"


@admin.register(TradeRecord)
class TradeRecordAdmin(admin.ModelAdmin):
    list_display = (
        "executed_at_fmt",
        "account_badge",
        "action_type_badge",
        "tradingsymbol",
        "product",
        "quantity",
        "price",
        "value_fmt",
        "realized_pnl_badge",
    )
    list_filter = ("action_type", "product", "account__broker_name", "account")
    search_fields = ("tradingsymbol", "order_id", "account__name", "account__account_id")
    readonly_fields = ("executed_at", "raw_payload")

    def account_badge(self, obj):
        return obj.account.account_id or obj.account.name
    account_badge.short_description = "Account"

    def executed_at_fmt(self, obj):
        return obj.executed_at.strftime("%Y-%m-%d %H:%M:%S")
    executed_at_fmt.short_description = "Execution Time"

    def action_type_badge(self, obj):
        act = obj.action_type.upper()
        if "BUY" in act:
            return mark_safe('<span style="background:rgba(16,185,129,0.15);color:#10b981;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:bold;">BUY</span>')
        elif "SELL" in act:
            return mark_safe('<span style="background:rgba(239,68,68,0.15);color:#ef4444;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:bold;">SELL</span>')
        return format_html('<span style="background:rgba(100,116,139,0.15);color:#94a3b8;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:bold;">{}</span>', act)
    action_type_badge.short_description = "Action"

    def value_fmt(self, obj):
        return f"{obj.value:,.2f}"
    value_fmt.short_description = "Value"

    def realized_pnl_badge(self, obj):
        val = obj.realized_pnl
        if val == 0:
            return "-"
        color = "#10b981" if val > 0 else "#ef4444"
        val_str = f"{val:+,.2f}"
        return format_html('<span style="font-weight:bold;color:{};">{}</span>', color, val_str)
    realized_pnl_badge.short_description = "Realized PnL"


@admin.register(DailyPnLSnapshot)
class DailyPnLSnapshotAdmin(admin.ModelAdmin):
    list_display = (
        "date",
        "account_badge",
        "net_pnl_badge",
        "realized_pnl_fmt",
        "unrealized_pnl_fmt",
        "total_trades",
        "turnover_fmt",
        "updated_at",
    )
    list_filter = ("account__broker_name", "account", "date")
    search_fields = ("account__name", "account__account_id")
    date_hierarchy = "date"

    def account_badge(self, obj):
        return obj.account.account_id or obj.account.name
    account_badge.short_description = "Account"

    def net_pnl_badge(self, obj):
        val = obj.net_pnl
        color = "#10b981" if val > 0 else ("#ef4444" if val < 0 else "#94a3b8")
        val_str = f"{val:+,.2f}"
        return format_html('<span style="font-weight:bold;color:{};font-size:13px;">{}</span>', color, val_str)
    net_pnl_badge.short_description = "Net PnL"

    def realized_pnl_fmt(self, obj):
        val = obj.realized_pnl
        color = "#10b981" if val > 0 else ("#ef4444" if val < 0 else "#94a3b8")
        val_str = f"{val:+,.2f}"
        return format_html('<span style="font-weight:600;color:{};">{}</span>', color, val_str)
    realized_pnl_fmt.short_description = "Realized PnL"

    def unrealized_pnl_fmt(self, obj):
        val = obj.unrealized_pnl
        color = "#10b981" if val > 0 else ("#ef4444" if val < 0 else "#94a3b8")
        val_str = f"{val:+,.2f}"
        return format_html('<span style="font-weight:600;color:{};">{}</span>', color, val_str)
    unrealized_pnl_fmt.short_description = "Unrealized PnL"

    def turnover_fmt(self, obj):
        return f"{obj.turnover:,.2f}"
    turnover_fmt.short_description = "Turnover"

