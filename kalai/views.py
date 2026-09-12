import io
import os
import time
import logging
import secrets
import requests
from psycopg import sql

from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.db import connection, transaction
from django.http import HttpResponse, JsonResponse, FileResponse, StreamingHttpResponse, HttpResponseForbidden
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_http_methods
from django.views.decorators.csrf import csrf_exempt

from functools import wraps
from django.conf import settings
from kalai.models import (
    AlgoInfo,
    ExchangeMasterData,
    Broker,
    AlgoLog,
    BrokerPosition,
    TradeRecord,
    DailyPnLSnapshot,
)
from kalai.auth.totp import (
    get_totp_for_broker,
    clean_totp_secret,  # noqa: F401
    get_network_time_offset,  # noqa: F401
)
from algo_trading.app_config import config

logger = logging.getLogger(__name__)


def staff_or_token_required(view_func):
    """
    Dual-Authentication decorator for export APIs:
    1. Active staff session (request.user.is_authenticated and request.user.is_staff).
    2. Cryptographic M2M token verification via 'X-Server-Key' header or 'Authorization: Bearer <KEY>'
       compared against settings.M2M_SERVER_KEY or settings.SECRET_KEY using constant-time comparison.
    """
    @wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        # 1. Staff session check
        user = getattr(request, "user", None)
        if user and user.is_authenticated and user.is_staff:
            return view_func(request, *args, **kwargs)

        # 2. Machine-to-Machine (M2M) token verification
        provided_key = request.headers.get("X-Server-Key") or ""
        if not provided_key:
            auth_header = request.headers.get("Authorization") or ""
            if auth_header.startswith("Bearer "):
                provided_key = auth_header.split(" ", 1)[1].strip()

        m2m_key = getattr(settings, "M2M_SERVER_KEY", "") or ""
        secret_key = getattr(settings, "SECRET_KEY", "") or ""

        # Validate against dedicated M2M key first, falling back to SECRET_KEY
        if provided_key:
            if m2m_key and secrets.compare_digest(provided_key, m2m_key):
                return view_func(request, *args, **kwargs)
            if secret_key and secrets.compare_digest(provided_key, secret_key):
                return view_func(request, *args, **kwargs)

        return JsonResponse(
            {"error": "Unauthorized: Valid staff session or X-Server-Key header required."},
            status=401
        )
    return _wrapped_view


def _resolve_remote_host() -> str:
    """Extract clean remote server IP or hostname from environment settings."""
    server_ip = (os.getenv("SERVER_IP") or "").strip()
    remote_host = (os.getenv("REMOTE_DB_HOST") or "").strip()

    # Expand any literal ${SERVER_IP} syntax if present
    if remote_host.startswith("${") and remote_host.endswith("}"):
        var_name = remote_host[2:-1]
        remote_host = (os.getenv(var_name) or server_ip).strip()

    candidate = remote_host or server_ip
    if " #" in candidate:
        candidate = candidate.split(" #", 1)[0].strip()
    return candidate


@staff_or_token_required
@require_GET
def export_models_api(request):
    """
    API endpoint to export AlgoInfo state tables as JSON (Staff or M2M Token).
    Excludes Broker credentials to prevent sensitive credential exposure.
    Includes account_id string for clean cross-database foreign key alignment.
    """
    import orjson
    qs = AlgoInfo.objects.select_related("account").all()
    data = []
    for a in qs:
        data.append({
            "model": "kalai.algoinfo",
            "pk": a.pk,
            "fields": {
                "account": a.account_id,
                "account_id": a.account.account_id if a.account else None,
                "tablename": a.tablename,
                "tabledata": a.tabledata,
                "is_pinned": a.is_pinned,
                "timestamp": a.timestamp.isoformat() if a.timestamp else None,
            }
        })
    return HttpResponse(orjson.dumps(data), content_type='application/json')


@staff_or_token_required
@require_GET
def export_masters_api(request):
    """
    API endpoint to export ExchangeMasterData tables (cum_table, etc.) as JSON (Staff or M2M Token).
    Excludes Broker credentials to prevent sensitive credential exposure.
    Includes account_id string for clean cross-database foreign key alignment.
    """
    import orjson
    qs = ExchangeMasterData.objects.select_related("account").all()
    data = []
    for m in qs:
        data.append({
            "model": "kalai.exchangemasterdata",
            "pk": m.pk,
            "fields": {
                "account": m.account_id,
                "account_id": m.account.account_id if m.account else None,
                "master_name": m.master_name,
                "row_count": m.row_count,
                "updated_at": m.updated_at.isoformat() if m.updated_at else None,
                "tabledata": m.tabledata,
            }
        })
    return HttpResponse(orjson.dumps(data), content_type='application/json')


@staff_or_token_required
@require_GET
def export_pnl_api(request):
    """
    API endpoint to export BrokerPosition, DailyPnLSnapshot, and TradeRecord as JSON (Staff or M2M Token).
    Excludes Broker credentials to prevent sensitive credential exposure.
    Includes account_id string for clean cross-database foreign key alignment.
    """
    import orjson
    data = []
    for p in BrokerPosition.objects.select_related("account").all():
        data.append({
            "model": "kalai.brokerposition",
            "pk": p.pk,
            "fields": {
                "account": p.account_id,
                "account_id": p.account.account_id if p.account else None,
                "tradingsymbol": p.tradingsymbol,
                "instrument_token": p.instrument_token,
                "product": p.product,
                "quantity": str(p.quantity),
                "buy_quantity": str(p.buy_quantity),
                "buy_price": str(p.buy_price),
                "buy_value": str(p.buy_value),
                "sell_quantity": str(p.sell_quantity),
                "sell_price": str(p.sell_price),
                "sell_value": str(p.sell_value),
                "last_price": str(p.last_price),
                "unrealized_pnl": str(p.unrealized_pnl),
                "realized_pnl": str(p.realized_pnl),
                "total_pnl": str(p.total_pnl),
                "is_open": p.is_open,
                "raw_data": p.raw_data,
            }
        })
    for s in DailyPnLSnapshot.objects.select_related("account").all():
        data.append({
            "model": "kalai.dailypnlsnapshot",
            "pk": s.pk,
            "fields": {
                "account": s.account_id,
                "account_id": s.account.account_id if s.account else None,
                "date": s.date.isoformat() if s.date else None,
                "realized_pnl": str(s.realized_pnl),
                "unrealized_pnl": str(s.unrealized_pnl),
                "total_pnl": str(s.total_pnl),
                "turnover": str(s.turnover),
                "trades_count": s.trades_count,
            }
        })
    for t in TradeRecord.objects.select_related("account").all():
        data.append({
            "model": "kalai.traderecord",
            "pk": t.pk,
            "fields": {
                "account": t.account_id,
                "account_id": t.account.account_id if t.account else None,
                "order_id": t.order_id,
                "tradingsymbol": t.tradingsymbol,
                "product": t.product,
                "action_type": t.action_type,
                "quantity": str(t.quantity),
                "price": str(t.price),
                "value": str(t.value),
                "realized_pnl": str(t.realized_pnl),
                "executed_at": t.executed_at.isoformat() if t.executed_at else None,
            }
        })
    return HttpResponse(orjson.dumps(data), content_type='application/json')


@staff_or_token_required
@require_GET
def export_ticks_api(request):
    """
    API endpoint to export ProcessedTickStore as CSV using fast Postgres COPY (Staff or M2M Token).
    """
    try:
        hours = max(1, min(int(request.GET.get('hours', 1)), 8760))
    except (ValueError, TypeError):
        hours = 1

    buffer = io.BytesIO()
    with connection.cursor() as cursor:
        copy_out_sql = sql.SQL("""COPY (
            SELECT b.account_id, p.data, p.timestamp 
            FROM kalai_processedtickstore p
            JOIN kalai_broker b ON p.account_id = b.id
            WHERE p.timestamp >= NOW() - make_interval(hours => {hours})
        ) TO STDOUT WITH CSV HEADER""").format(hours=sql.Literal(hours))
        with cursor.copy(copy_out_sql) as copy:
            for row in copy:
                buffer.write(row)

    return HttpResponse(buffer.getvalue(), content_type='text/csv')


@staff_or_token_required
@require_GET
def export_algo_logs_api(request):
    """
    API endpoint to export AlgoLog records as CSV using fast Postgres COPY (Staff or M2M Token).
    """
    try:
        hours = max(1, min(int(request.GET.get('hours', 24)), 8760))
    except (ValueError, TypeError):
        hours = 24

    buffer = io.BytesIO()
    with connection.cursor() as cursor:
        copy_out_sql = sql.SQL("""COPY (
            SELECT COALESCE(b.account_id, '') as account_id,
                   l.algo_name,
                   l.tag,
                   l.level,
                   l.message,
                   l.timestamp
            FROM (
                SELECT account_id, algo_name, tag, level, message, timestamp
                FROM kalai_algolog
                WHERE timestamp >= NOW() - make_interval(hours => {hours})
                ORDER BY timestamp DESC
                LIMIT 50000
            ) l
            LEFT JOIN kalai_broker b ON l.account_id = b.id
            ORDER BY l.timestamp ASC
        ) TO STDOUT WITH CSV HEADER""").format(hours=sql.Literal(hours))
        with cursor.copy(copy_out_sql) as copy:
            for row in copy:
                buffer.write(row)

    return HttpResponse(buffer.getvalue(), content_type='text/csv')


def home_view(request):
    """Serve templates/pages/home.html."""
    return render(request, "pages/home.html")


@staff_member_required
def broker_login_view(request):
    """Serve templates/admin/broker-login.html and handle login redirection."""
    from django.urls import reverse
    from django.db.models import Q
    from kalai.auth import get_auth_adapter

    broker_records = list(Broker.objects.all().order_by('-enable_websocket', 'name'))
    now = timezone.now()

    broker_items = []
    for b in broker_records:
        adapter = get_auth_adapter(b)
        acc_id = b.account_id or b.name

        try:
            callback_path = reverse("custom_admin:broker_callback_account", kwargs={"account_id": acc_id})
            callback_url = request.build_absolute_uri(callback_path)
        except Exception:
            callback_url = request.build_absolute_uri(f"/broker-admin/callback/{acc_id}/")

        if not b.access_token:
            token_status = "MISSING"
        elif b.access_token_updated_at and timezone.localtime(b.access_token_updated_at).date() == timezone.localtime(now).date():
            token_status = "ACTIVE_TODAY"
        elif b.access_token_updated_at and (now - b.access_token_updated_at).total_seconds() < 86400:
            token_status = "ACTIVE_24H"
        else:
            token_status = "EXPIRED"

        broker_items.append({
            "broker": b,
            "account_id": acc_id,
            "adapter": adapter,
            "callback_url": callback_url,
            "token_status": token_status,
            "supports_oauth": adapter.supports_oauth(),
            "supports_direct_login": adapter.supports_direct_login(),
        })

    admin_index_url = reverse("admin:index")
    next_url = request.GET.get("next")
    if next_url and "broker-login" not in next_url:
        request.session["admin_return_url"] = next_url

    session_return = request.session.get("admin_return_url")
    referer = request.META.get("HTTP_REFERER")

    if next_url and "broker-login" not in next_url:
        return_url = next_url
    elif session_return and "broker-login" not in session_return:
        return_url = session_return
    elif referer and "broker-login" not in referer and (request.get_host() in referer):
        return_url = referer
    else:
        return_url = admin_index_url

    template_context = {
        "broker_items": broker_items,
        "brokers": broker_records,
        "return_url": return_url,
    }

    if request.method == "POST":
        account_target = request.POST.get("broker") or request.POST.get("account_id")
        if not account_target:
            messages.error(request, "No broker account selected for login.")
            return render(request, "admin/broker-login.html", template_context)

        broker = Broker.objects.filter(Q(name=account_target) | Q(account_id=account_target)).first()
        if not broker:
            messages.error(request, f"Account '{account_target}' not found in database.")
            return render(request, "admin/broker-login.html", template_context)

        adapter = get_auth_adapter(broker)

        if adapter.supports_oauth():
            try:
                cb_path = reverse("custom_admin:broker_callback_account", kwargs={"account_id": broker.account_id or broker.name})
                cb_url = request.build_absolute_uri(cb_path)
            except Exception:
                cb_url = request.build_absolute_uri(f"/broker-admin/callback/{broker.account_id or broker.name}/")

            try:
                oauth_state = secrets.token_urlsafe(32)
                request.session['oauth_state'] = oauth_state
                request.session['login_broker_name'] = broker.name
                request.session['login_account_id'] = broker.account_id or broker.name

                login_url = adapter.get_login_url(broker, request, callback_url=cb_url)
                return redirect(login_url)
            except Exception as exc:
                messages.error(request, f"Could not generate login URL for {broker.account_id or broker.name}: {exc}")
                return render(request, "admin/broker-login.html", template_context)

        elif adapter.supports_direct_login():
            totp = request.POST.get("totp") or get_totp_for_broker(broker)
            mpin = request.POST.get("mpin") or broker.api_secret
            success, msg = adapter.handle_direct_login(broker, totp=totp, mpin=mpin)
            if success:
                messages.success(request, msg)
            else:
                messages.error(request, msg)
            return redirect("custom_admin:broker_login")

        else:
            messages.info(request, f"Account '{broker.account_id or broker.name}' does not require interactive OAuth login.")
            return redirect("custom_admin:broker_login")

    return render(request, "admin/broker-login.html", template_context)


def broker_callback_view(request, account_id: str | None = None):
    """
    Handle OAuth callback redirects for any broker and any account.
    Resolves account deterministically from:
    1. URL path param (e.g. /broker-admin/callback/HS6525/)
    2. Query param (?account_id=... / ?broker=... / ?state=...)
    3. Session key ('login_account_id' / 'login_broker_name')
    4. Fallback search by single matching active OAuth broker
    """
    from django.db.models import Q
    from kalai.auth import get_auth_adapter

    # Validate state parameter if present in session
    session_state = request.session.get("oauth_state")
    req_state = request.GET.get("state")
    if session_state and req_state and session_state != req_state:
        logger.warning("OAuth state verification mismatch: expected %s, got %s", session_state, req_state)
        messages.error(request, "OAuth state verification failed: Invalid state token. Login aborted for security.")
        request.session.pop("login_broker_name", None)
        request.session.pop("login_account_id", None)
        request.session.pop("oauth_state", None)
        return redirect("custom_admin:broker_login")

    target_id = (
        account_id
        or request.GET.get("account_id")
        or request.GET.get("broker")
        or request.GET.get("state")
        or request.session.get("login_account_id")
        or request.session.get("login_broker_name")
    )

    broker = None
    if target_id:
        broker = Broker.objects.filter(Q(account_id__iexact=target_id) | Q(name__iexact=target_id)).first()

    # Fallback if session was lost across cross-origin redirect and no account param was passed
    if not broker:
        request_token = request.GET.get("request_token") or request.GET.get("code") or request.GET.get("auth_token")
        if request_token:
            oauth_brokers = [b for b in Broker.objects.filter(enable_websocket=True) if get_auth_adapter(b).supports_oauth()]
            if len(oauth_brokers) == 1:
                broker = oauth_brokers[0]
                logger.info("Callback fell back to single active OAuth account: %s", broker.account_id or broker.name)

    if not broker:
        messages.error(request, "Could not identify the broker account for this callback. Please initiate login from the Broker Login dashboard.")
        return redirect("custom_admin:broker_login")

    adapter = get_auth_adapter(broker)
    success, msg = adapter.handle_callback(broker, request)

    if success:
        messages.success(request, msg)
    else:
        messages.error(request, msg)

    request.session.pop("login_broker_name", None)
    request.session.pop("login_account_id", None)
    request.session.pop("oauth_state", None)

    return redirect("custom_admin:broker_login")

@staff_member_required
def default_totp_view(request):
    """Serve templates/admin/default-totp.html displaying TOTP only for accounts with a configured TOTP key."""
    # Filter only accounts that have both a non-empty account_id AND a configured totp_secret
    brokers = [
        b for b in Broker.objects.all()
        if b.account_id and b.account_id.strip() and b.totp_secret and b.totp_secret.strip()
    ]
    default_broker_obj = brokers[0] if brokers else None
    default_account_id = default_broker_obj.account_id if default_broker_obj else None
    
    from django.urls import reverse

    # Generate current TOTP code for each account using stored secret
    totps = {b.account_id: get_totp_for_broker(b) for b in brokers}
    default_totp = totps.get(default_account_id) if default_account_id else None

    admin_index_url = reverse("admin:index")
    next_url = request.GET.get("next")
    if next_url and "default-totp" not in next_url:
        request.session["totp_return_url"] = next_url

    session_return = request.session.get("totp_return_url")
    referer = request.META.get("HTTP_REFERER")

    if next_url and "default-totp" not in next_url:
        return_url = next_url
    elif session_return and "default-totp" not in session_return:
        return_url = session_return
    elif referer and "default-totp" not in referer and (request.get_host() in referer):
        return_url = referer
    else:
        return_url = admin_index_url

    broker_items = []
    for b in brokers:
        broker_items.append({
            "broker": b,
            "account_id": b.account_id,
            "totp": totps.get(b.account_id, "------"),
            "broker_name": b.broker_name.name if b.broker_name else (b.broker_name_id or "Broker"),
            "api_provider": b.api_provider.name if b.api_provider else None,
        })

    context = {
        "brokers": brokers,
        "broker_items": broker_items,
        "default_broker": default_account_id,
        "totps": totps,
        "default_totp": default_totp,
        "next_url": return_url,
        "return_url": return_url,
    }
    return render(request, "admin/default-totp.html", context)

@staff_member_required
def fetch_totp_api(request):
    """API endpoint to dynamically fetch the updated TOTP code for an account using account_id (Staff Only)."""
    account_id = request.GET.get("account_id") or request.GET.get("broker")
    if not account_id:
        return JsonResponse({"error": "account_id is required"}, status=400)
    
    from django.db.models import Q
    broker = Broker.objects.filter(Q(account_id=account_id) | Q(name=account_id)).first()
    if not broker or not broker.account_id or not broker.account_id.strip():
        return JsonResponse({"error": "Account not found or account_id is empty"}, status=404)
        
    if not broker.totp_secret or not broker.totp_secret.strip():
        return JsonResponse({"error": "No TOTP key configured for this account"}, status=400)

    return JsonResponse({
        "account_id": broker.account_id,
        "broker": broker.account_id,
        "totp": get_totp_for_broker(broker)
    })

@staff_member_required
def fetch_remote_data_view(request):
    """Legacy alias delegating to clone_remote_data_api."""
    return clone_remote_data_api(request)

@staff_member_required
def data_hub_view(request):
    """Serve templates/admin/kalai/data_hub.html with broker accounts and configured remote host."""
    from django.contrib import admin
    import re
    remote_host = _resolve_remote_host()
    ssl_verify = getattr(settings, "SSL_VERIFY", True)
    is_ip_host = bool(re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", (remote_host or "").strip()))
    context = {
        **admin.site.each_context(request),
        "title": "Data Hub & Cloud Synchronizer",
        "opts": Broker._meta,
        "brokers": Broker.objects.all().order_by("name"),
        "remote_db_host": remote_host or "",
        "ssl_verify": ssl_verify,
        "is_ip_host": is_ip_host,
    }
    return render(request, "admin/kalai/data_hub.html", context)


def _parse_timeframe_filter(period: str, start_date_str: str = "", end_date_str: str = ""):
    """Helper to resolve timezone-aware UTC datetime bounds for queries."""
    from datetime import datetime, time, timedelta
    import zoneinfo
    tz = zoneinfo.ZoneInfo("Asia/Kolkata")
    now = timezone.now().astimezone(tz)
    
    start_dt = None
    end_dt = now

    if period == "today":
        start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "yesterday":
        yesterday = now - timedelta(days=1)
        start_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif period == "last_7_days":
        start_dt = now - timedelta(days=7)
    elif period == "last_30_days":
        start_dt = now - timedelta(days=30)
    elif period == "custom":
        if start_date_str:
            try:
                sd = datetime.strptime(start_date_str.strip(), "%Y-%m-%d")
                start_dt = datetime.combine(sd.date(), time.min, tzinfo=tz)
            except ValueError:
                pass
        if end_date_str:
            try:
                ed = datetime.strptime(end_date_str.strip(), "%Y-%m-%d")
                end_dt = datetime.combine(ed.date(), time.max, tzinfo=tz)
            except ValueError:
                pass
    return start_dt, end_dt


def _convert_tabledata_to_polars(td, account_id=None):
    """
    Safely converts a JSON tabledata payload (list of dicts, dict of columns, etc.)
    into a typed Polars DataFrame with an optional leading Account_ID column.
    """
    import polars as pl
    if td is None:
        return pl.DataFrame()
    try:
        if isinstance(td, list):
            if not td:
                return pl.DataFrame()
            if isinstance(td[0], dict):
                df = pl.DataFrame(td)
            else:
                df = pl.DataFrame({"value": td})
        elif isinstance(td, dict):
            try:
                df = pl.DataFrame(td)
            except Exception:
                df = pl.DataFrame({k: [v] for k, v in td.items()})
        else:
            return pl.DataFrame({"data": [str(td)]})
    except Exception:
        return pl.DataFrame()

    if account_id and not df.is_empty():
        if "Account_ID" not in df.columns and "account_id" not in df.columns:
            df = df.with_columns(pl.lit(str(account_id)).alias("Account_ID"))
            cols = ["Account_ID"] + [c for c in df.columns if c != "Account_ID"]
            df = df.select(cols)

    return df


@staff_or_token_required
def export_data_endpoint(request):
    """
    Unified, zero-lock, throttled streaming export endpoint.
    Supports exporting all 7 DeltaZero26 tables across custom timeframes in Excel, CSV, or JSON.
    """
    import polars as pl
    import orjson
    from algo_trading.algos.polars_excel import write_polars_sheets_to_excel

    table_key = request.GET.get("table", "algolog").lower()
    account_id = request.GET.get("account_id", "ALL")
    period = request.GET.get("period", "last_7_days")
    start_date_str = request.GET.get("start_date", "")
    end_date_str = request.GET.get("end_date", "")
    fmt = request.GET.get("format", "xlsx").lower()
    limit = max(1, min(int(request.GET.get("limit", 50000)), 100000))

    start_dt, end_dt = _parse_timeframe_filter(period, start_date_str, end_date_str)
    ts_label = timezone.now().strftime("%Y%m%d_%H%M%S")
    account_filter = None if account_id in ("ALL", "", None) else account_id

    # ── Fast Streaming CSV for High-Volume Tick & Log Tables ──────────────────
    if fmt == "csv" and table_key == "ticks":
        def tick_stream_generator():
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY ISOLATION LEVEL READ COMMITTED;")
                # Use psycopg copy stream
                with cursor.copy(sql.SQL("""COPY (
                    SELECT b.account_id, p.data, p.timestamp 
                    FROM kalai_processedtickstore p
                    JOIN kalai_broker b ON p.account_id = b.id
                    WHERE ({acc} IS NULL OR b.account_id = {acc})
                      AND ({s_dt}::timestamptz IS NULL OR p.timestamp >= {s_dt})
                      AND ({e_dt}::timestamptz IS NULL OR p.timestamp <= {e_dt})
                    ORDER BY p.timestamp DESC
                ) TO STDOUT WITH CSV HEADER""").format(
                    acc=sql.Literal(account_filter),
                    s_dt=sql.Literal(start_dt.isoformat() if start_dt else None),
                    e_dt=sql.Literal(end_dt.isoformat() if end_dt else None)
                )) as copy:
                    for chunk in copy:
                        yield chunk

        response = StreamingHttpResponse(tick_stream_generator(), content_type="text/csv")
        response["Content-Disposition"] = f'attachment; filename="ticks_{account_id}_{ts_label}.csv"'
        return response

    if fmt == "csv" and table_key == "algolog":
        def log_stream_generator():
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY ISOLATION LEVEL READ COMMITTED;")
                with cursor.copy(sql.SQL("""COPY (
                    SELECT COALESCE(b.account_id, '') as account_id,
                           l.algo_name,
                           l.tag,
                           l.level,
                           l.message,
                           l.timestamp
                    FROM kalai_algolog l
                    LEFT JOIN kalai_broker b ON l.account_id = b.id
                    WHERE ({acc} IS NULL OR b.account_id = {acc})
                      AND ({s_dt}::timestamptz IS NULL OR l.timestamp >= {s_dt})
                      AND ({e_dt}::timestamptz IS NULL OR l.timestamp <= {e_dt})
                    ORDER BY l.timestamp DESC
                ) TO STDOUT WITH CSV HEADER""").format(
                    acc=sql.Literal(account_filter),
                    s_dt=sql.Literal(start_dt.isoformat() if start_dt else None),
                    e_dt=sql.Literal(end_dt.isoformat() if end_dt else None)
                )) as copy:
                    for chunk in copy:
                        yield chunk

        response = StreamingHttpResponse(log_stream_generator(), content_type="text/csv")
        response["Content-Disposition"] = f'attachment; filename="algologs_{account_id}_{ts_label}.csv"'
        return response

    # ── Exchange Masters Full Tabledata Export (Multi-Sheet Excel / CSV / JSON) ─
    if table_key == "exchangemaster":
        qs = ExchangeMasterData.objects.select_related("account").all()
        if account_filter:
            qs = qs.filter(account__account_id=account_filter)
        qs = qs.order_by("master_name", "-updated_at")

        if fmt == "xlsx":
            sheets = {}
            cum_dfs = []
            aug_dfs = []
            summary_records = []

            for m in qs:
                acc_label = m.account.account_id if m.account else "Global"
                summary_records.append({
                    "Master_Name": m.master_name,
                    "Account_ID": acc_label,
                    "Row_Count": m.row_count,
                    "Updated_At": m.updated_at.isoformat() if m.updated_at else "",
                    "Content_Size_KB": round(len(orjson.dumps(m.tabledata)) / 1024, 1) if m.tabledata else 0,
                })
                if m.tabledata:
                    df = _convert_tabledata_to_polars(m.tabledata, acc_label)
                    if not df.is_empty():
                        if m.master_name == "cum_table":
                            cum_dfs.append(df)
                        elif m.master_name == "aug_table":
                            aug_dfs.append(df)
                        sheet_title = f"{m.master_name}_{acc_label}"[:31]
                        sheets[sheet_title] = df

            if cum_dfs:
                try:
                    combined_cum = pl.concat(cum_dfs, how="diagonal_relaxed") if len(cum_dfs) > 1 else cum_dfs[0]
                except Exception:
                    combined_cum = cum_dfs[0]
                sheets = {"CUM_TABLE": combined_cum, **sheets}
            elif aug_dfs:
                try:
                    combined_aug = pl.concat(aug_dfs, how="diagonal_relaxed") if len(aug_dfs) > 1 else aug_dfs[0]
                except Exception:
                    combined_aug = aug_dfs[0]
                sheets = {"AUG_TABLE": combined_aug, **sheets}

            if summary_records:
                sheets["MASTERS_INFO"] = pl.DataFrame(summary_records)

            target_dir = os.path.join(settings.BASE_DIR, "logs", "exports")
            os.makedirs(target_dir, exist_ok=True)
            out_file = os.path.join(target_dir, f"cum_table_{account_id}_{ts_label}.xlsx")
            final_path = write_polars_sheets_to_excel(sheets, out_file)

            return FileResponse(
                open(final_path, "rb"),
                as_attachment=True,
                filename=f"cum_table_{account_id}_{ts_label}.xlsx",
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        elif fmt == "csv":
            cum_dfs = []
            for m in qs:
                if m.master_name == "cum_table" and m.tabledata:
                    df = _convert_tabledata_to_polars(m.tabledata, m.account.account_id if m.account else "Global")
                    if not df.is_empty():
                        cum_dfs.append(df)
            if not cum_dfs:
                for m in qs:
                    if m.tabledata:
                        df = _convert_tabledata_to_polars(m.tabledata, m.account.account_id if m.account else "Global")
                        if not df.is_empty():
                            cum_dfs.append(df)
            if cum_dfs:
                try:
                    combined_df = pl.concat(cum_dfs, how="diagonal_relaxed") if len(cum_dfs) > 1 else cum_dfs[0]
                except Exception:
                    combined_df = cum_dfs[0]
                csv_bytes = combined_df.write_csv().encode("utf-8")
            else:
                csv_bytes = b"Account_ID,Info\nGlobal,No master data found\n"

            response = HttpResponse(csv_bytes, content_type="text/csv")
            response["Content-Disposition"] = f'attachment; filename="cum_table_{account_id}_{ts_label}.csv"'
            return response

        elif fmt == "json":
            masters_data = []
            all_rows = []
            for m in qs:
                acc_label = m.account.account_id if m.account else "Global"
                masters_data.append({
                    "master_name": m.master_name,
                    "account_id": acc_label,
                    "row_count": m.row_count,
                    "updated_at": m.updated_at.isoformat() if m.updated_at else "",
                    "tabledata": m.tabledata,
                })
                if isinstance(m.tabledata, list):
                    for row in m.tabledata:
                        if isinstance(row, dict):
                            all_rows.append({"Account_ID": acc_label, "Master_Name": m.master_name, **row})
            return HttpResponse(
                orjson.dumps({"table": table_key, "count": len(all_rows), "masters": masters_data, "data": all_rows}),
                content_type="application/json",
            )

    # ── Strategy State Full Tabledata Export (Multi-Sheet Excel / CSV / JSON) ──
    if table_key == "algoinfo":
        qs = AlgoInfo.objects.select_related("account").all()
        if account_filter:
            qs = qs.filter(account__account_id=account_filter)
        qs = qs.order_by("tablename", "-timestamp")[:limit]

        if fmt == "xlsx":
            sheets = {}
            summary_records = []
            for a in qs:
                acc_label = a.account.account_id if a.account else "Global"
                summary_records.append({
                    "Table_Name": a.tablename,
                    "Account_ID": acc_label,
                    "Is_Pinned": a.is_pinned,
                    "Timestamp": a.timestamp.isoformat() if a.timestamp else "",
                    "Content_Size_KB": round(len(orjson.dumps(a.tabledata)) / 1024, 1) if a.tabledata else 0,
                })
                if a.tabledata:
                    df = _convert_tabledata_to_polars(a.tabledata, acc_label)
                    if not df.is_empty():
                        base_title = f"{a.tablename}_{acc_label}" if account_filter is None else a.tablename
                        sheet_title = base_title[:31]
                        count = 1
                        while sheet_title in sheets:
                            sheet_title = f"{base_title[:28]}_{count}"
                            count += 1
                        sheets[sheet_title] = df

            if summary_records:
                sheets["STATE_SUMMARY"] = pl.DataFrame(summary_records)

            target_dir = os.path.join(settings.BASE_DIR, "logs", "exports")
            os.makedirs(target_dir, exist_ok=True)
            out_file = os.path.join(target_dir, f"algoinfo_{account_id}_{ts_label}.xlsx")
            final_path = write_polars_sheets_to_excel(sheets, out_file)

            return FileResponse(
                open(final_path, "rb"),
                as_attachment=True,
                filename=f"algoinfo_{account_id}_{ts_label}.xlsx",
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        elif fmt == "csv":
            dfs = []
            for a in qs:
                if a.tabledata:
                    df = _convert_tabledata_to_polars(a.tabledata, a.account.account_id if a.account else "Global")
                    if not df.is_empty():
                        dfs.append(df)
            if dfs:
                try:
                    combined_df = pl.concat(dfs, how="diagonal_relaxed") if len(dfs) > 1 else dfs[0]
                except Exception:
                    combined_df = dfs[0]
                csv_bytes = combined_df.write_csv().encode("utf-8")
            else:
                csv_bytes = b"Account_ID,Info\nGlobal,No state data found\n"
            response = HttpResponse(csv_bytes, content_type="text/csv")
            response["Content-Disposition"] = f'attachment; filename="algoinfo_{account_id}_{ts_label}.csv"'
            return response

        elif fmt == "json":
            state_data = []
            for a in qs:
                state_data.append({
                    "tablename": a.tablename,
                    "account_id": a.account.account_id if a.account else "Global",
                    "is_pinned": a.is_pinned,
                    "timestamp": a.timestamp.isoformat() if a.timestamp else "",
                    "tabledata": a.tabledata,
                })
            return HttpResponse(
                orjson.dumps({"table": table_key, "count": len(state_data), "tables": state_data}),
                content_type="application/json",
            )

    # ── General Query Building for Excel / JSON / Non-streaming CSV ────────────
    records = []
    headers = []

    if table_key == "algolog":
        qs = AlgoLog.objects.select_related("account").all()
        if account_filter:
            qs = qs.filter(account__account_id=account_filter)
        if start_dt:
            qs = qs.filter(timestamp__gte=start_dt)
        if end_dt:
            qs = qs.filter(timestamp__lte=end_dt)
        qs = qs.order_by("-timestamp")[:limit]
        
        records = [{
            "Log_ID": log_entry.id,
            "Timestamp": log_entry.timestamp.isoformat(),
            "Account_ID": log_entry.account.account_id if log_entry.account else "",
            "Algorithm": log_entry.algo_name,
            "Tag": log_entry.tag,
            "Level": log_entry.level,
            "Message": log_entry.message,
        } for log_entry in qs]
        headers = ["Log_ID", "Timestamp", "Account_ID", "Algorithm", "Tag", "Level", "Message"]

    elif table_key == "positions":
        qs = BrokerPosition.objects.select_related("account").all()
        if account_filter:
            qs = qs.filter(account__account_id=account_filter)
        if start_dt:
            qs = qs.filter(updated_at__gte=start_dt)
        if end_dt:
            qs = qs.filter(updated_at__lte=end_dt)
        qs = qs.order_by("-updated_at")[:limit]
        records = [{
            "Account_ID": p.account.account_id if p.account else "",
            "Tradingsymbol": p.tradingsymbol,
            "Exchange": p.exchange,
            "Quantity": p.quantity,
            "Buy_Price": float(p.buy_price or 0.0),
            "Last_Price": float(p.last_price or 0.0),
            "M2M_PnL": float(p.m2m_pnl or 0.0),
            "Realised_PnL": float(p.realised_pnl or 0.0),
            "Updated_At": p.updated_at.isoformat() if p.updated_at else "",
        } for p in qs]
        headers = ["Account_ID", "Tradingsymbol", "Exchange", "Quantity", "Buy_Price", "Last_Price", "M2M_PnL", "Realised_PnL", "Updated_At"]

    elif table_key == "daily_pnl":
        qs = DailyPnLSnapshot.objects.select_related("account").all()
        if account_filter:
            qs = qs.filter(account__account_id=account_filter)
        if start_dt:
            qs = qs.filter(date__gte=start_dt.date())
        if end_dt:
            qs = qs.filter(date__lte=end_dt.date())
        qs = qs.order_by("-date")[:limit]
        records = [{
            "Account_ID": d.account.account_id if d.account else "",
            "Date": str(d.date),
            "Realised_PnL": float(d.realised_pnl or 0.0),
            "Unrealised_PnL": float(d.unrealised_pnl or 0.0),
            "Net_PnL": float(d.net_pnl or 0.0),
            "Trades_Count": d.trades_count,
            "Starting_Balance": float(d.starting_balance or 0.0),
            "Closing_Balance": float(d.closing_balance or 0.0),
        } for d in qs]
        headers = ["Account_ID", "Date", "Realised_PnL", "Unrealised_PnL", "Net_PnL", "Trades_Count", "Starting_Balance", "Closing_Balance"]

    elif table_key == "trades":
        qs = TradeRecord.objects.select_related("account").all()
        if account_filter:
            qs = qs.filter(account__account_id=account_filter)
        if start_dt:
            qs = qs.filter(trade_time__gte=start_dt)
        if end_dt:
            qs = qs.filter(trade_time__lte=end_dt)
        qs = qs.order_by("-trade_time")[:limit]
        records = [{
            "Account_ID": t.account.account_id if t.account else "",
            "Tradingsymbol": t.tradingsymbol,
            "Transaction_Type": t.transaction_type,
            "Quantity": t.quantity,
            "Price": float(t.price or 0.0),
            "Order_ID": t.order_id,
            "Trade_Time": t.trade_time.isoformat() if t.trade_time else "",
        } for t in qs]
        headers = ["Account_ID", "Tradingsymbol", "Transaction_Type", "Quantity", "Price", "Order_ID", "Trade_Time"]

    # ── Render Formats ────────────────────────────────────────────────────────
    if fmt == "json":
        return HttpResponse(
            orjson.dumps({"table": table_key, "count": len(records), "data": records}),
            content_type="application/json"
        )

    # Excel Workbook
    df = pl.DataFrame(records) if records else pl.DataFrame(schema={c: pl.Utf8 for c in headers})
    
    target_dir = os.path.join(settings.BASE_DIR, 'logs', 'exports')
    os.makedirs(target_dir, exist_ok=True)
    out_file = os.path.join(target_dir, f"{table_key}_{account_id}_{ts_label}.xlsx")
    
    final_path = write_polars_sheets_to_excel({table_key.upper(): df}, out_file)
    
    return FileResponse(
        open(final_path, "rb"),
        as_attachment=True,
        filename=os.path.basename(final_path),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def _clear_table_data(model_class):
    """
    Safely and quickly deletes existing local records from a database table.
    Uses direct SQL DELETE FROM for robust, trigger-safe clearing across both
    PostgreSQL and SQLite without transaction abort or lock contention risks.
    """
    table_name = model_class._meta.db_table
    with connection.cursor() as cursor:
        cursor.execute(f"DELETE FROM {table_name};")


def _build_remote_broker_map(models_data=None, masters_data=None):
    """
    Builds a robust mapping from remote integer account primary keys to local Broker instances.
    Prevents foreign key constraint violations during cross-database replication where auto-increment
    primary keys differ across PostgreSQL environments.
    """
    broker_map = {}
    models_data = models_data or []
    masters_data = masters_data or []

    # 1. Inspect AlgoInfo records for {account_id}_inst_tokens or explicit account_id
    for item in models_data:
        fields = item.get("fields", {}) if isinstance(item, dict) else {}
        r_acc = fields.get("account")
        tbl = fields.get("tablename", "")
        acc_str = fields.get("account_id")
        if r_acc and acc_str and r_acc not in broker_map:
            b = Broker.objects.filter(account_id__iexact=acc_str).first()
            if b:
                broker_map[r_acc] = b

        if r_acc and tbl.endswith("_inst_tokens") and r_acc not in broker_map:
            aid = tbl[:-12]
            b = Broker.objects.filter(account_id__iexact=aid).first()
            if b:
                broker_map[r_acc] = b

    # 2. Inspect ExchangeMasterData contract symbols
    for item in masters_data:
        fields = item.get("fields", {}) if isinstance(item, dict) else {}
        r_acc = fields.get("account")
        acc_str = fields.get("account_id")
        if r_acc and acc_str and r_acc not in broker_map:
            b = Broker.objects.filter(account_id__iexact=acc_str).first()
            if b:
                broker_map[r_acc] = b

        if r_acc and r_acc not in broker_map:
            tdata_str = str(fields.get("tabledata", ""))
            # Crypto symbols
            if any(sym in tdata_str for sym in ["BTC", "ETH", "SOL", "USDT", "USDC", "delta"]):
                b = (
                    Broker.objects.filter(broker_name__code__in=["delta_india", "delta", "coinswitch"], enable_trade=True).first()
                    or Broker.objects.filter(broker_name__code__in=["delta_india", "delta", "coinswitch"]).first()
                )
                if b:
                    broker_map[r_acc] = b
            # Indian symbols
            elif any(sym in tdata_str for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY", "NATGAS", "CRUDE", "MIDCPNIFTY"]):
                b = (
                    Broker.objects.filter(broker_name__code__in=["kotak_neo", "kotak", "zerodha", "angel"], enable_trade=True).first()
                    or Broker.objects.filter(broker_name__code__in=["kotak_neo", "kotak", "zerodha", "angel"]).first()
                )
                if b:
                    broker_map[r_acc] = b

    return broker_map


@csrf_exempt
@staff_or_token_required
@require_http_methods(["POST"])
def clone_remote_data_api(request):
    """
    API endpoint to execute throttled, time-sliced cloud-to-local data cloning.
    Called by Data Hub UI or programmatic agents.
    Automatically purges existing local table data for selected target categories
    (Exchange Masters, Strategy State, Algorithm Logs, Positions & P&L Snapshots)
    before ingesting fresh cloud records.
    """
    import orjson

    try:
        data = orjson.loads(request.body) if request.body else {}
    except Exception:
        data = request.POST.dict()

    remote_host = data.get("remote_host") or getattr(settings, "REMOTE_DB_HOST", "") or _resolve_remote_host()
    if not remote_host or remote_host in ("127.0.0.1", "localhost", "0.0.0.0"):
        return JsonResponse({"success": False, "error": "No remote cloud host IP configured."}, status=400)

    hours = max(1, min(int(data.get("hours", 24)), 168))
    chunk_hours = max(1, min(int(data.get("chunk_hours", 4)), hours))
    pause = max(0.0, float(data.get("pause", 0.5)))
    datasets = set(data.get("datasets", ["masters", "state", "logs", "ticks", "pnl"]))
    clear_tables = data.get("clear_tables", True)

    base_url = f"https://{remote_host}" if not remote_host.startswith("http") else remote_host
    m2m_key = data.get("remote_key") or getattr(settings, "M2M_SERVER_KEY", "") or getattr(settings, "SECRET_KEY", "")
    headers = {"X-Server-Key": m2m_key, "User-Agent": "DeltaZero26-WebCloner/1.0"}

    ssl_verify_param = data.get("ssl_verify")
    if ssl_verify_param is not None:
        if isinstance(ssl_verify_param, str):
            ssl_verify = ssl_verify_param.strip().lower() not in ("false", "0", "no", "f", "off")
        else:
            ssl_verify = bool(ssl_verify_param)
    else:
        ssl_verify = getattr(settings, "SSL_VERIFY", True)

    if not ssl_verify:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    results = {}

    try:
        broker_map = {}

        # 1. Masters (ExchangeMasterData)
        if "masters" in datasets:
            if clear_tables:
                _clear_table_data(ExchangeMasterData)
            m_res = requests.get(f"{base_url}/api/export/masters/", headers=headers, verify=ssl_verify, timeout=(5.0, 30.0))
            if m_res.status_code == 200:
                try:
                    masters_data = orjson.loads(m_res.content) if m_res.content else []
                except Exception:
                    masters_data = []

                if not broker_map:
                    try:
                        s_peek = requests.get(f"{base_url}/api/export/models/", headers=headers, verify=ssl_verify, timeout=(5.0, 15.0))
                        models_peek = orjson.loads(s_peek.content) if s_peek.status_code == 200 and s_peek.content else []
                    except Exception:
                        models_peek = []
                    broker_map = _build_remote_broker_map(models_peek, masters_data)

                saved = 0
                with transaction.atomic():
                    for item in masters_data:
                        fields = item.get("fields", {}) if isinstance(item, dict) else {}
                        r_acc = fields.get("account")
                        acc_str = fields.get("account_id")
                        local_acc = (Broker.objects.filter(account_id__iexact=acc_str).first() if acc_str else None) or broker_map.get(r_acc)
                        m_name = fields.get("master_name", "cum_table")
                        tdata = fields.get("tabledata")
                        rcount = fields.get("row_count") or (len(tdata) if isinstance(tdata, list) else 0)
                        ExchangeMasterData.save_master(
                            account=local_acc,
                            master_name=m_name,
                            data=tdata,
                            row_count=rcount,
                        )
                        saved += 1
                results["Exchange Masters"] = f"Cleared local table; {saved} records imported"
            else:
                results["Exchange Masters"] = f"Cleared local table; fetch returned HTTP {m_res.status_code}"

        # 2. State (AlgoInfo)
        if "state" in datasets:
            if clear_tables:
                _clear_table_data(AlgoInfo)
            s_res = requests.get(f"{base_url}/api/export/models/", headers=headers, verify=ssl_verify, timeout=(5.0, 30.0))
            if s_res.status_code == 200:
                try:
                    state_data = orjson.loads(s_res.content) if s_res.content else []
                except Exception:
                    state_data = []

                if not broker_map:
                    broker_map = _build_remote_broker_map(state_data, [])

                saved = 0
                with transaction.atomic():
                    for item in state_data:
                        fields = item.get("fields", {}) if isinstance(item, dict) else {}
                        r_acc = fields.get("account")
                        acc_str = fields.get("account_id")
                        local_acc = (Broker.objects.filter(account_id__iexact=acc_str).first() if acc_str else None) or broker_map.get(r_acc)
                        tbl = fields.get("tablename")
                        tdata = fields.get("tabledata")
                        if tbl:
                            if tbl.endswith("_inst_tokens") and local_acc:
                                tbl = local_acc.token_tablename
                            AlgoInfo.create_or_update(
                                account=local_acc,
                                tablename=tbl,
                                tabledata=tdata,
                            )
                            saved += 1
                results["Strategy State (AlgoInfo)"] = f"Cleared local table; {saved} records imported"
            else:
                results["Strategy State (AlgoInfo)"] = f"Cleared local table; fetch returned HTTP {s_res.status_code}"

        # 3. Logs (AlgoLog - Chunked)
        if "logs" in datasets:
            if clear_tables:
                _clear_table_data(AlgoLog)
            logs_count = 0
            for offset in range(0, hours, chunk_hours):
                slice_h = min(chunk_hours, hours - offset)
                l_res = requests.get(f"{base_url}/api/export/algo-logs/?hours={slice_h}", headers=headers, verify=ssl_verify, timeout=(5.0, 60.0))
                if l_res.status_code == 200 and len(l_res.content) > 0:
                    with transaction.atomic():
                        with connection.cursor() as cursor:
                            cursor.execute("""
                                CREATE TEMP TABLE temp_algo_logs (
                                    account_id VARCHAR(100),
                                    algo_name VARCHAR(100),
                                    tag VARCHAR(50),
                                    level VARCHAR(20),
                                    message TEXT,
                                    timestamp TIMESTAMPTZ
                                ) ON COMMIT DROP;
                            """)
                            copy_sql = "COPY temp_algo_logs(account_id, algo_name, tag, level, message, timestamp) FROM STDIN WITH CSV HEADER"
                            with cursor.copy(copy_sql) as copy:
                                copy.write(l_res.content)
                            insert_sql = """
                                INSERT INTO kalai_algolog (account_id, algo_name, tag, level, message, timestamp)
                                SELECT b.id, t.algo_name, t.tag, t.level, t.message, t.timestamp
                                FROM temp_algo_logs t
                                LEFT JOIN kalai_broker b ON b.account_id = t.account_id
                                ON CONFLICT DO NOTHING;
                            """
                            cursor.execute(insert_sql)
                            logs_count += cursor.rowcount if cursor.rowcount > 0 else 0
                if pause > 0:
                    time.sleep(pause)
            results["Algorithm Logs"] = f"Cleared local table; {logs_count} records imported"

        # 4. Processed Ticks (COPY stream)
        if "ticks" in datasets:
            ticks_count = 0
            for offset in range(0, hours, chunk_hours):
                slice_h = min(chunk_hours, hours - offset)
                t_res = requests.get(f"{base_url}/api/export/ticks/?hours={slice_h}", headers=headers, verify=ssl_verify, timeout=(5.0, 90.0))
                if t_res.status_code == 200 and len(t_res.content) > 0:
                    with transaction.atomic():
                        with connection.cursor() as cursor:
                            cursor.execute("""
                                CREATE TEMP TABLE temp_ticks (
                                    account_id VARCHAR(100),
                                    data JSONB,
                                    timestamp TIMESTAMPTZ
                                ) ON COMMIT DROP;
                            """)
                            copy_sql = "COPY temp_ticks(account_id, data, timestamp) FROM STDIN WITH CSV HEADER"
                            with cursor.copy(copy_sql) as copy:
                                copy.write(t_res.content)
                            insert_sql = """
                                INSERT INTO kalai_processedtickstore (account_id, data, timestamp)
                                SELECT b.id, t.data, t.timestamp
                                FROM temp_ticks t
                                JOIN kalai_broker b ON b.account_id = t.account_id
                                ON CONFLICT DO NOTHING;
                            """
                            cursor.execute(insert_sql)
                            ticks_count += cursor.rowcount if cursor.rowcount > 0 else 0
                if pause > 0:
                    time.sleep(pause)
            results["Processed Ticks"] = f"{ticks_count} ticks ingested"

        # 5. Positions & P&L Snapshots
        if "pnl" in datasets or "positions" in datasets:
            if clear_tables:
                _clear_table_data(BrokerPosition)
                _clear_table_data(DailyPnLSnapshot)
                _clear_table_data(TradeRecord)
            pnl_saved = 0
            try:
                p_res = requests.get(f"{base_url}/api/export/pnl/", headers=headers, verify=ssl_verify, timeout=(5.0, 45.0))
                if p_res.status_code == 200 and p_res.text.strip():
                    try:
                        pnl_items = orjson.loads(p_res.content) if p_res.content else []
                    except Exception:
                        pnl_items = []
                    if not broker_map:
                        broker_map = _build_remote_broker_map([], [])
                    with transaction.atomic():
                        for item in pnl_items:
                            model_name = item.get("model", "")
                            fields = item.get("fields", {})
                            r_acc = fields.get("account")
                            acc_str = fields.get("account_id")
                            local_acc = (Broker.objects.filter(account_id__iexact=acc_str).first() if acc_str else None) or broker_map.get(r_acc)
                            if not local_acc:
                                local_acc = Broker.objects.filter(enable_trade=True).first()
                            if not local_acc:
                                continue

                            if model_name == "kalai.brokerposition":
                                BrokerPosition.objects.update_or_create(
                                    account=local_acc,
                                    tradingsymbol=fields.get("tradingsymbol", ""),
                                    product=fields.get("product", "NRML"),
                                    defaults={
                                        "instrument_token": fields.get("instrument_token"),
                                        "quantity": fields.get("quantity", 0),
                                        "buy_quantity": fields.get("buy_quantity", 0),
                                        "buy_price": fields.get("buy_price", 0),
                                        "buy_value": fields.get("buy_value", 0),
                                        "sell_quantity": fields.get("sell_quantity", 0),
                                        "sell_price": fields.get("sell_price", 0),
                                        "sell_value": fields.get("sell_value", 0),
                                        "last_price": fields.get("last_price", 0),
                                        "unrealized_pnl": fields.get("unrealized_pnl", 0),
                                        "realized_pnl": fields.get("realized_pnl", 0),
                                        "total_pnl": fields.get("total_pnl", 0),
                                        "is_open": fields.get("is_open", True),
                                        "raw_data": fields.get("raw_data", {}),
                                    }
                                )
                                pnl_saved += 1
                            elif model_name == "kalai.dailypnlsnapshot":
                                date_val = fields.get("date")
                                if date_val:
                                    DailyPnLSnapshot.objects.update_or_create(
                                        account=local_acc,
                                        date=date_val,
                                        defaults={
                                            "realized_pnl": fields.get("realized_pnl", 0),
                                            "unrealized_pnl": fields.get("unrealized_pnl", 0),
                                            "total_pnl": fields.get("total_pnl", 0),
                                            "turnover": fields.get("turnover", 0),
                                            "trades_count": fields.get("trades_count", 0),
                                        }
                                    )
                                    pnl_saved += 1
                            elif model_name == "kalai.traderecord":
                                TradeRecord.objects.create(
                                    account=local_acc,
                                    order_id=fields.get("order_id"),
                                    tradingsymbol=fields.get("tradingsymbol", ""),
                                    product=fields.get("product", "NRML"),
                                    action_type=fields.get("action_type", "BUY"),
                                    quantity=fields.get("quantity", 0),
                                    price=fields.get("price", 0),
                                    value=fields.get("value", 0),
                                    realized_pnl=fields.get("realized_pnl", 0),
                                )
                                pnl_saved += 1
                    results["Positions & P&L Snapshots"] = f"Cleared local tables; {pnl_saved} records imported"
                else:
                    results["Positions & P&L Snapshots"] = f"Cleared local tables; fetch returned HTTP {p_res.status_code}"
            except Exception as e_pnl:
                results["Positions & P&L Snapshots"] = f"Cleared local tables (Remote fetch notice: {e_pnl})"

        return JsonResponse({
            "success": True,
            "message": f"Successfully synchronized remote data from {remote_host} for the last {hours} hours.",
            "details": results
        })
    except requests.exceptions.SSLError as e:
        logger.error(f"SSL certificate verification failed when syncing with {remote_host}: {e}")
        return JsonResponse({
            "success": False,
            "error": f"SSL Certificate Verification Failed: Remote host '{remote_host}' presented a self-signed or untrusted certificate. Check 'Accept Self-Signed SSL' in the Data Hub UI or set SSL_VERIFY=false in your .env configuration."
        }, status=400)
    except Exception as e:
        logger.error(f"Cloner error: {e}", exc_info=True)
        return JsonResponse({"success": False, "error": str(e)}, status=500)


def health_check_view(request):
    """Lightweight health check for Docker container monitoring."""
    from django.db import connection
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        return JsonResponse({"status": "healthy"}, status=200)
    except Exception as e:
        return JsonResponse({"status": "unhealthy", "error": str(e)}, status=500)


def process_and_export_algo_logs(hours=24, account_id=None, algo_name=None, output_path=None):
    """
    Fetches algorithm logs from remote cloud server (when running in Local/Debug mode)
    or directly from local DB (when running on Production Cloud Server),
    parses structured telemetry data, and exports a formatted multi-sheet Excel file.
    """
    import re
    import polars as pl
    from django.utils import timezone
    from datetime import timedelta
    from django.conf import settings
    from algo_trading.algos.polars_excel import write_polars_sheets_to_excel

    remote_host = _resolve_remote_host()
    is_remote_configured = bool(remote_host and remote_host not in ["127.0.0.1", "localhost", "0.0.0.0"])
    is_local_client = not config.is_production

    # 1. When running as a local client and a remote server IP is configured:
    # MUST fetch logs from the cloud server. If unreachable or failing, abort with clear error.
    if is_local_client and is_remote_configured:
        base_url = f"https://{remote_host}"
        logs_url = f"{base_url}/api/export/algo-logs/?hours={hours or 24}"
        m2m_key = getattr(settings, "M2M_SERVER_KEY", "") or getattr(settings, "SECRET_KEY", "")
        headers = {
            "X-Server-Key": m2m_key
        }
        ssl_verify = getattr(settings, "SSL_VERIFY", True)
        try:
            res = requests.get(logs_url, headers=headers, verify=ssl_verify, timeout=(4.0, 30.0))
            if res.status_code == 200 and len(res.content) > 0:
                with transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            CREATE TEMP TABLE IF NOT EXISTS temp_algo_logs (
                                account_id VARCHAR(100),
                                algo_name VARCHAR(100),
                                tag VARCHAR(50),
                                level VARCHAR(20),
                                message TEXT,
                                timestamp TIMESTAMPTZ
                            ) ON COMMIT DROP;
                            TRUNCATE TABLE temp_algo_logs;
                        """)
                        copy_in_sql = "COPY temp_algo_logs(account_id, algo_name, tag, level, message, timestamp) FROM STDIN WITH CSV HEADER"
                        with cursor.copy(copy_in_sql) as copy:
                            copy.write(res.content)

                        insert_sql = """
                            INSERT INTO kalai_algolog (account_id, algo_name, tag, level, message, timestamp)
                            SELECT b.id, t.algo_name, t.tag, t.level, t.message, t.timestamp
                            FROM temp_algo_logs t
                            LEFT JOIN kalai_broker b ON b.account_id = t.account_id
                            ON CONFLICT DO NOTHING;
                        """
                        cursor.execute(insert_sql)
            elif res.status_code in [401, 403]:
                raise RuntimeError(
                    f"Authentication failed with cloud server at {remote_host} (HTTP {res.status_code}). "
                    f"Please verify that DJANGO_SECRET_KEY in your local .env matches the cloud server."
                )
            else:
                raise RuntimeError(
                    f"Cloud server at {remote_host} returned HTTP {res.status_code} ({res.text[:150]})."
                )
        except Exception as e_fetch:
            err_msg = str(e_fetch)
            if "ConnectTimeout" in err_msg or "timed out" in err_msg:
                err_msg = "Connection timed out after 4 seconds (server is unreachable or offline)."
            elif "ConnectionRefused" in err_msg or "actively refused" in err_msg:
                err_msg = "Connection refused (server is not running or port 443 is closed)."
            raise RuntimeError(
                f"Export not possible: Could not reach cloud server at {remote_host}. {err_msg}"
            )

    # 2. Query AlgoLog with optional filters (bounded, memory-efficient values query)
    MAX_EXPORT_RECORDS = 25000
    qs = AlgoLog.objects.all()
    if hours and int(hours) > 0:
        cutoff = timezone.now() - timedelta(hours=int(hours))
        qs = qs.filter(timestamp__gte=cutoff)
    if account_id:
        qs = qs.filter(account__account_id=account_id)
    if algo_name:
        qs = qs.filter(algo_name=algo_name)

    qs_values = list(
        qs.order_by("-timestamp").values(
            "id", "timestamp", "algo_name", "tag", "level", "message",
            "account__account_id", "account__name"
        )[:MAX_EXPORT_RECORDS]
    )

    records = []
    for log in reversed(qs_values):
        msg = log.get("message") or ""
        acc_id = log.get("account__account_id") or "SYSTEM"
        acc_name = log.get("account__name") or "SYSTEM"
        ts = log.get("timestamp")

        # Derived metric extractions
        balance_match = re.search(r"Balance=Rs\.?([0-9,.]+)", msg)
        positions_match = re.search(r"OpenPositions=([0-9]+)", msg)
        orders_match = re.search(r"Orders=([0-9]+)", msg)
        exec_time_match = re.search(r"executed in ([0-9.]+)s", msg)

        lvl = str(log.get("level") or "INFO").upper()
        tag = log.get("tag") or "ALGO"

        category = "GENERAL"
        if "Balance=" in msg or "Synced:" in msg:
            category = "ACCOUNT_SYNC"
        elif "slu" in msg or "Stop-Loss" in msg:
            category = "STOP_LOSS"
        elif "order" in msg.lower() or tag == "TRADE":
            category = "TRADE_ORDER"
        elif lvl in ["ERROR", "EXCEPTION"]:
            category = "ERROR"
        elif tag:
            category = tag

        records.append({
            "Log_ID": log["id"],
            "Timestamp": ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "",
            "Account_ID": acc_id,
            "Account_Name": acc_name,
            "Algorithm": log.get("algo_name") or "",
            "Tag": tag,
            "Level": lvl,
            "Category": category,
            "Balance_Rs": float(balance_match.group(1).replace(",", "")) if balance_match else None,
            "Open_Positions": int(positions_match.group(1)) if positions_match else None,
            "Orders_Count": int(orders_match.group(1)) if orders_match else None,
            "Execution_Time_s": float(exec_time_match.group(1)) if exec_time_match else None,
            "Message": msg,
        })

    all_columns = [
        "Log_ID", "Timestamp", "Account_ID", "Account_Name", "Algorithm",
        "Tag", "Level", "Category", "Balance_Rs", "Open_Positions",
        "Orders_Count", "Execution_Time_s", "Message"
    ]

    df_all = pl.DataFrame(records) if records else pl.DataFrame(schema={c: pl.Utf8 for c in all_columns})

    # 3. Determine Output File Path
    if not output_path:
        target_dir = os.path.join(settings.BASE_DIR, 'logs')
        os.makedirs(target_dir, exist_ok=True)
        acc_str = f"{account_id}_" if account_id else ""
        output_path = os.path.join(target_dir, f"{acc_str}algo_logs_export.xlsx")

    # 4. Create Multi-Sheet Excel Workbook
    # Sheet 1: All Logs
    df_all_export = df_all

    # Sheet 2: Account Sync & Telemetry
    if not df_all.is_empty() and "Category" in df_all.columns:
        cat_cond = pl.col("Category").is_in(["ACCOUNT_SYNC", "TRADE_ORDER", "STOP_LOSS"])
        bal_cond = pl.col("Balance_Rs").is_not_null() if "Balance_Rs" in df_all.columns else pl.lit(False)
        pos_cond = pl.col("Open_Positions").is_not_null() if "Open_Positions" in df_all.columns else pl.lit(False)
        ord_cond = pl.col("Orders_Count").is_not_null() if "Orders_Count" in df_all.columns else pl.lit(False)
        exec_cond = pl.col("Execution_Time_s").is_not_null() if "Execution_Time_s" in df_all.columns else pl.lit(False)
        df_sync = df_all.filter(cat_cond & (bal_cond | pos_cond | ord_cond | exec_cond))
    else:
        df_sync = pl.DataFrame()

    df_sync_export = df_sync if not df_sync.is_empty() else pl.DataFrame({"Info": ["No telemetry sync records found for this period."]})

    # Sheet 3: Errors & Warnings
    if not df_all.is_empty() and "Level" in df_all.columns:
        df_errors = df_all.filter(pl.col("Level").is_in(["ERROR", "WARNING", "EXCEPTION"]))
    else:
        df_errors = pl.DataFrame()

    df_errors_export = df_errors if not df_errors.is_empty() else pl.DataFrame({"Status": ["No errors or warnings recorded in selected time window."]})

    # Sheet 4: Summary Statistics
    summary_data = {
        "Metric": [
            "Total Logs Processed",
            "Unique Accounts",
            "Unique Algorithms",
            "Error Count",
            "Warning Count",
            "Time Window (Hours)",
            "Export Generated At"
        ],
        "Value": [
            len(df_all),
            len(df_all["Account_ID"].unique().to_list()) if not df_all.is_empty() and "Account_ID" in df_all.columns else 0,
            len(df_all["Algorithm"].unique().to_list()) if not df_all.is_empty() and "Algorithm" in df_all.columns else 0,
            len(df_all.filter(pl.col("Level") == "ERROR")) if not df_all.is_empty() and "Level" in df_all.columns else 0,
            len(df_all.filter(pl.col("Level") == "WARNING")) if not df_all.is_empty() and "Level" in df_all.columns else 0,
            hours or "All",
            timezone.now().strftime("%Y-%m-%d %H:%M:%S %Z")
        ]
    }

    final_path = write_polars_sheets_to_excel({
        "All_Logs": df_all_export,
        "Account_Telemetry": df_sync_export,
        "Errors_&_Warnings": df_errors_export,
        "Summary": summary_data,
    }, output_path)

    return {
        "file_path": final_path,
        "row_count": len(df_all),
        "accounts": df_all["Account_ID"].unique().to_list() if not df_all.is_empty() and "Account_ID" in df_all.columns else [],
    }


@staff_member_required
def export_algo_logs_view(request):
    """
    View to process and export AlgoLog entries to Excel in Debug / Staff mode.
    Supports both JSON AJAX response and direct file download.
    Restricted strictly to DEBUG mode to prevent stressing cloud server CPU and memory.
    """
    if not getattr(settings, "DEBUG", False):
        return HttpResponseForbidden(
            "<h2>403 Forbidden — Debug Mode Required</h2>"
            "<p>Exporting algorithm logs to Excel is restricted strictly to Debug / Local mode (<code>settings.DEBUG = True</code>) "
            "to protect server CPU and memory.</p>"
        )

    hours = request.GET.get("hours") or request.POST.get("hours") or 24
    try:
        hours = int(hours)
    except (ValueError, TypeError):
        hours = 24

    account_id = request.GET.get("account_id") or request.POST.get("account_id") or None
    download_requested = request.GET.get("download") in ["true", "1", "True"] or request.POST.get("download") in ["true", "1", "True"]

    try:
        result = process_and_export_algo_logs(hours=hours, account_id=account_id)

        if download_requested:
            return FileResponse(
                open(result["file_path"], "rb"),
                as_attachment=True,
                filename=f"algo_logs_{account_id or 'all'}_{timezone.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        return JsonResponse({
            "success": True,
            "message": f"Successfully processed and exported {result['row_count']} logs to {os.path.basename(result['file_path'])}.",
            "row_count": result["row_count"],
            "file_path": result["file_path"],
            "download_url": f"/broker-admin/export-algo-logs/?download=true&hours={hours}" + (f"&account_id={account_id}" if account_id else "")
        })
    except Exception as e:
        logger.error(f"Error exporting algo logs: {e}", exc_info=True)
        return JsonResponse({"success": False, "error": str(e)}, status=502)


# ─── Deployment Hub & Operations Views ────────────────────────────────────────

@staff_member_required
def deployment_hub_view(request):
    """
    Renders the interactive Linux Deployment Hub operations console in Django Admin.
    Displays live Git metadata, container status, and live ANSI terminal log viewer.
    """
    from django.contrib import admin
    from kalai.deployment import get_git_metadata, is_deployment_active
    from kalai.views import _resolve_remote_host

    git_info = get_git_metadata()
    remote_host = _resolve_remote_host()
    active_deploy = is_deployment_active()

    context = {
        **admin.site.each_context(request),
        "title": "🚀 Cloud Deployment & Git Operations Hub",
        "opts": Broker._meta,
        "git_info": git_info,
        "remote_db_host": remote_host or "",
        "is_deploy_active": active_deploy,
    }
    return render(request, "admin/kalai/deployment_hub.html", context)


@staff_or_token_required
@require_GET
def git_info_api(request):
    """API endpoint returning live Git repository metadata as JSON."""
    from kalai.deployment import get_git_metadata
    return JsonResponse({
        "success": True,
        "git": get_git_metadata(),
    })


@csrf_exempt
@staff_or_token_required
def trigger_deploy_api(request):
    """
    Secure API endpoint to trigger out-of-container deployment on the Linux host.
    Requires Staff session (or M2M Token) and superuser privileges for session users.
    """
    import orjson
    from kalai.deployment import trigger_host_deployment

    # Superuser privilege check for authenticated session users
    user = getattr(request, "user", None)
    if user and user.is_authenticated and not user.is_superuser:
        return JsonResponse({"success": False, "error": "Superuser privileges required to trigger deployment."}, status=403)

    try:
        data = orjson.loads(request.body) if request.body else {}
    except Exception:
        data = request.POST.dict() if request.method == "POST" else {}

    branch = data.get("branch") or request.GET.get("branch") or "main"
    build_val = data.get("build") if "build" in data else request.GET.get("build")
    force_build = build_val in [True, "true", "1", 1, "build"]

    username = user.username if user and user.is_authenticated else "M2M_Token"
    ip = request.META.get("HTTP_X_FORWARDED_FOR") or request.META.get("REMOTE_ADDR") or ""
    if "," in ip:
        ip = ip.split(",")[0].strip()

    result = trigger_host_deployment(branch=branch, operator_username=username, client_ip=ip, force_build=force_build)
    status_code = 200 if result.get("success") else (409 if result.get("status") == "BUSY" else 400)
    return JsonResponse(result, status=status_code)


@staff_or_token_required
@require_GET
def deploy_status_api(request):
    """
    API endpoint streaming incremental terminal execution logs and deployment status.
    """
    from kalai.deployment import get_deployment_status

    job_id = request.GET.get("job_id", "")
    offset_raw = request.GET.get("offset", "0")
    try:
        offset = max(0, int(offset_raw))
    except (ValueError, TypeError):
        offset = 0

    status_data = get_deployment_status(job_id=job_id, offset=offset)
    return JsonResponse({
        "success": True,
        **status_data,
    })


@staff_or_token_required
@require_GET
def admin_hub_events_api(request):
    """
    Live API endpoint providing real-time System Hub telemetry, broker logins,
    credential alerts, and AlgoLog errors with staff authentication enforcement.
    """
    from kalai.context_processors import get_admin_system_hub_data
    force = bool(request.GET.get("force") in ("1", "true", "True"))
    data = get_admin_system_hub_data(force=force)
    return JsonResponse({
        "success": True,
        **data,
    })






