#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kalai/management/commands/clone_remote_data.py
─────────────────────────────────────────────
Robust, throttled Cloud-to-Local synchronization CLI.
Clones operational telemetry, logs, ticks, strategy states, and exchange masters
from the remote production cloud server into the local PostgreSQL instance
with constant-time M2M authentication, time-sliced chunking, and zero-lock queries.
"""

from __future__ import annotations
import time
import requests
import orjson
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connection, transaction
from kalai.models import (
    AlgoInfo,
    ExchangeMasterData,
    AlgoLog,
    BrokerPosition,
    DailyPnLSnapshot,
    TradeRecord,
    Broker,
)
from kalai.views import _clear_table_data, _build_remote_broker_map


class Command(BaseCommand):
    help = "Clone operational data (logs, ticks, states, masters, PnL) from remote cloud production server with chunking and throttle protection."

    def add_arguments(self, parser):
        parser.add_argument(
            "--host",
            type=str,
            default="",
            help="Remote host IP / domain (defaults to settings.REMOTE_DB_HOST or .env REMOTE_DB_HOST)",
        )
        parser.add_argument(
            "--key",
            type=str,
            default="",
            help="M2M authorization server key (defaults to settings.M2M_SERVER_KEY / DJANGO_SECRET_KEY)",
        )
        parser.add_argument(
            "--hours",
            type=int,
            default=24,
            help="Time window in hours for logs and ticks (default: 24, max: 168 [7 days])",
        )
        parser.add_argument(
            "--chunk-hours",
            type=int,
            default=4,
            help="Chunk slice duration in hours to prevent cloud server CPU throttling (default: 4)",
        )
        parser.add_argument(
            "--pause",
            type=float,
            default=0.5,
            help="Polite pause in seconds between remote slice requests (default: 0.5s)",
        )
        parser.add_argument(
            "--include",
            type=str,
            default="logs,ticks,state,masters,pnl,positions",
            help="Comma-separated datasets to clone (logs, ticks, state, masters, pnl, positions, trades)",
        )
        parser.add_argument(
            "--insecure",
            "--no-ssl-verify",
            action="store_true",
            dest="insecure",
            default=False,
            help="Disable SSL/TLS certificate verification (allow self-signed certificates or direct IP access)",
        )

    def handle(self, *args, **options):
        import sys
        if hasattr(sys.stdout, "reconfigure"):
            try:
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
                sys.stderr.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

        remote_host = options["host"] or getattr(settings, "REMOTE_DB_HOST", "")
        if not remote_host:
            from algo_trading.app_config import config
            remote_host = getattr(config, "ALLOWED_HOSTS", "").split(",")[0].strip()

        if not remote_host or remote_host in ("127.0.0.1", "localhost", "0.0.0.0"):
            self.stderr.write(self.style.ERROR(
                "❌ No valid remote host configured! Provide --host <IP_OR_DOMAIN> or set REMOTE_DB_HOST in .env"
            ))
            return

        m2m_key = options["key"] or getattr(settings, "M2M_SERVER_KEY", "") or getattr(settings, "SECRET_KEY", "")
        hours = max(1, min(options["hours"], 168))
        chunk_hours = max(1, min(options["chunk_hours"], hours))
        pause = max(0.0, options["pause"])
        include_set = {x.strip().lower() for x in options["include"].split(",") if x.strip()}

        base_url = f"https://{remote_host}" if not remote_host.startswith("http") else remote_host
        headers = {"X-Server-Key": m2m_key, "User-Agent": "DeltaZero26-LocalCloner/1.0"}

        if options.get("insecure"):
            ssl_verify = False
        else:
            ssl_verify = getattr(settings, "SSL_VERIFY", True)

        if not ssl_verify:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.stdout.write(self.style.SUCCESS("🚀 Initializing Cloud Data Cloner & Synchronizer"))
        self.stdout.write(f"   • Remote Host   : {base_url}")
        self.stdout.write(f"   • SSL Verify    : {ssl_verify}")
        self.stdout.write(f"   • Time Horizon  : Last {hours} hours (Chunk size: {chunk_hours}h, Pause: {pause}s)")
        self.stdout.write(f"   • Datasets      : {', '.join(sorted(include_set))}\n")

        start_time = time.time()
        summary_results = {}
        broker_map = {}

        # 1. Clone Exchange Master Universes (cum_table, etc.)
        if "masters" in include_set:
            self.stdout.write("📦 [1/5] Cloning Exchange Master Universes...")
            _clear_table_data(ExchangeMasterData)
            self.stdout.write(self.style.NOTICE("   🗑️ Purged existing local ExchangeMasterData table"))
            try:
                masters_url = f"{base_url}/api/export/masters/"
                res = requests.get(masters_url, headers=headers, verify=ssl_verify, timeout=(5.0, 45.0))
                if res.status_code == 200:
                    try:
                        masters_data = orjson.loads(res.content) if res.content else []
                    except Exception:
                        masters_data = []

                    if not broker_map:
                        try:
                            s_peek = requests.get(f"{base_url}/api/export/models/", headers=headers, verify=ssl_verify, timeout=(5.0, 15.0))
                            models_peek = orjson.loads(s_peek.content) if s_peek.status_code == 200 and s_peek.content else []
                        except Exception:
                            models_peek = []
                        broker_map = _build_remote_broker_map(models_peek, masters_data)

                    saved_count = 0
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
                            saved_count += 1
                    summary_results["masters"] = f"{saved_count} master tables"
                    self.stdout.write(self.style.SUCCESS(f"   ✓ Ingested {saved_count} ExchangeMasterData records"))
                else:
                    summary_results["masters"] = f"HTTP {res.status_code}"
                    self.stderr.write(self.style.WARNING(f"   ⚠ Masters fetch returned HTTP {res.status_code}"))
            except requests.exceptions.SSLError as e:
                summary_results["masters"] = "SSL Verification Failed"
                self.stderr.write(self.style.ERROR(f"   ❌ SSL Error: {e}\n   👉 Use --insecure or set SSL_VERIFY=false in .env for self-signed certificates."))
                return
            except Exception as e:
                summary_results["masters"] = f"Error: {e}"
                self.stderr.write(self.style.ERROR(f"   ❌ Masters clone failed: {e}"))

        # 2. Clone Runtime Strategy State (AlgoInfo)
        if "state" in include_set or "algoinfo" in include_set:
            self.stdout.write("⚙️  [2/5] Cloning Runtime Strategy State (AlgoInfo)...")
            _clear_table_data(AlgoInfo)
            self.stdout.write(self.style.NOTICE("   🗑️ Purged existing local AlgoInfo table"))
            try:
                models_url = f"{base_url}/api/export/models/"
                res = requests.get(models_url, headers=headers, verify=ssl_verify, timeout=(5.0, 30.0))
                if res.status_code == 200:
                    try:
                        state_data = orjson.loads(res.content) if res.content else []
                    except Exception:
                        state_data = []

                    if not broker_map:
                        broker_map = _build_remote_broker_map(state_data, [])

                    saved_count = 0
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
                                saved_count += 1
                    summary_results["state"] = f"{saved_count} state tables"
                    self.stdout.write(self.style.SUCCESS(f"   ✓ Ingested {saved_count} AlgoInfo records"))
                else:
                    summary_results["state"] = f"HTTP {res.status_code}"
                    self.stderr.write(self.style.WARNING(f"   ⚠ AlgoInfo fetch returned HTTP {res.status_code}"))
            except Exception as e:
                summary_results["state"] = f"Error: {e}"
                self.stderr.write(self.style.ERROR(f"   ❌ AlgoInfo clone failed: {e}"))

        # 3. Clone Algorithm Execution Logs with Chunking
        if "logs" in include_set:
            self.stdout.write("📜 [3/5] Cloning Algorithm Execution Logs...")
            _clear_table_data(AlgoLog)
            self.stdout.write(self.style.NOTICE("   🗑️ Purged existing local AlgoLog table"))
            total_logs_ingested = 0
            # Pull via chunked hours
            for offset in range(0, hours, chunk_hours):
                slice_h = min(chunk_hours, hours - offset)
                logs_url = f"{base_url}/api/export/algo-logs/?hours={slice_h}"
                try:
                    res = requests.get(logs_url, headers=headers, verify=ssl_verify, timeout=(5.0, 60.0))
                    if res.status_code == 200 and len(res.content) > 0:
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
                                    copy.write(res.content)

                                insert_sql = """
                                    INSERT INTO kalai_algolog (account_id, algo_name, tag, level, message, timestamp)
                                    SELECT b.id, t.algo_name, t.tag, t.level, t.message, t.timestamp
                                    FROM temp_algo_logs t
                                    LEFT JOIN kalai_broker b ON b.account_id = t.account_id
                                    ON CONFLICT DO NOTHING;
                                """
                                cursor.execute(insert_sql)
                                total_logs_ingested += cursor.rowcount if cursor.rowcount > 0 else 0
                    if pause > 0:
                        time.sleep(pause)
                except Exception as e:
                    self.stderr.write(self.style.WARNING(f"   ⚠ Logs slice offset {offset}h failed: {e}"))

            summary_results["logs"] = f"{total_logs_ingested} records"
            self.stdout.write(self.style.SUCCESS(f"   ✓ Ingested {total_logs_ingested} Algorithm Log records"))

        # 4. Clone Processed Ticks with Time-Sliced Chunking
        if "ticks" in include_set:
            self.stdout.write("⏱️  [4/5] Cloning Processed Ticks (Chunked Streaming)...")
            total_ticks_ingested = 0
            for offset in range(0, hours, chunk_hours):
                slice_h = min(chunk_hours, hours - offset)
                ticks_url = f"{base_url}/api/export/ticks/?hours={slice_h}"
                try:
                    res = requests.get(ticks_url, headers=headers, verify=ssl_verify, timeout=(5.0, 90.0))
                    if res.status_code == 200 and len(res.content) > 0:
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
                                    copy.write(res.content)

                                insert_sql = """
                                    INSERT INTO kalai_processedtickstore (account_id, data, timestamp)
                                    SELECT b.id, t.data, t.timestamp
                                    FROM temp_ticks t
                                    JOIN kalai_broker b ON b.account_id = t.account_id
                                    ON CONFLICT DO NOTHING;
                                """
                                cursor.execute(insert_sql)
                                total_ticks_ingested += cursor.rowcount if cursor.rowcount > 0 else 0
                    if pause > 0:
                        time.sleep(pause)
                except Exception as e:
                    self.stderr.write(self.style.WARNING(f"   ⚠ Ticks slice offset {offset}h failed: {e}"))

            summary_results["ticks"] = f"{total_ticks_ingested} ticks"
            self.stdout.write(self.style.SUCCESS(f"   ✓ Ingested {total_ticks_ingested} Ticks"))

        # 5. Clone Positions & P&L Snapshots
        if "pnl" in include_set or "positions" in include_set:
            self.stdout.write("📈 [5/5] Cloning Positions & P&L Snapshots...")
            _clear_table_data(BrokerPosition)
            _clear_table_data(DailyPnLSnapshot)
            _clear_table_data(TradeRecord)
            self.stdout.write(self.style.NOTICE("   🗑️ Purged existing local Positions & P&L tables"))
            try:
                pnl_url = f"{base_url}/api/export/pnl/"
                res = requests.get(pnl_url, headers=headers, verify=ssl_verify, timeout=(5.0, 45.0))
                if res.status_code == 200 and res.text.strip():
                    try:
                        pnl_items = orjson.loads(res.content) if res.content else []
                    except Exception:
                        pnl_items = []
                    if not broker_map:
                        broker_map = _build_remote_broker_map([], [])
                    saved_count = 0
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
                                saved_count += 1
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
                                    saved_count += 1
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
                                saved_count += 1
                    summary_results["pnl"] = f"{saved_count} records"
                    self.stdout.write(self.style.SUCCESS(f"   ✓ Ingested {saved_count} Positions & P&L records"))
                else:
                    summary_results["pnl"] = f"HTTP {res.status_code}"
                    self.stderr.write(self.style.WARNING(f"   ⚠ PnL fetch returned HTTP {res.status_code}"))
            except Exception as e:
                summary_results["pnl"] = f"Error: {e}"
                self.stderr.write(self.style.ERROR(f"   ❌ PnL clone failed: {e}"))

        elapsed = time.time() - start_time
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(self.style.SUCCESS(f"🎉 Cloud Synchronization Finished in {elapsed:.2f}s"))
        for k, v in summary_results.items():
            self.stdout.write(f"   • {k.capitalize():15s}: {v}")
        self.stdout.write("=" * 60 + "\n")
