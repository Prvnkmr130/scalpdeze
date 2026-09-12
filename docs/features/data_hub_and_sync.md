# Data Hub & Cloud Synchronizer

The **Data Hub & Cloud Synchronizer** is a unified operational and debugging interface within DeltaZero26. It centralizes multi-table data exports across custom time horizons, provides zero-lock streaming downloads, and powers a rate-limited, chunked cloud-to-local synchronization pipeline for local development and debugging without straining production servers.

---

## 1. Architectural Highlights

### A. Dedicated `ExchangeMasterData` Isolation
Prior architectures stored multi-megabyte pre-computed contract universes (`cum_table`, `aug_table`) within `kalai_algoinfo`. This resulted in high JSON serialization overhead during routine state updates.

- **`ExchangeMasterData` (`kalai_exchangemaster`)**: Dedicated table storing large static exchange datasets.
- **`AlgoInfo` (`kalai_algoinfo`)**: Kept strictly lean (< 50 KB total per account) for operational trading states, stop-loss counters, and live candle caches.
- **Engine Recovery**: All production engines ([`zerodha_opt_trde_polars.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/zerodha_opt_trde_polars.py), [`kotak_opt_trde_polars.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/kotak_opt_trde_polars.py), and [`coinswitch_opt_trde_polars.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/coinswitch_opt_trde_polars.py)) persist and restore master universes via `Broker.get_master_data()` and `Broker.set_master_data()`.

---

## 2. Unified Data Hub Interface (`/admin/kalai/broker/data-hub/` & `/data-hub/`)

The Data Hub web dashboard provides two operational modes:

### 📥 Tab 1: Export & Download Dataset
Enables filtering and downloading data from any of the 7 core DeltaZero26 tables:
1. **📜 Algorithm Logs (`kalai_algolog`)**: Structured strategy execution, telemetry, and error logs.
2. **⏱️ Processed Ticks (`kalai_processedtickstore`)**: High-resolution market tick stream archives.
3. **🏛️ Exchange Masters (`kalai_exchangemaster`)**: Static derivative universes (`cum_table`).
4. **⚙️ Strategy State (`kalai_algoinfo`)**: Dynamic runtime state and parameter configurations.
5. **💼 Live Positions (`kalai_broker_position`)**: Open and historical broker positions.
6. **📈 Daily P&L Snapshots (`kalai_daily_pnl_snapshot`)**: Daily realized/unrealized P&L records.
7. **🧾 Trade Records (`kalai_traderecord`)**: Executed order history.

#### Export Filters & Parameters:
- **Account Filter**: `All Accounts` or specific accounts (`HS6525`, `W1NPY`, `Prvn_coinswitch`).
- **Time Horizons**: `Today`, `Yesterday`, `Last 7 Days`, `Last 30 Days`, `All Time`, or `Custom Date Range` (with start and end pickers).
- **Supported Formats**:
  - **Excel (`.xlsx`)**: Formatted multi-sheet workbooks generated via Polars and `polars_excel`.
  - **Streaming CSV (`.csv`)**: High-throughput zero-lock streaming using PostgreSQL `COPY TO STDOUT`.
  - **Structured JSON (`.json`)**: Fast JSON serialization using `orjson`.

#### Full Tabledata Extraction for Exchange Masters (`cum_table`) & Strategy State:
When exporting **Exchange Masters** (`exchangemaster`) or **Strategy State** (`algoinfo`), the endpoint unpacks the actual underlying records from `tabledata` into native Polars DataFrames rather than exporting high-level metadata:
- **Excel Multi-Sheet Hierarchy**:
  - **`CUM_TABLE` (Premier Active Sheet)**: Opens directly on load with all combined contracts across selected accounts (`Account_ID` as the leading column) via relaxed schema concatenation (`how="diagonal_relaxed"`).
  - **Per-Broker Account Worksheets**: Individual sheets (`cum_table_W1NPY`, `cum_table_73270496`, `aug_table_...`) preserve each broker's native schema and columns without null padding.
  - **`MASTERS_INFO` / `STATE_SUMMARY`**: Summary worksheet placed at the end detailing row counts, update timestamps, and storage sizes.
- **CSV & JSON Exports**: Export the actual contract records and state dictionaries directly into CSV rows and JSON payloads.

---

### 🚀 Tab 2: Cloud-to-Local Cloner & Synchronizer

Allows developers running a local instance (or debugging node) to pull operational data from the remote production cloud server without causing CPU spikes or network bottlenecks.

#### Key Characteristics & Guardrails:
- **Fresh Sync Table Purge**: When **"Start Cloud Synchronization"** is triggered, existing local records in selected target categories are automatically cleared prior to importing fresh remote data, preventing stale state contamination:
  1. **Exchange Masters**: Clears `kalai_exchangemaster` (`ExchangeMasterData`).
  2. **Strategy State**: Clears `kalai_algoinfo` (`AlgoInfo`).
  3. **Algorithm Logs**: Clears `kalai_algolog` (`AlgoLog`).
  4. **Positions & P&L Snapshots**: Clears `kalai_brokerposition` (`BrokerPosition`), `kalai_dailypnlsnapshot` (`DailyPnLSnapshot`), and `kalai_traderecord` (`TradeRecord`).
  - *Note*: High-resolution tick archives (`kalai_processedtickstore`) are preserved and merged via `ON CONFLICT DO NOTHING`.
- **Automatic `.env` Host Resolution**: The target remote host is resolved dynamically from `.env` (`REMOTE_DB_HOST` / `SERVER_IP`) via `_resolve_remote_host()`. The web UI displays an informative status badge (`☁️ Target Host: ... (from .env)`) without requiring manual IP entries on the dashboard.
- **Time-Sliced Chunking**: Automatically slices requests spanning 24 hours to 7 days into discrete 2–4 hour windows.
- **Polite Pauses (`0.5s`)**: Enforces a non-blocking pause between remote slice requests to prevent cloud server CPU or bandwidth starvation.
- **Streaming Ingestion**: Bulk-ingests stream data directly into PostgreSQL temporary tables using `COPY FROM STDIN` inside atomic transactions.
- **Cross-Environment Broker Foreign Key Remapping**: 
  - Database primary keys (`kalai_broker.id`) are auto-incrementing integers that differ across PostgreSQL instances (e.g., Kotak `W1NPY` may be `id=1` on remote but `id=70` locally).
  - To prevent foreign key constraint violations (`Key (account_id)=(1) is not present in table "kalai_broker"`), the cloner utilizes `_build_remote_broker_map()`:
    1. **Canonical Account ID**: Uses explicit string `account_id` if provided in the export payload.
    2. **Token Tablename Discovery**: Scans `AlgoInfo` records for `{account_id}_inst_tokens` to map remote numeric IDs directly to local brokers.
    3. **Underlying Contract Inference**: Inspects symbols in contract universes (`BTC`, `ETH`, `SOL` $\to$ Delta Crypto `73270496`; `NIFTY`, `NATGAS` $\to$ Kotak Indian `W1NPY`).
    4. **Safe Nullable Fallback**: For nullable relationships (`ExchangeMasterData`, `AlgoInfo`), falls back to `None` (Global) rather than aborting synchronization if a broker does not exist locally.
  - Ingestion executes via domain model classmethods (`ExchangeMasterData.save_master`, `AlgoInfo.create_or_update`, and `BrokerPosition.objects.update_or_create`) rather than raw `obj.save()`, guaranteeing sequence safety and idempotency.
- **Fast `orjson` Serialization**: Export APIs (`/api/export/masters/`, `/api/export/models/`, `/api/export/pnl/`) stream compact JSON serialized via `orjson`, embedding canonical `account_id` strings and eliminating `call_command('dumpdata')` overhead.
- **TLS / SSL Certificate Verification**: 
  - By default, outbound HTTPS connections verify certificates against the system CA bundle via `settings.SSL_VERIFY`.
  - When connecting to remote servers via direct IP address (e.g. `https://172.235.29.89`) or when using self-signed development certificates, check **"Accept Self-Signed SSL / Insecure TLS"** in the UI or set `SSL_VERIFY=false` in `.env`.
  - The UI automatically pre-checks this option if the resolved remote host is a raw IPv4 address or if `SSL_VERIFY=false` is configured.
  - The CLI supports `--insecure` (or `--no-ssl-verify`) to bypass certificate validation.

---

## 3. Command-Line Interface (CLI)

You can run data cloning directly via Django's management CLI:

```bash
# Clone the last 24 hours of logs, masters, states, and ticks in 4-hour chunks (allowing self-signed/IP SSL)
python manage.py clone_remote_data --hours 24 --chunk-hours 4 --pause 0.5 --include logs,ticks,masters,state,pnl --insecure

# Clone only exchange masters and strategy states
python manage.py clone_remote_data --include masters,state --insecure

# Clone with custom remote host and key
python manage.py clone_remote_data --host 172.235.29.89 --key YOUR_M2M_KEY --hours 48 --insecure
```

### Supported CLI Flags:
| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--host` | `str` | `.env REMOTE_DB_HOST` | Remote server IP / domain. |
| `--key` | `str` | `M2M_SERVER_KEY` | Machine-to-machine cryptographic authentication key. |
| `--hours` | `int` | `24` | Total time window in hours to fetch (max: 168 / 7 days). |
| `--chunk-hours` | `int` | `4` | Slice duration per HTTP request (throttle guard). |
| `--pause` | `float` | `0.5` | Sleep duration (in seconds) between slice requests. |
| `--include` | `str` | `logs,ticks,state,masters,pnl,positions` | Comma-separated list of datasets to clone. |
| `--insecure` | `bool` | `False` (or `SSL_VERIFY`) | Bypass SSL/TLS certificate validation (for IP / self-signed certs). |

---

## 4. Django Admin Performance & Column Deferral

To ensure instant changelist rendering (< 15ms) across multi-megabyte `ExchangeMasterData` tables in Django Admin:

- **`.defer("tabledata")` in `ExchangeMasterDataAdmin.get_queryset`**: Prevents PostgreSQL from transferring 8–20 MB JSON arrays during list views, completely eliminating HTTP 504 Gateway Timeouts.
- **`exclude = ("tabledata",)` in Change Form**: Bypasses rendering huge raw JSON strings in HTML textareas; displays a lightweight 3-item preview (`tabledata_summary_view`) and a direct link to the Data Hub exporter.

---

## 5. Security & Authentication Guardrails

- **Cryptographic Constant-Time Auth**: Dual-layer `@staff_or_token_required` decorator checks `X-Server-Key` or `Authorization: Bearer <KEY>` using `secrets.compare_digest`.
- **Zero Credential Serialization**: `kalai.Broker` models are strictly excluded from API model dumps.
- **CSRF Exemption for Programmatic Endpoints (`@csrf_exempt`)**: The `/api/data-hub/clone/` endpoint is decorated with `@csrf_exempt` alongside `@staff_or_token_required` to allow machine-to-machine, CLI (`manage.py clone_remote_data`), and frontend asynchronous AJAX requests without CSRF token rejections or HTML 403 errors.
- **Extended Nginx Gateway Timeout (`proxy_read_timeout 600s`)**: In `nginx.conf`, the `/api/data-hub/clone/` location is configured with an extended `proxy_read_timeout 600s;` (compared to the standard 60s default). This ensures long-running, multi-chunk synchronous database synchronization runs to completion without triggering HTTP 504 Gateway Timeouts.
- **Browser Password Autofill Isolation**: Form fields in `templates/admin/kalai/data_hub.html` avoid generic `<input type="password">` types for server secret keys (`X-Server-Key`). This prevents browser password managers from erroneously autofilling the user's admin login credentials over the cryptographic server key, which previously caused remote HTTP 401 Unauthorized rejections.

---

## 6. Algo Monitoring & Operations Hub (`/admin/kalai/broker/algo-monitoring/`)

A centralized operations dashboard unifying all 3 real-time monitoring tools:
1. **⚡ Algorithm Status Monitor** (`/admin/kalai/broker/algo-status/`): Real-time strategy heartbeats and execution loops.
2. **📊 Positions & P&L Analytics** (`/admin/kalai/broker/positions-pnl/`): Live M2M valuations, realized profit tracking, and multi-period analysis.
3. **📦 Data Hub & Cloud Sync** (`/admin/kalai/broker/data-hub/`): Dataset exports and throttled cloner.
