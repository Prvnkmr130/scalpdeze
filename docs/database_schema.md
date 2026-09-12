# Database Schema

DeltaZero26 utilizes PostgreSQL as its primary database. The schema is defined through Django's ORM within `kalai/models.py` along with high-throughput raw time-series tables for market stream data and system logging.

This document details all core models, tables, indexes, and their architectural purposes.

---

## Core Django ORM Models

### 1. `BrokerType`
Stores static information about supported brokerage firms.
- **`code` (PK):** A short string identifier (e.g., `'zerodha'`, `'kotak_neo'`, `'coindcx'`, `'tradovate'`, `'angel'`, `'delta'`).
- **`name`:** A human-readable display name (e.g., `'Zerodha'`, `'Kotak Neo'`, `'CoinDCX'`).

### 2. `ApiProvider`
Stores static information about the API feeds used for market data and execution. In many cases, this is the same as the BrokerType, but it allows for decoupling (e.g., trading through one broker, but streaming data from another provider).
- **`code` (PK):** A short string identifier (e.g., `'zerodha'`, `'kotak_neo'`, `'coindcx'`, `'tradovate'`).
- **`name`:** A human-readable display name.

### 3. `Broker` (Account)
The central configuration table for a user's trading account and its connection settings.
- **Identity & Credentials:**
  - `account_id`: Unique string identifying the account (e.g., `'HS6525'`, `'PR45134584'`).
  - `name`: Friendly name for the account (e.g., `'prvn_zerodha'`, `'coindcx'`).
  - `broker_name`: ForeignKey to `BrokerType`.
  - `api_provider`: ForeignKey to `ApiProvider`.
  - `api_key`, `api_secret`: API authentication credentials.
  - `api_secret_updated_at`: Timestamp tracking when the API Secret was last created or modified.
  - `enable_secret_key_expiry`: Boolean. Enables secret key validity lifecycle tracking and warning alerts (auto-defaults to `True` for CoinSwitch PRO, `False` for other brokers).
  - `secret_key_validity_days`: Positive integer (default: 90). Validity duration in days before key expires.
  - `secret_key_warn_days`: Positive integer (default: 7). Warning notification threshold in days before key expiration.
  - `totp_secret`: Secret key used for programmatic TOTP generation during automated logins (automatically sanitized to standard Base32 on save).
  - `access_token`, `access_token_updated_at`, `refresh_token`: Tokens acquired after OAuth/login handshakes.
  - `redirect_url`, `base_redirect_url`, `api_endpoint`: Deterministic URLs auto-generated for OAuth handshakes.
- **Feature Flags:**
  - `enable_trade`: Boolean. If true, the Algo Engine is authorized to execute trades on this account.
  - `enable_websocket`: Boolean. If true, the WebSocket Engine will attempt to connect and stream tick data for this account.
- **Scheduling Config:**
  - `enable_schedule`: Boolean. Determines if the connection should follow a time-based schedule. Defaults to `False` for cryptocurrency accounts to guarantee 24/7/365 uninterrupted operation; defaults to `True` for Indian equity brokers with regular exchange hours.
  - `ws_start_time`, `ws_stop_time`: Daily operating hours (evaluated only when `enable_schedule=True` on non-crypto accounts).
  - `ws_operating_days`: Enum (`'WEEKDAYS'`, `'ALL'`).
- **Core Methods & Helpers:**
  - **`is_crypto`**: Common model property returning `True` if the broker is a cryptocurrency provider (e.g. Delta Exchange, CoinSwitch PRO, CoinDCX). Enforces 24/7 runtime execution without broker-specific flag proliferation.
  - **`get_totp()`**: Generates the live 6-digit TOTP for this account with network clock drift compensation.
  - **`secret_key_expires_at` / `days_until_secret_key_expiry`**: Computes exact expiry datetime and remaining days when `enable_secret_key_expiry=True`.
  - **`secret_key_expiry_status`**: Expiry status classification (`'ACTIVE'`, `'EXPIRING_SOON'`, `'EXPIRED'`, `'NOT_SET'`, or `'NOT_APPLICABLE'`).
  - **`notify_engine_on_account_save` / `notify_engine_on_account_delete` (Signals):** When a `Broker` record is created, updated, or deleted, a PostgreSQL `pg_notify` command is fired on the `engine_control` and `algo_control` channels. This allows running background engines to instantly reload their configurations or stop inactive accounts without restarting the container.
  - **`auto_populate_redirect_url(request)`**: Automatically computes and stores `https://<domain>/broker-admin/callback/<account_id>/`.
  - **`get_subscribed_tokens()`**: Returns the live subscribed instrument tokens for *this* account from `AlgoInfo`.
  - **`set_subscribed_tokens(tokens)`**: Atomically updates or creates the `{account_id}_inst_tokens` table for *this* account in `AlgoInfo`.
  - **`get_algo_state(tablename)` / `set_algo_state(tablename, data)`**: Safely reads/writes custom strategy state scoped strictly to *this* account.
  - **`Broker.resolve(account_id)`**: Class method that safely loads a `Broker` instance with eager foreign keys.

### 4. `AlgoLog` (`kalai_algolog`)
**Dedicated database table exclusively for persisting structured algorithm execution, lifecycle events, trade signals, performance metrics, and error traces.**
- **`account` (FK):** ForeignKey to `Broker` (`on_delete=models.SET_NULL`, nullable). Binds the log entry to a specific trading account or marks system-wide logs as `null`. When an account is deleted from the DB, historical logs are preserved with `account=null` rather than cascading deletion.
- **`algo_name` (CharField, Indexed):** Strategy identifier (e.g., `'zerodha_opt'`, `'sample_algo'`).
- **`tag` (CharField, Indexed):** Category tag (e.g., `'GENERAL'`, `'TRADE'`, `'STRIKE'`, `'PERF'`, `'ERROR'`).
- **`level` (CharField, Indexed):** Log severity level (`'INFO'`, `'WARNING'`, `'ERROR'`, `'EXCEPTION'`).
- **`message` (TextField):** Full log message body. Utilizes PostgreSQL native `TEXT` datatype supporting strings up to 1 GB per row, with transparent PostgreSQL **TOAST compression** for large JSON dumps and error traces.
- **`timestamp` (DateTimeField, Indexed):** ISO datetime when the event occurred.
- **Compound Indexes:**
  - `['-timestamp']`
  - `['account', '-timestamp']`
  - `['tag', '-timestamp']`
  - `['level', '-timestamp']`
- **Django Admin Integration:** Fully accessible via `AlgoLogAdmin` with color-coded severity badges, filter sidebars, and full-text search.

### 5. `ExchangeMasterData` (`kalai_exchangemaster`)
**Dedicated database table exclusively for persisting heavy, daily static exchange contract universes and instrument master scrips.**
- **`account` (FK):** Reference to the associated `Broker` (`on_delete=models.CASCADE`, nullable).
- **`master_name` (CharField, Indexed):** Identifier of the static master dataset (e.g. `'cum_table'`, `'aug_table'`, `'coindcx_instruments'`).
- **`row_count` (IntegerField):** Number of contracts / records stored in the master array for instant inspection without parsing JSON.
- **`updated_at` (DateTimeField, Indexed):** Timestamp of when the master table was last resolved and persisted.
- **`tabledata` (JSONField):** Complete serialized contract universe (e.g., 6,000+ rows × 67 columns for Zerodha/Kotak, or 700+ pairs for CoinSwitch).
- **Compound Database Indexes:**
  - `['account', 'master_name']`
  - `['master_name']`
  - `['-updated_at']`
- **Architectural Purpose:** Completely isolates multi-megabyte daily static universes from `AlgoInfo`, keeping `kalai_algoinfo` lean (< 50 KB) for runtime strategy counters, stop-loss states, and live candle caches.

### 6. `AlgoInfo` (`kalai_algoinfo`)
A high-performance algorithmic state-storage table used strictly for **lightweight runtime state persistence**, broker instrument caches, and token routing:
- **`account` (FK):** Reference to the associated `Broker` (`on_delete=models.SET_NULL`, nullable). Preserves state entries and token records when an account is deleted.
- **`tablename` (CharField, Indexed):** A string key denoting the runtime state data type:
  - **`{account_id}_inst_tokens`**: **Universal Token Subscription Table** storing `{"tokenid": [...]}` for live WebSocket stream subscriptions.
  - **`master_tkn_list_update_time`**: Daily ISO timestamp of instrument resolution.
  - **`stock_data_info` / `cap_config`**: Lightweight parameters and configuration metadata.
  - **`nse_holiday_info` / `bit_holiday_info`**: Market trading holiday calendars.
  - **`zerodha_stop_loss` / `stop_loss_info`**: Live active stop-loss monitoring states.
  - **`zerodha_strike_entry` / `zerodha_strike_exit`**: Candidate strikes and exit conditions.
  - **Candle Buffers (`fwd_10_all`, `fwd_30_all`, `day_cdl_all`)**: Active multi-timeframe aggregated candles.
- **`is_pinned` (BooleanField, Indexed, default=False):** Pin priority flag enabling critical tables (e.g. active positions, tokens, or custom states) to stay permanently pinned at the top of the Django Admin changelist view.
- **`tabledata` (JSONField):** Arbitrary JSON payload representing the state.
- **`timestamp` (DateTimeField, Indexed):** Automatically updated timestamp of the last modification.
- **Compound Database Indexes:**
  - `['account', 'tablename']` (`kalai_algoi_account_5bd418_idx`): Enables instant sub-millisecond lookup for account-scoped tables.
  - `['tablename']` (`kalai_algoi_tablena_007992_idx`): Fast filtering across all accounts by table type.
  - `['-is_pinned', '-timestamp']` (`kalai_algoi_is_pinn_014d49_idx`): Sub-millisecond composite sorting for pinned admin changelist views.
  - `['-timestamp']` (`kalai_algoi_timesta_69f0dd_idx`): High-speed descending ordering for timestamped queries.
- **Django Admin Integration:**
  - **Inline 1-Click Pinning**: Directly editable checkboxes via `list_editable = ('is_pinned',)`.
  - **Bulk Actions**: Includes `📌 Pin selected tables to top` and `Unpin selected tables` actions.
- **Guardrails:**
  - `clean()`: Enforces that any table ending in `_inst_tokens` strictly matches `account.token_tablename`, throwing a `ValidationError` if mismatched to prevent cross-account pollution.
  - `create_or_update(account, tablename, tabledata)`: Safely upserts state data without unique constraint race conditions.

### 6. `BrokerPosition` (`kalai_brokerposition`)
Unified multi-broker position table capturing active intraday and overnight holdings:
- **`account` (FK):** Associated `Broker` account.
- **`tradingsymbol` (CharField):** Scrip/symbol name (e.g. `'NIFTY26AUG24500CE'`, `'BTCUSDT'`).
- **`instrument_token` (BigIntegerField, Nullable):** Broker instrument token.
- **`product` (CharField):** Product type (e.g. `'NRML'`, `'MIS'`, `'FUT'`, `'SPOT'`).
- **`quantity` (DecimalField):** Current open quantity (positive for LONG, negative for SHORT, 0 for closed).
- **`buy_quantity`, `buy_price`, `buy_value`**: Executed buy metrics.
- **`sell_quantity`, `sell_price`, `sell_value`**: Executed sell metrics.
- **`last_price` (DecimalField):** Latest market price (LTP) or mark price.
- **`unrealized_pnl` (DecimalField):** Live mark-to-market (M2M) P&L.
- **`realized_pnl` (DecimalField):** Realized book profit/loss.
- **`total_pnl` (DecimalField):** Sum of realized and unrealized P&L.
- **`is_open` (BooleanField, Indexed):** `True` for open positions, `False` for closed/squared-off positions.
- **`raw_data` (JSONField):** Complete un-truncated raw broker position response payload.
- **`updated_at` (DateTimeField, auto_now=True, Indexed):** Last synchronization timestamp.
- **Compound Constraints & Indexes:**
  - `unique_together = [('account', 'tradingsymbol', 'product')]`
  - `['account', 'is_open', '-updated_at']`

### 7. `TradeRecord` (`kalai_traderecord`)
**Long-term 1+ year trading execution ledger** designed for comprehensive multi-month and annual performance inference:
- **`account` (FK):** Associated `Broker` account.
- **`order_id` (CharField, Indexed):** Broker unique order identifier.
- **`tradingsymbol` (CharField, Indexed):** Traded instrument symbol.
- **`product` (CharField):** Product type (`'NRML'`, `'MIS'`, etc.).
- **`action_type` (CharField):** Order side (`'BUY'`, `'SELL'`).
- **`quantity`, `price`, `value`**: Trade fill volume and average price.
- **`realized_pnl` (DecimalField):** Realized gain/loss recorded upon fill.
- **`brokerage`, `taxes`**: Transaction overhead tracking.
- **`executed_at` (DateTimeField, Indexed):** Timestamp of trade fill.
- **Retention**: **Preserved permanently for at least 1 year** (exempt from daily log purge routines).

### 8. `DailyPnLSnapshot` (`kalai_dailypnlsnapshot`)
Pre-aggregated daily performance snapshots enabling sub-2ms historical P&L analytics over arbitrary date ranges (7D, 30D, 90D, YTD, 1-Year, Custom):
- **`account` (FK):** Associated `Broker` account.
- **`date` (DateField, Indexed):** Snapshot date.
- **`realized_pnl`, `unrealized_pnl`, `net_pnl`**: Daily performance totals.
- **`total_trades`, `winning_trades`, `losing_trades`**: Daily execution counts.
- **`turnover` (DecimalField):** Daily gross traded value.
- **`notes` (TextField):** Optional daily notes or anomaly details.
- **Compound Constraints & Indexes:**
  - `unique_together = [('account', 'date')]`
  - `['account', '-date']`
  - `['-date']`

### 9. `ProcessedTickStore` (`kalai_processedtickstore`)
A high-throughput table designed to store normalized incoming market tick data for fast analytical queries and strategy ingestion:
- **`account` (FK):** Reference to the `Broker` that received the data (`on_delete=models.SET_NULL`, nullable). Preserves tick data when an account is deleted.
- **`data` (JSONField):** The normalized tick payload received from the broker's WebSocket.
- **`timestamp` (DateTimeField, Indexed):** Time the tick was processed (UTC / timezone-aware).
- **Compound Database Indexes:**
  - `['account', '-timestamp']` (`kalai_proce_account_eb30c6_idx`): Enables instant index-scan retrieval of the most recent tick series per broker account for 2.0s strategy cycles.
  - `['account', 'timestamp']` (`kalai_proce_account_6cc742_idx`): Fast range queries across time windows for historical tick reconstruction.
  - `['-timestamp']` (`kalai_proce_timesta_796de5_idx`): High-speed descending sorting for global recent tick retrieval.

---

## High-Throughput Stream & System Tables

### 10. `{safe_account_name}_stream_kv` (Stream Partition Tables)
Per-account high-performance time-series tables auto-created by `ensure_broker_table()` in the WebSocket engine.
- **Weekly Date-Range Partitioning:** Automatically creates weekly partitions (`{table_name}_yYYYY_wWW`) via database triggers.
- **Schema:**
  - `timestamp`: BigInt epoch nanoseconds.
  - `broker`: Broker identifier string.
  - `data`: JSONB raw tick payload.
  - `log_date`: Date used for partition pruning.
- **Ingestion:** Populated via asynchronous binary `COPY` operations for maximum throughput and near-zero latency.

### 8. `system_logs`
A high-throughput raw logging table created directly via raw SQL in the WebSocket engine (`algo_trading/brokers/engine.py`).
- **`timestamp`:** Timestamp with timezone of the event.
- **`level`:** The log level (e.g., `'INFO'`, `'ERROR'`).
- **`context`:** A string identifying the source of the log (e.g., `'ENGINE'`, `'SYSTEM'`).
- **`message`:** The actual log message text.
- **`log_date`:** Date of the log.

---

## Automated Database Maintenance (`kalai/tasks.py`)

DeltaZero26 runs automated background maintenance tasks via the Django-Q worker cluster and the asynchronous WebSocket broker engine to ensure database storage remains lean without impacting live trading operations:

1. **`clear_old_logs(days=2, chunk_size=5000, sleep_interval=0.05)`**:
   - Automatically purges records older than $N$ days (default: 2 days) from **both** the raw `system_logs` table (via `ctid` Tid Scan) and the dedicated structured [`AlgoLog`](file:///c:/Users/Admin/Documents/deltazero26/kalai/models.py) (`kalai_algolog`) table (via indexed `id`).
   - Executes deletions in bounded chunks of 5,000 rows with a 0.05s pause between chunks, preventing lock escalation and CPU spikes.
2. **`clear_old_ticks(days=2, chunk_size=5000, sleep_interval=0.05)`**:
   - Automatically purges processed tick entries older than $N$ days from `ProcessedTickStore` (`kalai_processedtickstore`) in throttled batches of 5,000 rows via indexed `id`.
3. **CLI Management Command (`python manage.py clear_logs`)**:
   - Supports manual execution with `--days`, `--tick-days`, `--chunk-size` (default: 5000), and `--sleep` (default: 0.05s) to throttle CPU during ad-hoc database maintenance.
4. **WebSocket Engine Pruning (`engine.py:prune_old_db_logs_worker`)**:
   - Runs on startup and daily midnight via `asyncpg` with throttled asynchronous chunked deletions.


---

## PostgreSQL Configuration Parameters

In [`pg_entrypoint.sh`](file:///c:/Users/Admin/Documents/deltazero26/pg_entrypoint.sh) and [`docker-compose.yml`](file:///c:/Users/Admin/Documents/deltazero26/docker-compose.yml):
- **`max_connections = 100`**: Allocated to comfortably support Uvicorn, WebSocket Engine, Django-Q workers, Algo Engine, and external host Python scripts simultaneously.
- **Port Mapping (`127.0.0.1:5432:5432`)**: Bridges PostgreSQL from the Docker container to the Windows host, enabling instant local CLI test runs without connection timeouts.
- **Authentication**: Enforces `scram-sha-256` for all connections.
