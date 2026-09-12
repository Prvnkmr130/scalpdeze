# DeltaZero26

Multi-broker algorithmic trading platform and low-latency tick processing engine containerized for deployment on resource-constrained cloud instances.

---

## Key Features

- **Multi-Broker Integration**: Unified authentication, WebSocket streaming, and order execution adapters for Zerodha (Kite Connect), Kotak Neo, CoinSwitch PRO, and CoinDCX.
- **Pure Polars Vectorized Strategy Architecture (Zero Pandas)**:
  - 100% migrated to native **Polars + openpyxl** with zero Pandas runtime dependencies for lightning-fast vectorized SIMD candle calculations, Heikin-Ashi conversions, and indicator evaluations.
  - **Deterministic Synchronous Sequential Execution**: Runs active strategies sequentially across enabled accounts every **2.0 seconds** (`ALGO_LOOP_INTERVAL_SECONDS`), eliminating coroutine context switching, asyncpg connection pool contention, and background thread jitter.
  - **`zerodha_opt_trde_polars.py`**: High-performance options strategy executing multi-timeframe SIMD candle downsampling, technical indicators (Heikin-Ashi, momentum paths 1–13_2), automatic weekend force-run detection, in-memory Arrow execution, and optional `--debug` Excel export.
  - **`kotak_opt_trde_polars.py`** & **`coinswitch_opt_trde_polars.py`**: High-speed equity derivatives and 24/7 crypto strategies.
- **Asynchronous WebSocket Engine (`ws_engine`)**:
  - Epoll event-driven streaming with PostgreSQL binary `COPY` writes into weekly date-range partitioned tables with near-zero CPU idle footprint.
- **Security & Performance Hardening**:
  - **Ultra-Lean ~25–32 MB Active Heap**: Rust-powered CSV parsing (`pl.read_csv()`) and immediate post-assembly heap deallocation (`gc.collect()`) reduce active strategy memory by 95.8% (from 766 MB).
  - **Instant Zero-Download Crash Recovery**: Pre-computed `cum_table` (~50 KB) persistence enables immediate state restoration in <50s upon mid-day container restart without downloading 150k exchange scrips.
  - **M2M Authentication Key Decoupling**: API endpoints authenticate programmatic clients via dedicated `M2M_SERVER_KEY` headers, isolating export access from Django's core `SECRET_KEY`.
  - **Strict TLS Verification**: All outbound remote synchronization calls enforce validated TLS (`SSL_VERIFY`).
  - **Strict OAuth State Verification**: Aborts callback processing with immediate error upon state token mismatch.
  - **Database Index Optimization**: Compound composite indexes `(account, -timestamp)` and `(account, timestamp)` on `kalai_processedtickstore` for sub-millisecond strategy tick queries.
  - **Host Memory & ZRAM Compression**: `zram-tools` integration (`zstd` compression, 60% RAM capacity, sysctl tuning) for in-RAM compressed swap.

---

## Documentation

Full architectural documentation, database schemas, and code references are available in the [`docs/`](docs/index.md) directory:

- **[System Architecture](docs/architecture.md)**: Container topology, process management with `supervisord`, and event flows.
- **[Environment & Deployment](docs/environment_and_deployment.md)**: Dependencies, `.env` configuration, PostgreSQL tuning, BuildKit optimizations, and Docker deployment.
- **[Database Schema](docs/database_schema.md)**: Full schema reference for `Broker`, `AlgoInfo`, `AlgoLog`, and `ProcessedTickStore`.
- **[Algorithmic Trading Engine](docs/features/algo_engine.md)**: Polars-native options & crypto strategy architecture, execution lifecycles, and Excel log export pipelines.
- **[Code Reference](docs/code_reference.md)**: Auth adapters, WebSocket feeds, strategy engines, and log exporters.
- **[Zerodha Utilities](docs/features/zerodha_utils.md)** & **[CoinDCX Utilities](docs/features/coindcx_utils.md)**: Order execution and market data APIs.
- **[Broker Management](docs/features/broker_management.md)**: Multi-account token isolation and OAuth callback routing.

---

## Windows Thin Client Setup & Supervisor

The platform runs natively on **Windows 10/11 Local Thin Client PCs** with zero Docker or Linux dependencies.

### 1. Automated Setup (`setup_windows.ps1`)
Installs Python dependencies, Playwright Chromium binaries, and registers the Windows Task Scheduler RTC Wake task:
```powershell
powershell -ExecutionPolicy Bypass -File .\setup_windows.ps1
```

### 2. Supervisor Watchdog (`run_watchdog.ps1`)
Supervises the signal engine with automatic crash recovery and graceful shutdown:
```powershell
powershell -ExecutionPolicy Bypass -File .\run_watchdog.ps1
```

### 3. Direct Django CLI
```powershell
python manage.py run_scraped_algo
```

---

## Strategy Engine Execution, Debug Modes & Excel Log Exports

The Polars trading strategies operate purely in-memory for sub-second tick execution (`0.2s - 0.5s`). When executed directly from the terminal with `--debug`, the engine enforces safety caps and generates fresh, isolated multi-sheet Excel reports:

- **Automatic Iteration Safety Cap**: In debug mode (`--debug` or `debug_mode=True`), the engine executes exactly **5 iterations** (configurable via `--iterations N`) and terminates cleanly, avoiding infinite background loops.
- **Fresh Multi-Sheet Excel Log Exporter**: Automatically exports locally collected session logs to `logs/{account_id}_algo_logs_export.xlsx` (or `{algo_name}_algo_logs_export.xlsx`), overwriting previous runs cleanly while providing timestamp fallbacks (`{account_id}_algo_logs_export_YYYYMMDD_HHMMSS.xlsx`) if the file is currently locked/open in Microsoft Excel.
- **Dedicated Workbook Sheets**:
  1. `All_Logs`: Full parsed telemetry with clean messages, caller coordinates, and metrics.
  2. `Account_Telemetry`: Balances, positions, order counts, and execution latency.
  3. `Trades_&_Orders`: Trade candidate signals, buy/sell orders, trailing stops, and cancellations.
  4. `Errors_&_Warnings`: System warnings, exceptions, and API retry events.
  5. `Summary`: Macro session statistics, execution timestamps, and event counts.

```bash
# Run Zerodha options strategy in debug mode (5 iterations + fresh Excel log export)
python algo_trading/algos/zerodha_opt_trde_polars.py --debug --force-run

# Run CoinDCX crypto strategy in debug mode
python algo_trading/algos/coindcx_opt_trde_polars.py --debug --force-run

# Custom iteration count in debug mode
python algo_trading/algos/zerodha_opt_trde_polars.py --debug --iterations 10
```
