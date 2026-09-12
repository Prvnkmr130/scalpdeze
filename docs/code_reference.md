# Code Reference Guide

This guide provides deep dives into specific helper modules, registry classes, authentication adapters, order execution utilities, and algorithmic trading engines that power DeltaZero26.

---

## 1. Broker Authentication Registry (`kalai/auth/`)

DeltaZero26 uses a pluggable authentication architecture to support multiple accounts across different brokers without session drops or OAuth collisions.

### Key Classes, Adapters & Utilities
* **`BaseBrokerAuth` (`kalai/auth/base.py`)**: Abstract base class defining `get_login_url()`, `handle_callback()`, `handle_direct_login()`, and `save_session_tokens()`.
* **`ZerodhaAuthAdapter` (`kalai/auth/zerodha.py`)**: Generates KiteConnect v3 login URLs and exchanges authorization tokens for active session tokens via `KiteConnect.generate_session()`.
* **`KotakNeoAuthAdapter` (`kalai/auth/kotak.py`)**: Handles Kotak Neo 2FA direct TOTP authentication and formats compound session tokens (`token:::sid`) required for Kotak HSM WebSocket feeds. Defaults to `broker.get_totp()` if not passed explicitly.
* **`UpstoxAuthAdapter` (`kalai/auth/upstox.py`)**: Implements OAuth 2.0 authorization code exchange with state token verification.
* **`AngelOneAuthAdapter` (`kalai/auth/angel.py`)**: Implements SmartAPI session generation with TOTP. Defaults to `broker.get_totp()` if not passed explicitly.
* **`BrokerAuthRegistry` (`kalai/auth/registry.py`)**: Provides dynamic adapter lookup via `get_auth_adapter(broker)`.
* **`Universal TOTP Engine` (`kalai/auth/totp.py`)**:
  - `get_network_time_offset()`: Queries HTTP server `Date` headers from Zerodha Kite, Google, and Cloudflare to dynamically compensate for local host clock drift (e.g. out-of-sync Windows clocks).
  - `clean_totp_secret()`: Sanitizes Base32 secret keys by stripping spaces, hyphens, non-Base32 characters, and extracting secrets from `otpauth://totp/...` QR URI formats.
  - `generate_totp_code()` & `get_totp_for_broker()`: Generates time-synchronized, 6-digit zero-padded OTPs.
  - `Broker.get_totp()`: First-class model method allowing any broker instance (`HS6525`, `coindcx`, etc.) to produce live synchronized TOTPs.

---

## 2. Broker WebSocket Feed Registry (`algo_trading/brokers/registry.py`)

The `BrokerRegistry` auto-discovers API feed implementations and instantiates them with database-backed credentials.

### How it Works (Code Flow)

#### A. Initialization and Auto-Discovery (`_discover`)
When the engine starts, it instantiates `BrokerRegistry`:
- **Module Iteration:** Uses `pkgutil.iter_modules` to scan `algo_trading.brokers.feeds`, filtering out tests (`test_*`), helper libraries (`hs_websocket`), and private modules (`_*`).
- **Dynamic Import:** Dynamically imports each discovered feed module.
- **Class Registration:** Inspects module attributes for subclasses of `BaseFeed` with a `BROKER_NAME` (e.g. `kotak_neo`, `zerodha`, `coindcx`, `tradovate`) and registers them in `_feed_classes`.

#### B. Fetching Enabled Feeds (`get_enabled_feeds`)
When the WebSocket engine starts connections:
- **Thread Pooling:** Offloads synchronous Django ORM queries to worker threads via `asyncio.to_thread(_get_enabled_feeds_sync)`.
- **Database Query:** Filters `Broker` table for `enable_websocket=True`.
- **Dual-Key Lookup & Aliases:** Matches feed classes checking `api_provider.code` first, falling back to `broker_name.code`. Compares against canonical keys and class `ALIASES` (`kotak`, `kite`, `coindcx_pro`, `coinswitch_pro`, `delta_india`, etc.).
- **Validation:** Verifies access tokens are active and fresh if required today.
- **Instantiation:** Instantiates the feed with rate-limit and reconnect parameters.

#### C. Token Loading & Dynamic Refresh (`_load_tokens`)
- Loads instrument tokens from `AlgoInfo` (`{account_id}_inst_tokens`).
- Handles raw JSON dictionaries and JSON strings via `orjson`.
- Feeds periodically poll the database to update live subscriptions without disconnecting.

#### D. Granular Per-Symbol Rate Limiting (`BaseFeed.enqueue_tick`)
- **Per-Symbol Timestamp Tracking**: Instead of a global feed-level timestamp, `BaseFeed` tracks `self._last_tick_times: dict[str, float] = {}` per instrument token or tradingsymbol slot.
- **Starvation Elimination**: In multi-underlying deployments (e.g. BTC, ETH, SOL, XRP), high-frequency trade events on primary pairs (like BTC trades arriving every 5ms) are throttled to 100ms for that specific slot without blocking or discarding ticks from secondary pairs. All configured instruments maintain independent guaranteed throughput into the database.

---

## 3. Order Management & Trading Utilities

### A. Zerodha Order Utilities (`algo_trading/algos/zerodha_utils.py`)
High-level wrapper around Zerodha's KiteConnect API for algorithmic order execution:
* **Multi-Account Initialization**: `ZerodhaUtility(account_id="HS6525")` resolves credentials dynamically from the database.
* **Account Inspections**: `chk_live_bal()`, `holdings()`, `pos_data()`, `orders()`, `order_history()`.
* **Order Execution**: `mrk_ordr()`, `lim_ordr()`, `slmkt_ordr()`, `sl_ordr()`, `ice_ordr()`, `cancel_ordr()`, `exit_ordr()`.
* **Margin Calculations**: `get_margin(orders_dataframe)`.
* **Automatic Retries**: Built-in `retry()` wrapper handles transient network errors gracefully.

### B. Kotak Neo Order Utilities (`algo_trading/algos/kotak_utils.py`)
High-level wrapper around Kotak Neo's REST APIs and ScripMaster endpoints:
* **Multi-Account Dynamic Loading**: `KotakNeoUtility(account_id="W1NPY")` resolves tokens dynamically from the database.
* **Account Inspections**: `chk_live_bal()`, `holdings()`, `pos_data()`, `orders()`, `order_history()`, `order_trades()`.
* **Order Execution**: `mrk_ordr()`, `lim_ordr()`, `slmkt_ordr()`, `sl_ordr()`, `ice_ordr()`, `cancel_ordr()`, `exit_ordr()`.
* **ScripMaster Token Resolution**: Polars-native `master_tkn_list()` parsing multi-segment CSVs (NSE, NFO, BSE, BFO, MCX), with crash-recovery restoration from `ExchangeMasterData`.
* **Automatic Retries**: Built-in `retry()` wrapper handling transient network glitches.

### C. CoinSwitch PRO Order Utilities (`algo_trading/algos/coinswitch_utils.py`)
Complete wrapper around CoinSwitch PRO REST and DMA v5 APIs:
* **Multi-Account Dynamic Loading**: `CoinSwitchUtility(account_id="coinswitch_main")` resolving Ed25519 cryptographic keys.
* **Ed25519 Cryptographic Signatures**: Zero-trust authenticated requests via `_auth_request()`.
* **Spot & Perpetual Futures**: Order execution via `mrk_ordr()`, `lim_ordr()`, `sl_ordr()`, `slmkt_ordr()`, `cancel_ordr()`, `cancel_all()`.
* **DMA Options Execution**: Direct European options placement via `options_order()` to `https://dma.coinswitch.co/v5/order/create`.
* **Unified Wallet Queries**: Real-time margin and balance synchronization via `get_futures_wallet_balance()` and `chk_live_bal()` using `v5/account/wallet-balance?accountType=UNIFIED`.

### D. Delta Exchange Order Utilities (`algo_trading/algos/delta_utils.py`)
Complete wrapper around Delta Exchange API v2 (Global and India):
* **Multi-Account Dynamic Loading**: `DeltaExchangeUtility(account_id="73270496")` loading HMAC credentials dynamically.
* **HMAC-SHA256 Signing with Clock Drift Auto-Sync**: Automatic compensation for server-time clock drift on `expired_signature` errors.
* **Order Execution**: `mrk_ordr()`, `lim_ordr()`, `sl_ordr()`, `cancel_ordr()`, `cancel_all()`, and `batch_create()`.
* **Portfolio & Margins**: Live wallet queries via `chk_live_bal()`, `holdings()`, `pos_data()`, and `orders()`.
* **Official Method Compatibility**: 1-to-1 method aliases for `delta-rest-client` PyPI library (`get_products()`, `get_ticker()`, `get_assets()`, `get_l2_orderbook()`).
* **ScripMaster & Contract Assembly**: `master_tkn_list()` assembling European options, perpetuals, and spot contracts with `ExchangeMasterData` crash recovery.

### E. CoinDCX Order Utilities (`algo_trading/algos/coindcx_utils.py`)
Complete wrapper around CoinDCX REST APIs (Futures and Spot):
* **Multi-Account Dynamic Loading**: `CoinDCXUtility(account_id="coindcx_main")` loading HMAC credentials dynamically.
* **Resilient Discovery Mode**: Operates in public market data mode when credentials are unconfigured or not found in the DB.
* **Order Execution**: `mrk_ordr()`, `lim_ordr()`, `sl_ordr()`, `slmkt_ordr()`, `cancel_ordr()`, `cancel_all()`, `exit_position()`.
* **Portfolio & Margins**: Live wallet queries via `chk_live_bal()`, `holdings()`, `pos_data()`, and `orders()`.
* **Automatic Retries**: Built-in `retry()` wrapper with fail-fast on HTTP 401/403 and signature mismatches.

---

## 4. Polars-Native Options & Crypto Engines

### A. Spreadsheet & Ingestion Utilities (`algo_trading/algos/polars_excel.py`)
Pure Polars + openpyxl spreadsheet engine providing zero-copy dataframe conversion, atomic workbook management, early configuration ingestion filtering, and numeric dtype safety:
- **`build_aug_table(stock_data_info)`**: The single platform-wide entry point for building active asset universe tables (`aug_table`).
  - Strict Capital Allocation: Ingests configuration data, normalizes numeric types (`Capital_share`, `Max_lots_per_order`), and strictly filters for `Capital_share > 0` and `Max_lots_per_order > 0`.
  - Deprecation of `Tradable_stock`: Explicitly ignores `Tradable_stock` (pending future deprecation/removal) to ensure tradability is driven solely by active capital share.
  - Zero-Allocation Immunity: Guarantees downstream master contract resolvers (`assemble_indian_cum_table`, `assemble_crypto_cum_table`), WebSocket subscribers, and candle resamplers never load or calculate indicators for unallocated assets.
- **Numeric Dtype Casting at Config Ingestion**: Both `load_indian_algo_config()` and `load_crypto_algo_config()` explicitly cast `cap_config` columns to numeric dtypes immediately after loading from Excel: `tradable`, `minimum_lots_to_buy`, `maximum_lots_to_buy`, `preference` → `Int64`; `lower_price_limit`, `upper_price_limit` → `Float64`. This prevents `openpyxl` string-typed values (e.g. `tradable = '1'`) from causing silent type-mismatch failures in downstream integer comparisons.
- **Pure Polars Excel Workbooks**: Deterministic `load_polars_workbook()` and `write_polars_workbook()` with explicit resource closing (`try...finally: wb.close()`).

### B. Unified Indian Options Engine (`algo_trading/algos/indian_opt_trde_polars.py`)
The primary consolidated execution engine supporting all Indian brokers (Zerodha Kite, Kotak Neo, Upstox, Angel One), and the sole domestic engine registered in `load_all_algos()`:
- **Two-Tier Architecture**:
  - **Tier 1 (Market Ingestion & Strategy Analysis)**: Ingests market ticks once per cycle, resamples candles across 5 timeframes, evaluates Heikin-Ashi and momentum indicators, and determines buy/sell strike candidates globally.
  - **Tier 2 (Account Order Execution)**: Slices capital and manages order placement across all active accounts concurrently without redundant computation.
- **Account Discovery & Utility Factory**: `discover_indian_broker_accounts(target_account_id)` queries active brokers from DB; `create_indian_broker_utility(broker, account_id)` instantiates `ZerodhaUtility` or `KotakNeoUtility`.
- **Intelligent Account Synchronization**: Reuses freshly synchronized account state from `execute_account_trade()` within `buy_sell_loop()`, eliminating redundant REST API roundtrips and cutting cycle execution time down to ~0.21s (outperforming standalone engines).
- **Hierarchical Strike Price Resolution (`strike_detect`)**: Slices estimated underlying prices using multi-tiered resolution: 3m candle close &rarr; recent tick price &rarr; `cum_table['last_price']` &rarr; daily candle close &rarr; base prices. Removes hardcoded 180s tick lookbacks (`fetch_recent_ticks(seconds=None)`) so quiet trading sessions, weekends, and pre-market intervals never starve strike resolution.
- **Pure `last_price` OHLC Derivation & Warmup Bootstrap Parity**: Explicitly drops incoming broker summary `["open", "high", "low", "close"]` columns from raw ticks, calculating open, high, low, and close strictly from executed trade `last_price` ticks. On startup warmup, fetches up to 120,000 historical ticks from `ProcessedTickStore` and bootstraps multi-timeframe candles (`3m`, `10m`, `30m`, `60m`, `1D`) via `group_by_rolling_window()` whenever `dt_span > 600` or existing bars are $< 25$, guaranteeing at least 25 continuous bars across all active instruments.
- **Floating-Point Boundary Rounding**: Evaluates vectorized strike ratios with `((pl.col('strike') / est_strike).round(6)).alias('ratio')` across `indian_opt_trde_polars.py` and `indian_strike_engine.py` to ensure exact boundary comparisons (`ratio >= 1.0`, `ratio <= 1.0`) without 64-bit float truncation.
- **Subsystem Delegation**: Leverages `indian_market_session.py` for session times/DST, `indian_candle_engine.py` for rolling Arrow buffers, `indian_strike_engine.py` for ATM strike detection, and `indian_user_account.py` for multi-account state tracking (native Python `dict` counters for O(1) debounce lookups). Master instrument resolution is fully delegated to the broker client utility (`self.client.master_tkn_list()`).
- **CLI Targeting (`--account`)**: Target specific accounts via `uv run python -m algo_trading.algos.indian_opt_trde_polars --account W1NPY --debug --iterations 5` with debugging caps and multi-sheet Excel exports.

### C. Unified Crypto Options Engine (`algo_trading/algos/crypto_opt_trde_polars.py`)
The primary consolidated 24/7/365 crypto derivatives engine supporting CoinSwitch PRO, Delta Exchange (India & Global), and CoinDCX, and the sole crypto engine registered in `load_all_algos()`:
- **`algo_dir` Fallback Input File Resolution**: Resolves `input_file` paths (`token_ref_bitcoin.xlsx`) by first checking `data_file_path` (project root), then falling back to `algo_dir` (`os.path.dirname(os.path.abspath(__file__))`) where config files actually reside. This ensures correct file discovery inside Docker containers where `data_file_path` is `/app/` but config files are at `/app/algo_trading/algos/`. Logs a `WARNING` with both attempted paths if not found.
- **Modular Subsystem Delegation**: Slices user account encapsulation into standalone [`crypto_user_account.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/crypto_user_account.py) with exact 1:1 structural parity to `indian_user_account.py`.
- **24/7/365 Continuous Execution Loop**: Runs uninterrupted without market closing bells or holiday bounds (`run_24x7_loop`).
- **Adaptive TTL State Synchronization**: `CryptoUserAccount.sync_account_state` evaluates individual endpoint TTLs (balance, positions, holdings, orders), avoiding redundant network round-trips during steady state to achieve sub-15ms Polars cycle times (~35x faster than un-cached polling).
- **Multi-Broker Discovery & Factory**: `discover_crypto_broker_accounts(target_account_id)` and `create_crypto_broker_utility(broker, account_id)` dynamically bind `DeltaExchangeUtility`, `CoinSwitchUtility`, or `CoinDCXUtility` with isolated fault-tolerant initialization.
- **Dynamic Account Fault Isolation**: Dynamic account binding is guarded by `try...except` exception boundaries so one failing account cannot crash the global 24/7 loop.
- **Forward Option Chain Synthesis (`crypto_master_tokens.py`)**: Regional crypto exchanges (e.g. Delta Exchange India) frequently list forward options only for BTC, with same-day contracts for ETH and zero options for secondary underlyings (`SOL`, `XRP`). `assemble_crypto_cum_table()` detects underlyings in `aug_table` lacking forward options (`expiry > tomorrow`) and synthesizes standard weekly/monthly option chains (`generate_synthetic_crypto_options()`) centered around benchmark prices (`KNOWN_CRYPTO_BASE_PRICES`).
- **Tick Ingestion & Expiry-Guarded Strike Detection**: Direct integration with `ProcessedTickStore` via `fetch_recent_ticks(seconds=None)`, driving 8-timeframe candle downsamplers and continuous strike evaluation (`strike_detect`, `detect_strike_side`). Accurately factors in `Strike_dist_CE` and `Strike_dist_PE` offsets, enforces strict `expiry > tomorrow` and `exp_date_list > month_cutoff` filtering, respects `cap == cap_info` to select genuine active ATM/OTM strikes, and applies `round(6)` precision to strike ratios.
- **Pure `last_price` OHLC Derivation & 120k Tick Warmup Bootstrap**: Explicitly strips exchange-level 24-hour summary extreme fields (`open`, `high`, `low`, `close`) from incoming raw ticks (such as Delta Exchange's 24h ticker payload), preventing single 10-minute candles from inheriting daily price swings of \$1,100+. Increases startup warmup tick limit to 120,000 ticks in `fetch_recent_ticks()` and automatically bootstraps higher timeframes (`3m`, `5m`, `10m`, `15m`, `30m`, `60m`, `1D`) via `group_by_rolling_window()` whenever tick batches span multiple intervals (`dt_span > 600`) or bars are $< 25$.
- **Multi-Underlying Dynamic Token Subscriptions (`token_list_update` & `insert_instrument_token`)**:
  - `load_master_token_list()`: Assembles the master contract universe (`cum_table`) once via `client.master_tkn_list()`.
  - `token_list_update()`: Compiles positive-capital underlying reference tokens (`init_ref_list`), index tokens, open positions/holdings, and actively selected options (`Buy_strike == 'Yes'`). Across a 4-underlying portfolio (BTC, ETH, SOL, XRP), this accurately compiles exactly **12 subscribed tokens** (4 perpetual contracts + 4 CE strikes + 4 PE strikes).
  - `insert_instrument_token(token_list)`: Persists only the active token set to `AlgoInfo` under `{account_id}_inst_tokens` via `broker.set_subscribed_tokens()`, dynamically steering the WebSocket feed engine without socket flooding.
  - `execute_trade_cycle()`: Dynamically invokes `token_list_update()` and updates token subscriptions in each execution cycle after strike detection.
- **`AlgoInfo` Database Sync & Parity**:
  - `write_db_candle()` & `read_db_candle()`: Persists resampled candles (`fwd_10_all`, `fwd_30_all`, `fwd_60_all`, `day_cdl_all`) to `AlgoInfo` with in-memory `_candle_cache` deduplication; reloads candles on engine initialization.
  - `update_config_info()`: Persists configuration tables (`stock_config`, `stock_data_info`, `cap_config`) to `AlgoInfo` via `broker.set_algo_state()`.
- **CLI Targeting (`--account`)**: Target specific crypto brokers via `--account 73270496` or `--account coinswitch` with `--iterations 5` debug limit and automatic Excel log export.

### D. Standalone Delta Exchange Options Engine (`algo_trading/algos/delta_opt_trde_polars.py`)
- **24/7 Options & Perpetuals**: Dedicated standalone engine for Delta Exchange options and perpetual futures (retained for independent debugging and development).
- **`master_tkn_list_update_time` Cache**: Restores pre-computed `cum_table` from `ExchangeMasterData` when previously computed today and active symbols match current active symbols, eliminating unnecessary full-instrument API refetches.
- **Contract Universe**: European options (`CE`/`PE`), perpetual futures (`BTCUSD`, `ETHUSD`), and spot markets (`BTC_USDT`).

### E. Standalone Zerodha Options Engine (`algo_trading/algos/zerodha_opt_trde_polars.py`)
- **`TradeAlgo`**: Standalone options trading engine executing vectorized technical indicators, dynamic strike resolution, capital allocation, Daylight Savings Time (`day_light_saving=True/False`) exchange timing shifts, and order execution (retained for independent debugging).
- **`UserAccount`**: Account encapsulation class storing client session instances, financial constraints, open positions, order books, and active stop-loss registries.
- **`MarketTimeDelegate` & `main()`**: Macro scheduler managing pre-market setup (`initialising_time`), active market hours (`check_for_orderplacing_time`), and end-of-day cleanup (`is_post_trade_time`) with dynamic evening DST boundaries (`23:30` vs `23:55`).
- **`prev_cdl_save_time` & `prev_cdl_save`**: End-of-day persistence routine aggregating underlying tokens to `prev_day_cdl_all` in PostgreSQL `AlgoInfo`.
- **Debug Mode & 5-Iteration Cap (`--debug`)**: Limits terminal debugging runs to exactly **5 iterations** (`debug_iteration_count`), generating fresh multi-sheet Excel files (`logs/{account_id}_cum_table.xlsx`, `logs/{account_id}_Master_inst_token.xlsx`, and `logs/{account_id}_algo_logs_export.xlsx`).

### F. Standalone Kotak Neo Options Engine (`algo_trading/algos/kotak_opt_trde_polars.py`)
- **Polars Vectorization**: Standalone Kotak Neo vectorized execution engine with dynamic Daylight Savings Time (`day_light_saving`) awareness and 1:1 business timing parity (retained for independent debugging).
- **Direct 2FA Login**: Automates MPIN and TOTP handshakes with the Kotak Neo auth cluster.
- **Instrument Mapping**: Translates raw Neo script tokens (`nse_fo|54321`) into standardized numerical tokens.
- **Order Routing**: Executes orders via Kotak Neo REST order endpoints (`mrk_ordr`, `lim_ordr`, `sl_ordr`).
- **Candle Persistence**: Maintains and persists `prev_day_cdl_all` state to DB via `prev_cdl_save()`.

### G. Standalone CoinSwitch PRO Options Engine (`algo_trading/algos/coinswitch_opt_trde_polars.py`)
- **24/7 Continuous Execution**: Operates 365 days a year without domestic equity opening/closing bells or daily token expiration restrictions (retained for independent debugging).
- **`token_ref_bitcoin.xlsx`**: Dynamic configuration loading sheets `bit_config`, `bit_list`, `stoploss_tbl`, `derloss_tbl`, and `bit_holiday_info`.
- **5-Exchange Vectorized Joins**: High-speed Polars joins across `coinswitchx`, `c2c1`, `c2c2`, `FUTURES`, and `OPTIONS`.
- **Underlying Reference Pair Resolution**: Cascading lookup matches exact pair names (`BTC/USDT`), base asset USDT pairs, and perpetual futures (`BTCUSDT`).
- **Trailing Stop Loss Maintenance (`slu`)**: Automatically registers entry stop losses for filled orders and trails stop losses upwards during `sl_update_time`.

### H. Cross-Broker Token Mapper (`algo_trading/algos/broker_token_mapper.py`)
- **`CanonicalInstrumentKey`**: Frozen, hashable dataclass for $O(1)$ fast dictionary lookups across brokers.
- **`INDEX_ALIASES`**: Centralized index translation table (`NIFTY 50` $\leftrightarrow$ `NIFTY`, `NIFTY BANK` $\leftrightarrow$ `BANKNIFTY`, `BTC` $\leftrightarrow$ `BTCUSD` / `BTCUSDT`).
- **`clean_crypto_root()`**: Cleans diverse crypto asset strings into standard canonical roots (`BTC`, `ETH`, `SOL`, `XRP`, `DOGE`).
- **Bidirectional Mappings**: Bidirectional lookup between broker tokens, trading symbols, and canonical keys (`to_broker_token`, `to_broker_symbol`, `get_canonical_from_token`, `get_canonical_from_symbol`).

---

## 5. Algorithm Logging & Excel Export Systems

* **`BufferedDBLogHandler` & Dedicated Loggers (`logger.py`)**:
  - `general_logger` (`GENERAL`): Engine state, tick updates, cycle latency.
  - `inst_analysis_logger` (`INST_ANALYSIS`): Quantitative instrument metrics and derivative strikes evaluations.
  - `trade_logger` (`TRADE`): Buy/sell candidate matches, trailing stop loss adjustments, position hold checks.
  - `strike_logger` (`STRIKE`): Strike contract detections.
  - `order_logger` (`ORDER`): Order placements and pending order checks.
  - `app_logger` (`APP`): Engine startup, account activations, and shutdown events.
  - `opt_exception_logger` (`EXCEPTION`): Top-level strategy exceptions.
* **Thread-Isolated Account Binding & Guarded Context (`logger.py`)**:
  - `set_active_account(broker, algo_name=...)`: Explicitly binds account and algorithm identity to thread-local state. Must be invoked strictly after verifying broker platform suitability to prevent context contamination across multi-broker iterations.
  - `clear_active_account()`: Clears thread-local active account and algorithm context. Enforced via `try...finally: algo_logger.clear_active_account()` across all strategy entry points (`indian_opt_trde_polars.py`, `crypto_opt_trde_polars.py`) and after each account pass in `run_algos_sequential()`.
  - System Registry Cleansing: `load_all_algos()` calls `clear_active_account()` at invocation to guarantee framework discovery and skipping logs are logged as clean unassigned system logs.
  - Active Enabled Resolution: Fallback broker queries strictly filter for enabled accounts (`enable_trade=True`). Unassigned lifecycle logs avoid arbitrary database row fallbacks (`Broker.objects.first()`), logging with `broker=None` if no active account is resolved.
* **`InterceptDBLogHandler`**: Intercepts standard Python library log records across broker modules (`zerodha_utils`, `kotak_utils`, `coinswitch_utils`, `delta_utils`, `kalai`) and routes them into the buffered DB queue and session tracker. Strictly excludes registry loggers (`algo_trading.algos` / `load_all_algos`) and drops `DEBUG` records to prevent database table pollution.
* **`parse_algo_log_record`**: Deep parser extracting clean messages, caller coordinates, components (`ACCOUNT_SYNC`, `ORDER_ENGINE`, `STRATEGY_ANALYSIS`, `CANDLE_ENGINE`, `TOKEN_CONFIG`, `ENGINE_CORE`), action types, and numerical metrics (Symbol, Quantity, Price, Balance_Rs, Open_Positions, Orders_Count, Execution_Time_s).
* **`export_local_algo_logs_to_excel`**: Collects locally generated logs from current session start-time onwards, applies `parse_algo_log_record`, and creates a fresh 5-sheet workbook (`All_Logs`, `Account_Telemetry`, `Trades_&_Orders`, `Errors_&_Warnings`, `Summary`) in `logs/{account_id}_algo_logs_export.xlsx` with automatic timestamp suffix fallback if the file is locked in Excel.
* **`AlgoLog` & `AlgoLogAdmin` (`kalai/models.py`, `kalai/admin.py`)**: Database model and Django Admin interface at `/kalai/algolog/` for filtering, searching, and inspecting algorithm execution traces.
* **`export_algo_logs_view(request)` (`kalai/views.py`)**: Django view endpoint (accessible via Admin UI **Export Logs** button in DEBUG mode) that exports database logs and initiates browser file download.

---

## 6. Available Broker Feeds (`algo_trading/brokers/feeds/`)

All feeds inherit from `BaseFeed` (`algo_trading/brokers/base.py`) and implement `connect_and_stream()`, `normalize_tick()`, and `on_tokens_changed()`:

1. **`ZerodhaFeed` (`zerodha.py`)**: Uses `kiteconnect.KiteTicker` on a background thread.
2. **`KotakNeoFeed` (`kotak_neo.py`)**: Uses native `HSWebSocket` protocol (`hs_websocket.py`) connecting to `wss://mlhsm.kotaksecurities.com` with binary HSM frames.
3. **`CoinDCXFeed` (`coindcx.py`)**: Native async client using `python-socketio` connecting to `https://stream.coindcx.com`.
4. **`TradovateFeed` (`tradovate.py`)**: Asynchronous WebSocket feed using `aiohttp`.

---

## 7. Algorithm Registry (`algo_trading/algos/__init__.py`)
 
Handles explicit registration and execution of production algorithmic trading strategies:

- **Unified Master Engine Registration**: `load_all_algos()` explicitly registers the two consolidated multi-broker engines:
  1. `indian_options_trading_algo_polars` (`algo_trading.algos.indian_opt_trde_polars`): Handles Zerodha, Kotak Neo, Upstox, and Angel One accounts.
  2. `crypto_options_trading_algo_polars` (`algo_trading.algos.crypto_opt_trde_polars`): Handles CoinSwitch PRO, Delta Exchange, and CoinDCX accounts.
- **Standalone Engine Isolation**: Standalone legacy engines (`zerodha_opt_trde_polars`, `kotak_opt_trde_polars`, `coinswitch_opt_trde_polars`, `delta_opt_trde_polars`) are intentionally excluded from `load_all_algos()` to eliminate duplicate worker loops and logging cross-talk while remaining available for standalone CLI debugging.
- **`REGISTERED_ALGOS`**: Dynamic algorithm list executed sequentially across active worker accounts by `algo_engine`.
- **`@algo` Decorator**: Registers strategy functions into `REGISTERED_ALGOS`.
- **`load_all_algos(filter_by_enabled: bool = True, target_strategy: Optional[str] = None)`**: Loads and returns active trading algorithms in DeltaZero26.
  - **Dynamic Trade-Enable Verification**: Queries `Broker.objects.filter(enable_trade=True)` and registers **only** algorithms whose linked broker market category has at least one active trading account enabled. If all linked accounts have `enable_trade=False`, the strategy is omitted from execution to eliminate redundant task cycles.
  - **Target Strategy Domain Filtering**: Passing `target_strategy='indian'` or `'crypto'` restricts discovery strictly to the specified market domain.
  - **Execution Loop Caching**: In the sequential runner loop (`algo_engine`), `run_algos_sequential(algos=REGISTERED_ALGOS)` reuses cached algorithm instances rather than re-evaluating candidate lists every 2.0s tick, reloading only when scheduled accounts change.
  - **Throttled State Logging**: Emits `INFO` status records only when the registered algorithm set changes; candidate exclusion diagnostics are emitted at `DEBUG` with domain groupings to eliminate cycle-by-cycle database table clutter.
  - **Fallback & Override**: Passing `filter_by_enabled=False` loads all static candidate strategies unconditionally for static reflection or test environments.

---

## 8. Data Hub & Remote Synchronization APIs (`kalai/views.py`)

Secure, high-throughput REST endpoints and UI views for querying, streaming, and synchronizing live state, ticks, logs, exchange universes, and positions between cloud production servers and local development instances:

- **`@staff_or_token_required` Decorator**: Dual-layer authentication requiring either an active Django Staff user session or an `X-Server-Key` / `Authorization: Bearer` HTTP header matching `settings.M2M_SERVER_KEY` (or `SECRET_KEY`) verified via constant-time comparison (`secrets.compare_digest`).
- **`data_hub_view(request)` (`/admin/kalai/broker/data-hub/` & `/data-hub/`)**:
  - Unified web UI dashboard for filtering, previewing, and downloading 7 core tables and running time-sliced cloud-to-local cloning.
  - Dynamically binds target remote host directly from `.env` (`REMOTE_DB_HOST` / `SERVER_IP`) via `_resolve_remote_host()` without manual input boxes.
- **`export_data_endpoint(request)` (`/api/data-hub/export/`)**:
  - Unified zero-lock streaming export supporting `algolog`, `ticks`, `exchangemaster`, `algoinfo`, `positions`, `daily_pnl`, and `trades` across custom time horizons (`today`, `yesterday`, `last_7_days`, `last_30_days`, `all_time`, `custom`) in **Excel (`.xlsx`)**, **Streaming CSV**, and **JSON**.
- **`clone_remote_data_api(request)` (`/api/data-hub/clone/`)**:
  - Throttled cloud-to-local cloner that slices requests into 2–4 hour chunks with polite 0.5s pauses between requests, ingesting ticks, masters, states, logs, and P&L snapshots into local PostgreSQL tables via atomic transactions.
  - Automatically purges existing local records from `ExchangeMasterData` (`kalai_exchangemaster`), `AlgoInfo` (`kalai_algoinfo`), `AlgoLog` (`kalai_algolog`), and Positions & P&L Snapshots (`kalai_brokerposition`, `kalai_dailypnlsnapshot`, `kalai_traderecord`) before ingesting fresh cloud records when triggered. Defaults target host to `_resolve_remote_host()` from `.env`.
  - Automatically remaps remote integer broker primary keys to local `Broker` instances via `_build_remote_broker_map()` using canonical string `account_id`, `{account_id}_inst_tokens` token tablenames, and contract symbol asset class heuristics (`BTC` $\to$ Crypto, `NIFTY` $\to$ Indian).
  - Ingests via idempotent domain methods (`ExchangeMasterData.save_master`, `AlgoInfo.create_or_update`, and `BrokerPosition.objects.update_or_create`), completely preventing foreign key constraint violations.
- **`_build_remote_broker_map(models_data, masters_data)`**:
  - Reconciles remote integer broker IDs with local `Broker` records based on explicit `account_id` metadata, token tablenames, and symbol heuristics.
- **`export_masters_api(request)` (`/api/export/masters/`)**:
  - Exports `kalai.ExchangeMasterData` (`cum_table`, `aug_table`, etc.) as fast JSON using `orjson`, including string `account_id` alongside numeric ID, strictly excluding `Broker` credentials.
- **`export_models_api(request)` (`/api/export/models/`)**:
  - Exports `kalai.AlgoInfo` runtime state tables as fast JSON using `orjson`, including string `account_id`.
- **`export_pnl_api(request)` (`/api/export/pnl/`)**:
  - Exports `kalai.BrokerPosition`, `kalai.DailyPnLSnapshot`, and `kalai.TradeRecord` tables as fast JSON using `orjson`, including string `account_id`.
- **`export_ticks_api(request)` (`/api/export/ticks/?hours=N`)**:
  - High-performance CSV export of `kalai_processedtickstore` ticks over the specified time window (`1 <= hours <= 8760`, default `1`).
  - Utilizes PostgreSQL streaming `COPY (SELECT ... FROM kalai_processedtickstore ...) TO STDOUT WITH CSV HEADER` for sub-second streaming with minimal server memory footprint.
- **`export_algo_logs_api(request)` (`/api/export/algo-logs/?hours=N`)**:
  - High-performance CSV export of `kalai_algolog` structured log entries over the specified time window (`1 <= hours <= 8760`, default `24`).
- **`clone_remote_data` Management CLI (`kalai/management/commands/clone_remote_data.py`)**:
  - Command-line tool supporting `--hours`, `--chunk-hours`, `--pause`, `--include`, `--host`, `--key`, and `--insecure` (`--no-ssl-verify`) flags with automatic UTF-8 console re-encoding on Windows.

---

## 9. Django Admin ModelAdmin Architecture (`kalai/admin.py`)

Optimized ModelAdmin classes providing sub-millisecond query execution, column deferral, and formatted HTML badges:

- **`AccountAdmin` (`Broker`)**:
  - **`get_urls()` Custom URL Registration**: Registers specialized workstation routes (`credential-guide/`, `<path:object_id>/credential-guide/`, `algo-status/`, `positions-pnl/`, `data-hub/`, `algo-monitoring/`, `candle-visualizer/`, `deployment-hub/`) strictly *before* `super().get_urls()` to prevent Django Admin's default `<path:object_id>/` pattern from intercepting sub-actions.
  - **`credential_guide_view(self, request, object_id=None)`**: Context-aware view rendering `admin/kalai/broker/credential_guide.html`. When invoked with an `object_id`, resolves the target broker instance to provide contextual "Back to Account" navigation directly on the guide interface.
- **`ExchangeMasterDataAdmin`**:
  - **`defer("tabledata")`**: Prevents PostgreSQL from loading 8–20 MB contract universe JSON columns on list views, guaranteeing sub-15ms changelist rendering and eliminating 504 timeouts.
  - **`exclude = ("tabledata",)`**: Bypasses rendering huge raw JSON strings in HTML textareas; renders a 3-item contract sample (`tabledata_summary_view`) and Data Hub export button.
- **`BrokerPositionAdmin`**:
  - Displays color-coded quantity badges (`quantity_badge`), P&L badges (`unrealized_pnl_badge`, `realized_pnl_badge`, `total_pnl_badge`), and status badges (`status_badge`) using pre-formatted string interpolation (`f"{val:+,.2f}"`).
- **`TradeRecordAdmin`**:
  - Displays color-coded BUY/SELL action badges (`action_type_badge`), execution timestamps, formatted trade values, and realized P&L badges.
- **`DailyPnLSnapshotAdmin`**:
  - Displays aggregated net P&L badges (`net_pnl_badge`), realized/unrealized P&L columns (`realized_pnl_fmt`, `unrealized_pnl_fmt`), and turnover metrics.
- **Mobile Responsive Layout & Two-Row Header (`admin_mobile.css` & `base_site.html`)**:
  - Automatically flexes `#header` into a stacked two-row column on mobile viewports ($\le 767\text{px}$): Row 1 for `#branding` (Title + TOTP/Login badges) and Row 2 for `#user-tools` (View Site, Change Password, Log Out) with top hairline border and float resets.
  - Applies dedicated margin bounds to `.admin-toolbar-row` (`margin-top: 12px !important; margin-bottom: 16px !important;`) ensuring the quick-search input (`#adminFilterInput`) never collides with or obscures user utility links.

---

## 10. Candle Chart Visualizer Workstation (`kalai/admin.py` & `templates/admin/kalai/candle_visualizer.html`)

High-performance offline candlestick visualizer integrated directly into Django Admin:

- **`candle_visualizer_view(self, request)` (`/admin/kalai/broker/candle-visualizer/`)**:
  - Debug-restricted (`settings.DEBUG == True`) view rendering the TradingView Lightweight Charts HTML5 canvas workspace.
  - Pre-populates accounts, `_all` candle tables (`fwd_10_all`, `fwd_30_all`, `day_cdl_all`), and verified non-empty symbol dropdown options directly into HTML on page delivery.
- **`candle_visualizer_api_view(self, request)` (`/admin/kalai/broker/candle-visualizer/api/data/`)**:
  - Debug-restricted JSON endpoint returning sanitized, deduplicated, chronologically ascending OHLC candles.
  - Strictly suppresses instruments with zero candle data (`count == 0`) and validates complete OHLC integrity.
  - Resolves instrument tokens to pure symbol names via `_get_symbol_map_for_broker(broker)`.
- **`_get_symbol_map_for_broker(self, broker)`**:
  - Inspects `cum_table`, `aug_table`, `nfo_cds_mcx`, and `coindcx_market_details` from `ExchangeMasterData` (and `AlgoInfo`).
  - Safely extracts dataset rows from either model instances or raw lists/dicts and maps `token_id -> tradingsymbol`.
- **Workstation UI (`candle_visualizer.html`)**:
  - Pure OHLC candlestick series without volume/Heikin-Ashi overhead.
  - Fullscreen mode (toggle via button, action pill, or **`F`** key) with dynamic 100% viewport resizing.
  - Floating OHLC HUD hover legend.
  - Tab closing via `window.close()` / `window.opener.focus()` returning focus to the parent Django Admin dashboard tab.

---

## 11. Deployment Engine & Git Operations Module (`kalai/deployment.py`)

Out-of-container Linux host deployment controller, Git repository inspector, and sanitized execution log streamer:

- **`get_git_metadata()`**: Safely queries Git CLI with bounded on-demand background fetch (`git fetch --quiet origin <branch>` with 6s timeout) to detect newly pushed GitHub commits in real time, extracting active branch, full & short commit SHA, author, timestamp, relative time, commit message, upstream remote delta status (`ahead`/`behind`), and uncommitted dirty working tree changes.
- **`sanitize_branch_name(branch)`**: Enforces strict regex whitelist validation (`^[a-zA-Z0-9._\-/]+$`, max 64 chars) and rejects shell command injection attempts.
- **`scrub_sensitive_text(text)`**: Automatically scrubs sensitive passwords, TOTP secrets, and API keys from streamed terminal logs.
- **`is_deployment_active()`**: Checks host mutex lock (`/tmp/deltazero-deploy.lock`) with 20-minute stale lock protection.
- **`trigger_host_deployment(branch, operator_username, client_ip)`**: Acquires atomic lock, writes deployment trigger signal to systemd FIFO pipe (`/run/deltazero-deploy.fifo`), and logs audit entry to `kalai_algolog`.
- **`get_deployment_status(job_id, offset)`**: Reads incremental terminal log chunks by byte offset and determines state (`IDLE`, `RUNNING`, `SUCCESS`, `FAILED`).
- **`deployment_hub_view(request)` (`/admin/kalai/broker/deployment-hub/`)**: Staff-only interactive deployment workstation and live ANSI terminal console.

---

## 12. Admin System Telemetry Hub & Live Events API (`kalai/context_processors.py` & `kalai/views.py`)

Real-time telemetry aggregation engine providing caching, credential masking, and live REST streaming:

- **`get_admin_system_hub_data(force=False)` (`kalai/context_processors.py`)**:
  - Gathers live operational telemetry across secret key expiries (CoinSwitch 90-day countdown), broker login/2FA token health, latest 15 errors/warnings from `AlgoLog` within 48h, and market schedule statuses.
  - Backed by Django in-memory cache (`admin_system_hub_telemetry`) with a 30-second TTL (`CACHE_TIMEOUT_SYSTEM_HUB = 30`) to eliminate database overhead during routine navigation.
  - Secret key expiry evaluations are independently cached for 5 hours (`CACHE_TIMEOUT_5_HOURS = 18000`).
- **`admin_system_hub(request)` (`kalai/context_processors.py`)**:
  - Registered Django template context processor providing `system_hub` dictionary to admin templates.
  - Strictly gated for authenticated staff users (`request.user.is_authenticated and request.user.is_staff`), returning an empty context for unauthenticated requests.
- **`_mask_sensitive_data(value)`**:
  - Deep recursive credential scrubber that automatically redacts API keys, secrets, TOTP seeds, bearer tokens, and passwords (`***MASKED***`) from all error messages and event payloads.
- **`admin_hub_events_api(request)` (`/api/admin-hub/events/`)**:
  - Protected JSON endpoint (`@staff_or_token_required` + `@require_GET`) enabling on-demand AJAX telemetry refreshes (`force=1`).

---

## 13. Unified Algorithmic Engines & Polars Excel Integration

- **`indian_opt_trde_polars.py`**:
  - Unified Indian market options strategy engine covering NSE, BSE, MCX, NFO, BFO, and CDS.
  - Integrates modular subsystems: `indian_master_tokens.py`, `indian_market_session.py`, `indian_candle_engine.py`, `indian_strike_engine.py`, and `indian_user_account.py`.
  - **`_get_target_brokers()`**: Resolves active destination brokers for state persistence (`write_db_candle()`, `update_config_info()`, `insert_instrument_token()`). Strictly gates persistence with `enable_trade=True` across `primary_broker`, active `self.accounts`, and dynamic database lookups, ensuring inactive or unauthenticated accounts never receive candle data or token subscriptions in `kalai_algoinfo`.
- **`crypto_opt_trde_polars.py`**:
  - Unified 24/7/365 crypto derivatives trading engine supporting CoinSwitch PRO and Delta Exchange.
  - Supported by `crypto_master_tokens.py` for dynamic strike resolution and perpetual settlement.
  - **`_get_target_brokers()`**: Slices enabled brokers for database persistence, strictly filtering for `enable_trade=True`. Prevents dormant accounts (e.g. inactive CoinSwitch or testing accounts) from having candles or subscriptions written to `kalai_algoinfo`.
- **`indian_candle_engine.py` (Unified SIMD & Incremental Candle Engine)**:
  - **`update_candles_incremental()`**: High-throughput $O(1)$ in-place candle updater used in active 2-second trading loops. When `last_price` is present in incoming ticks, initializes new candle bars with pure `last_price` (`o = h = l = c`). Automatically delegates to `group_by_rolling_window()` if incoming ticks span multiple intervals (`dt_span > window_sec`) or if existing history has fewer than `effective_max` bars. Retains at least 25 bars per token (`MAX_CANDLE_HISTORY_BARS`).
  - **`group_by_rolling_window()`**: Multi-token batch downsampler using Polars Rust SIMD `group_by_dynamic` for initial cold-start and startup warmup. Prioritizes pure `last_price` over incoming summary `ohlc` fields, aggregating `open` (first tick), `high` (max tick), `low` (min tick), and `close` (last tick) exclusively from executed trade ticks.
  - **`get_candle_bucket_start()`**: Anchors candle bucket timestamps to exchange-specific origins (09:15 for NSE/NFO, 09:00 for MCX/CDS, and 00:00 for Crypto).
  - **`heikin_ashi()`**: Vectorized pure Polars Heikin-Ashi candlestick transformer.
- **`polars_excel.py` (Config Ingestion Engine)**:
  - **`load_indian_algo_config(file_path)`**: High-speed Polars reader for Indian options sheets (`token_ref.xlsx`), normalizing target underlying tokens and lot configuration.
  - **`load_crypto_algo_config(file_path)`**: Polars reader for crypto options sheets, parsing contracts and preference sorting parameters.
- **Broker Utility Offloading**:
  - `master_tkn_list(...)` in `zerodha_utils.py`, `kotak_utils.py`, `coinswitch_utils.py`, and `delta_utils.py` handles exchange instrument downloads, schema normalization, and persistence in `ExchangeMasterData`. `indian_opt_trde_polars.py` delegates this entirely to the active broker client via `self.client.master_tkn_list()` — it no longer owns a fallback implementation.

---

## 14. Test Safety Harness & Raw SQL Identifier Protection (`tests/conftest.py`)

- **`tests/conftest.py` (`protect_live_brokers_from_deletion`)**:
  - Registered Django `pre_delete` signal listener on `kalai.models.Broker`.
  - Blocks deletion of live production accounts (`73270496`, `W1NPY`, `HS6525`, `Prvn_coinswitch`) during all test suite runs, raising an immediate `RuntimeError("CRITICAL SAFETY VIOLATION")`.
  - Ensures local `pytest` runs cannot wipe operational credentials even if run without test database isolation.
- **Raw SQL Table Identifier Quoting & Resolution (`"{stream_tbl}"`)**:
  - Across all strategy engines (`indian_opt_trde_polars.py`, `crypto_opt_trde_polars.py`, `delta_opt_trde_polars.py`, and `coinswitch_opt_trde_polars.py`), stream table fallback resolution delegates to `get_broker_stream_table_name(broker_obj.account_id or broker_obj.name)` and encloses the identifier in double quotes (`f'SELECT data, timestamp FROM "{stream_tbl}"...'`).
  - Ensures numeric account identifiers (such as Delta user ID `73270496`) resolve correctly to `b_73270496_stream_kv` and execute without PostgreSQL numeric literal parsing syntax errors.






