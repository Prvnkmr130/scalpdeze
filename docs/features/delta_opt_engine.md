# Delta Exchange Polars Monolithic Options Engine

The **Delta Exchange Polars Options Engine** ([`algo_trading/algos/delta_opt_trde_polars.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/delta_opt_trde_polars.py)) is a high-performance, Polars-native algorithmic trading engine engineered for 24/7 crypto options, perpetual futures, and spot markets on **Delta Exchange** (supporting both Global and India endpoints).

---

## 1. Core Architecture & Design

```mermaid
flowchart TD
    ConfigExcel[token_ref_bitcoin.xlsx] --> InitEngine[TradeAlgo.__init__]
    DeltaAPI[Delta Exchange REST API /v2/products] --> InstFetch[DeltaExchangeUtility.get_products]
    
    InitEngine --> MasterTknList[TradeAlgo.master_tkn_list]
    InstFetch --> MasterTknList
    
    subgraph Polars Vectorized Assembly [Vectorized Polars Joining]
        MasterTknList --> AugTable[self.aug_table (Filtered Crypto Underlyings)]
        MasterTknList --> MasterSheets[4 Exchange Sheets: FUTURES, OPTIONS, SPOT, ALL_CRYPTO]
        MasterSheets --> UnifiedDerivatives[self.nfo_cds_mcx (Concat All Instruments)]
        AugTable -- Left Join on Symbol == name --> CumTable[self.cum_table (Derivatives & Spot Contracts)]
    end
    
    CumTable --> StreamSync[DB Token Subscriptions: {account}_inst_tokens]
    StreamSync --> WSEngine[DeltaExchangeFeed (wss://public-socket.india.delta.exchange)]
    WSEngine --> ProcessedTickStore[(ProcessedTickStore / b_{account}_stream_kv DB)]
    
    subgraph Sub-Second Execution Loop [24/7 Cycle Loop]
        ProcessedTickStore --> TickNormalize[Polars Fast Tick Ingestion]
        TickNormalize --> MultiResample[8-Timeframe Candle Resampler]
        MultiResample --> HeikinAshi[Vectorized Heikin-Ashi Indicator Engine]
        HeikinAshi --> StrikeEval[Dynamic Strike & Jump Evaluator]
        StrikeEval --> OrderRouter[DeltaExchangeUtility HMAC REST Execution]
    end
```

---

## 2. Key Capabilities & Differences from Equity Engines

| Feature | Equity Engines (Zerodha / Kotak) | Delta Exchange Crypto Engine |
| :--- | :--- | :--- |
| **Market Hours** | 09:15 – 15:30 IST (Mon–Fri) | **24/7 Continuous Trading (365 days/year)** |
| **Asset Configuration** | `token_ref.xlsx` (`nfo_config`, `nfo_list`) | **`token_ref_bitcoin.xlsx` (`bit_config`, `bit_list`)** |
| **Instrument Universe** | NSE / NFO / MCX / CDS / BFO | **FUTURES (Perpetuals), OPTIONS (Calls/Puts), SPOT** |
| **Option Protocol** | Broker Order Routing (Kite / Neo REST) | **Delta Exchange v2 REST API (`/v2/orders`, `/v2/orders/batch`)** |
| **Authentication** | OAuth 2.0 / TOTP 2FA Session Bearers | **HMAC-SHA256 Signatures with Clock Drift Auto-Sync** |
| **Wallet Balance** | Segmented Cash & Collateral Margins | **Delta Exchange Wallet Balance (`/v2/wallet/balances`)** |
| **Stop Loss Trailing** | Afternoon Session (12:45 – 15:30 IST) | **Nightly Window (`sl_update_time`: 01:30 – 01:31 AM IST / Continuous in Debug)** |
| **WebSocket Streaming** | Binary tick protocol (Kite / Neo) | **Real-Time JSON WebSocket (`wss://public-socket.india.delta.exchange`)** |

---

## 3. Master Instrument Token Resolution & `cum_table` Assembly

The engine resolves live contract specifications directly from Delta Exchange via [`DeltaExchangeUtility.get_products()`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/delta_utils.py#L705) and constructs a normalized 36-column `cum_table` matching the exact schema of [`coinswitch_opt_trde_polars.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/coinswitch_opt_trde_polars.py):

### A. Contract Categorization & Sheets
1. **`FUTURES`**: Perpetual contracts (`BTCUSD`, `ETHUSD`, `SOLUSD`) and dated quarterly futures.
2. **`OPTIONS`**: European Call (`CE`) and Put (`PE`) option contracts across multiple strike prices and expirations (e.g. `C-BTC-77400-050926`, `P-ETH-2900-250926`).
3. **`SPOT`**: Spot market pairs (`BTC_USDT`, `ETH_USDT`).
4. **`ALL_CRYPTO` / `nfo_cds_mcx`**: Unified master dataframe of all tradeable contracts.

### B. Normalized Schema (36 Columns)
```python
[
    'instrument_token', 'exchange_token', 'tradingsymbol', 'name', 'last_price',
    'expiry', 'strike', 'tick_size', 'lot_size', 'instrument_type', 'segment',
    'exchange', 'max_leverage', 'status', 'exp_date_list', 'exp_month', 'cap',
    'Capital_share', 'Max_lots_per_order', 'Tradable_stock', 'Ref_stock',
    'Ref_stock_tkn', 'Index_tkn', 'current_month', 'CE_jump', 'PE_jump',
    'day_fall', 'day_rise', 'buy_signal_PE', 'buy_signal_CE', 'current_value',
    'ATM_ITM_OTM', 'Buy_strike', 'recent_sell_order_time', 'recent_buy_order_time', 'week_dist'
]
```

### C. Underlying Asset & Reference Contract Resolution
- **Underlying Base Mapping**: Strips derivatives down to base coin symbol (`BTC`, `ETH`, `SOL`).
- **Ref Stock Mapping**: Automatically links every option to its underlying perpetual futures contract (e.g. `Ref_stock = "BTCUSD"`, `Ref_stock_tkn = str_to_token("BTCUSD")`, `Index_tkn = Ref_stock_tkn`).
- **Expiry Classification**:
  - `exp_date_list <= 7` $\rightarrow$ `cap = "weekely_options"`
  - `exp_date_list > 7` $\rightarrow$ `cap = "monthly_options"`
  - `segment == 'FUTURES'` $\rightarrow$ `cap = "crypto_perp_futures"`

### D. ExchangeMasterData Fast Crash Recovery & Unified Parity
- **Instant Crash Recovery with Set-Equality Invalidation**: On engine startup or restart, `master_tkn_list` inspects `master_tkn_list_update_time` in database state. If previously updated today (`up_time == today_ist()`) and cached symbols match current active configuration symbols (`Capital_share > 0`) using exact set equality (`cached_all_symbols == current_active_symbols`), it restores the pre-assembled `cum_table` directly from `ExchangeMasterData` via zero-copy Polars deserialization, completely bypassing external REST API calls. If active symbols differ or inactive underlyings are detected, the cache is automatically invalidated and fresh contracts assembled.
- **Unified Engine Parity**: The table generated by `delta_opt_trde_polars.py` maintains **100% exact parity** (identical rows, columns, tokens, and symbols) with the unified crypto engine [`crypto_opt_trde_polars.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/crypto_opt_trde_polars.py).

### E. Forward Option Chain Synthesis & Multi-Underlying Token Parity
- **Exchange Options Availability**: While Delta Exchange lists full options chains for `BTC`, other configured underlyings (`ETH`, `SOL`, `XRP` in `token_ref_bitcoin.xlsx`) may have only short-dated same-day options or no options listed on the regional exchange.
- **Dynamic Forward Chain Generation (`generate_synthetic_crypto_options`)**: When `assemble_crypto_cum_table()` detects that an underlying in `aug_table` lacks forward options (`expiry > tomorrow`), it automatically generates standard weekly and monthly CE/PE option chains centered around base prices (`KNOWN_CRYPTO_BASE_PRICES`).
- **Exact 12-Token Subscription Parity**: With forward options generated for all 4 configured underlyings, `TradeAlgo.token_list_update()` resolves exactly **12 subscribed tokens** (4 underlying perpetual contracts + 4 CE strikes + 4 PE strikes) and persists them to `{account_id}_inst_tokens`, ensuring full options coverage across the entire portfolio.

---

## 4. WebSocket Feed & Database Partitioning

### A. Delta Exchange Feed ([`algo_trading/brokers/feeds/delta.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/brokers/feeds/delta.py))
- **India Endpoint**: `wss://public-socket.india.delta.exchange`
- **Global Endpoint**: `wss://socket.delta.exchange`
- **Subscription Channels**: Dispatches individual JSON payload frames for `ticker` (India) / `v2/ticker` (Global), `all_trades`, and `l2_orderbook`.
- **Normalization**: Parses compact ticker `d` array structures (Mark Price, Spot Price, OHLC, Best Bid/Ask, and Turnover) into unified DeltaZero26 tick dictionary format.

### B. PostgreSQL Partitioning & Numeric Account Safety
- Table naming safely handles leading digits on account IDs (e.g. `73270496` $\rightarrow$ `b_73270496_stream_kv`).
- Auto-creates weekly date-range partitions (`b_73270496_stream_kv_yYYYY_wWW`) via `ensure_weekly_partition()` stored procedures.
- Dual-persists into both account-specific stream tables and Django Admin [`kalai_processedtickstore`](file:///c:/Users/Admin/Documents/deltazero26/kalai/models.py#L381).

---

## 5. Clock Drift Auto-Sync & Authentication

### A. HMAC-SHA256 Signing
Signatures are computed over `METHOD + TIMESTAMP + PATH + QUERY_STRING + PAYLOAD`:
```python
signature = hmac.new(
    api_secret.encode("utf-8"),
    message.encode("utf-8"),
    hashlib.sha256,
).hexdigest()
```

### B. Network Clock Drift Compensation
Delta Exchange rejects signature timestamps with $> 5\text{s}$ drift (`expired_signature`). The utility automatically inspects server time in error contexts, calculates `_server_time_offset = server_time - local_time`, and resends requests with zero-latency timestamp precision.

---

## 6. Execution & Verification

### Running Strategy Iteration
```powershell
# Run standalone debug iteration
.venv\Scripts\python.exe algo_trading\algos\delta_opt_trde_polars.py --debug --iterations 5

# Run automated tests
.venv\Scripts\pytest.exe tests/test_delta_utils.py tests/test_delta_feed.py -v
```
