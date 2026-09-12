# Delta Exchange Order Management & WebSocket Utility (`DeltaExchangeUtility`)

The `DeltaExchangeUtility` class (`algo_trading/algos/delta_utils.py`) and `DeltaExchangeFeed` (`algo_trading/brokers/feeds/delta.py`) provide production-grade integration with **Delta Exchange** (both Global and India platforms). It abstracts multi-account database credential loading, HMAC-SHA256 request signing, network retries with exponential backoff, account margin/balance checks, spot holdings, perpetual futures & options positions, candlestick history, order execution, and real-time WebSocket tick streaming.

---

## 1. Overview & Initialization

`DeltaExchangeUtility` supports multi-account trading by resolving credentials (`api_key` and `api_secret`) dynamically via `account_id` from the `kalai_broker` table or direct parameters.

### Multi-Environment Support:
* **Global**: `https://api.delta.exchange/v2` (REST) | `wss://socket.delta.exchange` (WebSocket)
* **India**: `https://api.india.delta.exchange/v2` (REST) | `wss://socket.india.delta.exchange` (WebSocket)

### Initialization Examples:

```python
from algo_trading.algos.delta_utils import DeltaExchangeUtility

# 1. Initialize for a specific account (Recommended for multi-account)
util = DeltaExchangeUtility(account_id="delta_main")

# 2. Or auto-load the primary active Delta Exchange account from database
util = DeltaExchangeUtility()

# 3. Direct API key initialization (useful for isolated scripts/testing)
util = DeltaExchangeUtility(
    api_key="YOUR_DELTA_API_KEY",
    api_secret="YOUR_DELTA_API_SECRET",
    base_url="https://api.delta.exchange", # or https://api.india.delta.exchange
    account_id="direct_delta"
)

# 4. Inject a pre-configured or mock client
util = DeltaExchangeUtility(client=mock_client)
```

> [!NOTE]
> **Resilient Discovery & Authentication Guards**: When Delta Exchange credentials are unconfigured or not found in the DB, `DeltaExchangeUtility` falls back cleanly to public mode without raising an unhandled exception. Public endpoints (`get_products()`, `get_ticker()`, `get_candles()`) operate unhindered, while authenticated endpoints (`_auth_request`, order placement, balances) fail fast with clear error logging. Network retries skip 401/403 and signature mismatch errors immediately.

---

## 2. API Method Reference

### A. Account Balances & Portfolio Inspection

#### `chk_live_bal()`
Fetches live available cash and total net portfolio capital across all assets (USDT, USD, BTC, INR).
* **Returns**: `tuple[float, float]` $\rightarrow$ `(available_cash, total_net_capital)`
* **Example**:
  ```python
  avail_cash, net_cap = util.chk_live_bal()
  print(f"Available Cash: {avail_cash:,.2f} | Total Capital: {net_cap:,.2f}")
  ```

#### `holdings()`
Fetches spot/collateral wallet balances returned as a Polars DataFrame.
* **Returns**: `pl.DataFrame | None`
* **Columns**: `asset_symbol`, `balance`, `available_balance`, `locked_balance`, `tradingsymbol`
* **Example**:
  ```python
  df = util.holdings()
  if df is not None and not df.is_empty():
      print(df)
  ```

#### `pos_data(pair=None)`
Fetches active open positions for Perpetual Futures and Options derivatives.
* **Parameters**: `pair: str | None` (e.g. `'BTCUSD'`, `'ETHUSD'`)
* **Returns**: `tuple[pl.DataFrame | None, pl.DataFrame | None]` $\rightarrow$ `(day_positions_df, net_positions_df)`
* **Columns**: `symbol`, `product_id`, `size`, `entry_price`, `mark_price`, `liquidation_price`, `margin`, `unrealized_pnl`, `realized_pnl`

---

### B. Order Execution Methods

All standard placement methods return `tuple[order_id, order_msg]`. If placement fails, `order_id` is `-1` and `order_msg` contains the error description.

#### 1. Market Order (`mrk_ordr`)
```python
order_id, msg = util.mrk_ordr(
    symbol="BTCUSD",
    quantity=1,
    buy_sell="BUY"
)
```

#### 2. Limit Order (`lim_ordr`)
```python
order_id, msg = util.lim_ordr(
    symbol="BTCUSD",
    quantity=1,
    buy_sell="BUY",
    price=90000.0,
    time_in_force="gtc"
)
```

#### 3. Stop-Loss Order (`sl_ordr`)
```python
order_id, msg = util.sl_ordr(
    symbol="BTCUSD",
    quantity=1,
    buy_sell="SELL",
    trig_price=88000.0,
    price=87900.0
)
```

#### 4. Order Cancellation (`cancel_ordr` & `cancel_all`)
```python
# Cancel single order
success, msg = util.cancel_ordr(order_id="1234567")

# Cancel all open orders for a product or entire account
success, msg = util.cancel_all(symbol="BTCUSD")
```

#### 5. Historical Candlesticks (`candles`)
```python
df_candles = util.candles(symbol="BTCUSD", resolution="1m", limit=100)
```

---

### C. `delta-rest-client` Official Method Aliases

For developers transitioning from the official `delta-rest-client` PyPI library, `DeltaExchangeUtility` includes 1-to-1 method aliases:

| `delta-rest-client` Method | `DeltaExchangeUtility` Method | Description |
| :--- | :--- | :--- |
| `get_wallet(asset_id)` | `util.get_wallet(asset_id)` | Query account wallet balances |
| `get_ticker(symbol)` | `util.get_ticker(symbol)` | Fetch 24hr market ticker |
| `get_product(product_id)` | `util.get_product(product_id)` | Lookup instrument metadata |
| `get_products()` | `util.get_products()` | Retrieve list of all tradeable products |
| `get_assets()` | `util.get_assets()` | Query all underlying assets |
| `get_orders(status)` | `util.get_orders(status)` | Query open / past orders as list |
| `orders()` / `order_book()` | `util.orders()` / `util.order_book()` | Query open orders as Polars DataFrame |
| `create_order(...)` | `util.create_order(...)` | Place custom order |
| `batch_create(...)` | `util.batch_create(...)` | Place multiple orders in a single request |
| `cancel_order(pid, oid)` | `util.cancel_order(pid, oid)` | Cancel an order |
| `get_positions()` | `util.get_positions()` | Get open positions |
| `get_l2_orderbook(pid)` | `util.get_l2_orderbook(pid)` | Level 2 Orderbook depth |

---

## 3. Clock Drift & Authentication Resilience

`DeltaExchangeUtility` implements zero-latency signature synchronization and credential defense:
- **HMAC-SHA256**: Generates signatures over `METHOD + TIMESTAMP + PATH + QUERY_STRING + PAYLOAD`.
- **Clock Drift Auto-Compensation**: When Delta Exchange returns `expired_signature` due to network or local clock skew, `_auth_request` reads `server_time` from the response error context, sets `_server_time_offset`, and automatically resends the request with zero manual intervention.
- **Public Endpoint Fallback**: Public calls (`get_products()`, `get_ticker()`, `get_assets()`, `candles()`) execute unauthenticated, allowing instrument discovery even before API secrets are provided.
- **Pre-Flight Credentials Defense & Safe Returns**: Authenticated endpoints (`pos_data()`, `chk_live_bal()`, `holdings()`) verify that both `api_key` and `api_secret` are present. If either is missing:
  - Logs a structured error directly to `algo_logger.log_sync` bound to `self.broker` and `crypto_opt_trde_polars`.
  - Returns safe empty Polars DataFrames `(empty_df, empty_df)` or zero balances `(0.0, 0.0)` instead of raising uncaught exceptions or cascading `None` type errors.
- **Engine Auto-Suspension & Dynamic Reactivation**: In `crypto_opt_trde_polars.py`, accounts with `enable_trade=True` but incomplete credentials log a single warning and have trading suspended, preventing tight error loops. The engine dynamically checks the broker record on each cycle: as soon as the `api_secret` is saved in Django Admin, live trading activates automatically.

---

## 4. WebSocket Real-Time Feed (`DeltaExchangeFeed`)

The WebSocket feed (`algo_trading/brokers/feeds/delta.py`) connects to Delta Exchange WebSocket streams, handles automatic reconnects, manages private channel HMAC-SHA256 authentication, and normalizes real-time tickers and trades into the unified DeltaZero26 queue.

### Endpoints:
* **India Platform**: `wss://public-socket.india.delta.exchange`
* **Global Platform**: `wss://socket.delta.exchange`

### Subscription Channels:
* `ticker` (India) / `v2/ticker` (Global): Real-time mark price, spot price, OHLC, 24h volume, top bid/ask.
* `all_trades`: Live trade execution stream.
* `l2_orderbook`: Level-2 orderbook bids and asks.

---

## 5. Master Instrument Token Assembly & Config-Aware Crash Recovery

`DeltaExchangeUtility.master_tkn_list(input_file, ...)` manages instrument discovery, contract normalization, and active subscription tables:
- **Broker-Aware Underlying Resolution**: Resolves reference underlying symbols by checking candidates `[f"{c_root}USD", f"{c_root}USDT", ...]`. For Delta Exchange India/Global, this correctly resolves `Ref_stock = "BTCUSD"` with genuine perpetual contract ID `27`, completely eliminating fallback pseudo-MD5 hashes.
- **`CanonicalInstrumentKey` Registration**: Every derivative and perpetual contract is mapped and registered into [`BrokerTokenMapper`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/broker_token_mapper.py) with canonical option/future keys.
- **Config-Aware Cache Invalidation**: When evaluating cached `cum_table` entries in `ExchangeMasterData`, checks active symbols where `Capital_share > 0` against the current configuration. If symbols change, the cache is invalidated and fresh contracts are assembled from the Delta Exchange API. Current `Capital_share` weights are dynamically synchronized into restored tables.
- **Strict Active Subscription Filtering**: `init_ref_list` and `index_ref_list` filter strictly for `(Capital_share > 0) & (instrument_token > 0)`, ensuring zero inactive contracts are subscribed.
- **Forward Option Chain Synthesis**: For underlyings in `token_ref_bitcoin.xlsx` that lack exchange forward options (`expiry > tomorrow`) on Delta Exchange India (e.g. `ETH` daily options only, `SOL` and `XRP` perpetuals only), `assemble_crypto_cum_table()` synthesizes standard weekly and monthly option chains centered around benchmark base prices. This ensures all active underlyings produce 1 CE and 1 PE option candidate, reaching the expected **12 subscribed tokens** (4 perpetual underlyings + 8 option strikes) in `{account_id}_inst_tokens`.
- **`week_dist` Integer Calendar Week Parity**: Upon restoring cached `cum_table` from `ExchangeMasterData`, `week_dist` is explicitly cast to `Int64` to maintain strict calendar week integer offset parity with `crypto_master_tokens.py` and `indian_master_tokens.py`.


