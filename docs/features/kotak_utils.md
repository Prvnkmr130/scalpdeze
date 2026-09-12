# Kotak Neo Order Management & Portfolio Utility (`KotakNeoUtility`)

The `KotakNeoUtility` class ([`algo_trading/algos/kotak_utils.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/kotak_utils.py)) provides a high-level wrapper around Kotak Neo's REST APIs and ScripMaster endpoints. It abstracts session retrieval, network retries, account margin checks, position inspections, ScripMaster token normalization, and multi-variety order execution for algorithmic trading strategies in DeltaZero26.

---

## 1. Overview & Initialization

`KotakNeoUtility` automatically resolves credentials and active session tokens (`token:::sid` or direct Bearer tokens) from the `Broker` database table. It supports multi-account trading by resolving accounts dynamically via `account_id` or explicit `broker_obj`.

### Initialization Examples

```python
from algo_trading.algos.kotak_utils import KotakNeoUtility

# 1. Initialize for a specific Kotak Neo account (Recommended)
util = KotakNeoUtility(account_id="W1NPY")

# 2. Or pass a Django Broker instance directly
util = KotakNeoUtility(broker_obj=broker_instance)

# 3. Direct API key & session token initialization (useful for isolated scripts/testing)
util = KotakNeoUtility(
    api_key="YOUR_CONSUMER_KEY",
    access_token="YOUR_SESSION_TOKEN:::SID",
    account_id="direct_kotak"
)

# 4. Inject a pre-configured or mock client
util = KotakNeoUtility(client=mock_client)
```

> [!NOTE]
> **Resilient Discovery & Fast Failover**: If Kotak credentials or access tokens are missing or expired, `KotakNeoUtility` initializes in unauthenticated mode without raising an exception. This allows ScripMaster discovery (`instruments_list()`, `master_tkn_list()`) to succeed, while live order and portfolio methods return structured error codes (`-1`) or `(0.0, 0.0)`. Network retries fail fast on HTTP 401/403 errors.

---

## 2. API Method Reference

### A. Account Balances & Portfolio Inspection

#### `chk_live_bal()`
Fetches live equity cash and net available margin from Kotak Neo Limits API.
* **Returns**: `tuple[float, float]` $\rightarrow$ `(avail_cash, net_capital)`
* **Example**:
  ```python
  cash, cap = util.chk_live_bal()
  print(f"Available Cash: ₹{cash:,.2f} | Net Capital: ₹{cap:,.2f}")
  ```

#### `holdings()`
Fetches long-term equity portfolio holdings returned as a Polars DataFrame.
* **Returns**: `pl.DataFrame | None`
* **Columns**: `['tradingsymbol', 'exchange', 'quantity', 'average_price', 'last_price', 'pnl']`

#### `pos_data()`
Fetches current day and net open positions returned as Polars DataFrames.
* **Returns**: `tuple[pl.DataFrame | None, pl.DataFrame | None]` $\rightarrow$ `(day_positions_df, net_positions_df)`
* **Example**:
  ```python
  day_df, net_df = util.pos_data()
  if net_df is not None and not net_df.is_empty():
      print("Open Positions:", len(net_df))
  ```

#### `orders()`, `order_history(ord_id)`, & `order_trades(ord_id)`
Fetches today's order book, historical transitions for a specific order ID, or trade executions.
* **Returns**: `list[dict]`
* **Example**:
  ```python
  all_orders = util.orders()
  history = util.order_history(ord_id="240903000123")
  trades = util.order_trades(ord_id="240903000123")
  ```

---

### B. Order Execution Methods

All order placement methods return `tuple[order_id, order_msg]`. If placement fails, `order_id` is `-1` and `order_msg` contains the error description.

#### 1. Market Order (`mrk_ordr`)
Places an instantaneous market order (`variety='regular'`, `order_type='MKT'`).
```python
order_id, msg = util.mrk_ordr(
    symbol="NIFTY26SEP24500CE",
    quantity=50,
    buy_sell="BUY",    # 'BUY' or 'SELL'
    exchg="NFO",       # 'NSE', 'BSE', 'NFO', 'MCX', 'BFO'
    prod="MIS",        # 'MIS' (Intraday) or 'NRML' / 'CNC'
    ttl_value=None     # Optional Time-To-Live in seconds
)
```

#### 2. Limit Order (`lim_ordr`)
Places a limit order at a specific price.
```python
order_id, msg = util.lim_ordr(
    symbol="NIFTY26SEP24500CE",
    quantity=50,
    buy_sell="BUY",
    exchg="NFO",
    prod="NRML",
    price=125.50,
    validity="DAY"     # 'DAY' or 'IOC'
)
```

#### 3. Stop-Loss Limit & Market Orders (`sl_ordr`, `slmkt_ordr`)
Places trigger-based stop-loss orders.
```python
# Stop-Loss Limit
order_id, msg = util.sl_ordr(
    symbol="NIFTY26SEP24500CE",
    quantity=50,
    buy_sell="SELL",
    exchg="NFO",
    prod="NRML",
    price=95.00,
    trigger_price=96.00
)

# Stop-Loss Market
order_id, msg = util.slmkt_ordr(
    symbol="NIFTY26SEP24500CE",
    quantity=50,
    buy_sell="SELL",
    exchg="NFO",
    prod="NRML",
    trigger_price=96.00
)
```

#### 4. Iceberg Order (`ice_ordr`)
Splits large quantities into smaller visible slices with configurable leg delays:
```python
order_id, msg = util.ice_ordr(
    symbol="BANKNIFTY26SEP52000CE",
    total_quantity=600,
    legs=4,            # Slices of 150 each
    buy_sell="BUY",
    exchg="NFO",
    prod="NRML",
    order_type="L",
    price=210.0,
    leg_delay=1.0
)
```

#### 5. Order Cancellation & Exit (`cancel_ordr`, `exit_ordr`)
Cancels open orders or closes open positions:
```python
success, msg = util.cancel_ordr(ord_id="240903000123")
success, msg = util.exit_ordr(sym="NIFTY26SEP24500CE", exchg="NFO", prod="NRML")
```

---

### C. ScripMaster & Master Token Resolution (`master_tkn_list`)

`KotakNeoUtility.master_tkn_list()` is a high-performance Polars-native ScripMaster parser and contract assembler:
1. **Config-Aware Crash Recovery with Exact Set Equality**: Checks if `ExchangeMasterData` contains a pre-computed `cum_table` updated today. Verifies that active symbols configured with `Capital_share > 0` match the cached table using exact set equality (`cached_all_symbols == current_active_symbols`). If configuration has changed or inactive symbols are present in cache, the cache is automatically invalidated and a fresh assembly is triggered immediately. When restoring from cache, current `Capital_share` weights are dynamically synchronized into the table.
2. **Early Ingestion Filtering (`build_aug_table`)**: Ingests configuration via `build_aug_table()`, strictly filtering by `Capital_share > 0` and `Max_lots_per_order > 0` upfront while ignoring `Tradable_stock`.
3. **Index Alias Normalization**: Uses [`BrokerTokenMapper`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/broker_token_mapper.py) to map cash index symbols from Excel configurations (`NIFTY 50`, `NIFTY BANK`, `NIFTY FIN SERVICE`) to Kotak Neo's native index representations (`NIFTY` token `26000`, `BANKNIFTY` token `26009`, `FINNIFTY` token `26037`), ensuring index tokens resolve accurately without falling back to futures tokens.
4. **ScripMaster Download**: Fetches scrip master CSVs across NSE (`nse_cm`), NFO (`nse_fo`), BSE (`bse_cm`), BFO (`bse_fo`), and MCX (`mcx_fo`).
5. **Normalization & Canonical Keys**: Converts raw Kotak tokens (e.g. `nse_fo|54321`) into numerical integers, extracts strike prices, expiry dates, and contract types (`CE`, `PE`, `FUT`), and registers all instruments into `BrokerTokenMapper` with canonical keys.
6. **Strict Subscription Filtering**: Filters `init_ref_list` and `index_ref_list` strictly requiring `(Capital_share > 0) & (instrument_token > 0)`, ensuring zero inactive contracts are subscribed.
7. **Persistence**: Saves the resulting table to `ExchangeMasterData` for instant sub-second engine recovery.

