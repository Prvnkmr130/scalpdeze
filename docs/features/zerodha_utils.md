# Zerodha Order Management & Portfolio Utility (`ZerodhaUtility`)

The `ZerodhaUtility` class (`algo_trading/algos/zerodha_utils.py`) provides a high-level wrapper around Zerodha's KiteConnect v3 API. It abstracts session retrieval, network retries, account margin checks, position inspections, and multi-variety order execution for algorithmic trading strategies in DeltaZero26.

---

## 1. Overview & Initialization

`ZerodhaUtility` automatically fetches credentials and active `access_token` from the `Broker` database table. It supports multi-account trading by resolving accounts dynamically via `account_id`.

### Initialization Examples

```python
from algo_trading.algos.zerodha_utils import ZerodhaUtility

# 1. Initialize for a specific account (Recommended for multi-account)
util = ZerodhaUtility(account_id="HS6525")

# 2. Or auto-load the primary active Zerodha account
util = ZerodhaUtility()

# 3. Direct API key & access token initialization (useful for isolated scripts/testing)
util = ZerodhaUtility(
    api_key="YOUR_API_KEY",
    access_token="YOUR_ACCESS_TOKEN",
    account_id="direct_zerodha"
)

# 4. Inject a pre-configured or mock client
util = ZerodhaUtility(client=mock_kite_client)
```

> [!NOTE]
> **Resilient Discovery**: If credentials or `access_token` are unconfigured, `ZerodhaUtility` initializes safely in unauthenticated mode. Public instrument discovery methods (`master_tkn_list()`, `instruments_list()`) operate without crashing, while authenticated trading methods are guarded and fail gracefully.

---

## 2. API Method Reference

### A. Account Balances & Portfolio Inspection

#### `chk_live_bal()`
Fetches live equity cash and net available margin from Kite Connect API.
* **Returns**: `tuple[float, float]` $\rightarrow$ `(avail_cash, net_capital)`
* **Example**:
  ```python
  cash, cap = util.chk_live_bal()
  print(f"Available Cash: ₹{cash:,.2f} | Net Capital: ₹{cap:,.2f}")
  ```

#### `holdings()`
Fetches long-term equity portfolio holdings returned as a native Polars DataFrame.
* **Returns**: `pl.DataFrame | None`
* **Example**:
  ```python
  df = util.holdings()
  if df is not None and not df.is_empty():
      print(df.select(['tradingsymbol', 'quantity', 'average_price', 'pnl']))
  ```

#### `pos_data()`
Fetches current day and net open positions returned as Polars DataFrames.
* **Returns**: `tuple[pl.DataFrame | None, pl.DataFrame | None]` $\rightarrow$ `(day_positions_df, net_positions_df)`
* **Example**:
  ```python
  day_df, net_df = util.pos_data()
  if net_df is not None and not net_df.is_empty():
      print("Net Open Positions:", len(net_df))
  ```

#### `orders()` & `order_history(ord_id)`
Fetches today's order book or the history of a specific order ID.
* **Returns**: `list[dict]`
* **Example**:
  ```python
  all_orders = util.orders()
  history = util.order_history(ord_id="240821000123")
  ```

---

### B. Order Execution Methods

All order placement methods return `tuple[order_id, order_msg]`. If placement fails, `order_id` is `-1` and `order_msg` contains the error description.

#### 1. Market Order (`mrk_ordr`)
Places an instantaneous market order (`variety='regular'`, `order_type='MARKET'`).
```python
order_id, msg = util.mrk_ordr(
    symbol="INFY",
    quantity=10,
    buy_sell="BUY",    # 'BUY' or 'SELL'
    exchg="NSE",       # 'NSE', 'BSE', 'NFO', 'MCX'
    prod="MIS",        # 'MIS' (Intraday) or 'CNC' (Delivery) / 'NRML'
    ttl_value=None     # Optional Time-To-Live in seconds
)
```

#### 2. Limit Order (`lim_ordr`)
Places a limit order at a specific price.
```python
order_id, msg = util.lim_ordr(
    symbol="INFY",
    quantity=10,
    buy_sell="BUY",
    exchg="NSE",
    prod="MIS",
    price=1850.50,
    validity="DAY"     # 'DAY' or 'IOC'
)
```

#### 3. Stop-Loss Market Order (`slmkt_ordr`)
Places a Stop-Loss Market order (`SL-M`) triggered when market price crosses `trig_price`.
```python
order_id, msg = util.slmkt_ordr(
    symbol="INFY",
    quantity=10,
    buy_sell="SELL",
    exchg="NSE",
    prod="MIS",
    trig_price=1800.00
)
```

#### 4. Stop-Loss Limit Order (`sl_ordr`)
Places a Stop-Loss Limit order (`SL`) triggered at `trig_price` and executed at `price`.
```python
order_id, msg = util.sl_ordr(
    symbol="INFY",
    quantity=10,
    buy_sell="SELL",
    exchg="NSE",
    prod="MIS",
    price=1798.00,
    trig_price=1800.00
)
```

#### 5. Iceberg Order (`ice_ordr`)
Splits a large quantity order into smaller visible legs to minimize market impact.
```python
order_id, msg = util.ice_ordr(
    symbol="INFY",
    quantity=1000,
    buy_sell="BUY",
    exchg="NSE",
    prod="MIS",
    price=1850.00,
    legs=5,            # Split into 5 smaller orders
    ice_qty=200,       # 200 shares per leg
    order_type="LIMIT"
)
```

#### 6. Cancel & Exit Orders (`cancel_ordr` & `exit_ordr`)
```python
# Cancel an open limit/SL order
util.cancel_ordr(variety="regular", order_id="240821000123")

# Exit an open bracket / regular position
util.exit_ordr(ord_id="240821000123")
```

---

### C. Margin Calculation (`get_margin`)

Calculates required margin before placing trades.
* **Input**: `pd.DataFrame` or `list[dict]` containing order parameters.
* **Example**:
  ```python
  orders_to_check = [{
      "exchange": "NSE",
      "tradingsymbol": "INFY",
      "transaction_type": "BUY",
      "variety": "regular",
      "product": "MIS",
      "order_type": "MARKET",
      "quantity": 10
  }]
  margin_data = util.get_margin(orders_to_check)
  print("Total Margin Required: ₹", margin_data[0]["total"])
  ```

---

## 3. Asynchronous Strategy Integration Pattern

Because `KiteConnect` makes synchronous HTTP network requests, algorithmic strategies running on the asyncio event loop should execute `ZerodhaUtility` calls inside `asyncio.to_thread` to maintain sub-millisecond execution speeds:

```python
import asyncio
from algo_trading.algos import algo
from algo_trading.algos.logger import algo_logger
from algo_trading.algos.zerodha_utils import ZerodhaUtility

@algo
async def execute_trading_strategy(account_id: str):
    # 1. Initialize utility
    util = ZerodhaUtility(account_id=account_id)
    
    # 2. Inspect balance in worker thread
    avail_cash, net_cap = await asyncio.to_thread(util.chk_live_bal)
    
    # 3. Log with ISO timestamp
    await algo_logger.add_log(
        account_id,
        f"Available Cash: ₹{avail_cash:,.2f} | Net Capital: ₹{net_cap:,.2f}",
        tablename="strategy_logs"
    )
    
    # 4. Conditionally place order
    if net_cap > 50000:
        order_id, msg = await asyncio.to_thread(
            util.mrk_ordr,
            symbol="INFY",
            quantity=5,
            buy_sell="BUY",
            prod="MIS"
        )
```

---

## 4. Master Instrument Token Resolution & Config-Aware Recovery

`ZerodhaUtility.master_tkn_list(input_file, ...)` delegates to `assemble_indian_cum_table(broker_name="zerodha")`:
1. **Config-Aware Crash Recovery with Exact Set Equality**: Checks if `ExchangeMasterData` contains a pre-computed `cum_table` updated today. Validates that active symbols configured with `Capital_share > 0` match the cached table using exact set equality (`cached_all_symbols == current_active_symbols`). If configuration has changed or inactive symbols are present in cache, the cache is automatically invalidated and a fresh assembly is triggered immediately. When restoring from cache, current `Capital_share` weights are dynamically synchronized into the table.
2. **Early Ingestion Filtering (`build_aug_table`)**: Candidate underlyings are filtered upfront via `build_aug_table()` strictly by `Capital_share > 0` and `Max_lots_per_order > 0`, ignoring `Tradable_stock`.
3. **Canonical Key Registration**: Registers derivative and cash instruments into [`BrokerTokenMapper`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/broker_token_mapper.py) with canonical option/future keys (`CanonicalInstrumentKey`).
4. **Strict Active Subscription Filtering**: Filters `init_ref_list` and `index_ref_list` strictly requiring `(Capital_share > 0) & (instrument_token > 0)`, ensuring zero inactive contracts are subscribed.
5. **Persistence**: Saves the assembled table to `ExchangeMasterData` for sub-second engine startup and restart recovery.

