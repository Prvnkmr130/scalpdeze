# CoinDCX Order Management & Portfolio Utility (`CoinDCXUtility`)

The `CoinDCXUtility` class (`algo_trading/algos/coindcx_utils.py`) provides a high-level wrapper around CoinDCX's authenticated REST and public Market Data APIs. It abstracts credential loading, HMAC-SHA256 request signing, network retries, account margin checks, position inspections, candlestick retrieval, and multi-variety order execution for algorithmic trading in DeltaZero26.

---

## 1. Overview & Initialization

`CoinDCXUtility` automatically fetches credentials (`api_key` and `api_secret`) from the `Broker` database table. It supports multi-account trading by resolving accounts dynamically via `account_id` or direct parameter initialization.

### Initialization Examples

```python
from algo_trading.algos.coindcx_utils import CoinDCXUtility

# 1. Initialize for a specific account (Recommended for multi-account)
util = CoinDCXUtility(account_id="coindcx_main")

# 2. Or auto-load the primary active CoinDCX account from database
util = CoinDCXUtility()

# 3. Direct API key initialization (useful for isolated scripts/testing)
util = CoinDCXUtility(
    api_key="YOUR_COINDCX_API_KEY",
    api_secret="YOUR_COINDCX_API_SECRET",
    account_id="custom_coindcx"
)

# 4. Inject a pre-configured or mock client
util = CoinDCXUtility(client=mock_client)
```

> [!NOTE]
> **Resilient Discovery & Authentication Guards**: When CoinDCX credentials are unconfigured or missing from the database, `CoinDCXUtility` initializes in public market data mode. Public endpoints (`instruments_list()`, `get_ticker()`, `get_orderbook()`, `get_candles()`) function normally, while authenticated methods (`_auth_get`, `_post`, `chk_live_bal()`, `pos_data()`, order placement) are safely guarded and fail fast without network spamming.

---

## 2. API Method Reference

### A. Account Balances & Portfolio Inspection

#### `chk_live_bal()`
Fetches live USDT available cash and total net capital across Spot wallets and Futures derivatives accounts.
* **Returns**: `tuple[float, float]` $\rightarrow$ `(avail_cash, net_capital)`
* **Example**:
  ```python
  cash, cap = util.chk_live_bal()
  print(f"Available Cash (USDT): ${cash:,.2f} | Net Capital: ${cap:,.2f}")
  ```

#### `holdings()`
Fetches non-zero spot wallet crypto holdings with balances and locked amounts as a native Polars DataFrame.
* **Returns**: `pl.DataFrame | None`
* **Columns**: `currency`, `balance`, `locked_balance`, `total`, `tradingsymbol`
* **Example**:
  ```python
  df = util.holdings()
  if df is not None and not df.is_empty():
      print(df.select(['tradingsymbol', 'balance', 'locked_balance', 'total']))
  ```

#### `pos_data(pair=None)`
Fetches current open Futures derivatives positions returned as Polars DataFrames.
* **Parameters**: `pair: str | None` (e.g. `'B-BTC_USDT'`)
* **Returns**: `tuple[pl.DataFrame | None, pl.DataFrame | None]` $\rightarrow$ `(day_positions_df, net_positions_df)`
* **Columns**: `pair`, `active_pos`, `avg_price`, `liquidation_price`, `locked_margin`, `unrealized_pnl`, `realized_pnl`
* **Example**:
  ```python
  day_df, net_df = util.pos_data(pair="B-BTC_USDT")
  if net_df is not None and not net_df.is_empty():
      print("Active Positions:", len(net_df))
  ```

#### `orders(is_futures=True, pair=None)` & `order_history(ord_id, is_futures=True)`
Fetches active open orders or the status history of a specific order ID.
* **Returns**: `list[dict]`
* **Example**:
  ```python
  active_orders = util.orders(is_futures=True, pair="B-BTC_USDT")
  status = util.order_history(ord_id="12345678", is_futures=True)
  ```

#### `order_trades(ord_id=None, is_futures=True)`
Fetches trade execution history for the account.
* **Returns**: `list[dict]`
* **Example**:
  ```python
  trades = util.order_trades(ord_id="12345678", is_futures=True)
  ```

---

### B. Order Execution Methods

All order placement methods return `tuple[order_id, order_msg]`. If placement fails, `order_id` is `-1` and `order_msg` contains the error description.

#### 1. Market Order (`mrk_ordr`)
Places an instantaneous market order executed at current market price. Supports both Futures and Spot.
```python
# Futures Market Order (auto-detected via 'B-' prefix or is_futures=True)
order_id, msg = util.mrk_ordr(
    symbol="B-BTC_USDT",
    quantity=0.01,
    buy_sell="BUY",      # 'BUY' or 'SELL'
    leverage=10,         # Leverage multiplier for futures
)

# Spot Market Order
order_id, msg = util.mrk_ordr(
    symbol="BTCUSDT",
    quantity=0.005,
    buy_sell="BUY",
    is_futures=False,
)
```

#### 2. Limit Order (`lim_ordr`)
Places a limit order at a specified limit price.
```python
order_id, msg = util.lim_ordr(
    symbol="B-BTC_USDT",
    quantity=0.01,
    buy_sell="BUY",
    price=59500.0,
    leverage=10,
    time_in_force="good_till_cancel"
)
```

#### 3. Stop-Loss Limit Order (`sl_ordr`)
Places a Stop-Loss Limit (`stop_limit`) order triggered when the market reaches `trig_price`.
```python
order_id, msg = util.sl_ordr(
    symbol="B-BTC_USDT",
    quantity=0.01,
    buy_sell="SELL",
    price=58900.0,       # Execution limit price
    trig_price=59000.0,  # Stop trigger price
    leverage=10,
)
```

#### 4. Stop-Loss Market Order (`slmkt_ordr`)
Places a Stop-Loss Market / Take-Profit trigger order.
```python
order_id, msg = util.slmkt_ordr(
    symbol="B-BTC_USDT",
    quantity=0.01,
    buy_sell="SELL",
    trig_price=58500.0,
    leverage=10,
)
```

#### 5. Order Cancellation & Flattening
```python
# Cancel a specific order by ID
util.cancel_ordr(order_id="12345678", is_futures=True)

# Cancel all open orders for a trading pair
util.cancel_all(symbol="B-BTC_USDT", is_futures=True)

# Flatten/exit an open position with an opposing market order
order_id, msg = util.exit_position(pair="B-BTC_USDT", is_futures=True)
```

---

### C. Market Data & Candlesticks

#### `candles(pair, interval='1m', limit=500)`
Fetches historical OHLCV candlestick data directly from public market data streams into a `pandas.DataFrame`.
```python
df = util.candles(pair="B-BTC_USDT", interval="5m", limit=100)
if df is not None:
    print(df[['datetime', 'open', 'high', 'low', 'close', 'volume']])
```

#### `order_book(pair)`
Fetches live Level-2 order book depth (bids & asks).
```python
book = util.order_book(pair="B-BTC_USDT")
print("Top Ask:", book.get("asks", [])[:1])
print("Top Bid:", book.get("bids", [])[:1])
```

#### `ticker(market=None)`
Fetches current LTP, 24h high/low, and volume.
```python
btc_ticker = util.ticker(market="BTCUSDT")
print(f"LTP: {btc_ticker.get('last_price')}")
```

#### `instruments_list(is_futures=True)`
Fetches active instruments specifications, contract multipliers, step sizes, and min order quantities.
```python
futures_instruments = util.instruments_list(is_futures=True)
```

#### `get_margin(orders_data)`
Calculates approximate margin requirement for orders.
```python
margins = util.get_margin([
    {"symbol": "B-BTC_USDT", "quantity": 0.1, "price": 60000.0, "leverage": 10},
])
print(f"Required Margin: ${margins[0]['initial_margin']}")
```
