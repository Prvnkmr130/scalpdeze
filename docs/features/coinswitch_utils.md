# CoinSwitch Order Management & Portfolio Utility (`CoinSwitchUtility`)

The `CoinSwitchUtility` class (`algo_trading/algos/coinswitch_utils.py`) provides a high-level, production-grade wrapper around CoinSwitch PRO's authenticated REST and public Market Data APIs. It abstracts credential loading, Ed25519 cryptographic request signing, network retries, account margin checks, spot holdings, futures positions, candlestick retrieval, and multi-variety order execution for algorithmic trading in DeltaZero26.

---

## 1. Overview & Initialization

`CoinSwitchUtility` automatically fetches credentials (`api_key` and `api_secret` Ed25519 private key) from the `Broker` database table. It supports multi-account trading by resolving accounts dynamically via `account_id` or direct parameter initialization.

### Initialization Examples

```python
from algo_trading.algos.coinswitch_utils import CoinSwitchUtility

# 1. Initialize for a specific account (Recommended for multi-account)
util = CoinSwitchUtility(account_id="coinswitch_main")

# 2. Or auto-load the primary active CoinSwitch account from database
util = CoinSwitchUtility()

# 3. Direct API key initialization (useful for isolated scripts/testing)
util = CoinSwitchUtility(
    api_key="YOUR_COINSWITCH_API_KEY_HEX",
    api_secret="YOUR_COINSWITCH_ED25519_PRIVATE_KEY_HEX",
    account_id="custom_coinswitch"
)

# 4. Inject a pre-configured or mock client
util = CoinSwitchUtility(client=mock_client)
```

> [!NOTE]
> **Resilient Discovery & Polars Native**: If credentials are not configured, `CoinSwitchUtility` falls back gracefully to public mode. Public endpoints (`instruments_list()`, `get_ticker()`, `get_candles()`) operate without crashing. All tabular data is processed in native Polars (`pl.DataFrame`). Methods like `exit_position()` strictly use `.is_empty()` and bracket indexing to prevent runtime `AttributeError` exceptions.

---

## 2. API Method Reference

### A. Account Balances & Portfolio Inspection

#### `chk_live_bal()`
Fetches live available cash (USDT/INR) and total net portfolio capital across Spot wallets and DMA Options & Perpetual Futures Unified Wallets (`v5/account/wallet-balance?accountType=UNIFIED`).
* **Returns**: `tuple[float, float]` $\rightarrow$ `(avail_cash, net_capital)`
* **Example**:
  ```python
  cash, cap = util.chk_live_bal()
  print(f"Available Cash: {cash:,.2f} | Net Capital: {cap:,.2f}")
  ```

#### `get_futures_wallet_balance()`
Queries CoinSwitch DMA unified wallet balance endpoint (`https://dma.coinswitch.co/v5/account/wallet-balance?accountType=UNIFIED`) returning `totalAvailableBalance`, `totalMarginBalance`, `totalWalletBalance`, and `totalEquity`.

#### `holdings()`
Fetches non-zero spot wallet crypto holdings with available, locked, and total balances.
* **Returns**: `pl.DataFrame | None`
* **Columns**: `currency`, `balance`, `locked_balance`, `total`, `tradingsymbol`
* **Example**:
  ```python
  df = util.holdings()
  if df is not None and not df.is_empty():
      print(df.select(['tradingsymbol', 'balance', 'locked_balance', 'total']))
  ```

#### `pos_data(pair=None)`
Fetches current open Perpetual Futures derivatives positions.
* **Parameters**: `pair: str | None` (e.g. `'BTC/USDT'` or `'ETH/USDT'`)
* **Returns**: `tuple[pl.DataFrame | None, pl.DataFrame | None]` $\rightarrow$ `(day_positions_df, net_positions_df)`
* **Columns**: `symbol`, `active_pos`, `avg_price`, `mark_price`, `liquidation_price`, `locked_margin`, `unrealized_pnl`, `realized_pnl`, `leverage`
* **Example**:
  ```python
  day_df, net_df = util.pos_data(pair="BTC/USDT")
  if net_df is not None and not net_df.empty:
      print("Active Positions:", len(net_df))
  ```

#### `orders(is_futures=False, symbol=None, exchange="coinswitchx", open_only=True)`
Fetches open orders or recent orders for the account.
* **Returns**: `list[dict]`
* **Example**:
  ```python
  active_orders = util.orders(is_futures=False, symbol="BTC/INR")
  ```

#### `order_history(order_id=None, client_order_id=None, is_futures=False)`
Fetches execution details and status of a specific order.
* **Returns**: `dict | None`

#### `order_trades(symbol=None, exchange="coinswitchx")`
Fetches user trade execution history.
* **Returns**: `list[dict]`

---

### B. Order Execution Methods

All order placement methods return `tuple[order_id, order_msg]`. If placement fails, `order_id` is `-1` and `order_msg` contains the error description.

#### 1. Market Order (`mrk_ordr`)
Places an instantaneous market order executed at current market price. Supports both Spot and Perpetual Futures.
```python
# Spot Market Order
order_id, msg = util.mrk_ordr(
    symbol="BTC/INR",
    quantity=0.001,
    buy_sell="BUY",
    exchange="coinswitchx"
)

# Futures Market Order
order_id, msg = util.mrk_ordr(
    symbol="BTC/USDT",
    quantity=0.01,
    buy_sell="BUY",
    is_futures=True,
    leverage=10
)
```

#### 2. Limit Order (`lim_ordr`)
Places a limit order at a specified limit price.
```python
order_id, msg = util.lim_ordr(
    symbol="BTC/USDT",
    quantity=0.01,
    buy_sell="BUY",
    price=60000.0,
    exchange="coinswitchx",
    is_futures=False,
    time_in_force="GTC"
)
```

#### 3. Options Order (`options_order`)
Places European options orders directly to the CoinSwitch DMA Options gateway (`https://dma.coinswitch.co/v5/order/create`).
```python
order_id, msg = util.options_order(
    symbol="BTC-04SEP26-75500-CE",
    quantity=0.1,
    buy_sell="BUY",
    order_type="LIMIT",
    price=1250.0
)
```

#### 4. Stop-Loss Limit Order (`sl_ordr`)
Places a Stop-Loss Limit (`stop_limit`) order triggered when the market reaches `trig_price`.
```python
order_id, msg = util.sl_ordr(
    symbol="BTC/USDT",
    quantity=0.01,
    buy_sell="SELL",
    price=58900.0,       # Execution limit price
    trig_price=59000.0,  # Stop trigger price
    is_futures=True,
    leverage=10
)
```

#### 5. Stop-Loss Market Order (`slmkt_ordr`)
Places a Stop-Loss Market (`stop_market`) trigger order.
```python
order_id, msg = util.slmkt_ordr(
    symbol="BTC/USDT",
    quantity=0.01,
    buy_sell="SELL",
    trig_price=58500.0,
    is_futures=True,
    leverage=10
)
```

#### 6. Order Cancellation & Flattening
```python
# Cancel a specific order by ID or client_order_id
util.cancel_ordr(order_id="12345678", exchange="coinswitchx")

# Cancel all open orders for a trading pair
util.cancel_all(symbol="BTC/USDT", exchange="coinswitchx")

# Flatten/exit an open futures position with an opposing market order
order_id, msg = util.exit_position(symbol="BTC/USDT", is_futures=True)
```

---

### C. Market Data & Candlesticks

#### `candles(symbol, interval='1m', limit=100, exchange='coinswitchx')`
Fetches historical OHLCV candlestick data directly into a `pandas.DataFrame`.
```python
df = util.candles(symbol="BTC/USDT", interval="5m", limit=100)
if df is not None:
    print(df[['datetime', 'open', 'high', 'low', 'close', 'volume']])
```

#### `order_book(symbol, exchange='coinswitchx')`
Fetches live Level-2 order book depth (bids & asks).
```python
book = util.order_book(symbol="BTC/USDT")
print("Top Ask:", book.get("asks", [])[:1])
print("Top Bid:", book.get("bids", [])[:1])
```

#### `ticker(symbol=None, exchange='coinswitchx')`
Fetches 24hr price ticker and volume statistics.
```python
btc_ticker = util.ticker(symbol="BTC/USDT")
```

#### `instruments_list(exchange='coinswitchx', is_futures=False)`
Fetches active instruments specifications, trade precision, and contract multipliers.

#### `get_margin(symbol, quantity, price=None, leverage=1)`
Calculates approximate margin requirement for orders.
```python
margins = util.get_margin(symbol="BTC/USDT", quantity=0.1, price=60000.0, leverage=10)
print(f"Required Margin: ${margins['initial_margin']}")
```

---

## 3. WebSocket Real-time Feed (`CoinSwitchFeed`)

The `CoinSwitchFeed` class (`algo_trading/brokers/feeds/coinswitch.py`) connects to CoinSwitch's real-time Socket.IO v4 streaming engine:

* **Namespaces Supported**: `/coinswitchx`, `/c2c1`, `/c2c2`.
* **Events Subscribed**: `orderbook`, `depth-update`, `trades`, `new-trade`, `candlestick`, `balance-update`, `order-update`.
* **Token Format in DB**: Standard symbol pairs with commas or slashes, e.g. `["BTC,INR", "BTC,USDT", "ETH,USDT"]`.
* **Tick Normalization**: Automatically converts all incoming payloads into DeltaZero26 standard tick frames:
  ```python
  {
      "instrument_token": "BTC/INR",
      "tradingsymbol": "BTC/INR",
      "last_price": 5850000.0,
      "open": 5800000.0,
      "high": 5900000.0,
      "low": 5750000.0,
      "close": 5850000.0,
      "volume": 12.45,
      "buy_demand": 1.25,
      "sell_demand": 0.85,
      "depth": {"buy": [...], "sell": [...]},
      "date_time": "2026-08-26T19:30:00+00:00"
  }
  ```

---

## 4. Master Instrument Token Assembly & Config-Aware Crash Recovery

`CoinSwitchUtility.master_tkn_list(input_file, ...)` manages instrument discovery, contract normalization, and active subscription tables:
- **Broker-Aware Underlying Resolution**: Resolves reference underlying symbols by checking candidates `[f"{c_root}/USDT", f"{c_root}USDT"]`, mapping CoinSwitch perpetual futures to genuine contract tokens (e.g. `91107767` for `BTCUSDT`).
- **`CanonicalInstrumentKey` Registration**: Every derivative and perpetual contract is mapped and registered into [`BrokerTokenMapper`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/broker_token_mapper.py) with canonical option/future keys.
- **Config-Aware Cache Invalidation**: When evaluating cached `cum_table` entries in `ExchangeMasterData`, checks active symbols where `Capital_share > 0` against the current configuration. If symbols change, the cache is invalidated and fresh contracts are assembled across exchanges (`coinswitchx`, `c2c1`, `c2c2`, `FUTURES`, `OPTIONS`). Current `Capital_share` weights are dynamically synchronized into restored tables.
- **Strict Active Subscription Filtering**: `init_ref_list` and `index_ref_list` filter strictly for `(Capital_share > 0) & (instrument_token > 0)`, ensuring zero inactive contracts are subscribed.
- **`week_dist` Integer Calendar Week Parity**: Upon restoring cached `cum_table` from `ExchangeMasterData`, `week_dist` is explicitly cast to `Int64` to maintain strict calendar week integer offset parity with `crypto_master_tokens.py` and `indian_master_tokens.py`.


