# Multi-Broker Positions & Historical P&L Analytics

DeltaZero26 features an integrated, cross-broker position monitoring and historical profit/loss (P&L) analytics engine. It provides real-time visibility into open/closed intraday and overnight holdings, long-term trade audit tracking (1+ year retention), and instant on-demand performance metrics across any desired time horizon.

---

## 1. Architectural Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    PRODUCTION TRADING                       │
│  [zerodha/kotak/coinswitch_opt_trde_polars.py]              │
│         │                                                   │
│         ▼ (in-memory Polars self.account.pos_net_frame)     │
│  sync_positions_from_algo_frames()                          │
│  • 1-minute throttle (min_interval_seconds=60)              │
│  • MD5 payload hash deduplication (0 redundant writes)      │
│  • force=True override on order fills / square-offs         │
└──────────────────────────────┬──────────────────────────────┘
                               │ (Direct In-Memory DB Write - 0 API Calls)
                               ▼
            ┌────────────────────────────────────┐
            │        PostgreSQL Database         │
            │  • kalai_brokerposition            │
            │  • kalai_traderecord (1+ yr)       │
            │  • kalai_dailypnlsnapshot          │
            └──────────────────┬─────────────────┘
                               │ (Sub-2ms On-Demand ORM Queries - 0 API Calls)
                               ▼
            ┌────────────────────────────────────┐
            │        Django Admin Panel          │
            │     /admin/kalai/positions-pnl/    │
            │  • Live Intraday KPI Summary Cards │
            │  • 7D / 30D / 90D / YTD / 1-Year   │
            │  • Custom Date Range Inference     │
            │  • Cumulative Progression Timeline │
            └────────────────────────────────────┘
```

---

## 2. Core Operational Principles

### A. Zero Outbound API Calls in Production
- **Problem**: Periodic background cron workers making direct REST API calls to brokers (`ZerodhaUtility.pos_data()`, etc.) can collide with running trading sessions, trigger rate limits, and cause unnecessary server/network overhead.
- **Solution**: In production (`DEBUG=False`), **no external REST API calls are made**. The active strategy engines ([`zerodha_opt_trde_polars.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/zerodha_opt_trde_polars.py), [`kotak_opt_trde_polars.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/kotak_opt_trde_polars.py), [`coinswitch_opt_trde_polars.py`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/coinswitch_opt_trde_polars.py)) already maintain `pos_net_frame` in memory. During routine state sync, they write directly into the database.
- **Debug Mode**: In local development (`DEBUG=True`) or explicit testing (`force_api=True`), the system can query the broker REST APIs directly on demand.

### B. 1-Minute Throttling & In-Memory Deduplication
- High-frequency algorithm loops (running every 2–5 seconds) do not flood PostgreSQL with position writes.
- `sync_positions_from_algo_frames` tracks monotonic timestamps per account and enforces a **minimum 60-second interval** (`min_interval_seconds=60.0`).
- An MD5 hash of the active positions (`_LAST_POSITION_HASH`) skips SQL updates if position quantities and P&L metrics have not changed.
- Critical trade lifecycle events (e.g. order placement, stop-loss trigger, or position closure) pass `force=True` for instant updates.

---

## 3. Data Models ([`kalai/models.py`](file:///c:/Users/Admin/Documents/deltazero26/kalai/models.py))

### 1. `BrokerPosition`
Tracks current live and squared-off intraday positions across all brokers.
- **Key Fields**: `account`, `tradingsymbol`, `product`, `quantity`, `buy_quantity`, `buy_price`, `buy_value`, `sell_quantity`, `sell_price`, `sell_value`, `last_price`, `unrealized_pnl`, `realized_pnl`, `total_pnl`, `is_open`, `raw_data`, `updated_at`.
- **Constraint**: `unique_together = [('account', 'tradingsymbol', 'product')]`.

### 2. `TradeRecord`
Granular trade audit ledger retained for **at least 1 year** for quantitative analysis.
- **Key Fields**: `account`, `order_id`, `tradingsymbol`, `product`, `action_type` (`BUY`/`SELL`), `quantity`, `price`, `value`, `realized_pnl`, `brokerage`, `taxes`, `executed_at`.
- **Retention**: Exempt from short-term log purge tasks.

### 3. `DailyPnLSnapshot`
Daily summary table enabling sub-2ms historical analytics.
- **Key Fields**: `account`, `date`, `realized_pnl`, `unrealized_pnl`, `net_pnl`, `total_trades`, `winning_trades`, `losing_trades`, `turnover`, `notes`.
- **Constraint**: `unique_together = [('account', 'date')]`.

### 4. Comparison of the 3 Data Tiers

| Tier / Model | Table Name | Purpose | Retention | How It Is Populated |
| :--- | :--- | :--- | :--- | :--- |
| **`BrokerPosition`** | `kalai_brokerposition` | Real-time open & closed positions, M2M, and live intraday valuation. | Current trading day | In-memory sync from running strategy engines (`sync_positions_from_algo_frames`) or broker API. |
| **`DailyPnLSnapshot`** | `kalai_daily_pnl_snapshot` | Aggregated daily multi-account P&L, trade counts, and total turnover. | 1+ years | Nightly automated background task (`create_daily_pnl_snapshots` at 12:05 AM IST) or manual sync. |
| **`TradeRecord`** | `kalai_traderecord` | Granular order-by-order execution fills (order ID, price, quantity, realized P&L). | 1+ years | Generated upon live market order execution fills or cloned via Data Hub. |

---

## 4. Multi-Broker Normalization Engine ([`kalai/positions.py`](file:///c:/Users/Admin/Documents/deltazero26/kalai/positions.py))

Normalizes payloads across all supported brokers into a uniform internal schema:

| Broker | Underlying Utility / Payload Source | Normalized Columns |
| :--- | :--- | :--- |
| **Zerodha** | `ZerodhaUtility.pos_data()` / KiteConnect `net` positions | `tradingsymbol`, `quantity`, `buy_price`, `sell_price`, `unrealised`, `realised`, `pnl` |
| **Kotak Neo** | `KotakNeoUtility.pos_data()` / Neo REST `/positions` | `trdSym`, `tok`, `flBuyQty`, `flSellQty`, `buyAvgPrice`, `sellAvgPrice`, `urPnl`, `rPnl` |
| **CoinSwitch PRO** | `CoinSwitchUtility.pos_data()` / Perpetual Futures Positions | `pair`, `active_pos`, `avg_price`, `mark_price`, `unrealized_pnl`, `realized_pnl` |
| **CoinDCX** | `CoinDCXUtility.pos_data()` / Perpetual Contracts | `pair`, `active_pos`, `avg_price`, `mark_price`, `unrealized_pnl`, `realized_pnl` |

---

## 5. On-Demand Time-Period P&L Analytics

Accessible via `compute_pnl_analytics(account_id, period_code, start_date, end_date)`. Queries `DailyPnLSnapshot` and returns:

- **Net P&L, Realized P&L, Live Unrealized P&L**
- **Win Rate (%)**: `winning_days / total_trading_days * 100`
- **Profit Factor**: `gross_profit / abs(gross_loss)`
- **Best Day & Worst Day P&L**
- **Total Trades & Gross Turnover**
- **Daily Performance Timeline**: Chronological progression with running cumulative P&L.

### Supported Presets:
1. `Today`
2. `Yesterday`
3. `This Week`
4. `Last 7 Days`
5. `This Month`
6. `Last 30 Days`
7. `Last 90 Days`
8. `Year to Date (YTD)`
9. `Past 1 Year (365 Days)`
10. `Custom Date Range` (`start_date` to `end_date`)

---

## 6. Django Admin Dashboard

- **URL**: `/admin/kalai/positions-pnl/`
- **Navigation Placement**: Positioned under **`⚡ Algo Monitoring` &rarr; `📊 Positions & P&L`** in both the main index and the left collapsible sidebar.
- **Features**:
  - **Filter Bar**: Account dropdown and Time-Period preset selector with a manual **"📊 Generate Report"** trigger.
  - **Executive KPI Cards**: Color-coded cards showing Total Net P&L, Win Rate %, Profit Factor, and Trading Days.
  - **Live Positions Table**: Real-time intraday positions with status badges (`OPEN` / `CLOSED`), LTP, and M2M P&L.
  - **Performance Progression Table**: Day-by-day table of Realized, Unrealized, Net P&L, and Cumulative P&L.

---

## 7. Numerical Formatting & Defensive Standards

### A. Template Currency & Number Formatting
- **Standard**: All numerical P&L, turnover, prices, and totals rendered in the positions dashboard utilize Django's built-in `humanize` library with `floatformat` and `intcomma`:
  ```django
  {% load humanize %}
  {% if val > 0 %}+{% endif %}{{ val|floatformat:2|intcomma }}
  ```
- **Avoid Legacy `%` printf String Formatting**: Never use `|stringformat:"+,.2f"` or `|stringformat:",.2f"` in templates. Python's classic `%` formatting does not support `,` (thousands separator) and causes Django to silently return an empty string (`""`), resulting in blank values across dashboards.
- **Dependency**: `"django.contrib.humanize"` must be included in `INSTALLED_APPS` in `algo_trading/settings.py`.

### B. Defensive Null Coalescing in Aggregations
- `compute_pnl_analytics()` implements explicit null coalescing (`s.realized_pnl or Decimal(0)`, `p.unrealized_pnl or Decimal(0)`) across all snapshot sums and live position iterations to prevent `TypeError` when aggregating empty or incomplete records.
- Account relationship traversal safely checks foreign key availability (`(s.account.account_id or s.account.name) if s.account else "Account"`).

