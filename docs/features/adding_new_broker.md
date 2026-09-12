# Comprehensive Guide: Adding a New Broker to DeltaZero26

This guide provides a detailed, end-to-end walkthrough for integrating a new broker or cryptocurrency exchange into the **DeltaZero26** algorithmic trading platform.

---

## 1. High-Level Architecture Overview

DeltaZero26 enforces a clean separation of concerns across four decoupled modules:

```mermaid
flowchart TD
    subgraph UI_Admin [1. Django Admin & Models]
        BT[BrokerType Table] --> AdminPanel[Django Admin Account Form]
        AP[ApiProvider Table] --> AdminPanel
        AdminPanel --> BrokerDB[(kalai_broker Table)]
    end

    subgraph Auth [2. Authentication Adapters]
        BrokerDB --> AuthReg[BrokerAuthRegistry]
        AuthReg --> CustomAuth[CustomBrokerAuthAdapter]
        CustomAuth -->|OAuth / Direct Login| TokenStorage[access_token & timestamps]
    end

    subgraph Streaming [3. WebSocket Real-time Feed]
        BrokerDB --> FeedReg[BrokerRegistry Auto-Discovery]
        FeedReg --> CustomFeed[CustomBrokerFeed (BaseFeed)]
        CustomFeed -->|Normalize Ticks| TickQ[[Async Tick Queue]]
        TickQ --> DBFlush[(PostgreSQL Timescale / COPY)]
    end

    subgraph OrderMgmt [4. Order Management & REST Utility]
        BrokerDB --> CustomUtil[CustomBrokerUtility]
        CustomUtil -->|Live Balances & Positions| AlgoStrategy[Strategy / Polars Engine]
        CustomUtil -->|Execute Orders| BrokerREST[Broker REST APIs]
    end
```

---

## 2. Step 1: Database Registration & Migration

The Django Admin panel populates the **Broker Name** and **API Provider** dropdowns dynamically from two models in `kalai/models.py`: `BrokerType` and `ApiProvider`.

### Create a Data Migration
Create a new migration file in `kalai/migrations/` (e.g., `00XX_add_<broker>_broker_and_provider.py`):

```python
# kalai/migrations/00XX_add_mybroker_broker_and_provider.py
from django.db import migrations

def populate_broker_choices(apps, schema_editor):
    BrokerType = apps.get_model('kalai', 'BrokerType')
    ApiProvider = apps.get_model('kalai', 'ApiProvider')

    # Register Broker Type (the firm/platform name)
    BrokerType.objects.get_or_create(
        code='mybroker',
        defaults={'name': 'My Broker Platform'}
    )

    # Register API Provider (the data feed provider name)
    ApiProvider.objects.get_or_create(
        code='mybroker',
        defaults={'name': 'My Broker API'}
    )

def remove_broker_choices(apps, schema_editor):
    BrokerType = apps.get_model('kalai', 'BrokerType')
    ApiProvider = apps.get_model('kalai', 'ApiProvider')

    BrokerType.objects.filter(code='mybroker').delete()
    ApiProvider.objects.filter(code='mybroker').delete()

class Migration(migrations.Migration):
    dependencies = [
        ('kalai', '0014_add_coinswitch_broker_and_provider'), # Replace with last migration
    ]

    operations = [
        migrations.RunPython(populate_broker_choices, reverse_code=remove_broker_choices),
    ]
```

---

## 3. Step 2: Unified Account Credential Mapping & Django Admin Guidance

DeltaZero26 maintains a single, unified `Broker` account model across all brokers and exchanges. Rather than creating broker-specific database tables or adding dynamic columns for every platform, you map the new broker's credentials into the standard fields of the `Broker` model.

### 1. Unified Credential Mapping Matrix

| Standard Field (`Broker` Model) | Zerodha Kite | Kotak Neo | CoinDCX | CoinSwitch PRO | Tradovate / Future Brokers |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`account_id`** | User ID (e.g. `HS6525`) | Client ID / UCC (e.g. `W1NPY`) | Account Identifier / Email | Account Identifier | Username / Customer Number |
| **`api_key`** | Kite API Key | **Consumer / Customer Key** | API Key | PRO API Key | CID / App ID / Public Key |
| **`api_secret`** | Kite API Secret | 6-digit Neo MPIN (e.g. `123456`) | API Secret (HMAC) | Secret Key (Ed25519/HMAC) | App Secret / Master Password |
| **`totp_secret`** | 32-char TOTP Seed (2FA) | 32-char TOTP 2FA Secret Key | — *(Leave blank)* | — *(Leave blank)* | Master 2FA Seed / PIN |
| **`access_token`** | Daily OAuth Token | Bearer Session Token (`<token>:::<sid>`) | — *(Leave blank)* | — *(Leave blank)* | Bearer Access Token |
| **`refresh_token`** | — *(Leave blank)* | Registered Mobile Number (`+91...`) | — *(Leave blank)* | — *(Leave blank)* | Refresh Token (if required) |

> [!TIP]
> **Kotak Neo Specifics**: Kotak Neo uses **Trade API v2 Direct 2FA**. Map the Consumer Key into `api_key`, the 6-digit MPIN into `api_secret`, the 32-character TOTP secret into `totp_secret`, your registered mobile number into `refresh_token`, and your Client ID / UCC into `account_id`.

### 2. Updating Django Admin Guidance (`kalai/admin.py`)

Whenever adding a new broker, update the inline guidance in [`kalai/admin.py`](file:///c:/Users/Admin/Documents/deltazero26/kalai/admin.py) so administrators know exactly what to paste into each field:

1. **Update `AccountAdminForm` Placeholders & Help Text**:
   ```python
   # kalai/admin.py
   class AccountAdminForm(forms.ModelForm):
       class Meta:
           model = Broker
           fields = "__all__"
           widgets = {
               "account_id": forms.TextInput(attrs={"placeholder": "e.g. HS6525 / W1NPY (Kotak Neo) / mybroker_user"}),
               "api_key": forms.TextInput(attrs={"placeholder": "API Key / Consumer Key / CID"}),
               ...
           }
   ```
2. **Update `broker_credentials_guide` Table**:
   Add a row for your new broker to the HTML table inside `AccountAdmin.broker_credentials_guide()` in `kalai/admin.py`.

---

## 4. Step 3: Authentication Adapter (`kalai/auth/`)

The authentication layer handles token generation, OAuth redirects, or direct programmatic login (e.g., TOTP + PIN or API Key / Ed25519 validation).

### 1. Create `kalai/auth/<broker>.py`
Subclass `BaseBrokerAuth` (`kalai/auth/base.py`):

```python
# kalai/auth/mybroker.py
from __future__ import annotations
import logging
from typing import TYPE_CHECKING, Any
from .base import BaseBrokerAuth

if TYPE_CHECKING:
    from django.http import HttpRequest
    from kalai.models import Broker

logger = logging.getLogger("kalai.auth")

class MyBrokerAuthAdapter(BaseBrokerAuth):
    """
    Authentication adapter for MyBroker.
    """
    BROKER_CODES = ["mybroker", "mybroker_pro"]
    DISPLAY_NAME = "My Broker"
    
    # Case A: Browser-based OAuth 2.0 flow
    SUPPORTS_OAUTH = True
    SUPPORTS_DIRECT_LOGIN = False

    def get_login_url(self, broker: Broker, request: HttpRequest, callback_url: str | None = None) -> str:
        cb_url = callback_url or broker.get_callback_url(request)
        return f"https://api.mybroker.com/v1/oauth/authorize?client_id={broker.api_key}&redirect_uri={cb_url}"

    def handle_callback(self, broker: Broker, request: HttpRequest) -> tuple[bool, str]:
        auth_code = request.GET.get("code") or request.GET.get("auth_token")
        if not auth_code:
            return False, "Missing authorization code in callback URL."
        
        # Exchange code for access_token via REST POST
        # ...
        access_token = "GENERATED_SESSION_TOKEN"
        self.save_session_tokens(broker, access_token=access_token)
        return True, f"Successfully authenticated {broker.name}."

    # Case B: Programmatic / Direct API Key validation
    # SUPPORTS_OAUTH = False
    # SUPPORTS_DIRECT_LOGIN = True
    # def handle_direct_login(self, broker: Broker, **kwargs: Any) -> tuple[bool, str]:
    #     ...
```

### 2. Register in `kalai/auth/registry.py`
Import and register the adapter in `BrokerAuthRegistry`:

```python
# kalai/auth/registry.py
from .mybroker import MyBrokerAuthAdapter

class BrokerAuthRegistry:
    def _register_defaults(self) -> None:
        for cls in (
            ZerodhaAuthAdapter,
            KotakNeoAuthAdapter,
            UpstoxAuthAdapter,
            AngelOneAuthAdapter,
            CoinSwitchAuthAdapter,
            MyBrokerAuthAdapter, # <-- Add here
        ):
            self.register(cls)
```

---

## 5. Step 4: WebSocket Streaming Feed (`algo_trading/brokers/feeds/`)

WebSocket feeds run inside asynchronous background worker tasks managed by the WebSocket engine. The `BrokerRegistry` auto-discovers feed files inside `algo_trading/brokers/feeds/`.

### 1. Create `algo_trading/brokers/feeds/<broker>.py`
Subclass `BaseFeed` (`algo_trading/brokers/base.py`):

```python
# algo_trading/brokers/feeds/mybroker.py
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
import orjson
import aiohttp # or socketio or websockets

from algo_trading.brokers.base import BaseFeed

logger = logging.getLogger("algo_trading.brokers")

class MyBrokerFeed(BaseFeed):
    """WebSocket streaming feed for MyBroker."""

    # Unique identifier matching DB broker_name or api_provider code
    BROKER_NAME = "mybroker"
    ALIASES = ["mybroker_pro", "mybroker_feed"]

    SOCKET_URL = "wss://stream.mybroker.com/ws"
    DEFAULT_INSTRUMENT_TOKENS = ["BTC/USDT", "ETH/USDT"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._ws = None

    async def connect_and_stream(self) -> None:
        """
        Connect to WebSocket server, subscribe to tokens, and listen for messages.
        Reconnection and exponential backoff are automatically managed by BaseFeed.run().
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.SOCKET_URL, headers=headers) as ws:
                self._ws = ws
                self.log("INFO", "Connected to MyBroker WebSocket. Subscribing...")
                await self._subscribe()

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = orjson.loads(msg.data)
                        tick = self.normalize_tick(data)
                        if self._tick_queue and tick.get("last_price", 0) > 0:
                            await self._tick_queue.put(tick)
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        break

    def normalize_tick(self, raw_data: dict, event_type: str = "trade") -> dict[str, Any]:
        """
        Transform broker-specific tick frames into standard DeltaZero26 tick dictionary format.
        """
        symbol = raw_data.get("s") or raw_data.get("symbol", "")
        ltp = float(raw_data.get("p") or raw_data.get("price") or 0.0)
        volume = float(raw_data.get("v") or raw_data.get("volume") or 0.0)

        return {
            "instrument_token": symbol,
            "tradingsymbol": symbol,
            "last_price": ltp,
            "open": float(raw_data.get("o") or ltp),
            "high": float(raw_data.get("h") or ltp),
            "low": float(raw_data.get("l") or ltp),
            "close": ltp,
            "volume": volume,
            "buy_demand": float(raw_data.get("bid_qty") or 0.0),
            "sell_demand": float(raw_data.get("ask_qty") or 0.0),
            "depth": {"buy": [], "sell": []},
            "date_time": datetime.now(timezone.utc).isoformat(),
        }

    async def _subscribe(self) -> None:
        """Send subscription frames to socket."""
        tokens = self.instrument_tokens or self.DEFAULT_INSTRUMENT_TOKENS
        if self._ws and not self._ws.closed:
            payload = {"action": "subscribe", "symbols": tokens}
            await self._ws.send_str(orjson.dumps(payload).decode())

    async def on_tokens_changed(self, new_tokens: list) -> None:
        """Invoked dynamically when instrument tokens table updates in DB."""
        self.instrument_tokens = list(new_tokens)
        await self._subscribe()
```

---

## 6. Step 5: Order Management & Portfolio Utility (`algo_trading/algos/`)

The utility class provides high-level synchronous REST wrappers for algorithmic trading strategies, portfolio checks, positions, and order execution.

### 1. Create `algo_trading/algos/<broker>_utils.py`

```python
# algo_trading/algos/mybroker_utils.py
from __future__ import annotations
import hashlib
import hmac
import logging
import time
from typing import Any, Callable
import polars as pl
import requests

logger = logging.getLogger("algo_trading.algos.mybroker_utils")

def retry(func: Callable, max_retries: int = 3, delay: float = 1.0, backoff: float = 1.5) -> Any:
    """Retry helper with exponential backoff for network resilience."""
    last_exc = None
    curr_delay = delay
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exc = e
            time.sleep(curr_delay)
            curr_delay *= backoff
    if last_exc:
        raise last_exc

class MyBrokerUtility:
    """Order execution and account portfolio manager for MyBroker."""

    BASE_URL = "https://api.mybroker.com/v1"

    def __init__(
        self,
        account_id: str | None = None,
        broker_obj: Any = None,
        api_key: str | None = None,
        api_secret: str | None = None,
    ) -> None:
        self.session = requests.Session()

        # Multi-account database resolution
        if api_key and api_secret:
            self.broker = broker_obj
            self.account_id = account_id or "direct_mybroker"
            self.api_key = api_key.strip()
            self.api_secret = api_secret.strip()
            self.access_token = ""
        else:
            from django.db.models import Q
            from kalai.models import Broker

            if broker_obj is not None:
                self.broker = broker_obj
            elif account_id:
                self.broker = Broker.objects.filter(
                    Q(account_id__iexact=account_id) | Q(name__iexact=account_id)
                ).first()
            else:
                self.broker = Broker.objects.filter(
                    Q(broker_name__code="mybroker") | Q(name__icontains="mybroker")
                ).first()

            if not self.broker:
                raise ValueError(f"No Broker configuration found for account '{account_id or 'auto'}'.")

            self.account_id = self.broker.account_id or self.broker.name
            self.api_key = (self.broker.api_key or "").strip()
            self.api_secret = (self.broker.api_secret or "").strip()
            self.access_token = (self.broker.access_token or "").strip()

    # ─── Standard DeltaZero26 Methods ────────────────────────────

    def chk_live_bal(self) -> tuple[float, float]:
        """Returns (available_cash, total_net_capital)."""
        # Call GET /wallet/balance
        return 50000.0, 100000.0

    def holdings(self) -> pd.DataFrame | None:
        """Returns spot portfolio holdings as a DataFrame."""
        # Columns: currency, balance, locked_balance, total, tradingsymbol
        return pd.DataFrame()

    def pos_data(self, pair: str | None = None) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        """Returns (day_positions_df, net_positions_df)."""
        return pd.DataFrame(), pd.DataFrame()

    def mrk_ordr(self, symbol: str, quantity: float, buy_sell: str = "BUY", **kwargs) -> tuple[str | int, str]:
        """Place Market Order -> returns (order_id, status_message)."""
        try:
            # Send POST /order with type=market
            return "ORDER_12345", "Order placed successfully"
        except Exception as e:
            return -1, str(e)

    def lim_ordr(self, symbol: str, quantity: float, buy_sell: str = "BUY", price: float | None = None, **kwargs) -> tuple[str | int, str]:
        """Place Limit Order -> returns (order_id, status_message)."""
        if not price or price <= 0:
            return -1, "Price must be provided and > 0 for Limit Orders."
        try:
            return "ORDER_12346", "Order placed successfully"
        except Exception as e:
            return -1, str(e)

    def sl_ordr(self, symbol: str, quantity: float, buy_sell: str = "BUY", price: float | None = None, trig_price: float | None = None, **kwargs) -> tuple[str | int, str]:
        """Place Stop-Loss Limit Order."""
        return "ORDER_12347", "Stop order placed successfully"

    def cancel_ordr(self, order_id: str | None = None, **kwargs) -> tuple[bool, str]:
        """Cancel an open order."""
        return True, "Order cancelled"

    def cancel_all(self, symbol: str | None = None, **kwargs) -> tuple[bool, str]:
        """Cancel all open orders."""
        return True, "All orders cancelled"

    def candles(self, symbol: str, interval: str = "1m", limit: int = 100) -> pd.DataFrame | None:
        """Fetch historical candlestick bars into a pandas DataFrame."""
        return pd.DataFrame()
```

---

## 7. Step 6: Unified Strategy Engine & Account Container Integration

To enable your broker in the unified multi-account execution engines, plug your utility into the appropriate account factory:

### For Indian Brokers (`indian_opt_trde_polars.py` & `indian_user_account.py`)
1. In `algo_trading/algos/indian_opt_trde_polars.py`:
   - Update `create_indian_broker_utility()` to instantiate your utility when `broker.broker_name.code == "mybroker"`.
   - The engine automatically wraps your utility into an [`IndianUserAccount`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/indian_user_account.py) container.

### For Crypto Brokers (`crypto_opt_trde_polars.py` & `crypto_user_account.py`)
1. In `algo_trading/algos/crypto_opt_trde_polars.py`:
   - Update `create_crypto_broker_utility()` to instantiate your utility when `broker.broker_name.code == "mybroker"`.
   - The engine automatically wraps your utility into a [`CryptoUserAccount`](file:///c:/Users/Admin/Documents/deltazero26/algo_trading/algos/crypto_user_account.py) container.

### Required Utility API Surface for User Accounts
Both `IndianUserAccount` and `CryptoUserAccount` expect the underlying utility to implement 4 core inspection methods:
* `chk_live_bal() -> tuple[float, float]`: Returns `(avail_cash, live_balance)`.
* `holdings() -> pl.DataFrame | list | dict`: Returns account assets/holdings.
* `orders() -> pl.DataFrame | list | dict`: Returns recent/active orders.
* `pos_data() -> tuple[pl.DataFrame, pl.DataFrame]`: Returns `(day_positions, net_positions)`.

Both user account containers automatically provide:
- **Tiered TTL caching**: Prevents redundant REST queries during steady-state loops.
- **Isolated ThreadPool sync**: Per-endpoint `try...except` isolation prevents transient errors on one endpoint from crashing the cycle.
- **Optimistic balance tracking**: `mark_order_placed()` decrements balances immediately upon order placement to prevent over-allocation.

---

## 8. Step 7: Logging & Performance Rules

All algorithms and broker utility calls must strictly adhere to DeltaZero26 logging standards:

1. **ISO Timestamp Prefix**: Attach `[{datetime.now().isoformat()}]` to all log strings.
2. **In-Memory Buffering & Batch Flushing**: Buffer system logs in memory and write to `AlgoLog` via periodic batch limits (`LOG_BATCH_SIZE_LIMIT`) or time intervals (`LOG_FLUSH_INTERVAL`) with `flush_if_needed()`.
3. **Low Server Overhead**: Avoid immediate forced database writes (`force=True`) during routine parameter updates or tick logs.

---

## 9. Step 8: Unit Testing (`tests/`)

Create comprehensive unit tests covering:
1. `tests/test_<broker>_utils.py`:
   - Direct API key & DB lookup initialization
   - Cryptographic signing / headers
   - Market, Limit, and Stop-Loss orders (mocked)
   - Balance checks and DataFrame outputs
   - Error handling and `retry()` helper
2. `tests/test_<broker>_feed.py`:
   - Broker registry auto-discovery (`assert "mybroker" in reg.registered_brokers`)
   - Tick normalization (`normalize_tick`)
   - Dynamic token changes (`on_tokens_changed`)

Run tests via:
```bash
.venv\Scripts\pytest.exe tests/test_mybroker_utils.py tests/test_mybroker_feed.py -v
```

---

## 10. Step 9: Verification & Docker Deployment

After creating your broker components:

1. **Check Compilation**:
   ```bash
   .venv\Scripts\python.exe -m py_compile algo_trading/algos/mybroker_utils.py algo_trading/brokers/feeds/mybroker.py kalai/auth/mybroker.py
   ```

2. **Run Full Test Suite**:
   ```bash
   .venv\Scripts\pytest.exe -q
   ```

3. **Deploy & Apply Migrations in Docker**:
   ```bash
   docker compose down
   docker compose up --build -d
   ```

4. **Verify in Django Admin**:
   - Open Django Admin at `https://<YOUR_DOMAIN>/<ADMIN_PATH>/`.
   - Go to **Accounts** -> **Add Account**.
   - Verify that your new broker appears in the **Broker Name** and **API Provider** dropdowns.
   - Confirm that the credential mapping instructions guide displays clearly on the form.
