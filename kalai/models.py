import logging
from typing import Any, Optional, Union
from django.utils import timezone
from dotenv import load_dotenv
from django.db import models
import warnings
import json
from datetime import time
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from django.db import connection

logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

load_dotenv()

class BrokerType(models.Model):
    code = models.CharField(max_length=50, primary_key=True, help_text="e.g. 'zerodha', 'angel'")
    name = models.CharField(max_length=100, help_text="e.g. 'Zerodha', 'Angel Broking'")
    
    def __str__(self):
        return self.name

class ApiProvider(models.Model):
    code = models.CharField(max_length=50, primary_key=True, help_text="e.g. 'zerodha', 'angel'")
    name = models.CharField(max_length=100, help_text="e.g. 'Zerodha Kite API', 'Angel Broking API'")

    def __str__(self):
        return self.name

class Broker(models.Model):
    class Meta:
        db_table = 'kalai_broker'
        verbose_name = 'Account'
        verbose_name_plural = 'Accounts'

    account_id = models.CharField(max_length=100, unique=True, blank=True, null=True, help_text="Unique Account ID")
    name = models.CharField(max_length=100, unique=True, help_text="Account name or identifier")
    broker_name = models.ForeignKey(BrokerType, on_delete=models.PROTECT, default='zerodha', help_text="Brokerage firm name", db_column="broker_name")
    api_provider = models.ForeignKey(
        ApiProvider,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        help_text="Select API feed provider. If left unselected, trading and websocket connections will be disabled.",
        db_column="api_provider"
    )
    api_key = models.TextField(blank=True, null=True)
    api_secret = models.TextField(blank=True, null=True)
    api_secret_updated_at = models.DateTimeField(blank=True, null=True, help_text="Timestamp of when the API Secret was last created or updated")
    enable_secret_key_expiry = models.BooleanField(default=False, help_text="Enable secret key validity tracking and expiry alerts for this account (e.g. CoinSwitch 90-day expiry)")
    secret_key_validity_days = models.PositiveIntegerField(default=90, help_text="Secret key validity duration in days (default: 90 for CoinSwitch)")
    secret_key_warn_days = models.PositiveIntegerField(default=7, help_text="Days before expiry to trigger warning notifications (default: 7 days)")
    access_token = models.TextField(blank=True, null=True)
    access_token_updated_at = models.DateTimeField(blank=True, null=True, help_text="Timestamp of when the access token was last updated")
    refresh_token = models.TextField(blank=True, null=True)
    redirect_url = models.TextField(blank=True, null=True, help_text="Redirect URL for the broker's API authentication flow")
    base_redirect_url = models.TextField(blank=True, null=True, help_text="Base URL used for OAuth redirect in the API authentication")
    api_endpoint = models.TextField(blank=True, null=True, help_text="API endpoint URL for the broker")
    enable_trade = models.BooleanField(default=False, help_text="Enable or disable trading functionality for this account")
    enable_websocket = models.BooleanField(default=False, help_text="Enable or disable websocket connection")
    OPERATING_DAYS_CHOICES = [
        ('WEEKDAYS', 'Weekdays (Mon-Fri)'),
        ('ALL', 'All Days'),
    ]
    enable_schedule = models.BooleanField(default=True, help_text="Enable time-based automatic start/stop schedule for WebSocket")
    ws_start_time = models.TimeField(default=time(9, 0, 0), blank=True, null=True, help_text="Daily start time for WebSocket/Algo (default: 09:00:00)")
    ws_stop_time = models.TimeField(default=time(23, 55, 0), blank=True, null=True, help_text="Daily stop time for WebSocket/Algo (default: 23:55:00)")
    ws_operating_days = models.CharField(max_length=50, choices=OPERATING_DAYS_CHOICES, default="ALL", help_text="Select operating days for WebSocket schedule")
    access_token_required = models.BooleanField(default=False, help_text="Require a daily access token to establish websocket connection")
    totp_secret = models.CharField(max_length=255, blank=True, null=True, help_text="TOTP secret key for generating active OTPs")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        acc = self.account_id or self.name
        provider = f" ({self.api_provider.name})" if self.api_provider else " (No API Provider)"
        b_name = self.broker_name.name if self.broker_name else ""
        return f"{acc} - {b_name}{provider}"

    def get_callback_url(self, request=None) -> str:
        """Compute the deterministic callback URL for this account."""
        import os
        acc = self.account_id or self.name or ""
        path = f"/broker-admin/callback/{acc}/"
        if request:
            return request.build_absolute_uri(path)

        if self.redirect_url and self.redirect_url.strip():
            return self.redirect_url.strip()
        if self.base_redirect_url and self.base_redirect_url.strip():
            return f"{self.base_redirect_url.rstrip('/')}{path}"
        
        domain = os.getenv("DOMAIN_NAME")
        if domain and domain.strip() and domain.strip() not in ("localhost", "127.0.0.1"):
            return f"https://{domain.strip()}{path}"

        return f"http://127.0.0.1{path}"

    def auto_populate_redirect_url(self, request=None) -> None:
        """Auto-populate base_redirect_url and redirect_url if not explicitly set."""
        import os
        acc = self.account_id or self.name
        if not acc:
            return
        
        domain = os.getenv("DOMAIN_NAME")
        is_cloud_domain = domain and domain.strip() and domain.strip() not in ("localhost", "127.0.0.1")

        if not self.base_redirect_url or not self.base_redirect_url.strip():
            if request:
                self.base_redirect_url = request.build_absolute_uri("/")[:-1]
            elif is_cloud_domain:
                self.base_redirect_url = f"https://{domain.strip()}"
            else:
                self.base_redirect_url = "http://127.0.0.1"

        if not self.redirect_url or not self.redirect_url.strip():
            self.redirect_url = f"{self.base_redirect_url.rstrip('/')}/broker-admin/callback/{acc}/"

    @property
    def token_tablename(self) -> str:
        """Deterministic table name for this account's token subscriptions."""
        acc = (self.account_id or self.name or "").strip()
        return f"{acc}_inst_tokens"

    def get_subscribed_tokens(self) -> list:
        """
        Fetch subscribed instrument tokens strictly for this account instance.
        Returns list of tokens (e.g. [256265, 408065]).
        """
        import orjson
        from .models import AlgoInfo

        tablename = self.token_tablename
        try:
            record = AlgoInfo.objects.filter(account=self, tablename=tablename).first()
            if record and record.tabledata:
                data = record.tabledata
                if isinstance(data, dict):
                    return data.get("tokenid", [])
                elif isinstance(data, str):
                    parsed = orjson.loads(data)
                    return parsed.get("tokenid", []) if isinstance(parsed, dict) else []
                elif isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    def set_subscribed_tokens(self, tokens: list):
        """
        Atomically update or create the subscribed token list for THIS account only.
        Guarantees zero cross-account contamination.
        """
        from .models import AlgoInfo

        if not isinstance(tokens, list):
            tokens = list(tokens)

        tablename = self.token_tablename
        return AlgoInfo.create_or_update(
            account=self,
            tablename=tablename,
            tabledata={"tokenid": tokens}
        )

    def get_algo_state(self, tablename: str) -> Any:
        """Fetch arbitrary algorithmic state strictly scoped to this account."""
        from .models import AlgoInfo
        try:
            record = AlgoInfo.objects.filter(account=self, tablename=tablename).first()
            if not record or not record.tabledata:
                return {}
            data = record.tabledata
            if isinstance(data, str):
                try:
                    import orjson
                    return orjson.loads(data)
                except Exception:
                    return data
            return data
        except Exception:
            return {}

    def set_algo_state(self, tablename: str, data: dict):
        """Atomically persist arbitrary algorithmic state strictly scoped to this account."""
        from .models import AlgoInfo
        return AlgoInfo.create_or_update(
            account=self,
            tablename=tablename,
            tabledata=data
        )

    def get_master_data(self, master_name: str = "cum_table") -> Any:
        """Fetch daily static exchange master data (cum_table, aug_table, etc.) strictly scoped to this account."""
        from .models import ExchangeMasterData
        try:
            return ExchangeMasterData.get_master(account=self, master_name=master_name)
        except Exception:
            return None

    def set_master_data(self, master_name: str = "cum_table", data: Any = None, row_count: Optional[int] = None):
        """Atomically persist daily static exchange master data (cum_table, aug_table, etc.) strictly scoped to this account."""
        from .models import ExchangeMasterData
        return ExchangeMasterData.save_master(account=self, master_name=master_name, data=data, row_count=row_count)

    @classmethod
    def resolve(cls, identifier: Union[str, 'Broker']):
        """
        Safely resolve a Broker instance by account_id or name with related models eagerly loaded.
        Accepts either a string or an existing Broker instance.
        """
        if isinstance(identifier, cls):
            return identifier
        
        from django.db.models import Q
        ident = str(identifier).strip()
        return (
            cls.objects.select_related("broker_name", "api_provider")
            .filter(Q(account_id__iexact=ident) | Q(name__iexact=ident))
            .first()
        )

    def get_totp(self) -> str:
        """
        Generate the current valid 6-digit TOTP for this broker account,
        with automatic network clock drift synchronization and Base32 secret sanitization.
        """
        from kalai.auth.totp import get_totp_for_broker
        return get_totp_for_broker(self)

    @property
    def is_coinswitch(self) -> bool:
        """Check if this broker account is CoinSwitch PRO."""
        b_code = self.broker_name.code.lower() if self.broker_name else ""
        p_code = self.api_provider.code.lower() if self.api_provider else ""
        return "coinswitch" in b_code or "coinswitch" in p_code

    @property
    def is_crypto(self) -> bool:
        """Check if this broker account is a cryptocurrency broker (e.g. Delta Exchange, CoinSwitch, CoinDCX)."""
        b_code = (self.broker_name.code.lower() if self.broker_name else "")
        b_name = (self.broker_name.name.lower() if self.broker_name else "")
        p_code = (self.api_provider.code.lower() if self.api_provider else "")
        p_name = (self.api_provider.name.lower() if self.api_provider else "")
        acc_name = (self.name.lower() if self.name else "")
        acc_id = (self.account_id.lower() if self.account_id else "")
        return any(
            k in b_code or k in b_name or k in p_code or k in p_name or k in acc_name or k in acc_id
            for k in ("delta", "coinswitch", "coindcx", "crypto", "bitcoin")
        )

    @property
    def secret_key_added_at(self):
        """Return the effective date when the secret key was set or updated."""
        return self.api_secret_updated_at or self.created_at

    @property
    def secret_key_expires_at(self):
        """Return the expiry datetime for this broker's secret key if tracking is enabled."""
        if not self.enable_secret_key_expiry or not self.api_secret or not self.secret_key_added_at:
            return None
        from datetime import timedelta
        validity = self.secret_key_validity_days or 90
        return self.secret_key_added_at + timedelta(days=validity)

    @property
    def days_until_secret_key_expiry(self) -> int | None:
        """Calculate remaining days before secret key expires."""
        if not self.enable_secret_key_expiry:
            return None
        expires_at = self.secret_key_expires_at
        if not expires_at:
            return None
        now = timezone.now()
        diff = expires_at - now
        return int(diff.total_seconds() // 86400)

    @property
    def secret_key_expiry_status(self) -> str:
        """
        Return the expiry classification:
        - 'NOT_APPLICABLE': Expiry tracking disabled for this account
        - 'NOT_SET': No API Secret present
        - 'EXPIRED': Days remaining <= 0
        - 'EXPIRING_SOON': 0 < Days remaining <= secret_key_warn_days (default 7)
        - 'ACTIVE': Days remaining > secret_key_warn_days
        """
        if not self.enable_secret_key_expiry:
            return "NOT_APPLICABLE"
        if not self.api_secret:
            return "NOT_SET"
        days_left = self.days_until_secret_key_expiry
        if days_left is None:
            return "ACTIVE"
        if days_left <= 0:
            return "EXPIRED"
        warn_threshold = self.secret_key_warn_days or 7
        if days_left <= warn_threshold:
            return "EXPIRING_SOON"
        return "ACTIVE"

    @property
    def is_secret_key_expiring_soon(self) -> bool:
        """Returns True if the account secret key is expired or expiring within warning threshold."""
        return self.secret_key_expiry_status in ("EXPIRING_SOON", "EXPIRED")

    def save(self, *args, **kwargs):
        if not self.account_id:
            self.account_id = self.name
        if not self.api_provider:
            self.enable_trade = False
            self.enable_websocket = False
        if self.totp_secret:
            from kalai.auth.totp import clean_totp_secret
            cleaned = clean_totp_secret(self.totp_secret)
            if cleaned:
                self.totp_secret = cleaned

        # Auto-enable secret key expiry tracking for CoinSwitch if not explicitly set on creation
        if not self.pk and self.is_coinswitch and not self.enable_secret_key_expiry:
            self.enable_secret_key_expiry = True

        # Auto-configure 24/7 operation for crypto accounts by defaulting schedule off
        if not self.pk and self.is_crypto and self.enable_schedule:
            self.enable_schedule = False

        # Auto-track API Secret creation / update timestamp
        if self.api_secret:
            if not self.pk:
                if not self.api_secret_updated_at:
                    self.api_secret_updated_at = timezone.now()
            else:
                try:
                    old_obj = Broker.objects.filter(pk=self.pk).only("api_secret", "api_secret_updated_at").first()
                    if old_obj:
                        if old_obj.api_secret != self.api_secret or not self.api_secret_updated_at:
                            self.api_secret_updated_at = timezone.now()
                except Exception:
                    pass

        self.auto_populate_redirect_url()
        super().save(*args, **kwargs)

# Aliases
account_id = Broker
AccountId = Broker


@receiver(post_save, sender=Broker)
def notify_engine_on_account_save(sender, instance, **kwargs):
    """
    Send a PostgreSQL NOTIFY when an Account is saved, so the engines can reload.
    """
    action = "start" if instance.enable_websocket else "stop"
    payload = json.dumps({
        "account_id": instance.account_id or instance.name,
        "broker": instance.broker_name.code.lower() if instance.broker_name else instance.name.lower(),
        "api_provider": instance.api_provider.code.lower() if instance.api_provider else "",
        "action": action
    })
    
    algo_payload = json.dumps({
        "account_id": instance.account_id or instance.name,
        "action": "reload"
    })
    
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_notify('engine_control', %s)", [payload])
        cursor.execute("SELECT pg_notify('algo_control', %s)", [algo_payload])


@receiver(post_delete, sender=Broker)
def notify_engine_on_account_delete(sender, instance, **kwargs):
    """
    Send a PostgreSQL NOTIFY when an Account is deleted, so the engines stop that account.
    """
    payload = json.dumps({
        "account_id": instance.account_id or instance.name,
        "broker": instance.broker_name.code.lower() if instance.broker_name else instance.name.lower(),
        "api_provider": instance.api_provider.code.lower() if instance.api_provider else "",
        "action": "stop"
    })
    
    algo_payload = json.dumps({
        "account_id": instance.account_id or instance.name,
        "action": "reload"
    })
    
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_notify('engine_control', %s)", [payload])
            cursor.execute("SELECT pg_notify('algo_control', %s)", [algo_payload])
    except Exception:
        pass


class ProcessedTickStore(models.Model):
    class Meta:
        db_table = 'kalai_processedtickstore'
        verbose_name = 'Processed Tick Store'
        verbose_name_plural = 'Processed Tick Stores'
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['account', '-timestamp']),
            models.Index(fields=['account', 'timestamp']),
            models.Index(fields=['-timestamp']),
        ]

    account = models.ForeignKey(Broker, on_delete=models.SET_NULL, null=True, blank=True, db_index=True)
    data = models.JSONField(blank=True, null=True)
    timestamp = models.DateTimeField(default=timezone.now, db_index=True)

    @property
    def broker(self):
        return self.account

    @broker.setter
    def broker(self, value):
        self.account = value

    def __repr__(self):
        return f'<id {self.id}>'


class AlgoInfo(models.Model):
    class Meta:
        db_table = 'kalai_algoinfo'
        verbose_name = 'Algo State / Table'
        verbose_name_plural = 'Algo State / Tables'
        ordering = ['-is_pinned', '-timestamp']
        indexes = [
            models.Index(fields=['account', 'tablename']),
            models.Index(fields=['tablename']),
            models.Index(fields=['-is_pinned', '-timestamp']),
            models.Index(fields=['-timestamp']),
        ]

    account = models.ForeignKey(Broker, on_delete=models.SET_NULL, null=True, blank=True, db_index=True)
    tablename = models.CharField(max_length=100, default="none", db_index=True)
    is_pinned = models.BooleanField(default=False, db_index=True, help_text="Pin this table to the top of the admin changelist")
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)
    tabledata = models.JSONField(blank=True, null=True)

    @property
    def broker(self):
        return self.account

    @broker.setter
    def broker(self, value):
        self.account = value

    def clean(self):
        from django.core.exceptions import ValidationError
        # If storing instrument tokens, strictly enforce tablename matches account
        if self.account and self.tablename and self.tablename.endswith("_inst_tokens"):
            expected = self.account.token_tablename
            if self.tablename.lower() != expected.lower():
                raise ValidationError(
                    f"Invalid token tablename '{self.tablename}' for account '{self.account.account_id}'. "
                    f"Must match '{expected}' to prevent cross-account contamination."
                )

    def save(self, *args, **kwargs):
        if self.account and self.tablename and self.tablename.endswith("_inst_tokens"):
            self.tablename = self.account.token_tablename
        self.clean()
        super().save(*args, **kwargs)

    @classmethod
    def create_or_update(cls, account=None, tablename=None, tabledata=None, ttl=None, broker=None):
        acc = account or broker
        now = timezone.now()
        updated = cls.objects.filter(account=acc, tablename=tablename).update(
            tabledata=tabledata,
            timestamp=now
        )
        if updated == 0:
            cls.objects.update_or_create(
                account=acc,
                tablename=tablename,
                defaults={"tabledata": tabledata, "timestamp": now},
            )

    @classmethod
    def get_table_data(cls, account=None, tablename=None, broker=None):
        acc = account or broker
        query = cls.objects.filter(account=acc, tablename=tablename)
        result = {}
        for item in query:
            result[item.tablename] = item.tabledata
        return result


class ExchangeMasterData(models.Model):
    """
    Dedicated database table exclusively for persisting heavy, daily static exchange
    contract universes (cum_table, aug_table, market metadata).
    Isolates multi-megabyte contract arrays from dynamic runtime strategy state in AlgoInfo.
    """
    class Meta:
        db_table = 'kalai_exchangemaster'
        verbose_name = 'Exchange Master Data'
        verbose_name_plural = 'Exchange Master Data'
        ordering = ['-updated_at']
        indexes = [
            models.Index(fields=['account', 'master_name']),
            models.Index(fields=['master_name']),
            models.Index(fields=['-updated_at']),
        ]

    account = models.ForeignKey(Broker, on_delete=models.CASCADE, null=True, blank=True, db_index=True, related_name='exchange_masters')
    master_name = models.CharField(max_length=100, default="cum_table", db_index=True, help_text="e.g. cum_table, aug_table, coindcx_market_details")
    row_count = models.IntegerField(default=0, help_text="Number of contracts/rows stored")
    updated_at = models.DateTimeField(auto_now=True, db_index=True)
    tabledata = models.JSONField(blank=True, null=True)

    def __str__(self):
        acc = self.account.account_id if self.account else "Global"
        return f"{self.master_name} ({acc}) - {self.row_count} rows"

    @classmethod
    def save_master(cls, account=None, master_name="cum_table", data=None, row_count=None, broker=None):
        acc = account or broker
        count = row_count if row_count is not None else (len(data) if isinstance(data, list) else (len(data.get(master_name, [])) if isinstance(data, dict) and isinstance(data.get(master_name), list) else 0))
        now = timezone.now()
        obj, _ = cls.objects.update_or_create(
            account=acc,
            master_name=master_name,
            defaults={"tabledata": data, "row_count": count, "updated_at": now}
        )
        return obj

    @classmethod
    def get_master(cls, account=None, master_name="cum_table", broker=None):
        acc = account or broker
        item = cls.objects.filter(account=acc, master_name=master_name).first()
        if not item:
            return None
        return item.tabledata


class AlgoLog(models.Model):
    """
    Dedicated database table exclusively for persisting structured algorithm execution,
    trade signals, error traces, and lifecycle logs.
    """
    class Meta:
        db_table = 'kalai_algolog'
        verbose_name = 'Algorithm Log'
        verbose_name_plural = 'Algorithm Logs'
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['-timestamp']),
            models.Index(fields=['account', '-timestamp']),
            models.Index(fields=['tag', '-timestamp']),
            models.Index(fields=['level', '-timestamp']),
        ]

    account = models.ForeignKey(Broker, on_delete=models.SET_NULL, null=True, blank=True, db_index=True, related_name='algo_logs')
    algo_name = models.CharField(max_length=100, default='zerodha_opt', db_index=True, help_text="Algorithm identifier")
    tag = models.CharField(max_length=50, default='GENERAL', db_index=True, help_text="Category tag (e.g. GENERAL, TRADE, STRIKE, PERF)")
    level = models.CharField(max_length=20, default='INFO', db_index=True, help_text="Log level (INFO, WARNING, ERROR)")
    message = models.TextField()
    timestamp = models.DateTimeField(default=timezone.now, db_index=True)

    def __str__(self):
        acc = self.account.account_id if self.account else 'SYSTEM'
        return f"[{self.timestamp.isoformat()}] [{acc}] [{self.tag}] {self.message[:60]}"


class BrokerPosition(models.Model):
    """
    Persists live and squared-off intraday/overnight positions per broker account.
    Updated automatically in background (1-min) and on page load.
    """
    class Meta:
        db_table = 'kalai_brokerposition'
        verbose_name = 'Position'
        verbose_name_plural = 'Positions'
        ordering = ['-updated_at']
        unique_together = ('account', 'tradingsymbol', 'product')
        indexes = [
            models.Index(fields=['account', 'is_open']),
            models.Index(fields=['tradingsymbol']),
            models.Index(fields=['-updated_at']),
        ]

    account = models.ForeignKey(Broker, on_delete=models.CASCADE, related_name='positions')
    tradingsymbol = models.CharField(max_length=100, db_index=True)
    instrument_token = models.BigIntegerField(blank=True, null=True, db_index=True)
    product = models.CharField(max_length=50, default='NRML', help_text="Product type: NRML, MIS, CNC, FUT, SPOT")
    quantity = models.DecimalField(max_digits=18, decimal_places=4, default=0, help_text="Net active quantity (positive for Long, negative for Short, 0 for Closed)")
    buy_quantity = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    buy_price = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    buy_value = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    sell_quantity = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    sell_price = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    sell_value = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    last_price = models.DecimalField(max_digits=18, decimal_places=4, default=0, help_text="Last Traded Price (LTP)")
    unrealized_pnl = models.DecimalField(max_digits=18, decimal_places=4, default=0, help_text="Unrealized M2M profit/loss")
    realized_pnl = models.DecimalField(max_digits=18, decimal_places=4, default=0, help_text="Realized booked profit/loss")
    total_pnl = models.DecimalField(max_digits=18, decimal_places=4, default=0, help_text="Total profit/loss (Realized + Unrealized)")
    is_open = models.BooleanField(default=True, db_index=True, help_text="True if net quantity != 0")
    raw_data = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)

    def __str__(self):
        acc = self.account.account_id or self.account.name
        status = "OPEN" if self.is_open else "CLOSED"
        return f"[{acc}] {self.tradingsymbol} ({self.product}) Qty:{self.quantity} PnL:{self.total_pnl:+.2f} [{status}]"


class TradeRecord(models.Model):
    """
    Granular execution ledger of every filled order and trade event.
    Retained for 1+ years for statistical inference and audit trail.
    """
    class Meta:
        db_table = 'kalai_traderecord'
        verbose_name = 'Trade Record'
        verbose_name_plural = 'Trade Records'
        ordering = ['-executed_at']
        indexes = [
            models.Index(fields=['account', '-executed_at']),
            models.Index(fields=['tradingsymbol', '-executed_at']),
            models.Index(fields=['-executed_at']),
        ]

    account = models.ForeignKey(Broker, on_delete=models.CASCADE, related_name='trade_records')
    order_id = models.CharField(max_length=100, blank=True, null=True, db_index=True)
    tradingsymbol = models.CharField(max_length=100, db_index=True)
    product = models.CharField(max_length=50, default='NRML')
    action_type = models.CharField(max_length=20, default='BUY', help_text="BUY, SELL, STOP_LOSS, SQUARE_OFF")
    quantity = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    price = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    value = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    realized_pnl = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    executed_at = models.DateTimeField(default=timezone.now, db_index=True)
    raw_payload = models.JSONField(default=dict, blank=True)

    def __str__(self):
        acc = self.account.account_id or self.account.name
        return f"[{self.executed_at.strftime('%Y-%m-%d %H:%M')}] [{acc}] {self.action_type} {self.quantity}x {self.tradingsymbol} @ {self.price}"


class DailyPnLSnapshot(models.Model):
    """
    Daily aggregated performance and P&L snapshots per broker account.
    Enables sub-2ms multi-week, monthly, and 1-year historical analytics.
    """
    class Meta:
        db_table = 'kalai_dailypnlsnapshot'
        verbose_name = 'Daily P&L Snapshot'
        verbose_name_plural = 'Daily P&L Snapshots'
        ordering = ['-date', 'account']
        unique_together = ('account', 'date')
        indexes = [
            models.Index(fields=['account', '-date']),
            models.Index(fields=['-date']),
        ]

    account = models.ForeignKey(Broker, on_delete=models.CASCADE, related_name='daily_pnl_snapshots')
    date = models.DateField(db_index=True)
    realized_pnl = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    unrealized_pnl = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    net_pnl = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    total_trades = models.IntegerField(default=0)
    winning_trades = models.IntegerField(default=0)
    losing_trades = models.IntegerField(default=0)
    turnover = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    closing_balance = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        acc = self.account.account_id or self.account.name
        return f"[{self.date}] [{acc}] Net PnL: {self.net_pnl:+.2f} (Trades: {self.total_trades})"



