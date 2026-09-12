"""
algo_trading/brokers/registry.py
─────────────────────────────────
Auto-discovers broker feed classes from ``algo_trading.brokers.feeds``
and matches them against DB-enabled brokers.

Adding a new broker:
    1. Create ``algo_trading/brokers/feeds/<broker_name>.py``
    2. Define a class that subclasses ``BaseFeed`` with ``BROKER_NAME = "<broker_name>"``
    3. Enable the broker in the DB (``Broker.enable_websocket = True``)
    4. Run the engine — the registry auto-discovers and spawns it.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from algo_trading.brokers.base import BaseFeed

logger = logging.getLogger("algo_trading.brokers")


class BrokerRegistry:
    """
    Discovers all ``BaseFeed`` subclasses inside the ``feeds`` sub-package
    and provides helpers to instantiate them with DB credentials.
    """

    def __init__(self) -> None:
        # Maps broker_name → feed class  (e.g. "zerodha" → ZerodhaFeed)
        self._feed_classes: dict[str, type[BaseFeed]] = {}
        self._logged_token_skips: set[tuple[str, object]] = set()
        self._logged_empty_feeds: bool = False
        self._discover()

    # ──────────────────────────────────────────────────────────────
    # Discovery
    # ──────────────────────────────────────────────────────────────

    def _discover(self) -> None:
        """
        Import every module inside ``algo_trading.brokers.feeds`` and
        register any class that has a non-empty ``BROKER_NAME`` attribute
        and is a subclass of ``BaseFeed``.
        """
        from algo_trading.brokers.base import BaseFeed

        feeds_package = importlib.import_module("algo_trading.brokers.feeds")
        package_path = feeds_package.__path__

        for _importer, module_name, _is_pkg in pkgutil.iter_modules(package_path):
            if module_name.startswith("test_") or module_name.startswith("_") or module_name == "hs_websocket":
                continue
            full_name = f"algo_trading.brokers.feeds.{module_name}"
            try:
                module = importlib.import_module(full_name)
            except Exception as exc:
                logger.warning("Failed to import feed module '%s': %s", full_name, exc)
                continue

            for attr_name in dir(module):
                obj = getattr(module, attr_name)
                if (
                    isinstance(obj, type)
                    and issubclass(obj, BaseFeed)
                    and obj is not BaseFeed
                    and getattr(obj, "BROKER_NAME", "")
                ):
                    broker_name = obj.BROKER_NAME.lower()
                    self._feed_classes[broker_name] = obj
                    for alias in getattr(obj, "ALIASES", []):
                        self._feed_classes[alias.lower()] = obj
                    logger.info(
                        "Registered feed: %s -> %s",
                        broker_name,
                        obj.__name__,
                    )

    # ──────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────

    async def get_enabled_feeds(
        self,
        account_filter: list[str] | None = None,
    ) -> list[BaseFeed]:
        """
        Asynchronously discover and load enabled feeds using a thread pool
        for database operations to prevent SynchronousOnlyOperation.
        """
        import asyncio
        return await asyncio.to_thread(self._get_enabled_feeds_sync, account_filter)

    def _get_enabled_feeds_sync(
        self,
        account_filter: list[str] | None = None,
    ) -> list[BaseFeed]:
        """
        Query the DB for enabled broker accounts, optionally filtered by account ID/name,
        and return instantiated feed objects with credentials loaded.
        """
        from algo_trading.brokers.config import engine_config
        from kalai.models import Broker

        enabled_brokers = list(Broker.objects.filter(enable_websocket=True))

        if account_filter:
            filter_set = {a.lower() for a in account_filter}
            enabled_brokers = [
                b for b in enabled_brokers
                if (b.account_id and b.account_id.lower() in filter_set) or (b.name.lower() in filter_set) or (b.broker_name.code.lower() if b.broker_name else "" in filter_set)
            ]

        feeds: list[BaseFeed] = []

        for broker_obj in enabled_brokers:
            account_id = broker_obj.account_id or broker_obj.name
            api_provider = (broker_obj.api_provider.code if broker_obj.api_provider else "").lower()

            broker_name_code = (broker_obj.broker_name.code if broker_obj.broker_name else "").lower()
            feed_cls = self._feed_classes.get(api_provider) or self._feed_classes.get(broker_name_code)

            if feed_cls is None:
                logger.warning(
                    "Account '%s' (Provider: '%s', Broker: '%s') has no registered WebSocket feed class. Skipping.",
                    account_id,
                    api_provider,
                    broker_name_code,
                )
                continue

            # Ensure access token is from today if required
            if broker_obj.access_token_required:
                from django.utils import timezone
                today = timezone.localtime().date()
                token_date = timezone.localtime(broker_obj.access_token_updated_at).date() if broker_obj.access_token_updated_at else None
                if not token_date or token_date != today:
                    skip_key = (account_id, today)
                    if skip_key not in self._logged_token_skips:
                        self._logged_token_skips.add(skip_key)
                        logger.warning("%s access token is not from today (%s). Skipping WebSocket connection until operator logs in.", account_id, today)
                    continue
                else:
                    self._logged_token_skips.discard((account_id, today))

            # Load instrument tokens
            tokens = self._load_tokens(broker_obj, account_id, broker_obj.broker_name.code if broker_obj.broker_name else "", feed_cls)

            # Instantiate
            feed = feed_cls(
                account_id=account_id,
                broker_name=broker_obj.broker_name.code if broker_obj.broker_name else broker_obj.name,
                api_provider=api_provider,
                api_key=broker_obj.api_key or "",
                access_token=broker_obj.access_token or "",
                api_secret=broker_obj.api_secret or "",
                api_endpoint=broker_obj.api_endpoint or "",
                instrument_tokens=tokens,
                rate_limit_interval=engine_config.RATE_LIMIT_INTERVAL,
                reconnect_base_delay=engine_config.RECONNECT_BASE_DELAY,
                reconnect_max_delay=engine_config.RECONNECT_MAX_DELAY,
                token_refresh_interval=engine_config.TOKEN_REFRESH_INTERVAL,
            )
            feeds.append(feed)
            logger.info("Instantiated feed: %r", feed)

        if not feeds:
            if not self._logged_empty_feeds:
                self._logged_empty_feeds = True
                logger.warning("No feeds were instantiated. Check broker account DB records.")
        else:
            self._logged_empty_feeds = False

        return feeds

    async def get_feed_for_account(self, account_id: str) -> BaseFeed | None:
        """Fetch and instantiate a single feed by account ID."""
        import asyncio
        return await asyncio.to_thread(self._get_feed_for_account_sync, account_id)

    async def get_feed_for_broker(self, broker_name: str) -> BaseFeed | None:
        """Backward compatibility alias for get_feed_for_account."""
        return await self.get_feed_for_account(broker_name)

    def _get_feed_for_account_sync(self, account_id: str) -> BaseFeed | None:
        from algo_trading.brokers.config import engine_config
        from kalai.models import Broker
        from django.db.models import Q

        try:
            broker_obj = Broker.objects.get(Q(account_id__iexact=account_id) | Q(name__iexact=account_id))
        except Broker.DoesNotExist:
            logger.error("Account '%s' not found in DB.", account_id)
            return None

        acc_id = broker_obj.account_id or broker_obj.name
        api_provider = (broker_obj.api_provider.code if broker_obj.api_provider else "").lower()

        if not api_provider:
            logger.warning("Account '%s' has no api_provider set.", acc_id)
            return None

        feed_cls = self._feed_classes.get(api_provider)

        if feed_cls is None:
            logger.warning("No feed class registered for API provider '%s'.", api_provider)
            return None

        if broker_obj.access_token_required:
            from django.utils import timezone
            today = timezone.localtime().date()
            token_date = timezone.localtime(broker_obj.access_token_updated_at).date() if broker_obj.access_token_updated_at else None
            if not token_date or token_date != today:
                skip_key = (acc_id, today)
                if skip_key not in self._logged_token_skips:
                    self._logged_token_skips.add(skip_key)
                    logger.warning("%s access token is not from today (%s). Skipping WebSocket connection until operator logs in.", acc_id, today)
                return None
            else:
                self._logged_token_skips.discard((acc_id, today))

        tokens = self._load_tokens(broker_obj, acc_id, broker_obj.broker_name.code if broker_obj.broker_name else "", feed_cls)

        feed = feed_cls(
            account_id=acc_id,
            broker_name=broker_obj.broker_name.code if broker_obj.broker_name else broker_obj.name,
            api_provider=api_provider,
            api_key=broker_obj.api_key or "",
            access_token=broker_obj.access_token or "",
            api_secret=broker_obj.api_secret or "",
            api_endpoint=broker_obj.api_endpoint or "",
            instrument_tokens=tokens,
            rate_limit_interval=engine_config.RATE_LIMIT_INTERVAL,
            reconnect_base_delay=engine_config.RECONNECT_BASE_DELAY,
            reconnect_max_delay=engine_config.RECONNECT_MAX_DELAY,
            token_refresh_interval=engine_config.TOKEN_REFRESH_INTERVAL,
        )
        logger.info("Instantiated feed on-demand: %r", feed)
        return feed

    # ──────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────

    @staticmethod
    def _load_tokens(broker_obj, account_id: str, broker_name: str, feed_cls: type[BaseFeed] | None = None) -> list:
        """Read instrument tokens strictly bound to this account instance."""
        tokens = broker_obj.get_subscribed_tokens() if broker_obj else []

        if not tokens and feed_cls:
            default_toks = getattr(feed_cls, "DEFAULT_INSTRUMENT_TOKENS", [])
            if default_toks:
                tokens = list(default_toks)

        return tokens

    @property
    def registered_brokers(self) -> list[str]:
        """Return names of all discovered broker feed classes."""
        return list(self._feed_classes.keys())
