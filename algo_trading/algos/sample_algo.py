import asyncio
import logging
from datetime import datetime
from asgiref.sync import sync_to_async
from algo_trading.algos import algo
from algo_trading.algos.logger import algo_logger
from algo_trading.algos.zerodha_utils import ZerodhaUtility

# Django models
from kalai.models import Broker, ProcessedTickStore

logger = logging.getLogger(__name__)


class SampleTradingAlgo:
    """
    Sample trading strategy integrating ZerodhaUtility for portfolio inspection,
    margin verification, order management, and dynamic WebSocket token rotation.
    """

    def __init__(self, rotation_interval: int = 5):
        # Execution count per account
        self.execution_counts: dict[str, int] = {}
        self.rotation_interval = rotation_interval
        # Cached ZerodhaUtility instances per account
        self.zerodha_utils: dict[str, ZerodhaUtility] = {}
        # Predefined token sets for dynamic subscription rotation
        self.sample_token_sets = [
            [256265, 260105],  # NIFTY 50, NIFTY BANK
            [341249, 408065],  # HDFCBANK, INFY
            [25601, 738561],   # TCS, RELIANCE
        ]

    @sync_to_async
    def get_broker_keys(self, account_id: str):
        """Fetch Broker database instance and API credentials."""
        try:
            from django.db.models import Q
            broker = (
                Broker.objects.select_related("broker_name", "api_provider")
                .filter(Q(account_id__iexact=account_id) | Q(name__iexact=account_id))
                .first()
            )
            if broker:
                return broker.api_key, broker.api_secret, broker
            return None, None, None
        except Exception as e:
            logger.error("Error fetching broker for %s: %s", account_id, e)
            return None, None, None

    @sync_to_async
    def _write_tokens_to_db(self, broker, new_tokens: list):
        """Update AlgoInfo token subscription table strictly bound to this broker instance."""
        broker.set_subscribed_tokens(new_tokens)

    async def update_subscribed_tokens(self, broker, account_id: str, new_tokens: list) -> bool:
        """Dynamically rotate tokens in AlgoInfo to trigger WebSocket feed subscription updates."""
        tablename = broker.token_tablename
        iso_ts = datetime.now().isoformat()
        log_msg = f"[{iso_ts}] [TOKEN ROTATION] Requesting token list update for '{account_id}' -> {new_tokens}"
        
        logger.info(log_msg)
        await algo_logger.add_log(account_id, log_msg, tablename="sample_algo_logs")
        
        try:
            await self._write_tokens_to_db(broker, new_tokens)
            success_msg = f"[{datetime.now().isoformat()}] [TOKEN ROTATION SUCCESS] Updated table '{tablename}' with tokens: {new_tokens}"
            logger.info(success_msg)
            await algo_logger.add_log(account_id, success_msg, tablename="sample_algo_logs")
            return True
        except Exception as e:
            error_msg = f"[{datetime.now().isoformat()}] [TOKEN ROTATION ERROR] Failed for '{account_id}': {e}"
            logger.error(error_msg)
            await algo_logger.add_log(account_id, error_msg, tablename="sample_algo_logs")
            return False

    @sync_to_async
    def get_latest_tick_data(self, broker, limit=3):
        """Fetch recent websocket ticks from ProcessedTickStore."""
        if not broker:
            return []
        try:
            ticks = ProcessedTickStore.objects.filter(account=broker).order_by('-timestamp')[:limit]
            return [t.data for t in ticks]
        except Exception as e:
            logger.error("Error fetching tick data for %s: %s", broker.name, e)
            return []

    def _get_zerodha_utility(self, account_id: str, broker) -> ZerodhaUtility | None:
        """Get or initialize cached ZerodhaUtility for the account."""
        if account_id in self.zerodha_utils:
            return self.zerodha_utils[account_id]

        try:
            util = ZerodhaUtility(account_id=account_id, broker_obj=broker)
            self.zerodha_utils[account_id] = util
            return util
        except Exception as exc:
            logger.warning("Could not initialize ZerodhaUtility for '%s': %s", account_id, exc)
            return None

    async def inspect_zerodha_account(self, account_id: str, broker) -> dict:
        """
        Asynchronously query Zerodha live margins, holdings, positions, and sample margin check.
        Runs API calls in thread pool to prevent blocking the event loop.
        """
        util = self._get_zerodha_utility(account_id, broker)
        if not util:
            return {"status": "skipped", "reason": "ZerodhaUtility not initialized"}

        result = {}
        try:
            # 1. Fetch live margins & cash
            avail_cash, net_cap = await asyncio.to_thread(util.chk_live_bal)
            result["available_cash"] = avail_cash
            result["net_capital"] = net_cap

            # 2. Fetch holdings
            holdings_df = await asyncio.to_thread(util.holdings)
            result["holdings_count"] = len(holdings_df) if holdings_df is not None else 0

            # 3. Fetch positions
            day_pos_df, net_pos_df = await asyncio.to_thread(util.pos_data)
            result["day_positions"] = len(day_pos_df) if day_pos_df is not None else 0
            result["net_positions"] = len(net_pos_df) if net_pos_df is not None else 0

            # 4. Sample margin calculation check for 1 share of INFY
            sample_order = [{
                "exchange": "NSE",
                "tradingsymbol": "INFY",
                "transaction_type": "BUY",
                "variety": "regular",
                "product": "MIS",
                "order_type": "MARKET",
                "quantity": 1,
            }]
            try:
                margins = await asyncio.to_thread(util.get_margin, sample_order)
                if margins and isinstance(margins, list):
                    result["sample_margin_required"] = margins[0].get("total", 0.0)
            except Exception:
                result["sample_margin_required"] = "N/A"

            result["status"] = "success"
        except Exception as exc:
            result["status"] = "error"
            result["error"] = str(exc)

        return result

    async def execute(self, account_id: str):
        """Main strategy execution routine."""
        iso_now = datetime.now().isoformat()
        await algo_logger.add_log(
            account_id,
            f"[{iso_now}] Executing SampleTradingAlgo for account: {account_id}",
            tablename="sample_algo_logs"
        )
        
        # 1. Fetch broker record
        api_key, api_secret, broker = await self.get_broker_keys(account_id)
        if not broker:
            await algo_logger.add_log(
                account_id,
                f"[{datetime.now().isoformat()}] Broker not found for account {account_id}",
                tablename="sample_algo_logs"
            )
            return

        # 2. If this is a Zerodha account, inspect live balances and portfolio via ZerodhaUtility
        is_zerodha = (
            (broker.broker_name and broker.broker_name.code.lower() == "zerodha")
            or (broker.api_provider and broker.api_provider.code.lower() == "zerodha")
            or "zerodha" in broker.name.lower()
        )
        
        if is_zerodha and broker.access_token:
            z_data = await self.inspect_zerodha_account(account_id, broker)
            if z_data.get("status") == "success":
                cash = z_data.get("available_cash", 0.0)
                cap = z_data.get("net_capital", 0.0)
                hold_cnt = z_data.get("holdings_count", 0)
                margin_req = z_data.get("sample_margin_required", "N/A")
                log_msg = (
                    f"[{datetime.now().isoformat()}] [ZERODHA LIVE] "
                    f"Cash: Rs {cash:,.2f} | Net Capital: Rs {cap:,.2f} | "
                    f"Holdings: {hold_cnt} | Sample INFY Margin: {margin_req}"
                )
                await algo_logger.add_log(account_id, log_msg, tablename="sample_algo_logs")
            elif z_data.get("status") == "error":
                await algo_logger.add_log(
                    account_id,
                    f"[{datetime.now().isoformat()}] [ZERODHA ERROR] {z_data.get('error')}",
                    tablename="sample_algo_logs"
                )

        # 3. Dynamic token rotation trigger
        count = self.execution_counts.get(account_id, 0) + 1
        self.execution_counts[account_id] = count

        if count % self.rotation_interval == 0:
            set_index = (count // self.rotation_interval) % len(self.sample_token_sets)
            new_tokens = self.sample_token_sets[set_index]
            await self.update_subscribed_tokens(broker, account_id, new_tokens)

        # 4. Inspect latest live websocket tick data
        latest_ticks = await self.get_latest_tick_data(broker, limit=1)
        if latest_ticks:
            tick_preview = str(latest_ticks[0])[:120]
            await algo_logger.add_log(
                account_id,
                f"[{datetime.now().isoformat()}] [TICK DATA] {tick_preview}...",
                tablename="sample_algo_logs"
            )

        # 5. Flush logs to AlgoInfo periodically
        await algo_logger.flush_if_needed(account_id, broker, tablename="sample_algo_logs")


# Preserved singleton strategy instance
sample_strategy = SampleTradingAlgo(rotation_interval=5)


@algo
async def my_trading_strategy(account_id: str):
    """
    Primary trading strategy executed automatically by algo_engine.
    """
    await sample_strategy.execute(account_id)
