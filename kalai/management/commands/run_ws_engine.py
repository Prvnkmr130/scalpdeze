"""
Management command: run_ws_engine
─────────────────────────────────
Start the async WebSocket engine from the Django CLI.

Examples:
    python manage.py run_ws_engine                            # all enabled
    python manage.py run_ws_engine --brokers zerodha          # one broker
    python manage.py run_ws_engine --brokers zerodha coindcx  # specific set
"""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Start the async multi-account WebSocket engine."

    def add_arguments(self, parser):
        parser.add_argument(
            "--accounts",
            "--brokers",
            dest="accounts",
            nargs="*",
            default=None,
            help=(
                "Space-separated account IDs or broker names to run "
                "(default: all DB-enabled accounts). "
                "Example: --accounts ACC_1001 ACC_1002"
            ),
        )

    def handle(self, *args, **options):
        account_filter = options["accounts"]

        if account_filter:
            self.stdout.write(
                self.style.NOTICE(
                    f"Starting engine for accounts: {', '.join(account_filter)}"
                )
            )
        else:
            self.stdout.write(
                self.style.NOTICE("Starting engine for ALL enabled broker accounts.")
            )

        from algo_trading.brokers.engine import start_engine

        start_engine(account_filter)
