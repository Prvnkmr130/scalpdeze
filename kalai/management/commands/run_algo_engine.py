"""
Management command: run_algo_engine
─────────────────────────────────
Start the async Algorithm Execution engine from the Django CLI.

Examples:
    python manage.py run_algo_engine
"""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Start the async multi-account Algorithm Execution engine."

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.NOTICE("Starting Algorithm Execution engine for ALL active broker accounts.")
        )

        from algo_trading.brokers.algo_engine import start_algo_engine

        start_algo_engine()
