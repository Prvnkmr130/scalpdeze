"""
algo_trading/wsgi.py
────────────────────
WSGI config for algo_trading project.
Used as a fallback when not running under ASGI (Uvicorn).
"""

import os
from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")

application = get_wsgi_application()
