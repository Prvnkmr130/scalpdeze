"""
algo_trading/asgi.py
────────────────────
ASGI config for algo_trading project.
Routes HTTP and WebSocket protocols through Django Channels.
"""

import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
django.setup()

from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from django.core.asgi import get_asgi_application

websocket_urlpatterns = []


application = ProtocolTypeRouter({
    "http": get_asgi_application(),
    "websocket": AuthMiddlewareStack(
        URLRouter(websocket_urlpatterns)
    ),
})
