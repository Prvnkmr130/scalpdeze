"""
algo_trading/urls.py
────────────────────
HTTP URL configuration for algo_trading project.
"""

from django.contrib import admin
from django.urls import path, include

from django.contrib.auth.views import LogoutView

import os

ADMIN_PATH = os.getenv("ADMIN_URL_PATH", "d4f8g9h2j1m5k8p3").strip("/")

# Redirect unauthenticated admin access to home page
admin.site.login_url = '/'
admin.site.index_title = ""
admin.site.site_title = "DeltaZero"

urlpatterns = [
    # Intercept admin logout and redirect to home page
    path(f"{ADMIN_PATH}/logout/", LogoutView.as_view(next_page='/')),
    path(f"{ADMIN_PATH}/", admin.site.urls),
    # Add your app URL patterns below:
    path("", include("kalai.urls")),
]

#changes