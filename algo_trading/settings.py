"""
algo_trading/settings.py
────────────────────────
Django 6.0 settings.
Reads all configuration from app_config.config singleton.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from algo_trading.app_config import config

BASE_DIR = Path(__file__).resolve().parent.parent

# ═══════════════════════════════════════════════════════════════
# CORE
# ═══════════════════════════════════════════════════════════════
SECRET_KEY = config.SECRET_KEY
M2M_SERVER_KEY = config.M2M_SERVER_KEY
SSL_VERIFY = config.SSL_VERIFY
DEBUG = config.is_debug
ALLOWED_HOSTS = config.allowed_hosts_list
CSRF_TRUSTED_ORIGINS = [
    f"http://{h}" for h in ALLOWED_HOSTS if h != "*"
] + [
    f"https://{h}" for h in ALLOWED_HOSTS if h != "*"
]

ROOT_URLCONF = "algo_trading.urls"
WSGI_APPLICATION = "algo_trading.wsgi.application"
ASGI_APPLICATION = "algo_trading.asgi.application"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ═══════════════════════════════════════════════════════════════
# TIMEZONE & LOCALE
# ═══════════════════════════════════════════════════════════════
TIME_ZONE = config.TIMEZONE
LANGUAGE_CODE = "en-us"
USE_I18N = False          # Single-user app — disable translation machinery for performance
USE_TZ = True

# ═══════════════════════════════════════════════════════════════
# INSTALLED APPS
# ═══════════════════════════════════════════════════════════════
INSTALLED_APPS = [
    "daphne",             # Required for Channels runserver replacement
    "channels",           # Django Channels for WebSocket support
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.humanize",
    "kalai",              # Your app
    "django_q",           # Django Q2 for task scheduling
]

# ═══════════════════════════════════════════════════════════════
# MIDDLEWARE — STANDARD SECURE DJANGO STACK
# ═══════════════════════════════════════════════════════════════
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

X_FRAME_OPTIONS = "SAMEORIGIN"

# ═══════════════════════════════════════════════════════════════
# DATABASE — PostgreSQL with Connection Pooling
# ═══════════════════════════════════════════════════════════════
import sys

_is_management_cmd = (
    len(sys.argv) > 0
    and (
        not any(server_cmd in sys.argv[0].lower() or server_cmd in " ".join(sys.argv[1:]).lower() for server_cmd in ["runserver", "uvicorn", "daphne", "gunicorn"])
    )
)

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": config.DB_NAME,
        "USER": config.DB_USER,
        "PASSWORD": config.DB_PASSWORD,
        "HOST": config.DB_HOST,
        "PORT": str(config.DB_PORT),
        "CONN_MAX_AGE": 0,     # Managed by connection pool
        "CONN_HEALTH_CHECKS": True,                  # Django 6: verify connection before reuse
    },
    "remote": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("REMOTE_DB_NAME", "algo_trading"),
        "USER": os.getenv("REMOTE_DB_USER", "appuser"),
        "PASSWORD": os.getenv("REMOTE_DB_PASSWORD", ""),
        "HOST": os.getenv("REMOTE_DB_HOST", "127.0.0.1"),
        "PORT": os.getenv("REMOTE_DB_PORT", "5432"),
        "CONN_MAX_AGE": 0,
        "CONN_HEALTH_CHECKS": True,
    }
}

# Connection pool configuration:
# Differentiates web server (uvicorn/daphne) from linear management commands / worker processes.
# Uses min_size: 0 and max_idle: 30 so idle connections cleanly terminate and free Postgres backend RAM.
if _is_management_cmd:
    DATABASES["default"]["OPTIONS"] = {
        "pool": {
            "min_size": 0,
            "max_size": 2,
            "timeout": 10.0,
            "max_idle": 15,
        },
    }
elif config.is_debug:
    # In DEBUG / Local mode, provide an expanded connection pool (min_size: 1, max_size: 6)
    # to handle interactive developer tools, log analyzer, and parallel browser tabs without pool starvation.
    DATABASES["default"]["OPTIONS"] = {
        "pool": {
            "min_size": getattr(config, "DB_POOL_MIN", 1),
            "max_size": max(int(getattr(config, "DB_POOL_MAX", 6)), 6),
            "timeout": 10.0,
            "max_idle": 30,
        },
    }
else:
    # In PRODUCTION, keep a lean pool (min_size: 0, default max_size: 2-3) to protect PostgreSQL
    # max_connections and avoid CPU / RAM stress from idle persistent backend processes.
    DATABASES["default"]["OPTIONS"] = {
        "pool": {
            "min_size": 0,
            "max_size": getattr(config, "DB_POOL_MAX", getattr(config, "DB_CONN_POOL_SIZE", 2)),
            "timeout": 10.0,
            "max_idle": 30,
        },
    }

# ═══════════════════════════════════════════════════════════════
# CHANNEL LAYERS — WebSocket message bus
# ═══════════════════════════════════════════════════════════════
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
        "CONFIG": {
            "capacity": 100,  # Bound message queue size to prevent memory bloat
            "expiry": 10,     # Discard unread messages after 10s
        },
        # ──────────────────────────────────────────────────────
        # UPGRADE TO REDIS WHEN SCALING:
        # "BACKEND": "channels_redis.core.RedisChannelLayer",
        # "CONFIG": {
        #     "hosts": [("127.0.0.1", 6379)],
        #     "capacity": 500,
        #     "expiry": 10,
        # },
        # ──────────────────────────────────────────────────────
    },
}

# ═══════════════════════════════════════════════════════════════
# TEMPLATES
# ═══════════════════════════════════════════════════════════════
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "kalai.context_processors.debug_mode",
                "kalai.context_processors.coinswitch_expiry_alerts",
                "kalai.context_processors.admin_system_hub",
            ],
        },
    },
]

# ═══════════════════════════════════════════════════════════════
# SECURITY SETTINGS
# ═══════════════════════════════════════════════════════════════
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

if config.is_production:
    CSRF_COOKIE_SECURE = True
    SESSION_COOKIE_SECURE = True
    SECURE_HSTS_SECONDS = 31536000
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True
    SECURE_SSL_REDIRECT = True
else:
    CSRF_COOKIE_SECURE = False
    SESSION_COOKIE_SECURE = False
    SECURE_SSL_REDIRECT = False

# Exclude health checks from SSL redirection (avoids 301 errors in docker healthchecks)
SECURE_REDIRECT_EXEMPT = [r'^health/$']

# Django 6 CSP — Explicitly disabled
SECURE_CSP = None
SECURE_CSP_REPORT_ONLY = None

# Session timeout for single-user security
SESSION_COOKIE_AGE = 86400  # 24 hours

# ═══════════════════════════════════════════════════════════════
# PASSWORD VALIDATION
# ═══════════════════════════════════════════════════════════════
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
     "OPTIONS": {"min_length": 12}},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# ═══════════════════════════════════════════════════════════════
# STATIC FILES
# ═══════════════════════════════════════════════════════════════
STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "{asctime} [{levelname}] {name}.{funcName}:{lineno} — {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "app_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": BASE_DIR / "logs" / "app.log",
            "maxBytes": 5 * 1024 * 1024,       # 5 MB
            "backupCount": 3,
            "formatter": "verbose",
        },
    },
    "root": {
        "handlers": ["console", "app_file"],
        "level": config.LOG_LEVEL,
    },
    "loggers": {
        "django": {"level": "WARNING", "propagate": True},
        "django.db.backends": {"level": "WARNING", "propagate": False},
        "websockets": {"level": config.LOG_LEVEL, "propagate": True},
        "algo_trading.brokers": {"level": config.LOG_LEVEL, "propagate": True},
    },
}

# ═══════════════════════════════════════════════════════════════
# AUTHENTICATION REDIRECTS
# ═══════════════════════════════════════════════════════════════
LOGIN_URL = '/'
LOGOUT_REDIRECT_URL = '/'

# ═══════════════════════════════════════════════════════════════
# DJANGO Q2 CLUSTER CONFIGURATION
# ═══════════════════════════════════════════════════════════════
Q_CLUSTER = {
    "name": "deltazero_q",
    "workers": config.Q_CLUSTER_WORKERS,
    "recycle": 50,  # Recycle worker process after 50 tasks to free heap RAM
    "max_rss": 120000,  # Recycle worker process if RSS exceeds 120 MB (in KB)
    "save_limit": 20,  # Keep only recent 20 task results to prevent RAM bloat
    "timeout": 60,
    "retry": 120,
    "queue_limit": 50,
    "bulk": 10,
    "orm": "default",
}
