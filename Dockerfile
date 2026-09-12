# --------------------------------------------------------------
# Stage 1: Builder — Install Python dependencies with uv
# --------------------------------------------------------------
FROM python:3.14-slim AS builder

# Grab prebuilt uv binary in 0.1s
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install dependencies first (cached layer using lockfile)
COPY pyproject.toml uv.lock ./
RUN --network=host --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Install the project itself
COPY . .
RUN --network=host --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


# --------------------------------------------------------------
# Stage 2: Runtime — Django + PostgreSQL in one container
# --------------------------------------------------------------
FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1
ENV APP_HOME=/app
ENV POLARS_MAX_THREADS=2
ENV OPENBLAS_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV NUMEXPR_NUM_THREADS=1
ENV OMP_NUM_THREADS=1
ENV VECLIB_MAXIMUM_THREADS=1
ENV MALLOC_ARENA_MAX=2
ENV MALLOC_TRIM_THRESHOLD_=131072

WORKDIR $APP_HOME

# Install runtime dependencies + PostgreSQL server + supervisord + nginx with BuildKit cache
RUN --network=host --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates lsb-release gnupg \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/postgresql-keyring.gpg] http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    git \
    libpq5 \
    postgresql-client-18 \
    postgresql-18 \
    postgresql-common \
    supervisor \
    nginx \
    && rm -rf /var/lib/apt/lists/* \
    && git config --system --add safe.directory '*'

# Copy Python virtual environment from builder
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy application source
COPY . .

# Create directories
RUN mkdir -p /app/logs /app/staticfiles /var/run/postgresql /var/lib/postgresql/data \
    && chown -R postgres:postgres /var/run/postgresql /var/lib/postgresql/data

# Collect static files
RUN python manage.py collectstatic --noinput

# Generate self-signed certificate for fallback HTTPS
RUN mkdir -p /etc/nginx/ssl && openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/nginx/ssl/nginx-selfsigned.key -out /etc/nginx/ssl/nginx-selfsigned.crt -subj "/CN=localhost"

# Copy configuration files
COPY supervisord.conf /etc/supervisor/supervisord.conf
COPY pg_entrypoint.sh /usr/local/bin/pg_entrypoint.sh
COPY nginx.conf /etc/nginx/nginx.conf
RUN sed -i 's/\r$//' /usr/local/bin/pg_entrypoint.sh && chmod +x /usr/local/bin/pg_entrypoint.sh

EXPOSE 80 443

# PostgreSQL data directory is mounted externally
VOLUME ["/var/lib/postgresql/data"]

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fsk https://localhost/health/ || exit 1

CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"]
