#!/bin/bash
# docker/pg_entrypoint.sh
# ═══════════════════════════════════════════════════════════════
# Initialize and start PostgreSQL within the single container
# ═══════════════════════════════════════════════════════════════

set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data/pgdata}"
export PGDATA
export PATH="/usr/lib/postgresql/18/bin:$PATH"

POSTGRES_USER="${POSTGRES_USER:-appuser}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-CHANGE_ME_strong_password_here}"
POSTGRES_DB="${POSTGRES_DB:-algo_trading}"

# Fix permissions on mounted volume directory
mkdir -p "$PGDATA" /var/run/postgresql
chown -R postgres:postgres /var/lib/postgresql/data /var/run/postgresql
chmod 700 "$PGDATA"

# Initialize the database if not already done
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    PWFILE="/tmp/.pgpass_init"
    echo "$POSTGRES_PASSWORD" > "$PWFILE"
    chown postgres:postgres "$PWFILE"
    chmod 600 "$PWFILE"
    runuser -u postgres -- initdb --username="$POSTGRES_USER" --pwfile="$PWFILE" -D "$PGDATA"
    rm -f "$PWFILE"

    # Configure PostgreSQL for low-memory environment (1 vCPU / 2 GB RAM)
    cat >> "$PGDATA/postgresql.conf" <<EOF

# ─── Custom: Lean configuration for 1 vCPU / 2 GB RAM ────────
listen_addresses = '*'
max_connections = 40
shared_buffers = 128MB
effective_cache_size = 256MB
work_mem = 4MB
maintenance_work_mem = 32MB
wal_buffers = 4MB
checkpoint_completion_target = 0.9
random_page_cost = 1.1
effective_io_concurrency = 200
min_wal_size = 32MB
max_wal_size = 256MB
logging_collector = off
log_destination = 'stderr'
EOF

    # Configure authentication (scram-sha-256 for security)
    echo "host all all 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"

    # Start temporarily to create the application database
    runuser -u postgres -- pg_ctl -D "$PGDATA" -o "-c listen_addresses='*'" -w start
    runuser -u postgres -- psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE $POSTGRES_DB;" 2>/dev/null || true
    runuser -u postgres -- pg_ctl -D "$PGDATA" -w stop

    echo "==> PostgreSQL initialization complete."
fi

# Ensure existing postgresql.conf allows 40 connections and limits WAL storage
if [ -f "$PGDATA/postgresql.conf" ]; then
    sed -i "s/^max_connections = .*/max_connections = 40/" "$PGDATA/postgresql.conf" 2>/dev/null || true
    sed -i "s/^shared_buffers = .*/shared_buffers = 128MB/" "$PGDATA/postgresql.conf" 2>/dev/null || true
    sed -i "s/^work_mem = .*/work_mem = 4MB/" "$PGDATA/postgresql.conf" 2>/dev/null || true
    sed -i "s/^max_wal_size = .*/max_wal_size = 512MB/" "$PGDATA/postgresql.conf" 2>/dev/null || true
    sed -i "s/^min_wal_size = .*/min_wal_size = 64MB/" "$PGDATA/postgresql.conf" 2>/dev/null || true
fi

echo "==> Starting PostgreSQL..."
exec runuser -u postgres -- postgres -D "$PGDATA" -c max_connections=40
