# Environment & Deployment

This document covers the required dependencies, environment configurations, database optimizations, security architecture, and deployment strategies for DeltaZero26.

---

## Dependencies

DeltaZero26 is built on a modern Python 3.14+ stack. The core dependencies, as defined in `pyproject.toml`, are grouped by their functionality:

- **Web Framework:** `django>=6.1`, `channels>=4.2` (for ASGI routing), `daphne>=4.1`, `uvicorn[standard]`
- **Database & Data Handling:** `psycopg[binary,pool]` (synchronous DB access & connection pooling), `asyncpg` (asynchronous DB access for engines), `polars` & `pyarrow` (high-performance vector strategy processing), `openpyxl` (for Excel config ingestion and telemetry exports via pure Polars).
- **Background Tasks:** `django-q2` (handling scheduled jobs, log clearing, etc.)
- **External Communications:** `aiohttp` (async REST requests), `python-socketio[asyncio_client]`, `kiteconnect` (Zerodha broker API integration), `websocket-client` (Kotak Neo HSM WebSocket streaming), `requests` (sync REST requests).
- **Security & Utilities:** `python-dotenv`, `pyotp` (for TOTP generation), `cryptography` (Ed25519 asymmetric signatures), `orjson` (fast JSON serialization).

---

## Environment Variables (`.env`)

The application requires an environment file (`.env`) placed in the root directory to configure the deployment. 

> [!IMPORTANT]
> Never commit your `.env` file to version control. Use the provided `.env.example` as a template.

Key environment variables include:

- `DJANGO_SECRET_KEY`: Security key for Django sessions and cryptographic signing.
- `M2M_SERVER_KEY`: Dedicated API key for programmatic Machine-to-Machine endpoints (`/api/export/*`), decoupling programmatic export access from Django's core session key.
- `SSL_VERIFY`: Boolean (`true`/`false`) or path to custom CA certificate bundle for validating outbound remote HTTPS requests.
- `ALGO_LOOP_INTERVAL_SECONDS`: Execution interval for the synchronous sequential strategy runner (default: `2.0` seconds).
- `APP_MODE`: Set to `production` in production (`debug` for local testing).
- `ADMIN_URL_PATH`: Obfuscated admin path prefix (e.g. `d4f8g9h2j1m5k8p3`).
- `DOMAIN_NAME`: The base domain name (e.g., `prvntrde.net.in`), used by Nginx and Certbot for SSL.
- `BIND_ADDRESS` / `BIND_PORT`: Used by Uvicorn within supervisord to bind the ASGI server (defaults: `0.0.0.0`, `8000`).
- `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`: PostgreSQL connection credentials.
- `ALGO_INFO_SYNC_CYCLES`: Trading loop count between AlgoInfo state and candle table database syncs in production (default: `15` loops ≈ 30s; `0` or debug mode forces instant sync every cycle).
- `POLARS_MAX_THREADS`: Caps the thread pool size for Polars vector operations (default: `2`).
- `OPENBLAS_NUM_THREADS` / `OMP_NUM_THREADS`: Set to `1` on Windows / resource-constrained instances to prevent OpenBLAS thread allocation memory issues.

---

## Security Architecture
 
 - **Standard Django Security Stack**:
   `algo_trading.settings` enables standard `SecurityMiddleware`, `SessionMiddleware`, `CommonMiddleware`, `CsrfViewMiddleware`, `AuthenticationMiddleware`, `MessageMiddleware`, and `XFrameOptionsMiddleware` (`SAMEORIGIN`).
 - **Dedicated M2M Authentication (`M2M_SERVER_KEY`)**:
   API endpoints (`/api/export/*`) authenticate programmatic clients via a dedicated `M2M_SERVER_KEY` header (`X-Server-Key` or `Authorization: Bearer <KEY>`) compared with `secrets.compare_digest`, avoiding exposure of `DJANGO_SECRET_KEY`.
 - **Strict TLS Certificate Verification**:
   Outbound synchronization requests (`fetch_remote_data_view`) enforce validated TLS (`verify=SSL_VERIFY`), preventing MitM attacks on data feeds.
 - **Strict OAuth Login CSRF Nonce Verification**:
   All OAuth redirects generate cryptographically secure 32-byte nonces (`oauth_state`) saved in session. Callbacks strictly abort execution with an error upon any state token mismatch.
 - **Safe SQL Parameterization**:
   High-speed Postgres bulk exports (`COPY`) use `psycopg.sql.SQL` with bounded interval validation.
 - **Admin UI Masking**:
   `AccountAdminForm` masks `api_secret`, `totp_secret`, and `refresh_token` using `forms.PasswordInput`.

---

## Docker & PostgreSQL Configuration

### `Dockerfile`
The [`Dockerfile`](../Dockerfile) utilizes an optimized multi-stage build process with Astral's official `uv` binary and BuildKit cache mounts:
1. **Stage 1: Builder (Fast Dependency Synchronization):**
   - Uses `python:3.14-slim`.
   - Directly copies the precompiled `uv` binary (`COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/`) in $< 0.1\text{s}$, eliminating `pip` installation overhead.
   - Copies `pyproject.toml` and `uv.lock` together, executing `uv sync --frozen --no-install-project --no-dev` using BuildKit cache mounts (`--mount=type=cache,target=/root/.cache/uv`). Dependencies are locked cryptographically with zero PyPI re-resolution latency.
   - Installs the project source in a dedicated cached layer with bytecode compilation (`UV_COMPILE_BYTECODE=1`).
2. **Stage 2: Runtime (Monolith Django + PostgreSQL Container):**
   - Installs OS-level dependencies (Nginx with HTTP/2 and static open-file cache, PostgreSQL 18, Supervisord, `git`) with BuildKit APT cache mounts (`--mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked`), preventing redundant package re-downloads across builds.
   - Sets runtime allocator constraints: `MALLOC_ARENA_MAX=2` and `MALLOC_TRIM_THRESHOLD_=131072` to prevent glibc heap fragmentation across multithreaded Python workers (saving ~200–400 MB RAM).
   - Registers `git config --system --add safe.directory '*'` to permit in-container telemetry tools to query host-mounted `.git` metadata seamlessly across users.
   - Copies the pre-built `.venv` from the builder stage.
   - Generates fallback self-signed SSL certificates, runs `collectstatic`, and configures `/usr/bin/supervisord` as PID 1.
   - Subsequent code builds complete in **5–15 seconds** (versus 6+ minutes cold builds).

### `pg_entrypoint.sh` & Database Performance
- **`max_connections = 40`**: Optimized to support all active background daemons (Uvicorn, WebSocket Engine, Django-Q worker, Algo Engine) with plenty of headroom while capping per-backend memory overhead to keep PostgreSQL's total memory footprint under ~150 MB.
- **`shared_buffers = 128MB` & `effective_cache_size = 256MB`**: Sized specifically for 1–2 vCPU / 2–4 GB RAM cloud instances to leave maximum physical RAM available for real-time Polars trading calculations.
- **Port Mapping (`127.0.0.1:5432:5432`)**: Bridges PostgreSQL strictly to the host machine for running local test suites (`pytest`) and standalone strategy execution without exposing the port to public WAN.

### `nginx.conf` & Reverse Proxy Performance
- **Static Asset Serving & Caching:** Requests to `/static/` bypass dynamic rate limiters and return `Cache-Control: public, max-age=86400`, preventing browser 503 errors and saving server disk I/O.
- **Dynamic Rate Limiting:** Application routes (`location /`) apply `limit_req zone=req_limit burst=100 nodelay` (`rate=50r/s`), returning RFC 429 (`limit_req_status 429;`) upon limit exhaustion.
- **Proxy Buffer Sizing:** Configured with `proxy_buffer_size 16k;`, `proxy_buffers 8 16k;`, and `proxy_busy_buffers_size 32k;` to support large Django cookie and session headers without disk overflow.
- **Direct Health Check Access:** Port 80 and 443 both route `/health/` directly to Uvicorn, permitting container and script health verification without SSL redirection failures.

### `docker-compose.yml`
The [`docker-compose.yml`](../docker-compose.yml) orchestrates the main application container and an ephemeral `certbot` container for Let's Encrypt certificates.
- **Build Network Mode**: Uses `network: host` under `app.build` to prevent bridge network DNS timeouts on WSL2/Docker Desktop.
- **Volumes:** Persists PostgreSQL data (`postgres_data`), logs (`logs`), static assets (`staticfiles`), and Let's Encrypt certificates (`certbot_certs`, `certbot_webroot`).

---

## Memory & Resource Optimization Architecture

DeltaZero runs 6 concurrent services inside the container (`postgresql`, `uvicorn`, `algo_engine`, `ws_engine`, `django_q`, `nginx`). To maintain low memory footprints on resource-constrained cloud VMs (1–2 vCPU, 2–4 GB RAM):

1. **Why Low Swap Usage is Desirable**:
   - Trading processes touch memory constantly (incoming ticks every millisecond, loop cycles every 2 seconds). The Linux kernel classifies this as **ACTIVE (hot)** memory.
   - Hot memory is never paged out to swap while physical RAM is available.
   - Paging active trading heap to disk would introduce 50–200ms page fault latency, causing missed ticks and execution timeouts. Low swap utilization is normal and healthy.
2. **Glibc Allocator Arena Capping**:
   - Set `MALLOC_ARENA_MAX=2` and `MALLOC_TRIM_THRESHOLD_=131072` in [`Dockerfile`](../Dockerfile) to prevent multithreaded virtual arena sprawl and force glibc to return freed heap to the OS.
3. **Django Query Cache Resetting**:
   - `algo_engine` invokes `django.db.reset_queries()` alongside periodic `gc.collect()` every 10 cycles to purge accumulated query objects.
   - `APP_MODE=production` in `.env` disables Django's in-memory query history logging.
4. **Bounded Channel Buffers**:
   - `InMemoryChannelLayer` in `settings.py` enforces `capacity: 100` and `expiry: 10` to avoid unbounded broadcast queuing in RAM.
5. **Lean PostgreSQL Profile**:
   - Capped `max_connections = 40`, `shared_buffers = 128MB`, `work_mem = 4MB` in `pg_entrypoint.sh`.

---

## Deployment Scripts

### 1. `install.sh`
Run during initial server provisioning or optimization passes (as root via `sudo ./install.sh`):
- **OS Leaning & Bloatware Pruning:** Automatically stops, disables, and masks unnecessary background services (`snapd`, `multipathd`, `packagekit`, `whoopsie`, `apport`, `modemmanager`, `udisks2`, `cups`, `avahi-daemon`, etc.) and caps virtual consoles (`NAutoVTs=1`) to eliminate idle RAM consumption on headless VPS nodes.
- **RAM & In-Memory Compressed Swap:** Configures ZRAM compressed swap (`zram-tools`, `zstd` algorithm, 60% RAM, Priority 100) and ensures an idempotent 2 GB `/swapfile` disk fallback (Priority 10) is active.
- **Systemd Journal RAM Limits:** Caps `systemd-journald` memory (`RuntimeMaxUse=20M`) and storage (`SystemMaxUse=50M`) with automatic log vacuuming.
- **Unified Kernel Virtual Memory Tuning:** Enforces `/etc/sysctl.d/99-deltazero.conf` (`vm.swappiness=30`, `vm.vfs_cache_pressure=150`, `vm.dirty_background_ratio=5`, `vm.dirty_ratio=10`, `vm.page-cluster=0`) and network security hardening.
- **Server & SSH Hardening:** Restricts root login, caps PAM auth tries (`MaxAuthTries 3`), secures daemon configs, configures `fail2ban` permanent SSH jail, and secures `/dev/shm`.
- **Automated Docker Permissions:** Configures `/etc/systemd/system/docker.socket.d/override.conf` (`SocketMode=0666`) and adds the non-root admin to the `docker` group, permanently preventing `docker.sock` permission denied errors.
- **Multi-User Git Safe Directories:** Registers `git config --system --add safe.directory "*"` across the OS to prevent multi-user "dubious ownership" errors.
- **Host Deployment Automation Daemon:** Configures the out-of-container `/usr/local/bin/deltazero-deploy` wrapper, systemd daemon (`deltazero-deploy.service`), and FIFO trigger pipe (`/run/deltazero-deploy.fifo`).
- **Idempotency & Non-Destructive Safety:** Verifies existing configurations, packages, and running Docker containers before taking action. Safely bypasses tasks already completed, protecting active trading engines from unwanted restarts.
- **Execution & Resource Delta Report:** Captures pre- and post-run RAM, Swap, and process metrics, prints a comprehensive summary table, and archives the report to `/var/log/deltazero-install-report.log`.

### 2. `deploy.sh`
The primary tool for managing application updates and container lifecycles:

| Command | Action |
| :--- | :--- |
| `./deploy.sh` | Interactive selection menu. |
| `./deploy.sh --deploy` | Pulls latest code, auto-relaxes socket permissions, builds/restarts Docker containers, runs migrations, automatically runs volume-safe `docker system prune -a -f` to clean unwanted images, verifies `/health/`, and non-interactively ensures superuser verification. |
| `./deploy.sh --reinstall` | Complete wipe of all Docker containers, networks, volumes, and images (`down -v --rmi all`), then rebuilds and redeploys from scratch. |
| `./deploy.sh --prune` | Explicit smart deploy + volume-safe `docker system prune -a -f` (included by default). |
| `./deploy.sh --no-prune` | Skips automatic Docker system prune during deployment. |
| `./deploy.sh --prune-only` | Runs `docker system prune -a -f` without rebuilding. |
| `./deploy.sh --restart-only` | Fast restart of containers without rebuilding images. |
| `./deploy.sh --no-pull` | Skips `git pull` (essential when testing or deploying local unpushed code). |
| `./deploy.sh -b <branch>` | Specifies a custom git branch to deploy (default: `main`). |

**Key Resilience Enhancements in `deploy.sh`:**
- **Automated Post-Deploy Image Pruning**: Whenever code is deployed or updated from GitHub, `deploy.sh` automatically runs volume-safe `docker system prune -a -f` and `docker builder prune -f` after containers are running. This reclaims disk space from superseded image versions, dangling `<none>` images, and build layers, preventing VPS disk exhaustion while guaranteeing that named volumes (database and certificates) remain completely untouched.
- **Database Startup Race Guard (`pg_isready`)**: Container restarts and rebuilds execute an active loop checking `pg_isready -h 127.0.0.1 -p 5432` before running `manage.py migrate`, completely eliminating `FATAL: the database system is starting up` errors during service spin-up.
- **Auto-Healing Docker Permissions:** `ensure_docker_access()` checks `/var/run/docker.sock` before execution, automatically running `sudo chmod 666` and falling back to `sudo docker compose` if unprivileged access is denied.
- **Auto-Healing Repository & `.git` Permissions:** Restores repository user ownership (`chown -R $USER:$USER .git`) and write permissions (`chmod -R u+rwX .git`) before pulling, eliminating `insufficient permission for adding an object to repository database .git/objects` errors caused by prior sudo/root commands.
- **Transparent SSH & Active Agent Resolution:** Attempts standard Git fetch first using the shell's active SSH configuration and `ssh-agent`, avoiding unnecessary passphrase prompts. Falls back to explicit deploy keys (`$SSH_KEY`) only when default fetch fails.
- **Dubious Ownership Immunity:** Enforces `git -c safe.directory=*` inline on all Git fetch/pull/status operations.
- **Orphan Lock Protection:** Traps `EXIT INT TERM` signals to delete `/tmp/deltazero-deploy.lock`, ensuring the UI never gets permanently stuck in `RUNNING` if a pull fails.
- **Clean-Slate Deployment Logs:** Truncates `/var/log/deltazero-deploy.log` at the beginning of each deployment run (`tee "$LOG_FILE"` / `open(DEPLOY_LOG_FILE, "w")`), preventing stale terminal output from preceding jobs from confusing operators.
- **Non-Interactive Deployments:** Eliminates interactive password prompts during regular `--deploy` cycles by reading `SUPERUSER_PASSWORD` directly from `.env` or database state.

---

## SSL & Domain Configuration

For complete instructions on domain DNS records, automated Let's Encrypt certificates, dry-run testing, and auto-renewal cron schedules, consult the **[Certbot, SSL & Domain Configuration Guide](features/certbot_and_ssl.md)**.
