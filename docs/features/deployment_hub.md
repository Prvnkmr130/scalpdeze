# Cloud Deployment & Git Operations Hub

The **Cloud Deployment & Git Operations Hub** is an automated, out-of-container deployment workstation integrated directly into the Django Admin interface (`/admin/kalai/broker/deployment-hub/`). It enables operators to inspect live Git repository metadata and trigger robust, zero-downtime Linux host deployments (`git pull` $\to$ `docker compose build & restart` $\to$ migrations $\to$ health verification) directly from their browser while preserving all manual deployment options.

---

## 1. Key Architectural Highlights

* **Out-of-Container Detached Execution**: The deployment process runs on the Linux host OS outside the Docker container via a dedicated systemd daemon and FIFO trigger pipe (`/run/deltazero-deploy.fifo`). Rebuilding and restarting Docker containers **never terminates the deployment process prematurely**.
* **Zero Impact on Manual Deployment**:
  - Running `./deploy.sh` (Interactive Menu) or `./deploy.sh --deploy` in an SSH terminal remains $100\%$ functional and untouched.
  - The web trigger executes `./deploy.sh --deploy` under the hood on the Linux host, guaranteeing absolute behavioral parity.
* **One-Time Dynamic Setup in `install.sh`**:
  - During the one-time `./install.sh` execution, the Linux host automatically generates `/usr/local/bin/deltazero-deploy` and configures the `deltazero-deploy.service` systemd daemon.
* **Live Git Repository Telemetry & Pure-Python Fallback**:
  - Displays active branch, latest commit hash (with copy tool), author, commit message, and relative timestamp.
  - Features a pure-Python fallback (`_read_git_directory_fallback`) that directly parses `.git/HEAD`, `.git/refs/heads/`, and `.git/logs/HEAD`, ensuring metadata availability even when the `git` CLI binary is uninstalled or executing inside restricted environments.
  - **On-Demand Remote Fetching (`git fetch --quiet origin <branch>`)**: Automatically runs a bounded background fetch (`timeout=6s`) against GitHub `origin` before evaluating `git rev-list --left-right --count origin/<branch>...HEAD`. This ensures the synchronization status instantly detects and displays freshly pushed GitHub commits (e.g. `Behind origin/main by X commit(s) — Ready to update`) rather than comparing against stale local cached references.
  - Monitors working tree state (`Clean` vs `Dirty (Uncommitted Changes)`) with interactive "Refresh Git Info" button feedback.
* **Clean-Slate ANSI Terminal Streaming & Auto-Reset**:
  - Automatically truncates prior deployment logs (`open(DEPLOY_LOG_FILE, "w")` / `tee "$LOG_FILE"`) upon starting a new run, preventing stale logs from mixing into new deployment traces.
  - Frontend console resets terminal text, clears polling timers, and restarts byte-offset tracking (`logOffset = 0`) upon dispatch.
  - Streams real-time terminal output directly into the Django Admin dashboard using lightweight byte-offset polling ($< 2$ KB per poll).
* **Security, CSRF Resilience & Privilege Gating**:
  - Triggering deployments requires **both** an active Django staff session and `is_superuser == True` (or a cryptographic constant-time M2M HMAC key via `secrets.compare_digest`).
  - Endpoint `api/deploy/trigger/` is decorated with `@csrf_exempt` to support M2M API calls, while browser requests include `X-CSRFToken` extracted from hidden form inputs or cookies.
  - Defensive frontend parsing inspects `Content-Type: application/json` before invoking `.json()`, cleanly surfacing HTTP error statuses (e.g. 500, 502, 403) instead of cryptic `JSON.parse` client syntax crashes.
  - Branch names are validated against a strict regex whitelist (`^[a-zA-Z0-9._\-/]+$`) to eliminate command injection vectors.
  - Sensitive environment secrets (`DJANGO_SECRET_KEY`, database passwords, API secrets) are automatically scrubbed from terminal outputs before streaming to the browser.
* **Concurrency Protection & Self-Healing Mutex Lock**:
  - An atomic file lock (`/tmp/deltazero-deploy.lock`) prevents multiple concurrent builds, rejecting secondary requests with HTTP `409 Conflict`.
  - **Self-Healing Error Recovery**: If an execution terminates abnormally, `deploy.sh` traps `EXIT INT TERM` signals to remove the lock immediately. Simultaneously, `get_deployment_status()` inspects the tail of `/var/log/deltazero-deploy.log` for terminal error tokens (`❌ Error`, `fatal:`, etc.) to automatically release lingering locks and switch the dashboard state from `RUNNING` to `FAILED`.
* **Database Readiness Wait Loop (`deploy.sh`)**:
  - Container restarts execute an active wait loop polling `pg_isready -h 127.0.0.1 -p 5432` before running `manage.py migrate`, completely eliminating `FATAL: the database system is starting up` race condition failures.
* **Multi-User Git Security & Dubious Ownership Immunity**:
  - Out-of-container daemons executing under `root` operate seamlessly on repositories owned by non-root users (`Deltauser`) via inline `git -c safe.directory=*` directives and system-wide Git configuration.
* **Automated Post-Deployment Docker System Prune**:
  - Every deployment automatically executes a volume-safe `docker system prune -a -f` and `docker builder prune -f` immediately after containers are healthy. This automatically cleans up superseded image versions, dangling `<none>` layers, and build cache from disk without touching named persistent volumes (`pgdata`, `certbot_etc`).

---

## 2. Web Interface Walkthrough (`/admin/kalai/broker/deployment-hub/`)

| Section | Description |
| :--- | :--- |
| **🌿 Live Git Repository Information** | Displays current branch, latest commit SHA, commit author, commit subject, dirty status, and tracking status against GitHub `origin`. Includes an interactive "Refresh Git Info" button with real-time feedback. |
| **🚀 Deployment Control Bar** | Select the target branch (`main`, custom feature branches) and choose between **⚡ Fast Restart (~2s)** (live host volume reload for Python code/strategies) or **🔨 Full Docker Rebuild (~30s)** (for dependency or Dockerfile changes). |
| **Terminal Console Viewer** | Dark HTML5 terminal console displaying step-by-step stdout (`git pull` $\to$ `docker compose restart app` or `build` $\to$ migrations $\to$ `/health/` probe). Automatically clears previous log traces when starting a new deployment. |
| **Status Chip Bar** | Dynamic color-coded status indicator: `IDLE` (gray), `RUNNING` (pulsing amber), `SUCCESS` (green), `FAILED` (red). |

---

## 3. URL Routing & Endpoints

All deployment routes are registered under `kalai/urls.py` and `AccountAdmin.get_urls()`:

| URL Pattern | View Function | Method | Auth Level | Description |
| :--- | :--- | :--- | :--- | :--- |
| `kalai/broker/deployment-hub/` | `deployment_hub_view` | `GET` | Staff Only | Renders the Django Admin Deployment Hub operations console. |
| `api/deploy/git-info/` | `git_info_api` | `GET` | Staff / M2M | Returns live Git repository metadata as JSON. |
| `api/deploy/trigger/` | `trigger_deploy_api` | `POST` | Superuser / M2M (`@csrf_exempt`) | Dispatches an out-of-container host deployment. |
| `api/deploy/status/` | `deploy_status_api` | `GET` | Staff / M2M | Streams incremental terminal logs and execution state. |

---

## 4. Host Daemon Implementation Mechanics

```bash
# 1. Systemd Service Configuration (/etc/systemd/system/deltazero-deploy.service)
[Unit]
Description=DeltaZero26 Out-of-Container Deployment Host Daemon
After=network.target docker.service

[Service]
Type=simple
ExecStart=/usr/local/bin/deltazero-deploy-daemon
Restart=always
RestartSec=3
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```
