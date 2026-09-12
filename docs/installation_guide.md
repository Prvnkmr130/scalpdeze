# Complete Server Installation & Deployment Guide

This guide provides a comprehensive, step-by-step walkthrough for provisioning a new cloud VPS server, configuring permissions, setting up passphrase-free GitHub SSH Deploy Keys, and running automated container deployments for **DeltaZero26**.

---

## 1. Prerequisites

* **Operating System**: Ubuntu 22.04 / 24.04 LTS or Debian 12 (64-bit x86_64).
* **Hardware Sizing**:
  * Minimum: 1 vCPU, 2 GB RAM (or 1 GB RAM + 2 GB Swap).
  * Recommended: 2 vCPUs, 4 GB RAM.
* **Network**: Open inbound ports `22` (SSH), `80` (HTTP), `443` (HTTPS).
* **GitHub Access**: Admin access to the repository [Prvnkmr130/deltazero26](https://github.com/Prvnkmr130/deltazero26) to add Deploy Keys.

---

## 2. Initial Setup & Server Hardening (`install.sh`)

### Step 2.1: Clone Repository
Log into your server via SSH as `root` (or your initial provider user):
```bash
git clone https://github.com/Prvnkmr130/deltazero26.git
cd deltazero26
```

### Step 2.2: Make Executable & Run Installer
```bash
chmod +x install.sh
sudo ./install.sh
```

> [!TIP]
> **If you see `Permission denied (os error 13)`**:
> Ensure execute permissions are granted via `chmod +x install.sh`, or execute with `sudo bash install.sh`.

### What `install.sh` Automatically Configures:
1. **OS Leaning & Bloatware Pruning**: Stops, disables, and permanently masks heavy background services unnecessary on a headless trading node (`snapd`, `multipathd`, `packagekit`, `whoopsie`, `apport`, `modemmanager`, `udisks2`, `cups`, `avahi-daemon`, etc.) and caps virtual console allocation (`NAutoVTs=1`) to eliminate idle RAM usage.
2. **RAM & Compressed ZRAM Swap**: Deploys in-memory compressed swap via `zram-tools` (`zstd` algorithm, 60% RAM, Priority 100) paired with an idempotent 2 GB disk `/swapfile` fallback (Priority 10) for maximum memory efficiency and OOM prevention.
3. **Journald RAM & Storage Limits**: Restricts systemd journal memory (`RuntimeMaxUse=20M`) and persistent disk storage (`SystemMaxUse=50M`), preventing system logging bloat.
4. **Unified Kernel Virtual Memory & Security Tuning**: Applies `/etc/sysctl.d/99-deltazero.conf` (`vm.swappiness=30`, `vm.vfs_cache_pressure=150`, `vm.page-cluster=0`, `vm.dirty_background_ratio=5`, and network hardening).
5. **System & SSH Access Hardening**: Configures SSH daemon security directives (`MaxAuthTries 3`, disables password root login, restricts PAM), configures `fail2ban` permanent SSH jail, and secures `/dev/shm`.
6. **Dedicated Non-Root Admin User**: Configures user `Deltauser` with `sudo` and `docker` privileges.
7. **Permanent Docker Socket Permissions**: Deploys a systemd socket override (`/etc/systemd/system/docker.socket.d/override.conf`) with `SocketMode=0666` so non-root users can interact with Docker without permission denials.
8. **Multi-User Git Security**: Registers `git config --system --add safe.directory "*"` across the OS to prevent multi-user "dubious ownership" fatal errors.
9. **Host Deployment Daemon**: Installs `/usr/local/bin/deltazero-deploy` and starts the `deltazero-deploy.service` background daemon.
10. **VPS Storage Optimization**: Reclaims ext4 reserved root blocks from 5% down to 1% (`tune2fs -m 1`), freeing ~1 GB on small VPS disks.
11. **Idempotency & Rerun Protection**: Verifies existing configurations and active Docker containers before executing. Safe to rerun at any time without disrupting running trading engines or wiping state.
12. **Comprehensive Execution & Resource Delta Report**: Computes initial vs. final RAM, Swap, and process deltas, prints a formatted execution matrix, and saves the audit record to `/var/log/deltazero-install-report.log`.

---

## 3. GitHub SSH Deploy Key Setup (Automated & Passphrase-Free)

For background deployments, cron jobs, and 1-click Django Admin deploys to pull code from GitHub without human prompts, a dedicated SSH Deploy Key with **no passphrase** is required.

### Step 3.1: Generate a Dedicated Deploy Key
Log in or switch to the non-root admin user (`Deltauser`):
```bash
# Generate a dedicated ed25519 key with empty passphrase (-N "")
ssh-keygen -t ed25519 -C "vps-deltazero26-deploy" -N "" -f ~/.ssh/id_ed25519_deploy
```

### Step 3.2: Synchronize Key to Root
Because the background deployment daemon executes as `root`, copy the key to `/root/.ssh/`:
```bash
sudo mkdir -p /root/.ssh
sudo cp ~/.ssh/id_ed25519_deploy* /root/.ssh/
sudo chmod 600 /root/.ssh/id_ed25519_deploy
sudo chmod 644 /root/.ssh/id_ed25519_deploy.pub
```

### Step 3.3: Configure SSH Client Config
Create or update `~/.ssh/config` so Git automatically uses this key when connecting to GitHub:
```bash
cat << 'EOF' >> ~/.ssh/config
Host github.com
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_ed25519_deploy
    IdentitiesOnly yes
EOF
chmod 600 ~/.ssh/config

# Copy config to root as well
sudo cp ~/.ssh/config /root/.ssh/config
sudo chmod 600 /root/.ssh/config
```

### Step 3.4: Add the Public Key to GitHub
1. Display the public key:
   ```bash
   cat ~/.ssh/id_ed25519_deploy.pub
   ```
2. Copy the full line starting with `ssh-ed25519 AAAA...`.
3. Open your GitHub repository in your browser:
   👉 **[github.com/Prvnkmr130/deltazero26](https://github.com/Prvnkmr130/deltazero26) $\to$ Settings $\to$ Deploy Keys**
4. Click **Add deploy key**.
5. Set Title: `VPS Production Deploy Key`
6. Paste the copied public key into the **Key** textarea.
7. *(Optional)* Check **Allow write access** if you plan to push commits or tags from the server.
8. Click **Add key**.

> [!NOTE]
> **Why did GitHub say "Key is already in use"?**
> GitHub does not allow the same public key to be registered both under your personal user account ([github.com/settings/keys](https://github.com/settings/keys)) and as a repository Deploy Key. Generating a dedicated key (`id_ed25519_deploy`) avoids any collisions.

### Step 3.5: Verify SSH Authentication
Test that GitHub accepts your connection:
```bash
ssh -T git@github.com
```
Expected output:
```text
Hi Prvnkmr130/deltazero26! You've successfully authenticated, but GitHub does not provide shell access.
```

---

## 4. Environment Configuration (`.env`)

DeltaZero26 requires a production `.env` file in the project root.

### Step 4.1: Create `.env` from Template
```bash
cp .env.example .env
chmod 600 .env
```

### Step 4.2: Populate Mandatory Secrets
Open `.env` in a text editor (`nano .env`) and set:
```bash
# Core Mode & Host Security
APP_MODE=production
DJANGO_DEBUG=False
DJANGO_SECRET_KEY=<generate_secure_random_string_min_32_chars>
M2M_SERVER_KEY=<generate_distinct_random_string_min_32_chars>
SUPERUSER_PASSWORD=<your_django_admin_password>

# Domain & Web
DOMAIN_NAME=prvntrde.net.in
ALLOWED_HOSTS=localhost,127.0.0.1,<your_server_ip>,prvntrde.net.in

# Database Credentials
POSTGRES_DB=algo_trading_db
POSTGRES_USER=algo_trader
POSTGRES_PASSWORD=<strong_postgres_password>
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
```

---

## 5. Application Deployment (`deploy.sh`)

The application is deployed as a single optimized monolith container running Nginx, PostgreSQL 18, Uvicorn, WebSocket ingestion engines, Supervisord, and the Polars trading engine.

### Run Deployment
```bash
./deploy.sh --deploy
```

### What Happens During `./deploy.sh --deploy`:
1. **Git Safe Directory & Permissions Healing**: Automatically registers repo directory with `git -c safe.directory=*` and restores user ownership/write permissions on `.git/` to prevent `insufficient permission for adding an object to repository database .git/objects` errors.
2. **Transparent Git Pull**: Stashes local uncommitted changes, uses the shell's working SSH configuration and active `ssh-agent` (falling back to explicit deploy keys only if needed), fetches remote changes, and checks out `main` without unnecessary passphrase prompts.
3. **Socket Healing**: Automatically validates `/var/run/docker.sock` and grants `0666` permissions if needed.
4. **Smart Deploy (Fast Restart vs Rebuild)**:
   - **Routine Code Changes** (Python strategies, templates, docs): Since the repository is mounted into the container via host volume (`.:/app`), code changes are reflected immediately. The script skips rebuilding and executes a **lightning-fast container restart in ~2 seconds**.
   - **Core/Dependency Changes** (`Dockerfile`, `pyproject.toml`, `uv.lock`, `docker-compose.yml`, `nginx.conf`, `supervisord.conf`): Automatically triggers a BuildKit build (`5–15s`).
5. **Database Migrations**: Runs `python manage.py migrate --noinput`.
6. **Static File Collection**: Collects Django admin and chart assets into `/app/staticfiles`.
7. **Health Verification**: Pings `http://localhost/health/` until HTTP 200 is confirmed.

---

## 6. Available Deployment Commands

| Command | Purpose |
| :--- | :--- |
| `./deploy.sh` | Interactive CLI menu. |
| `./deploy.sh --deploy` | **Smart Routine Deploy**: Fast restart if only code changed; rebuilds if dependencies/Dockerfile changed; automatically runs volume-safe `docker system prune -a -f` to clean unused images. |
| `./deploy.sh --build` | **Force Rebuild**: Forces full Docker image rebuild regardless of git diff + auto system prune. |
| `./deploy.sh --no-prune` | Skips automatic Docker system prune during deployment. |
| `./deploy.sh --deploy --no-pull` | Deploys local code changes without pulling from GitHub remote. |
| `./deploy.sh --restart-only` | Restarts existing containers in 2 seconds without rebuilding images or pulling code. |
| `./deploy.sh --prune` | Explicit smart deploy + volume-safe `docker system prune -a -f` (included by default). |
| `./deploy.sh --prune-only` | Cleans up unused Docker images and build caches (named volumes are preserved). |
| `./deploy.sh --reinstall` | **Clean Slate**: Stops containers, wipes all volumes/networks (`down -v --rmi all`), and rebuilds fresh. |
| `./deploy.sh -b <branch>` | Pulls and deploys a specific Git branch (e.g. `./deploy.sh --deploy -b feature-test`). |

---

## 7. Django Admin Web Deployment Hub

Once running, deployments can be triggered directly from your browser:

1. Log into Django Admin: `https://<your-domain>/admin/` (or your obfuscated admin URL).
2. Navigate to: **Kalai $\to$ Brokers $\to$ Cloud Deployment & Git Operations Hub** (`/admin/kalai/broker/deployment-hub/`).
3. Inspect live Git metadata: Current branch, latest commit hash, commit message, dirty status.
4. **Choose Target Branch & Build Mode**:
   - **`⚡ Fast Restart (No Rebuild - ~2s)`** *(Default)*: Live-mounts code updates and restarts the app container in seconds.
   - **`🔨 Full Docker Rebuild (--build - ~30s)`**: Executes full Docker BuildKit image build. Use when Python dependencies (`pyproject.toml`, `uv.lock`) or `Dockerfile` change.
5. Click **Start Production Deployment**.
6. Watch real-time terminal output stream line-by-line into the dark console.
7. The dashboard automatically transitions from `RUNNING` $\to$ `SUCCESS` or `FAILED`.

---

## 8. Common Troubleshooting Guide

### 1. `Permission denied (os error 13)`
* **Cause**: Script lacks execution permissions.
* **Fix**: Run `chmod +x install.sh && sudo ./install.sh`.

### 2. `fatal: detected dubious ownership in repository`
* **Cause**: Git prevents `root` or other users from operating on a repository owned by a different user.
* **Fix**: Run:
  ```bash
  sudo git config --system --add safe.directory '*'
  sudo git config --global --add safe.directory '*'
  ```

### 3. `permission denied while trying to connect to the docker API`
* **Cause**: Current user lacks read/write access to `/var/run/docker.sock`.
* **Fix**: Run:
  ```bash
  sudo chmod 666 /var/run/docker.sock
  sudo usermod -aG docker $USER
  newgrp docker
  ```

### 4. `git@github.com: Permission denied (publickey)`
* **Cause**: Public deploy key missing from GitHub repo settings, or key is locked with a passphrase.
* **Fix**:
  1. Remove passphrase: `ssh-keygen -p -f ~/.ssh/id_ed25519`.
  2. Verify public key is in GitHub: **Repo Settings $\to$ Deploy Keys $\to$ Add Deploy Key**.
  3. Verify connection: `ssh -T git@github.com`.

### 5. `error: insufficient permission for adding an object to repository database .git/objects`
* **Cause**: Prior commands executed via `root` or `sudo` (e.g. `sudo ./deploy.sh` or Docker bind-mounts) created files owned by `root` inside `.git/` or `.git/objects`.
* **Fix**: Restore repository ownership to the current user:
  ```bash
  sudo chown -R $USER:$USER ~/deltazero26
  sudo chmod -R u+rwX ~/deltazero26/.git
  ```
  *(Note: The latest `deploy.sh` automatically heals `.git` ownership and permissions before pulling).*

### 6. Status stuck in `RUNNING` on Deployment Hub
* **Cause**: A previous deployment failed or was killed, leaving behind the lock file.
* **Fix**: Run:
  ```bash
  sudo rm -f /tmp/deltazero-deploy.lock /run/deltazero-deploy.lock
  ```
  *(Note: The latest `deploy.sh` and `kalai/deployment.py` automatically clear this lock via exit signal traps).*

---

## 9. VPS Storage Architecture & 25 GB Sizing Best Practices

When deploying on a budget cloud VPS with **25 GB NVMe/SSD storage**, effective database capacity is approximately **12 to 15 GB** after operating system and container overhead.

### Storage Breakdown (25 GB VPS)

| Layer | Allocated Size | Component Description |
| :--- | :--- | :--- |
| **Filesystem Overhead** | ~1.0 – 1.2 GB | Inode tables and ext4 filesystem metadata. |
| **Linux OS Base** | ~3.0 – 4.0 GB | Ubuntu LTS kernel, systemd, base OS libraries. |
| **System Packages & Docker** | ~1.5 – 2.0 GB | Docker CE, Git, build utilities, SSL/Certbot. |
| **App & Monolith Container** | ~2.5 – 3.5 GB | DeltaZero26 monolithic Docker image with Python runtimes & Polars. |
| **Swap & Journal Logs** | ~1.5 – 2.0 GB | 1–2 GB swapfile + systemd journald logs (capped at 100 MB). |
| **OS Safety Buffer** | ~1.0 GB | Headroom preventing root filesystem lockups. |
| **PostgreSQL Database (`/var/lib/postgresql`)** | **~12.0 – 15.0 GB** | **Usable space dedicated for high-frequency tick and log storage.** |

### Retention & Capacity Economics
- **Tick Storage (`kalai_processedtickstore`)**: ~300 bytes per record. Over a 2-day rolling window across active symbols, this uses ~1.5 GB to 3.5 GB.
- **System & Algo Logs (`system_logs`, `kalai_algolog`)**: ~500 MB to 1.5 GB over 2 days.
- **Twice-Daily Scheduled Cleanup**: Runs pre-market (08:30 AM IST) and post-market midnight (12:00 AM IST) with 5,000-row throttled chunks to continuously recycle dead tuples without lock contention.
- **WAL Protection**: PostgreSQL Write-Ahead Logs are clamped to `max_wal_size = 1GB` and `min_wal_size = 256MB` in `pg_entrypoint.sh` and `install.sh`.

