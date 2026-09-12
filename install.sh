#!/bin/bash

# ==============================================================================
# DeltaZero26 App Installer, Server Hardening & OS Lean Optimization
# Purpose : Automate server hardening, reduce RAM usage, eliminate bloatware,
#           configure out-of-container deployment daemon, and deploy app safely.
# Usage   : Run as root: sudo ./install.sh
# ==============================================================================

set -o pipefail

# ------------------------------------------------------------------------------
# 0. Root Check & Execution Initialization
# ------------------------------------------------------------------------------
if [ "$(id -u)" -ne 0 ]; then
    echo "❌ Error: This script must be run as root (e.g. sudo ./install.sh)" >&2
    exit 1
fi

START_TIME=$(date +%s)
REPORT_FILE="/var/log/deltazero-install-report.log"

# Color Codes for Terminal Output
CLR_RESET="\033[0m"
CLR_BOLD="\033[1m"
CLR_GREEN="\033[32m"
CLR_YELLOW="\033[33m"
CLR_RED="\033[31m"
CLR_CYAN="\033[36m"
CLR_BLUE="\033[34m"

# Task Tracking Registry
declare -a TASK_NAMES=()
declare -a TASK_STATUSES=()
declare -a TASK_DETAILS=()

record_task() {
    local name="$1"
    local status="$2" # EXECUTED, SKIPPED, FAILED
    local details="$3"
    TASK_NAMES+=("$name")
    TASK_STATUSES+=("$status")
    TASK_DETAILS+=("$details")
}

# Logging Helpers
print_section() {
    echo ""
    echo -e "${CLR_BLUE}================================================================================${CLR_RESET}"
    echo -e "${CLR_BOLD}${CLR_CYAN}  $1${CLR_RESET}"
    echo -e "${CLR_BLUE}================================================================================${CLR_RESET}"
    echo ""
}

log_info() {
    echo -e "  [INFO] $1"
}

log_success() {
    echo -e "  ${CLR_GREEN}[PASS] $1${CLR_RESET}"
}

log_skip() {
    echo -e "  ${CLR_YELLOW}[SKIP] $1${CLR_RESET}"
}

log_warn() {
    echo -e "  ${CLR_YELLOW}[WARN] $1${CLR_RESET}"
}

log_error() {
    echo -e "  ${CLR_RED}[FAIL] $1${CLR_RESET}"
}

backup_file() {
    if [ -f "$1" ]; then
        local bkp="$1.$(date +%Y%m%d_%H%M%S).bak"
        cp "$1" "$bkp"
        log_info "Backup created: $bkp"
    fi
}

# System Metric Capture Helper
capture_system_metrics() {
    local mem_line
    mem_line=$(free -m | awk '/^Mem:/{print $2, $3, $4, $7}')
    METRIC_MEM_TOTAL=$(echo "$mem_line" | awk '{print $1}')
    METRIC_MEM_USED=$(echo "$mem_line" | awk '{print $2}')
    METRIC_MEM_FREE=$(echo "$mem_line" | awk '{print $3}')
    METRIC_MEM_AVAIL=$(echo "$mem_line" | awk '{print $4}')
    
    local swap_line
    swap_line=$(free -m | awk '/^Swap:/{print $2, $3}')
    METRIC_SWAP_TOTAL=$(echo "$swap_line" | awk '{print $1}')
    METRIC_SWAP_USED=$(echo "$swap_line" | awk '{print $2}')
    
    METRIC_PROC_COUNT=$(ps -ef | wc -l)
}

# Capture Baseline Metrics
capture_system_metrics
INIT_MEM_TOTAL=$METRIC_MEM_TOTAL
INIT_MEM_USED=$METRIC_MEM_USED
INIT_MEM_FREE=$METRIC_MEM_FREE
INIT_MEM_AVAIL=$METRIC_MEM_AVAIL
INIT_SWAP_TOTAL=$METRIC_SWAP_TOTAL
INIT_SWAP_USED=$METRIC_SWAP_USED
INIT_PROC_COUNT=$METRIC_PROC_COUNT

echo ""
echo -e "${CLR_BOLD}================================================================================${CLR_RESET}"
echo -e "${CLR_BOLD}      DELTAZERO26 SERVER HARDENING, LEAN OS & RAM OPTIMIZER                     ${CLR_RESET}"
echo -e "${CLR_BOLD}================================================================================${CLR_RESET}"
echo "  Started at           : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "  Target Hostname      : $(hostname)"
echo "  Kernel Version       : $(uname -r)"
echo "  Baseline RAM Used    : ${INIT_MEM_USED} MB / ${INIT_MEM_TOTAL} MB (${INIT_MEM_AVAIL} MB available)"
echo "  Baseline Active Swap : ${INIT_SWAP_USED} MB / ${INIT_SWAP_TOTAL} MB"
echo "  Baseline Processes   : ${INIT_PROC_COUNT} active"
echo "================================================================================"

# ------------------------------------------------------------------------------
# 1. Interactive Configuration / Pre-flight Questionnaire
# ------------------------------------------------------------------------------
admin_username="Deltauser"
create_user="n"
admin_password=""
disable_root="n"

# Check if admin user already exists
if id "$admin_username" &>/dev/null; then
    log_info "Admin user '$admin_username' already exists. Re-using existing user without clobbering."
    create_user="n"
else
    read -p "Do you want to create a non-root admin user '$admin_username' with sudo/docker privileges? (y/n) [y]: " input_create_user
    input_create_user=${input_create_user:-y}
    if [[ "$input_create_user" =~ ^[Yy]$ ]]; then
        create_user="y"
        read -s -p "Enter password for '$admin_username' / Django Superuser: " admin_password
        echo
        if [[ -n "$admin_password" ]]; then
            SUPERUSER_PASSWORD="$admin_password"
        fi
    fi
fi

# Check if SSH PermitRootLogin is already hardened
if grep -qE "^PermitRootLogin\s+(no|prohibit-password)" /etc/ssh/sshd_config 2>/dev/null; then
    log_info "SSH root login is already restricted in /etc/ssh/sshd_config."
else
    read -p "Do you want to disable root SSH login completely (y) or allow key-only login (n)? (y/n) [n]: " input_disable_root
    disable_root=${input_disable_root:-n}
fi

# Check Django superuser password in environment or .env
if [[ -z "$SUPERUSER_PASSWORD" ]]; then
    if [ -f ./.env ] && grep -q "^DJANGO_SUPERUSER_PASSWORD=" ./.env 2>/dev/null; then
        SUPERUSER_PASSWORD=$(grep "^DJANGO_SUPERUSER_PASSWORD=" ./.env | cut -d '=' -f2- | tr -d ' "' | tr -d "'")
        log_info "Detected existing Django Superuser Password in .env file."
    else
        read -s -p "Enter Django Superuser Password (for default superuser 'prvnkumar130' or press Enter to skip): " input_su_pass
        echo
        if [[ -n "$input_su_pass" ]]; then
            SUPERUSER_PASSWORD="$input_su_pass"
        fi
    fi
fi
export SUPERUSER_PASSWORD

echo ""
log_info "Configuration captured. Starting automated idempotent provisioning..."
sleep 2

# ------------------------------------------------------------------------------
# 2. Non-Root Admin User Setup (Idempotent)
# ------------------------------------------------------------------------------
print_section "PHASE 1: NON-ROOT ADMIN USER & ENVIRONMENT PERMISSIONS"

if id "$admin_username" &>/dev/null; then
    # Verify groups
    usermod -aG sudo "$admin_username" 2>/dev/null || true
    usermod -aG docker "$admin_username" 2>/dev/null || true
    log_skip "User '$admin_username' already exists. Ensured sudo and docker group memberships."
    record_task "Admin User Setup" "SKIPPED" "User '$admin_username' already exists and configured"
elif [[ "$create_user" =~ ^[Yy]$ ]]; then
    useradd -m -s /bin/bash "$admin_username"
    if [[ -n "$admin_password" ]]; then
        echo "$admin_username:$admin_password" | chpasswd
    fi
    usermod -aG sudo "$admin_username" 2>/dev/null || true
    usermod -aG docker "$admin_username" 2>/dev/null || true
    
    USER_HOME=$(eval echo ~"$admin_username")
    SSH_DIR="$USER_HOME/.ssh"
    if [ ! -d "$SSH_DIR" ]; then
        mkdir -p "$SSH_DIR"
        chown -R "$admin_username":"$admin_username" "$SSH_DIR"
        chmod 700 "$SSH_DIR"
    fi
    log_success "Created non-root admin user '$admin_username' with sudo and docker privileges."
    record_task "Admin User Setup" "EXECUTED" "Created '$admin_username' with sudo/docker"
else
    log_skip "Skipped creating non-root admin user."
    record_task "Admin User Setup" "SKIPPED" "User creation declined"
fi

# Configure Git safe directory globally to avoid multi-user ownership blocks
git config --system --add safe.directory "*" 2>/dev/null || true
git config --global --add safe.directory "$(pwd)" 2>/dev/null || true
git config --global --add safe.directory "*" 2>/dev/null || true
log_success "Git safe directory exceptions registered system-wide."

# Permanent Docker Socket permissions (0666) so non-root and root operate seamlessly
mkdir -p /etc/systemd/system/docker.socket.d
if [ ! -f /etc/systemd/system/docker.socket.d/override.conf ]; then
    cat > /etc/systemd/system/docker.socket.d/override.conf << 'EOF'
[Socket]
SocketMode=0666
EOF
    systemctl daemon-reload 2>/dev/null || true
    systemctl restart docker.socket 2>/dev/null || true
    log_success "Configured permanent Docker socket mode 0666."
    record_task "Docker Socket Permissions" "EXECUTED" "Configured SocketMode=0666 override"
else
    log_skip "Docker socket override (0666) already in place."
    record_task "Docker Socket Permissions" "SKIPPED" "SocketMode=0666 already configured"
fi
chmod 666 /var/run/docker.sock 2>/dev/null || true

# ------------------------------------------------------------------------------
# 3. System Packages & Base Security Tools (Idempotent)
# ------------------------------------------------------------------------------
print_section "PHASE 2: SYSTEM PACKAGE MANAGEMENT & SECURITY TOOLS"

REQUIRED_PACKAGES=("fail2ban" "ufw" "auditd" "chrony" "zram-tools" "apparmor" "apparmor-utils" "libpam-pwquality" "acct")
MISSING_PACKAGES=()

for pkg in "${REQUIRED_PACKAGES[@]}"; do
    if ! dpkg -s "$pkg" &>/dev/null; then
        MISSING_PACKAGES+=("$pkg")
    fi
done

if [ ${#MISSING_PACKAGES[@]} -eq 0 ]; then
    log_skip "All required base packages (${REQUIRED_PACKAGES[*]}) are already installed."
    record_task "Package Installation" "SKIPPED" "All required base packages already present"
else
    log_info "Missing packages detected: ${MISSING_PACKAGES[*]}. Updating repositories and installing..."
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y "${MISSING_PACKAGES[@]}"
    log_success "Installed missing packages: ${MISSING_PACKAGES[*]}."
    record_task "Package Installation" "EXECUTED" "Installed: ${MISSING_PACKAGES[*]}"
fi

# Clean package caches to reclaim storage immediately
apt-get clean 2>/dev/null || true
rm -rf /var/lib/apt/lists/* 2>/dev/null || true
log_success "Apt caches cleaned to conserve storage."

# Ensure host Nginx/Apache are disabled so Docker monolith container can bind 80/443
systemctl stop nginx apache2 2>/dev/null || true
systemctl disable nginx apache2 2>/dev/null || true

# ------------------------------------------------------------------------------
# 4. SSH & Access Hardening (Idempotent)
# ------------------------------------------------------------------------------
print_section "PHASE 3: SSH & ACCESS HARDENING"

SSHD_CONFIG="/etc/ssh/sshd_config"
SSH_NEEDS_RESTART=false

if [ -f "$SSHD_CONFIG" ]; then
    if grep -q "^MaxAuthTries 3" "$SSHD_CONFIG" && grep -q "^X11Forwarding no" "$SSHD_CONFIG"; then
        log_skip "SSH daemon configuration already contains hardened directives."
        record_task "SSH Hardening" "SKIPPED" "Already hardened (MaxAuthTries 3, X11Forwarding no)"
    else
        backup_file "$SSHD_CONFIG"
        sed -i 's/^#*PasswordAuthentication.*/PasswordAuthentication yes/' "$SSHD_CONFIG"
        
        if [[ "$disable_root" =~ ^[Yy]$ ]]; then
            sed -i 's/^#*PermitRootLogin.*/PermitRootLogin no/' "$SSHD_CONFIG"
        else
            sed -i 's/^#*PermitRootLogin.*/PermitRootLogin prohibit-password/' "$SSHD_CONFIG"
        fi
        
        sed -i 's/^#*X11Forwarding.*/X11Forwarding no/' "$SSHD_CONFIG"
        sed -i 's/^#*MaxAuthTries.*/MaxAuthTries 3/' "$SSHD_CONFIG"
        sed -i 's/^#*IgnoreRhosts.*/IgnoreRhosts yes/' "$SSHD_CONFIG"
        sed -i 's/^#*HostbasedAuthentication.*/HostbasedAuthentication no/' "$SSHD_CONFIG"
        sed -i 's/^#*PermitEmptyPasswords.*/PermitEmptyPasswords no/' "$SSHD_CONFIG"
        sed -i 's/^#*ClientAliveInterval.*/ClientAliveInterval 300/' "$SSHD_CONFIG"
        sed -i 's/^#*ClientAliveCountMax.*/ClientAliveCountMax 2/' "$SSHD_CONFIG"
        sed -i 's/^#*Protocol.*/Protocol 2/' "$SSHD_CONFIG"
        sed -i 's/^#*LogLevel.*/LogLevel VERBOSE/' "$SSHD_CONFIG"
        
        grep -q "^AllowAgentForwarding" "$SSHD_CONFIG" || echo "AllowAgentForwarding no" >> "$SSHD_CONFIG"
        grep -q "^AllowTcpForwarding" "$SSHD_CONFIG" || echo "AllowTcpForwarding no" >> "$SSHD_CONFIG"
        grep -q "^Banner" "$SSHD_CONFIG" || echo "Banner /etc/issue.net" >> "$SSHD_CONFIG"
        
        echo "WARNING: Unauthorized access to this system is prohibited." > /etc/issue.net
        echo "All connections are monitored and recorded." >> /etc/issue.net
        
        SSH_NEEDS_RESTART=true
        log_success "SSH daemon configuration hardened."
        record_task "SSH Hardening" "EXECUTED" "Applied hardened directives & security banner"
    fi
fi

if [ "$SSH_NEEDS_RESTART" = true ]; then
    if systemctl is-active --quiet sshd; then
        systemctl restart sshd
    elif service ssh status >/dev/null 2>&1; then
        service ssh restart
    fi
    log_success "SSH service restarted with hardened configuration."
fi

# Configure Fail2ban SSH jail idempotently
if command -v fail2ban-client &>/dev/null; then
    if [ ! -f /etc/fail2ban/jail.local ] || ! grep -q "bantime = 0" /etc/fail2ban/jail.local; then
        cat > /etc/fail2ban/jail.local << 'EOF'
[DEFAULT]
bantime = 0
findtime = 600
maxretry = 5

[sshd]
enabled = true
port = ssh
filter = sshd
logpath = %(sshd_log)s
maxretry = 3
bantime = 0
EOF
        systemctl enable fail2ban 2>/dev/null || true
        systemctl restart fail2ban 2>/dev/null || true
        log_success "Configured Fail2ban SSH permanent jail."
        record_task "Fail2ban Jail" "EXECUTED" "Configured permanent ban jail for SSH"
    else
        systemctl is-active --quiet fail2ban || systemctl start fail2ban 2>/dev/null || true
        log_skip "Fail2ban configuration already active and verified."
        record_task "Fail2ban Jail" "SKIPPED" "Fail2ban jail already configured and running"
    fi
fi

# Secure Shared Memory (/dev/shm)
if ! grep -q "/dev/shm" /etc/fstab 2>/dev/null; then
    echo "tmpfs /dev/shm tmpfs defaults,noexec,nosuid,nodev 0 0" >> /etc/fstab
    log_success "Secured /dev/shm with noexec,nosuid,nodev in /etc/fstab."
    record_task "Shared Memory Hardening" "EXECUTED" "Mounted /dev/shm with noexec,nosuid,nodev"
else
    log_skip "/dev/shm already secured in /etc/fstab."
    record_task "Shared Memory Hardening" "SKIPPED" "Already in /etc/fstab"
fi

# Restrict core dumps
if ! grep -q "hard core 0" /etc/security/limits.conf 2>/dev/null; then
    echo "* hard core 0" >> /etc/security/limits.conf
    log_success "Restricted core dump generation in /etc/security/limits.conf."
    record_task "Core Dump Restriction" "EXECUTED" "Set hard core 0 limit"
else
    log_skip "Core dump restrictions already present in limits.conf."
    record_task "Core Dump Restriction" "SKIPPED" "Already configured"
fi

# ------------------------------------------------------------------------------
# 5. OS Lean Transformation & Background Process Pruning
# ------------------------------------------------------------------------------
print_section "PHASE 4: OS LEAN TRANSFORMATION & PROCESS PRUNING"

# Candidate bloatware services that consume RAM needlessly on a headless trading server
BLOATWARE_SERVICES=(
    "snapd.service"
    "snapd.socket"
    "snapd.seeded.service"
    "multipathd.service"
    "multipathd.socket"
    "packagekit.service"
    "whoopsie.service"
    "apport.service"
    "modemmanager.service"
    "udisks2.service"
    "cups.service"
    "cups-browsed.service"
    "avahi-daemon.service"
    "bluetooth.service"
    "rpcbind.service"
    "speech-dispatcher.service"
    "thermal-monitor.service"
    "bolt.service"
)

DISABLED_SERVICES_COUNT=0
ALREADY_INACTIVE_COUNT=0

for srv in "${BLOATWARE_SERVICES[@]}"; do
    if systemctl is-active --quiet "$srv" 2>/dev/null || [ "$(systemctl is-enabled "$srv" 2>/dev/null)" = "enabled" ]; then
        log_info "Deactivating bloatware service: $srv..."
        systemctl stop "$srv" 2>/dev/null || true
        systemctl disable "$srv" 2>/dev/null || true
        systemctl mask "$srv" 2>/dev/null || true
        ((DISABLED_SERVICES_COUNT++))
    else
        ((ALREADY_INACTIVE_COUNT++))
    fi
done

if [ "$DISABLED_SERVICES_COUNT" -gt 0 ]; then
    log_success "Stopped, disabled, and masked $DISABLED_SERVICES_COUNT unnecessary background services."
    record_task "OS Process Pruning" "EXECUTED" "Masked $DISABLED_SERVICES_COUNT background services (snapd, multipathd, etc.)"
else
    log_skip "All $ALREADY_INACTIVE_COUNT targeted bloatware services are already stopped and masked."
    record_task "OS Process Pruning" "SKIPPED" "All bloatware services already inactive/masked"
fi

# Optimize Virtual Consoles (TTYs) - Headless VPS only needs 1 getty, not 6
LOGIND_CONF="/etc/systemd/logind.conf"
if [ -f "$LOGIND_CONF" ]; then
    if ! grep -q "^NAutoVTs=1" "$LOGIND_CONF"; then
        sed -i 's/^#*NAutoVTs=.*/NAutoVTs=1/' "$LOGIND_CONF"
        grep -q "^NAutoVTs=1" "$LOGIND_CONF" || echo "NAutoVTs=1" >> "$LOGIND_CONF"
        systemctl restart systemd-logind 2>/dev/null || true
        log_success "Capped virtual console gettys to 1 (NAutoVTs=1) to eliminate idle RAM processes."
        record_task "TTY Optimization" "EXECUTED" "Capped NAutoVTs=1 in logind.conf"
    else
        log_skip "Virtual console allocation already tuned to NAutoVTs=1."
        record_task "TTY Optimization" "SKIPPED" "NAutoVTs=1 already active"
    fi
fi

# Disable motd-news timer if active (prevents periodic background python invocations)
if systemctl is-active --quiet motd-news.timer 2>/dev/null; then
    systemctl stop motd-news.timer 2>/dev/null || true
    systemctl disable motd-news.timer 2>/dev/null || true
    log_info "Disabled motd-news background timer."
fi

# ------------------------------------------------------------------------------
# 6. RAM, Swap & Storage Optimization (Unified & Idempotent)
# ------------------------------------------------------------------------------
print_section "PHASE 5: RAM, COMPRESSED ZRAM & KERNEL MEMORY TUNING"

# 1. ZRAM Compressed Memory Swap Configuration (zstd, 60% RAM, Priority 100)
ZRAM_CONF="/etc/default/zramswap"
ZRAM_NEEDS_SETUP=false

if [ ! -f "$ZRAM_CONF" ] || ! grep -q "ALGO=zstd" "$ZRAM_CONF" 2>/dev/null || ! grep -q "PERCENT=60" "$ZRAM_CONF" 2>/dev/null; then
    ZRAM_NEEDS_SETUP=true
fi

if [ "$ZRAM_NEEDS_SETUP" = true ]; then
    log_info "Configuring ZRAM in-memory compressed swap (zstd algorithm, 60% RAM)..."
    cat > "$ZRAM_CONF" << 'EOF'
# DeltaZero26 In-Memory Compressed Swap
ALGO=zstd
PERCENT=60
PRIORITY=100
EOF
    systemctl enable zramswap 2>/dev/null || true
    systemctl restart zramswap 2>/dev/null || true
    log_success "ZRAM compressed swap initialized (60% RAM, zstd compression, Priority 100)."
    record_task "ZRAM Compressed Swap" "EXECUTED" "Configured 60% RAM zstd swap at Priority 100"
else
    systemctl is-active --quiet zramswap || systemctl start zramswap 2>/dev/null || true
    log_skip "ZRAM compressed swap is already configured and active."
    record_task "ZRAM Compressed Swap" "SKIPPED" "Already configured (zstd, 60% RAM)"
fi

# 2. Disk Swapfile Fallback (Priority 10)
ACTIVE_SWAP_KB=$(grep SwapTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo "0")
SWAPFILE_EXISTS=false
[ -f /swapfile ] && SWAPFILE_EXISTS=true

if [ "$ACTIVE_SWAP_KB" -lt 1048576 ] || [ "$SWAPFILE_EXISTS" = false ]; then
    TOTAL_RAM_MB=$(free -m | awk '/^Mem:/{print $2}')
    SWAP_TARGET_MB=2048
    [ "$TOTAL_RAM_MB" -gt 2048 ] && SWAP_TARGET_MB="$TOTAL_RAM_MB"
    
    ROOT_AVAIL_MB=$(df -m / 2>/dev/null | tail -1 | awk '{print $4}' || echo "0")
    if [ "$ROOT_AVAIL_MB" -gt $(( SWAP_TARGET_MB + 2000 )) ]; then
        log_info "Creating ${SWAP_TARGET_MB} MB disk /swapfile fallback (Priority 10)..."
        fallocate -l "${SWAP_TARGET_MB}M" /swapfile 2>/dev/null || dd if=/dev/zero of=/swapfile bs=1M count="$SWAP_TARGET_MB" status=none
        chmod 600 /swapfile
        mkswap /swapfile >/dev/null 2>&1
        swapon -p 10 /swapfile 2>/dev/null || swapon /swapfile 2>/dev/null || true
        
        if ! grep -q "/swapfile" /etc/fstab 2>/dev/null; then
            echo "/swapfile none swap sw,pri=10 0 0" >> /etc/fstab
        fi
        log_success "Created and activated ${SWAP_TARGET_MB} MB disk /swapfile (Priority 10)."
        record_task "Disk Swap Fallback" "EXECUTED" "Created ${SWAP_TARGET_MB} MB /swapfile with pri=10"
    else
        log_warn "Insufficient disk space (${ROOT_AVAIL_MB} MB free) to safely create swapfile; skipping."
        record_task "Disk Swap Fallback" "SKIPPED" "Insufficient disk headroom"
    fi
else
    # Swap is already active - ensure it is swapon without re-formatting
    swapon /swapfile 2>/dev/null || true
    log_skip "Sufficient swap space already active ($(awk '/SwapTotal/ {printf "%.1f GB", $2/1048576}' /proc/meminfo 2>/dev/null))."
    record_task "Disk Swap Fallback" "SKIPPED" "Existing swap active; non-destructive bypass"
fi

# 3. Systemd Journal RAM & Storage Limit
JOURNAL_CONF="/etc/systemd/journald.conf"
JOURNAL_CHANGED=false

if [ -f "$JOURNAL_CONF" ]; then
    if ! grep -q "^SystemMaxUse=50M" "$JOURNAL_CONF" || ! grep -q "^RuntimeMaxUse=20M" "$JOURNAL_CONF"; then
        backup_file "$JOURNAL_CONF"
        sed -i 's/^#*SystemMaxUse=.*/SystemMaxUse=50M/' "$JOURNAL_CONF"
        sed -i 's/^#*RuntimeMaxUse=.*/RuntimeMaxUse=20M/' "$JOURNAL_CONF"
        sed -i 's/^#*MaxRetentionSec=.*/MaxRetentionSec=7day/' "$JOURNAL_CONF"
        grep -q "^SystemMaxUse=50M" "$JOURNAL_CONF" || echo "SystemMaxUse=50M" >> "$JOURNAL_CONF"
        grep -q "^RuntimeMaxUse=20M" "$JOURNAL_CONF" || echo "RuntimeMaxUse=20M" >> "$JOURNAL_CONF"
        grep -q "^MaxRetentionSec=7day" "$JOURNAL_CONF" || echo "MaxRetentionSec=7day" >> "$JOURNAL_CONF"
        JOURNAL_CHANGED=true
    fi
fi

if [ "$JOURNAL_CHANGED" = true ]; then
    systemctl restart systemd-journald 2>/dev/null || true
    journalctl --vacuum-size=50M 2>/dev/null || true
    log_success "Capped systemd-journald RAM (20M) and storage (50M)."
    record_task "Journald RAM Cap" "EXECUTED" "Capped at 20M RAM / 50M Disk"
else
    journalctl --vacuum-size=50M 2>/dev/null || true
    log_skip "Journald memory limits already enforced."
    record_task "Journald RAM Cap" "SKIPPED" "Already configured (20M RAM / 50M Disk)"
fi

# 4. Consolidated Unified Sysctl Parameters
SYSCTL_FILE="/etc/sysctl.d/99-deltazero.conf"
rm -f /etc/sysctl.d/99-security.conf /etc/sysctl.d/99-deltazero-memory.conf 2>/dev/null || true

cat > "$SYSCTL_FILE" << 'EOF'
# DeltaZero26 Unified Kernel Memory & Security Tuning
# ===================================================

# Virtual Memory & RAM Optimization
vm.swappiness = 30
vm.vfs_cache_pressure = 150
vm.dirty_background_ratio = 5
vm.dirty_ratio = 10
vm.page-cluster = 0

# Security & Network Protection
net.ipv4.conf.all.rp_filter = 1
net.ipv4.conf.default.rp_filter = 1
net.ipv4.conf.all.accept_source_route = 0
net.ipv4.conf.default.accept_source_route = 0
net.ipv4.icmp_echo_ignore_broadcasts = 1
net.ipv4.conf.all.accept_redirects = 0
net.ipv4.conf.default.accept_redirects = 0
net.ipv4.conf.all.secure_redirects = 0
net.ipv4.conf.default.secure_redirects = 0
net.ipv6.conf.all.accept_redirects = 0
net.ipv6.conf.default.accept_redirects = 0
net.ipv4.conf.all.send_redirects = 0
net.ipv4.conf.default.send_redirects = 0
net.ipv4.tcp_syncookies = 1
net.ipv4.tcp_max_syn_backlog = 2048
net.ipv4.tcp_synack_retries = 2
net.ipv4.tcp_syn_retries = 5
net.ipv4.conf.all.log_martians = 1
net.ipv4.conf.default.log_martians = 1
kernel.randomize_va_space = 2
kernel.kptr_restrict = 2
kernel.dmesg_restrict = 1
kernel.sysrq = 0
kernel.yama.ptrace_scope = 1
fs.suid_dumpable = 0
net.ipv4.tcp_timestamps = 0
net.ipv4.icmp_ignore_bogus_error_responses = 1
EOF

sysctl -p "$SYSCTL_FILE" >/dev/null 2>&1 || true
log_success "Applied unified kernel memory and security parameters (swappiness=30, vfs_cache_pressure=150, page-cluster=0)."
record_task "Kernel Sysctl Tuning" "EXECUTED" "Applied unified /etc/sysctl.d/99-deltazero.conf"

# 5. Storage Reserved Block Optimization (ext4 5% down to 1%)
ROOT_DEV=$(df / | tail -1 | awk '{print $1}')
if [ -b "$ROOT_DEV" ]; then
    FS_TYPE=$(blkid -s TYPE -o value "$ROOT_DEV" 2>/dev/null || df -T / | tail -1 | awk '{print $2}')
    if [ "$FS_TYPE" = "ext4" ]; then
        tune2fs -m 1 "$ROOT_DEV" 2>/dev/null || true
        log_success "Reduced ext4 reserved root blocks to 1% to gain ~1GB storage."
        record_task "Storage Reclaim" "EXECUTED" "Reclaimed ext4 reserved blocks (tune2fs -m 1)"
    fi
fi

# 6. Optimize Host PostgreSQL WAL if installed locally
for PG_CONF in /etc/postgresql/*/main/postgresql.conf; do
    if [ -f "$PG_CONF" ]; then
        sed -i 's/^#*max_wal_size = .*/max_wal_size = 1GB/' "$PG_CONF" 2>/dev/null || true
        sed -i 's/^#*min_wal_size = .*/min_wal_size = 256MB/' "$PG_CONF" 2>/dev/null || true
        systemctl reload postgresql 2>/dev/null || true
        log_success "Optimized host PostgreSQL WAL limits in $PG_CONF."
    fi
done

# 7. Drop inactive filesystem caches so reclaimed RAM is immediately freed
sync
sysctl -w vm.drop_caches=3 >/dev/null 2>&1 || true
log_success "Flushed inactive kernel pagecaches to immediately release freed RAM."

# ------------------------------------------------------------------------------
# 7. Out-of-Container Host Deployment Daemon
# ------------------------------------------------------------------------------
print_section "PHASE 6: OUT-OF-CONTAINER HOST DEPLOYMENT DAEMON"

APP_DIR="$(pwd)"
DEPLOY_SCRIPT="/usr/local/bin/deltazero-deploy"
DAEMON_SCRIPT="/usr/local/bin/deltazero-deploy-daemon"
DAEMON_SERVICE="/etc/systemd/system/deltazero-deploy.service"
FIFO_PATH="/run/deltazero-deploy.fifo"

# Check if daemon is already active and healthy
DAEMON_ACTIVE=false
if systemctl is-active --quiet deltazero-deploy.service 2>/dev/null && [ -f "$DEPLOY_SCRIPT" ] && [ -f "$DAEMON_SCRIPT" ]; then
    DAEMON_ACTIVE=true
fi

if [ "$DAEMON_ACTIVE" = true ]; then
    log_skip "Out-of-container deployment daemon is already active and running."
    record_task "Deployment Daemon" "SKIPPED" "deltazero-deploy.service already active"
else
    # 1. Deployment wrapper script
    cat > "$DEPLOY_SCRIPT" << EOF
#!/bin/bash
set -eo pipefail
APP_DIR="$APP_DIR"
LOCK_FILE="/tmp/deltazero-deploy.lock"
LOG_FILE="/var/log/deltazero-deploy.log"

cleanup() {
    rm -f "\$LOCK_FILE"
}
trap cleanup EXIT INT TERM

cd "\$APP_DIR"

BRANCH="\${1:-main}"
BUILD_FLAG="\${2:-nobuild}"

DEPLOY_ARGS=("--deploy" "--branch" "\$BRANCH")
if [ "\$BUILD_FLAG" == "build" ]; then
    DEPLOY_ARGS=("--deploy" "--build" "--branch" "\$BRANCH")
fi

echo "[\$(date -Iseconds)] 🚀 Host deployment triggered on branch '\$BRANCH' (Mode: \$BUILD_FLAG)..." | tee "\$LOG_FILE"

if [ -f "./deploy.sh" ]; then
    chmod +x ./deploy.sh
    ./deploy.sh "\${DEPLOY_ARGS[@]}" 2>&1 | tee -a "\$LOG_FILE"
else
    echo "❌ Error: deploy.sh not found in \$APP_DIR" | tee -a "\$LOG_FILE"
    exit 1
fi

echo "[\$(date -Iseconds)] ✅ Host deployment finished successfully." | tee -a "\$LOG_FILE"
EOF
    chmod +x "$DEPLOY_SCRIPT"

    # 2. FIFO Trigger Pipe & Deployment Daemon
    touch /var/log/deltazero-deploy.log
    chmod 666 /var/log/deltazero-deploy.log 2>/dev/null || true
    rm -rf "$FIFO_PATH" 2>/dev/null || true
    mkfifo "$FIFO_PATH" 2>/dev/null || true
    chmod 666 "$FIFO_PATH" 2>/dev/null || true

    cat > "$DAEMON_SCRIPT" << EOF
#!/bin/bash
FIFO_PATH="$FIFO_PATH"

while true; do
    if [ -d "\$FIFO_PATH" ] || [ ! -p "\$FIFO_PATH" ]; then
        rm -rf "\$FIFO_PATH" 2>/dev/null || true
        mkfifo "\$FIFO_PATH" 2>/dev/null || true
        chmod 666 "\$FIFO_PATH" 2>/dev/null || true
    fi
    if read -r line < "\$FIFO_PATH"; then
        if [ -n "\$line" ]; then
            JOB_ID="\$(echo "\$line" | cut -d':' -f1)"
            BRANCH="\$(echo "\$line" | cut -d':' -f2)"
            BUILD_FLAG="\$(echo "\$line" | cut -d':' -f3)"
            [ -z "\$BRANCH" ] && BRANCH="main"
            [ -z "\$BUILD_FLAG" ] && BUILD_FLAG="nobuild"
            echo "[\$(date -Iseconds)] Received deployment trigger for job '\$JOB_ID' on branch '\$BRANCH' (Mode: \$BUILD_FLAG)" >> /var/log/deltazero-deploy.log
            /usr/local/bin/deltazero-deploy "\$BRANCH" "\$BUILD_FLAG" &
        fi
    fi
    sleep 1
done
EOF
    chmod +x "$DAEMON_SCRIPT"

    # 3. Systemd Service Unit
    cat > "$DAEMON_SERVICE" << EOF
[Unit]
Description=DeltaZero26 Out-of-Container Deployment Host Daemon
After=network.target docker.service

[Service]
Type=simple
ExecStart=$DAEMON_SCRIPT
Restart=always
RestartSec=3
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload 2>/dev/null || true
    systemctl enable deltazero-deploy.service 2>/dev/null || true
    systemctl restart deltazero-deploy.service 2>/dev/null || true
    log_success "Configured and started deltazero-deploy.service daemon."
    record_task "Deployment Daemon" "EXECUTED" "Configured systemd service & FIFO pipe"
fi

# ------------------------------------------------------------------------------
# 8. Application Deployment (Guarded & Non-Destructive)
# ------------------------------------------------------------------------------
print_section "PHASE 7: APPLICATION DEPLOYMENT VERIFICATION"

CONTAINER_NAME="algo_trading_monolith"
CONTAINER_RUNNING=false

if command -v docker &>/dev/null && docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${CONTAINER_NAME}$"; then
    CONTAINER_RUNNING=true
fi

if [ "$CONTAINER_RUNNING" = true ]; then
    log_skip "Docker container '$CONTAINER_NAME' is already up and running."
    log_info "Skipping deploy.sh to protect active trading algorithms from unexpected restart."
    record_task "Application Deployment" "SKIPPED" "Container '$CONTAINER_NAME' already running healthy"
elif [ -f "./deploy.sh" ]; then
    log_info "No active '$CONTAINER_NAME' container detected. Initiating initial deployment via deploy.sh..."
    chmod +x ./deploy.sh
    export SUPERUSER_PASSWORD
    ./deploy.sh --deploy
    record_task "Application Deployment" "EXECUTED" "Executed ./deploy.sh --deploy"
else
    log_warn "./deploy.sh not found in $(pwd). Skipping deployment."
    record_task "Application Deployment" "SKIPPED" "deploy.sh not found"
fi

# ------------------------------------------------------------------------------
# 9. Production Security & Performance Audit
# ------------------------------------------------------------------------------
print_section "PHASE 8: PRODUCTION SECURITY & PERFORMANCE AUDIT"

AUDIT_PASS=0
AUDIT_WARN=0
AUDIT_FAIL=0
ENV_FILE="./.env"

# 1. Check Root Login & Non-root user
if [[ "$disable_root" =~ ^[Yy]$ ]] || grep -qE "^PermitRootLogin\s+(no|prohibit-password)" /etc/ssh/sshd_config 2>/dev/null; then
    log_success "SSH: Root login is restricted."
    ((AUDIT_PASS++))
else
    log_warn "SSH: Root login is unrestricted. Recommend disabling or setting prohibit-password."
    ((AUDIT_WARN++))
fi

# 2. Check Fail2ban
if systemctl is-active --quiet fail2ban 2>/dev/null; then
    log_success "Intrusion Prevention: fail2ban is active and protecting SSH."
    ((AUDIT_PASS++))
else
    log_warn "Intrusion Prevention: fail2ban service is not active."
    ((AUDIT_WARN++))
fi

# 3. Check System RAM & Swap Capacity
if [ -f /proc/meminfo ]; then
    TOTAL_MEM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    SWAP_MEM_KB=$(grep SwapTotal /proc/meminfo | awk '{print $2}')
    if [ "${TOTAL_MEM_KB:-0}" -ge 1800000 ] || [ "${SWAP_MEM_KB:-0}" -ge 1800000 ]; then
        log_success "Memory Architecture: RAM + Swap capacity >= 2 GB for stable engine execution."
        ((AUDIT_PASS++))
    else
        log_warn "Memory Architecture: Combined RAM/Swap is below 2 GB. Container may experience OOM under heavy load."
        ((AUDIT_WARN++))
    fi
fi

# 4. Check Disk Storage Headroom
DISK_FREE_KB=$(df / 2>/dev/null | tail -1 | awk '{print $4}')
DISK_TOTAL_KB=$(df / 2>/dev/null | tail -1 | awk '{print $2}')
if [ -n "$DISK_FREE_KB" ] && [ -n "$DISK_TOTAL_KB" ]; then
    DISK_FREE_GB=$(( DISK_FREE_KB / 1024 / 1024 ))
    DISK_TOTAL_GB=$(( DISK_TOTAL_KB / 1024 / 1024 ))
    if [ "${DISK_FREE_GB:-0}" -ge 8 ]; then
        log_success "Storage Capacity: ${DISK_FREE_GB} GB free of ${DISK_TOTAL_GB} GB total (ample database headroom)."
        ((AUDIT_PASS++))
    elif [ "${DISK_FREE_GB:-0}" -ge 4 ]; then
        log_warn "Storage Capacity: ${DISK_FREE_GB} GB free of ${DISK_TOTAL_GB} GB total. Monitor PostgreSQL space."
        ((AUDIT_WARN++))
    else
        log_error "Storage Capacity: Low disk space (${DISK_FREE_GB} GB free)! Risk of PostgreSQL write stalls."
        ((AUDIT_FAIL++))
    fi
fi

# 5. Check .env configuration
if [ -f "$ENV_FILE" ]; then
    log_success "Configuration: .env file exists."
    ((AUDIT_PASS++))
    
    ENV_PERMS=$(stat -c "%a" "$ENV_FILE" 2>/dev/null || stat -f "%OLp" "$ENV_FILE" 2>/dev/null)
    if [ "$ENV_PERMS" == "600" ] || [ "$ENV_PERMS" == "400" ]; then
        log_success "Secret Permissions: .env file restricted to mode 600/400."
        ((AUDIT_PASS++))
    else
        chmod 600 "$ENV_FILE" 2>/dev/null || true
        log_warn "Secret Permissions: Tightened .env permissions to 600."
        ((AUDIT_WARN++))
    fi

    if grep -Ei "^(DEBUG|DJANGO_DEBUG)\s*=\s*(True|1)" "$ENV_FILE" >/dev/null 2>&1; then
        log_error "Django Security: DEBUG is set to True in .env! Must be False in production."
        ((AUDIT_FAIL++))
    else
        log_success "Django Security: DEBUG is disabled."
        ((AUDIT_PASS++))
    fi
else
    log_warn "Configuration: No .env file detected in current directory."
    ((AUDIT_WARN++))
fi

# 6. Check Docker Container State
if [ "$CONTAINER_RUNNING" = true ] || (command -v docker &>/dev/null && docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${CONTAINER_NAME}$"); then
    log_success "Service State: Container '$CONTAINER_NAME' is active."
    ((AUDIT_PASS++))
else
    log_warn "Service State: Container '$CONTAINER_NAME' is not currently running."
    ((AUDIT_WARN++))
fi

# ------------------------------------------------------------------------------
# 10. Final Comprehensive Execution & Resource Delta Report
# ------------------------------------------------------------------------------
capture_system_metrics
FINAL_MEM_TOTAL=$METRIC_MEM_TOTAL
FINAL_MEM_USED=$METRIC_MEM_USED
FINAL_MEM_FREE=$METRIC_MEM_FREE
FINAL_MEM_AVAIL=$METRIC_MEM_AVAIL
FINAL_SWAP_TOTAL=$METRIC_SWAP_TOTAL
FINAL_SWAP_USED=$METRIC_SWAP_USED
FINAL_PROC_COUNT=$METRIC_PROC_COUNT

ELAPSED_SEC=$(( $(date +%s) - START_TIME ))
ELAPSED_MIN=$(( ELAPSED_SEC / 60 ))
ELAPSED_REM_SEC=$(( ELAPSED_SEC % 60 ))

# Calculate Deltas
RAM_SAVED=$(( INIT_MEM_USED - FINAL_MEM_USED ))
AVAIL_INCREASE=$(( FINAL_MEM_AVAIL - INIT_MEM_AVAIL ))
PROC_REDUCED=$(( INIT_PROC_COUNT - FINAL_PROC_COUNT ))

generate_report() {
    echo ""
    echo "================================================================================"
    echo "       DELTAZERO26 PROVISIONING & OPTIMIZATION EXECUTION REPORT                "
    echo "================================================================================"
    echo "  Execution Date   : $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "  Hostname         : $(hostname)"
    echo "  Kernel           : $(uname -r)"
    echo "  Execution Time   : ${ELAPSED_MIN}m ${ELAPSED_REM_SEC}s"
    echo "--------------------------------------------------------------------------------"
    echo "TASK EXECUTION SUMMARY:"
    printf "  %-32s | %-10s | %s\n" "Task Name" "Status" "Details"
    echo "  ---------------------------------+------------+-------------------------------"
    for i in "${!TASK_NAMES[@]}"; do
        printf "  %-32s | %-10s | %s\n" "${TASK_NAMES[$i]}" "${TASK_STATUSES[$i]}" "${TASK_DETAILS[$i]}"
    done
    echo "--------------------------------------------------------------------------------"
    echo "SYSTEM RESOURCE OPTIMIZATION MATRIX:"
    printf "  %-24s | %-12s | %-12s | %s\n" "Resource Metric" "Initial" "Final" "Net Change"
    echo "  -------------------------+--------------+--------------+----------------------"
    printf "  %-24s | %-12s | %-12s | %s\n" "RAM In Use" "${INIT_MEM_USED} MB" "${FINAL_MEM_USED} MB" "${RAM_SAVED} MB freed"
    printf "  %-24s | %-12s | %-12s | %s\n" "RAM Available" "${INIT_MEM_AVAIL} MB" "${FINAL_MEM_AVAIL} MB" "+${AVAIL_INCREASE} MB available"
    printf "  %-24s | %-12s | %-12s | %s\n" "Swap Capacity" "${INIT_SWAP_TOTAL} MB" "${FINAL_SWAP_TOTAL} MB" "$(( FINAL_SWAP_TOTAL - INIT_SWAP_TOTAL )) MB delta"
    printf "  %-24s | %-12s | %-12s | %s\n" "Active Processes" "${INIT_PROC_COUNT}" "${FINAL_PROC_COUNT}" "${PROC_REDUCED} stopped"
    echo "--------------------------------------------------------------------------------"
    echo "PRODUCTION SECURITY AUDIT SUMMARY:"
    echo "  Passed: $AUDIT_PASS | Warnings: $AUDIT_WARN | Critical Failures: $AUDIT_FAIL"
    echo "================================================================================"
    
    if [ "$AUDIT_FAIL" -gt 0 ]; then
        echo "🚨 ATTENTION: $AUDIT_FAIL critical security failures require immediate remediation."
    elif [ "$AUDIT_WARN" -gt 0 ]; then
        echo "⚠️  NOTE: $AUDIT_WARN warning(s) detected. Review the audit checklist above."
    else
        echo "🎉 SUCCESS: System is hardened, lean, and fully optimized for low-latency trading."
    fi
    echo "A full copy of this report has been saved to: $REPORT_FILE"
    echo "================================================================================"
    echo ""
}

# Print report to stdout and append to log file
generate_report | tee "$REPORT_FILE"

exit 0
