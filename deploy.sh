#!/bin/bash

# Automated Deployment & Docker Management Script
# Note: ALL options perform a Git Pull from GitHub first.
# Usage:
#   ./deploy.sh                  (Interactive mode)
#   ./deploy.sh --deploy         (Git pull + Docker build & restart + Auto system prune)
#   ./deploy.sh --build          (Git pull + Force Docker rebuild & restart + Auto system prune)
#   ./deploy.sh --no-prune       (Skip automatic Docker system prune)
#   ./deploy.sh --prune          (Git pull + Docker build & restart + System prune without touching volumes)
#   ./deploy.sh --prune-only     (Git pull + Docker system prune -a -f ONLY)
#   ./deploy.sh --restart-only   (Git pull + Docker container restart)
#   ./deploy.sh --reinstall      (Git pull + COMPLETELY WIPE ALL DATA, VOLUMES, NETWORKS, then deploy)
#   ./deploy.sh --branch <name>  (Specify branch, default: main)
#   ./deploy.sh --pull-only      (Git pull only (useful to fetch a new deploy.sh))
#
set -e

# Cleanup trap to ensure deployment lock file is never orphaned on script exit/failure
cleanup_lock() {
    rm -f /tmp/deltazero-deploy.lock /run/deltazero-deploy.lock 2>/dev/null || true
}
trap cleanup_lock EXIT INT TERM

# Ensure Git allows repository access regardless of user/systemd context
git config --system --add safe.directory "*" 2>/dev/null || sudo git config --system --add safe.directory "*" 2>/dev/null || true
git config --global --add safe.directory "*" 2>/dev/null || true
git config --global --add safe.directory "$(pwd)" 2>/dev/null || true
git config --global --add safe.directory "/home/Deltauser/deltazero26" 2>/dev/null || true

# Default settings
BRANCH="main"
ACTION="interactive"
PRUNE_IMAGES=true
FORCE_BUILD=false
SKIP_PULL=false
HEALTH_URL="http://localhost/health/"

# Print styled section headers
print_header() {
    echo ""
    echo "===================================================="
    echo "$1"
    echo "===================================================="
    echo ""
}

# Parse command line flags
while [[ $# -gt 0 ]]; do
    case $1 in
        --deploy)
            ACTION="deploy"
            shift
            ;;
        --build)
            ACTION="deploy"
            FORCE_BUILD=true
            shift
            ;;
        --prune)
            ACTION="deploy"
            PRUNE_IMAGES=true
            shift
            ;;
        --no-prune)
            PRUNE_IMAGES=false
            shift
            ;;
        --pull-only)
            ACTION="pull_only"
            shift
            ;;
        --prune-only)
            ACTION="prune_only"
            shift
            ;;
        --restart-only)
            ACTION="restart_only"
            shift
            ;;
        --reinstall)
            ACTION="reinstall"
            shift
            ;;
        --no-pull)
            SKIP_PULL=true
            shift
            ;;
        -b|--branch)
            BRANCH="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: ./deploy.sh [OPTIONS]"
            echo ""
            echo "Options (All options pull latest git code first):"
            echo "  --deploy         Smart deploy: Fast restart if code changed; rebuild only on core/dependency changes + auto prune"
            echo "  --build          Force full Docker image rebuild & deploy + auto prune"
            echo "  --prune          Smart deploy + volume-safe docker system prune"
            echo "  --no-prune       Skip automatic Docker system prune"
            echo "  --pull-only      Git pull only (useful to fetch a new deploy.sh)"
            echo "  --prune-only     Git pull + docker system prune (preserves volumes)"
            echo "  --restart-only   Git pull + restart containers without rebuild"
            echo "  --reinstall      Git pull + COMPLETELY WIPE ALL DATA, VOLUMES, NETWORKS, then deploy"
            echo "  --no-pull        Skip git pull"
            echo "  -b, --branch     Git branch to pull (default: main)"
            echo "  -h, --help       Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use ./deploy.sh --help for available options."
            exit 1
            ;;
    esac
done

# Interactive Menu if no action flag provided
if [ "$ACTION" == "interactive" ]; then
    print_header "AUTOMATED DEPLOYMENT & DOCKER MANAGEMENT"
    echo "Note: All options automatically pull the latest code from GitHub first."
    echo ""
    echo "1) Smart Deploy (Fast Restart if only code changed; Rebuild if dependencies changed; Auto Prune Unused Images)"
    echo "2) Force Full Docker Rebuild & Restart (Auto Prune Unused Images)"
    echo "3) System Prune ONLY (Volume-Safe)"
    echo "4) Restart Containers ONLY (No Code Pull, No Prune)"
    echo "5) Reinstall (WIPE ALL DATA, VOLUMES, NETWORKS, then deploy)"
    echo "6) Exit"
    echo ""
    read -p "Select an option [1-6]: " choice

    case $choice in
        1)
            ACTION="deploy"
            ;;
        2)
            ACTION="deploy"
            FORCE_BUILD=true
            ;;
        3)
            ACTION="prune_only"
            ;;
        4)
            ACTION="restart_only"
            SKIP_PULL=true
            PRUNE_IMAGES=false
            ;;
        5)
            ACTION="reinstall"
            ;;
        6)
            echo "Deployment cancelled."
            exit 0
            ;;
        *)
            echo "Invalid selection. Exiting."
            exit 1
            ;;
    esac
fi

# Disable interactive password prompts during automated git commands
export GIT_TERMINAL_PROMPT=0

# Function: Pull latest git code
pull_code() {
    if [ "$SKIP_PULL" == "true" ]; then
        echo "Skipping git pull (--no-pull flag set)."
        return
    fi

    # Configure git safe directory (system and global) to prevent dubious ownership fatal errors
    git config --system --add safe.directory "*" 2>/dev/null || sudo git config --system --add safe.directory "*" 2>/dev/null || true
    git config --global --add safe.directory "*" 2>/dev/null || true
    git config --global --add safe.directory "$(pwd)" 2>/dev/null || true
    git config --global --add safe.directory "/home/Deltauser/deltazero26" 2>/dev/null || true

    # Fix .git ownership and permissions so git operations never fail with "insufficient permission"
    if [ -d ".git" ]; then
        if [ "$(id -u)" -eq 0 ] && [ -n "$SUDO_USER" ]; then
            chown -R "$SUDO_USER:$SUDO_USER" .git 2>/dev/null || true
        elif command -v sudo >/dev/null 2>&1; then
            sudo chown -R "$(id -un):$(id -gn)" .git 2>/dev/null || true
        fi
        chmod -R u+rwX .git 2>/dev/null || sudo chmod -R u+rwX .git 2>/dev/null || true
    fi

    # Check git status
    if [ -n "$(git -c safe.directory=* status --porcelain 2>/dev/null || true)" ]; then
        echo "⚠️  Local changes detected. Stashing local uncommitted changes..."
        git -c safe.directory=* stash || true
    fi

    # Ensure SSH keys are synchronized between root and Deltauser
    if [ "$(id -u)" -eq 0 ] && [ -d "/home/Deltauser/.ssh" ]; then
        mkdir -p /root/.ssh
        chmod 700 /root/.ssh
        cp -n /home/Deltauser/.ssh/id_* /root/.ssh/ 2>/dev/null || true
        chmod 600 /root/.ssh/id_* 2>/dev/null || true
        chmod 644 /root/.ssh/*.pub 2>/dev/null || true
    fi

    # Detect available SSH private key for Git
    SSH_KEY=""
    if [ -f "$HOME/.ssh/id_ed25519" ]; then
        SSH_KEY="$HOME/.ssh/id_ed25519"
    elif [ -f "/home/Deltauser/.ssh/id_ed25519" ]; then
        SSH_KEY="/home/Deltauser/.ssh/id_ed25519"
    elif [ -f "/root/.ssh/id_ed25519" ]; then
        SSH_KEY="/root/.ssh/id_ed25519"
    elif [ -f "$HOME/.ssh/id_rsa" ]; then
        SSH_KEY="$HOME/.ssh/id_rsa"
    fi

    echo "Fetching latest changes from remote..."

    # Attempt 1: Standard Git fetch using user's working shell/SSH environment
    # (Works seamlessly without passphrase prompt if manual git pull already works)
    FETCH_OK=false
    if git -c safe.directory=* fetch origin 2>/dev/null; then
        FETCH_OK=true
    else
        # Attempt 2: Fallback to explicitly discovered SSH private key
        if ssh-add -l &>/dev/null; then
            export GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new"
        elif [ -n "$SSH_KEY" ]; then
            export GIT_SSH_COMMAND="ssh -i $SSH_KEY -o StrictHostKeyChecking=accept-new"
        fi

        if git -c safe.directory=* fetch origin; then
            FETCH_OK=true
        fi
    fi

    if [ "$FETCH_OK" != "true" ]; then
        echo "❌ Error: Failed to fetch from GitHub."
        echo ""
        echo "================================================================================"
        echo "🔑 GITHUB SSH DEPLOY KEY ACCESS REQUIRED"
        echo "================================================================================"
        PUB_KEY=""
        [ -f "$HOME/.ssh/id_ed25519.pub" ] && PUB_KEY=$(cat "$HOME/.ssh/id_ed25519.pub")
        [ -z "$PUB_KEY" ] && [ -f "/home/Deltauser/.ssh/id_ed25519.pub" ] && PUB_KEY=$(cat "/home/Deltauser/.ssh/id_ed25519.pub")
        [ -z "$PUB_KEY" ] && [ -f "/root/.ssh/id_ed25519.pub" ] && PUB_KEY=$(cat "/root/.ssh/id_ed25519.pub")
        if [ -n "$PUB_KEY" ]; then
            echo "Please add this server public key to your GitHub repository:"
            echo "👉 GitHub Repository -> Settings -> Deploy Keys -> 'Add deploy key'"
            echo ""
            echo "$PUB_KEY"
            echo ""
            echo "Also ensure the key has NO passphrase: ssh-keygen -p -f ~/.ssh/id_ed25519"
        else
            echo "No SSH key found. Run: ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519"
        fi
        echo "================================================================================"
        exit 1
    fi

    PREV_COMMIT=$(git -c safe.directory=* rev-parse HEAD 2>/dev/null || true)

    echo "Checking out branch '$BRANCH' and pulling..."
    git -c safe.directory=* checkout "$BRANCH"
    if ! git -c safe.directory=* pull origin "$BRANCH"; then
        echo "❌ Error: 'git pull' failed."
        exit 1
    fi

    NEW_COMMIT=$(git -c safe.directory=* rev-parse HEAD 2>/dev/null || true)
    if [ -n "$PREV_COMMIT" ] && [ -n "$NEW_COMMIT" ] && [ "$PREV_COMMIT" != "$NEW_COMMIT" ]; then
        echo "✅ Code updated successfully from GitHub ($PREV_COMMIT -> $NEW_COMMIT)."
    else
        echo "✅ Code is up to date with origin/$BRANCH ($NEW_COMMIT)."
    fi
}

# Function: Ensure Docker Access
ensure_docker_access() {
    if [ -S /var/run/docker.sock ] && [ ! -w /var/run/docker.sock ]; then
        sudo chmod 666 /var/run/docker.sock 2>/dev/null || true
        sudo usermod -aG docker "$USER" 2>/dev/null || true
    fi
}

# Function: Detect Docker Compose command
get_docker_compose_cmd() {
    ensure_docker_access

    if docker compose version &> /dev/null; then
        echo "docker compose"
    elif command -v docker-compose &> /dev/null && docker-compose version &> /dev/null; then
        echo "docker-compose"
    elif sudo docker compose version &> /dev/null; then
        echo "sudo docker compose"
    elif command -v docker-compose &> /dev/null && sudo docker-compose version &> /dev/null; then
        echo "sudo docker-compose"
    else
        echo "❌ Error: Neither 'docker-compose' nor 'docker compose' was found." >&2
        exit 1
    fi
}

# Function: Ensure Deployment FIFO & Log Files Exist
prepare_deployment_pipes() {
    ensure_docker_access
    if [ -d /var/log/deltazero-deploy.log ]; then
        sudo rm -rf /var/log/deltazero-deploy.log 2>/dev/null || rm -rf /var/log/deltazero-deploy.log 2>/dev/null || true
    fi
    sudo touch /var/log/deltazero-deploy.log 2>/dev/null || touch /var/log/deltazero-deploy.log 2>/dev/null || true
    sudo chmod 666 /var/log/deltazero-deploy.log 2>/dev/null || chmod 666 /var/log/deltazero-deploy.log 2>/dev/null || true

    if [ -d /run/deltazero-deploy.fifo ] || [ ! -p /run/deltazero-deploy.fifo ]; then
        sudo rm -rf /run/deltazero-deploy.fifo 2>/dev/null || rm -rf /run/deltazero-deploy.fifo 2>/dev/null || true
        sudo mkfifo /run/deltazero-deploy.fifo 2>/dev/null || mkfifo /run/deltazero-deploy.fifo 2>/dev/null || true
        sudo chmod 666 /run/deltazero-deploy.fifo 2>/dev/null || chmod 666 /run/deltazero-deploy.fifo 2>/dev/null || true
    fi
}

# Function: Determine whether a full Docker image rebuild is required
is_rebuild_needed() {
    # 1. If explicitly forced with --build
    if [ "$FORCE_BUILD" == "true" ]; then
        echo "🔧 Force rebuild requested (--build flag)."
        return 0
    fi

    # 2. Check if the app container is currently running
    DOCKER_COMPOSE_CMD=$(get_docker_compose_cmd)
    if ! $DOCKER_COMPOSE_CMD ps --services --filter "status=running" 2>/dev/null | grep -q "app"; then
        echo "🚀 Monolith container is not currently running. Full build required."
        return 0
    fi

    # 3. Check if any core files affecting the container image changed between commits
    if [ -n "$PREV_COMMIT" ] && [ -n "$NEW_COMMIT" ] && [ "$PREV_COMMIT" != "$NEW_COMMIT" ]; then
        local CHANGED_BUILD_FILES
        CHANGED_BUILD_FILES=$(git -c safe.directory=* diff --name-only "$PREV_COMMIT" "$NEW_COMMIT" 2>/dev/null | grep -E "^(Dockerfile|pyproject\.toml|uv\.lock|docker-compose.*\.yml|supervisord\.conf|nginx\.conf|pg_entrypoint\.sh)$" || true)
        if [ -n "$CHANGED_BUILD_FILES" ]; then
            echo "📦 Core configuration/dependency changes detected:"
            echo "$CHANGED_BUILD_FILES" | sed 's/^/   - /'
            echo "Full Docker image rebuild required."
            return 0
        fi
    fi

    # 4. Code changes are live-mounted via host volume (.:/app); fast restart is sufficient!
    return 1
}

# Function: Smart Rebuild or Fast Restart of Docker Containers
rebuild_or_restart_docker() {
    prepare_deployment_pipes
    DOCKER_COMPOSE_CMD=$(get_docker_compose_cmd)

    if is_rebuild_needed; then
        print_header "STEP: REBUILDING & STARTING DOCKER CONTAINERS"
        echo "Building and starting containers in detached mode..."
        DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 $DOCKER_COMPOSE_CMD up -d --build
    else
        print_header "STEP: FAST RESTART (HOST VOLUME SYNC)"
        echo "⚡ Application code is live-mounted via host volume (.:/app)."
        echo "⚡ No dependency or Dockerfile changes. Restarting container in seconds..."
        $DOCKER_COMPOSE_CMD restart app
    fi

    echo "Waiting for PostgreSQL to be ready inside container..."
    MAX_PG_RETRIES=20
    PG_READY=false
    for i in $(seq 1 $MAX_PG_RETRIES); do
        if $DOCKER_COMPOSE_CMD exec -T app pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then
            PG_READY=true
            echo "✅ PostgreSQL is ready."
            break
        fi
        sleep 1
    done
    if [ "$PG_READY" != "true" ]; then
        echo "⚠️ PostgreSQL did not report ready in ${MAX_PG_RETRIES}s. Proceeding with migration..."
    fi

    echo "Running database migrations inside container..."
    $DOCKER_COMPOSE_CMD exec -T app python manage.py migrate --noinput || echo "⚠️ Migration failed or container unavailable."

    echo "Collecting static files..."
    $DOCKER_COMPOSE_CMD exec -T app python manage.py collectstatic --noinput || echo "⚠️ Collectstatic skipped or failed."

    echo "✅ Docker containers updated and running."
}

# Function: Restart Docker Containers Without Rebuild
restart_docker_fast() {
    print_header "STEP: RESTARTING DOCKER CONTAINERS"
    DOCKER_COMPOSE_CMD=$(get_docker_compose_cmd)

    echo "Restarting running containers..."
    $DOCKER_COMPOSE_CMD restart

    echo "✅ Docker containers restarted."
}

# Function: Docker System Prune (Without touching volumes)
prune_system() {
    print_header "STEP: DOCKER SYSTEM PRUNE (VOLUME-SAFE)"
    echo "Removing unused images, stopped containers, build caches, and dangling networks..."
    echo "Note: Named volumes (e.g., database & cert data) are PRESERVED and untouched."
    echo ""
    
    ensure_docker_access
    if docker info &> /dev/null; then
        docker system prune -a -f
        docker builder prune -f 2>/dev/null || true
    else
        sudo docker system prune -a -f
        sudo docker builder prune -f 2>/dev/null || true
    fi

    echo ""
    echo "✅ Docker system prune completed safely."
}

# Function: Health Check Verification
verify_health() {
    print_header "STEP: POST-DEPLOYMENT HEALTH CHECK"
    echo "Checking application endpoint: $HEALTH_URL"
    
    MAX_RETRIES=6
    RETRY_COUNT=0
    SUCCESS=false

    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        STATUS_CODE=$(curl -skL -o /dev/null -w "%{http_code}" "$HEALTH_URL" || echo "000")
        if [ "$STATUS_CODE" -eq 200 ]; then
            SUCCESS=true
            break
        fi
        echo "Waiting for app to start... (Attempt $((RETRY_COUNT+1))/$MAX_RETRIES - Status: $STATUS_CODE)"
        sleep 5
        RETRY_COUNT=$((RETRY_COUNT+1))
    done

    if [ "$SUCCESS" == "true" ]; then
        echo "🎉 Health check PASSED (HTTP 200 OK)."
    else
        echo "⚠️ Health check timed out or failed (HTTP Status: $STATUS_CODE). Please check logs with 'docker compose logs'."
    fi
}

# Function: Reinstall - Wipe all Docker resources
reinstall_system() {
    print_header "STEP: REINSTALL - WIPING ALL DOCKER RESOURCES"
    DOCKER_COMPOSE_CMD=$(get_docker_compose_cmd)
    echo "Stopping containers and removing all volumes, networks, and images..."
    $DOCKER_COMPOSE_CMD down -v --rmi all --remove-orphans || true
    echo "✅ All resources wiped."
}

# Function: Configure environment
configure_env() {
    print_header "STEP: CONFIGURE ENVIRONMENT & SECRETS"
    
    # Read SUPERUSER_PASSWORD from .env if available
    if [ -z "$SUPERUSER_PASSWORD" ] && [ -f .env ]; then
        SUPERUSER_PASSWORD=$(grep -E "^SUPERUSER_PASSWORD=" .env | cut -d '=' -f2- | tr -d ' "' | tr -d "'" || true)
    fi

    # Only prompt if running an explicit reinstall and password is still unset
    if [ -z "$SUPERUSER_PASSWORD" ] && [ "$ACTION" == "reinstall" ]; then
        read -s -p "Enter Django Superuser Password for 'prvnkumar130' (or press Enter to skip): " SUPERUSER_PASSWORD
        echo ""
    fi
    
    echo "Fetching external IP..."
    EXTERNAL_IP=$(curl -s ifconfig.me || true)
    if [ -n "$EXTERNAL_IP" ] && [ -f .env ]; then
        sed -i 's/^APP_MODE=.*/APP_MODE=production/' .env
        sed -i "s/^ALLOWED_HOSTS=.*/ALLOWED_HOSTS=$EXTERNAL_IP,localhost/" .env
        echo "✅ Updated .env (APP_MODE=production, ALLOWED_HOSTS=$EXTERNAL_IP,localhost)"
    else
        echo "ℹ️ Kept existing ALLOWED_HOSTS configuration in .env."
    fi
}

# Function: Create Superuser
create_superuser() {
    if [ -z "$SUPERUSER_PASSWORD" ]; then
        return
    fi
    print_header "STEP: CONFIGURE SUPERUSER"
    DOCKER_COMPOSE_CMD=$(get_docker_compose_cmd)
    echo "Ensuring superuser 'prvnkumar130' is configured..."
    $DOCKER_COMPOSE_CMD exec -T app python manage.py shell -c "
from django.contrib.auth import get_user_model
User = get_user_model()
user, _ = User.objects.get_or_create(username='prvnkumar130', defaults={'email': '123@gmail.com', 'is_superuser': True, 'is_staff': True})
user.set_password('${SUPERUSER_PASSWORD}')
user.is_superuser = True
user.is_staff = True
user.save()
print('✅ Superuser prvnkumar130 verified.')
" 2>/dev/null || echo "ℹ️ Superuser step skipped."
}

# Execution Flow (All actions pull code first)
case $ACTION in
    deploy)
        pull_code
        configure_env
        rebuild_or_restart_docker
        if [ "$PRUNE_IMAGES" == "true" ]; then
            prune_system
        fi
        verify_health
        create_superuser
        ;;
    reinstall)
        pull_code
        reinstall_system
        configure_env
        FORCE_BUILD=true
        rebuild_or_restart_docker
        if [ "$PRUNE_IMAGES" == "true" ]; then
            prune_system
        fi
        verify_health
        create_superuser
        ;;
    pull_only)
        pull_code
        echo "✅ Pull-only action complete."
        ;;
    prune_only)
        pull_code
        prune_system
        ;;
    restart_only)
        pull_code
        restart_docker_fast
        if [ "$PRUNE_IMAGES" == "true" ]; then
            prune_system
        fi
        verify_health
        ;;
esac

print_header "DEPLOYMENT ACTION COMPLETE"
date
