# -*- coding: utf-8 -*-
"""
kalai/deployment.py
───────────────────
Core engine for managing Linux host deployments, querying Git metadata,
dispatching out-of-container trigger signals, and streaming sanitized execution logs.
"""

import os
import re
import time
import subprocess
from datetime import datetime
from django.conf import settings
from django.db import transaction

# Mutex and FIFO paths
DEPLOY_LOCK_FILE = os.getenv("DEPLOY_LOCK_FILE", "/tmp/deltazero-deploy.lock" if os.name != "nt" else os.path.join(settings.BASE_DIR, "logs", "deploy.lock"))
DEPLOY_FIFO_PATH = os.getenv("DEPLOY_FIFO_PATH", "/run/deltazero-deploy.fifo" if os.name != "nt" else os.path.join(settings.BASE_DIR, "logs", "deploy.fifo"))
DEPLOY_LOG_FILE = os.getenv("DEPLOY_LOG_FILE", "/var/log/deltazero-deploy.log" if os.name != "nt" else os.path.join(settings.BASE_DIR, "logs", "deploy.log"))

# Regex for strict branch name validation (prevents command injection)
BRANCH_REGEX = re.compile(r"^[a-zA-Z0-9._\-/]{1,64}$")

# Secrets to scrub from output
SECRET_KEYS_TO_SCRUB = [
    "DJANGO_SECRET_KEY",
    "POSTGRES_PASSWORD",
    "M2M_SERVER_KEY",
    "SUPERUSER_PASSWORD",
    "TOTP_SECRET",
    "API_KEY",
    "API_SECRET",
    "ACCESS_TOKEN",
]


def sanitize_branch_name(branch: str) -> str:
    """Validate and sanitize branch name against strict regex whitelist."""
    if not branch or not isinstance(branch, str):
        return "main"
    branch_clean = branch.strip()
    if not BRANCH_REGEX.match(branch_clean) or ".." in branch_clean or branch_clean.startswith("-"):
        raise ValueError(f"Invalid git branch name: '{branch_clean}'")
    return branch_clean


def scrub_sensitive_text(text: str) -> str:
    """Mask any sensitive configuration strings or passwords from terminal output."""
    if not text:
        return ""
    scrubbed = text
    # Mask known values from settings / env
    for var_name in SECRET_KEYS_TO_SCRUB:
        val = (os.getenv(var_name) or getattr(settings, var_name, "") or "").strip()
        if val and len(val) >= 6:
            scrubbed = scrubbed.replace(val, "********")
    return scrubbed


def _read_git_directory_fallback(base_dir: str, git_data: dict) -> dict:
    """Pure Python reader for .git metadata when git CLI is unavailable or errors."""
    git_dir = os.path.join(base_dir, ".git")
    if not os.path.isdir(git_dir):
        return git_data

    try:
        # 1. Branch from .git/HEAD
        head_file = os.path.join(git_dir, "HEAD")
        if os.path.isfile(head_file):
            with open(head_file, "r", encoding="utf-8", errors="ignore") as f:
                head_content = f.read().strip()
            if head_content.startswith("ref: refs/heads/"):
                git_data["branch"] = head_content[len("ref: refs/heads/"):]
            elif len(head_content) == 40:
                git_data["commit_hash"] = head_content
                git_data["short_hash"] = head_content[:7]

        # 2. Ref from branch
        branch_ref_file = os.path.join(git_dir, "refs", "heads", git_data["branch"])
        if os.path.isfile(branch_ref_file):
            with open(branch_ref_file, "r", encoding="utf-8", errors="ignore") as f:
                commit_hash = f.read().strip()
            if len(commit_hash) == 40:
                git_data["commit_hash"] = commit_hash
                git_data["short_hash"] = commit_hash[:7]

        # 3. Read last line of .git/logs/HEAD for author, timestamp, message
        logs_head_file = os.path.join(git_dir, "logs", "HEAD")
        if os.path.isfile(logs_head_file):
            with open(logs_head_file, "r", encoding="utf-8", errors="ignore") as f:
                lines = [l.strip() for l in f if l.strip()]
            if lines:
                last_line = lines[-1]
                parts = last_line.split("\t", 1)
                meta_part = parts[0].split()
                if len(meta_part) >= 5:
                    git_data["commit_hash"] = meta_part[1]
                    git_data["short_hash"] = meta_part[1][:7]
                    git_data["author"] = meta_part[2]
                    try:
                        ts = int(meta_part[-2])
                        dt = datetime.fromtimestamp(ts)
                        git_data["date"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                        diff_sec = int(time.time() - ts)
                        if diff_sec < 60:
                            git_data["relative_time"] = "just now"
                        elif diff_sec < 3600:
                            git_data["relative_time"] = f"{diff_sec // 60}m ago"
                        elif diff_sec < 86400:
                            git_data["relative_time"] = f"{diff_sec // 3600}h ago"
                        else:
                            git_data["relative_time"] = f"{diff_sec // 86400}d ago"
                    except Exception:
                        pass
                if len(parts) > 1:
                    raw_msg = parts[1]
                    if ":" in raw_msg:
                        raw_msg = raw_msg.split(":", 1)[1].strip()
                    git_data["message"] = raw_msg

        # 4. List branches from .git/refs/heads/
        heads_dir = os.path.join(git_dir, "refs", "heads")
        if os.path.isdir(heads_dir):
            b_list = os.listdir(heads_dir)
            if b_list:
                git_data["available_branches"] = sorted(b_list)

        git_data["remote_status"] = f"Tracked via host volume ({git_data['branch']})"
    except Exception:
        pass

    return git_data


def get_git_metadata() -> dict:
    """
    Safely extract live Git repository metadata using git CLI with graceful fallbacks.
    Returns:
        dict: branch, commit_hash, short_hash, author, date, relative_time, message, is_dirty, remote_status
    """
    base_dir = str(settings.BASE_DIR)
    git_data = {
        "branch": "main",
        "commit_hash": "unknown",
        "short_hash": "unknown",
        "author": "system",
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "relative_time": "unknown",
        "message": "No commit info available",
        "is_dirty": False,
        "dirty_count": 0,
        "remote_status": "Unknown",
        "available_branches": ["main"],
    }

    try:
        # 1. Current Branch
        res = subprocess.run(
            ["git", "-c", "safe.directory=*", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=base_dir,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if res.returncode == 0 and res.stdout.strip():
            git_data["branch"] = res.stdout.strip()

        # 2. Latest Commit Metadata: Hash, Short Hash, Author, Date, Relative Date, Subject
        res = subprocess.run(
            ["git", "-c", "safe.directory=*", "log", "-1", "--format=%H%x1f%h%x1f%an%x1f%ad%x1f%ar%x1f%s", "--date=iso"],
            cwd=base_dir,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if res.returncode == 0 and res.stdout.strip():
            parts = res.stdout.strip().split("\x1f")
            if len(parts) >= 6:
                git_data["commit_hash"] = parts[0]
                git_data["short_hash"] = parts[1]
                git_data["author"] = parts[2]
                git_data["date"] = parts[3]
                git_data["relative_time"] = parts[4]
                git_data["message"] = parts[5]

        # 3. Dirty Working Tree Check
        res = subprocess.run(
            ["git", "-c", "safe.directory=*", "status", "--porcelain"],
            cwd=base_dir,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if res.returncode == 0:
            lines = [l for l in res.stdout.splitlines() if l.strip()]
            git_data["is_dirty"] = len(lines) > 0
            git_data["dirty_count"] = len(lines)

        # 4. Remote Status (ahead/behind tracking)
        # Fetch latest remote refs so remote_status detects newly pushed GitHub commits
        try:
            subprocess.run(
                ["git", "-c", "safe.directory=*", "fetch", "--quiet", "origin", git_data["branch"]],
                cwd=base_dir,
                capture_output=True,
                text=True,
                timeout=6,
            )
        except Exception:
            pass

        res = subprocess.run(
            ["git", "-c", "safe.directory=*", "rev-list", "--left-right", "--count", f"origin/{git_data['branch']}...HEAD"],
            cwd=base_dir,
            capture_output=True,
            text=True,
            timeout=4,
        )
        if res.returncode == 0 and res.stdout.strip():
            counts = res.stdout.strip().split()
            if len(counts) == 2:
                behind, ahead = int(counts[0]), int(counts[1])
                if behind == 0 and ahead == 0:
                    git_data["remote_status"] = f"Up-to-date with origin/{git_data['branch']}"
                elif behind > 0 and ahead == 0:
                    git_data["remote_status"] = f"Behind origin/{git_data['branch']} by {behind} commit(s) — Ready to update"
                elif ahead > 0 and behind == 0:
                    git_data["remote_status"] = f"Ahead of origin/{git_data['branch']} by {ahead} commit(s)"
                else:
                    git_data["remote_status"] = f"Diverged: {ahead} ahead, {behind} behind origin/{git_data['branch']}"

        # 5. List available local & remote branches
        res = subprocess.run(
            ["git", "-c", "safe.directory=*", "branch", "-a", "--format=%(refname:short)"],
            cwd=base_dir,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if res.returncode == 0 and res.stdout.strip():
            branches = set()
            for b in res.stdout.splitlines():
                clean_b = b.strip().replace("origin/", "")
                if clean_b and "HEAD" not in clean_b:
                    branches.add(clean_b)
            if branches:
                git_data["available_branches"] = sorted(list(branches))

    except Exception:
        pass

    # If git CLI was unavailable or commit_hash was unresolvable, invoke pure-Python .git fallback
    if git_data.get("commit_hash") == "unknown":
        git_data = _read_git_directory_fallback(base_dir, git_data)

    return git_data


def resolve_log_file_path() -> str:
    """Return valid file path for deployment log, resolving directory mount collisions."""
    path = DEPLOY_LOG_FILE
    if os.path.isdir(path):
        return os.path.join(path, "deltazero-deploy.log")
    return path


def resolve_lock_file_path() -> str:
    """Return valid file path for deployment lock, resolving directory mount collisions."""
    path = DEPLOY_LOCK_FILE
    if os.path.isdir(path):
        return os.path.join(path, "deltazero-deploy.lock")
    return path


def is_deployment_active() -> bool:
    """Check if a deployment is currently running via mutex lock file."""
    lock_file = resolve_lock_file_path()
    if not os.path.exists(lock_file) or os.path.isdir(lock_file):
        return False
    try:
        # Check lock file age (stale lock guard after 20 minutes)
        mtime = os.path.getmtime(lock_file)
        if time.time() - mtime > 1200:  # 20 minutes
            try:
                os.remove(lock_file)
            except Exception:
                pass
            return False
        return True
    except Exception:
        return False


def trigger_host_deployment(branch: str = "main", operator_username: str = "system", client_ip: str = "", force_build: bool = False) -> dict:
    """
    Safely trigger out-of-container deployment on the Linux host.
    Returns:
        dict: success, message, job_id, timestamp
    """
    clean_branch = sanitize_branch_name(branch)

    # 1. Concurrency Mutex Lock Check
    if is_deployment_active():
        return {
            "success": False,
            "error": "A deployment is already actively running. Please wait for the current job to complete.",
            "status": "BUSY",
        }

    job_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    lock_file = resolve_lock_file_path()
    log_file = resolve_log_file_path()
    os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(lock_file) or ".", exist_ok=True)

    mode_label = "Full Docker Rebuild" if force_build else "Smart Fast Restart"

    # 2. Acquire Mutex Lock
    try:
        with open(lock_file, "w", encoding="utf-8") as f:
            f.write(f"JOB_ID={job_id}\nBRANCH={clean_branch}\nMODE={'build' if force_build else 'nobuild'}\nUSER={operator_username}\nIP={client_ip}\nTIME={datetime.now().isoformat()}\n")
    except Exception as e:
        return {"success": False, "error": f"Failed to acquire deployment lock: {str(e)}"}

    # 3. Log initial deployment entry in log file (truncate old output)
    init_msg = (
        f"====================================================\n"
        f"🚀 DEPLOYMENT JOB STARTED [ID: {job_id}]\n"
        f"Time: {datetime.now().isoformat()}\n"
        f"Branch: {clean_branch} (Mode: {mode_label})\n"
        f"Triggered By: {operator_username} (IP: {client_ip})\n"
        f"====================================================\n"
    )
    try:
        with open(log_file, "w", encoding="utf-8") as f:
            f.write(init_msg)
    except Exception:
        pass

    # 4. Audit Log in Database
    try:
        from kalai.models import AlgoLog
        AlgoLog.objects.create(
            algo_name="DEPLOY_ENGINE",
            log_type="INFO",
            component="SYSTEM",
            action_type="DEPLOY_START",
            message=f"[{datetime.now().isoformat()}] Deployment job #{job_id} initiated on branch '{clean_branch}' ({mode_label}) by user '{operator_username}'.",
        )
    except Exception:
        pass

    # 5. Dispatch signal to Host Deployment Runner via FIFO Pipe or Detached Subprocess
    fifo_dispatched = False
    build_flag = "build" if force_build else "nobuild"
    if os.name != "nt" and os.path.exists(DEPLOY_FIFO_PATH) and not os.path.isdir(DEPLOY_FIFO_PATH):
        try:
            # Non-blocking write to systemd FIFO trigger pipe
            fifo_fd = os.open(DEPLOY_FIFO_PATH, os.O_WRONLY | os.O_NONBLOCK)
            with os.fdopen(fifo_fd, "w") as fifo:
                fifo.write(f"{job_id}:{clean_branch}:{build_flag}\n")
            fifo_dispatched = True
        except Exception:
            fifo_dispatched = False

    if not fifo_dispatched:
        # Fallback: Trigger host runner script via detached background process
        deploy_script = "/usr/local/bin/deltazero-deploy" if os.path.exists("/usr/local/bin/deltazero-deploy") else os.path.join(str(settings.BASE_DIR), "deploy.sh")
        if os.path.exists(deploy_script):
            try:
                deploy_args = ["--deploy", "--branch", clean_branch]
                if force_build:
                    deploy_args.insert(1, "--build")
                cmd = ["bash", deploy_script] + deploy_args
                subprocess.Popen(
                    cmd,
                    cwd=str(settings.BASE_DIR),
                    stdout=open(log_file, "a"),
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            except Exception as e:
                # Remove lock if dispatch failed
                try:
                    os.remove(lock_file)
                except Exception:
                    pass
                return {"success": False, "error": f"Failed to spawn deployment process: {str(e)}"}

    return {
        "success": True,
        "message": f"Deployment job #{job_id} successfully dispatched for branch '{clean_branch}'.",
        "job_id": job_id,
        "branch": clean_branch,
        "timestamp": datetime.now().isoformat(),
    }


def get_deployment_status(job_id: str = "", offset: int = 0) -> dict:
    """
    Read incremental execution logs and check deployment state.
    Returns:
        dict: state (IDLE/RUNNING/SUCCESS/FAILED), new_offset, log_chunk, is_active
    """
    log_content = ""
    new_offset = offset
    log_file = resolve_log_file_path()
    lock_file = resolve_lock_file_path()

    if os.path.exists(log_file) and not os.path.isdir(log_file):
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                f.seek(offset)
                raw_chunk = f.read()
                new_offset = f.tell()
                log_content = scrub_sensitive_text(raw_chunk)
        except Exception:
            pass

    # Read tail of log to check for terminal status markers
    tail_content = ""
    if os.path.exists(log_file) and not os.path.isdir(log_file):
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(max(0, size - 8192))
                tail_content = f.read()
        except Exception:
            tail_content = log_content

    active = is_deployment_active()

    # If log contains terminal indicators, release active lock automatically
    has_failed = "❌ Error" in tail_content or "fatal:" in tail_content or "Deployment cancelled" in tail_content
    has_succeeded = "DEPLOYMENT ACTION COMPLETE" in tail_content or "Docker containers updated and running" in tail_content or "🎉 Health check PASSED" in tail_content

    if (has_failed or has_succeeded) and active:
        if os.path.exists(lock_file) and not os.path.isdir(lock_file):
            try:
                os.remove(lock_file)
            except Exception:
                pass
        active = False

    # Determine state
    if active:
        state = "RUNNING"
    elif has_failed:
        state = "FAILED"
    elif has_succeeded:
        state = "SUCCESS"
    else:
        state = "IDLE"

    return {
        "active": active,
        "state": state,
        "job_id": job_id,
        "offset": new_offset,
        "log_chunk": log_content,
    }
