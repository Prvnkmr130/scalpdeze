# -*- coding: utf-8 -*-
import os
import json
import pytest
import django
from unittest.mock import patch, MagicMock

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "algo_trading.settings")
django.setup()

from django.test import RequestFactory
from django.contrib.auth.models import User
from django.contrib.admin.sites import AdminSite
from kalai.models import Broker
from kalai.admin import AccountAdmin
from kalai.views import (
    deployment_hub_view,
    git_info_api,
    trigger_deploy_api,
    deploy_status_api,
)
from kalai.deployment import (
    sanitize_branch_name,
    scrub_sensitive_text,
    get_git_metadata,
    is_deployment_active,
    trigger_host_deployment,
    get_deployment_status,
)


@pytest.fixture
def superuser(db):
    user = User.objects.filter(username="test_admin").first()
    if not user:
        user = User.objects.create_superuser("test_admin", "admin@test.com", "password123")
    return user


@pytest.fixture
def regular_staff(db):
    user = User.objects.filter(username="test_staff").first()
    if not user:
        user = User.objects.create_user("test_staff", "staff@test.com", "password123", is_staff=True, is_superuser=False)
    return user


@pytest.fixture
def non_staff(db):
    user = User.objects.filter(username="test_user").first()
    if not user:
        user = User.objects.create_user("test_user", "user@test.com", "password123", is_staff=False)
    return user


def test_sanitize_branch_name_valid():
    assert sanitize_branch_name("main") == "main"
    assert sanitize_branch_name("feature/deploy-hub") == "feature/deploy-hub"
    assert sanitize_branch_name("v1.2.3") == "v1.2.3"
    assert sanitize_branch_name("") == "main"
    assert sanitize_branch_name(None) == "main"


def test_sanitize_branch_name_injection_prevention():
    with pytest.raises(ValueError):
        sanitize_branch_name("main; rm -rf /")

    with pytest.raises(ValueError):
        sanitize_branch_name("main && echo hacked")

    with pytest.raises(ValueError):
        sanitize_branch_name("../../../etc/passwd")

    with pytest.raises(ValueError):
        sanitize_branch_name("-flag-injection")


def test_scrub_sensitive_text(monkeypatch):
    monkeypatch.setenv("DJANGO_SECRET_KEY", "super_secret_django_key_12345")
    raw_log = "Error on key super_secret_django_key_12345 while connecting."
    scrubbed = scrub_sensitive_text(raw_log)
    assert "super_secret_django_key_12345" not in scrubbed
    assert "********" in scrubbed


def test_get_git_metadata_returns_structure():
    meta = get_git_metadata()
    assert isinstance(meta, dict)
    assert "branch" in meta
    assert "commit_hash" in meta
    assert "short_hash" in meta
    assert "author" in meta
    assert "message" in meta
    assert "remote_status" in meta
    assert "available_branches" in meta


def test_git_info_api_auth(superuser, non_staff):
    rf = RequestFactory()

    # 1. Non-staff -> 401 Unauthorized
    req = rf.get("/api/deploy/git-info/")
    req.user = non_staff
    resp = git_info_api(req)
    assert resp.status_code == 401

    # 2. Superuser -> 200 OK
    req = rf.get("/api/deploy/git-info/")
    req.user = superuser
    resp = git_info_api(req)
    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data["success"] is True
    assert "git" in data


def test_trigger_deploy_api_privilege_gating(superuser, regular_staff, non_staff):
    rf = RequestFactory()

    # 1. Non-staff -> 401
    req = rf.post("/api/deploy/trigger/", json.dumps({"branch": "main"}), content_type="application/json")
    req.user = non_staff
    resp = trigger_deploy_api(req)
    assert resp.status_code == 401

    # 2. Regular staff (non-superuser) -> 403 Forbidden
    req = rf.post("/api/deploy/trigger/", json.dumps({"branch": "main"}), content_type="application/json")
    req.user = regular_staff
    resp = trigger_deploy_api(req)
    assert resp.status_code == 403
    data = json.loads(resp.content)
    assert "Superuser privileges required" in data["error"]

    # 3. Superuser -> 200 OK (dispatched)
    with patch("kalai.deployment.trigger_host_deployment") as mock_trigger:
        mock_trigger.return_value = {
            "success": True,
            "message": "Deployment dispatched",
            "job_id": "20260901_180000",
            "branch": "main",
        }
        req = rf.post("/api/deploy/trigger/", json.dumps({"branch": "main"}), content_type="application/json")
        req.user = superuser
        resp = trigger_deploy_api(req)
        assert resp.status_code == 200
        data = json.loads(resp.content)
        assert data["success"] is True
        assert data["job_id"] == "20260901_180000"


def test_trigger_deploy_api_mutex_busy(superuser):
    rf = RequestFactory()

    with patch("kalai.deployment.trigger_host_deployment") as mock_trigger:
        mock_trigger.return_value = {
            "success": False,
            "error": "A deployment is already actively running.",
            "status": "BUSY",
        }
        req = rf.post("/api/deploy/trigger/", json.dumps({"branch": "main"}), content_type="application/json")
        req.user = superuser
        resp = trigger_deploy_api(req)
        assert resp.status_code == 409
        data = json.loads(resp.content)
        assert data["success"] is False
        assert "already actively running" in data["error"]


def test_deploy_status_api(superuser, tmp_path, monkeypatch):
    fake_log = tmp_path / "deploy.log"
    fake_log.write_text("Line 1: Git pull starting...\nLine 2: Docker containers updated and running.\n", encoding="utf-8")
    monkeypatch.setattr("kalai.deployment.DEPLOY_LOG_FILE", str(fake_log))
    monkeypatch.setattr("kalai.deployment.is_deployment_active", lambda: False)

    rf = RequestFactory()
    req = rf.get("/api/deploy/status/?offset=0")
    req.user = superuser
    resp = deploy_status_api(req)
    assert resp.status_code == 200
    data = json.loads(resp.content)
    assert data["success"] is True
    assert data["state"] == "SUCCESS"
    assert "Docker containers updated" in data["log_chunk"]


def test_deployment_hub_view_rendering(superuser):
    rf = RequestFactory()
    req = rf.get("/admin/kalai/broker/deployment-hub/")
    req.user = superuser

    site = AdminSite()
    admin_inst = AccountAdmin(Broker, site)
    resp = admin_inst.deployment_hub_view(req)
    assert resp.status_code == 200
    assert b"Cloud Deployment & Git Operations Hub" in resp.content
    assert b"Live Git Repository Information" in resp.content


def test_resolve_log_and_lock_paths_when_directory(tmp_path, monkeypatch):
    from kalai.deployment import resolve_log_file_path, resolve_lock_file_path

    dir_log = tmp_path / "deltazero-deploy.log"
    dir_log.mkdir()
    dir_lock = tmp_path / "deltazero-deploy.lock"
    dir_lock.mkdir()

    monkeypatch.setattr("kalai.deployment.DEPLOY_LOG_FILE", str(dir_log))
    monkeypatch.setattr("kalai.deployment.DEPLOY_LOCK_FILE", str(dir_lock))

    eff_log = resolve_log_file_path()
    eff_lock = resolve_lock_file_path()

    assert eff_log == str(dir_log / "deltazero-deploy.log")
    assert eff_lock == str(dir_lock / "deltazero-deploy.lock")


def test_trigger_host_deployment_directory_collision(tmp_path, monkeypatch):
    from kalai.deployment import trigger_host_deployment

    dir_log = tmp_path / "deltazero-deploy.log"
    dir_log.mkdir()
    dir_lock = tmp_path / "deltazero-deploy.lock"
    dir_lock.mkdir()

    monkeypatch.setattr("kalai.deployment.DEPLOY_LOG_FILE", str(dir_log))
    monkeypatch.setattr("kalai.deployment.DEPLOY_LOCK_FILE", str(dir_lock))
    monkeypatch.setattr("kalai.deployment.DEPLOY_FIFO_PATH", str(tmp_path / "non_existent.fifo"))

    # Popen should not crash with [Errno 21] Is a directory
    with patch("subprocess.Popen") as mock_popen, \
         patch("kalai.deployment.is_deployment_active", return_value=False):
        res = trigger_host_deployment(branch="main", operator_username="admin", client_ip="127.0.0.1")
        assert res["success"] is True
        assert (dir_lock / "deltazero-deploy.lock").exists()

