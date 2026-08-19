from __future__ import annotations

import asyncio
import subprocess
import sys
import time

import pytest

from vanth.server import JobManager


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def wait_status(manager: JobManager, job_id: str, status: str, timeout: float = 30) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = manager.status(job_id)
        if current["status"] == status:
            return current
        time.sleep(0.05)
    return manager.status(job_id)


def test_running_count_counts_running_rows(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        assert manager._running_count() == 0
        started = asyncio.run(manager.start(cmd("import time; time.sleep(3)")))
        try:
            assert manager._running_count() == 1
        finally:
            manager.stop_sync(started["job_id"], kill_after_seconds=2)
    finally:
        manager.close()


def test_max_running_jobs_defaults_to_unlimited(tmp_path, monkeypatch):
    monkeypatch.delenv("VANTH_MAX_RUNNING_JOBS", raising=False)
    manager = JobManager(tmp_path / "state")
    try:
        assert manager.max_running_jobs == 0
    finally:
        manager.close()


def test_concurrent_job_quota_rejects_third_job(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_MAX_RUNNING_JOBS", "2")
    manager = JobManager(tmp_path / "state")
    try:
        first = asyncio.run(manager.start(cmd("import time; time.sleep(30)")))
        second = asyncio.run(manager.start(cmd("import time; time.sleep(30)")))
        assert manager._running_count() == 2
        with pytest.raises(ValueError, match="quota"):
            asyncio.run(manager.start(cmd("import time; time.sleep(30)")))
        manager.stop_sync(first["job_id"], kill_after_seconds=2)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and manager.status(first["job_id"])["status"] == "running":
            time.sleep(0.05)
        assert manager.status(first["job_id"])["status"] == "cancelled"
        third = asyncio.run(manager.start(cmd("import time; time.sleep(5)")))
        assert third["status"] == "running"
        manager.stop_sync(second["job_id"], kill_after_seconds=2)
        manager.stop_sync(third["job_id"], kill_after_seconds=2)
    finally:
        manager.close()


def test_rerun_sync_inherits_concurrent_quota(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_MAX_RUNNING_JOBS", "1")
    manager = JobManager(tmp_path / "state")
    try:
        first = asyncio.run(manager.start(cmd("import time; time.sleep(30)")))
        assert manager._running_count() == 1
        with pytest.raises(ValueError, match="quota"):
            manager.rerun_sync(first["job_id"])
        manager.stop_sync(first["job_id"], kill_after_seconds=2)
    finally:
        manager.close()


def test_retention_disabled_by_default(tmp_path, monkeypatch):
    monkeypatch.delenv("VANTH_RETENTION_SECONDS", raising=False)
    monkeypatch.delenv("VANTH_RETENTION_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("VANTH_RETENTION_DRY_RUN", raising=False)
    manager = JobManager(tmp_path / "state")
    try:
        assert manager.max_retention_seconds == 0
        assert manager.retention_interval_seconds == 3600
        assert manager.retention_dry_run is True
        assert manager._last_retention_run is None
        manager._maybe_auto_cleanup()
        assert manager._last_retention_run is None
    finally:
        manager.close()


def test_retention_removes_old_terminal_job_when_not_dry_run(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_RETENTION_SECONDS", "1")
    monkeypatch.setenv("VANTH_RETENTION_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("VANTH_RETENTION_DRY_RUN", "0")
    monkeypatch.setenv("VANTH_DELIVERY_POLL_INTERVAL", "3600")
    manager = JobManager(tmp_path / "state")
    try:
        started = asyncio.run(manager.start(cmd("print('done')")))
        job_id = started["job_id"]
        wait_status(manager, job_id, "completed")
        old_stamp = "2026-01-01T00:00:00Z"
        with manager.db_lock:
            manager.db.execute("UPDATE jobs SET updated_at=? WHERE job_id=?", (old_stamp, job_id))
            manager.db.commit()
        manager._maybe_auto_cleanup()
        assert manager._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)) is None
        assert manager._last_retention_run is not None
    finally:
        manager.close()


def test_retention_dry_run_reports_but_keeps_job(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_RETENTION_SECONDS", "1")
    monkeypatch.setenv("VANTH_RETENTION_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("VANTH_RETENTION_DRY_RUN", "1")
    monkeypatch.setenv("VANTH_DELIVERY_POLL_INTERVAL", "3600")
    manager = JobManager(tmp_path / "state")
    try:
        started = asyncio.run(manager.start(cmd("print('done')")))
        job_id = started["job_id"]
        wait_status(manager, job_id, "completed")
        old_stamp = "2026-01-01T00:00:00Z"
        with manager.db_lock:
            manager.db.execute("UPDATE jobs SET updated_at=? WHERE job_id=?", (old_stamp, job_id))
            manager.db.commit()
        result = manager._maybe_auto_cleanup()
        assert result["count"] == 1
        assert result["dry_run"] is True
        assert manager.status(job_id)["status"] == "completed"
    finally:
        manager.close()


def test_retention_does_not_remove_fresh_jobs(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_RETENTION_SECONDS", "3600")
    monkeypatch.setenv("VANTH_RETENTION_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("VANTH_RETENTION_DRY_RUN", "0")
    monkeypatch.setenv("VANTH_DELIVERY_POLL_INTERVAL", "3600")
    manager = JobManager(tmp_path / "state")
    try:
        started = asyncio.run(manager.start(cmd("print('done')")))
        job_id = started["job_id"]
        wait_status(manager, job_id, "completed")
        result = manager._maybe_auto_cleanup()
        assert result["count"] == 0
        assert manager.status(job_id)["status"] == "completed"
    finally:
        manager.close()


def test_retention_interval_guards_sweeps(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_RETENTION_SECONDS", "1")
    monkeypatch.setenv("VANTH_RETENTION_INTERVAL_SECONDS", "3600")
    monkeypatch.setenv("VANTH_RETENTION_DRY_RUN", "1")
    monkeypatch.setenv("VANTH_DELIVERY_POLL_INTERVAL", "3600")
    manager = JobManager(tmp_path / "state")
    try:
        old_stamp = "2026-01-01T00:00:00Z"
        with manager.db_lock:
            manager.db.execute(
                "INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, events_path) VALUES (?, ?, 'completed', ?, ?, ?, ?, ?)",
                ("job_old", "true", old_stamp, old_stamp, str(manager.logs / "a.out"), str(manager.logs / "a.err"), str(manager.events_dir / "a.jsonl")),
            )
            manager.db.commit()
        first = manager._maybe_auto_cleanup()
        assert first["count"] == 1
        stamp = manager._last_retention_run
        assert stamp is not None
        second = manager._maybe_auto_cleanup()
        assert second is None
        assert manager._last_retention_run == stamp
    finally:
        manager.close()


def test_doctor_reports_quota_and_retention(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_MAX_RUNNING_JOBS", "4")
    monkeypatch.setenv("VANTH_RETENTION_SECONDS", "7200")
    monkeypatch.setenv("VANTH_RETENTION_INTERVAL_SECONDS", "1800")
    monkeypatch.setenv("VANTH_RETENTION_DRY_RUN", "1")
    manager = JobManager(tmp_path / "state")
    try:
        started = asyncio.run(manager.start(cmd("import time; time.sleep(3)")))
        try:
            report = manager.doctor()
            assert report["running_jobs"] == 1
            assert report["max_running_jobs"] == 4
            assert report["retention"] == {
                "seconds": 7200,
                "interval_seconds": 1800,
                "dry_run": True,
            }
        finally:
            manager.stop_sync(started["job_id"], kill_after_seconds=2)
    finally:
        manager.close()
