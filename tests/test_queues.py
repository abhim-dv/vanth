"""Pools, priority, and pause/resume for queued jobs (#5)."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import time

import pytest

from vanth.server import JobManager


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


SLEEP = "import time; time.sleep(30)"


def _launched(manager: JobManager, job_id: str) -> bool:
    return manager.status(job_id)["status"] in {"launching", "running"}


def test_pool_job_queues_and_dispatches_by_priority(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.pool_configure("p", max_parallel=1)
        low = asyncio.run(manager.start(cmd(SLEEP), pool="p", priority=0))
        high = asyncio.run(manager.start(cmd(SLEEP), pool="p", priority=5))
        assert low["status"] == "queued" and high["status"] == "queued"

        manager._dispatch_queued_jobs()
        assert _launched(manager, high["job_id"]), "higher priority must launch first"
        assert manager.status(low["job_id"])["status"] == "queued", "pool cap 1 must hold the rest"

        manager.stop_sync(high["job_id"])
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not _launched(manager, low["job_id"]):
            manager._dispatch_queued_jobs()
            time.sleep(0.05)
        assert _launched(manager, low["job_id"]), "freeing capacity must launch the waiter"
        manager.stop_sync(low["job_id"])
    finally:
        manager.close()


def test_pool_pause_holds_then_resume_drains(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.pool_configure("p", max_parallel=2, paused=True)
        job = asyncio.run(manager.start(cmd(SLEEP), pool="p"))
        manager._dispatch_queued_jobs()
        assert manager.status(job["job_id"])["status"] == "queued", "paused pool must not launch"

        manager.pool_configure("p", max_parallel=2, paused=False)
        manager._dispatch_queued_jobs()
        assert _launched(manager, job["job_id"])
        manager.stop_sync(job["job_id"])

        listed = manager.pool_list()["pools"]
        assert listed and listed[0]["pool"] == "p"
    finally:
        manager.close()


def test_job_pause_and_resume(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.pool_configure("p", max_parallel=1)
        job = asyncio.run(manager.start(cmd(SLEEP), pool="p"))
        manager.job_pause(job["job_id"])
        manager._dispatch_queued_jobs()
        assert manager.status(job["job_id"])["status"] == "queued"

        manager.job_resume(job["job_id"])
        manager._dispatch_queued_jobs()
        assert _launched(manager, job["job_id"])
        manager.stop_sync(job["job_id"])
    finally:
        manager.close()


def test_pause_running_job_rejected(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        started = asyncio.run(manager.start(cmd(SLEEP)))
        for _ in range(200):
            if manager.status(started["job_id"])["status"] == "running":
                break
            time.sleep(0.05)
        with pytest.raises(ValueError, match="only a queued job"):
            manager.job_pause(started["job_id"])
        manager.stop_sync(started["job_id"])
    finally:
        manager.close()


def test_global_quota_limits_queued_dispatch(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_MAX_RUNNING_JOBS", "1")
    manager = JobManager(tmp_path, recover=False)
    try:
        direct = asyncio.run(manager.start(cmd(SLEEP)))
        for _ in range(200):
            if manager.status(direct["job_id"])["status"] == "running":
                break
            time.sleep(0.05)
        queued = asyncio.run(manager.start(cmd(SLEEP), pool="p"))
        assert queued["status"] == "queued"
        manager._dispatch_queued_jobs()
        assert manager.status(queued["job_id"])["status"] == "queued", "global quota must still gate"

        manager.stop_sync(direct["job_id"])
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not _launched(manager, queued["job_id"]):
            manager._dispatch_queued_jobs()
            time.sleep(0.05)
        assert _launched(manager, queued["job_id"])
        manager.stop_sync(queued["job_id"])
    finally:
        manager.close()


def test_trigger_and_pool_gate_together(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.pool_configure("p", max_parallel=1)
        parent = asyncio.run(manager.start(cmd(SLEEP)))
        child = asyncio.run(manager.start(
            cmd(SLEEP),
            trigger={"job_id": parent["job_id"], "status": "completed"},
            pool="p",
        ))
        assert child["status"] == "queued"
        manager._dispatch_queued_jobs()
        assert manager.status(child["job_id"])["status"] == "queued", "trigger not satisfied yet"
        manager.stop_sync(parent["job_id"])
    finally:
        manager.close()
