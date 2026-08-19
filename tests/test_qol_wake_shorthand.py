"""Tests for the daemon_wake shorthand (_build_wake_target) + wake delivery.

Uses the same patterns as test_qol_mcp.py: a direct JobManager instance with a
tmp_path home and real jobs started via asyncio.run(manager.start(...)).
"""

import asyncio
import subprocess
import sys
import time

import pytest

from vanth.server import JobManager, _build_wake_target


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def wait_event(manager: JobManager, job_id: str, event_type: str) -> dict:
    return asyncio.run(manager.wait(job_id, [event_type], timeout_seconds=10))


def start_job(manager, code, **kwargs):
    return asyncio.run(manager.start(cmd(code), **kwargs))["job_id"]


def test_build_wake_target_passthrough():
    target = {"type": "codex_thread", "events": ["completed"], "session_id": "s1"}
    assert _build_wake_target(target, None, None, {}) is target


def test_build_wake_target_shorthand():
    assert _build_wake_target(None, ["checkpoint"], "local_command", {}) == {
        "type": "local_command",
        "events": ["checkpoint"],
    }
    assert _build_wake_target(None, None, "opencode_thread", {"session_id": "s1"}) == {
        "type": "opencode_thread",
        "events": ["completed", "failed"],
        "session_id": "s1",
    }
    assert _build_wake_target(None, None, "local_command", {"command": ["echo", "hi"]}) == {
        "type": "local_command",
        "events": ["completed", "failed"],
        "command": ["echo", "hi"],
    }


def test_build_wake_target_validation():
    with pytest.raises(ValueError, match="type is required"):
        _build_wake_target(None, None, None, {})
    with pytest.raises(ValueError, match="unsupported wake target type"):
        _build_wake_target(None, None, "http", {})
    with pytest.raises(ValueError, match="non-empty"):
        _build_wake_target(None, [], "local_command", {})
    with pytest.raises(ValueError, match="non-empty"):
        _build_wake_target(None, ["x", 3], "local_command", {})
    with pytest.raises(ValueError, match="type is required"):
        _build_wake_target(None, None, "", {})
    with pytest.raises(ValueError, match="dict"):
        _build_wake_target("http", None, None, {})


def test_wake_shorthand_enqueues_delivery_on_completion(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('wake shorthand')")
        result = manager.add_wake_target(
            job_id,
            _build_wake_target(None, ["completed"], "local_command", {"command": [sys.executable, "-c", "import sys; sys.exit(0)"]}),
        )
        assert result["result"] == "ok"
        assert result["job_id"] == job_id
        assert result["target_id"].startswith("target_")
        assert result["target_type"] == "local_command"
        assert result["events"] == ["completed"]

        wait_event(manager, job_id, "completed")
        deadline = time.monotonic() + 10
        deliveries = []
        while time.monotonic() < deadline:
            deliveries = manager.deliveries(job_id)["deliveries"]
            if any(d["target_type"] == "local_command" for d in deliveries):
                break
            time.sleep(0.05)
        assert any(d["target_type"] == "local_command" for d in deliveries)
    finally:
        manager.close()


def test_add_wake_target_rejects_unsupported_type(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('reject')")
        with pytest.raises(ValueError, match="unsupported wake target type"):
            manager.add_wake_target(job_id, {"type": "http", "events": ["completed"]})
    finally:
        manager.close()
