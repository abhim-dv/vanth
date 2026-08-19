"""Tests for rerun-with-overrides and status_batch QoL features."""

import asyncio
import subprocess
import sys

import pytest

from vanth.server import JobManager


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def wait_event(manager: JobManager, job_id: str, event_type: str) -> dict:
    return asyncio.run(manager.wait(job_id, [event_type], timeout_seconds=10))


def start_job(manager, code, **kwargs):
    return asyncio.run(manager.start(cmd(code), **kwargs))["job_id"]


def test_rerun_overrides_command_and_env(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('orig')", env={"K": "1"})
        wait_event(manager, job_id, "completed")

        reran = manager.rerun_sync(job_id, command=cmd("print('new')"), env={"K": "2"})
        assert reran["job_id"] != job_id
        new_id = reran["job_id"]
        wait_event(manager, new_id, "completed")

        status = manager.status(new_id)
        assert "print('new')" in status["command"]
        assert "print('orig')" not in status["command"]
        assert status["env"] == {"K": "2"}
    finally:
        manager.close()


def test_rerun_env_merge_preserves_stored_keys(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('x')", env={"K": "1", "STAY": "yes"})
        wait_event(manager, job_id, "completed")

        reran = manager.rerun_sync(job_id, env={"K": "2"})
        wait_event(manager, reran["job_id"], "completed")
        env = manager.status(reran["job_id"])["env"]
        assert env["K"] == "2"
        assert env["STAY"] == "yes"
    finally:
        manager.close()


def test_rerun_no_overrides_is_replay(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('same')", env={"K": "1"})
        wait_event(manager, job_id, "completed")

        reran = manager.rerun_sync(job_id)
        assert reran["job_id"] != job_id
        wait_event(manager, reran["job_id"], "completed")
        status = manager.status(reran["job_id"])
        assert status["command"] == manager.status(job_id)["command"]
        assert status["env"] == {"K": "1"}
    finally:
        manager.close()


def test_rerun_async_with_overrides(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('orig')", env={"K": "1"})
        wait_event(manager, job_id, "completed")
        reran = asyncio.run(manager.rerun(job_id, command=cmd("print('async')"), env={"K": "9"}))
        wait_event(manager, reran["job_id"], "completed")
        status = manager.status(reran["job_id"])
        assert "print('async')" in status["command"]
        assert status["env"] == {"K": "9"}
    finally:
        manager.close()


def test_rerun_override_timeout_name_tags_notes_cwd(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(
            manager, "import time; time.sleep(0.2)", name="original", tags=["old"], notes="old note"
        )
        wait_event(manager, job_id, "completed")
        reran = manager.rerun_sync(
            job_id,
            name="renamed",
            tags=["new"],
            notes="new note",
            cwd=str(tmp_path),
            timeout_seconds=120,
        )
        wait_event(manager, reran["job_id"], "completed")
        status = manager.status(reran["job_id"])
        assert status["name"] == "renamed"
        assert status["tags"] == ["new"]
        assert status["notes"] == "new note"
        assert status["cwd"] == str(tmp_path)
        assert status["timeout_seconds"] == 120
    finally:
        manager.close()


def test_status_batch_mixed_known_unknown(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_a = start_job(manager, "print('a')")
        job_b = start_job(manager, "print('b')")
        wait_event(manager, job_a, "completed")
        wait_event(manager, job_b, "completed")

        result = manager.status_batch([job_a, job_b, "job_bogus"])
        assert result["count"] == 3
        assert len(result["jobs"]) == 3
        assert {j["status"] for j in result["jobs"][:2]} == {"completed"}
        bogus = result["jobs"][2]
        assert bogus["status"] == "unknown"
        assert bogus["error"] == "Unknown job_id"
        assert result["unknown"] == ["job_bogus"]
    finally:
        manager.close()


def test_status_batch_empty_raises(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        with pytest.raises(ValueError, match="job_ids must be a non-empty list"):
            manager.status_batch([])
        with pytest.raises(ValueError, match="job_ids"):
            manager.status_batch(["a"] * 501)
        with pytest.raises(ValueError, match="job_ids must be a list of strings"):
            manager.status_batch(["a", 42])
        with pytest.raises(ValueError, match="job_ids must be a list of strings"):
            manager.status_batch([True])
    finally:
        manager.close()
