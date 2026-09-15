"""Tests for the durable approval/decision primitive.

A decision is a small state machine (pending -> resolved/withdrawn/expired)
attached to a non-terminal job, kept out of ``jobs.status``. Requesting one
emits ``decision_requested`` (which reuses the wake-target delivery path so the
owning thread is notified); resolving emits ``decision_resolved``. Covers the
lifecycle, expiry (including the maintenance sweep), input validation,
idempotency, wake delivery, ``job_wait`` integration, and the v15->v16
migration.
"""

import asyncio
import os
import socket
import subprocess
import sys
import time

import pytest

from vanth.client import VanthClient
from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def running_job(manager: JobManager) -> str:
    started = asyncio.run(manager.start(cmd("import time; time.sleep(20)")))
    job_id = started["job_id"]
    asyncio.run(manager.wait(job_id, ["started"], timeout_seconds=10))
    return job_id


def event_types(manager: JobManager, job_id: str) -> list[str]:
    rows = manager.db.execute("SELECT type FROM events WHERE job_id=? ORDER BY seq", (job_id,)).fetchall()
    return [row["type"] for row in rows]


def decision_row(manager: JobManager, decision_id: str):
    return manager.db.execute("SELECT * FROM decisions WHERE decision_id=?", (decision_id,)).fetchone()


def test_request_decision_is_pending_and_wakes_owner(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = running_job(manager)
        manager.add_wake_target(job_id, {
            "type": "local_command",
            "command": cmd("pass"),
            "events": ["decision_requested"],
            "auto_dispatch": False,
        })
        decision = manager.request_decision(job_id, "Ship the release?")
        assert decision["status"] == "pending"
        assert decision["options"] == ["approve", "deny"]
        assert decision["choice"] is None
        assert decision["expires_at"] is None
        assert "decision_requested" in event_types(manager, job_id)
        # The owner is notified through the ordinary delivery queue.
        deliveries = manager.db.execute(
            "SELECT target_type, status FROM deliveries WHERE job_id=?", (job_id,)
        ).fetchall()
        assert [row["target_type"] for row in deliveries] == ["local_command"]
        # The job itself is untouched.
        assert manager.status(job_id)["status"] == "running"
    finally:
        manager.close()


def test_request_decision_without_wake_target_still_records(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = running_job(manager)
        decision = manager.request_decision(job_id, "Proceed?", options=["yes", "no"])
        assert decision_row(manager, decision["decision_id"])["status"] == "pending"
        assert manager.db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 0
    finally:
        manager.close()


def test_resolve_records_choice_and_is_waitable(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = running_job(manager)
        decision = manager.request_decision(job_id, "Deploy?", options=["approve", "deny"])
        resolved = manager.resolve_decision(job_id, decision["decision_id"], "approve")
        assert resolved["status"] == "resolved"
        assert resolved["choice"] == "approve"
        assert resolved["resolved_by"] == "user"
        assert resolved["resolved_at"] is not None
        assert event_types(manager, job_id)[-1] == "decision_resolved"

        waited = asyncio.run(manager.wait(job_id, ["decision_resolved"], timeout_seconds=5))
        assert waited["result"] == "event"
        assert waited["event"]["type"] == "decision_resolved"
        assert waited["event"]["data"]["choice"] == "approve"
    finally:
        manager.close()


def test_resolve_is_idempotent_for_same_choice_and_conflicts_otherwise(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = running_job(manager)
        decision = manager.request_decision(job_id, "Deploy?", options=["approve", "deny"])
        first = manager.resolve_decision(job_id, decision["decision_id"], "deny")
        again = manager.resolve_decision(job_id, decision["decision_id"], "deny")
        assert again["choice"] == first["choice"] == "deny"
        with pytest.raises(ValueError, match="already resolved"):
            manager.resolve_decision(job_id, decision["decision_id"], "approve")
    finally:
        manager.close()


def test_withdraw_decision(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = running_job(manager)
        decision = manager.request_decision(job_id, "Deploy?")
        withdrawn = manager.withdraw_decision(job_id, decision["decision_id"])
        assert withdrawn["status"] == "withdrawn"
        assert event_types(manager, job_id)[-1] == "decision_withdrawn"
        with pytest.raises(ValueError, match="withdrawn"):
            manager.resolve_decision(job_id, decision["decision_id"], "approve")
    finally:
        manager.close()


def test_expired_decision_cannot_be_resolved_and_sweep_emits(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = running_job(manager)
        decision = manager.request_decision(job_id, "Deploy?", timeout_seconds=3600)
        assert decision["expires_at"] is not None
        # Simulate the clock passing without sleeping.
        manager.db.execute(
            "UPDATE decisions SET expires_at=? WHERE decision_id=?",
            ("2000-01-01T00:00:00Z", decision["decision_id"]),
        )
        manager.db.commit()
        manager._expire_decisions()
        assert decision_row(manager, decision["decision_id"])["status"] == "expired"
        assert "decision_expired" in event_types(manager, job_id)
        with pytest.raises(ValueError, match="expired"):
            manager.resolve_decision(job_id, decision["decision_id"], "approve")
    finally:
        manager.close()


def test_expiry_sweep_leaves_future_decisions_pending(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = running_job(manager)
        decision = manager.request_decision(job_id, "Deploy?", timeout_seconds=3600)
        manager._expire_decisions()
        assert decision_row(manager, decision["decision_id"])["status"] == "pending"
    finally:
        manager.close()


def test_request_decision_validates_inputs(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = running_job(manager)
        with pytest.raises(ValueError, match="non-empty"):
            manager.request_decision(job_id, "   ")
        with pytest.raises(ValueError, match="options"):
            manager.request_decision(job_id, "Deploy?", options=[])
        with pytest.raises(ValueError, match="options"):
            manager.request_decision(job_id, "Deploy?", options=["ok", ""])
        with pytest.raises(ValueError, match="timeout_seconds"):
            manager.request_decision(job_id, "Deploy?", timeout_seconds=0)
        with pytest.raises(ValueError, match="Unknown job_id"):
            manager.request_decision("job_missing", "Deploy?")
        assert manager.db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
    finally:
        manager.close()


def test_request_decision_rejects_terminal_job(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = asyncio.run(manager.start(cmd("print('done')")))
        job_id = started["job_id"]
        asyncio.run(manager.wait(job_id, ["completed"], timeout_seconds=10))
        with pytest.raises(ValueError, match="terminal"):
            manager.request_decision(job_id, "Too late?")
    finally:
        manager.close()


def test_resolve_rejects_unknown_choice_and_mismatched_token(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = running_job(manager)
        other = running_job(manager)
        decision = manager.request_decision(job_id, "Deploy?", options=["approve", "deny"])
        with pytest.raises(ValueError, match="choice must be one of"):
            manager.resolve_decision(job_id, decision["decision_id"], "maybe")
        with pytest.raises(ValueError, match="Unknown decision"):
            manager.resolve_decision(other, decision["decision_id"], "approve")
        with pytest.raises(ValueError, match="token"):
            manager.resolve_decision(job_id, "", "approve")
    finally:
        manager.close()


def test_list_decisions_filters_by_job_and_status(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = running_job(manager)
        other = running_job(manager)
        first = manager.request_decision(job_id, "One")
        manager.request_decision(job_id, "Two")
        manager.request_decision(other, "Three")
        manager.resolve_decision(job_id, first["decision_id"], "approve")

        assert manager.list_decisions(job_id=other)["count"] == 1
        assert manager.list_decisions(status="pending")["count"] == 2
        resolved = manager.list_decisions(job_id=job_id, status="resolved")
        assert [item["decision_id"] for item in resolved["decisions"]] == [first["decision_id"]]
        with pytest.raises(ValueError, match="status must be one of"):
            manager.list_decisions(status="bogus")
    finally:
        manager.close()


def test_v15_database_upgrades_to_decisions_table(tmp_path):
    home = tmp_path / "state"
    manager = JobManager(home)
    manager.db.execute("DROP TABLE decisions")
    manager.db.execute("PRAGMA user_version=15")
    manager.db.commit()
    manager.close()

    reopened = JobManager(home)
    try:
        assert reopened.doctor()["schema_version"] == 16
        assert reopened.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='decisions'"
        ).fetchone() is not None
    finally:
        reopened.close()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_decisions_over_http(tmp_path):
    """The daemon routes + client wrappers for the decision tools."""
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env={**os.environ, "VANTH_HOME": str(tmp_path / "state"), "VANTH_DAEMON_PORT": str(port)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = VanthClient(f"http://127.0.0.1:{port}", tmp_path / "state")
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                assert client.get("/health") == {"ok": True}
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)

        job = client.post("/jobs", {"command": cmd("import time; time.sleep(20)")})
        client.post(f"/jobs/{job['job_id']}/wait", {"filters": ["started"], "timeout_seconds": 10})

        decision = client.post(f"/jobs/{job['job_id']}/decision", {"prompt": "Deploy?", "options": ["approve", "deny"]})
        assert decision["status"] == "pending"
        assert client.get("/decisions", {"job_id": job["job_id"]})["count"] == 1

        resolved = client.post(f"/jobs/{job['job_id']}/decision/{decision['decision_id']}/resolve", {"choice": "approve"})
        assert resolved["status"] == "resolved" and resolved["choice"] == "approve"

        second = client.post(f"/jobs/{job['job_id']}/decision", {"prompt": "Again?", "options": ["yes"]})
        bad = client.post(f"/jobs/{job['job_id']}/decision/{second['decision_id']}/resolve", {"choice": "no"})
        assert bad["result"] == "error"
        withdrawn = client.post(f"/jobs/{job['job_id']}/decision/{second['decision_id']}/withdraw")
        assert withdrawn["status"] == "withdrawn"
        assert client.get("/decisions", {"status": "withdrawn"})["count"] == 1
    finally:
        proc.terminate()
        proc.wait(timeout=5)
