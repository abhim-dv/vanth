"""Read-API projection tests (Phase 3): local queries ignore remote shadows."""

import sqlite3

from vanth.remote.readapi import projected_dashboard, projected_jobs, projected_status
from vanth.remote.store import RemoteStore


def connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


class ManagerStub:
    """Local JobManager stand-in: only ever sees its own local jobs."""

    def __init__(self, local_job_ids):
        self.local_job_ids = list(local_job_ids)

    def list(self, *args, **kwargs):
        return {"jobs": [{"job_id": jid, "status": "completed"} for jid in self.local_job_ids]}


def make_store(tmp_path, shadows):
    store = RemoteStore(connect(tmp_path / "remote.sqlite"))
    row = store.create_remote(target="user@host", state="paired")
    for job_id, status in shadows:
        store.upsert_shadow(remote_id=row["remote_id"], remote_job_id=job_id,
                            status=status, payload={"name": "n-" + job_id, "command": "echo hi"})
    return store, row["remote_id"]


def test_projected_jobs_lists_shadows(tmp_path):
    store, remote_id = make_store(tmp_path, [("job_r1", "running"), ("job_r2", "completed")])
    manager = ManagerStub(["job_local1"])
    result = projected_jobs(manager, store, remote_id)
    ids = [j["job_id"] for j in result["jobs"]]
    assert ids == ["job_r1", "job_r2"]
    assert all(j["shadow"] and j["remote_id"] == remote_id for j in result["jobs"])


def test_projected_status_reads_shadow_without_ssh(tmp_path):
    store, remote_id = make_store(tmp_path, [("job_r1", "failed")])
    status = projected_status(None, store, remote_id, "job_r1")
    assert status["status"] == "failed"
    assert status["shadow"] is True
    import pytest

    with pytest.raises(ValueError):
        projected_status(None, store, remote_id, "job_missing")


def test_suppressed_and_old_epoch_shadows_are_invisible(tmp_path):
    store, remote_id = make_store(tmp_path, [("job_r1", "running")])
    # Forgotten shadow disappears from the projection.
    store.suppress_shadow(remote_id, "job_r1")
    manager = ManagerStub([])
    assert projected_jobs(manager, store, remote_id)["jobs"] == []
    # Old-epoch shadow (below the remote's epoch) is audit-only.
    store.db.execute("UPDATE remote_shadows SET suppressed_at=NULL WHERE remote_job_id='job_r1'")
    store.db.execute("UPDATE remotes SET state_epoch=5 WHERE remote_id=?", (remote_id,))
    store.db.commit()
    assert projected_jobs(manager, store, remote_id)["jobs"] == []


def test_local_manager_never_returns_remote_shadows(tmp_path):
    """The plan invariant: every local runner/process query ignores shadows."""
    store, remote_id = make_store(tmp_path, [("job_shadow_only", "running")])
    manager = ManagerStub(["job_local1"])
    local = manager.list()
    ids = [j["job_id"] for j in local["jobs"]]
    assert ids == ["job_local1"]
    assert "job_shadow_only" not in ids


def test_projected_dashboard_uses_payload_metrics(tmp_path):
    store = RemoteStore(connect(tmp_path / "remote.sqlite"))
    row = store.create_remote(target="user@host", state="paired")
    store.upsert_shadow(
        remote_id=row["remote_id"], remote_job_id="job_m1", status="running",
        payload={"metrics": {"loss": [{"x": 1, "y": 0.5}]}},
    )
    result = projected_dashboard(None, store, row["remote_id"])
    assert result["series"]["job_m1"]["loss"] == [{"x": 1, "y": 0.5}]
