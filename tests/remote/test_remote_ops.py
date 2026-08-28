"""Remote-side accepted operations and dispatch tests (Phase 2) — in-memory sqlite + fake launch."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from vanth.remote.protocol import VanthRemoteProtocolError, request_digest
from vanth.remote.remote import RemoteJobManager
from vanth.remote.store import RemoteOperationStore


def connect(path: Path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    from vanth.migrations import migrate

    migrate(db, path.parent)
    return db


def start_payload():
    return {"command": "echo hi"}


def request_frame(method="job.start", payload=None, key="key-ops-0001"):
    payload = payload if payload is not None else start_payload()
    return {
        "version": "1", "kind": "request", "request_id": "req_" + "0" * 32,
        "idempotency_key": key, "method": method, "payload": payload,
        "digest": request_digest(method, payload, key),
        "expected_state_epoch": 1, "expected_instance_id": "test-instance",
        "sent_at": "2026-08-20T12:00:00Z",
    }


class FakeManager:
    """Stand-in for JobManager: records prepare/launch calls."""

    def __init__(self, logs_dir=None, events_dir=None, launch_status="running", prepare_fails=False, db=None):
        self.logs = Path(logs_dir or ".")
        self.events_dir = Path(events_dir or ".")
        self.launch_status = launch_status
        self.prepare_fails = prepare_fails
        self.prepared = []
        self.launched = []
        self.db = db

    def prepare_launch(self, job_id):
        self.prepared.append(job_id)
        if self.prepare_fails:
            return None
        return {
            "job_id": job_id,
            "stdout_path": self.logs / f"{job_id}.stdout.log",
            "stderr_path": self.logs / f"{job_id}.stderr.log",
            "events_path": self.events_dir / f"{job_id}.jsonl",
            "spec_path": self.logs / f"{job_id}.json",
        }

    def _launch_prepared(self, launch):
        self.launched.append(launch["job_id"])
        return {"job_id": launch["job_id"], "status": self.launch_status}

    def stop_sync(self, job_id, signal="terminate", kill_after_seconds=10):
        self.stopped = getattr(self, "stopped", [])
        self.stopped.append(job_id)
        return {"job_id": job_id, "status": "cancelled"}


def make_jobs_db():
    """In-memory jobs/events tables mirroring the remote daemon's schema."""
    import sqlite3

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript(
        """
        CREATE TABLE jobs (
          job_id TEXT PRIMARY KEY, name TEXT, command TEXT NOT NULL, cwd TEXT,
          status TEXT NOT NULL, pid INTEGER, worker_pid INTEGER,
          runner_heartbeat_at TEXT, stop_requested_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          started_at TEXT, ended_at TEXT, exit_code INTEGER, timeout_seconds INTEGER,
          notify_on TEXT, origin_thread_id TEXT, wake_thread_id TEXT, tags_json TEXT,
          env_json TEXT, notes TEXT, run_json TEXT, stdout_path TEXT NOT NULL, stderr_path TEXT NOT NULL, events_path TEXT NOT NULL,
          trigger_json TEXT, policy_json TEXT
        );
        CREATE TABLE events (
          event_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, seq INTEGER NOT NULL,
          type TEXT NOT NULL, level TEXT, message TEXT, data_json TEXT,
          source TEXT, created_at TEXT NOT NULL
        );
        CREATE TABLE deliveries (
          delivery_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, target_id TEXT NOT NULL,
          job_id TEXT NOT NULL, target_type TEXT NOT NULL, status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """
    )
    return db


def seed_job(db, job_id="job_snap1", status="running", seq=1):
    stamp = "2026-01-01T00:00:00Z"
    db.execute(
        "INSERT INTO jobs(job_id, name, command, status, exit_code, created_at, updated_at, stdout_path, stderr_path, events_path) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'o', 'e', 'ev')",
        (job_id, "n-" + job_id, "echo hi", status, 0 if status == "completed" else None, stamp, stamp),
    )
    db.execute(
        "INSERT INTO events(event_id, job_id, seq, type, source, created_at) VALUES (?, ?, ?, 'progress', 'stdout', ?)",
        ("evt_" + job_id, job_id, seq, stamp),
    )
    db.commit()


def make_remote(tmp_path, **kwargs):
    store = RemoteOperationStore(connect(tmp_path / "remote.sqlite"))
    store.db.execute("UPDATE remote_state SET instance_id='test-instance' WHERE id=1")
    store.db.commit()
    kwargs.setdefault("db", store.db)
    fake = FakeManager(logs_dir=tmp_path, events_dir=tmp_path / "events", **kwargs)
    return RemoteJobManager(store, fake, home=tmp_path), store, fake


# ---------------------------------------------------------------------------
# handle_request atomicity
# ---------------------------------------------------------------------------


def test_handle_request_commits_op_queued_job_origin_in_one_transaction(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    response = remote.handle_request(request_frame())
    assert response["kind"] == "response"
    result = response["result"]
    assert result["status"] == "queued"
    job_id = result["job_id"]
    assert job_id.startswith("job_")

    ops = store.db.execute("SELECT * FROM remote_operations").fetchall()
    assert len(ops) == 1
    assert ops[0]["status"] == "queued"
    jobs = store.db.execute("SELECT * FROM jobs").fetchall()
    assert len(jobs) == 1
    assert jobs[0]["job_id"] == job_id
    assert jobs[0]["status"] == "queued"
    origins = store.db.execute("SELECT * FROM remote_job_origins").fetchall()
    assert len(origins) == 1
    assert origins[0]["remote_job_id"] == job_id
    assert origins[0]["launch_intent"] == "queued"
    assert fake.prepared == []  # not launched yet


def test_handle_request_forced_exception_rolls_back_everything(tmp_path, monkeypatch):
    remote, store, fake = make_remote(tmp_path)
    original = store.record_queued_job

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(store, "record_queued_job", boom)
    response = remote.handle_request(request_frame())
    assert response["kind"] == "error"
    assert response["code"] == "INVALID_REQUEST"
    assert store.db.execute("SELECT COUNT(*) FROM remote_operations").fetchone()[0] == 0
    assert store.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0
    # The origin mapping DDL + row were inside the same transaction, so nothing
    # from the failed acceptance is visible (table either absent or empty).


def test_handle_request_persists_policy_on_remote_job(tmp_path):
    """policy is forwarded through the remote protocol and persisted on the
    remote queued job row (review P1-5)."""
    remote, store, fake = make_remote(tmp_path)
    policy = {"restart": {"max_retries": 2, "backoff_seconds": 5}}
    response = remote.handle_request(request_frame(payload={"command": "echo hi", "policy": policy}))
    assert response["kind"] == "response"
    job_id = response["result"]["job_id"]
    row = store.db.execute("SELECT policy_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    assert json.loads(row["policy_json"]) == policy


def test_handle_request_rejects_invalid_policy(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    response = remote.handle_request(request_frame(payload={"command": "echo hi", "policy": {"on_failure": {"after_n": 0, "action": "alert"}}}))
    assert response["kind"] == "error"
    assert "policy" in response["message"]
    assert store.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0
    table = store.db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='remote_job_origins'"
    ).fetchone()
    if table:
        assert store.db.execute("SELECT COUNT(*) FROM remote_job_origins").fetchone()[0] == 0


# ---------------------------------------------------------------------------
# replay
# ---------------------------------------------------------------------------


def test_handle_request_replay_same_key_same_digest_no_second_job(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    first = remote.handle_request(request_frame())
    job_id = first["result"]["job_id"]
    second = remote.handle_request(request_frame())
    assert second["result"]["job_id"] == job_id
    assert second["result"].get("replayed") is True
    assert store.db.execute("SELECT COUNT(*) FROM remote_operations").fetchone()[0] == 1
    assert store.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1


def test_handle_request_replay_same_key_different_digest_rejected(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    remote.handle_request(request_frame())
    changed = request_frame(payload={"command": "echo DIFFERENT"})
    response = remote.handle_request(changed)
    assert response["kind"] == "error"
    assert response["code"] == "PROTOCOL_REPLAY_MISMATCH"
    assert store.db.execute("SELECT COUNT(*) FROM remote_operations").fetchone()[0] == 1


def test_handle_request_restart_between_acceptance_and_launch(tmp_path):
    path = tmp_path / "remote.sqlite"
    store = RemoteOperationStore(connect(path))
    store.db.execute("UPDATE remote_state SET instance_id='test-instance' WHERE id=1")
    store.db.commit()
    fake = FakeManager(logs_dir=tmp_path, events_dir=tmp_path / "events")
    remote = RemoteJobManager(store, fake, home=tmp_path)
    response = remote.handle_request(request_frame())
    job_id = response["result"]["job_id"]
    assert fake.prepared == []

    # Simulate a daemon restart: close and reopen the shared sqlite.
    store.db.close()
    reopened = RemoteOperationStore(connect(path))
    fake2 = FakeManager(logs_dir=tmp_path, events_dir=tmp_path / "events", db=reopened.db)
    remote2 = RemoteJobManager(reopened, fake2, home=tmp_path)
    remote2._dispatch_queued_ops()

    ops = reopened.db.execute("SELECT * FROM remote_operations").fetchall()
    assert ops[0]["status"] == "running"
    assert fake2.prepared == [job_id]
    assert fake2.launched == [job_id]


# ---------------------------------------------------------------------------
# dispatcher lifecycle
# ---------------------------------------------------------------------------


def test_dispatcher_launches_queued_op_through_terminal(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    response = remote.handle_request(request_frame())
    job_id = response["result"]["job_id"]
    remote._dispatch_queued_ops()
    op = store.db.execute("SELECT * FROM remote_operations").fetchone()
    # launched -> running happened during dispatch; the fake launch reported
    # "running", so the operation stays running (runner owns terminal state).
    assert op["status"] == "running"
    assert fake.prepared == [job_id]
    assert fake.launched == [job_id]


def test_dispatcher_marks_completed_when_launch_terminal(tmp_path):
    remote, store, fake = make_remote(tmp_path, launch_status="completed")
    remote.handle_request(request_frame())
    remote._dispatch_queued_ops()
    op = store.db.execute("SELECT * FROM remote_operations").fetchone()
    assert op["status"] == "completed"


def test_dispatcher_recovers_stuck_launched_after_restart(tmp_path):
    """A crash between the `launched` commit and the spawn must resume."""
    remote, store, fake = make_remote(tmp_path)
    response = remote.handle_request(request_frame())
    job_id = response["result"]["job_id"]
    op_id = store.db.execute("SELECT op_id FROM remote_operations").fetchone()["op_id"]
    # Simulate: op committed `launched` but the process never spawned.
    store.update_operation_status(op_id, "launched")

    store.db.close()
    reopened = RemoteOperationStore(connect(tmp_path / "remote.sqlite"))
    fake2 = FakeManager(logs_dir=tmp_path, events_dir=tmp_path / "events")
    remote2 = RemoteJobManager(reopened, fake2, home=tmp_path)
    remote2._dispatch_queued_ops()
    assert fake2.prepared == [job_id]
    assert fake2.launched == [job_id]
    assert reopened.db.execute("SELECT status FROM remote_operations").fetchone()["status"] == "running"


def test_dispatcher_converges_running_op_to_terminal(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    response = remote.handle_request(request_frame())
    job_id = response["result"]["job_id"]
    op_id = store.db.execute("SELECT op_id FROM remote_operations").fetchone()["op_id"]
    remote._dispatch_queued_ops()
    assert store.db.execute("SELECT status FROM remote_operations").fetchone()["status"] == "running"
    # The runner records a terminal status on the local jobs row.
    store.db.execute("UPDATE jobs SET status='completed', ended_at='2026-08-20T12:00:00Z' WHERE job_id=?", (job_id,))
    store.db.commit()
    remote._sync_terminal_ops()
    assert store.db.execute("SELECT status FROM remote_operations").fetchone()["status"] == "completed"


# ---------------------------------------------------------------------------
# job.stop / job.rerun dispatch (review P0-3: no phantom jobs)
# ---------------------------------------------------------------------------


def seed_running_job(store, job_id="job_target00001"):
    stamp = "2026-01-01T00:00:00Z"
    store.db.execute(
        "INSERT INTO jobs(job_id, name, command, status, created_at, updated_at, env_json, tags_json,"
        " stdout_path, stderr_path, events_path) "
        "VALUES (?, 'target', 'sleep 30', 'running', ?, ?, '{}', '[]', 'o', 'e', 'ev')",
        (job_id, stamp, stamp),
    )
    store.db.commit()
    return job_id


def test_stop_targets_existing_job_and_creates_no_new_job(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    job_id = seed_running_job(store)
    response = remote.handle_request(request_frame(method="job.stop", payload={"job_id": job_id}))
    assert response["kind"] == "response"
    result = response["result"]
    assert result["status"] == "cancelled"
    assert fake.stopped == [job_id]
    # Exactly ONE job exists — the target — stop never spawns work.
    jobs = store.db.execute("SELECT job_id FROM jobs").fetchall()
    assert len(jobs) == 1 and jobs[0]["job_id"] == job_id


def test_stop_replay_is_idempotent(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    job_id = seed_running_job(store)
    first = remote.handle_request(request_frame(method="job.stop", payload={"job_id": job_id}))
    second = remote.handle_request(request_frame(method="job.stop", payload={"job_id": job_id}))
    assert second["result"]["replayed"] is True
    assert second["result"]["op_id"] == first["result"]["op_id"]
    assert fake.stopped.count(job_id) == 1
    assert len(store.db.execute("SELECT 1 FROM jobs").fetchall()) == 1


def test_stop_unknown_job_reports_error_without_creating_work(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    response = remote.handle_request(request_frame(method="job.stop", payload={"job_id": "job_missing"}))
    assert response["kind"] == "response"
    assert "unknown" in (response["result"].get("error") or "")
    assert store.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_rerun_resolves_spec_and_queues_exactly_one_rerun(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    source = seed_running_job(store, "job_source000001")
    response = remote.handle_request(request_frame(
        method="job.rerun", key="key-rerun-0001", payload={"job_id": source},
    ))
    result = response["result"]
    assert result["status"] == "queued"
    new_id = result["job_id"]
    assert new_id != source
    row = store.db.execute("SELECT command, status FROM jobs WHERE job_id=?", (new_id,)).fetchone()
    assert row["command"] == "sleep 30" and row["status"] == "queued"
    assert store.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 2


def test_rerun_replay_returns_same_new_job(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    source = seed_running_job(store, "job_source000001")
    first = remote.handle_request(request_frame(
        method="job.rerun", key="key-rerun-0002", payload={"job_id": source},
    ))
    second = remote.handle_request(request_frame(
        method="job.rerun", key="key-rerun-0002", payload={"job_id": source},
    ))
    assert second["result"]["replayed"] is True
    assert second["result"]["job_id"] == first["result"]["job_id"]
    assert store.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 2


def test_job_status_method_validated_and_routed(tmp_path):
    remote, store, fake = make_remote(tmp_path)
    started = remote.handle_request(request_frame())
    job_id = started["result"]["job_id"]
    frame = request_frame(method="job.status", payload={"job_id": job_id}, key="key-status-02")
    response = remote.handle_request(frame)
    assert response["kind"] == "response"
    assert response["result"]["job_id"] == job_id
    assert response["result"]["found"] is True
    assert response["result"]["status"] == "queued"


def test_snapshot_and_log_range_serve_reads(tmp_path):
    """Phase 3: snapshot + log_range are implemented reads (no longer stubs)."""
    db = make_jobs_db()
    seed_job(db, "job_snap1")
    (tmp_path / "job_snap1.stdout.log").write_bytes(b"hello remote log\n")
    remote, store, fake = make_remote(tmp_path, db=db)
    snap = remote.handle_snapshot_request(request_frame(method="job.snapshot", payload={}))
    assert snap["kind"] == "response"
    assert snap["result"]["kind"] == "snapshot"
    assert snap["result"]["jobs"][0]["job_id"] == "job_snap1"
    logr = remote.handle_log_range_request(
        request_frame(method="job.log_range", payload={"remote_job_id": "job_snap1"})
    )
    assert logr["kind"] == "response"
    assert logr["result"]["kind"] == "log_range"


def test_snapshot_pages_are_immutable_after_first_page(tmp_path):
    db = make_jobs_db()
    seed_job(db, "job_a", "running")
    seed_job(db, "job_b", "running")
    remote, _store, _fake = make_remote(tmp_path, db=db)
    remote.SNAPSHOT_PAGE_SIZE = 1

    first = remote.handle_snapshot_request(request_frame(method="job.snapshot", payload={}))
    assert first["result"]["jobs"][0]["job_id"] == "job_a"
    cursor = first["result"]["cursor"]

    db.execute("UPDATE jobs SET status='failed' WHERE job_id='job_b'")
    db.execute("DELETE FROM jobs WHERE job_id='job_a'")
    seed_job(db, "job_c", "running")

    second = remote.handle_snapshot_request(
        request_frame(method="job.snapshot", payload={"cursor": cursor}, key="key-snapshot-page-2")
    )
    assert second["result"]["jobs"] == [
        {
            "job_id": "job_b", "name": "n-job_b", "command": "echo hi", "status": "running",
            "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
            "exit_code": None,
        }
    ]


# ---------------------------------------------------------------------------
# daemon route wiring
# ---------------------------------------------------------------------------


def test_daemon_helper_route_dispatches_handle_request(tmp_path, monkeypatch):
    """POST /remote/helper routes frames to RemoteJobManager.handle_request."""
    import threading

    import vanth.daemon as daemon
    from vanth.client import ensure_auth_token
    from vanth.paths import canonical_home

    home = tmp_path / "state"
    monkeypatch.setenv("VANTH_HOME", str(home))
    token = ensure_auth_token(home)
    manager = daemon.JobManager(home)
    # The real daemon opens its sqlite with check_same_thread=False because the
    # dispatcher thread reads it; mirror that here.
    db = sqlite3.connect(tmp_path / "remote.sqlite", check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    from vanth.migrations import migrate

    migrate(db, tmp_path)
    store = RemoteOperationStore(db)
    store.db.execute("UPDATE remote_state SET instance_id='test-instance' WHERE id=1")
    store.db.commit()
    fake = FakeManager(logs_dir=tmp_path, events_dir=tmp_path / "events")
    remote = RemoteJobManager(store, fake, home=tmp_path)
    remote.start()
    monkeypatch.setattr(daemon, "get_manager", lambda: manager)
    monkeypatch.setattr(daemon, "_remote_job_manager", lambda: remote)

    server = daemon.TrackingHTTPServer(("127.0.0.1", 0), daemon.Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        import http.client

        frame = request_frame()
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connection.request(
            "POST", "/remote/helper",
            body=json.dumps({"frame": frame}).encode(),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()
        assert response.status == 200
        assert payload["kind"] == "response"
        assert payload["result"]["status"] == "queued"
        assert payload["result"]["job_id"].startswith("job_")
        assert store.db.execute("SELECT COUNT(*) FROM remote_operations").fetchone()[0] == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        remote.stop()
        manager.close()
