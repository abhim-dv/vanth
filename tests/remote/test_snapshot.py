"""Snapshot recovery, log-range reads, and suppression tests (Phase 3)."""

import base64
import json
import sqlite3
from pathlib import Path

import pytest

from vanth.remote.control import RemoteControl
from vanth.remote.protocol import VanthRemoteProtocolError, decode_frame, request_digest, validate_request
from vanth.remote.remote import RemoteJobManager
from vanth.remote.store import RemoteOperationStore, RemoteStore

sys_path_ready = True


def connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


class FakeSessionTransport:
    """In-process transport: controller -> remote handler without SSH."""

    def __init__(self, handler):
        self.handler = handler
        self.fail = False

    def open_session(self, remote_row, *, home=None):
        if self.fail:
            return None

        class S:
            def __init__(self, handler):
                self.handler = handler

            def exchange(self, frame_bytes):
                frame = decode_frame(frame_bytes.decode("utf-8").rstrip("\n"))
                response = self.handler(frame)
                return (json.dumps(response, separators=(",", ":")) + "\n")

        return S(self.handler)


def make_world(tmp_path, job_rows=None, log_bytes=b""):
    """Controller store + control + remote manager wired in-process."""
    controller_db = connect(tmp_path / "controller.sqlite")
    cstore = RemoteStore(controller_db)
    remote_row = cstore.create_remote(target="user@host", state="paired")

    rstore = RemoteOperationStore(connect(tmp_path / "remote.sqlite"))
    import sqlite3 as _s

    jobs_db = _s.connect(":memory:")
    jobs_db.row_factory = _s.Row
    jobs_db.executescript(
        """
        CREATE TABLE jobs (
          job_id TEXT PRIMARY KEY, name TEXT, command TEXT NOT NULL, cwd TEXT,
          status TEXT NOT NULL, pid INTEGER, worker_pid INTEGER,
          runner_heartbeat_at TEXT, stop_requested_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          started_at TEXT, ended_at TEXT, exit_code INTEGER, timeout_seconds INTEGER,
          notify_on TEXT, origin_thread_id TEXT, wake_thread_id TEXT, tags_json TEXT,
          env_json TEXT, notes TEXT, run_json TEXT, stdout_path TEXT NOT NULL, stderr_path TEXT NOT NULL, events_path TEXT NOT NULL,
          trigger_json TEXT
        );
        CREATE TABLE events (
          event_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, seq INTEGER NOT NULL,
          type TEXT NOT NULL, level TEXT, message TEXT, data_json TEXT,
          source TEXT, created_at TEXT NOT NULL
        );
        CREATE TABLE deliveries (
          delivery_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, target_id TEXT NOT NULL,
          job_id TEXT NOT NULL, target_type TEXT NOT NULL, status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, payload_json TEXT NOT NULL, created_at TEXT NOT NULL
        );
        """
    )
    stamp = "2026-01-01T00:00:00Z"
    for index, (job_id, status) in enumerate(job_rows or []):
        jobs_db.execute(
            "INSERT INTO jobs(job_id, name, command, status, exit_code, created_at, updated_at, stdout_path, stderr_path, events_path) "
            "VALUES (?, ?, 'echo hi', ?, 0, ?, ?, 'o', 'e', 'ev')",
            (job_id, "n-" + job_id, status, stamp, stamp),
        )
        jobs_db.execute(
            "INSERT INTO events(event_id, job_id, seq, type, source, created_at) VALUES (?, ?, ?, 'progress', 'stdout', ?)",
            ("evt_" + job_id, job_id, index + 1, stamp),
        )
    jobs_db.commit()

    logs_dir = tmp_path / "logs"
    logs_dir.mkdir(exist_ok=True)
    (logs_dir / "job_a.stdout.log").write_bytes(log_bytes)

    class ManagerStub:
        db = jobs_db
        logs = logs_dir
        events_dir = tmp_path / "events"

    remote = RemoteJobManager(rstore, ManagerStub(), home=tmp_path)
    transport = FakeSessionTransport(remote.handle_request)
    control = RemoteControl(cstore, transport=transport, home=tmp_path)
    return cstore, rstore, remote, control, remote_row, jobs_db, logs_dir


def snap_of(remote, payload=None):
    """Direct handler call, unwrapped to the snapshot result."""
    return remote.handle_snapshot_request({"payload": payload or {}})["result"]


def logr_of(remote, payload):
    return remote.handle_log_range_request({"payload": payload})["result"]


def test_snapshot_handler_paginates(tmp_path):
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[(f"job_{i:03d}", "completed") for i in range(120)]
    )
    page1 = snap_of(remote)
    assert page1["kind"] == "snapshot"
    assert len(page1["jobs"]) == remote.SNAPSHOT_PAGE_SIZE
    assert page1["has_more"] is True
    total = len(page1["jobs"])
    cursor = page1["cursor"]
    pages = 1
    while cursor and pages < 10:
        page = snap_of(remote, {"cursor": cursor})
        total += len(page["jobs"])
        pages += 1
        if not page["has_more"]:
            break
        cursor = page["cursor"]
    assert total == 120
    assert pages == 3


def test_apply_snapshot_upserts_and_advances_cursor_one_tx(tmp_path):
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[("job_a", "running")]
    )
    snapshot = snap_of(remote)
    result = control.apply_snapshot(row["remote_id"], snapshot)
    assert result["applied"] == 1
    shadows = control.shadows(row["remote_id"])
    assert len(shadows) == 1
    assert shadows[0]["remote_job_id"] == "job_a"
    assert shadows[0]["status"] == "running"
    cursor = cstore.get_snapshot_cursor(row["remote_id"])
    assert cursor is not None and "high_water" in cursor


def test_full_snapshot_repairs_deletions_without_merging_epochs(tmp_path):
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[("job_a", "running"), ("job_b", "running")]
    )
    snap1 = snap_of(remote)
    control.apply_snapshot(row["remote_id"], snap1)
    assert len(control.shadows(row["remote_id"])) == 2

    # job_b is deleted on the remote; a new epoch begins (remote db restore).
    jobs_db.execute("DELETE FROM jobs WHERE job_id='job_b'")
    jobs_db.commit()
    rstore.set_state_epoch(2)
    snap2 = snap_of(remote)
    result = control.apply_snapshot(row["remote_id"], snap2)

    # Deletion repaired: only job_a remains live.
    live = [s["remote_job_id"] for s in control.shadows(row["remote_id"])]
    assert live == ["job_a"]
    assert result["deleted"] >= 1
    # Old-epoch rows retained for audit but excluded from the read path.
    audit = cstore.db.execute(
        "SELECT COUNT(*) FROM remote_shadows WHERE remote_id=? AND superseded_at IS NOT NULL",
        (row["remote_id"],),
    ).fetchone()[0]
    assert audit >= 1
    # Epochs never merged: every live shadow is at the current epoch.
    for shadow in control.shadows(row["remote_id"]):
        assert shadow["state_epoch"] == 2


def test_forget_shadow_prevents_resurrection(tmp_path):
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[("job_a", "running")]
    )
    snap = snap_of(remote)
    control.apply_snapshot(row["remote_id"], snap)
    control.forget_shadow(row["remote_id"], "job_a")
    # A later snapshot containing the same job must not resurrect it.
    control.apply_snapshot(row["remote_id"], snap_of(remote))
    assert control.shadows(row["remote_id"]) == []


def test_snapshot_application_never_creates_deliveries(tmp_path):
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[("job_a", "completed")]
    )
    control.apply_snapshot(row["remote_id"], snap_of(remote))
    count = jobs_db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
    assert count == 0


def test_log_range_roundtrips_arbitrary_bytes(tmp_path):
    payload = bytes(range(256)) * 4 + b"\x00\r\n tail"
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[("job_a", "running")], log_bytes=payload
    )
    response = control.log_range(row["remote_id"], "job_a", stream="stdout", offset=0, size=len(payload))
    assert base64.b64decode(response["content"]) == payload
    assert response["truncated"] is False
    # Offset read: exact window.
    mid = control.log_range(row["remote_id"], "job_a", stream="stdout", offset=10, size=5)
    assert base64.b64decode(mid["content"]) == payload[10:15]
    assert mid["truncated"] is True


def test_new_methods_validate_via_protocol():
    validate_request("job.snapshot", {})
    validate_request("job.snapshot", {"cursor": {"offset": 50}})
    with pytest.raises(VanthRemoteProtocolError):
        validate_request("job.snapshot", {"cursor": "not-an-object"})
    validate_request("job.log_range", {"remote_job_id": "job_x"})
    validate_request("job.log_range", {"remote_job_id": "job_x", "stream": "stderr", "offset": 0, "size": 10})
    with pytest.raises(VanthRemoteProtocolError):
        validate_request("job.log_range", {"stream": "stdout"})
    with pytest.raises(VanthRemoteProtocolError):
        validate_request("job.log_range", {"remote_job_id": "j", "stream": "stdin"})
    with pytest.raises(VanthRemoteProtocolError):
        validate_request("job.log_range", {"remote_job_id": "j", "offset": -1})


def test_sync_snapshot_follows_pagination(tmp_path):
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[(f"job_{i:03d}", "completed") for i in range(60)]
    )
    totals = control.sync_snapshot(row["remote_id"])
    assert totals["pages"] >= 2
    assert totals["applied"] == 60

