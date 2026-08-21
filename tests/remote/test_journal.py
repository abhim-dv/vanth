"""Client request journal, CLI retry seam, and rotation stub tests (Phase 4)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from vanth.remote.control import RemoteControl
from vanth.remote.journal import RequestJournal
from vanth.remote.protocol import VanthRemoteProtocolError
from vanth.remote.store import RemoteOperationStore, RemoteStore


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
                frame = json.loads(frame_bytes.decode("utf-8").rstrip("\n"))
                response = self.handler(frame)
                return (json.dumps(response, separators=(",", ":")) + "\n")

        return S(self.handler)


def make_world(tmp_path, job_rows=None):
    """Controller store + control + remote manager wired in-process."""
    controller_db = connect(tmp_path / "controller.sqlite")
    cstore = RemoteStore(controller_db)
    remote_row = cstore.create_remote(target="user@host", state="paired")

    rstore = RemoteOperationStore(connect(tmp_path / "remote.sqlite"))
    # The remote-side jobs/events tables live in the SAME database as the
    # operation store (as on a real remote daemon).
    rstore.db.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
          job_id TEXT PRIMARY KEY, name TEXT, command TEXT NOT NULL, cwd TEXT,
          status TEXT NOT NULL, pid INTEGER, worker_pid INTEGER,
          runner_heartbeat_at TEXT, stop_requested_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          started_at TEXT, ended_at TEXT, exit_code INTEGER, timeout_seconds INTEGER,
          notify_on TEXT, origin_thread_id TEXT, wake_thread_id TEXT, tags_json TEXT,
          env_json TEXT, notes TEXT, run_json TEXT, stdout_path TEXT NOT NULL, stderr_path TEXT NOT NULL, events_path TEXT NOT NULL,
          trigger_json TEXT
        );
        CREATE TABLE IF NOT EXISTS events (
          event_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, seq INTEGER NOT NULL,
          type TEXT NOT NULL, level TEXT, message TEXT, data_json TEXT,
          source TEXT, created_at TEXT NOT NULL
        );
        """
    )
    stamp = "2026-01-01T00:00:00Z"
    for job_id, status in job_rows or []:
        rstore.db.execute(
            "INSERT INTO jobs(job_id, name, command, status, exit_code, created_at, updated_at, stdout_path, stderr_path, events_path) "
            "VALUES (?, ?, 'echo hi', ?, 0, ?, ?, 'o', 'e', 'ev')",
            (job_id, "n-" + job_id, status, stamp, stamp),
        )
    rstore.db.commit()
    jobs_db = rstore.db

    class ManagerStub:
        db = jobs_db
        logs = tmp_path / "logs"
        events_dir = tmp_path / "events"

    from vanth.remote.remote import RemoteJobManager

    remote = RemoteJobManager(rstore, ManagerStub(), home=tmp_path)
    transport = FakeSessionTransport(remote.handle_request)
    control = RemoteControl(cstore, transport=transport, home=tmp_path)
    return cstore, rstore, remote, control, remote_row, transport


def make_journal(tmp_path) -> RequestJournal:
    return RequestJournal(tmp_path / "client-requests.sqlite")


# ---------------------------------------------------------------------------
# Journal lifecycle
# ---------------------------------------------------------------------------


def test_submit_journals_pending_and_completed_run_resolves(tmp_path):
    cstore, rstore, remote, control, row, transport = make_world(tmp_path)
    rid = row["remote_id"]
    journal = make_journal(tmp_path)
    control = RemoteControl(cstore, transport=transport, home=tmp_path, journal=journal)

    request = control.submit(rid, "job.start", {"command": "echo hi"}, idempotency_key="key-jr-0001")
    pending = journal.pending()
    assert len(pending) == 1
    assert pending[0]["request_id"] == request["request_id"]
    assert pending[0]["status"] == "pending"
    assert pending[0]["idempotency_key"] == "key-jr-0001"
    assert pending[0]["method"] == "job.start"

    result = control.run_request(rid, request)
    assert result["status"] == "completed"
    assert journal.pending() == []
    entry = journal.get(request["request_id"])
    assert entry["status"] == "resolved"


def test_transport_failure_leaves_pending(tmp_path):
    cstore, rstore, remote, control, row, transport = make_world(tmp_path)
    rid = row["remote_id"]
    journal = make_journal(tmp_path)
    control = RemoteControl(cstore, transport=transport, home=tmp_path, journal=journal)

    transport.fail = True
    request = control.submit(rid, "job.start", {"command": "echo hi"}, idempotency_key="key-lost-jr1")
    result = control.run_request(rid, request)
    assert result["status"] == "submitting"
    pending = journal.pending()
    assert len(pending) == 1
    assert pending[0]["status"] == "pending"


def test_failed_error_frame_resolves_entry(tmp_path):
    cstore, rstore, remote, control, row, transport = make_world(tmp_path)
    rid = row["remote_id"]
    journal = make_journal(tmp_path)
    control = RemoteControl(cstore, transport=transport, home=tmp_path, journal=journal)

    # Make the remote reject the mutation: unknown method payload mismatch.
    def bad_handler(frame):
        from vanth.remote.protocol import encode_frame

        return json.loads(encode_frame({
            "version": "1", "kind": "error", "request_id": frame.get("request_id"),
            "method": frame.get("method"), "code": "INVALID_REQUEST",
            "message": "nope", "sent_at": "2026-08-20T12:00:00Z",
        }).decode("utf-8").rstrip("\n"))

    transport.handler = bad_handler
    request = control.submit(rid, "job.start", {"command": "echo hi"}, idempotency_key="key-err-jr1")
    result = control.run_request(rid, request)
    assert result["status"] == "failed"
    assert journal.pending() == []


def test_unique_remote_idempotency_key_enforced(tmp_path):
    journal = make_journal(tmp_path)
    entry = {
        "request_id": "req_" + "a" * 32, "remote_id": "rmt_" + "b" * 32,
        "idempotency_key": "key-dup-0001", "method": "job.start",
        "payload": {"command": "echo hi"}, "digest": "d" * 64,
    }
    journal.record(entry)
    # Re-recording a replayed request is a no-op.
    journal.record(entry)
    rows = journal.db.execute("SELECT COUNT(*) FROM client_requests").fetchone()[0]
    assert rows == 1
    # A different request with the same (remote_id, key) violates the UNIQUE.
    with pytest.raises(sqlite3.IntegrityError):
        journal.db.execute(
            "INSERT INTO client_requests(request_id, remote_id, idempotency_key, method,"
            " payload_json, digest, status, created_at, updated_at)"
            " VALUES ('req_other', ?, ?, ?, '{}', ?, 'pending', 't', 't')",
            (entry["remote_id"], entry["idempotency_key"], entry["method"], entry["digest"]),
        )
        journal.db.commit()


def test_no_journal_keeps_existing_call_sites_working(tmp_path):
    cstore, rstore, remote, control, row, transport = make_world(tmp_path)
    rid = row["remote_id"]
    request = control.submit(rid, "job.start", {"command": "echo hi"}, idempotency_key="key-nojr-001")
    result = control.run_request(rid, request)
    assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# Retry with the original key
# ---------------------------------------------------------------------------


def test_retry_reruns_original_request_with_original_key(tmp_path):
    cstore, rstore, remote, control, row, transport = make_world(tmp_path)
    rid = row["remote_id"]
    journal = make_journal(tmp_path)
    journalled_control = RemoteControl(cstore, transport=transport, home=tmp_path, journal=journal)

    payload = {"command": "echo hi"}
    first = journalled_control.submit(rid, "job.start", payload, idempotency_key="key-retry-001")
    transport.fail = True
    result = journalled_control.run_request(rid, first)
    assert result["status"] == "submitting"
    transport.fail = False

    # Retry path (as `vanth remote retry` does): re-submit with the ORIGINAL
    # key/payload — replay returns the SAME durable request — then run it.
    retried = control.submit(rid, first["method"], first["payload"],
                             idempotency_key=first["idempotency_key"])
    assert retried["request_id"] == first["request_id"]
    final = journalled_control.run_request(rid, retried)
    assert final["status"] == "completed"

    # Exactly one controller request and exactly one remote operation.
    assert cstore.db.execute(
        "SELECT COUNT(*) FROM remote_requests WHERE remote_id=?", (rid,)
    ).fetchone()[0] == 1
    assert rstore.db.execute("SELECT COUNT(*) FROM remote_operations").fetchone()[0] == 1
    # The journal entry resolves once the retry completes.
    resolved = journal.get(first["request_id"])
    assert resolved is not None and resolved["status"] == "resolved"


# ---------------------------------------------------------------------------
# Credential rotation stub
# ---------------------------------------------------------------------------


def test_rotate_credentials_raises_unsupported_feature(tmp_path):
    cstore, rstore, remote, control, row, transport = make_world(tmp_path)
    with pytest.raises(VanthRemoteProtocolError) as exc:
        control.rotate_credentials(row["remote_id"])
    assert exc.value.code == "UNSUPPORTED_FEATURE"
    assert "pairing" in str(exc.value)
