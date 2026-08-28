"""Change-feed outbox, feed serving, and controller feed_sync tests (Phase 4)."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from vanth.remote.control import RemoteControl
from vanth.remote.feed import FeedStore
from vanth.remote.protocol import request_digest
from vanth.remote.remote import RemoteJobManager
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
    rstore.db.execute("UPDATE remote_state SET instance_id='test-instance' WHERE id=1")
    rstore.db.commit()
    cstore.set_instance_id(remote_row["remote_id"], "test-instance")
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
          trigger_json TEXT, policy_json TEXT
        );
        CREATE TABLE IF NOT EXISTS events (
          event_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, seq INTEGER NOT NULL,
          type TEXT NOT NULL, level TEXT, message TEXT, data_json TEXT,
          source TEXT, created_at TEXT NOT NULL
        );
        """
    )
    stamp = "2026-01-01T00:00:00Z"
    for index, (job_id, status) in enumerate(job_rows or []):
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

    remote = RemoteJobManager(rstore, ManagerStub(), home=tmp_path)
    transport = FakeSessionTransport(remote.handle_request)
    control = RemoteControl(cstore, transport=transport, home=tmp_path)
    return cstore, rstore, remote, control, remote_row, jobs_db


def request_frame(method="job.start", payload=None, key="key-feed-0001"):
    payload = payload if payload is not None else {"command": "echo hi"}
    return {
        "version": "1", "kind": "request", "request_id": "req_" + "0" * 32,
        "idempotency_key": key, "method": method, "payload": payload,
        "digest": request_digest(method, payload, key),
        "expected_state_epoch": 1, "expected_instance_id": "test-instance",
        "sent_at": "2026-08-20T12:00:00Z",
    }


def all_feed_rows(rstore):
    return [dict(row) for row in rstore.db.execute(
        "SELECT * FROM remote_feed ORDER BY feed_seq ASC").fetchall()]


# ---------------------------------------------------------------------------
# Outbox recording hooks
# ---------------------------------------------------------------------------


def test_mutation_and_terminal_convergence_append_outbox_rows(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(tmp_path)
    rid = row["remote_id"]
    response = remote.handle_request(request_frame())
    assert response["result"]["status"] == "queued"
    job_id = response["result"]["job_id"]

    rows = all_feed_rows(rstore)
    assert len(rows) == 1
    assert rows[0]["kind"] == "job.upsert"
    assert rows[0]["job_id"] == job_id
    assert json.loads(rows[0]["payload_json"])["status"] == "queued"

    # Converge the operation to terminal via the dispatcher sync path.
    op_id = response["result"]["op_id"]
    rstore.update_operation_status(op_id, "launched")
    rstore.update_operation_status(op_id, "running")
    jobs_db.execute("UPDATE jobs SET status='completed' WHERE job_id=?", (job_id,))
    jobs_db.commit()
    remote._sync_terminal_ops()
    status = jobs_db.execute(
        "SELECT status FROM remote_operations WHERE op_id=?", (op_id,)
    ).fetchone()[0]
    assert status == "completed"

    rows = all_feed_rows(rstore)
    assert len(rows) == 2
    assert rows[1]["kind"] == "job.upsert"
    assert json.loads(rows[1]["payload_json"])["status"] == "completed"


def test_replayed_mutation_does_not_append_outbox_row(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(tmp_path)
    remote.handle_request(request_frame(key="key-replay-fd1"))
    remote.handle_request(request_frame(key="key-replay-fd1"))
    assert len(all_feed_rows(rstore)) == 1


# ---------------------------------------------------------------------------
# Feed serving: pagination + long-poll
# ---------------------------------------------------------------------------


def test_handle_feed_request_paginates_with_strict_cursor(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(tmp_path)
    for i in range(25):
        remote.feed.append("job.upsert", job_id=f"job_{i:02d}",
                           payload={"job_id": f"job_{i:02d}", "status": "queued"})
    total = 0
    cursor = None
    pages = 0
    while True:
        payload = {"limit": 10}
        if cursor:
            payload["cursor"] = cursor
        batch = remote.handle_feed_request({"payload": payload})["result"]
        pages += 1
        total += len(batch["changes"])
        if cursor:
            assert batch["cursor"]["seq"] > cursor["seq"]
        cursor = batch["cursor"]
        if not batch["has_more"]:
            break
        assert pages < 10
    assert total == 25
    assert pages == 3
    assert cursor["seq"] == 25
    # Cursor epochs match the remote's current epochs.
    assert cursor["state_epoch"] == rstore.get_state_epoch()
    assert cursor["feed_epoch"] == rstore.get_feed_epoch()


def test_feed_wait_ms_zero_returns_immediately_empty(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(tmp_path)
    start = time.monotonic()
    batch = remote.handle_feed_request({"payload": {"wait_ms": 0}})["result"]
    elapsed = time.monotonic() - start
    assert batch["changes"] == []
    assert batch["has_more"] is False
    assert elapsed < 0.5


def test_feed_long_poll_returns_after_rows_appear(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(tmp_path)

    def append_later():
        # Own connection: sqlite objects are thread-bound.
        time.sleep(0.05)
        db = sqlite3.connect(str(tmp_path / "remote.sqlite"))
        db.execute("PRAGMA busy_timeout=30000")
        state = db.execute(
            "SELECT state_epoch, feed_epoch FROM remote_state WHERE id=1"
        ).fetchone()
        db.execute(
            "INSERT INTO remote_feed(state_epoch, feed_epoch, kind, job_id, payload_json, created_at) "
            "VALUES (?, ?, 'job.upsert', 'job_lw', '{\"job_id\": \"job_lw\", \"status\": \"queued\"}', '2026-01-01T00:00:00Z')",
            (state[0], state[1] or 1),
        )
        db.commit()
        db.close()

    thread = threading.Thread(target=append_later)
    thread.start()
    try:
        start = time.monotonic()
        batch = remote.handle_feed_request({"payload": {"wait_ms": 1500}})["result"]
        elapsed = time.monotonic() - start
    finally:
        thread.join(timeout=2)
    assert len(batch["changes"]) == 1
    assert batch["changes"][0]["kind"] == "job.upsert"
    assert elapsed >= 0.04
    assert elapsed < 2.0


def test_feed_result_carries_gap_detection_fields(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(tmp_path)
    seq = remote.feed.append("job.upsert", job_id="job_g", payload={"job_id": "job_g"})
    batch = remote.handle_feed_request({"payload": {}})["result"]
    assert batch["oldest_seq"] == seq
    assert batch["high_water_seq"] == seq
    # Empty feed reports None for both bounds.
    empty = remote.feed.read(cursor={"seq": seq}, limit=10)
    assert empty["changes"] == []
    assert empty["oldest_seq"] == seq
    assert empty["high_water_seq"] == seq
    assert remote.feed.feed_high_water() == seq


def test_job_feed_validates_via_protocol():
    from vanth.remote.protocol import VanthRemoteProtocolError, validate_request

    validate_request("job.feed", {})
    validate_request("job.feed", {"limit": 10, "wait_ms": 100})
    validate_request("job.feed", {"cursor": {"state_epoch": 1, "feed_epoch": 1, "seq": 7}})
    with pytest.raises(VanthRemoteProtocolError):
        validate_request("job.feed", {"cursor": "not-an-object"})
    with pytest.raises(VanthRemoteProtocolError):
        validate_request("job.feed", {"limit": 0})
    with pytest.raises(VanthRemoteProtocolError):
        validate_request("job.feed", {"limit": 501})
    with pytest.raises(VanthRemoteProtocolError):
        validate_request("job.feed", {"wait_ms": 10001})
    with pytest.raises(VanthRemoteProtocolError):
        validate_request("job.feed", {"bogus": 1})


# ---------------------------------------------------------------------------
# Controller-side feed_sync
# ---------------------------------------------------------------------------


def test_feed_sync_applies_upserts_and_tombstones_and_advances_cursor(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(tmp_path)
    rid = row["remote_id"]
    request = control.submit(rid, "job.start", {"command": "echo hi"}, idempotency_key="key-fs-00001",
                             expected_state_epoch=1, expected_instance_id="test-instance")
    completed = control.run_request(rid, request)
    assert completed["status"] == "completed"
    job_id = completed["response"]["job_id"]

    # A forget on the remote records a tombstone outbox row.
    remote.feed.append("job.tombstone", job_id=job_id, payload={"job_id": job_id})

    result = control.feed_sync(rid)
    assert result["mode"] == "feed"
    assert result["applied"] >= 1
    assert result["suppressed"] == 1
    cursor = cstore.get_feed_cursor(rid)
    assert cursor is not None and cursor["seq"] > 0
    # Tombstone suppressed the shadow.
    suppressed = cstore.db.execute(
        "SELECT COUNT(*) FROM remote_shadows WHERE remote_id=? AND remote_job_id=? AND suppressed_at IS NOT NULL",
        (rid, job_id),
    ).fetchone()[0]
    assert suppressed == 1

    # Second sync is a clean no-op.
    again = control.feed_sync(rid)
    assert again["mode"] == "feed"
    assert again["applied"] == 0
    assert again["suppressed"] == 0
    assert again["changes"] == 0


def test_feed_sync_cursor_advances_only_when_batch_applies(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(tmp_path)
    rid = row["remote_id"]
    remote.feed.append("job.upsert", job_id="job_tx", payload={"job_id": "job_tx"})
    before = cstore.get_feed_cursor(rid)

    original_upsert = cstore.upsert_shadow

    def exploding(**kwargs):
        raise RuntimeError("boom")

    cstore.upsert_shadow = exploding
    with pytest.raises(RuntimeError):
        control.feed_sync(rid)
    cstore.upsert_shadow = original_upsert
    # Transactional application: cursor did NOT advance on failure.
    assert cstore.get_feed_cursor(rid) == before
    # And a healthy retry applies everything.
    result = control.feed_sync(rid)
    assert result["applied"] == 1
    assert cstore.get_feed_cursor(rid)["seq"] > 0


def test_cursor_gap_recovery_falls_back_to_snapshot(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(
        tmp_path, job_rows=[("job_a", "running"), ("job_b", "running")]
    )
    rid = row["remote_id"]
    # Seed the outbox with the current jobs (as a live remote would record).
    remote.feed.append("job.upsert", job_id="job_a",
                       payload={"job_id": "job_a", "status": "running"})
    remote.feed.append("job.upsert", job_id="job_b",
                       payload={"job_id": "job_b", "status": "running"})
    first = control.feed_sync(rid)
    assert first["mode"] == "feed"
    assert first["applied"] == 2

    # Remote database restore: job_b gone, BOTH epochs bump.
    jobs_db.execute("DELETE FROM jobs WHERE job_id='job_b'")
    jobs_db.commit()
    rstore.set_state_epoch(3)

    recovered = control.feed_sync(rid)
    assert recovered["mode"] == "snapshot"
    assert recovered["state_epoch"] == 3
    live = [s["remote_job_id"] for s in control.shadows(rid)]
    assert live == ["job_a"]
    for shadow in control.shadows(rid):
        assert shadow["state_epoch"] == 3

    # The reset cursor resumes cleanly against the new timeline.
    followup = control.feed_sync(rid)
    assert followup["mode"] == "feed"
    assert followup["changes"] == 0


def test_restore_bumps_both_epochs(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(tmp_path)
    assert rstore.get_state_epoch() == 1
    assert rstore.get_feed_epoch() == 1
    rstore.set_state_epoch(4)
    assert rstore.get_state_epoch() == 4
    assert rstore.get_feed_epoch() == 2
    # Re-setting the SAME epoch is not a restore: feed epoch holds.
    rstore.set_state_epoch(4)
    assert rstore.get_feed_epoch() == 2


def test_feed_sync_after_restore_serves_only_new_epoch_rows(tmp_path):
    cstore, rstore, remote, control, row, jobs_db = make_world(tmp_path)
    rid = row["remote_id"]
    old_seq = remote.feed.append("job.upsert", job_id="job_old", payload={"job_id": "job_old"})
    control.feed_sync(rid)
    rstore.set_state_epoch(2)  # restore -> feed_epoch bumps to 2
    new_seq = remote.feed.append("job.upsert", job_id="job_new", payload={"job_id": "job_new"})
    assert new_seq > old_seq
    batch = remote.handle_feed_request({"payload": {}})["result"]
    assert [c["seq"] for c in batch["changes"]] == [new_seq]
    assert batch["state_epoch"] == 2
    assert batch["feed_epoch"] == 2
