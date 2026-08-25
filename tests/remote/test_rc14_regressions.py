"""rc14 re-review regressions: concurrency, snapshot fail-fast, stop recovery."""

import json
import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

sys.path.insert(0, "F:/git/vanth/tests/remote")
from test_remote_ops import FakeManager, make_jobs_db, request_frame  # noqa: E402
from test_snapshot import FakeSessionTransport, connect, make_world  # noqa: E402


JOBS_SCHEMA = """
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


def test_concurrent_starts_never_corrupt_transactions(tmp_path):
    """Review rc14 P1-1: 30 concurrent handle_request calls must all succeed —
    interleaved BEGIN/commit sequences used to produce nested-transaction
    errors on 28 of 30. Production shares ONE cross-thread connection between
    the HTTP handlers and the dispatcher, serialized by store.db_lock."""
    import sqlite3 as _sq

    from vanth.remote.remote import RemoteJobManager
    from vanth.remote.store import RemoteOperationStore

    db_path = tmp_path / "shared.sqlite"
    db = _sq.connect(db_path, check_same_thread=False)
    db.row_factory = _sq.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(JOBS_SCHEMA)
    db.commit()

    # RemoteOperationStore binds the SHARED connection (as the daemon does).
    rstore = RemoteOperationStore(db)
    rstore.db.execute("UPDATE remote_state SET instance_id='test-instance' WHERE id=1")
    rstore.db.commit()
    manager = FakeManager(logs_dir=tmp_path, events_dir=tmp_path / "events", db=db)
    remote = RemoteJobManager(rstore, manager, home=tmp_path)

    errors = []
    results = []
    gate = threading.Barrier(30)

    def call(i):
        try:
            gate.wait(timeout=10)
            frame = request_frame(key=f"key-stress-{i:04d}")
            resp = remote.handle_request(frame)
            assert resp["kind"] == "response", resp
            results.append(resp["result"]["job_id"])
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(f"{type(exc).__name__}: {exc}")

    with ThreadPoolExecutor(max_workers=30) as pool:
        list(pool.map(call, range(30)))

    assert not errors, f"{len(errors)} concurrent failures: {errors[:3]}"
    assert len(results) == 30
    rows = rstore.db.execute("SELECT COUNT(*) FROM remote_operations").fetchone()[0]
    assert rows == 30


def test_failed_snapshot_page_aborts_sync_without_suppression(tmp_path):
    """Review rc14 P0-2: a lost/failed page must abort the sync — never
    finalize an empty authoritative set."""
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path,
        job_rows=[("job_a", "running"), ("job_b", "running")],
    )
    rid = row["remote_id"]
    control.sync_snapshot(rid)
    assert {s["remote_job_id"] for s in control.shadows(rid)} == {"job_a", "job_b"}

    control.transport.fail = True
    with pytest.raises(Exception):
        control.sync_snapshot(rid)
    control.transport.fail = False

    live = {s["remote_job_id"] for s in control.shadows(rid)}
    assert live == {"job_a", "job_b"}


def test_replay_keeps_original_durable_epoch_binding(tmp_path):
    """Sol review P0: a replayed submit must carry the epoch SQLite stored —
    never rebind in memory while the durable row keeps the original."""
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(tmp_path)
    rid = row["remote_id"]
    payload = {"name": "n", "command": "echo hi"}
    first = control.submit(rid, "job.start", payload, idempotency_key="key-epoch-0001",
                           expected_state_epoch=1, expected_instance_id="inst-test")
    assert int(first["expected_state_epoch"]) == 1
    # Timeline moves on; a retry sends the NEW expectation with the SAME key.
    cstore.db.execute("UPDATE remotes SET state_epoch=2 WHERE remote_id=?", (rid,))
    cstore.db.commit()
    replay = control.submit(rid, "job.start", payload, idempotency_key="key-epoch-0001",
                            expected_state_epoch=2, expected_instance_id="inst-test")
    assert replay["request_id"] == first["request_id"]
    assert int(replay["expected_state_epoch"]) == 1, (
        "replay must keep the durably stored epoch binding"
    )
    if control.journal is not None:
        pending = [e for e in control.journal.pending() if e.get("request_id") == first["request_id"]]
        assert pending and int(pending[0].get("expected_state_epoch") or 0) == 1


def test_snapshot_sync_advances_feed_cursor_to_boundary(tmp_path):
    """Sol review P1: after a snapshot sync the feed cursor must sit at the
    captured feed boundary so stale events cannot regress shadows."""
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[("job_a", "running"), ("job_b", "completed")]
    )
    rid = row["remote_id"]
    totals = control.sync_snapshot(rid)
    assert {s["remote_job_id"] for s in control.shadows(rid)} == {"job_a", "job_b"}
    boundary = rstore.db.execute(
        "SELECT COALESCE(MAX(feed_seq), 0) AS s FROM remote_feed"
    ).fetchone()["s"]
    cursor = cstore.get_feed_cursor(rid)
    assert cursor is not None
    assert int(cursor["seq"]) == int(boundary)


def test_snapshot_pages_carry_consistent_feed_boundary(tmp_path):
    """Every snapshot page echoes the same feed boundary + epoch (Sol review)."""
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[(f"job_{i:03d}", "running") for i in range(60)]
    )
    page = remote.handle_snapshot_request({"payload": {}})["result"]
    assert "feed_boundary_seq" in page and "feed_epoch" in page
    assert page["has_more"] is True
    page2 = remote.handle_snapshot_request({"payload": {"cursor": page["cursor"]}})["result"]
    assert page2["feed_boundary_seq"] == page["feed_boundary_seq"]
    assert page2["feed_epoch"] == page["feed_epoch"]
