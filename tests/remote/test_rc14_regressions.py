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


def _stop_world(tmp_path, job_rows):
    """World with ONE shared sqlite db for jobs + remote ops (as in prod)."""
    import sqlite3 as _sq

    from vanth.remote.remote import RemoteJobManager
    from vanth.remote.store import RemoteOperationStore

    db_path = tmp_path / "shared.sqlite"
    db = _sq.connect(db_path, check_same_thread=False)
    db.row_factory = _sq.Row
    db.executescript(JOBS_SCHEMA)
    stamp = "2026-01-01T00:00:00Z"
    for job_id, status in job_rows:
        db.execute(
            "INSERT INTO jobs(job_id, name, command, status, exit_code, created_at, updated_at,"
            " stdout_path, stderr_path, events_path) VALUES (?, ?, 'echo hi', ?, 0, ?, ?, 'o', 'e', 'ev')",
            (job_id, "n-" + job_id, status, stamp, stamp),
        )
    db.commit()

    rstore = RemoteOperationStore(db)
    rstore.db.execute("UPDATE remote_state SET instance_id='test-instance' WHERE id=1")
    rstore.db.commit()
    manager = FakeManager(logs_dir=tmp_path, events_dir=tmp_path / "events", db=db)
    remote = RemoteJobManager(rstore, manager, home=tmp_path)
    return rstore, remote


def test_transient_stop_failure_stays_retryable(tmp_path):
    """rc17 F1: a transient stop_sync exception leaves the op nonterminal so
    the dispatcher retries; only validation failures are terminal."""
    rstore, remote = _stop_world(tmp_path, [("job_a", "running")])

    class BoomManager:
        db_lock = threading.Lock()

        def __init__(self, db):
            self.logs = tmp_path
            self.db = db

        def stop_sync(self, job_id):
            raise RuntimeError("transient manager failure")

    remote.manager = BoomManager(rstore.db)
    resp = remote.handle_request(request_frame(method="job.stop", payload={"job_id": "job_a"}, key="key-stop-0001"))
    # rc19 N8a: a transient failure surfaces as an ERROR frame — the
    # controller's request must NOT be marked completed for a stop that is
    # still queued.
    assert resp["kind"] == "error"
    assert "temporarily failed" in resp["message"]
    op_status = rstore.db.execute(
        "SELECT status FROM remote_operations WHERE idempotency_key='key-stop-0001'"
    ).fetchone()["status"]
    assert op_status == "accepted", "transient failure must stay retryable"

    # Recovery: the same key re-drives the stop and completes it.
    class OkManager(BoomManager):
        def stop_sync(self, job_id):
            self.db.execute("UPDATE jobs SET status='completed' WHERE job_id=?", (job_id,))
            self.db.commit()
            return {"status": "completed"}

    remote.manager = OkManager(rstore.db)
    resp2 = remote.handle_request(request_frame(method="job.stop", payload={"job_id": "job_a"}, key="key-stop-0001"))
    assert resp2["kind"] == "response", resp2
    assert resp2["result"]["status"] == "completed"
    op_status2 = rstore.db.execute(
        "SELECT status FROM remote_operations WHERE idempotency_key='key-stop-0001'"
    ).fetchone()["status"]
    assert op_status2 == "completed"


def test_unknown_stop_target_is_terminal(tmp_path):
    rstore, remote = _stop_world(tmp_path, [])
    resp = remote.handle_request(request_frame(method="job.stop", payload={"job_id": "ghost"}, key="key-stop-0002"))
    assert resp["kind"] == "response", resp
    assert resp["result"]["status"] == "error"
    status = rstore.db.execute(
        "SELECT status FROM remote_operations WHERE idempotency_key='key-stop-0002'"
    ).fetchone()["status"]
    assert status == "failed"


def test_feed_cursor_never_moves_backward(tmp_path):
    """rc17 F3: on one timeline a cursor update with an OLDER seq is a no-op."""
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(tmp_path)
    rid = row["remote_id"]
    cstore.set_feed_cursor(rid, {"state_epoch": 1, "feed_epoch": 1, "seq": 9})
    out = control._advance_feed_cursor(rid, {"state_epoch": 1, "feed_epoch": 1, "seq": 5})
    assert int(out["seq"]) == 9
    stored = cstore.get_feed_cursor(rid)
    assert int(stored["seq"]) == 9
    # A NEW timeline may reset freely.
    out2 = control._advance_feed_cursor(rid, {"state_epoch": 2, "feed_epoch": 2, "seq": 0})
    assert int(out2["seq"]) == 0


def test_stale_feed_batch_cannot_regress_shadows(tmp_path):
    """rc18 R2: a batch superseded by durable progress is skipped BEFORE any
    shadow write — a stale 'running' can no longer overwrite fresher
    'completed' state."""
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[("job_a", "completed")]
    )
    rid = row["remote_id"]
    # Durable progress already sits at seq 9.
    cstore.set_feed_cursor(rid, {"state_epoch": 1, "feed_epoch": 1, "seq": 9})
    before = {s["remote_job_id"]: s["status"] for s in control.shadows(rid)}
    assert not before or before.get("job_a") == "completed"

    original_handler = control.transport.handler

    def stale_batch(frame):
        resp = original_handler(frame)
        if frame.get("method") == "job.feed" and resp.get("kind") == "response":
            resp["result"]["changes"] = [{
                "kind": "job.upsert", "job_id": "job_a",
                "payload": {"job_id": "job_a", "status": "running"},
            }]
            resp["result"]["cursor"] = {"seq": 5}
            resp["result"]["state_epoch"] = 1
            resp["result"]["feed_epoch"] = 1
        return resp

    control.transport.handler = stale_batch
    out = control.feed_sync(rid)
    assert out.get("stale_batch_skipped") is True
    after = {s["remote_job_id"]: s["status"] for s in control.shadows(rid)}
    assert after == before, "stale batch must not touch shadows"
    assert int(cstore.get_feed_cursor(rid)["seq"]) == 9


def test_compensate_failure_preserves_local_credentials(tmp_path):
    """rc18 R5: when remote cleanup fails, compensation keeps remote_dir so
    remove_remote can retry revocation later."""
    import sqlite3 as _sq

    from vanth.remote import pairing as pairing_mod

    home = tmp_path / "home"
    home.mkdir(parents=True)
    conn = _sq.connect(home / "remote.sqlite")
    conn.row_factory = _sq.Row
    store = pairing_mod.RemoteStore(conn)
    remote_id = "rmt_" + "c" * 32
    store.create_remote(target="u@h", state="error", name="x")
    # create_remote generates its own id; fetch it.
    remote_id = store.list_remotes()[0]["remote_id"]
    remote_dir = home / "remote" / remote_id
    remote_dir.mkdir(parents=True)
    (remote_dir / "id_ed25519").write_text("key")

    class FailingTransport:
        def remove_authorized_key(self, **kw):
            raise ConnectionError("host unreachable")

    pairing_mod._compensate(store, FailingTransport(), remote_id, remote_dir,
                            "vanth-remote:" + remote_id, "ssh-ed25519 AAA line",
                            "config", ["ssh"], remote_home=None)
    assert remote_dir.exists(), "failed cleanup must preserve credentials"


def test_remove_remote_without_material_requires_force(tmp_path):
    """rc18 R5: no local dir + installed authorization -> refuse to delete
    the record unless force=True."""
    import sqlite3 as _sq

    from vanth.remote import pairing as pairing_mod

    home = tmp_path / "home"
    home.mkdir(parents=True)
    conn = _sq.connect(home / "remote.sqlite")
    conn.row_factory = _sq.Row
    store = pairing_mod.RemoteStore(conn)
    store.create_remote(target="u@h", state="paired", name="x")
    rid = store.list_remotes()[0]["remote_id"]
    store.db.execute(
        "UPDATE remotes SET installed_authorization='command=\"x\" ssh-ed25519 AAA vanth-remote:' || ? "
        "WHERE remote_id=?", (rid, rid))
    store.db.commit()
    out = pairing_mod.remove_remote(home=home, remote_id=rid, store=store,
                                    transport=pairing_mod.DefaultTransport())
    assert out["result"] == "error"
    assert store.get_remote(rid) is not None
    out2 = pairing_mod.remove_remote(home=home, remote_id=rid, store=store, force=True)
    assert out2["result"] == "ok"


def test_cross_timeline_feed_batch_rejected(tmp_path):
    """rc19 N1: a batch whose timeline diverged from durable progress
    mid-flight is skipped before any shadow write. (Fully foreign timelines
    are intercepted earlier by gap-recovery -> snapshot resync.)"""
    cstore, rstore, remote, control, row, jobs_db, logs = make_world(
        tmp_path, job_rows=[("job_a", "running")]
    )
    rid = row["remote_id"]
    cstore.db.execute("UPDATE remotes SET state_epoch=2 WHERE remote_id=?", (rid,))
    cstore.db.commit()
    cstore.set_feed_cursor(rid, {"state_epoch": 2, "feed_epoch": 2, "seq": 9})
    cstore.upsert_shadow(remote_id=rid, remote_job_id="job_a", status="completed",
                         payload={"status": "completed"}, state_epoch=2)
    original_handler = control.transport.handler

    def diverging_batch(frame):
        resp = original_handler(frame)
        if frame.get("method") == "job.feed" and resp.get("kind") == "response":
            # A snapshot sync lands BEFORE the batch is applied: durable
            # progress moves under our feet, and the (simulated) response
            # still speaks the same timeline.
            cstore.upsert_shadow(remote_id=rid, remote_job_id="job_a", status="completed",
                                 payload={"status": "completed"}, state_epoch=2)
            cstore.set_feed_cursor(rid, {"state_epoch": 2, "feed_epoch": 2, "seq": 12})
            resp["result"]["state_epoch"] = 2
            resp["result"]["feed_epoch"] = 2
            resp["result"]["changes"] = [{
                "kind": "job.upsert", "job_id": "job_a",
                "payload": {"job_id": "job_a", "status": "running"},
            }]
            resp["result"]["cursor"] = {"seq": 11}
        return resp

    control.transport.handler = diverging_batch
    out = control.feed_sync(rid)
    assert out.get("stale_batch_skipped") is True
    shadow = next(s for s in control.shadows(rid) if s["remote_job_id"] == "job_a")
    assert shadow["status"] == "completed"
    cursor_after = cstore.get_feed_cursor(rid)
    assert int(cursor_after["seq"]) == 12


def test_shadow_upsert_never_regresses_epoch(tmp_path):
    from vanth.remote.store import RemoteStore as _RS
    db_path = tmp_path / "shadows.sqlite"
    import sqlite3 as _sq

    conn = _sq.connect(db_path)
    conn.row_factory = _sq.Row
    store = _RS(conn)
    rid = store.create_remote(target="u@h", state="paired")["remote_id"]
    first = store.upsert_shadow(remote_id=rid, remote_job_id="j1", status="completed",
                                payload={"status": "completed"}, state_epoch=3)
    stale = store.upsert_shadow(remote_id=rid, remote_job_id="j1", status="running",
                                payload={"status": "running"}, state_epoch=1)
    row = store.db.execute(
        "SELECT status FROM remote_shadows WHERE remote_id=? AND remote_job_id='j1'", (rid,)
    ).fetchone()
    assert row["status"] == "completed"
