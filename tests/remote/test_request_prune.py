"""Retention for controller request + journal rows.

The remote-wake poll loop submits a fresh request per tick, so these tables grow
without bound while a binding is live. Only SETTLED rows may be pruned; an
in-flight request is the durable replay/retry handle.
"""

from __future__ import annotations

import sqlite3

from vanth.remote.journal import RequestJournal
from vanth.remote.store import RemoteStore

OLD = "2000-01-01T00:00:00Z"
NEW = "2999-01-01T00:00:00Z"


def _store() -> RemoteStore:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return RemoteStore(db)


def _request(store, request_id, status, updated_at):
    store.db.execute(
        "INSERT INTO remote_requests(request_id, remote_id, idempotency_key, method, payload_json, "
        "digest, status, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (request_id, "host", f"key-{request_id}", "job.feed", "{}", "d" * 64, status, updated_at, updated_at),
    )


def test_prune_requests_keeps_inflight_and_recent():
    store = _store()
    _request(store, "r_old", "completed", OLD)
    _request(store, "r_recent", "completed", NEW)
    _request(store, "r_pending", "submitting", OLD)  # in-flight, must survive
    # `accepted` can be re-driven, so it is in-flight too and must survive.
    _request(store, "r_accepted", "accepted", OLD)
    store.db.execute(
        "INSERT INTO remote_replay_tombstones(tombstone_id, remote_id, idempotency_key, digest, created_at) "
        "VALUES ('t_old','host','k','d', ?)",
        (OLD,),
    )
    store.db.commit()

    counts = store.prune_requests(3600)

    assert counts == {"requests": 1, "tombstones": 1}
    remaining = {row["request_id"] for row in store.db.execute("SELECT request_id FROM remote_requests")}
    assert remaining == {"r_recent", "r_pending", "r_accepted"}


def test_prune_requests_disabled_by_zero_ttl():
    store = _store()
    _request(store, "r_old", "completed", OLD)
    store.db.commit()
    assert store.prune_requests(0) == {"requests": 0, "tombstones": 0}
    assert store.db.execute("SELECT COUNT(*) FROM remote_requests").fetchone()[0] == 1


def test_prune_resolved_keeps_pending(tmp_path):
    journal = RequestJournal(tmp_path / "client.sqlite")
    try:
        journal.db.execute(
            "INSERT INTO client_requests(request_id, remote_id, idempotency_key, method, payload_json, "
            "digest, status, created_at, updated_at) VALUES ('c_old','host','k1','job.feed','{}','d','resolved',?,?)",
            (OLD, OLD),
        )
        journal.db.execute(
            "INSERT INTO client_requests(request_id, remote_id, idempotency_key, method, payload_json, "
            "digest, status, created_at, updated_at) VALUES ('c_pending','host','k2','job.feed','{}','d','pending',?,?)",
            (OLD, OLD),
        )
        journal.db.commit()

        assert journal.prune_resolved(3600) == 1
        remaining = {row["request_id"] for row in journal.db.execute("SELECT request_id FROM client_requests")}
        assert remaining == {"c_pending"}
    finally:
        journal.close()
