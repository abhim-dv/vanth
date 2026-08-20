"""Durable remote stores, state transitions, and five crash cases (Phase 0)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from vanth.remote.protocol import VanthRemoteProtocolError, request_digest
from vanth.remote.store import (
    RemoteOperationStore,
    RemoteStore,
    transition,
)

KEY_1 = "key-crash-0001"
KEY_2 = "key-crash-0002"
KEY_3 = "key-crash-0003"
KEY_4 = "key-crash-0004"
KEY_5 = "key-crash-0005"


def connect(path: Path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def make_start_payload(key: str) -> dict:
    return {"command": "echo hi"}


def digest_of(key: str, method: str = "job.start", payload: dict | None = None) -> str:
    payload = payload if payload is not None else make_start_payload(key)
    return request_digest(method, payload, key)


# ---------------------------------------------------------------------------
# RemoteStore basics
# ---------------------------------------------------------------------------


def test_create_get_list_update_remote(tmp_path):
    store = RemoteStore(connect(tmp_path / "ctrl.sqlite"))
    remote = store.create_remote(name="demo", target="user@host")
    assert remote["remote_id"].startswith("rmt_")
    assert remote["state"] == "unpaired"
    assert store.get_remote(remote["remote_id"])["target"] == "user@host"
    assert [r["remote_id"] for r in store.list_remotes()] == [remote["remote_id"]]
    updated = store.update_remote_state(remote["remote_id"], "pairing")
    assert updated["state"] == "pairing"
    with pytest.raises(ValueError):
        store.get_remote("rmt_" + "0" * 32)


def test_record_request_same_key_same_request(tmp_path):
    store = RemoteStore(connect(tmp_path / "ctrl.sqlite"))
    remote = store.create_remote(target="user@host")
    digest = digest_of(KEY_1)
    first = store.record_request(
        remote_id=remote["remote_id"], idempotency_key=KEY_1,
        method="job.start", payload=make_start_payload(KEY_1), digest=digest,
    )
    second = store.record_request(
        remote_id=remote["remote_id"], idempotency_key=KEY_1,
        method="job.start", payload=make_start_payload(KEY_1), digest=digest,
    )
    assert first["request_id"] == second["request_id"]
    assert first["status"] == "creating"
    assert second["payload"] == make_start_payload(KEY_1)


def test_record_request_replay_mismatch(tmp_path):
    store = RemoteStore(connect(tmp_path / "ctrl.sqlite"))
    remote = store.create_remote(target="user@host")
    store.record_request(
        remote_id=remote["remote_id"], idempotency_key=KEY_1,
        method="job.start", payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    other = {"command": "echo DIFFERENT"}
    with pytest.raises(VanthRemoteProtocolError) as exc:
        store.record_request(
            remote_id=remote["remote_id"], idempotency_key=KEY_1,
            method="job.start", payload=other, digest=digest_of(KEY_1, payload=other),
        )
    assert exc.value.code == "PROTOCOL_REPLAY_MISMATCH"


def test_update_request_status_transitions(tmp_path):
    store = RemoteStore(connect(tmp_path / "ctrl.sqlite"))
    remote = store.create_remote(target="user@host")
    request = store.record_request(
        remote_id=remote["remote_id"], idempotency_key=KEY_1,
        method="job.start", payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    assert request["status"] == "creating"
    request = store.update_request_status(request["request_id"], "submitting")
    assert request["status"] == "submitting"
    request = store.update_request_status(request["request_id"], "accepted")
    assert request["status"] == "accepted"
    request = store.update_request_status(request["request_id"], "completed", response={"job_id": "job_x"})
    assert request["status"] == "completed"
    assert request["response"] == {"job_id": "job_x"}
    with pytest.raises(ValueError):
        store.update_request_status(request["request_id"], "accepted")


def test_replay_tombstone_record_lookup(tmp_path):
    store = RemoteStore(connect(tmp_path / "ctrl.sqlite"))
    remote = store.create_remote(target="user@host")
    digest = digest_of(KEY_1)
    tombstone = store.record_replay_tombstone(remote["remote_id"], KEY_1, digest)
    assert tombstone["tombstone_id"].startswith("tomb_")
    again = store.record_replay_tombstone(remote["remote_id"], KEY_1, digest)
    assert again["tombstone_id"] == tombstone["tombstone_id"]
    assert store.get_replay_tombstone(remote["remote_id"], KEY_1)["digest"] == digest


def test_upsert_shadow(tmp_path):
    store = RemoteStore(connect(tmp_path / "ctrl.sqlite"))
    remote = store.create_remote(target="user@host")
    shadow = store.upsert_shadow(remote_id=remote["remote_id"], remote_job_id="job_x", status="running", payload={"a": 1})
    assert shadow["shadow_id"].startswith("shd_")
    assert shadow["payload"] == {"a": 1}
    updated = store.upsert_shadow(remote_id=remote["remote_id"], remote_job_id="job_x", status="completed")
    assert updated["shadow_id"] == shadow["shadow_id"]
    assert updated["status"] == "completed"


# ---------------------------------------------------------------------------
# RemoteOperationStore basics
# ---------------------------------------------------------------------------


def test_remote_operation_record_replay(tmp_path):
    store = RemoteOperationStore(connect(tmp_path / "remote.sqlite"))
    op = store.record_operation(
        idempotency_key=KEY_1, method="job.start",
        payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    assert op["op_id"].startswith("op_")
    assert op["status"] == "accepted"
    replay = store.record_operation(
        idempotency_key=KEY_1, method="job.start",
        payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    assert replay["op_id"] == op["op_id"]
    with pytest.raises(VanthRemoteProtocolError) as exc:
        store.record_operation(
    idempotency_key=KEY_1, method="job.start",
    payload={"command": "other"}, digest=digest_of(KEY_1, payload={"command": "other"}),
        )
    assert exc.value.code == "PROTOCOL_REPLAY_MISMATCH"


def test_remote_operation_status_transitions(tmp_path):
    store = RemoteOperationStore(connect(tmp_path / "remote.sqlite"))
    op = store.record_operation(
        idempotency_key=KEY_1, method="job.start",
        payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    op = store.update_operation_status(op["op_id"], "queued")
    assert op["status"] == "queued"
    op = store.update_operation_status(op["op_id"], "launched")
    assert op["status"] == "launched"
    op = store.update_operation_status(op["op_id"], "running")
    assert op["status"] == "running"
    op = store.update_operation_status(op["op_id"], "completed")
    assert op["status"] == "completed"
    with pytest.raises(ValueError):
        store.update_operation_status(op["op_id"], "running")


def test_remote_replay_tombstone(tmp_path):
    store = RemoteOperationStore(connect(tmp_path / "remote.sqlite"))
    store.record_operation(
        idempotency_key=KEY_1, method="job.start",
        payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    tombstone = store.record_replay_tombstone(KEY_1, digest_of(KEY_1))
    assert tombstone["tombstone_id"].startswith("tomb_")
    assert store.get_replay_tombstone(KEY_1)["digest"] == digest_of(KEY_1)


# ---------------------------------------------------------------------------
# Five executable crash cases
# ---------------------------------------------------------------------------


def test_crash_1_controller_dies_before_request_commit(tmp_path):
    path = tmp_path / "ctrl.sqlite"
    store = RemoteStore(connect(path))
    remote = store.create_remote(target="user@host")
    store.db.execute("BEGIN IMMEDIATE")
    store.db.execute(
        """
        INSERT INTO remote_requests(request_id, remote_id, idempotency_key, method, payload_json,
          digest, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 'creating', 'now', 'now')
        """,
        ("req_" + "1" * 32, remote["remote_id"], KEY_1, "job.start",
         '{"command": "echo hi"}', digest_of(KEY_1)),
    )
    store.db.close()  # simulate a crash: close without committing

    reopened = RemoteStore(connect(path))
    with pytest.raises(ValueError):
        reopened.get_request_by_key(remote["remote_id"], KEY_1)


def test_crash_2_controller_dies_after_request_commit(tmp_path):
    path = tmp_path / "ctrl.sqlite"
    store = RemoteStore(connect(path))
    remote = store.create_remote(target="user@host")
    committed = store.record_request(
        remote_id=remote["remote_id"], idempotency_key=KEY_1,
        method="job.start", payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    store.db.close()

    reopened = RemoteStore(connect(path))
    replay = reopened.record_request(
        remote_id=remote["remote_id"], idempotency_key=KEY_1,
        method="job.start", payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    assert replay["request_id"] == committed["request_id"]
    rows = reopened.db.execute("SELECT COUNT(*) FROM remote_requests").fetchone()[0]
    assert rows == 1


def test_crash_3_remote_commits_start_but_response_lost(tmp_path):
    path = tmp_path / "remote.sqlite"
    store = RemoteOperationStore(connect(path))
    first = store.record_operation(
        idempotency_key=KEY_1, method="job.start",
        payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    assert first["status"] == "accepted"
    store.db.close()  # response never reached the controller

    reopened = RemoteOperationStore(connect(path))
    replay = reopened.record_operation(
        idempotency_key=KEY_1, method="job.start",
        payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    assert replay["op_id"] == first["op_id"]
    assert replay["status"] == "accepted"
    rows = reopened.db.execute("SELECT COUNT(*) FROM remote_operations").fetchone()[0]
    assert rows == 1


def test_crash_4_remote_commits_queued_job_but_dies_before_launch(tmp_path):
    path = tmp_path / "remote.sqlite"
    store = RemoteOperationStore(connect(path))
    op = store.record_operation(
        idempotency_key=KEY_1, method="job.start",
        payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    store.update_operation_status(op["op_id"], "queued")
    store.db.close()  # died before launching

    reopened = RemoteOperationStore(connect(path))
    recovered = reopened.get_operation(KEY_1)
    assert recovered["status"] == "queued"
    launched = reopened.update_operation_status(recovered["op_id"], "launched")
    assert launched["status"] == "launched"


def test_crash_5_same_key_reused_with_different_request(tmp_path):
    path = tmp_path / "ctrl.sqlite"
    store = RemoteStore(connect(path))
    remote = store.create_remote(target="user@host")
    store.record_request(
        remote_id=remote["remote_id"], idempotency_key=KEY_1,
        method="job.start", payload=make_start_payload(KEY_1), digest=digest_of(KEY_1),
    )
    store.db.close()

    reopened = RemoteStore(connect(path))
    other = {"command": "echo DIFFERENT"}
    with pytest.raises(VanthRemoteProtocolError) as exc:
        reopened.record_request(
            remote_id=remote["remote_id"], idempotency_key=KEY_1,
            method="job.start", payload=other, digest=digest_of(KEY_1, payload=other),
        )
    assert exc.value.code == "PROTOCOL_REPLAY_MISMATCH"
    rows = reopened.db.execute("SELECT COUNT(*) FROM remote_requests").fetchone()[0]
    assert rows == 1


# ---------------------------------------------------------------------------
# State transition helper
# ---------------------------------------------------------------------------


def test_transition_valid_and_invalid():
    assert transition("unpaired", "pairing", machine="pairing") == "pairing"
    assert transition("pairing", "paired", machine="pairing") == "paired"
    assert transition("pairing", "error", machine="pairing") == "error"

    assert transition("creating", "submitting", machine="request") == "submitting"
    assert transition("submitting", "accepted", machine="request") == "accepted"
    assert transition("accepted", "completed", machine="request") == "completed"
    assert transition("accepted", "failed", machine="request") == "failed"
    assert transition("accepted", "lost", machine="request") == "lost"

    assert transition("accepted", "queued", machine="operation") == "queued"
    assert transition("queued", "launched", machine="operation") == "launched"
    assert transition("launched", "running", machine="operation") == "running"
    assert transition("running", "completed", machine="operation") == "completed"
    assert transition("running", "failed", machine="operation") == "failed"

    invalid = [
        ("unpaired", "error", "pairing"),
        ("paired", "pairing", "pairing"),
        ("accepted", "queued", "request"),
        ("creating", "accepted", "request"),
        ("completed", "running", "request"),
        ("accepted", "running", "operation"),
        ("queued", "running", "operation"),
        ("completed", "failed", "operation"),
    ]
    for current, event, machine in invalid:
        with pytest.raises(ValueError):
            transition(current, event, machine=machine)

    with pytest.raises(ValueError):
        transition("bogus", "pairing", machine="pairing")
    with pytest.raises(ValueError):
        transition("unpaired", "pairing", machine="bogus")
