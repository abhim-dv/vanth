"""Controller-side durable request execution tests (Phase 2) — mocked transport."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from vanth.remote.control import RemoteControl
from vanth.remote.protocol import VanthRemoteProtocolError, encode_frame, request_digest
from vanth.remote.store import RemoteStore


def connect(path: Path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


class FakeTransport:
    """Returns canned responses; records frames it sent."""

    def __init__(self, response=None, error=None, fail=False):
        self.response = response
        self.error = error
        self.fail = fail
        self.sent = []
        self.sessions = []

    def open_session(self, remote_row, *, home=None):
        self.sessions.append(remote_row)
        return _FakeSession(self)

    def exchange(self, frame_bytes):
        self.sent.append(frame_bytes)
        if self.fail:
            return None
        if self.error is not None:
            return self.error
        return self.response


class _FakeSession:
    def __init__(self, transport):
        self._t = transport

    def exchange(self, frame_bytes):
        return self._t.exchange(frame_bytes)


def make_store(tmp_path):
    store = RemoteStore(connect(tmp_path / "ctrl.sqlite"))
    remote = store.create_remote(target="user@host")
    return store, remote["remote_id"]


def response_frame(request_id, method, result):
    return encode_frame({
        "version": "1", "kind": "response", "request_id": request_id,
        "method": method, "result": result, "sent_at": "2026-08-20T12:00:00Z",
    }).decode("utf-8").rstrip("\n")


def error_frame(request_id, method, code="INVALID_REQUEST", message="nope"):
    return encode_frame({
        "version": "1", "kind": "error", "request_id": request_id,
        "method": method, "code": code, "message": message, "sent_at": "2026-08-20T12:00:00Z",
    }).decode("utf-8").rstrip("\n")


def start_payload():
    return {"command": "echo hi"}


def status_response(job_id, status="running"):
    return response_frame("req_0" * 0, "job.status", {"job_id": job_id, "status": status})


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


def test_submit_creates_request_and_shadow_in_one_transaction(tmp_path):
    store, remote_id = make_store(tmp_path)
    control = RemoteControl(store, transport=FakeTransport())
    request = control.submit(remote_id, "job.start", start_payload(), idempotency_key="key-submit-01")
    assert request["status"] == "creating"
    assert request["request_id"].startswith("req_")
    rows = store.db.execute("SELECT COUNT(*) FROM remote_requests").fetchone()[0]
    assert rows == 1
    shadows = store.db.execute("SELECT * FROM remote_shadows WHERE remote_id=?", (remote_id,)).fetchall()
    assert len(shadows) == 1
    assert shadows[0]["status"] == "submitting"


def test_submit_replay_same_key_same_payload(tmp_path):
    store, remote_id = make_store(tmp_path)
    control = RemoteControl(store, transport=FakeTransport())
    first = control.submit(remote_id, "job.start", start_payload(), idempotency_key="key-replay-01")
    second = control.submit(remote_id, "job.start", start_payload(), idempotency_key="key-replay-01")
    assert first["request_id"] == second["request_id"]
    assert store.db.execute("SELECT COUNT(*) FROM remote_requests").fetchone()[0] == 1
    assert store.db.execute("SELECT COUNT(*) FROM remote_shadows").fetchone()[0] == 1


def test_submit_replay_same_key_different_payload_rejected(tmp_path):
    store, remote_id = make_store(tmp_path)
    control = RemoteControl(store, transport=FakeTransport())
    control.submit(remote_id, "job.start", start_payload(), idempotency_key="key-mismatch-01")
    with pytest.raises(VanthRemoteProtocolError) as exc:
        control.submit(remote_id, "job.start", {"command": "echo DIFFERENT"}, idempotency_key="key-mismatch-01")
    assert exc.value.code == "PROTOCOL_REPLAY_MISMATCH"
    assert store.db.execute("SELECT COUNT(*) FROM remote_requests").fetchone()[0] == 1


def test_submit_requires_idempotency_key(tmp_path):
    store, remote_id = make_store(tmp_path)
    control = RemoteControl(store, transport=FakeTransport())
    with pytest.raises(VanthRemoteProtocolError) as exc:
        control.submit(remote_id, "job.start", start_payload(), idempotency_key="")
    assert exc.value.code == "INVALID_REQUEST"


# ---------------------------------------------------------------------------
# run_request
# ---------------------------------------------------------------------------


def test_run_request_response_frame_completes(tmp_path):
    store, remote_id = make_store(tmp_path)
    transport = FakeTransport(response=response_frame("req_x", "job.start", {"job_id": "job_x", "status": "running"}))
    control = RemoteControl(store, transport=transport)
    request = control.submit(remote_id, "job.start", start_payload(), idempotency_key="key-run-0001")
    result = control.run_request(remote_id, request)
    assert result["status"] == "completed"
    assert result["response"] == {"job_id": "job_x", "status": "running"}
    shadow = store.db.execute(
        "SELECT * FROM remote_shadows WHERE remote_id=? AND remote_job_id='job_x'", (remote_id,)
    ).fetchone()
    assert shadow is not None and shadow["status"] == "running"
    assert len(transport.sent) == 1


def test_run_request_error_frame_fails(tmp_path):
    store, remote_id = make_store(tmp_path)
    transport = FakeTransport(error=error_frame("req_x", "job.start", code="INVALID_REQUEST", message="bad payload"))
    control = RemoteControl(store, transport=transport)
    request = control.submit(remote_id, "job.start", start_payload(), idempotency_key="key-error-01")
    result = control.run_request(remote_id, request)
    assert result["status"] == "failed"
    assert result["error"] and "INVALID_REQUEST" in result["error"]
    assert store.get_replay_tombstone(remote_id, "key-error-01")["digest"] == request["digest"]


def test_run_request_transport_failure_lost_and_replay_no_second_job(tmp_path):
    store, remote_id = make_store(tmp_path)
    control = RemoteControl(store, transport=FakeTransport(fail=True))
    first = control.submit(remote_id, "job.start", start_payload(), idempotency_key="key-lost-01")
    result = control.run_request(remote_id, first)
    assert result["status"] == "submitting"
    replay = control.replay(remote_id, "key-lost-01")
    assert replay["request_id"] == first["request_id"]
    again = control.submit(remote_id, "job.start", start_payload(), idempotency_key="key-lost-01")
    assert again["request_id"] == first["request_id"]
    assert store.db.execute("SELECT COUNT(*) FROM remote_requests").fetchone()[0] == 1


# ---------------------------------------------------------------------------
# state epoch
# ---------------------------------------------------------------------------


def test_state_epoch_mismatch_refused(tmp_path):
    store, remote_id = make_store(tmp_path)
    control = RemoteControl(store, transport=FakeTransport())
    store.set_state_epoch(remote_id, 1)
    from vanth.remote.ssh import VanthRemoteError

    with pytest.raises(VanthRemoteError, match="state_epoch"):
        control.submit(remote_id, "job.start", start_payload(), idempotency_key="key-epoch-01",
                       expected_state_epoch=5)
    assert store.db.execute("SELECT COUNT(*) FROM remote_requests").fetchone()[0] == 0


def test_state_epoch_match_allowed(tmp_path):
    store, remote_id = make_store(tmp_path)
    control = RemoteControl(store, transport=FakeTransport())
    store.set_state_epoch(remote_id, 3)
    request = control.submit(remote_id, "job.start", start_payload(), idempotency_key="key-epoch-02",
                             expected_state_epoch=3)
    assert request["request_id"].startswith("req_")


# ---------------------------------------------------------------------------
# status / stop / rerun
# ---------------------------------------------------------------------------


def test_status_builds_job_status_method(tmp_path):
    store, remote_id = make_store(tmp_path)
    transport = FakeTransport(response=status_response("job_remote_1", "running"))
    control = RemoteControl(store, transport=transport)
    result = control.status(remote_id, "job_remote_1", idempotency_key="key-status-01")
    assert result["status"] == "completed"
    assert result["response"] == {"job_id": "job_remote_1", "status": "running"}


def test_stop_builds_job_stop_method(tmp_path):
    store, remote_id = make_store(tmp_path)
    transport = FakeTransport(response=response_frame("req_x", "job.stop", {"job_id": "job_remote_1", "status": "cancelled"}))
    control = RemoteControl(store, transport=transport)
    result = control.stop(remote_id, "job_remote_1", idempotency_key="key-stop-001")
    assert result["response"] == {"job_id": "job_remote_1", "status": "cancelled"}
    frame_line = transport.sent[0].decode("utf-8").rstrip("\n")
    import json as _json

    frame = _json.loads(frame_line)
    assert frame["method"] == "job.stop"
    assert frame["payload"]["job_id"] == "job_remote_1"
    assert frame["idempotency_key"] == "key-stop-001"


def test_rerun_builds_job_rerun_method_with_overrides(tmp_path):
    store, remote_id = make_store(tmp_path)
    transport = FakeTransport(response=response_frame("req_x", "job.rerun", {"job_id": "job_remote_1", "status": "queued"}))
    control = RemoteControl(store, transport=transport)
    result = control.rerun(remote_id, "job_remote_1", {"command": "echo again"}, idempotency_key="key-rerun-01")
    assert result["response"] == {"job_id": "job_remote_1", "status": "queued"}
    import json as _json

    frame = _json.loads(transport.sent[0].decode("utf-8").rstrip("\n"))
    assert frame["method"] == "job.rerun"
    assert frame["payload"]["command"] == "echo again"
