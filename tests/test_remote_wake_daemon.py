"""Integration tests for the daemon's remote-wake registration path.

`_remote_submit` must strip wake targets before the remote sees them and
register them LOCALLY against the remote job id, on every request status that
can still yield a job id (creating / submitting / accepted), not only the fresh
"creating" case — a lost response or a crash mid-request used to skip it.
"""

import vanth.daemon as daemon


class FakeControl:
    def __init__(self, submit_status="creating", response=None):
        self.submit_status = submit_status
        self.response = response if response is not None else {"job_id": "job_remote_1"}
        self.submitted_payload = None
        self.ran = False

    def submit(self, remote_id, method, payload, *, idempotency_key, expected_state_epoch=None, expected_instance_id=None):
        self.submitted_payload = dict(payload)
        return {"request_id": "req1", "status": self.submit_status, "idempotency_key": idempotency_key,
                "response": self.response if self.submit_status not in {"creating", "submitting", "accepted"} else None}

    def run_request(self, remote_id, request, *, expected_state_epoch=None):
        self.ran = True
        return {"status": "completed", "response": self.response}


class FakeStore:
    def get_remote(self, remote_id):
        return {"instance_id": "inst1", "state_epoch": 1}


class FakeManager:
    def __init__(self):
        self.registered = None

    def register_remote_wake_targets(self, remote_id, remote_job_id, targets):
        self.registered = (remote_id, remote_job_id, targets)
        return ["target_1"]


def _wire(monkeypatch, control):
    store = FakeStore()
    manager = FakeManager()
    monkeypatch.setattr(daemon, "get_remote_control", lambda: control)
    monkeypatch.setattr(daemon, "get_remote_store", lambda: store)
    monkeypatch.setattr(daemon, "get_manager", lambda: manager)
    monkeypatch.setattr(daemon, "_remote_epoch", lambda remote_id: 1)
    return manager


def _payload():
    return {
        "idempotency_key": "key-1234-abcd",
        "command": "echo hi",
        "wake_targets": [{"type": "local_command", "events": ["completed"], "command": ["echo", "x"]}],
    }


def test_remote_submit_strips_and_registers(monkeypatch):
    control = FakeControl()
    manager = _wire(monkeypatch, control)
    daemon._remote_submit("host-a", "job.start", _payload())
    assert "wake_targets" not in control.submitted_payload  # never sent to the host
    assert manager.registered == (
        "host-a",
        "job_remote_1",
        [{"type": "local_command", "events": ["completed"], "command": ["echo", "x"]}],
    )


def test_remote_submit_redrives_and_registers_when_already_submitting(monkeypatch):
    """A lost response leaves the request `submitting`; a retry must re-drive it
    and still register the wake, not return early."""
    control = FakeControl(submit_status="submitting")
    manager = _wire(monkeypatch, control)
    daemon._remote_submit("host-a", "job.start", _payload())
    assert control.ran is True
    assert manager.registered is not None and manager.registered[1] == "job_remote_1"


def test_remote_submit_registers_from_stored_response_when_terminal(monkeypatch):
    control = FakeControl(submit_status="completed")
    manager = _wire(monkeypatch, control)
    daemon._remote_submit("host-a", "job.start", _payload())
    assert control.ran is False
    assert manager.registered is not None and manager.registered[1] == "job_remote_1"


def test_remote_submit_warns_on_non_terminal_events(monkeypatch):
    control = FakeControl()
    manager = _wire(monkeypatch, control)
    payload = _payload()
    payload["wake_targets"] = [{"type": "local_command", "events": ["checkpoint"], "command": ["echo", "x"]}]
    result = daemon._remote_submit("host-a", "job.start", payload)
    assert any("will never fire" in w for w in result.get("warnings") or [])
    assert manager.registered is not None  # still registered; the warning is advisory


def test_remote_submit_without_wake_targets_is_unchanged(monkeypatch):
    control = FakeControl()
    manager = _wire(monkeypatch, control)
    daemon._remote_submit("host-a", "job.start", {"idempotency_key": "key-1234-abcd", "command": "echo hi"})
    assert manager.registered is None
    assert not (control.submitted_payload or {}).get("warnings")
