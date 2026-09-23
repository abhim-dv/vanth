"""Remote `job.start` accepts wake metadata but the HOST never delivers it.

The controller registers remote wake targets locally (see
`_remote_submit` + `JobManager.register_remote_wake_targets`), so the fields are
shape-validated for compatibility with un-upgraded controllers and otherwise
ignored on the remote side — never rejected, which would break `job.start`.
"""

from __future__ import annotations

from vanth.remote.protocol import VanthRemoteProtocolError, request_digest, validate_frame


def request(payload: dict) -> dict:
    key = "key-wake-0001"
    return {
        "version": "1",
        "kind": "request",
        "idempotency_key": key,
        "method": "job.start",
        "payload": payload,
        "digest": request_digest("job.start", payload, key),
        "sent_at": "2026-08-20T12:00:00Z",
    }


def test_remote_start_accepts_wake_metadata():
    """Accepted (not rejected) so an un-upgraded controller can still start a job;
    the wake is delivered by the controller, not the host."""
    payload = {
        "command": "echo hi",
        "notify_on": ["completed"],
        "wake_targets": [{"type": "local_command", "command": ["echo", "hi"], "events": ["completed"]}],
    }
    assert validate_frame(request(payload)) == request(payload)


def test_remote_start_still_shape_checks_wake_targets():
    import pytest

    with pytest.raises(VanthRemoteProtocolError):
        validate_frame(request({"command": "echo hi", "wake_targets": "nope"}))
