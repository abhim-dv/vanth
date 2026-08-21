"""Review-fix regression: helper forwards daemon protocol frames UNCHANGED (P0-5)."""

import io
import json
import os
import sys

from vanth.remote.helper import main
from vanth.remote.protocol import decode_frame


def _run(lines, env):
    stdin = io.StringIO("\n".join(lines) + "\n")
    stdout = io.StringIO()
    old_in, old_out = sys.stdin, sys.stdout
    old_env = dict(os.environ)
    try:
        sys.stdin, sys.stdout = stdin, stdout
        os.environ.update(env)
        code = main([])
    finally:
        sys.stdin, sys.stdout = old_in, old_out
        os.environ.clear()
        os.environ.update(old_env)
    return code, [decode_frame(l) for l in stdout.getvalue().splitlines() if l.strip()]


def test_helper_forwards_daemon_frame_without_double_wrap():
    """The daemon already returns a validated response frame; the helper must
    pass it through flat so result.state_epoch etc. stay reachable."""
    daemon_body = {
        "version": "1", "kind": "response", "request_id": "req_" + "1" * 32,
        "method": "job.start",
        "result": {"job_id": "job_x", "status": "queued",
                   "state_epoch": 3, "acked_offset": 0},
        "sent_at": "2026-08-21T00:00:00Z",
    }
    request = {
        "version": "1", "kind": "request", "request_id": daemon_body["request_id"],
        "idempotency_key": "key-helper-001", "method": "job.start",
        "payload": {"command": "echo hi"}, "sent_at": "2026-08-21T00:00:00Z",
    }

    class FakeResponse:
        def read(self):
            return json.dumps(daemon_body).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    import urllib.request
    orig = urllib.request.urlopen
    urllib.request.urlopen = lambda request, timeout=None: FakeResponse()
    try:
        code, frames = _run(
            [json.dumps(request)],
            {"VANTH_REMOTE_HELPER_URL": "http://127.0.0.1:8765",
             "VANTH_REMOTE_HELPER_TOKEN": "tok"},
        )
    finally:
        urllib.request.urlopen = orig
    assert code == 0
    assert len(frames) == 1
    assert frames[0]["kind"] == "response"
    # Flat fields reachable — this is the exact shape the transfer broker needs.
    assert frames[0]["result"]["state_epoch"] == 3
    assert "response" not in json.dumps(frames[0]["result"]), "double-wrap detected"


def test_helper_rejects_mismatched_request_id():
    daemon_body = {
        "version": "1", "kind": "response", "request_id": "req_other",
        "method": "job.status", "result": {}, "sent_at": "2026-08-21T00:00:00Z",
    }
    request = {
        "version": "1", "kind": "request", "request_id": "req_mine00001",
        "idempotency_key": "key-helper-002", "method": "job.status",
        "payload": {"job_id": "job_x"}, "sent_at": "2026-08-21T00:00:00Z",
    }

    class FakeResponse:
        def read(self):
            return json.dumps(daemon_body).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    import urllib.request
    orig = urllib.request.urlopen
    urllib.request.urlopen = lambda request, timeout=None: FakeResponse()
    try:
        code, frames = _run(
            [json.dumps(request)],
            {"VANTH_REMOTE_HELPER_URL": "http://127.0.0.1:8765",
             "VANTH_REMOTE_HELPER_TOKEN": "tok"},
        )
    finally:
        urllib.request.urlopen = orig
    assert frames and frames[0]["kind"] == "error"
    assert frames[0]["code"] == "INVALID_REQUEST"
