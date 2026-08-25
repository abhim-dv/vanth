"""Remote forced-command helper tests (Phase 1) — no network, deterministic."""

import io
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from vanth.remote.helper import error_frame, main
from vanth.remote.protocol import decode_frame, encode_frame


def _hello_frame():
    return {
        "version": "1", "kind": "hello", "protocol": "vanth.remote",
        "sent_at": "2026-08-20T12:00:00Z",
    }


def _run_helper(lines, env=None):
    """Feed newline-delimited frames to helper.main; return (exit, output_frames)."""
    import os

    stdin = io.StringIO("\n".join(lines) + "\n")
    stdout = io.StringIO()
    old_in, old_out = sys.stdin, sys.stdout
    old_env = dict(os.environ)
    try:
        sys.stdin, sys.stdout = stdin, stdout
        if env:
            os.environ.update(env)
        code = main([])
    finally:
        sys.stdin, sys.stdout = old_in, old_out
        os.environ.clear()
        os.environ.update(old_env)
    frames = []
    for line in stdout.getvalue().splitlines():
        if line.strip():
            frames.append(decode_frame(line))
    return code, frames


def test_empty_stdin_sentinel_probe_returns_zero():
    code, frames = _run_helper([""])
    assert code == 0
    assert frames == []


def test_hello_responds_with_authenticated_identity(monkeypatch):
    class FakeResponse:
        def read(self):
            return json.dumps({"state_epoch": 3, "instance_id": "inst_test"}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(urllib.request, "urlopen", lambda request, timeout=None: FakeResponse())
    code, frames = _run_helper([json.dumps(_hello_frame())], env={
        "VANTH_REMOTE_HELPER_URL": "http://127.0.0.1:8765",
        "VANTH_REMOTE_HELPER_TOKEN": "tok",
    })
    assert code == 0
    assert len(frames) == 1
    frame = frames[0]
    assert frame["kind"] == "hello"
    assert frame["protocol"] == "vanth.remote"
    assert frame["version"] == "1"
    assert frame["instance_id"] == "inst_test"
    assert frame["state_epoch"] == 3


def test_hello_fails_closed_without_authenticated_daemon():
    code, frames = _run_helper([json.dumps(_hello_frame())])
    assert code == 0
    assert frames and frames[0]["kind"] == "error"
    assert frames[0]["code"] == "AUTH_FAILED"


def test_request_forwards_to_loopback_daemon(monkeypatch):
    captured = {}

    class FakeResponse:
        def read(self):
            return json.dumps({"result": "ok", "job_id": "job_x"}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        captured["auth"] = request.headers.get("Authorization")
        return FakeResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    frame = {
        "version": "1", "kind": "request", "request_id": "req_" + "0" * 32,
        "idempotency_key": "key-1234-abcd", "method": "job.start",
        "payload": {"command": "echo hi"},
        "sent_at": "2026-08-20T12:00:00Z",
    }
    env = {"VANTH_REMOTE_HELPER_URL": "http://127.0.0.1:8765",
           "VANTH_REMOTE_HELPER_TOKEN": "tok"}
    code, frames = _run_helper([json.dumps(frame)], env=env)
    assert code == 0
    assert frames and frames[0]["kind"] == "response"
    assert frames[0]["method"] == "job.start"
    assert frames[0]["result"]["job_id"] == "job_x"
    assert captured["url"] == "http://127.0.0.1:8765/remote/helper"
    assert captured["auth"] == "Bearer tok"


def test_request_non_loopback_url_rejected(monkeypatch):
    def fake_urlopen(request, timeout=None):
        raise AssertionError("should not reach the network")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    frame = {
        "version": "1", "kind": "request", "request_id": "req_" + "0" * 32,
        "idempotency_key": "key-1234-abcd", "method": "job.start",
        "payload": {"command": "echo hi"},
        "sent_at": "2026-08-20T12:00:00Z",
    }
    env = {"VANTH_REMOTE_HELPER_URL": "http://evil.example:8765",
           "VANTH_REMOTE_HELPER_TOKEN": "tok"}
    code, frames = _run_helper([json.dumps(frame)], env=env)
    assert code == 0
    assert frames and frames[0]["kind"] == "error"
    assert frames[0]["code"] == "AUTH_FAILED"


def test_unknown_kind_answered_with_error():
    frame = {"version": "1", "kind": "bogus", "sent_at": "2026-08-20T12:00:00Z"}
    code, frames = _run_helper([json.dumps(frame)])
    assert code == 0
    assert frames and frames[0]["kind"] == "error"
    assert frames[0]["code"] == "PROTOCOL_UNKNOWN_KIND"


def test_unsupported_kind_answered_with_error():
    frame = {"version": "1", "kind": "response", "method": "job.start",
             "result": {}, "sent_at": "2026-08-20T12:00:00Z"}
    code, frames = _run_helper([json.dumps(frame)])
    assert code == 0
    assert frames and frames[0]["kind"] == "error"
    assert frames[0]["code"] == "UNSUPPORTED_FEATURE"


def test_malformed_frame_answered_with_error():
    code, frames = _run_helper(["{not json"])
    assert code == 1
    assert frames and frames[0]["kind"] == "error"
    assert frames[0]["code"] == "PROTOCOL_MALFORMED"


def test_error_frame_has_request_ref():
    frame = error_frame({"method": "job.start", "request_id": "req_abc"}, code="INVALID_REQUEST", message="nope")
    assert frame["kind"] == "error"
    assert frame["code"] == "INVALID_REQUEST"
    assert frame["method"] == "job.start"
    assert frame["request_id"] == "req_abc"
