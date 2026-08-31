from __future__ import annotations

import json
import subprocess
import time
from typing import Any

import pytest

from vanth import codex_bridge
from vanth.codex_bridge import (
    CodexActiveWriterError,
    CodexBridgeError,
    send_delivery_to_codex,
    send_message_to_desktop_thread,
    send_message_to_thread,
)


class _DummyStdin:
    def write(self, data: str) -> int:
        return len(data)

    def flush(self) -> None:
        pass


class _FakeProc:
    def __init__(self, returncode: int | None = None) -> None:
        self.returncode = returncode
        self.stdin = _DummyStdin()
        self.stdout = None
        self.stderr = None

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float) -> int | None:
        return self.returncode


class _RaisingProc(_FakeProc):
    def __init__(self) -> None:
        super().__init__()
        self.error = RuntimeError("process control exploded")

    def terminate(self) -> None:
        raise self.error

    def wait(self, timeout: float) -> int | None:
        raise self.error


class _StubServer(codex_bridge._CodexAppServer):
    def __init__(self, proc: _FakeProc | None = None, timeout_seconds: int = 30) -> None:
        self.deadline = time.monotonic() + timeout_seconds
        self.stderr_tail: list[str] = []
        self.proc = proc if proc is not None else _FakeProc()


class _RecordingServer(_StubServer):
    def __init__(self, timeout_seconds: int = 30) -> None:
        super().__init__(timeout_seconds=timeout_seconds)
        self.sent: list[tuple[int, str, dict[str, Any]]] = []
        self.responses: dict[int, dict[str, Any]] = {}
        self.closed = False
        self.completed_turns: list[dict[str, Any]] = []

    def send(self, request_id: int, method: str, params: dict[str, Any]) -> None:
        self.sent.append((request_id, method, params))

    def response(self, request_id: int) -> dict[str, Any]:
        if request_id not in self.responses:
            raise CodexBridgeError("fake server returned no response")
        return self.responses[request_id]

    def wait_for_turn_completed(self, request_id: int, turn_id: str | None) -> dict[str, Any]:
        return {"id": turn_id or "fake", "status": "completed"}

    def close(self) -> None:
        self.closed = True


def _server_factory(monkeypatch: pytest.MonkeyPatch, server: _StubServer) -> None:
    monkeypatch.setattr(codex_bridge, "_CodexAppServer", lambda command, timeout: server)


def test_initialize_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    server = _RecordingServer()
    calls = {"n": 0}

    def flaky_response(request_id: int) -> dict[str, Any]:
        if request_id == 1:
            calls["n"] += 1
            if calls["n"] <= 2:
                raise CodexBridgeError("codex app-server not ready")
        return {"ok": True}

    server.responses = {1: {"ok": True}, 2: {"ok": True}, 3: {"ok": True}}
    server.response = flaky_response
    _server_factory(monkeypatch, server)

    result = send_message_to_thread("thread-1", "wake", timeout_seconds=30)
    assert result["turn"]["status"] == "completed"
    assert calls["n"] == 3
    assert server.closed


def test_initialize_fails_every_time_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    server = _RecordingServer()
    server.responses = {1: {"ok": True}, 2: {"ok": True}, 3: {"ok": True}}

    def always_fail(request_id: int) -> dict[str, Any]:
        raise CodexBridgeError("codex app-server not ready")

    server.response = always_fail
    _server_factory(monkeypatch, server)

    with pytest.raises(CodexBridgeError, match="not ready"):
        send_message_to_thread("thread-1", "wake", timeout_seconds=30)
    assert server.closed


def test_retries_respect_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    server = _RecordingServer()
    server.responses = {1: {"ok": True}, 2: {"ok": True}, 3: {"ok": True}}

    def flaky_response(request_id: int) -> dict[str, Any]:
        if request_id == 1:
            raise CodexBridgeError("codex app-server not ready")
        return {"ok": True}

    server.response = flaky_response
    server.deadline = time.monotonic() + 0.1
    _server_factory(monkeypatch, server)

    with pytest.raises(CodexBridgeError, match="not ready"):
        send_message_to_thread("thread-1", "wake", timeout_seconds=30)


def test_send_message_returns_server_never_resumes_without_initialize(monkeypatch: pytest.MonkeyPatch) -> None:
    server = _RecordingServer()
    server.responses = {1: {"ok": True}, 2: {"ok": True}, 3: {"ok": True}}
    _server_factory(monkeypatch, server)

    result = send_message_to_thread("thread-1", "wake", timeout_seconds=30)
    assert result["turn"]["status"] == "completed"
    methods = [m for _, m, _ in server.sent]
    assert methods == ["initialize", "thread/resume", "turn/start"]


def test_send_dead_process_raises_with_exit_code() -> None:
    server = _StubServer(proc=_FakeProc(returncode=42))
    server.stderr_tail = ["boom line"]
    with pytest.raises(CodexBridgeError, match="42.*boom line"):
        server.send(1, "initialize", {"clientInfo": {"name": "vanth", "version": "0"}})


def test_popen_launch_failure_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def raising_popen(*args, **kwargs):
        raise FileNotFoundError(2, "No such file or directory", "codex")

    monkeypatch.setattr(subprocess, "Popen", raising_popen)
    with pytest.raises(CodexBridgeError, match="failed to launch codex app-server"):
        send_message_to_thread("thread-1", "wake", timeout_seconds=30)


def test_cleanup_error_does_not_mask_original(monkeypatch: pytest.MonkeyPatch) -> None:
    server = _StubServer(proc=_RaisingProc())

    def always_fail(request_id: int) -> dict[str, Any]:
        raise CodexBridgeError("codex app-server not ready")

    server.response = always_fail
    _server_factory(monkeypatch, server)

    with pytest.raises(CodexBridgeError, match="not ready"):
        send_message_to_thread("thread-1", "wake", timeout_seconds=30)


def test_active_writer_is_permanent_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review P0-2: Codex Desktop's own app-server owns the task; a second
    app-server hitting 'already has an active writer' must raise a permanent,
    non-retryable error — not a generic transient failure."""
    server = _RecordingServer()

    def active_writer(request_id: int) -> dict[str, Any]:
        raise CodexActiveWriterError(
            "codex desktop task already has an active writer: thread already has an active writer"
        )

    server.response = active_writer
    _server_factory(monkeypatch, server)

    with pytest.raises(CodexActiveWriterError, match="active writer"):
        send_message_to_thread("thread-1", "wake", timeout_seconds=30)


def test_codex_desktop_requires_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review P0-2: codex_desktop is experimental and must never silently fall
    back to the CLI app-server; without a desktop_endpoint it errors clearly."""
    with pytest.raises(CodexBridgeError, match="desktop_endpoint"):
        send_delivery_to_codex(
            {"prompt": "wake", "target": {"type": "codex_desktop", "thread_id": "t1"}}
        )


def test_codex_desktop_posts_to_configured_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review P0-2: codex_desktop submits through a relay connected to Desktop's
    existing app (send_message_to_thread operation), not a second app-server."""
    import urllib.request

    seen = {}

    def fake_urlopen(request, timeout):
        seen["url"] = request.full_url
        seen["data"] = json.loads(request.data.decode("utf-8"))
        seen["timeout"] = timeout

        class _Resp:
            def getcode(self):
                return 200

            def read(self):
                return b'{"ok": true}'

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        return _Resp()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    result = send_message_to_desktop_thread(
        "desktop_thread_1",
        "wake now",
        desktop_endpoint="http://127.0.0.1:4096",
        timeout_seconds=15,
    )
    assert seen["url"] == "http://127.0.0.1:4096/send_message_to_thread"
    assert seen["data"] == {"threadId": "desktop_thread_1", "prompt": "wake now"}
    assert seen["timeout"] == 15
    assert result["desktop"] is True
