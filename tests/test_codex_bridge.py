from __future__ import annotations

import subprocess
import time
from typing import Any

import pytest

from vanth import codex_bridge
from vanth.codex_bridge import CodexBridgeError, send_message_to_thread


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

    def send(self, request_id: int, method: str, params: dict[str, Any]) -> None:
        self.sent.append((request_id, method, params))

    def response(self, request_id: int) -> dict[str, Any]:
        if request_id not in self.responses:
            raise CodexBridgeError("fake server returned no response")
        return self.responses[request_id]

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
    assert result == {"ok": True}
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
    assert result == {"ok": True}
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
