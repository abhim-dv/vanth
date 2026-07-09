from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


class CodexBridgeError(RuntimeError):
    pass


def _default_codex_command() -> list[str]:
    configured = os.environ.get("VANTH_CODEX_BIN")
    if configured:
        return [configured]
    win_default = Path(r"C:\codex\codex.exe")
    if sys.platform == "win32" and win_default.exists():
        return [str(win_default)]
    return ["codex"]


def _command_argv(command: Any) -> list[str]:
    if command is None:
        return _default_codex_command()
    if isinstance(command, list):
        return [str(part) for part in command]
    if isinstance(command, str):
        return [command]
    raise CodexBridgeError("codex_command must be a string path or argv list")


def _reader(stream, out: queue.Queue[str]) -> None:
    while True:
        line = stream.readline()
        if not line:
            return
        out.put(line.rstrip("\r\n"))


class _CodexAppServer:
    def __init__(self, command: Any, timeout_seconds: int) -> None:
        argv = _command_argv(command) + ["app-server", "--listen", "stdio://", "--analytics-default-enabled"]
        self.deadline = time.monotonic() + timeout_seconds
        self.stdout: queue.Queue[str] = queue.Queue()
        self.stderr: queue.Queue[str] = queue.Queue()
        self.stderr_tail: list[str] = []
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert self.proc.stdout is not None
        assert self.proc.stderr is not None
        threading.Thread(target=_reader, args=(self.proc.stdout, self.stdout), daemon=True).start()
        threading.Thread(target=_reader, args=(self.proc.stderr, self.stderr), daemon=True).start()

    def close(self) -> None:
        if self.proc.poll() is not None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=2)

    def send(self, request_id: int, method: str, params: dict[str, Any]) -> None:
        if self.proc.stdin is None:
            raise CodexBridgeError("codex app-server stdin closed")
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()

    def response(self, request_id: int) -> dict[str, Any]:
        while True:
            while not self.stderr.empty():
                self.stderr_tail.append(self.stderr.get())
                self.stderr_tail = self.stderr_tail[-5:]
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise CodexBridgeError(self._error("timed out waiting for codex app-server"))
            try:
                line = self.stdout.get(timeout=min(0.1, remaining))
            except queue.Empty:
                if self.proc.poll() is not None:
                    raise CodexBridgeError(self._error(f"codex app-server exited with {self.proc.returncode}"))
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise CodexBridgeError(self._error(message["error"].get("message", "codex app-server error")))
            return message.get("result", {})

    def _error(self, message: str) -> str:
        if not self.stderr_tail:
            return message
        return f"{message}: {' | '.join(self.stderr_tail)}"


def send_message_to_thread(
    thread_id: str,
    prompt: str,
    *,
    codex_command: Any = None,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    server = _CodexAppServer(codex_command, timeout_seconds)
    try:
        server.send(
            1,
            "initialize",
            {"clientInfo": {"name": "vanth", "version": "0"}, "capabilities": {"experimentalApi": True}},
        )
        server.response(1)
        server.send(2, "thread/resume", {"threadId": thread_id, "excludeTurns": True})
        server.response(2)
        server.send(3, "turn/start", {"threadId": thread_id, "input": [{"type": "text", "text": prompt}]})
        return server.response(3)
    finally:
        server.close()


def send_delivery_to_codex(payload: dict[str, Any]) -> dict[str, Any]:
    target = payload.get("target") or {}
    thread_id = target.get("thread_id") or target.get("threadId")
    prompt = payload.get("prompt")
    if not isinstance(thread_id, str) or not thread_id:
        raise CodexBridgeError("codex_thread target requires thread_id")
    if not isinstance(prompt, str) or not prompt:
        raise CodexBridgeError("delivery payload requires prompt")
    return send_message_to_thread(
        thread_id,
        prompt,
        codex_command=target.get("codex_command"),
        timeout_seconds=int(target.get("timeout_seconds", 30)),
    )


def main() -> None:
    payload = json.load(sys.stdin)
    result = send_delivery_to_codex(payload)
    print(json.dumps(result, separators=(",", ":")))
