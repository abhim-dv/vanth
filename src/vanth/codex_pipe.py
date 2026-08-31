"""Native app-tools host-pipe client for Codex Desktop wake (review rc36 P0).

Codex Desktop runs a private app-tools JSON-RPC server on a Windows named pipe
whose path is inherited by the MCP process as ``CODEX_APP_TOOLS_PIPE_PATH``.
The bundled app-tools MCP server exposes ``codex_app/send_message_to_thread``
on that pipe; calling it submits a follow-up prompt into the already-running
Desktop task — the visible app owns the task writer, so this never hits the
second-writer problem of spawning a second ``codex app-server``.

This module contains ONLY the framing + call logic. It takes the pipe path from
the current process's inherited environment (never from a job target or daemon
message), implements the length-prefixed JSON-RPC framing with strict size
caps and exact/fragmented reads, preflights the capability, and submits a
``tools/call`` envelope. It is deliberately small and replaceable so a future
stable public Desktop endpoint can swap in without touching the wake/relay
machinery.
"""

from __future__ import annotations

import json
import os
import struct
import time
from typing import Any

MAX_FRAME_BYTES = 8 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 30.0


class CodexPipeError(RuntimeError):
    """A protocol/host failure talking to the Codex Desktop app-tools pipe.

    The error text is surfaced in delivery errors but never includes pipe
    details (the path may be sensitive/private).
    """

    pass


class CodexPipeUnavailable(CodexPipeError):
    """The Desktop app-tools capability is not present or not reachable.

    Raised when the inherited env lacks ``CODEX_APP_TOOLS_PIPE_PATH``, the pipe
    cannot be opened, or the capability is not advertised. Callers treat this as
    fail-closed (never fall back to a second app-server).
    """

    pass


def app_tools_pipe_path() -> str | None:
    """Return the inherited Desktop app-tools pipe path, if any."""
    value = os.environ.get("CODEX_APP_TOOLS_PIPE_PATH")
    if not value:
        return None
    return value


def _read_exact(handle, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        try:
            if hasattr(handle, "recv"):
                chunk = handle.recv(length - len(chunks))
            else:
                chunk = handle.read(length - len(chunks))
        except OSError as exc:
            # A reset/aborted read on Windows surfaces as ConnectionResetError;
            # the peer is gone either way — treat as a closed connection.
            raise CodexPipeError("Codex Desktop app-tools host closed the connection") from exc
        if not chunk:
            raise CodexPipeError("Codex Desktop app-tools host closed the connection")
        chunks.extend(chunk)
    return bytes(chunks)


def _write_all(handle, data: bytes) -> None:
    view = memoryview(data)
    while view:
        try:
            if hasattr(handle, "send"):
                written = handle.send(view)
            else:
                written = handle.write(view)
        except OSError as exc:
            raise CodexPipeError("lost connection to Codex Desktop app-tools host (write)") from exc
        if written is None or written <= 0:
            raise CodexPipeError("Codex Desktop app-tools host closed the connection (write)")
        view = view[written:]


class CodexPipeClient:
    """Serialize one JSON-RPC request/response over the Desktop named pipe."""

    def __init__(self, pipe_path: str, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        if not pipe_path:
            raise CodexPipeUnavailable("Codex Desktop app-tools pipe is not configured")
        self.pipe_path = pipe_path
        self.timeout_seconds = timeout_seconds
        # `open(path, "r+b", buffering=0)` opens a Windows named pipe handle
        # (also works for a POSIX socket/FIFO for tests). Buffering=0 is
        # required for a true byte stream over a named pipe. Tests may pass a
        # raw socket instead (socket.socketpair) which also works.
        try:
            self.handle = open(pipe_path, "r+b", buffering=0)  # noqa: SIM115 - raw pipe handle
        except OSError as exc:
            raise CodexPipeUnavailable(f"cannot open Codex Desktop app-tools pipe: {exc}") from exc
        self._request_id = 0

    def close(self) -> None:
        try:
            if hasattr(self.handle, "close"):
                self.handle.close()
        except Exception:
            pass

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _send_frame(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        if len(body) > MAX_FRAME_BYTES:
            raise CodexPipeError("request frame exceeds 8 MiB cap")
        _write_all(self.handle, struct.pack("<I", len(body)) + body)

    def _read_frame(self) -> dict[str, Any]:
        header = _read_exact(self.handle, 4)
        (length,) = struct.unpack("<I", header)
        if length > MAX_FRAME_BYTES:
            raise CodexPipeError("response frame exceeds 8 MiB cap")
        body = _read_exact(self.handle, length)
        try:
            message = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise CodexPipeError("invalid JSON response from Codex Desktop app-tools host") from exc
        if not isinstance(message, dict):
            raise CodexPipeError("malformed response from Codex Desktop app-tools host")
        return message

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Send a JSON-RPC request and wait for its response.

        Requests are serialized per connection and the response id is verified
        so a stale/out-of-order frame is never misattributed.
        """
        request_id = self._next_id()
        self._send_frame(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        )
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CodexPipeError(f"timed out waiting for Codex Desktop {method} after {self.timeout_seconds}s")
            # Blocking read is fine for a serialized per-connection client; the
            # dispatcher runs us on its own worker thread and the delivery lease
            # covers the adapter timeout.
            message = self._read_frame()
            if message.get("id") != request_id:
                continue
            if "error" in message:
                error = message["error"]
                detail = error.get("message", "codex app-tools error") if isinstance(error, dict) else str(error)
                raise CodexPipeError(detail)
            result = message.get("result")
            return result if isinstance(result, dict) else {"result": result}

    def tools_list(self) -> list[dict[str, Any]]:
        result = self.call("tools/list", {"threadStartKind": "all"})
        tools = result.get("tools", result.get("toolsList", []))
        if not isinstance(tools, list):
            raise CodexPipeError("malformed tools/list response from Codex Desktop app-tools host")
        return [tool for tool in tools if isinstance(tool, dict)]

    def has_capability(self, name: str) -> bool:
        return any(tool.get("name") == name for tool in self.tools_list())

    def send_message_to_thread(
        self,
        *,
        destination_thread_id: str,
        prompt: str,
        caller_thread_id: str | None,
        call_id: str,
    ) -> dict[str, Any]:
        """Preflight the capability then submit a wake prompt into the task."""
        if not self.has_capability("codex_app/send_message_to_thread"):
            raise CodexPipeError(
                "Codex Desktop app-tools host does not advertise codex_app/send_message_to_thread "
                "(update Desktop or re-register Vanth)"
            )
        arguments: dict[str, Any] = {"threadId": destination_thread_id, "prompt": prompt}
        params: dict[str, Any] = {
            "arguments": arguments,
            "callId": call_id,
            "namespace": "codex_app",
            "tool": "send_message_to_thread",
            "turnId": call_id,
        }
        if caller_thread_id:
            params["threadId"] = caller_thread_id
        result = self.call("tools/call", params)
        if result.get("success") is not True:
            error_text = result.get("error") or result.get("message") or "send_message_to_thread did not report success"
            raise CodexPipeError(str(error_text))
        return result


def send_desktop_message(
    *,
    destination_thread_id: str,
    prompt: str,
    caller_thread_id: str | None = None,
    call_id: str,
    pipe_path: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    _handle: Any = None,
) -> dict[str, Any]:
    """Open the inherited pipe and submit a Desktop wake prompt.

    ``pipe_path`` overrides the inherited ``CODEX_APP_TOOLS_PIPE_PATH`` (used by
    tests); production always reads the inherited environment. ``_handle`` is a
    test-only raw socket/file handle that bypasses ``open(pipe_path)``.
    """
    if _handle is not None:
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = _handle
        client.timeout_seconds = timeout_seconds
        client._request_id = 0
        try:
            return client.send_message_to_thread(
                destination_thread_id=destination_thread_id,
                prompt=prompt,
                caller_thread_id=caller_thread_id,
                call_id=call_id,
            )
        finally:
            client.close()
    path = pipe_path or app_tools_pipe_path()
    if not path:
        raise CodexPipeUnavailable(
            "Codex Desktop wake is unavailable: CODEX_APP_TOOLS_PIPE_PATH is not set "
            "(Desktop integration not active; restart/update Desktop or re-register Vanth)"
        )
    client = CodexPipeClient(path, timeout_seconds)
    try:
        return client.send_message_to_thread(
            destination_thread_id=destination_thread_id,
            prompt=prompt,
            caller_thread_id=caller_thread_id,
            call_id=call_id,
        )
    finally:
        client.close()
