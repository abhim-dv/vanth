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
import queue
import struct
import subprocess
import sys
import threading
import time
from typing import Any

MAX_FRAME_BYTES = 8 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 30.0


def _produce(target_queue: "queue.Queue[Any]", fn, *args: Any) -> None:
    try:
        target_queue.put(fn(*args))
    except Exception as exc:  # noqa: BLE001 - surfaced via the queue
        target_queue.put(exc)


class CodexPipeError(RuntimeError):
    """A protocol/host failure talking to the Codex Desktop app-tools pipe.

    The error text is surfaced in delivery errors but never includes pipe
    details (the path may be sensitive/private). Raw exceptions carrying the
    path are logged separately; only sanitized text reaches delivery errors.
    """

    pass


class CodexPipeUnavailable(CodexPipeError):
    """The Desktop app-tools capability is not present or not reachable.

    Raised when the inherited env lacks ``CODEX_APP_TOOLS_PIPE_PATH``, the pipe
    cannot be opened, or the capability is not advertised. Callers treat this as
    fail-closed (never fall back to a second app-server). The message is
    sanitized — the raw OSError (which may embed the named-pipe path) is never
    included in the public text.
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
    """Serialize one JSON-RPC request/response over the Desktop named pipe.

    A single client instance is used for ONE call sequence (tools/list preflight
    then tools/call) and then closed. The blocking read loop runs on a worker
    thread so ``call()`` can enforce a wall-clock deadline by closing the handle
    (which unblocks the reader). Production delivery additionally runs the whole
    sequence in a killable helper subprocess with a hard deadline so a stalled
    named-pipe read can never hang the relay thread (review rc37 P1).
    """

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
            # Never embed the raw OSError: Windows errors contain the full
            # named-pipe path, which must not reach delivery errors (P2).
            raise CodexPipeUnavailable(
                "cannot open Codex Desktop app-tools pipe (Desktop integration unavailable; "
                "restart/update Desktop or re-register Vanth)"
            ) from exc
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

    def _read_loop(self, request_id: int, deadline: float) -> dict[str, Any]:
        """Blocking reader used on a worker thread; returns the response."""
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CodexPipeError(
                    f"timed out waiting for Codex Desktop response after {self.timeout_seconds}s"
                )
            message = self._read_frame()
            if message.get("id") != request_id:
                continue
            if "error" in message:
                error = message["error"]
                detail = error.get("message", "codex app-tools error") if isinstance(error, dict) else str(error)
                raise CodexPipeError(detail)
            result = message.get("result")
            return result if isinstance(result, dict) else {"result": result}

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Send a JSON-RPC request and wait for its response with a hard
        wall-clock deadline.

        The blocking read runs on a worker thread; if the deadline elapses the
        handle is closed (unblocking the reader) and a timeout error is raised.
        This is what lets a stalled host be interrupted in-process (tests). In
        production the whole sequence also runs inside a killable helper
        subprocess, so a named-pipe read that cannot be unblocked by close() is
        still killed at the process boundary (review rc37 P1).
        """
        request_id = self._next_id()
        self._send_frame(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        )
        deadline = time.monotonic() + self.timeout_seconds
        result_queue: queue.Queue[Any] = queue.Queue()
        reader = threading.Thread(
            target=_produce,
            args=(result_queue, self._read_loop, request_id, deadline),
            name="vanth-pipe-reader",
            daemon=True,
        )
        reader.start()
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # Close the handle to unblock the worker, then reap it.
                    self.close()
                    reader.join(timeout=2)
                    raise CodexPipeError(
                        f"timed out waiting for Codex Desktop {method} after {self.timeout_seconds}s"
                    )
                try:
                    outcome = result_queue.get(timeout=min(0.25, remaining))
                except queue.Empty:
                    continue
                if isinstance(outcome, BaseException):
                    raise outcome
                return outcome
        except CodexPipeError:
            raise
        except Exception:
            self.close()
            reader.join(timeout=2)
            raise

    def tools_list(self) -> list[dict[str, Any]]:
        result = self.call("tools/list", {"threadStartKind": "all"})
        tools = result.get("tools", result.get("toolsList", []))
        if not isinstance(tools, list):
            raise CodexPipeError("malformed tools/list response from Codex Desktop app-tools host")
        return [tool for tool in tools if isinstance(tool, dict)]

    def _tool_namespace_and_name(self, tool: dict[str, Any]) -> tuple[str | None, str | None]:
        """Extract (namespace, name) from a tools/list entry.

        The live Desktop catalog reports them as SEPARATE fields
        (``namespace="codex_app"``, ``name="send_message_to_thread"``); some
        builds also emit a combined ``name`` like ``"codex_app/send_message_to_thread"``.
        Both shapes are accepted (review rc37 P0).
        """
        namespace = tool.get("namespace")
        name = tool.get("name")
        if namespace and name:
            return namespace, name
        if name and isinstance(name, str) and "/" in name:
            parts = name.split("/", 1)
            return parts[0], parts[1]
        return namespace, name

    def has_capability(self, namespace: str, name: str) -> bool:
        return any(
            self._tool_namespace_and_name(tool) == (namespace, name)
            for tool in self.tools_list()
        )

    def send_message_to_thread(
        self,
        *,
        destination_thread_id: str,
        prompt: str,
        caller_thread_id: str,
        call_id: str,
    ) -> dict[str, Any]:
        """Preflight the capability then submit a wake prompt into the task.

        ``caller_thread_id`` (the outer ``params.threadId``) is REQUIRED: the
        native host rejects ``tools/call`` without it (``-32602 Invalid app tool
        request``). The destination is ``arguments.threadId``. Both identities
        are required (review rc37 P0).
        """
        if not isinstance(caller_thread_id, str) or not caller_thread_id:
            raise CodexPipeError(
                "Codex Desktop wake requires a caller thread id (the relay's executor identity); "
                "refusing to call the host without it"
            )
        if not self.has_capability("codex_app", "send_message_to_thread"):
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
            "threadId": caller_thread_id,
            "turnId": call_id,
        }
        result = self.call("tools/call", params)
        if result.get("success") is not True:
            error_text = result.get("error") or result.get("message") or "send_message_to_thread did not report success"
            raise CodexPipeError(str(error_text))
        return result


def _helper_hard_deadline(timeout_seconds: float) -> float:
    """Return the helper subprocess's hard wall-clock lifetime for a pipe call.

    The helper must never outlive the delivery's claim lease, otherwise a
    stalled helper could still be running while the daemon reclaims the
    delivery and a second relay submits the same wake concurrently (review
    rc38 P1). The pipe call itself enforces ``timeout_seconds`` internally, so
    the helper only needs a small buffer beyond it — this is strictly SHORTER
    than the claim lease (``timeout_seconds + delivery_lease_margin``), which
    guarantees the helper dies before the lease expires.
    """
    return timeout_seconds + 2.0


def _claim_lease_seconds(timeout_seconds: float, margin: int = 5) -> float:
    """Return the delivery claim lease length for a Desktop wake.

    Mirrors the daemon's lease computation (``timeout_seconds +
    delivery_lease_margin``) and is used to assert the invariant that the
    helper hard deadline is strictly shorter than the lease (review rc38 P1).
    """
    return timeout_seconds + max(1, margin)


def send_desktop_message(
    *,
    destination_thread_id: str,
    prompt: str,
    caller_thread_id: str,
    call_id: str,
    pipe_path: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    _handle: Any = None,
) -> dict[str, Any]:
    """Open the inherited pipe and submit a Desktop wake prompt.

    ``pipe_path`` overrides the inherited ``CODEX_APP_TOOLS_PIPE_PATH`` (used by
    tests); production always reads the inherited environment. ``_handle`` is a
    test-only raw socket/file handle that bypasses ``open(pipe_path)``.

    Production runs the blocking pipe I/O in a killable helper subprocess with a
    hard deadline (review rc37 P1) so a stalled named-pipe read can never hang
    the relay thread; the subprocess reads the pipe path from its inherited
    environment only. The hard deadline is strictly shorter than the delivery
    claim lease (review rc38 P1).
    """
    if not isinstance(caller_thread_id, str) or not caller_thread_id:
        raise CodexPipeError(
            "Codex Desktop wake requires a caller thread id; refusing before any pipe I/O"
        )
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
    request = {
        "pipe_path": path,
        "timeout_seconds": timeout_seconds,
        "destination_thread_id": destination_thread_id,
        "prompt": prompt,
        "caller_thread_id": caller_thread_id,
        "call_id": call_id,
    }
    argv = [sys.executable, "-m", "vanth.codex_pipe", "--helper"]
    # Strictly shorter than the claim lease (timeout + margin) so a stalled
    # helper can never outlive the lease and cause concurrent duplicate
    # delivery (review rc38 P1).
    hard_deadline = _helper_hard_deadline(timeout_seconds)
    try:
        result = subprocess.run(
            argv,
            input=json.dumps(request, separators=(",", ":")),
            capture_output=True,
            text=True,
            timeout=hard_deadline,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if sys.platform == "win32" else 0,
            start_new_session=sys.platform != "win32",
        )
    except subprocess.TimeoutExpired as exc:
        # The helper was killed by subprocess.run at the hard deadline.
        raise CodexPipeError(
            f"Codex Desktop wake timed out after {timeout_seconds}s (host did not respond)"
        ) from exc
    except OSError as exc:
        raise CodexPipeUnavailable(
            "Codex Desktop wake unavailable: could not start the Desktop bridge helper"
        ) from exc
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        # Sanitized: never surface the pipe path from stderr.
        if stderr:
            raise CodexPipeError(f"Codex Desktop wake failed: {stderr[:500]}")
        raise CodexPipeError("Codex Desktop wake failed (Desktop integration unavailable)")
    try:
        payload = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        raise CodexPipeError("Codex Desktop bridge helper returned an invalid response") from exc
    if payload.get("ok") is not True:
        raise CodexPipeError(payload.get("error", "Codex Desktop wake failed"))
    return payload.get("result", {})


def _helper_main() -> int:
    """Helper subprocess: perform one pipe call sequence with a hard deadline."""
    import queue as _q

    request = json.load(sys.stdin)
    pipe_path = request.get("pipe_path")
    timeout_seconds = float(request.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
    try:
        client = CodexPipeClient(pipe_path, timeout_seconds)
        try:
            result = client.send_message_to_thread(
                destination_thread_id=request["destination_thread_id"],
                prompt=request["prompt"],
                caller_thread_id=request["caller_thread_id"],
                call_id=request["call_id"],
            )
            sys.stdout.write(json.dumps({"ok": True, "result": result}, separators=(",", ":")))
            sys.stdout.flush()
            return 0
        finally:
            client.close()
    except CodexPipeUnavailable as exc:
        sys.stdout.write(json.dumps({"ok": False, "error": str(exc)}, separators=(",", ":")))
        sys.stdout.flush()
        return 0
    except CodexPipeError as exc:
        sys.stdout.write(json.dumps({"ok": False, "error": str(exc)}, separators=(",", ":")))
        sys.stdout.flush()
        return 0
    except Exception as exc:
        # Do not leak the pipe path from an unexpected raw exception.
        sys.stdout.write(
            json.dumps({"ok": False, "error": "Codex Desktop bridge helper failed unexpectedly"}, separators=(",", ":"))
        )
        sys.stdout.flush()
        return 1


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--helper":
        raise SystemExit(_helper_main())
    # Interactive misuse guard: reading a JSON request from stdin.
    request = json.load(sys.stdin)
    pipe_path = request.get("pipe_path")
    timeout_seconds = float(request.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
    client = CodexPipeClient(pipe_path, timeout_seconds)
    try:
        result = client.send_message_to_thread(
            destination_thread_id=request["destination_thread_id"],
            prompt=request["prompt"],
            caller_thread_id=request["caller_thread_id"],
            call_id=request["call_id"],
        )
        print(json.dumps({"ok": True, "result": result}, separators=(",", ":")))
    finally:
        client.close()


if __name__ == "__main__":
    # Module entry point: production launches the helper as
    # `python -m vanth.codex_pipe --helper` (review rc38 P0). Without this the
    # subprocess exits 0 with empty stdout and every Desktop wake fails as an
    # invalid helper response.
    main()
