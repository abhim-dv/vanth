"""The remote forced-command helper (Phase 1).

This program runs as the ``command=`` forced command on the remote host's
authorized_keys entry for the Vanth key. It reads newline-delimited protocol
frames from stdin and writes response frames to stdout.

The helper NEVER opens the remote jobs database. It forwards request frames to
the already-running remote daemon over its authenticated loopback API using
``VANTH_REMOTE_HELPER_URL`` / ``VANTH_REMOTE_HELPER_TOKEN`` (set by the
installation), and wraps the daemon's reply as a ``response`` frame.

It can never provide a shell, PTY, forwarding, subsystem, or arbitrary SSH
command: only protocol frames are accepted, and everything else is answered
with an ``error`` frame.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from .protocol import (
    FRAME_KINDS,
    VanthRemoteProtocolError,
    decode_frame,
    encode_frame,
)
from ..server import now_iso


def _loopback_url(url: str) -> bool:
    """Reject anything that is not a loopback daemon URL."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = parsed.hostname or ""
    return host in {"127.0.0.1", "localhost", "::1"}


def _forward_to_remote_daemon(frame: dict[str, Any]) -> dict[str, Any]:
    """Forward a request frame to the remote daemon's loopback helper API."""
    url = os.environ.get("VANTH_REMOTE_HELPER_URL", "").rstrip("/")
    token = os.environ.get("VANTH_REMOTE_HELPER_TOKEN", "")
    if not url or not _loopback_url(url):
        return error_frame(
            frame,
            code="AUTH_FAILED",
            message="remote helper is not configured with a loopback daemon URL",
        )
    if not token:
        return error_frame(frame, code="AUTH_FAILED", message="remote helper token is not set")
    body = json.dumps(frame, separators=(",", ":")).encode()
    request = urllib.request.Request(
        url + "/remote/helper",
        data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read().decode())
        except (ValueError, OSError):
            payload = {"result": "error", "error": f"daemon error {exc.code}"}
    except Exception as exc:
        return error_frame(frame, code="AUTH_FAILED", message=f"daemon unreachable: {exc}")
    # Boundary contract: the daemon returns a VALIDATED protocol frame
    # (kind response|error) as the HTTP body. Forward it UNCHANGED after
    # checking it answers THIS request — never re-wrap, or the controller
    # sees {"kind":"response","result":{"kind":"response",...}} and flat
    # result fields (state_epoch, acked_offset, ...) become unreachable.
    if isinstance(payload, dict) and payload.get("version") == "1" and payload.get("kind") in ("response", "error"):
        if payload.get("request_id") is not None and payload.get("request_id") != frame.get("request_id"):
            return error_frame(frame, code="INVALID_REQUEST", message="daemon replied to a different request_id")
        return payload
    # Legacy / non-frame daemon bodies: map HTTP-level errors to error frames,
    # otherwise wrap plain JSON results.
    if isinstance(payload, dict) and payload.get("result") == "error":
        return error_frame(frame, code="INVALID_REQUEST", message=payload.get("error", "remote rejected request"))
    return {
        "version": "1",
        "kind": "response",
        "request_id": frame.get("request_id"),
        "method": frame.get("method"),
        "result": payload,
        "sent_at": now_iso(),
    }


def error_frame(frame: dict[str, Any], *, code: str, message: str) -> dict[str, Any]:
    """Build an ``error`` frame referencing ``frame``'s request id/method."""
    result: dict[str, Any] = {
        "version": "1",
        "kind": "error",
        "code": code,
        "message": message[:2000],
        "sent_at": now_iso(),
    }
    if isinstance(frame, dict):
        request_id = frame.get("request_id")
        method = frame.get("method")
        if isinstance(request_id, str):
            result["request_id"] = request_id
        if isinstance(method, str):
            result["method"] = method
    return result


def _handle_frame(frame: dict[str, Any]) -> dict[str, Any]:
    kind = frame.get("kind")
    if kind == "hello":
        response: dict[str, Any] = {
            "version": "1",
            "kind": "hello",
            "protocol": "vanth.remote",
            "agent": "vanth-remote-helper",
            "sent_at": now_iso(),
        }
        remote_id = os.environ.get("VANTH_REMOTE_HELPER_REMOTE_ID")
        if remote_id:
            response["remote_id"] = remote_id
        state_epoch = _state_epoch()
        if state_epoch is not None:
            response["state_epoch"] = state_epoch
        return response
    if kind == "request":
        return _forward_to_remote_daemon(frame)
    if kind in {"snapshot", "log_range"}:
        return _forward_to_remote_daemon(frame)
    if kind in FRAME_KINDS:
        return error_frame(frame, code="UNSUPPORTED_FEATURE", message=f"frame kind {kind!r} is not accepted by the helper")
    return error_frame(frame, code="PROTOCOL_UNKNOWN_KIND", message=f"unknown frame kind: {kind!r}")


def _state_epoch() -> int | None:
    value = os.environ.get("VANTH_REMOTE_HELPER_STATE_EPOCH")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _read_line(stream) -> str:
    return stream.readline()


def main(argv: list[str] | None = None) -> int:
    """Entry point for the forced command. Reads frames from stdin."""
    line = _read_line(sys.stdin)
    # The sentinel probe sends an empty line; an empty/EOF stdin proves the
    # forced command runs and returns cleanly with exit code 0.
    if not line or not line.strip():
        return 0
    try:
        frame = decode_frame(line)
    except VanthRemoteProtocolError as exc:
        # Malformed JSON is a transport-level failure (exit 1). Frames that
        # parsed as JSON but failed validation (unknown kind/field, etc.) still
        # get a proper error response and exit 0 so the peer sees a real error.
        is_malformed = exc.code == "PROTOCOL_MALFORMED"
        try:
            sys.stdout.write(
                encode_frame(error_frame({}, code=exc.code, message=str(exc))).decode("utf-8")
            )
            sys.stdout.flush()
        except Exception:
            pass
        return 1 if is_malformed else 0
    try:
        response = _handle_frame(frame)
        sys.stdout.write(encode_frame(response).decode("utf-8"))
        sys.stdout.flush()
    except Exception as exc:
        try:
            sys.stdout.write(
                encode_frame(error_frame(frame, code="INVALID_REQUEST", message=str(exc))).decode("utf-8")
            )
            sys.stdout.flush()
        except Exception:
            pass
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
