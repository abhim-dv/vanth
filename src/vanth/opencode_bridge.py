from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from typing import Any


class OpenCodeBridgeError(RuntimeError):
    pass


class OpenCodeSessionNotFound(OpenCodeBridgeError):
    """The targeted opencode session is stale or gone.

    Raised before a model turn when a cheap `session list` probe confirms the
    session id no longer exists. Callers may treat this as permanently
    non-retryable (dead-letter) rather than burning backoff on a doomed turn.
    """

    pass


def _command_argv(command: Any) -> list[str]:
    if command is not None:
        if isinstance(command, list) and command:
            return [str(part) for part in command]
        if isinstance(command, str) and command:
            return [command]
        raise OpenCodeBridgeError("opencode_command must be a string path or argv list")
    configured = os.environ.get("VANTH_OPENCODE_BIN")
    if configured:
        return [configured]
    found = shutil.which("opencode")
    if found:
        return [found]
    return ["opencode"]


def _session_exists(session_id: str, opencode_command: Any, timeout_seconds: float = 5) -> bool | None:
    """Probe whether an opencode session id is still live.

    Cheap non-model probe: runs `opencode session list --format json` and checks
    the parsed array for the session id. Returns True when present, False when
    the probe succeeded but the id is absent, and None on ANY ambiguity
    (timeout, spawn failure, non-zero exit, invalid JSON, unexpected error) —
    None means "can't tell" and must never block a valid dispatch.
    """
    argv = _command_argv(opencode_command) + ["session", "list", "--format", "json"]
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_seconds)
    except (subprocess.TimeoutExpired, OSError):
        return None
    if result.returncode:
        return None
    try:
        sessions = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(sessions, list):
        return None
    for item in sessions:
        if isinstance(item, dict) and item.get("id") == session_id:
            return True
    return False


def send_message_to_session(
    session_id: str,
    prompt: str,
    *,
    opencode_command: Any = None,
    timeout_seconds: int = 30,
    directory: str | None = None,
    attach: str | None = None,
    skip_probe: bool = False,
) -> dict[str, Any]:
    # Probe-before-dispatch: when the target is a plain (non-attached) session
    # and the caller did not opt out, check the session is still live before
    # burning a model turn. The probe NEVER blocks — only a confirmed-missing
    # session (a permanent, retry-free failure) raises; ambiguity proceeds.
    skip_probe = skip_probe or os.environ.get("VANTH_OPENCODE_SKIP_PROBE") == "1"
    if not skip_probe and not attach:
        found = _session_exists(session_id, opencode_command)
        if found is False:
            raise OpenCodeSessionNotFound(
                f"opencode session not found: {session_id} "
                "(stale or removed; refresh the wake target's session_id or start a new session)"
            )

    argv = _command_argv(opencode_command) + ["run", "--session", session_id]
    if directory:
        argv += ["--dir", directory]
    if attach:
        argv += ["--attach", attach]
    argv += ["--format", "json", prompt]

    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        raise OpenCodeBridgeError(f"opencode session {session_id} timed out after {timeout_seconds} seconds") from exc
    except OSError as exc:
        raise OpenCodeBridgeError(f"failed to start opencode: {exc}") from exc

    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        suffix = f": {detail}" if detail else ""
        raise OpenCodeBridgeError(f"opencode exited with {result.returncode}{suffix}")
    return {"session_id": session_id, "stdout": result.stdout, "stderr": result.stderr}


def send_delivery_to_opencode(payload: dict[str, Any]) -> dict[str, Any]:
    target = payload.get("target") or {}
    config = target.get("config")
    if not isinstance(config, dict):
        config = {}
    session_id = (
        target.get("session_id")
        or target.get("sessionId")
        or target.get("thread_id")
        or target.get("threadId")
    )
    prompt = payload.get("prompt")
    if not isinstance(session_id, str) or not session_id:
        raise OpenCodeBridgeError("opencode_thread target requires session_id")
    if not isinstance(prompt, str) or not prompt:
        raise OpenCodeBridgeError("delivery payload requires prompt")

    directory = target.get("cwd") or target.get("dir")
    attach = target.get("attach")
    skip_probe = target.get("skip_probe") or config.get("skip_probe")
    if directory is not None and (not isinstance(directory, str) or not directory):
        raise OpenCodeBridgeError("cwd or dir must be a non-empty string")
    if attach is not None and (not isinstance(attach, str) or not attach):
        raise OpenCodeBridgeError("attach must be a non-empty string")

    return send_message_to_session(
        session_id,
        prompt,
        opencode_command=target.get("opencode_command"),
        timeout_seconds=int(target.get("timeout_seconds", 30)),
        directory=directory,
        attach=attach,
        skip_probe=skip_probe,
    )


def main() -> None:
    payload = json.load(sys.stdin)
    print(json.dumps(send_delivery_to_opencode(payload), separators=(",", ":")))
