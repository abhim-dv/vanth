from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from typing import Any


class OpenCodeBridgeError(RuntimeError):
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


def send_message_to_session(
    session_id: str,
    prompt: str,
    *,
    opencode_command: Any = None,
    timeout_seconds: int = 30,
    directory: str | None = None,
    attach: str | None = None,
) -> dict[str, Any]:
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
    )


def main() -> None:
    payload = json.load(sys.stdin)
    print(json.dumps(send_delivery_to_opencode(payload), separators=(",", ":")))
