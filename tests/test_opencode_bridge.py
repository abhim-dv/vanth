from __future__ import annotations

import shutil
import subprocess

import pytest

from vanth.opencode_bridge import OpenCodeBridgeError, send_delivery_to_opencode, _command_argv


def test_default_command_resolves_through_which(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = r"C:\Users\someone\AppData\Roaming\npm\opencode.CMD"
    monkeypatch.setattr(shutil, "which", lambda name: fake if name == "opencode" else None)
    monkeypatch.delenv("VANTH_OPENCODE_BIN", raising=False)
    assert _command_argv(None) == [fake]


def test_explicit_command_takes_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shutil, "which", lambda name: None)
    monkeypatch.setenv("VANTH_OPENCODE_BIN", r"C:\custom\opencode.exe")
    assert _command_argv(None) == [r"C:\custom\opencode.exe"]


def test_delivery_invokes_opencode_run(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = {}

    def fake_run(argv, **kwargs):
        seen["argv"] = argv
        seen["kwargs"] = kwargs
        return subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = send_delivery_to_opencode(
        {
            "prompt": "job finished",
            "target": {
                "type": "opencode_thread",
                "sessionId": "ses_123",
                "opencode_command": ["custom-opencode", "--pure"],
                "timeout_seconds": 12,
                "cwd": "F:/work/project",
                "attach": "http://127.0.0.1:4096",
            },
        }
    )

    assert seen == {
        "argv": [
            "custom-opencode",
            "--pure",
            "run",
            "--session",
            "ses_123",
            "--dir",
            "F:/work/project",
            "--attach",
            "http://127.0.0.1:4096",
            "--format",
            "json",
            "job finished",
        ],
        "kwargs": {"capture_output": True, "text": True, "timeout": 12},
    }
    assert result == {"session_id": "ses_123", "stdout": '{"type":"session.idle"}\n', "stderr": ""}


@pytest.mark.parametrize("key", ["session_id", "sessionId", "thread_id", "threadId"])
def test_session_id_aliases(monkeypatch: pytest.MonkeyPatch, key: str) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0, "", ""),
    )
    result = send_delivery_to_opencode({"prompt": "wake", "target": {key: "ses_alias"}})
    assert result["session_id"] == "ses_alias"


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"prompt": "wake", "target": {}}, "requires session_id"),
        ({"prompt": "", "target": {"session_id": "ses_1"}}, "requires prompt"),
    ],
)
def test_required_fields(payload: dict, message: str) -> None:
    with pytest.raises(OpenCodeBridgeError, match=message):
        send_delivery_to_opencode(payload)


def test_nonzero_exit_is_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 7, "", "bad session"),
    )
    with pytest.raises(OpenCodeBridgeError, match="opencode exited with 7: bad session"):
        send_delivery_to_opencode({"prompt": "wake", "target": {"session_id": "ses_1"}})


def test_timeout_is_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    def timeout(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(OpenCodeBridgeError, match="ses_1 timed out after 4 seconds"):
        send_delivery_to_opencode(
            {"prompt": "wake", "target": {"session_id": "ses_1", "timeout_seconds": 4}}
        )
