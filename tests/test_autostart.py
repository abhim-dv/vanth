"""Tests for vanth.autostart registration logic and its CLI wiring."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from vanth import autostart


class FakeProc:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _fake_run(results: list[FakeProc]) -> list[list[str]]:
    calls: list[list[str]] = []

    def run(command: list[str]) -> FakeProc:
        calls.append(list(command))
        if not results:
            raise AssertionError("unexpected _run call")
        return results.pop(0)

    return run, calls


@pytest.fixture(autouse=True)
def _patch_platform(monkeypatch):
    monkeypatch.setattr(autostart, "platform", lambda: "linux")


def test_plan_returns_dict_with_home_env_command(monkeypatch, tmp_path):
    command = [sys.executable, "-m", "vanth.daemon"]
    monkeypatch.setattr(autostart, "_command_line", lambda home: command)
    plan = autostart.plan(tmp_path)
    assert isinstance(plan, dict)
    assert plan["platform"] == "linux"
    assert plan["home"] == str(tmp_path)
    assert plan["command"] == command


def test_command_line_is_list_of_str():
    cmd = autostart._command_line(Path("/tmp/x"))
    assert isinstance(cmd, list)
    assert cmd
    assert all(isinstance(part, str) for part in cmd)


def test_detect_windows_enabled_when_schtasks_ok(monkeypatch):
    monkeypatch.setattr(autostart, "platform", lambda: "windows")
    run, _ = _fake_run([FakeProc(returncode=0)])
    state = autostart.detect(Path("/home/x"), _run=run)
    assert state["enabled"] is True


def test_detect_windows_disabled_when_schtasks_fails(monkeypatch):
    monkeypatch.setattr(autostart, "platform", lambda: "windows")
    run, _ = _fake_run([FakeProc(returncode=1, stderr="ERROR: The system cannot find the file specified.")])
    state = autostart.detect(Path("/home/x"), _run=run)
    assert state["enabled"] is False


def test_detect_macos_uses_file_existence(monkeypatch):
    monkeypatch.setattr(autostart, "platform", lambda: "macos")

    run, _ = _fake_run([])
    state = autostart.detect(Path("/home/x"), _run=run, _exists=lambda path: True)
    assert state["enabled"] is True

    state = autostart.detect(Path("/home/x"), _run=run, _exists=lambda path: False)
    assert state["enabled"] is False


def test_detect_linux_uses_unit_file(monkeypatch):
    monkeypatch.setattr(autostart, "platform", lambda: "linux")

    run, _ = _fake_run([FakeProc(returncode=0)])
    state = autostart.detect(Path("/home/x"), _run=run, _exists=lambda path: True)
    assert state["enabled"] is True

    run, _ = _fake_run([])
    state = autostart.detect(Path("/home/x"), _run=run, _exists=lambda path: False)
    assert state["enabled"] is False


def test_enable_dry_run_does_not_execute(monkeypatch, tmp_path):
    monkeypatch.setattr(autostart, "platform", lambda: "linux")

    def boom_run(command):
        raise AssertionError("_run must not be called in dry-run")

    result = autostart.enable(
        tmp_path,
        dry_run=True,
        _run=boom_run,
        _write=lambda path, content: (_ for _ in ()).throw(AssertionError("_write must not be called")),
    )
    assert result["dry_run"] is True
    assert "would_install" in result


def test_enable_writes_file_and_runs_enable(monkeypatch, tmp_path):
    monkeypatch.setattr(autostart, "platform", lambda: "linux")
    written: dict[Path, str] = {}
    run, calls = _fake_run([FakeProc(returncode=0)])

    def write(path: Path, content: str) -> None:
        written[path] = content

    result = autostart.enable(tmp_path, _run=run, _write=write)
    assert result["enabled"] is True
    assert calls, "enable should run the registration command"
    assert any(part and Path(part).parent == tmp_path for part in calls[0])
    assert written, "enable should write the unit file"
    unit = next(iter(written.values()))
    assert "VANTH_HOME" in unit and str(tmp_path) in unit


def test_enable_macos_plist_contains_home(monkeypatch, tmp_path):
    monkeypatch.setattr(autostart, "platform", lambda: "macos")
    written: dict[Path, str] = {}
    run, _ = _fake_run([FakeProc(returncode=0)])
    result = autostart.enable(tmp_path, _run=run, _write=lambda p, c: written.__setitem__(p, c))
    assert result["enabled"] is True
    plist = next(iter(written.values()))
    assert "com.vanth.daemon" in plist
    assert "VANTH_HOME" in plist
    assert str(tmp_path) in plist


def test_disable_removes_registration(monkeypatch, tmp_path):
    monkeypatch.setattr(autostart, "platform", lambda: "linux")
    run, calls = _fake_run([FakeProc(returncode=0)])
    removed: list[Path] = []

    result = autostart.disable(tmp_path, _run=run, _remove=removed.append)
    assert result["enabled"] is False
    assert calls, "disable should run the disable command"
    assert removed, "disable should remove the unit file"


def test_disable_dry_run_does_not_execute(monkeypatch, tmp_path):
    monkeypatch.setattr(autostart, "platform", lambda: "linux")
    result = autostart.disable(
        tmp_path,
        dry_run=True,
        _run=lambda command: (_ for _ in ()).throw(AssertionError("_run must not be called")),
        _remove=lambda path: (_ for _ in ()).throw(AssertionError("_remove must not be called")),
    )
    assert result["dry_run"] is True
    assert "would_uninstall" in result


def test_cli_autostart_status_json_disabled(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(autostart, "platform", lambda: "linux")
    monkeypatch.setattr(
        autostart, "detect", lambda home, **kwargs: {
            "platform": "linux",
            "target": "~/.config/systemd/user/vanth-daemon.service",
            "enabled": False,
        }
    )
    from vanth import cli

    code = cli.main(["autostart", "status", "--json"])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["enabled"] is False
    assert payload["platform"] == "linux"


def test_cli_autostart_help(monkeypatch, capsys):
    from vanth import cli

    code = cli.main(["autostart", "--help"])
    assert code == 0
    out = capsys.readouterr().out
    assert "enable|disable|status" in out
