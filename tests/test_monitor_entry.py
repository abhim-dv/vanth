import os
import shutil
import sys

import pytest

import vanth
from vanth.monitor import _binary_name, bundled_binary, find_monitor_binary, main


def _fake_bundled(tmp_path, monkeypatch):
    """Install a fake 'vanth' package dir with a bundled monitor binary."""
    pkg = tmp_path / "vanth"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("__version__ = '0.0.0'\n")
    bin_dir = pkg / "monitor-bin"
    bin_dir.mkdir()
    if os.name == "nt":
        bin = bin_dir / "vanth-monitor.exe"
        bin.write_bytes(b"MZ fake binary")
    else:
        bin = bin_dir / "vanth-monitor"
        bin.write_bytes(b"#!/bin/sh\n")
        bin.chmod(0o755)
    monkeypatch.setattr(vanth, "__file__", str(pkg / "__init__.py"))
    return pkg


def test_bundled_binary_found(tmp_path, monkeypatch):
    pkg = _fake_bundled(tmp_path, monkeypatch)
    found = bundled_binary()
    assert found is not None
    assert found.name == _binary_name()
    assert found.parent == pkg / "monitor-bin"


def test_bundled_binary_missing_returns_none(tmp_path, monkeypatch):
    pkg = _fake_bundled(tmp_path, monkeypatch)
    shutil.rmtree(pkg / "monitor-bin")
    assert bundled_binary() is None


def test_find_monitor_prefers_bundled(tmp_path, monkeypatch):
    pkg = _fake_bundled(tmp_path, monkeypatch)
    assert find_monitor_binary() == pkg / "monitor-bin" / _binary_name()


def test_no_binary_raises_with_reinstall_guidance(tmp_path, monkeypatch):
    """User report: source/sdist installs have no bundled binary. The wrapper
    must fail fast with a reinstall recommendation — no Go fallback, no
    standalone-binary override."""
    pkg = _fake_bundled(tmp_path, monkeypatch)
    shutil.rmtree(pkg / "monitor-bin")
    # A stale env override and cached dev build must both be ignored.
    monkeypatch.setenv("VANTH_MONITOR_BIN", str(tmp_path / "nope.exe"))
    cache_dir = tmp_path / "fresh-cache"
    monkeypatch.setenv("VANTH_CACHE_DIR", str(cache_dir))
    with pytest.raises(RuntimeError) as exc:
        find_monitor_binary()
    assert "uv tool install" in str(exc.value)
    assert "monitor-bin" in str(exc.value)


def test_main_prints_error_and_exits_2_without_binary(
    tmp_path, monkeypatch, capsys
):
    pkg = _fake_bundled(tmp_path, monkeypatch)
    shutil.rmtree(pkg / "monitor-bin")
    import sys as _sys

    monkeypatch.setattr(_sys, "argv", ["vanth-monitor"])
    assert main() == 2
    assert "not present in this install" in capsys.readouterr().err


def test_main_returns_binary_exit_code(tmp_path, monkeypatch):
    pkg = _fake_bundled(tmp_path, monkeypatch)
    bin = pkg / "monitor-bin" / _binary_name()
    bin.write_bytes(b"irrelevant")
    calls = {}
    import vanth.monitor as monitor_mod
    import subprocess

    def fake_call(cmd, **kw):
        calls["cmd"] = cmd
        return 17

    monkeypatch.setattr(monitor_mod.subprocess, "call", fake_call)
    monkeypatch.setattr(sys, "argv", ["vanth-monitor", "--flag"])
    assert main() == 17
    assert calls["cmd"][0] == str(bin)
    # Unknown first args default to the `monitor` subcommand.
    assert calls["cmd"][1:] == ["monitor", "--flag"]
