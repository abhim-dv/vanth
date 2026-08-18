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


def test_no_binary_and_no_go_raises(tmp_path, monkeypatch):
    pkg = _fake_bundled(tmp_path, monkeypatch)
    shutil.rmtree(pkg / "monitor-bin")
    monkeypatch.delenv("VANTH_CACHE_DIR", raising=False)
    # Ensure `go` is not resolvable.
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    (tmp_path / "empty").mkdir(exist_ok=True)
    with pytest.raises(RuntimeError, match="go"):
        find_monitor_binary()


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
    assert calls["cmd"][1:] == ["--flag"]
