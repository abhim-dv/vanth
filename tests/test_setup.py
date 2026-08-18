import json
import os
import stat
import subprocess
import sys
import time

from vanth import setup
from vanth.paths import canonical_home


def _scratch_configs(tmp_path):
    """Create fake opencode/codex/claude configs in tmp_path and point setup
    discovery at them. Returns the three paths."""
    opencode_path = tmp_path / "opencode.json"
    codex_path = tmp_path / "codex.toml"
    claude_path = tmp_path / "claude.json"

    opencode_path.write_text(
        json.dumps({"mcp": {"other": {"type": "local", "command": ["x"]}}}, indent=2), encoding="utf-8"
    )
    codex_path.write_text('[model]\nname = "gpt"\n\n[mcp_servers.other]\ncommand = "other"\n', encoding="utf-8")
    claude_path.write_text(json.dumps({"mcpServers": {"other": {"command": "x"}}}, indent=2), encoding="utf-8")

    setup.client_config_paths = lambda home=None: {
        "opencode": [opencode_path],
        "codex": [codex_path],
        "claude": [claude_path],
    }
    return opencode_path, codex_path, claude_path


def test_register_all_clients_and_idempotence(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.stdin", None)
    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path)
    home = canonical_home()

    assert setup.run_setup(None, home=home, assume_yes=True) == 0

    # opencode
    oc = json.loads(opencode_path.read_text(encoding="utf-8"))
    assert oc["mcp"]["vanth"] == {"type": "local", "command": ["vanth"], "enabled": True, "timeout": 15000}
    assert oc["mcp"]["other"] is not None  # untouched

    # codex
    import tomllib

    cd = tomllib.loads(codex_path.read_text(encoding="utf-8"))
    assert cd["mcp_servers"]["vanth"]["command"] == "vanth"
    assert cd["mcp_servers"]["vanth"]["env"]["VANTH_HOME"] == str(home)
    assert cd["model"]["name"] == "gpt"  # untouched

    # claude
    cl = json.loads(claude_path.read_text(encoding="utf-8"))
    assert cl["mcpServers"]["vanth"]["command"] == "vanth"
    assert cl["mcpServers"]["vanth"]["env"]["VANTH_HOME"] == str(home)
    assert cl["mcpServers"]["other"] is not None

    # second run is a no-op
    assert setup.run_setup(None, home=home, assume_yes=True) == 0
    assert json.loads(opencode_path.read_text(encoding="utf-8"))["mcp"]["vanth"] is not None


def test_remove_all_clients(tmp_path):
    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path)
    home = canonical_home()

    setup.run_setup(None, home=home, assume_yes=True)
    assert setup.run_setup(None, home=home, remove=True, assume_yes=True) == 0

    oc = json.loads(opencode_path.read_text(encoding="utf-8"))
    assert "vanth" not in oc["mcp"]
    assert oc["mcp"]["other"] is not None

    cd = codex_path.read_text(encoding="utf-8")
    assert "mcp_servers.vanth" not in cd
    assert "mcp_servers.other" in cd

    cl = json.loads(claude_path.read_text(encoding="utf-8"))
    assert "vanth" not in cl["mcpServers"]
    assert cl["mcpServers"]["other"] is not None


def test_backups_are_written_before_change(tmp_path):
    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path)
    home = canonical_home()
    setup.run_setup(None, home=home, assume_yes=True)
    backups = list(tmp_path.glob("*.bak"))
    assert len(backups) == 3


def test_detect_status(tmp_path):
    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path)
    home = canonical_home()
    status = setup.detect_status(home)
    assert status["opencode"][0]["configured"] is False
    setup.run_setup(None, home=home, assume_yes=True)
    status = setup.detect_status(home)
    assert all(entry["configured"] for entries in status.values() for entry in entries)


def test_unknown_client_is_usage_error(tmp_path):
    _scratch_configs(tmp_path)
    home = canonical_home()
    assert setup.run_setup(["bogus"], home=home, assume_yes=True) == 2


def test_no_configs_found_returns_one(tmp_path):
    home = tmp_path / "empty"
    home.mkdir()
    setup.client_config_paths = lambda home=None: {}
    assert setup.run_setup(None, home=home, assume_yes=True) == 1


def test_mcp_startup_hint_mentions_unconfigured_clients(tmp_path, capsys):
    import vanth.server as server

    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path)
    setup.client_config_paths = lambda home=None: {"claude": [claude_path]}
    server._hint_setup()
    captured = capsys.readouterr()
    assert "vanth setup" in captured.err


def test_mcp_startup_hint_silent_when_all_configured(tmp_path, capsys):
    import vanth.server as server

    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path)
    setup.run_setup(None, home=canonical_home(), assume_yes=True)
    setup.client_config_paths = lambda home=None: {"opencode": [opencode_path]}
    server._hint_setup()
    captured = capsys.readouterr()
    assert "vanth setup" not in captured.err


def test_mcp_startup_hint_respects_env_opt_out(tmp_path, capsys, monkeypatch):
    import vanth.server as server

    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path)
    setup.client_config_paths = lambda home=None: {"opencode": [opencode_path]}
    monkeypatch.setenv("VANTH_NO_SETUP_HINT", "1")
    server._hint_setup()
    captured = capsys.readouterr()
    assert "vanth setup" not in captured.err


def test_status_line_prints_client_states(tmp_path, capsys):
    from vanth.cli import _print_setup_status

    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path)
    _print_setup_status()
    captured = capsys.readouterr()
    assert "opencode=not configured" in captured.out
    setup.run_setup(None, home=canonical_home(), assume_yes=True)
    _print_setup_status()
    captured = capsys.readouterr()
    assert "opencode=configured" in captured.out
