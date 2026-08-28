import json
import os
import stat
import subprocess
import sys
import time

import pytest

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


def test_cli_help_and_no_args_list_commands(capsys):
    from vanth import cli

    for argv in ([], ["--help"], ["-h"], ["help"]):
        assert cli.main(argv) == 0
        out = capsys.readouterr().out
        assert "usage: vanth" in out
        assert "status" in out
        assert "setup" in out


def test_server_entry_routes_help_to_cli(capsys):
    from vanth import server

    for argv in (["--help"], ["-h"], ["help"]):
        try:
            server.main(argv)
        except SystemExit as exc:
            assert exc.code == 0
        out = capsys.readouterr().out
        assert "usage: vanth" in out
        assert "setup" in out


def test_server_entry_keeps_bare_mcp(capsys, monkeypatch):
    """Bare `vanth` (no args) must keep running the MCP stdio server, not the CLI."""
    from vanth import server

    called: list = []
    monkeypatch.setattr(server.mcp, "run", lambda: called.append("mcp"))
    server.main([])
    assert called == ["mcp"]


def test_bare_vanth_tty_guard_prints_usage(monkeypatch, capsys):
    """User report rc23: bare `vanth` in an interactive terminal started the
    MCP stdio server and appeared to hang. With a TTY stdin+stdout and no
    args we must print guidance and exit 2 instead."""
    import vanth.server as server

    monkeypatch.setattr(server.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(server.sys.stdout, "isatty", lambda: True)
    called = {"mcp": False}

    def _boom():
        called["mcp"] = True

    monkeypatch.setattr(server, "_run_mcp_server", _boom)
    with pytest.raises(SystemExit) as exc:
        server.main([])
    assert exc.value.code == 2
    assert "vanth-monitor" in capsys.readouterr().err
    assert called["mcp"] is False


def test_bare_vanth_piped_still_runs_mcp(monkeypatch):
    """MCP clients always run bare `vanth` with pipes; the TTY guard must
    not interfere with them."""
    import vanth.server as server

    monkeypatch.setattr(server.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(server.sys.stdout, "isatty", lambda: False)
    monkeypatch.setenv("VANTH_NO_SETUP_HINT", "1")
    ran = {"mcp": False}
    monkeypatch.setattr(server, "_run_mcp_server", lambda: ran.__setitem__("mcp", True))
    server.main([])
    assert ran["mcp"] is True


def test_unknown_interactive_command_does_not_hang(monkeypatch, capsys):
    """Review P2-2: `vanth statsu` (a typo) in a terminal must not enter the
    MCP stdio read loop and look hung; it routes to a usage error and exits 2."""
    import vanth.server as server

    monkeypatch.setattr(server.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(server.sys.stdout, "isatty", lambda: True)
    called = {"mcp": False}
    monkeypatch.setattr(server, "_run_mcp_server", lambda: called.__setitem__("mcp", True))
    with pytest.raises(SystemExit) as exc:
        server.main(["statsu"])
    assert exc.value.code == 2
    assert "unknown command" in capsys.readouterr().err
    assert called["mcp"] is False
