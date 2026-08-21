"""SSH primitives tests (Phase 1) — no network, fast, deterministic."""

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from vanth.remote.ssh import (
    VanthRemoteError,
    allowlist_config,
    authorized_keys_line,
    detect_binaries,
    generate_identity,
    is_unrestricted,
    parse_authorized_keys,
    require_binaries,
    resolve_target,
    run_ssh,
    ssh_host_keys,
)

PUB = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKvneBC8iXW93IclDrFitmVcEaEulP4zFgRFakBjMYz5"


def test_detect_binaries_finds_openssh():
    bins = detect_binaries()
    assert set(bins) == {"ssh", "ssh-keygen", "scp"}
    assert bins["ssh"] and bins["ssh-keygen"]
    assert "ssh" in bins["ssh"].lower() and "ssh-keygen" in bins["ssh-keygen"].lower()


def test_require_binaries_passes_on_this_machine():
    found = require_binaries()
    assert found["ssh"] and found["ssh-keygen"]


def test_allowlist_config_contains_hardened_directives():
    cfg = allowlist_config(
        hostname="example.com", user="alice", port=22,
        identity_file="/tmp/id", known_hosts="/tmp/kh",
    )
    assert "HostName example.com" in cfg
    assert "User alice" in cfg
    assert "IdentitiesOnly yes" in cfg
    assert "BatchMode yes" in cfg
    assert "StrictHostKeyChecking yes" in cfg
    assert "ControlMaster no" in cfg
    assert "ForwardAgent no" in cfg
    assert "ForwardX11 no" in cfg
    assert "RequestTTY no" in cfg
    assert "ProxyCommand none" in cfg
    # Single-target dedicated file: Host * covers every invocation made with
    # -F, so directives can never be skipped by targeting the raw hostname
    # (review P0-1).
    assert "Host *" in cfg
    assert "IdentityFile /tmp/id" in cfg
    assert "IdentitiesOnly yes" in cfg


def test_authorized_keys_line_is_forced_command():
    line = authorized_keys_line(PUB, "/usr/local/bin/vanth-remote-helper")
    assert line.startswith("command=")
    assert "no-pty" in line
    assert "no-agent-forwarding" in line
    assert "no-port-forwarding" in line
    assert "no-X11-forwarding" in line
    assert "no-user-rc" in line
    assert PUB in line


def test_parse_authorized_keys_roundtrip():
    line = authorized_keys_line(PUB, "/usr/local/bin/vanth-remote-helper")
    parsed = parse_authorized_keys(line)
    assert parsed["key"] == PUB
    assert parsed["command"] == '"/usr/local/bin/vanth-remote-helper"'
    assert "no-pty" in parsed["options"]


def test_is_unrestricted():
    forced = authorized_keys_line(PUB, "/usr/local/bin/vanth-remote-helper")
    assert is_unrestricted(forced, PUB) is False
    # A bare key line (no command=) is unrestricted.
    assert is_unrestricted(PUB, PUB) is True
    # Different key is not a match regardless.
    other = "ssh-ed25519 AAAA"
    assert is_unrestricted(forced, other) is False


def test_generate_identity_real_keygen(tmp_path):
    identity = generate_identity(tmp_path, comment="test-key")
    assert Path(identity["private_key_path"]).exists()
    assert Path(identity["public_key_path"]).exists()
    assert identity["fingerprint"].startswith("SHA256:")
    pub = Path(identity["public_key_path"]).read_text().strip()
    assert pub.startswith("ssh-ed25519 ")


def test_ssh_host_keys():
    kh = tmp_path = Path(__import__("tempfile").mkdtemp()) / "known_hosts"
    kh.write_text("example.com ssh-ed25519 AAAA fake-key\n# comment\n")
    entries = ssh_host_keys(kh)
    assert len(entries) == 1
    assert entries[0]["hosts"] == "example.com"
    assert entries[0]["type"] == "ssh-ed25519"


def test_resolve_target_uses_generated_config(monkeypatch, tmp_path):
    """Ambient user config must be neutralized: the invocation uses -F with the
    generated allowlist config, never the user's real ~/.ssh/config."""
    captured = {}

    def fake_run(argv, *args, **kwargs):
        captured["argv"] = argv
        return subprocess.CompletedProcess(argv, 0, stdout=textwrap.dedent(
            """\
            hostname example.com
            user alice
            port 22
            identityfile /nope/id_ed25519
            identitiesonly yes
            userknownhostsfile /nope/known_hosts
            stricthostkeychecking yes
            proxycommand none
            controlmaster no
            """
        ), stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    resolved = resolve_target("alice@example.com", config_dir=tmp_path / "cfg",
                              known_hosts="/nope/kh", identity_file="/nope/id")
    assert captured["argv"][0] == "ssh"
    assert captured["argv"][1] == "-G"
    assert "-F" in captured["argv"]
    # The config file passed to -F is our generated one, not ~/.ssh/config.
    config_arg = captured["argv"][captured["argv"].index("-F") + 1]
    assert str(tmp_path) in config_arg
    assert resolved["hostname"] == "example.com"
    assert resolved["user"] == "alice"
    assert resolved["port"] == 22


def test_effective_config_offers_only_vanth_identity(tmp_path):
    """Review P0-1 required test: real `ssh -G -F <generated>` proves the
    dedicated identity/known-hosts/no-forwarding set applies to the final
    target - ambient ~/.ssh/config cannot inject anything."""
    import subprocess as sp

    from vanth.remote.ssh import allowlist_config

    identity = str(tmp_path / "id_ed25519")
    known_hosts = str(tmp_path / "known_hosts")
    cfg_text = allowlist_config(
        hostname="example.com", user="ubuntu", port=2222,
        identity_file=identity, known_hosts=known_hosts,
    )
    cfg_path = tmp_path / "ssh_config"
    cfg_path.write_text(cfg_text, encoding="utf-8")
    result = sp.run(
        ["ssh", "-G", "-F", str(cfg_path), "alice@example.com"],
        capture_output=True, text=True, timeout=20,
    )
    assert result.returncode == 0, result.stderr
    resolved = {}
    for line in result.stdout.splitlines():
        key, _, value = line.partition(" ")
        resolved[key] = value
    assert resolved["identityfile"] == identity
    assert resolved["identitiesonly"] in ("yes", "true")
    assert resolved["userknownhostsfile"] == known_hosts
    assert resolved["stricthostkeychecking"] in ("yes", "true")
    assert resolved["batchmode"] in ("yes", "true")
    assert resolved["forwardagent"] in ("no", "false")
    assert resolved["requesttty"] in ("no", "false")
    assert resolved["controlmaster"] in ("no", "false")
