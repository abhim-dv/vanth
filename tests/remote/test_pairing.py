"""Pairing orchestration tests (Phase 1, hardened per review P0-1)."""

import json
import sqlite3
from pathlib import Path

import pytest

from vanth.remote import pairing
from vanth.remote.pairing import (
    doctor_remote,
    list_remotes,
    pair_remote,
    validate_hello_response,
)
from vanth.remote.protocol import encode_frame
from vanth.remote import ssh
from vanth.remote.store import RemoteStore

PUB = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKvneBC8iXW93IclDrFitmVcEaEulP4zFgRFakBjMYz5"


def connect(path: Path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def make_store(tmp_path):
    return RemoteStore(connect(tmp_path / "remote.sqlite"))


def test_ipv6_target_argv_is_bracketed_for_openssh():
    assert pairing._target_argv({"user": "alice", "hostname": "2001:db8::1", "port": 2200}) == [
        "-p", "2200", "alice@[2001:db8::1]"
    ]


class FakeTransport:
    """Deterministic transport: no real SSH. Records calls; sentinel controllable."""

    def __init__(self, sentinel_ok=True, install_result="INSTALLED"):
        self.sentinel_ok = sentinel_ok
        self.install_result = install_result
        self.installed_lines = []
        self.removed_markers = []
        self.remove_scripts = []
        self.calls = []
        self.fail_sentinel_with = None

    def fetch_host_keys(self, hostname, port, *, known_hosts_path=None, fallback_config=None,
                        config_dir=None, allow_fallback_auth=False):
        self.calls.append("fetch_host_keys")
        return [f"{hostname} ssh-ed25519 AAAAfakehostkey"]

    def host_key_fingerprints(self, lines):
        return ["SHA256:fakehostkey"]

    def generate_identity(self, directory, *, comment=None):
        self.calls.append("generate_identity")
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "id_ed25519").write_text("PRIVATE", encoding="utf-8")
        (directory / "id_ed25519.pub").write_text(PUB + " " + (comment or "test") + "\n", encoding="utf-8")
        return {
            "private_key_path": str(directory / "id_ed25519"),
            "public_key_path": str(directory / "id_ed25519.pub"),
            "fingerprint": "SHA256:faketest",
        }

    def install_authorized_key(self, *, install_script, bootstrap_config, config_dir, target_argv, timeout=60.0):
        self.calls.append("install_authorized_key")
        assert "IdentityFile" not in bootstrap_config, "bootstrap must NOT offer the dedicated key"
        assert "UserKnownHostsFile" in bootstrap_config
        if "UNRESTRICTED_KEY_PRESENT" in install_script and self.install_result != "INSTALLED":
            raise ssh.VanthRemoteError("UNRESTRICTED_KEY_PRESENT")
        if "WRAPPER_INSTALLED" in install_script:
            self.last_wrapper_script = install_script
            import re
            match = re.search(r"VANTH_REMOTE_HELPER_REMOTE_ID='?([^';\n]+)", install_script)
            self.remote_id = match.group(1) if match else None
        elif "UNRESTRICTED_KEY_PRESENT" in install_script:
            self.last_install_script = install_script
        self.installed = True

    def remove_authorized_key(self, *, remove_script, bootstrap_config, config_dir, target_argv, timeout=60.0):
        self.calls.append("remove_authorized_key")
        self.remove_scripts.append(remove_script)
        import re

        m = re.search(r"grep -vF (\S+)", remove_script)
        self.removed_markers.append(m.group(1) if m else "?")

    def sentinel_probe(self, *, config, config_dir, target_argv, timeout=30.0):
        self.calls.append("sentinel_probe")
        assert "IdentitiesOnly yes" in config, "sentinel MUST use the dedicated identity"
        if self.fail_sentinel_with is not None:
            raise self.fail_sentinel_with
        if not self.sentinel_ok:
            raise ssh.VanthRemoteError("sentinel handshake failed: ssh exit 255")
        frame = {
            "version": "1", "kind": "hello", "protocol": "vanth.remote",
            "agent": "vanth-remote-helper", "remote_id": self.remote_id,
            "instance_id": "inst_test", "state_epoch": 3,
            "sent_at": "2026-08-21T00:00:00Z",
        }
        return frame

    def known_hosts_add(self, known_hosts_path, host, key_type, key):
        self.calls.append("known_hosts_add")


def _installed_marker_line(transport, marker):
    """Reconstruct the exact line the install script installs."""
    import shlex

    for ln in transport.last_install_script.splitlines():
        if ln.startswith("printf '%s\\n' "):
            arg = ln[len("printf '%s\\n' "):].strip()
            return shlex.split(arg)[0]
    return ""


def test_pair_remote_success_pins_host_key_and_installs_forced_command(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=True)
    result = pair_remote(target="alice@example.com", name="test", home=tmp_path,
                         store=store, transport=transport, accept_host_key=True)
    assert result["state"] == "paired"
    assert "fetch_host_keys" in transport.calls, "host key must be pinned before auth"
    assert result["host_key_fingerprints"] == ["SHA256:fakehostkey"]
    line = _installed_marker_line(transport, pairing.marker_comment(result["remote_id"]))
    # Per-remote wrapper filename (Sol review): shared wrapper files let
    # multiple remotes overwrite/delete each other's forced command.
    expected_wrapper = f'$HOME/.vanth/{ssh._wrapper_filename(result["remote_id"])}'
    assert line.startswith(f'command="{expected_wrapper}",no-pty,'), line
    assert PUB in line
    assert line.rstrip().endswith(pairing.marker_comment(result["remote_id"]))
    # Wrapper setup ran before the authorization install (review rc14 P0-1.4).
    scripts = [c for c in transport.calls if c == "install_authorized_key"]
    assert len(scripts) >= 2
    wrapper_script = getattr(transport, "last_wrapper_script", "")
    assert 'cat > "$WRAPPER"' in wrapper_script
    assert 'VANTH_REMOTE_HELPER_TOKEN=$TOKEN' in wrapper_script
    assert "vanth-remote-helper" in wrapper_script
    remote_dir = tmp_path / "remote" / result["remote_id"]
    assert (remote_dir / "known_hosts").exists()
    stored = store.get_remote(result["remote_id"])
    assert stored["installed_authorization"].startswith('command=')


def test_pair_remote_binds_explicit_vanth_home_and_helper(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=True)
    result = pair_remote(
        target="alice@example.com", home=tmp_path, store=store, transport=transport,
        accept_host_key=True, remote_home="/srv/vanth-state",
        helper_command="/opt/vanth/bin/vanth-remote-helper",
    )
    line = _installed_marker_line(transport, pairing.marker_comment(result["remote_id"]))
    # Literal remote home with the per-remote wrapper name; shlex.quote leaves
    # safe paths bare (Sol review).
    assert (f'command="/srv/vanth-state/{ssh._wrapper_filename(result["remote_id"])}"' in line), line
    wrapper = transport.last_wrapper_script
    assert wrapper.count("REMOTE_HOME=/srv/vanth-state") == 2
    assert "/opt/vanth/bin/vanth-remote-helper" in wrapper
    pairing.remove_remote(home=tmp_path, remote_id=result["remote_id"], store=store, transport=transport)
    assert any("REMOTE_HOME=/srv/vanth-state" in script for script in transport.remove_scripts)


def test_pair_remote_requires_host_key_consent(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport()
    with pytest.raises(ssh.VanthRemoteError, match="unknown host key"):
        pair_remote(target="alice@example.com", home=tmp_path, store=store, transport=transport)


def test_pair_remote_fingerprint_mismatch_refused(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport()
    with pytest.raises(ssh.VanthRemoteError, match="fingerprint mismatch"):
        pair_remote(target="alice@example.com", home=tmp_path, store=store, transport=transport,
                    host_fingerprint="SHA256:nottheone")


def test_pair_remote_rejects_injection_targets(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=True)
    for bad in ["alice@ex\nample.com", 'alice@host" -oProxyCommand=evil', "a b"]:
        with pytest.raises(ssh.VanthRemoteError):
            pair_remote(target=bad, home=tmp_path, store=store, transport=transport, accept_host_key=True)
    assert store.list_remotes() == [] or all(r["state"] in ("error",) for r in store.list_remotes())


def test_pair_remote_refuses_root_without_allow_root(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport()
    with pytest.raises(ssh.VanthRemoteError, match="root"):
        pair_remote(target="root@example.com", home=tmp_path, store=store, transport=transport)


def test_pair_remote_unrestricted_duplicate_refused_and_compensated(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(install_result="UNRESTRICTED_KEY_PRESENT")
    with pytest.raises(ssh.VanthRemoteError, match="(?i)unrestricted"):
        pair_remote(target="alice@example.com", home=tmp_path, store=store, transport=transport,
                    accept_host_key=True)
    rows = store.list_remotes()
    assert rows and rows[0]["state"] == "error"
    remote_dir = tmp_path / "remote" / rows[0]["remote_id"]
    assert not remote_dir.exists()


def test_pair_remote_sentinel_failure_compensates_by_removing_exact_line(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=False)
    with pytest.raises(ssh.VanthRemoteError, match="sentinel"):
        pair_remote(target="alice@example.com", home=tmp_path, store=store, transport=transport,
                    accept_host_key=True)
    rows = store.list_remotes()
    assert rows and rows[0]["state"] == "error"
    # Compensation removed ONLY our marker line.
    assert any(marker.startswith("vanth-remote:") for marker in transport.removed_markers)
    assert not (tmp_path / "remote" / rows[0]["remote_id"]).exists()


def test_remove_remote_revokes_only_marker_line(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=True)
    result = pair_remote(target="alice@example.com", home=tmp_path, store=store, transport=transport,
                         accept_host_key=True)
    removed = pairing.remove_remote(home=tmp_path, remote_id=result["remote_id"], store=store,
                                    transport=transport)
    assert removed["revoked_remote_authorization"] is True
    assert pairing.marker_comment(result["remote_id"]) in transport.removed_markers
    with pytest.raises(ValueError):
        store.get_remote(result["remote_id"])


def test_validate_hello_response_rejects_non_vanth():
    # Function-local binding: module-global exception references have proven
    # flaky under collection on this tree (see rc14 P2-5 note).
    from vanth.remote.ssh import VanthRemoteError

    with pytest.raises(VanthRemoteError, match="no response"):
        validate_hello_response(None)
    with pytest.raises(VanthRemoteError, match="unparseable"):
        validate_hello_response("")
    with pytest.raises(VanthRemoteError, match="not a vanth"):
        bad = {"version": "1", "kind": "hello", "protocol": "something.else"}
        validate_hello_response(json.dumps(bad))
    # rc14 P0-1.4: hello without a live daemon epoch is a false positive.
    with pytest.raises(VanthRemoteError, match="state_epoch"):
        no_epoch = {"version": "1", "kind": "hello", "protocol": "vanth.remote",
                    "agent": "x", "sent_at": "2026-08-21T00:00:00Z"}
        validate_hello_response(json.dumps(no_epoch))
    with pytest.raises(VanthRemoteError, match="instance_id"):
        no_identity = {"version": "1", "kind": "hello", "protocol": "vanth.remote",
                       "agent": "x", "state_epoch": 3,
                       "sent_at": "2026-08-21T00:00:00Z"}
        validate_hello_response(json.dumps(no_identity))


def test_doctor_remote(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=True)
    result = pair_remote(target="alice@example.com", home=tmp_path, store=store, transport=transport,
                         accept_host_key=True)
    report = doctor_remote(home=tmp_path, remote_id=result["remote_id"], store=store)
    assert report["binaries"]["ssh"]
    assert report["state"] == "paired"



def test_pair_two_remotes_get_distinct_wrappers_and_remove_is_scoped(tmp_path):
    """Sol review P1: wrapper files are per-remote — pairing a second remote
    must not overwrite the first's forced command target, and removing one
    remote must only remove ITS wrapper."""
    store = make_store(tmp_path)
    t1 = FakeTransport(sentinel_ok=True)
    r1 = pair_remote(target="alice@example.com", name="one", home=tmp_path,
                     store=store, transport=t1, accept_host_key=True)
    t2 = FakeTransport(sentinel_ok=True)
    r2 = pair_remote(target="bob@example.com", name="two", home=tmp_path,
                     store=store, transport=t2, accept_host_key=True)
    assert r1["remote_id"] != r2["remote_id"]
    w1 = ssh._wrapper_filename(r1["remote_id"])
    w2 = ssh._wrapper_filename(r2["remote_id"])
    assert w1 != w2
    assert w1 in t1.last_wrapper_script and w2 not in t1.last_wrapper_script
    assert w2 in t2.last_wrapper_script and w1 not in t2.last_wrapper_script
    # Removing remote 1 removes ONLY its wrapper file.
    pairing.remove_remote(home=tmp_path, remote_id=r1["remote_id"], store=store, transport=t1)
    removal_scripts = "\n".join(t1.remove_scripts)
    assert w1 in removal_scripts
    assert w2 not in removal_scripts
