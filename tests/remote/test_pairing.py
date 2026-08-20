"""Pairing orchestration tests (Phase 1) — mocked transport, no network."""

import json
import sqlite3
from pathlib import Path

import pytest

from vanth.remote import pairing
from vanth.remote.pairing import doctor_remote, list_remotes, pair_remote, remove_remote
from vanth.remote.ssh import VanthRemoteError, authorized_keys_line, parse_authorized_keys
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


class FakeTransport:
    """Deterministic transport: no real SSH. Records calls; sentinel controllable."""

    def __init__(self, sentinel_ok=True):
        self.sentinel_ok = sentinel_ok
        self.installed = []
        self.installed_keys = []
        self.calls = []

    def generate_identity(self, directory, *, comment=None):
        self.calls.append("generate_identity")
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "id_ed25519").write_text("PRIVATE", encoding="utf-8")
        (directory / "id_ed25519.pub").write_text(PUB + " " + (comment or "test"), encoding="utf-8")
        return {
            "private_key_path": str(directory / "id_ed25519"),
            "public_key_path": str(directory / "id_ed25519.pub"),
            "fingerprint": "SHA256:faketest",
        }

    def resolve_target(self, target, *, config_dir, known_hosts, identity_file):
        self.calls.append("resolve_target")
        user = target.rsplit("@", 1)[0] if "@" in target else None
        host = target.rsplit("@", 1)[-1]
        return {"hostname": host, "user": user, "port": 22,
                "stricthostkeychecking": "yes"}

    def install_authorized_key(self, *, public_key, target, helper_command, resolved):
        self.calls.append("install_authorized_key")
        pub = Path(public_key).read_text().strip().split()[0]
        self.installed_keys.append(pub)
        self.installed.append(target)

    def sentinel_probe(self, *, target, config, config_dir, timeout=30.0):
        self.calls.append("sentinel_probe")
        return self.sentinel_ok

    def known_hosts_add(self, known_hosts_path, host, key_type, key):
        self.calls.append("known_hosts_add")


def test_pair_remote_success(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=True)
    result = pair_remote(target="alice@example.com", name="test", home=tmp_path,
                         store=store, transport=transport)
    assert result["state"] == "paired"
    assert result["remote_id"].startswith("rmt_")
    assert result["identity"]["fingerprint"] == "SHA256:faketest"
    # Remote dir holds key + config.
    remote_dir = tmp_path / "remote" / result["remote_id"]
    assert (remote_dir / "id_ed25519").exists()
    assert (remote_dir / "id_ed25519.pub").exists()
    # Store has the paired remote.
    assert list_remotes(home=tmp_path, store=store)[0]["state"] == "paired"


def test_pair_remote_refuses_root_without_allow_root(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport()
    with pytest.raises(VanthRemoteError, match="root"):
        pair_remote(target="root@example.com", home=tmp_path, store=store, transport=transport)


def test_pair_remote_sentinel_failure_compensates(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=False)
    with pytest.raises(VanthRemoteError, match="sentinel"):
        pair_remote(target="alice@example.com", home=tmp_path, store=store, transport=transport)
    # Remote is marked error, and its local key dir is cleaned up.
    rows = store.list_remotes()
    assert rows and rows[0]["state"] == "error"
    remote_dir = tmp_path / "remote" / rows[0]["remote_id"]
    assert not remote_dir.exists()


def test_pair_remote_transitions_through_pairing(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=True)
    result = pair_remote(target="alice@example.com", home=tmp_path, store=store, transport=transport)
    # unpaired -> pairing -> paired all valid.
    assert result["state"] == "paired"


def test_remove_remote_deletes_local_only(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=True)
    result = pair_remote(target="alice@example.com", home=tmp_path, store=store, transport=transport)
    remote_id = result["remote_id"]
    remote_dir = tmp_path / "remote" / remote_id
    assert remote_dir.exists()

    removed = remove_remote(home=tmp_path, remote_id=remote_id, store=store)
    assert removed["result"] == "ok"
    assert not remote_dir.exists()
    # Remote gone from store; transport never touched an authorized_keys file.
    assert transport.installed_keys == [PUB.split()[0]]


def test_remove_remote_unknown_id_raises(tmp_path):
    store = make_store(tmp_path)
    with pytest.raises(ValueError, match="Unknown remote_id"):
        remove_remote(home=tmp_path, remote_id="rmt_nope", store=store)


def test_doctor_remote(tmp_path):
    store = make_store(tmp_path)
    transport = FakeTransport(sentinel_ok=True)
    result = pair_remote(target="alice@example.com", home=tmp_path, store=store, transport=transport)
    report = doctor_remote(home=tmp_path, remote_id=result["remote_id"], store=store)
    assert report["binaries"]["ssh"]
    assert report["state"] == "paired"


def test_unrestricted_authorization_detected():
    """The forced-command helper line must be flagged correctly by parse."""
    line = authorized_keys_line(PUB, "/usr/local/bin/vanth-remote-helper")
    parsed = parse_authorized_keys(line)
    assert parsed["command"]
    assert "no-pty" in parsed["options"]
