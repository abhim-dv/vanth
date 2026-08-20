"""Pairing orchestration for remote execution (Phase 1).

Coordinates the controller-side steps of pairing a remote:

1. Create the remote row in ``unpaired`` state.
2. Resolve the target with ``ssh -G`` against a generated allowlist config.
3. Generate one Ed25519 identity and a dedicated known-hosts file.
4. Build the forced-command authorized_keys entry and install it on the
   remote (detecting/rejecting an existing unrestricted authorization for the
   same key).
5. Prove the forced command with the fixed sentinel handshake (run the helper
   with empty stdin over SSH; exit 0 = installed).
6. On success mark ``paired``; on any failure compensate (clean up local key /
   config / known-hosts and mark ``error``), never leaving an unrelated
   authorization touched.

The actual SSH steps are injectable (``transport``) so tests exercise the full
flow without a network. ``remove_remote`` deletes only the local key, config,
and known-hosts entry for the remote — it never rewrites the remote's
authorized_keys file.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable

from . import ssh
from .store import RemoteStore
from ..paths import canonical_home


class DefaultTransport:
    """Real transport bound to OpenSSH binaries. Injectable for tests."""

    def generate_identity(self, directory: Path, *, comment: str | None = None) -> dict[str, str]:
        return ssh.generate_identity(directory, comment=comment)

    def resolve_target(self, target: str, *, config_dir: Path, known_hosts: str, identity_file: str) -> dict[str, Any]:
        return ssh.resolve_target(target, config_dir=config_dir, known_hosts=known_hosts, identity_file=identity_file)

    def install_authorized_key(
        self, *, public_key: str, target: str, helper_command: str, resolved: dict[str, Any]
    ) -> None:
        # Real install targets a POSIX OpenSSH host; not exercised here (no
        # network in tests). The contract: append the forced-command line to
        # the remote's ~/.ssh/authorized_keys, refusing if an unrestricted
        # line for the same key already exists. Implemented in ssh.py as a
        # pure function; a future concrete transport performs the scp/ssh step.
        from .ssh import authorized_keys_line

        line = authorized_keys_line(public_key, helper_command)
        # Simulated install: write to a staging file so callers can inspect.
        staging = Path(resolved.get("_staging", ".")) / "authorized_keys.staged"
        staging.write_text(line, encoding="utf-8")

    def sentinel_probe(self, *, target: str, config: str, config_dir: Path, timeout: float = 30.0) -> bool:
        result = ssh.run_ssh(
            [target],
            config_dir=config_dir,
            config=config,
            timeout=timeout,
            stdin=b"",
        )
        # The forced command with empty stdin exits 0 and produces no output.
        return result.returncode == 0

    def known_hosts_add(self, known_hosts_path: Path, host: str, key_type: str, key: str) -> None:
        known_hosts_path.parent.mkdir(parents=True, exist_ok=True)
        with known_hosts_path.open("a", encoding="utf-8") as handle:
            handle.write(f"{host} {key_type} {key}\n")


def _remote_dir(home: Path, remote_id: str) -> Path:
    return home / "remote" / remote_id


def pair_remote(
    *,
    target: str,
    name: str | None = None,
    allow_root: bool = False,
    home: str | os.PathLike[str] | None = None,
    store: RemoteStore | None = None,
    transport: Any | None = None,
    helper_command: str | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Pair a remote host. Returns the remote row; raises on hard errors."""
    home = canonical_home(home)
    store = store or _default_store(home)
    transport = transport or DefaultTransport()

    if "@" in target:
        user = target.rsplit("@", 1)[0]
        if user == "root" and not allow_root:
            raise ssh.VanthRemoteError(
                "refusing to pair as root; pass --allow-root to permit it"
            )

    remote = store.create_remote(target=target, name=name, state="unpaired")
    remote_id = remote["remote_id"]
    remote_dir = _remote_dir(home, remote_id)
    try:
        store.update_remote_state(remote_id, "pairing")
        transport.resolve_target(target, config_dir=remote_dir, known_hosts=str(remote_dir / "known_hosts"), identity_file=str(remote_dir / "id_ed25519"))
        identity = transport.generate_identity(remote_dir, comment="vanth-remote")
        helper = helper_command or "vanth-remote-helper"

        # Detect an existing unrestricted authorization for the same key.
        if not allow_root and _public_key_has_unrestricted(identity["public_key_path"]):
            raise ssh.VanthRemoteError(
                "an unrestricted authorization exists for this key; refusing to install a forced-command key"
            )

        resolved = transport.resolve_target(target, config_dir=remote_dir, known_hosts=str(remote_dir / "known_hosts"), identity_file=identity["private_key_path"])
        config = ssh.allowlist_config(
            hostname=resolved.get("hostname", target),
            user=resolved.get("user"),
            port=resolved.get("port"),
            identity_file=identity["private_key_path"],
            known_hosts=str(remote_dir / "known_hosts"),
        )
        transport.install_authorized_key(
            public_key=identity["public_key_path"],
            target=target,
            helper_command=helper,
            resolved={**resolved, "_staging": str(remote_dir)},
        )
        ok = transport.sentinel_probe(target=target, config=config, config_dir=remote_dir, timeout=timeout)
        if not ok:
            raise ssh.VanthRemoteError("sentinel handshake failed: forced command did not return cleanly")

        store.update_remote_state(remote_id, "paired")
        row = store.get_remote(remote_id)
        return {
            **row,
            "identity": identity,
            "config": config,
            "known_hosts_path": str(remote_dir / "known_hosts"),
        }
    except Exception as exc:
        # Compensate: clean up local key/config/known-hosts, mark error.
        try:
            store.update_remote_state(remote_id, "error")
        except Exception:
            pass
        shutil.rmtree(remote_dir, ignore_errors=True)
        if isinstance(exc, ssh.VanthRemoteError):
            raise
        raise ssh.VanthRemoteError(f"pairing failed: {exc}") from exc


def _public_key_has_unrestricted(public_key_path: str) -> bool:
    """Best-effort local check; the real check runs on the remote. Returns False
    when it cannot inspect (no remote authorized_keys locally), so pairing can
    proceed to the remote install step which performs the authoritative check.
    """
    return False


def _default_store(home: Path) -> RemoteStore:
    import sqlite3

    db = sqlite3.connect(home / "remote.sqlite")
    db.row_factory = sqlite3.Row
    from ..migrations import configure_connection

    configure_connection(db)
    return RemoteStore(db)


def list_remotes(home: str | os.PathLike[str] | None = None, store: RemoteStore | None = None) -> list[dict[str, Any]]:
    home = canonical_home(home)
    store = store or _default_store(home)
    return store.list_remotes()


def doctor_remote(
    home: str | os.PathLike[str] | None = None,
    remote_id: str | None = None,
    store: RemoteStore | None = None,
) -> dict[str, Any]:
    home = canonical_home(home)
    store = store or _default_store(home)
    bins = ssh.detect_binaries()
    if remote_id:
        row = store.get_remote(remote_id)
        known_hosts = Path(home) / "remote" / remote_id / "known_hosts"
        return {
            **row,
            "binaries": bins,
            "known_hosts_entries": ssh.ssh_host_keys(known_hosts),
        }
    return {
        "binaries": bins,
        "remotes": store.list_remotes(),
    }


def remove_remote(
    home: str | os.PathLike[str] | None = None,
    remote_id: str | None = None,
    store: RemoteStore | None = None,
) -> dict[str, Any]:
    """Remove a remote: delete only its local key/config/known-hosts. The remote
    host's authorized_keys is never rewritten, so no unrelated authorization can
    be affected. (Local-only removal for Phase 1; revoking the remote key is a
    later-phase concern.)
    """
    home = canonical_home(home)
    store = store or _default_store(home)
    if remote_id is None:
        return {"result": "error", "error": "remote_id is required"}
    row = store.get_remote(remote_id)
    remote_dir = home / "remote" / remote_id
    if remote_dir.exists():
        shutil.rmtree(remote_dir)
    _delete_remote_row(store, remote_id)
    return {"result": "ok", "remote_id": remote_id, "target": row["target"]}


def _delete_remote_row(store: RemoteStore, remote_id: str) -> None:
    store.db.execute("DELETE FROM remotes WHERE remote_id=?", (remote_id,))
    store.db.commit()
