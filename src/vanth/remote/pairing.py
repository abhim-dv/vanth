"""Pairing orchestration for remote execution (Phase 1, hardened per review P0-1).

The controller-side steps of pairing a remote:

1. Parse and strictly validate the ``[user@]host[:port]`` target.
2. Pin the host key BEFORE dedicated-key authentication: fetch via
   ``ssh-keyscan`` and either verify a caller-supplied SHA256 fingerprint or
   require explicit ``accept_host_key`` consent (TOFU with human approval).
3. Generate one Ed25519 identity and build a DEDICATED allowlist config
   (``Host *`` in a per-remote file always passed via ``-F``) so only the
   Vanth identity can ever authenticate.
4. Install the exact forced-command authorization using a BOOTSTRAP config
   (no IdentityFile — the installer's ambient key performs the one-time
   install). The remote script refuses unrestricted duplicates of our key,
   is idempotent, and replaces only marker-comment lines.
5. Prove the helper with a canonical hello frame and REQUIRE a validated
   hello response — an exit-0 from any command is not proof.
6. On success mark ``paired``; on any failure compensate: remove ONLY the
   exact installed marker line remotely (never unrelated authorizations),
   clean up local state, mark the remote ``error``.

All SSH steps are injectable (``transport``) so tests run without a network.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any

from . import ssh
from .protocol import decode_frame, encode_frame
from .store import RemoteStore
from ..migrations import configure_connection
from ..paths import canonical_home
from ..server import now_iso

# Marker comment embedded in the authorized_keys line; compensation removes
# ONLY lines carrying this marker for this remote id.
MARKER_PREFIX = "vanth-remote"


def marker_comment(remote_id: str) -> str:
    return f"{MARKER_PREFIX}:{remote_id}"


def _sentinel_hello_frame() -> dict[str, Any]:
    return {
        "version": "1",
        "kind": "hello",
        "protocol": "vanth.remote",
        "agent": "vanth-controller-pair",
        "sent_at": now_iso(),
    }


def validate_hello_response(line: str | bytes | None) -> dict[str, Any]:
    """Require an exact, validated Vanth hello response (review P0-1).

    An empty line / exit 0 from an arbitrary command proves nothing; only a
    well-formed ``hello`` frame announcing ``vanth.remote`` counts.
    """
    if line is None:
        raise ssh.VanthRemoteError("sentinel handshake failed: no response from remote helper")
    text = line.decode("utf-8", errors="replace") if isinstance(line, (bytes, bytearray)) else str(line)
    try:
        frame = decode_frame(text.strip())
    except Exception as exc:
        raise ssh.VanthRemoteError(f"sentinel handshake failed: unparseable response ({exc})") from None
    if frame.get("kind") != "hello" or frame.get("protocol") != "vanth.remote":
        raise ssh.VanthRemoteError(
            f"sentinel handshake failed: not a vanth.remote hello response (kind={frame.get('kind')!r})"
        )
    # Daemon-bound hello (review rc14 P0-1.4): the helper must have fetched
    # the epoch live from the remote daemon — a locally-faked hello without
    # one can no longer pass the sentinel.
    epoch = frame.get("state_epoch")
    if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
        raise ssh.VanthRemoteError(
            "sentinel handshake failed: hello is not bound to a reachable remote daemon "
            "(missing state_epoch)"
        )
    return frame


class DefaultTransport:
    """Real transport bound to OpenSSH binaries. Injectable for tests."""

    def fetch_host_keys(self, hostname: str, port: int | None, *, known_hosts_path: str | None = None,
                        fallback_config: str | None = None, config_dir: Any = None) -> list[str]:
        return ssh.fetch_host_keys(
            hostname, port,
            known_hosts_path=known_hosts_path,
            fallback_config=fallback_config,
            config_dir=config_dir,
        )

    def host_key_fingerprints(self, known_hosts_lines: list[str]) -> list[str]:
        return ssh.host_key_fingerprints(known_hosts_lines)

    def generate_identity(self, directory: Path, *, comment: str | None = None) -> dict[str, str]:
        return ssh.generate_identity(directory, comment=comment)

    def install_authorized_key(self, *, install_script: str, bootstrap_config: str, config_dir: Path,
                               target_argv: list[str], timeout: float = 60.0) -> str:
        """Run the install script on the remote over the BOOTSTRAP config."""
        result = ssh.run_ssh(
            [*target_argv, "sh", "-s"],
            config_dir=config_dir,
            config=bootstrap_config,
            timeout=timeout,
            stdin=install_script.encode("utf-8"),
        )
        out = result.stdout.decode("utf-8", errors="replace").strip()
        if result.returncode == 42 or "UNRESTRICTED_KEY_PRESENT" in out:
            raise ssh.VanthRemoteError(
                "an unrestricted authorization already exists for this key; "
                "remove it on the remote before pairing"
            )
        if result.returncode != 0:
            raise ssh.VanthRemoteError(
                f"authorized-keys install failed (exit {result.returncode}): "
                f"{result.stderr.decode('utf-8', errors='replace').strip() or out}"
            )
        return out

    def remove_authorized_key(self, *, remove_script: str, bootstrap_config: str, config_dir: Path,
                              target_argv: list[str], timeout: float = 60.0) -> None:
        ssh.run_ssh(
            [*target_argv, "sh", "-s"],
            config_dir=config_dir,
            config=bootstrap_config,
            timeout=timeout,
            stdin=remove_script.encode("utf-8"),
        )

    def sentinel_probe(self, *, config: str, config_dir: Path, target_argv: list[str],
                       timeout: float = 30.0) -> dict[str, Any]:
        """Send the canonical hello frame over the DEDICATED identity config."""
        result = ssh.run_ssh(
            target_argv,
            config_dir=config_dir,
            config=config,
            timeout=timeout,
            stdin=encode_frame(_sentinel_hello_frame()),
        )
        if result.returncode != 0:
            raise ssh.VanthRemoteError(
                f"sentinel handshake failed: ssh exit {result.returncode}: "
                f"{result.stderr.decode('utf-8', errors='replace').strip()[:300]}"
            )
        return validate_hello_response(result.stdout)


def _remote_dir(home: Path, remote_id: str) -> Path:
    return home / "remote" / remote_id


def _target_argv(target_info: dict[str, Any]) -> list[str]:
    argv: list[str] = []
    if target_info["user"]:
        argv.append(f"{target_info['user']}@{target_info['hostname']}")
    else:
        argv.append(target_info["hostname"])
    if target_info["port"] and target_info["port"] != 22:
        argv = ["-p", str(target_info["port"]), *argv]
    # Port must also be encoded in known_hosts entries ([host]:port form).
    return argv


def pair_remote(
    *,
    target: str,
    name: str | None = None,
    allow_root: bool = False,
    accept_host_key: bool = False,
    host_fingerprint: str | None = None,
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

    target_info = ssh.parse_target(target)
    if (target_info["user"] or "").lower() == "root" and not allow_root:
        raise ssh.VanthRemoteError("refusing to pair as root; pass --allow-root to permit it")

    remote = store.create_remote(target=target, name=name, state="unpaired")
    remote_id = remote["remote_id"]
    remote_dir = _remote_dir(home, remote_id)
    known_hosts_path = remote_dir / "known_hosts"
    marker = marker_comment(remote_id)
    installed_line: str | None = None
    bootstrap_config: str | None = None
    argv: list[str] | None = None
    try:
        store.update_remote_state(remote_id, "pairing")

        # --- 1. Host-key pinning BEFORE any authentication -----------------
        known_hosts_path.parent.mkdir(parents=True, exist_ok=True)
        bootstrap_probe_config = ssh.allowlist_config(
            hostname=target_info["hostname"], user=target_info["user"], port=target_info["port"],
            identity_file=str(remote_dir / "id_ed25519"), known_hosts=str(known_hosts_path),
            include_identity=False,
        )
        host_keys = transport.fetch_host_keys(
            target_info["hostname"], target_info["port"],
            known_hosts_path=str(known_hosts_path),
            fallback_config=bootstrap_probe_config,
            config_dir=remote_dir,
        )
        fingerprints = transport.host_key_fingerprints(host_keys)
        if host_fingerprint:
            if host_fingerprint not in fingerprints:
                raise ssh.VanthRemoteError(
                    f"host key fingerprint mismatch: expected {host_fingerprint}, "
                    f"remote offered {fingerprints}"
                )
        elif not accept_host_key:
            raise ssh.VanthRemoteError(
                "refusing to trust unknown host key; re-run with "
                f"--host-fingerprint {' or '.join(fingerprints[:2])} after verifying, "
                "or --accept-host-key to pin TOFU-style"
            )
        known_hosts_path.parent.mkdir(parents=True, exist_ok=True)
        known_hosts_path.write_text("\n".join(host_keys) + "\n", encoding="utf-8")

        # --- 2. Dedicated identity + configs -------------------------------
        identity = transport.generate_identity(remote_dir, comment=f"{marker}")
        pub_path = Path(identity["public_key_path"])
        pub_fields = pub_path.read_text(encoding="utf-8").strip().split()
        public_key_blob = " ".join(pub_fields[:2])
        helper = helper_command or "vanth-remote-helper"
        installed_line = ssh.authorized_keys_line(public_key_blob, helper, marker_comment=marker)
        bootstrap_config = ssh.allowlist_config(
            hostname=target_info["hostname"], user=target_info["user"], port=target_info["port"],
            identity_file=str(remote_dir / "id_ed25519"), known_hosts=str(known_hosts_path),
            include_identity=False,
        )
        dedicated_config = ssh.allowlist_config(
            hostname=target_info["hostname"], user=target_info["user"], port=target_info["port"],
            identity_file=str(remote_dir / "id_ed25519"), known_hosts=str(known_hosts_path),
        )
        argv = _target_argv(target_info)

        # --- 3. Wrapper + exact forced-command install over bootstrap -----
        # The forced command is a RESTRICTED WRAPPER that binds the helper to
        # the remote daemon's loopback URL and the remote user's own token at
        # exec time — a bare executable answered hello without any daemon and
        # made the sentinel a false positive (review rc14 P0-1.4).
        url_expr = ('sed -n \'s/.*"url"[[:space:]]*:[[:space:]]*"\\([^"]*\\)".*/\\1/p\' '
                    '"$HOME/.vanth/daemon.json" 2>/dev/null | head -1')
        wrapper_script = ssh.remote_wrapper_setup_script(url_expr)
        transport.install_authorized_key(
            install_script=wrapper_script,
            bootstrap_config=bootstrap_config,
            config_dir=remote_dir,
            target_argv=argv,
        )
        wrapper_command = "$HOME/.vanth/remote-wrapper.sh"
        installed_line = ssh.authorized_keys_line(
            public_key_blob, "", marker_comment=marker, forced_command=wrapper_command
        )
        transport.install_authorized_key(
            install_script=ssh.authorized_keys_install_script(
                installed_line, marker,
                key_blob=public_key_blob,
                wrapper_command="$HOME/.vanth/remote-wrapper.sh",
            ),
            bootstrap_config=bootstrap_config,
            config_dir=remote_dir,
            target_argv=argv,
        )
        store.db.execute(
            "UPDATE remotes SET installed_authorization=?, updated_at=? WHERE remote_id=?",
            (installed_line, now_iso(), remote_id),
        )
        store.db.commit()

        # --- 4. Sentinel hello over the DEDICATED identity ------------------
        transport.sentinel_probe(config=dedicated_config, config_dir=remote_dir, target_argv=argv)

        store.update_remote_state(remote_id, "paired")
        row = store.get_remote(remote_id)
        return {
            **row,
            "identity": identity,
            "config": dedicated_config,
            "bootstrap_config": bootstrap_config,
            "known_hosts_path": str(known_hosts_path),
            "host_key_fingerprints": fingerprints,
            "installed_authorization": installed_line,
        }
    except ssh.VanthRemoteError:
        _compensate(store, transport, remote_id, remote_dir, marker, installed_line, bootstrap_config, argv)
        raise
    except Exception as exc:
        _compensate(store, transport, remote_id, remote_dir, marker, installed_line, bootstrap_config, argv)
        raise ssh.VanthRemoteError(f"pairing failed: {exc}") from exc


def _compensate(store: RemoteStore, transport: Any, remote_id: str, remote_dir: Path,
                marker: str, installed_line: str | None,
                bootstrap_config: str | None, argv: list[str] | None) -> None:
    """Mark error + remove ONLY our exact authorization + local cleanup."""
    try:
        store.update_remote_state(remote_id, "error")
    except Exception:
        pass
    if installed_line and bootstrap_config and argv:
        try:
            transport.remove_authorized_key(
                remove_script=ssh.authorized_keys_remove_script(installed_line, marker),
                bootstrap_config=bootstrap_config,
                config_dir=remote_dir,
                target_argv=argv,
            )
        except Exception:
            pass
    shutil.rmtree(remote_dir, ignore_errors=True)


def _default_store(home: Path) -> RemoteStore:
    db = sqlite3.connect(home / "remote.sqlite")
    db.row_factory = sqlite3.Row
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
    transport: Any | None = None,
) -> dict[str, Any]:
    """Remove a remote: delete its local key/config/known-hosts and, when the
    host is reachable, revoke ONLY the exact marker authorization we installed.
    Unrelated authorizations are never touched (review P0-1).
    """
    home = canonical_home(home)
    store = store or _default_store(home)
    transport = transport or DefaultTransport()
    if remote_id is None:
        return {"result": "error", "error": "remote_id is required"}
    row = store.get_remote(remote_id)
    remote_dir = home / "remote" / remote_id
    revoked = False
    installed = row.get("installed_authorization") if isinstance(row, dict) else None
    if installed and remote_dir.exists():
        try:
            target_info = ssh.parse_target(row["target"])
            marker = marker_comment(remote_id)
            bootstrap = ssh.allowlist_config(
                hostname=target_info["hostname"], user=target_info["user"], port=target_info["port"],
                identity_file=str(remote_dir / "id_ed25519"),
                known_hosts=str(remote_dir / "known_hosts"),
                include_identity=False,
            )
            transport.remove_authorized_key(
                remove_script=ssh.authorized_keys_remove_script(installed, marker),
                bootstrap_config=bootstrap,
                config_dir=remote_dir,
                target_argv=_target_argv(target_info),
            )
            revoked = True
        except Exception:
            revoked = False
    if remote_dir.exists():
        shutil.rmtree(remote_dir, ignore_errors=True)
    store.db.execute("DELETE FROM remotes WHERE remote_id=?", (remote_id,))
    store.db.commit()
    return {"result": "ok", "remote_id": remote_id, "target": row["target"], "revoked_remote_authorization": revoked}