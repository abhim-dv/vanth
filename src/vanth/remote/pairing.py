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
import shlex
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
    instance_id = frame.get("instance_id")
    if not isinstance(instance_id, str) or not instance_id.strip():
        raise ssh.VanthRemoteError(
            "sentinel handshake failed: hello is not bound to an authenticated remote instance "
            "(missing instance_id)"
        )
    return frame


class DefaultTransport:
    """Real transport bound to OpenSSH binaries. Injectable for tests."""

    def fetch_host_keys(self, hostname: str, port: int | None, *, known_hosts_path: str | None = None,
                        fallback_config: str | None = None, config_dir: Any = None,
                        allow_fallback_auth: bool = False) -> list[str]:
        return ssh.fetch_host_keys(
            hostname, port,
            known_hosts_path=known_hosts_path,
            fallback_config=fallback_config,
            config_dir=config_dir,
            allow_fallback_auth=allow_fallback_auth,
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
        result = ssh.run_ssh(
            [*target_argv, "sh", "-s"],
            config_dir=config_dir,
            config=bootstrap_config,
            timeout=timeout,
            stdin=remove_script.encode("utf-8"),
        )
        if result.returncode != 0:
            raise ssh.VanthRemoteError(
                f"remote authorization cleanup failed (exit {result.returncode}): "
                f"{result.stderr.decode('utf-8', errors='replace').strip()[:300]}"
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
    hostname = target_info["hostname"]
    # OpenSSH requires bracketed literals when an IPv6 address is supplied in
    # the target operand (the port remains the separate -p argument).
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    if target_info["user"]:
        argv.append(f"{target_info['user']}@{hostname}")
    else:
        argv.append(hostname)
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
    remote_home: str | None = None,
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
            # A keyscan fallback must never authenticate merely because a
            # fingerprint was supplied: without keyscan output there is no
            # fingerprint to verify. Only explicit TOFU consent authorizes the
            # accept-new authenticated probe.
            allow_fallback_auth=bool(accept_host_key),
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
        remote_home_expr = shlex.quote(remote_home) if remote_home else '${HOME:-}/.vanth'
        wrapper_script = ssh.remote_wrapper_setup_script(
            url_expr, helper_command=helper, remote_id=remote_id,
            remote_home_expr=remote_home_expr,
        )
        transport.install_authorized_key(
            install_script=wrapper_script,
            bootstrap_config=bootstrap_config,
            config_dir=remote_dir,
            target_argv=argv,
        )
        wrapper_name = ssh._wrapper_filename(remote_id)
        if remote_home:
            # Literal remote home: quote for the shell so paths containing
            # spaces survive inside the forced command (Sol review).
            wrapper_command = shlex.quote(f"{remote_home.rstrip('/')}/{wrapper_name}")
        else:
            wrapper_command = f"$HOME/.vanth/{wrapper_name}"
        installed_line = ssh.authorized_keys_line(
            public_key_blob, "", marker_comment=marker, forced_command=wrapper_command
        )
        transport.install_authorized_key(
            install_script=ssh.authorized_keys_install_script(
                installed_line, marker,
                key_blob=public_key_blob,
                wrapper_command=wrapper_command,
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
        hello = transport.sentinel_probe(config=dedicated_config, config_dir=remote_dir, target_argv=argv)
        if not isinstance(hello, dict):
            raise ssh.VanthRemoteError("sentinel handshake failed: helper returned no identity")
        instance_id = hello.get("instance_id")
        if not isinstance(instance_id, str) or not instance_id.strip():
            raise ssh.VanthRemoteError("sentinel handshake failed: missing authenticated instance_id")
        hello_remote_id = hello.get("remote_id")
        if hello_remote_id != remote_id:
            raise ssh.VanthRemoteError(
                "sentinel handshake failed: authenticated remote_id does not match paired remote"
            )
        state_epoch = hello.get("state_epoch")
        if isinstance(state_epoch, bool) or not isinstance(state_epoch, int) or state_epoch < 1:
            raise ssh.VanthRemoteError("sentinel handshake failed: missing authenticated state_epoch")
        setter = getattr(store, "set_instance_id", None)
        if callable(setter):
            setter(remote_id, instance_id)
        else:
            columns = {row[1] for row in store.db.execute("PRAGMA table_info(remotes)").fetchall()}
            if "instance_id" not in columns:
                store.db.execute("ALTER TABLE remotes ADD COLUMN instance_id TEXT")
            store.db.execute(
                "UPDATE remotes SET instance_id=?, state_epoch=?, updated_at=? WHERE remote_id=?",
                (instance_id, state_epoch, now_iso(), remote_id),
            )
            store.db.commit()
        store.set_state_epoch(remote_id, state_epoch)

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
            "instance_id": instance_id,
            "state_epoch": state_epoch,
        }
    except ssh.VanthRemoteError:
        _compensate(store, transport, remote_id, remote_dir, marker, installed_line, bootstrap_config, argv, remote_home=remote_home)
        raise
    except Exception as exc:
        _compensate(store, transport, remote_id, remote_dir, marker, installed_line, bootstrap_config, argv, remote_home=remote_home)
        raise ssh.VanthRemoteError(f"pairing failed: {exc}") from exc


def _compensate(store: RemoteStore, transport: Any, remote_id: str, remote_dir: Path,
                marker: str, installed_line: str | None,
                bootstrap_config: str | None, argv: list[str] | None,
                *, remote_home: str | None = None) -> None:
    """Mark error + remove ONLY our exact authorization + local cleanup."""
    try:
        store.update_remote_state(remote_id, "error")
    except Exception:
        pass
    if installed_line and bootstrap_config and argv:
        try:
            transport.remove_authorized_key(
                remove_script=ssh.remote_wrapper_remove_script(
                    remote_home_expr=shlex.quote(remote_home) if remote_home else '${HOME:-}/.vanth'
                ),
                bootstrap_config=bootstrap_config,
                config_dir=remote_dir,
                target_argv=argv,
            )
        except Exception:
            pass
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
            forced = (ssh.parse_authorized_keys(installed).get("command") or "").strip('"')
            # The forced command may be single-quote-wrapped (spaces) — strip
            # that too before extracting the wrapper home (Sol review).
            if len(forced) >= 2 and forced.startswith("'") and forced.endswith("'"):
                forced = forced[1:-1]
            suffix = "/remote-wrapper.sh"
            per_remote_suffix = None
            if remote_id:
                per_remote_suffix = "/" + ssh._wrapper_filename(remote_id)
            if per_remote_suffix and forced.endswith(per_remote_suffix) and not forced.startswith("$"):
                wrapper_home = forced[:-len(per_remote_suffix)]
            elif forced.endswith(suffix) and not forced.startswith("$"):
                wrapper_home = forced[:-len(suffix)]  # legacy shared name
            else:
                wrapper_home = None
            transport.remove_authorized_key(
                remove_script=ssh.remote_wrapper_remove_script(
                    remote_home_expr=shlex.quote(wrapper_home) if wrapper_home else '${HOME:-}/.vanth',
                    remote_id=remote_id,
                ),
                bootstrap_config=bootstrap,
                config_dir=remote_dir,
                target_argv=_target_argv(target_info),
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
