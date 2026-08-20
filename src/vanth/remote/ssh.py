"""Secure SSH primitives for remote execution (Phase 1).

This module is the *only* place Vanth talks to the OpenSSH client. Its job is
to guarantee one invariant from the plan: **a user SSH config containing other
identities, remote commands, forwarding, known-host commands, or control
sockets cannot affect final-target authentication.**

Every actual ``ssh`` invocation runs with ``-F <generated allowlist config>``,
never with ``-o`` overrides stacked on top of the user's real config. The
generated config is built from an explicit allowlist (identities, host keys,
no forwarding, no agent, no TTY, no control master), so anything in
``~/.ssh/config`` is simply never read.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path
from typing import Any

# On Windows the OpenSSH client ships under System32; on POSIX it is on PATH.
_SSH_WINDOWS_DIR = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32" / "OpenSSH"

# Keys that the generated allowlist config explicitly pins. Anything a user
# config could have set (aliases, jump hosts, ProxyCommand, control sockets,
# other identities, forwarding) is deliberately absent.
_ALLOWLIST_HOST_KEYS = (
    "HostName",
    "User",
    "Port",
    "IdentityFile",
    "IdentitiesOnly",
    "UserKnownHostsFile",
    "StrictHostKeyChecking",
    "BatchMode",
    "ConnectTimeout",
    "LogLevel",
    "ProxyCommand",
    "ForwardAgent",
    "ForwardX11",
    "RequestTTY",
    "ControlMaster",
    "ControlPath",
)


class VanthRemoteError(RuntimeError):
    """Base error for SSH/pairing failures, surfaced to CLI/daemon."""


def _find_bin(name: str) -> str | None:
    if os.name == "nt":
        candidate = _SSH_WINDOWS_DIR / (name + ".exe")
        if candidate.exists():
            return str(candidate)
    found = shutil.which(name)
    return found


def detect_binaries() -> dict[str, str | None]:
    """Locate ssh / ssh-keygen / scp. Missing entries are None (not an error)."""
    return {
        "ssh": _find_bin("ssh"),
        "ssh-keygen": _find_bin("ssh-keygen"),
        "scp": _find_bin("scp"),
    }


def require_binaries() -> dict[str, str]:
    """Like ``detect_binaries`` but raises if any required binary is missing."""
    found = detect_binaries()
    missing = [name for name, path in found.items() if not path]
    if missing:
        raise VanthRemoteError(
            "OpenSSH binaries not found: " + ", ".join(missing)
            + " (install OpenSSH client; on Windows it ships under "
            + str(_SSH_WINDOWS_DIR) + ")"
        )
    return {name: str(path) for name, path in found.items()}


def allowlist_config(
    *,
    hostname: str,
    user: str | None,
    port: int | None,
    identity_file: str,
    known_hosts: str,
    timeout: float = 15.0,
) -> str:
    """Build a dedicated OpenSSH config from an explicit allowlist.

    The returned text is written to a generated file and passed via ``-F``.
    Only the allowlisted directives appear; ambient user config can never
    leak identities, forwarding, control sockets, ProxyCommand, or remote
    commands into the connection.
    """
    lines = [
        "Host vanth-remote",
        f"    HostName {hostname}",
    ]
    if user:
        lines.append(f"    User {user}")
    if port:
        lines.append(f"    Port {port}")
    lines += [
        f"    IdentityFile {identity_file}",
        "    IdentitiesOnly yes",
        f"    UserKnownHostsFile {known_hosts}",
        "    StrictHostKeyChecking yes",
        "    BatchMode yes",
        f"    ConnectTimeout {int(max(1, timeout))}",
        "    LogLevel ERROR",
        "    ProxyCommand none",
        "    ForwardAgent no",
        "    ForwardX11 no",
        "    RequestTTY no",
        "    ControlMaster no",
    ]
    return "\n".join(lines) + "\n"


def generate_identity(directory: Path, *, comment: str | None = None) -> dict[str, str]:
    """Generate one Ed25519 keypair with no passphrase.

    Returns ``{"private_key_path", "public_key_path", "fingerprint"}``. The
    private key is restricted to the owner (chmod 0600 / icacls on Windows).
    """
    require_binaries()
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    private_path = directory / "id_ed25519"
    public_path = directory / "id_ed25519.pub"
    comment = comment or "vanth-remote"
    result = subprocess.run(
        ["ssh-keygen", "-t", "ed25519", "-N", "", "-f", str(private_path), "-C", comment],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise VanthRemoteError(f"ssh-keygen failed: {result.stderr.strip()}")
    _restrict_owner(private_path)
    finger = subprocess.run(
        ["ssh-keygen", "-lf", str(public_path)],
        capture_output=True,
        text=True,
        timeout=15,
    )
    fingerprint = ""
    if finger.returncode == 0:
        fields = finger.stdout.strip().split()
        if len(fields) >= 2:
            fingerprint = fields[1]  # e.g. SHA256:<hex>
    return {
        "private_key_path": str(private_path),
        "public_key_path": str(public_path),
        "fingerprint": fingerprint,
    }


def _restrict_owner(path: Path) -> None:
    if os.name == "nt":
        try:
            subprocess.run(
                ["icacls", str(path), "/inheritance:r", "/grant:r", f"{_current_user_sid()}:F"],
                capture_output=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            pass
    else:
        try:
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            pass


def _current_user_sid() -> str:
    try:
        result = subprocess.run(["whoami", "/user"], capture_output=True, text=True, timeout=10, check=True)
        for line in result.stdout.splitlines():
            if "S-1-" in line:
                return line.strip().split()[-1]
    except (OSError, subprocess.SubprocessError):
        pass
    return os.environ.get("USERNAME", "SYSTEM")


# ---------------------------------------------------------------------------
# authorized_keys handling
# ---------------------------------------------------------------------------

_AUTH_KEY_OPTIONS = (
    "no-pty,no-agent-forwarding,no-port-forwarding,no-X11-forwarding,no-user-rc"
)


def authorized_keys_line(
    public_key: str,
    helper_command: str,
    *,
    extra_options: str | None = None,
) -> str:
    """Build a forced-command, restricted authorized_keys line.

    ``helper_command`` must be an absolute path to the remote helper
    (``vanth-remote-helper``) on the remote host. The forced command prevents
    the key from opening a shell or running arbitrary commands.
    """
    public_key = public_key.strip()
    options = _AUTH_KEY_OPTIONS
    if extra_options:
        options = f"command=\"{helper_command}\",{extra_options}"
    else:
        options = f"command=\"{helper_command}\",{options}"
    return f"{options} {public_key}\n"


def parse_authorized_keys(line: str) -> dict[str, Any]:
    """Parse one authorized_keys line into ``{"key","options","command","comment"}``.

    The key is the key-type + base64 blob (two whitespace-separated tokens);
    ``key`` returns them joined so callers can compare against a ``ssh-ed25519
    AAAA...`` string directly.
    """
    line = line.strip()
    if not line:
        return {"key": "", "options": {}, "command": None, "comment": ""}
    parts = line.split()
    options: dict[str, str] = {}
    index = 0
    while index < len(parts) and "=" in parts[index] and not parts[index].startswith("ssh-"):
        for option in parts[index].split(","):
            if "=" in option:
                name, _, value = option.partition("=")
                options[name] = value
            else:
                options[option] = ""
        index += 1
    remaining = parts[index:]
    key = ""
    comment = ""
    if len(remaining) >= 2:
        key = remaining[0] + " " + remaining[1]
        comment = " ".join(remaining[2:])
    elif remaining:
        key = remaining[0]
    return {
        "key": key,
        "options": options,
        "command": options.get("command"),
        "comment": comment,
    }


def is_unrestricted(line: str, public_key: str) -> bool:
    """True if ``line`` authorizes ``public_key`` without a forced command.

    A key line with no ``command=`` option is unrestricted and must never be
    created or left behind by Vanth. This is used to detect an existing
    unrestricted authorization using the same key and refuse to proceed.
    """
    parsed = parse_authorized_keys(line)
    if parsed["key"] != public_key.strip():
        return False
    return parsed["command"] is None


# ---------------------------------------------------------------------------
# ssh -G resolution and invocation
# ---------------------------------------------------------------------------


def _write_config(contents: str, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "ssh_config"
    path.write_text(contents, encoding="utf-8")
    _restrict_owner(path)
    return path


def resolve_target(target: str, *, config_dir: Path, known_hosts: str, identity_file: str) -> dict[str, Any]:
    """Resolve a ``user@host[:port]`` target using ``ssh -G`` against a generated
    allowlist config, so ambient user config cannot affect the resolution.

    Returns a dict of the resolved directives that matter (hostname, user,
    port, and the allowlist keys present in the output).
    """
    require_binaries()
    hostname = target.rsplit("@", 1)[-1]
    user = None
    if "@" in target:
        user = target.rsplit("@", 1)[0]
    port = None
    if hostname.startswith("["):
        # IPv6 literal: [addr]:port
        host, _, rest = hostname.partition("]")
        hostname = host.lstrip("[")
        if rest.startswith(":"):
            try:
                port = int(rest[1:])
            except ValueError:
                raise VanthRemoteError(f"invalid port in target: {target!r}") from None
    elif ":" in hostname:
        host, _, rest = hostname.rpartition(":")
        if rest.isdigit():
            hostname = host
            port = int(rest)
    config = allowlist_config(
        hostname=hostname,
        user=user,
        port=port,
        identity_file=identity_file,
        known_hosts=known_hosts,
    )
    config_path = _write_config(config, config_dir)
    result = subprocess.run(
        ["ssh", "-G", "-F", str(config_path), hostname],
        capture_output=True,
        text=True,
        timeout=20,
    )
    if result.returncode != 0:
        raise VanthRemoteError(f"ssh -G failed for {target!r}: {result.stderr.strip() or result.stdout.strip()}")
    resolved: dict[str, Any] = {"hostname": hostname, "user": user, "port": port}
    for line in result.stdout.splitlines():
        key, _, value = line.partition(" ")
        # ssh -G emits lowercase directive names; the allowlist uses Title case.
        title_key = key.title()
        if title_key in _ALLOWLIST_HOST_KEYS:
            if title_key == "HostName":
                resolved["hostname"] = value
            elif title_key == "User":
                resolved["user"] = value
            elif title_key == "Port":
                resolved["port"] = value
            else:
                resolved[title_key] = value
    if resolved.get("port") is not None:
        try:
            resolved["port"] = int(resolved["port"])
        except (TypeError, ValueError):
            resolved["port"] = None
    return resolved


def run_ssh(
    args: list[str],
    *,
    config_dir: Path,
    config: str,
    timeout: float = 30.0,
    stdin: bytes | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[bytes]:
    """Run ``ssh`` with a generated allowlist config; never the user's config."""
    require_binaries()
    config_path = _write_config(config, config_dir)
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    argv = ["ssh", "-F", str(config_path), *args]
    return subprocess.run(
        argv,
        capture_output=True,
        input=stdin,
        timeout=timeout,
        env=full_env,
    )


def ssh_host_keys(known_hosts_path: Path) -> list[str]:
    """Parse a known_hosts file into a list of host key entries (host + type)."""
    path = Path(known_hosts_path)
    if not path.exists():
        return []
    entries = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if len(fields) < 3:
            continue
        hosts, key_type = fields[0], fields[1]
        entries.append({"hosts": hosts, "type": key_type})
    return entries
