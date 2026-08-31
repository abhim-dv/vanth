"""One-shot MCP client registration for Vanth.

`vanth setup` connects the MCP server to the agent clients installed on this
machine. It detects known clients, shows what it found, lets the user pick
which to configure (interactively, or via flags for scripting), backs up any
config it touches, and upserts the Vanth MCP entry without clobbering the rest
of the file.

It writes the exact format each client expects:

- opencode: ``~/.config/opencode/opencode.json`` -> ``mcp.vanth``
- codex:    ``~/.codex/config.toml`` -> ``[mcp_servers.vanth]``
- generic ``mcpServers`` JSON clients (Claude Code, Cursor, ...) ->
  ``mcpServers.vanth``

Only the user's own config files are modified; nothing is installed or run.
A timestamped ``.vanth-setup-<ts>.bak`` backup is written before any change.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

from .paths import canonical_home

SETUP_KEY = "vanth"
BACKUP_SUFFIX = ".vanth-setup"

# client id -> (display name, path resolver)
_CLIENTS: dict[str, tuple[str, str]] = {
    "opencode": ("opencode", "~/.config/opencode/opencode.json"),
    "codex": ("codex", "~/.codex/config.toml"),
    "claude": ("Claude Code / Cursor (mcpServers)", "~/.claude.json"),
}


def client_config_paths(home: Path | None = None) -> dict[str, list[Path]]:
    """Return the config file paths for each known client that exists."""
    home = home or canonical_home()
    found: dict[str, list[Path]] = {}
    # opencode: also check the app data location on Windows.
    candidates: dict[str, list[str]] = {
        "opencode": [
            "~/.config/opencode/opencode.json",
            "~/.opencode.json",
        ],
        "codex": ["~/.codex/config.toml"],
        "claude": ["~/.claude.json"],
    }
    for client, paths in candidates.items():
        for raw in paths:
            path = Path(raw).expanduser()
            if path.is_file():
                found.setdefault(client, []).append(path)
    return found


def _backup(path: Path) -> Path:
    stamp = time.strftime("%Y%m%dT%H%M%S")
    backup = path.with_name(path.name + f"{BACKUP_SUFFIX}-{stamp}.bak")
    shutil.copy2(path, backup)
    return backup


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _merge_json(path: Path, merge_fn) -> tuple[bool, str]:
    """Merge into a JSON file, preserving everything except the key set by
    ``merge_fn``. Returns (changed, summary)."""
    original = path.read_text(encoding="utf-8")
    try:
        data = json.loads(original)
    except json.JSONDecodeError:
        return False, "invalid JSON"
    before = json.dumps(data, sort_keys=True)
    data = merge_fn(data)
    after = json.dumps(data, sort_keys=True)
    if after == before:
        return False, "already configured"
    _backup(path)
    _write_atomic(path, json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    return True, "updated"


def _mcp_vanth_entry(home: Path) -> dict[str, Any]:
    """The MCP entry used for mcpServers-style clients."""
    return {
        "command": "vanth",
        "env": {"VANTH_HOME": str(home)},
    }


def register_opencode(path: Path, home: Path) -> tuple[bool, str]:
    def merge(data: dict[str, Any]) -> dict[str, Any]:
        mcp = data.setdefault("mcp", {})
        mcp[SETUP_KEY] = {
            "type": "local",
            "command": ["vanth"],
            "enabled": True,
            "timeout": 15000,
            # Review P0-3: without VANTH_HOME, a custom-state installation could
            # reach a different daemon (the default home). Always pin the
            # configured home so MCP talks to THIS daemon.
            "environment": {"VANTH_HOME": str(home)},
        }
        return data

    return _merge_json(path, merge)


def register_mcp_servers(path: Path, home: Path) -> tuple[bool, str]:
    def merge(data: dict[str, Any]) -> dict[str, Any]:
        servers = data.setdefault("mcpServers", {})
        servers[SETUP_KEY] = _mcp_vanth_entry(home)
        return data

    return _merge_json(path, merge)


def register_codex(path: Path, home: Path) -> tuple[bool, str]:
    """Register under ``[mcp_servers.vanth]`` in a Codex config.toml.

    Uses tomllib to inspect, and a line-oriented rewrite to add/replace only
    the ``[mcp_servers.vanth]`` section without touching any other content,
    comments, or ordering.
    """
    try:
        import tomllib
    except ImportError:  # pragma: no cover - 3.11+ always has it
        return False, "tomllib unavailable"
    original = path.read_text(encoding="utf-8")
    try:
        data = tomllib.loads(original)
    except tomllib.TOMLDecodeError:
        return False, "invalid TOML"

    section = f"[mcp_servers.{SETUP_KEY}]"
    escaped_home = str(home).replace("\\", "\\\\")
    new_lines = [
        section,
        f'command = "{SETUP_KEY}"',
        "",
        f"[mcp_servers.{SETUP_KEY}.env]",
        f'VANTH_HOME = "{escaped_home}"',
        "",
    ]

    lines = original.splitlines()
    out: list[str] = []
    replaced = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.rstrip() == section:
            # Skip the existing section and any sub-sections of it
            # (e.g. [mcp_servers.vanth.env]) up to the next unrelated header.
            j = i + 1
            while j < len(lines):
                candidate = lines[j].lstrip()
                if candidate.startswith("["):
                    header = candidate[1:].rstrip().split("]")[0].strip()
                    if header.startswith(f"mcp_servers.{SETUP_KEY}."):
                        j += 1
                        continue
                    break
                j += 1
            out.extend(new_lines)
            replaced = True
            i = j
            continue
        out.append(line)
        i += 1

    if not replaced:
        # Append the section at the end (with a blank line separator if the
        # file doesn't already end with one).
        if out and out[-1] != "":
            out.append("")
        out.extend(new_lines)

    new_text = "\n".join(out).rstrip() + "\n"
    if new_text == original.rstrip("\n") + "\n":
        return False, "already configured"
    _backup(path)
    _write_atomic(path, new_text)
    return True, "updated"


def remove_opencode(path: Path) -> tuple[bool, str]:
    def merge(data: dict[str, Any]) -> dict[str, Any]:
        mcp = data.get("mcp")
        if mcp and SETUP_KEY in mcp:
            del mcp[SETUP_KEY]
        return data

    return _merge_json(path, merge)


def remove_mcp_servers(path: Path) -> tuple[bool, str]:
    def merge(data: dict[str, Any]) -> dict[str, Any]:
        servers = data.get("mcpServers")
        if servers and SETUP_KEY in servers:
            del servers[SETUP_KEY]
        return data

    return _merge_json(path, merge)


def remove_codex(path: Path) -> tuple[bool, str]:
    section = f"[mcp_servers.{SETUP_KEY}]"
    lines = path.read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    i = 0
    removed = False
    while i < len(lines):
        line = lines[i]
        if line.rstrip() == section:
            j = i + 1
            while j < len(lines):
                candidate = lines[j].lstrip()
                if candidate.startswith("["):
                    header = candidate[1:].rstrip().split("]")[0].strip()
                    if header.startswith(f"mcp_servers.{SETUP_KEY}."):
                        j += 1
                        continue
                    break
                j += 1
            i = j
            removed = True
            continue
        out.append(line)
        i += 1
    if not removed:
        return False, "not configured"
    _backup(path)
    _write_atomic(path, "\n".join(out).rstrip() + "\n")
    return True, "removed"


def detect_status(home: Path | None = None) -> dict[str, list[dict[str, Any]]]:
    """Return per-client status: present config files and whether vanth is
    already registered in each."""
    home = home or canonical_home()
    found = client_config_paths(home)
    result: dict[str, list[dict[str, Any]]] = {}
    for client, paths in found.items():
        entries = []
        for path in paths:
            configured = _is_configured(client, path)
            entries.append({"path": str(path), "configured": configured})
        result[client] = entries
    return result


def _is_configured(client: str, path: Path) -> bool:
    try:
        if client in {"opencode", "claude"}:
            data = json.loads(path.read_text(encoding="utf-8"))
            if client == "opencode":
                return bool((data.get("mcp") or {}).get(SETUP_KEY))
            return bool((data.get("mcpServers") or {}).get(SETUP_KEY))
        if client == "codex":
            import tomllib

            data = tomllib.loads(path.read_text(encoding="utf-8"))
            return bool((data.get("mcp_servers") or {}).get(SETUP_KEY))
    except (OSError, ValueError):
        return False
    return False


_REGISTRARS = {
    "opencode": register_opencode,
    "codex": register_codex,
    "claude": register_mcp_servers,
}
_REMOVERS = {
    "opencode": remove_opencode,
    "codex": remove_codex,
    "claude": remove_mcp_servers,
}


def run_setup(
    clients: list[str] | None = None,
    *,
    home: Path | None = None,
    remove: bool = False,
    assume_yes: bool = False,
    json_out: bool = False,
) -> int:
    """Configure (or remove) the Vanth MCP entry for the given clients.

    With no clients, detect and configure everything found. Returns 0 on
    success, 1 if any registration failed, 2 on usage errors.
    """
    home = home or canonical_home()
    found = client_config_paths(home)

    requested = list(clients or found.keys())
    if clients:
        unknown = set(clients) - {"opencode", "codex", "claude"}
        if unknown:
            if json_out:
                print(json.dumps({"ok": False, "error": f"unknown client(s): {', '.join(sorted(unknown))}"}))
            else:
                print(f"vanth setup: unknown client(s): {', '.join(sorted(unknown))}", file=sys.stderr)
                print("  known clients: opencode, codex, claude", file=sys.stderr)
            return 2

    verb = "remove" if remove else "register"
    targets: list[tuple[str, Path]] = []
    for client in requested:
        for path in found.get(client, []):
            targets.append((client, path))

    if not targets:
        if json_out:
            print(json.dumps({"ok": False, "error": "no known client configs found"}))
        else:
            print("vanth setup: no known client configs found.", file=sys.stderr)
            print("  searched: ~/.config/opencode/opencode.json, ~/.codex/config.toml, ~/.claude.json", file=sys.stderr)
            print("  pass an explicit client, e.g. `vanth setup opencode`", file=sys.stderr)
        return 1

    if not json_out:
        print(f"vanth setup: {verb} MCP server in {len(targets)} config file(s):")
        for client, path in targets:
            already = _is_configured(client, path)
            state = "already configured" if already else "not configured"
            print(f"  - {client}: {path} ({state})")

    if not assume_yes:
        if not sys.stdin.isatty():
            print("vanth setup: no interactive terminal; pass --yes to apply", file=sys.stderr)
            return 1
        answer = input(f"Continue with {verb}? [y/N] ").strip().lower()
        if answer not in {"y", "yes"}:
            print("aborted")
            return 1

    failures = 0
    results: list[dict[str, Any]] = []
    for client, path in targets:
        fn = _REMOVERS[client] if remove else _REGISTRARS[client]
        try:
            changed, summary = fn(path, home) if not remove else fn(path)
        except Exception as exc:
            results.append({"client": client, "path": str(path), "changed": False, "ok": False, "error": str(exc)})
            if not json_out:
                print(f"  FAILED {client}: {exc}")
            failures += 1
            continue
        ok = changed or summary in {"already configured"}
        results.append({"client": client, "path": str(path), "changed": changed, "ok": ok, "summary": summary})
        if not json_out:
            action = "configured" if changed else "skipped"
            print(f"  {client}: {action} ({summary})")
        if not ok:
            failures += 1
    if failures:
        if json_out:
            print(json.dumps({"ok": False, "error": f"{failures} file(s) failed", "results": results}, indent=2))
        else:
            print(f"vanth setup: {failures} file(s) failed", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps({"ok": True, "verb": verb, "results": results}, indent=2))
    else:
        print("vanth setup: done")
    return 0
