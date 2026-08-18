"""Human-facing CLI for the Vanth daemon.

Unlike the MCP tools (which are JSON request/response over stdio), these
commands are meant for a person at a terminal: ``vanth status``, ``vanth
doctor``, ``vanth restart``. They read the same daemon discovery metadata and
speak the same authenticated loopback HTTP, but print readable output and exit
with a meaningful status code (0 = healthy, 1 = problem, 2 = usage).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from .client import VanthClient
from .paths import canonical_home


def _discovery(home: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads((home / "daemon.json").read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except (OSError, ValueError):
        return None


def _daemon_version() -> str:
    """Best-effort version of the installed package (not the daemon process)."""
    try:
        from . import __version__

        return __version__
    except Exception:
        return "unknown"


def _health(url: str, token: str) -> dict[str, Any] | None:
    """Return the /health payload, or None if the daemon is unreachable."""
    try:
        request = urllib.request.Request(
            url + "/health", headers={"Authorization": f"Bearer {token}"}
        )
        with urllib.request.urlopen(request, timeout=2) as response:
            return json.loads(response.read().decode())
    except Exception:
        return None


def _pid_alive(pid: int) -> bool:
    """Return whether a process with the given PID is running."""
    if not pid:
        return False
    if sys.platform == "win32":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=2,
            )
            return str(pid) in result.stdout
        except Exception:
            return True  # assume alive on probe failure
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def cmd_status(home: Path, *, json_out: bool = False) -> int:
    disc = _discovery(home)
    url = disc["url"] if disc else None
    token_path = home / "token"
    token = token_path.read_text(encoding="utf-8").strip() if token_path.exists() else ""

    up = False
    health = None
    doctor = None
    if url and token:
        health = _health(url, token)
        if health is not None:
            up = True
            try:
                client = VanthClient(url, home)
                doctor = client.get("/doctor")
            except Exception:
                doctor = None

    if json_out:
        payload = {
            "up": up,
            "url": url,
            "pid": disc.get("pid") if disc else None,
            "daemon_schema_version": disc.get("schema_version") if disc else None,
            "started_at": disc.get("started_at") if disc else None,
            "package_version": _daemon_version(),
            "health": health,
            "doctor": doctor,
        }
        print(json.dumps(payload, indent=2, default=str))
        return 0 if up else 1

    running = []
    if up:
        try:
            client = VanthClient(url, home)
            running = client.get("/jobs", {"status": ["running"]}).get("jobs", [])
        except Exception:
            running = []

    print(f"vanth daemon: {'UP' if up else 'DOWN'}")
    print(f"  home:     {home}")
    print(f"  url:      {url or '(no daemon.json - never started)'}")
    if disc:
        print(f"  pid:      {disc.get('pid')}")
        print(f"  schema:   {disc.get('schema_version')}")
        print(f"  started:  {disc.get('started_at')}")
    print(f"  package:  {_daemon_version()}")
    if doctor:
        print(f"  running jobs: {len(running)}")
        for job in running[:10]:
            print(f"    - {job.get('job_id')}  {job.get('name') or ''}  ({job.get('status')})")
        print(f"  schema (db):  {doctor.get('schema_version')}")
        counts = doctor.get("delivery_counts") or {}
        if counts:
            print(f"  deliveries:   {counts}")
        if doctor.get("warnings"):
            print("  warnings:")
            for warning in doctor["warnings"]:
                print(f"    - {warning.get('type')}: {warning}")
    elif up:
        print("  doctor:   unreachable (auth/schema problem)")
    return 0 if up else 1


def cmd_doctor(home: Path, *, json_out: bool = False) -> int:
    client = VanthClient(home=home)
    try:
        client.ensure()
        report = client.get("/doctor")
    except Exception as exc:
        print(f"vanth doctor: failed to reach daemon: {exc}")
        return 1
    if json_out:
        print(json.dumps(report, indent=2, default=str))
    else:
        ok = report.get("ok")
        print(f"vanth doctor: {'OK' if ok else 'PROBLEM'}")
        print(f"  home:          {report.get('home')}")
        print(f"  schema:        {report.get('schema_version')}")
        print(f"  tables:        {len(report.get('tables', []))}")
        print(f"  deliveries:    {report.get('delivery_counts')}")
        print(f"  codex:         {'available' if report.get('codex', {}).get('available') else 'MISSING'}")
        print(f"  opencode:      {'available' if report.get('opencode', {}).get('available') else 'MISSING'}")
        print(f"  quick_check:   {report.get('quick_check')}")
        print(f"  disk_free:     {_fmt_bytes(report.get('disk_free_bytes', 0))}")
        for warning in report.get("warnings", []):
            print(f"  WARNING:       {warning}")
    return 0 if report.get("ok") else 1


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TiB"


def cmd_restart(home: Path, *, json_out: bool = False) -> int:
    """Gracefully stop the daemon (if running) and start it again fresh.

    In-flight jobs are owned by detached runner processes, so they survive the
    daemon restart; the new daemon reconciles them on startup. This is the
    reliable way to pick up a code/version update.
    """
    disc = _discovery(home)
    url = disc["url"] if disc else None
    token_path = home / "token"
    token = token_path.read_text(encoding="utf-8").strip() if token_path.exists() else ""
    was_up = bool(url and token and _health(url, token) is not None)
    old_pid = disc.get("pid") if disc else None

    if was_up:
        # Ask the daemon to shut down gracefully.
        try:
            request = urllib.request.Request(
                url + "/shutdown",
                data=b"{}",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            print(f"vanth restart: warning: shutdown returned HTTP {exc.code}: {exc.read()!r}", file=sys.stderr)
        except Exception as exc:
            print(f"vanth restart: warning: shutdown request failed: {exc}", file=sys.stderr)
        # Wait until the old daemon is truly gone: port closed, discovery
        # metadata removed, and the old process (if known) has exited so the
        # home lock is released before the new daemon starts.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            health_down = _health(url, token) is None
            metadata_gone = not (home / "daemon.json").exists()
            pid_gone = True
            if old_pid and sys.platform == "win32":
                pid_gone = not _pid_alive(old_pid)
            elif old_pid:
                pid_gone = not _pid_alive(old_pid)
            if health_down and metadata_gone and pid_gone:
                break
            time.sleep(0.2)

    # Start fresh and verify a new process is actually serving.
    client = VanthClient(home=home)
    try:
        # The old process may still be releasing its home lock; retry briefly.
        last_error: Exception | None = None
        doctor = None
        for _ in range(20):
            try:
                client.ensure()
                doctor = client.get("/doctor")
                break
            except Exception as exc:  # noqa: BLE001 - retry transient lock races
                last_error = exc
                time.sleep(0.25)
        if doctor is None:
            raise last_error or RuntimeError("vanthd did not start")
    except Exception as exc:
        if json_out:
            print(json.dumps({"ok": False, "error": str(exc)}))
        else:
            print(f"vanth restart: failed to start daemon: {exc}")
        return 1
    if json_out:
        print(json.dumps({"ok": True, "restarted_from_running": was_up, "schema_version": doctor.get("schema_version")}))
    else:
        print(f"vanth restart: daemon {'restarted' if was_up else 'started'} (schema v{doctor.get('schema_version')})")
    return 0


def cmd_setup(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    from .setup import run_setup

    if "-h" in argv or "--help" in argv:
        print(
            "usage: vanth setup [opencode] [codex] [claude] [--remove] [--yes]\n"
            "\n"
            "Register (or remove) the Vanth MCP server in the given clients' configs.\n"
            "With no client names, detects and configures every known client found.\n"
            "\n"
            "options:\n"
            "  --remove, -r   remove the Vanth MCP entry instead of adding it\n"
            "  --yes, -y      do not prompt; apply immediately\n"
            "  --json         machine-readable output\n"
        )
        return 0
    remove = "--remove" in argv or "-r" in argv
    assume_yes = "--yes" in argv or "-y" in argv
    clients = [arg for arg in argv if arg in {"opencode", "codex", "claude"}]
    return run_setup(clients or None, home=home, remove=remove, assume_yes=assume_yes, json_out=json_out)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    home = canonical_home()
    json_out = "--json" in argv
    argv = [arg for arg in argv if arg != "--json"]
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    command = argv[0]
    if command == "status":
        return cmd_status(home, json_out=json_out)
    if command == "doctor":
        return cmd_doctor(home, json_out=json_out)
    if command == "restart":
        return cmd_restart(home, json_out=json_out)
    if command == "setup":
        return cmd_setup(argv[1:], home, json_out=json_out)
    print(f"vanth: unknown command {command!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
