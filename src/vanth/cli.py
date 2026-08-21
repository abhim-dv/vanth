"""Human-facing CLI for the Vanth daemon.

Unlike the MCP tools (which are JSON request/response over stdio), these
commands are meant for a person at a terminal: ``vanth status``, ``vanth
doctor``, ``vanth restart``, ``vanth setup``. They read the same daemon
discovery metadata and speak the same authenticated loopback HTTP, but print
readable output and exit with a meaningful status code (0 = healthy, 1 =
problem, 2 = usage).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import autostart
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

    _print_setup_status()
    return 0 if up else 1


def _print_setup_status() -> None:
    """Show which MCP clients have the Vanth MCP server configured, so a user
    can see onboarding state at a glance and is pointed at `vanth setup`."""
    try:
        from .setup import client_config_paths, _is_configured

        found = client_config_paths()
        if not found:
            print("  mcp:      no known client configs found — run `vanth setup`")
            return
        parts = []
        for client in ("opencode", "codex", "claude"):
            paths = found.get(client) or []
            if not paths:
                continue
            configured = any(_is_configured(client, path) for path in paths)
            parts.append(f"{client}={('configured' if configured else 'not configured')}")
        if not parts:
            print("  mcp:      no known client configs found — run `vanth setup`")
            return
        state = "  mcp:      " + ", ".join(parts)
        print(state)
        if any("not configured" in part for part in parts):
            print("            run `vanth setup` to register")
    except Exception:
        pass


def cmd_doctor(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    client = VanthClient(home=home)
    reap = "--reap-orphans" in argv
    try:
        client.ensure()
        if reap:
            result = client.post("/reap-orphans", {})
            if json_out:
                print(json.dumps(result, indent=2, default=str))
            else:
                print(f"vanth doctor: reaped {len(result.get('reaped', []))} orphaned MCP server(s)")
                for entry in result.get("failed", []):
                    print(f"  failed:      pid {entry['pid']}: {entry['error']}")
            return 0
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
        orphaned = report.get("orphaned_mcp_servers") or []
        if orphaned:
            print(f"  orphaned_mcp: {len(orphaned)} (reap with `vanth doctor --reap-orphans`)")
            for entry in orphaned[:5]:
                print(f"    pid {entry['pid']} parent {entry['parent_pid']} started {entry['started']}")
        for warning in report.get("warnings", []):
            print(f"  WARNING:       {warning}")
        _print_setup_status()
    return 0 if report.get("ok") else 1


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TiB"


def _humanize(seconds: float) -> str:
    """Humanize a seconds count: "42s", "2m 3s", "1h", "3d 4h"."""
    if seconds is None:
        return ""
    seconds = int(max(0.0, float(seconds)))
    if seconds < 60:
        return f"{seconds}s"
    parts = []
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            count, seconds = divmod(seconds, size)
            parts.append(f"{count}{unit}")
        if len(parts) >= 2:
            break
    if parts and seconds:
        parts.append(f"{seconds}s")
    return " ".join(parts) if parts else f"{seconds}s"


def _parse_iso_ts(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _age_from(value: str | None) -> str:
    """Humanized age since an ISO timestamp (blank when unparsable)."""
    if not value:
        return ""
    parsed = _parse_iso_ts(value)
    if parsed is None:
        return ""
    delta = (datetime.now(timezone.utc) - parsed).total_seconds()
    return _humanize(max(0.0, delta))


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


def cmd_autostart(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """`vanth autostart enable|disable|status [--dry-run] [--yes] [--json]`."""
    if not argv or argv[0] in {"--help", "-h"}:
        print(
            "usage: vanth autostart enable|disable|status [--dry-run] [--yes] [--json]\n"
            "\n"
            "Register the Vanth daemon as a background service so it survives\n"
            "reboots (Windows Task Scheduler / macOS launchd / Linux systemd).\n"
            "\n"
            "actions:\n"
            "  status         show whether autostart is registered\n"
            "  enable         register the daemon to start automatically\n"
            "  disable        remove the autostart registration\n"
            "\n"
            "options:\n"
            "  --dry-run      show what would happen without changing anything\n"
            "  --yes, -y      do not prompt; apply immediately\n"
            "  --json         machine-readable output\n"
        )
        return 0
    action = argv[0]
    rest = argv[1:]
    dry_run = "--dry-run" in rest
    assume_yes = "--yes" in rest or "-y" in rest
    if action == "status":
        state = autostart.detect(home)
        if json_out:
            print(json.dumps(state, indent=2, default=str))
        else:
            enabled = state.get("enabled", False)
            print(f"vanth autostart: {'enabled' if enabled else 'disabled'}")
            print(f"  platform: {state.get('platform')}")
            print(f"  target:   {state.get('target')}")
            if state.get("error"):
                print(f"  error:    {state['error']}")
        return 0 if state.get("enabled") else 1
    if action == "enable":
        if not assume_yes and not dry_run:
            answer = input("Install Vanth autostart? [y/N] ").strip().lower()
            if answer not in {"y", "yes"}:
                print("vanth autostart: declined")
                return 0
        try:
            result = autostart.enable(home, dry_run=dry_run)
        except Exception as exc:
            print(f"vanth autostart: enable failed: {exc}", file=sys.stderr)
            return 1
        if json_out:
            print(json.dumps(result, indent=2, default=str))
        else:
            if result.get("dry_run"):
                print(f"vanth autostart: would install: {result.get('would_install')}")
            else:
                print(f"vanth autostart: enabled ({result.get('target')})")
        return 0
    if action == "disable":
        if not assume_yes and not dry_run:
            answer = input("Remove Vanth autostart? [y/N] ").strip().lower()
            if answer not in {"y", "yes"}:
                print("vanth autostart: declined")
                return 0
        try:
            result = autostart.disable(home, dry_run=dry_run)
        except Exception as exc:
            print(f"vanth autostart: disable failed: {exc}", file=sys.stderr)
            return 1
        if json_out:
            print(json.dumps(result, indent=2, default=str))
        else:
            if result.get("dry_run"):
                print(f"vanth autostart: would uninstall: {result.get('would_uninstall')}")
            else:
                print(f"vanth autostart: disabled ({result.get('target')})")
        return 0
    print(f"vanth: unknown autostart action {action!r}", file=sys.stderr)
    return 2


def cmd_version() -> int:
    print(_daemon_version())
    return 0


def _job_error(message: str) -> dict[str, Any]:
    return {"result": "error", "error": message}


def _expect_ok(payload: dict[str, Any]) -> bool:
    return bool(payload and payload.get("result") != "error")


def cmd_list(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    statuses: list[str] = []
    limit = 50
    include_all = False
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--status":
            i += 1
            if i >= len(argv):
                print("vanth list: --status requires a value", file=sys.stderr)
                return 2
            statuses.extend(item.strip() for item in argv[i].split(",") if item.strip())
        elif arg == "--limit":
            i += 1
            if i >= len(argv):
                print("vanth list: --limit requires a value", file=sys.stderr)
                return 2
            try:
                limit = int(argv[i])
            except ValueError:
                print(f"vanth list: invalid --limit value {argv[i]!r}", file=sys.stderr)
                return 2
        elif arg == "--all":
            include_all = True
        else:
            print(f"vanth list: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    client = VanthClient(home=home)
    try:
        client.ensure()
        params: dict[str, Any] = {"limit": limit}
        if statuses:
            params["status"] = statuses
        payload = client.get("/jobs", params)
    except Exception as exc:
        print(f"vanth list: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(payload):
        print(f"vanth list: daemon error: {payload.get('error') or payload}", file=sys.stderr)
        return 1
    jobs = payload.get("jobs") or []
    if not include_all:
        jobs = [job for job in jobs if job.get("status") == "running"]
    else:
        active = {"running", "queued", "pending", "retrying", "dispatching", "orphaned"}
        jobs = [job for job in jobs if job.get("status") not in active]
    if json_out:
        print(json.dumps(jobs, indent=2, default=str))
        return 0
    if not jobs:
        print("vanth list: no jobs")
        return 0
    jobs = sorted(
        jobs,
        key=lambda job: (job.get("status") != "running", job.get("created_at") or ""),
        reverse=True,
    )
    print(f"{'STATUS':<10} {'JOB ID':<24} {'NAME':<28} {'DURATION':<10} {'EXIT':<6} AGE")
    for job in jobs:
        runtime = job.get("runtime_seconds")
        if runtime is None:
            started_at = job.get("started_at")
            duration = _age_from(started_at) if started_at else ""
        else:
            duration = _humanize(runtime)
        exit_code = job.get("exit_code")
        exit_text = "" if exit_code is None else str(exit_code)
        age = _age_from(job.get("created_at") or job.get("updated_at"))
        print(
            f"{(job.get('status') or ''):<10} {(job.get('job_id') or ''):<24} "
            f"{(job.get('name') or ''):<28} {duration:<10} {exit_text:<6} {age}"
        )
    return 0


def cmd_logs(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    if not argv:
        print("vanth logs: missing job id", file=sys.stderr)
        return 2
    job_id = argv[0]
    stream = "stdout"
    max_bytes = 8192
    offset: int | None = None
    grep: str | None = None
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--stream":
            i += 1
            if i >= len(argv):
                print("vanth logs: --stream requires a value", file=sys.stderr)
                return 2
            value = argv[i]
            if value not in {"stdout", "stderr", "all"}:
                print("vanth logs: --stream must be stdout, stderr, or all", file=sys.stderr)
                return 2
            stream = value
        elif arg == "--max-bytes":
            i += 1
            if i >= len(argv):
                print("vanth logs: --max-bytes requires a value", file=sys.stderr)
                return 2
            try:
                max_bytes = int(argv[i])
            except ValueError:
                print(f"vanth logs: invalid --max-bytes value {argv[i]!r}", file=sys.stderr)
                return 2
        elif arg == "--offset":
            i += 1
            if i >= len(argv):
                print("vanth logs: --offset requires a value", file=sys.stderr)
                return 2
            try:
                offset = int(argv[i])
            except ValueError:
                print(f"vanth logs: invalid --offset value {argv[i]!r}", file=sys.stderr)
                return 2
        elif arg == "--grep":
            i += 1
            if i >= len(argv):
                print("vanth logs: --grep requires a value", file=sys.stderr)
                return 2
            grep = argv[i]
        else:
            print(f"vanth logs: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    client = VanthClient(home=home)
    try:
        client.ensure()
    except Exception as exc:
        print(f"vanth logs: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    streams = ["stdout", "stderr"] if stream == "all" else [stream]
    results: list[dict[str, Any]] = []
    for name in streams:
        params: dict[str, Any] = {"stream": name, "max_bytes": max_bytes}
        if offset is not None:
            params["offset"] = offset
        if grep is not None:
            params["grep"] = grep
        try:
            result = client.get(f"/jobs/{job_id}/tail", params)
        except Exception as exc:
            print(f"vanth logs: failed to reach daemon: {exc}", file=sys.stderr)
            return 1
        if not _expect_ok(result):
            error = result.get("error") or ""
            if "unknown" in error.lower() or "job_id" in error.lower() or "not found" in error.lower():
                print(f"vanth logs: unknown job {job_id}", file=sys.stderr)
                return 1
            print(f"vanth logs: daemon error: {error}", file=sys.stderr)
            return 1
        results.append(result)
    if json_out:
        print(json.dumps(results[0] if len(results) == 1 else results, indent=2, default=str))
        return 0
    for result in results:
        print(result.get("content") or "", end="")
    return 0


def cmd_diff(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    if len(argv) < 2:
        print("vanth diff: requires two job ids: <job> <other>", file=sys.stderr)
        return 2
    base_job_id, other_job_id = argv[0], argv[1]
    client = VanthClient(home=home)
    try:
        client.ensure()
        result = client.get(f"/jobs/{base_job_id}/diff", {"other": other_job_id})
    except Exception as exc:
        print(f"vanth diff: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        error = result.get("error") or ""
        if "unknown" in error.lower() or "job_id" in error.lower():
            print(f"vanth diff: unknown job (base={base_job_id} other={other_job_id})", file=sys.stderr)
            return 1
        print(f"vanth diff: daemon error: {error}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps(result, indent=2, default=str))
        return 0
    if result.get("identical"):
        print(f"vanth diff: {base_job_id} and {other_job_id} are identical")
        return 0
    print(f"vanth diff: {base_job_id} vs {other_job_id}")
    for change in result.get("changes") or []:
        field = change["field"]
        if field == "env":
            print(f"\n  env ({len(change.get('changes') or [])} keys changed):")
            for entry in change.get("changes") or []:
                print(f"    {entry['key']}: {entry.get('base')!r} -> {entry.get('other')!r}")
        else:
            print(f"  {field}: {change.get('base')!r} -> {change.get('other')!r}")
    return 0


def cmd_remote(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """`vanth remote <pair|list|doctor|remove>` — remote execution pairing."""
    if not argv or argv[0] in {"--help", "-h", "help"}:
        print("usage: vanth remote <pair|list|doctor|remove|pending|retry> [options]\n"
              "  pair <target>   pair a remote host (user@host[:port]) [--name N] [--allow-root]\n"
              "  list            list paired remotes\n"
              "  doctor          report SSH binaries and remote state [--remote <id>]\n"
              "  remove <id>     remove a remote [--yes]\n"
              "  pending         list unresolved client requests [--remote <id>]\n"
              "  retry <req_id>  re-run an unresolved request with its original key", file=sys.stderr)
        return 2 if not argv else 0
    sub = argv[0]
    client = VanthClient(home=home)
    if sub == "pending":
        from .remote.journal import RequestJournal

        remote_filter = None
        if "--remote" in argv:
            i = argv.index("--remote")
            if i + 1 < len(argv):
                remote_filter = argv[i + 1]
        journal = RequestJournal(home / "client-requests.sqlite")
        try:
            rows = journal.pending(remote_filter)
        finally:
            journal.close()
        if json_out:
            print(json.dumps({"pending": rows}, indent=2, default=str))
            return 0
        if not rows:
            print("vanth remote pending: no unresolved requests")
            return 0
        for row in rows:
            print(f"  {row['request_id']}  {row['remote_id']}  {row['method']}  key={row['idempotency_key']}  since={row['created_at']}")
        return 0
    if sub == "retry":
        from .remote.journal import RequestJournal

        targets = [a for a in argv[1:] if not a.startswith("--")]
        if not targets:
            print("vanth remote retry: missing request_id", file=sys.stderr)
            return 2
        request_id = targets[0]
        journal = RequestJournal(home / "client-requests.sqlite")
        try:
            entry = journal.get(request_id)
        finally:
            journal.close()
        if not entry:
            print(f"vanth remote retry: unknown request_id {request_id}", file=sys.stderr)
            return 1
        import sqlite3 as _sqlite3

        from .remote.control import RemoteControl
        from .remote.store import RemoteStore

        db = _sqlite3.connect(home / "remote.sqlite")
        db.row_factory = _sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        store = RemoteStore(db)
        control = RemoteControl(store)
        # Re-submit with the ORIGINAL method/payload/key: replay returns the
        # same durable request (never a second job), then run it.
        request = control.submit(
            entry["remote_id"], entry["method"], entry["payload"] or {},
            idempotency_key=entry["idempotency_key"],
        )
        result = control.run_request(entry["remote_id"], request)
        if json_out:
            print(json.dumps(result, indent=2, default=str))
        else:
            print(f"vanth remote retry: {request_id} status={result.get('status')}")
        return 0 if result.get("status") in ("completed", "failed") else 1
    if sub == "pair":
        args = argv[1:]
        allow_root = "--allow-root" in args
        name = None
        if "--name" in args:
            i = args.index("--name")
            if i + 1 < len(args):
                name = args[i + 1]
        targets = [a for a in args if not a.startswith("--")]
        if not targets:
            print("vanth remote pair: missing target (user@host[:port])", file=sys.stderr)
            return 2
        target = targets[0]
        try:
            client.ensure()
            result = client.post("/remotes/pair", {"target": target, "name": name, "allow_root": allow_root})
        except Exception as exc:
            print(f"vanth remote pair: failed to reach daemon: {exc}", file=sys.stderr)
            return 1
        if result.get("result") == "error" or "error" in result:
            print(f"vanth remote pair: {result.get('error', 'pairing failed')}", file=sys.stderr)
            return 1
        if json_out:
            print(json.dumps(result, indent=2, default=str))
            return 0
        print(f"vanth remote pair: paired {target} ({result.get('remote_id')})")
        return 0
    if sub == "list":
        try:
            client.ensure()
            result = client.get("/remotes")
        except Exception as exc:
            print(f"vanth remote list: failed to reach daemon: {exc}", file=sys.stderr)
            return 1
        remotes = result.get("remotes", result if isinstance(result, list) else [])
        if json_out:
            print(json.dumps(result, indent=2, default=str))
            return 0
        if not remotes:
            print("vanth remote list: no remotes paired")
            return 0
        for row in remotes:
            print(f"  {row.get('remote_id')}  {row.get('target')}  state={row.get('state')}")
        return 0
    if sub == "doctor":
        remote_id = None
        if "--remote" in argv:
            i = argv.index("--remote")
            if i + 1 < len(argv):
                remote_id = argv[i + 1]
        try:
            client.ensure()
            result = client.get("/remotes/doctor", {"remote_id": remote_id} if remote_id else None)
        except Exception as exc:
            print(f"vanth remote doctor: failed to reach daemon: {exc}", file=sys.stderr)
            return 1
        if json_out:
            print(json.dumps(result, indent=2, default=str))
            return 0
        bins = result.get("binaries") or {}
        missing = [name for name, path in bins.items() if not path]
        print("vanth remote doctor:")
        print(f"  ssh:        {bins.get('ssh') or 'MISSING'}")
        print(f"  ssh-keygen: {bins.get('ssh-keygen') or 'MISSING'}")
        print(f"  scp:        {bins.get('scp') or 'MISSING'}")
        if missing:
            print("  WARNING: OpenSSH binaries missing — remote execution unavailable")
        for row in result.get("remotes") or []:
            print(f"  remote:     {row.get('remote_id')} {row.get('target')} state={row.get('state')}")
        return 0 if not missing else 1
    if sub == "remove":
        args = argv[1:]
        targets = [a for a in args if not a.startswith("--")]
        if not targets:
            print("vanth remote remove: missing remote_id", file=sys.stderr)
            return 2
        remote_id = targets[0]
        assume_yes = "--yes" in args
        if not assume_yes:
            print(f"vanth remote remove: this removes local keys/config for {remote_id}; pass --yes to confirm", file=sys.stderr)
            return 1
        try:
            client.ensure()
            result = client.post("/remotes/remove", {"remote_id": remote_id})
        except Exception as exc:
            print(f"vanth remote remove: failed to reach daemon: {exc}", file=sys.stderr)
            return 1
        if result.get("result") == "error":
            print(f"vanth remote remove: {result.get('error')}", file=sys.stderr)
            return 1
        print(f"vanth remote remove: removed {remote_id}")
        return 0
    print(f"vanth remote: unknown subcommand {sub!r}", file=sys.stderr)
    return 2


def cmd_stop(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    if not argv:
        print("vanth stop: missing job id", file=sys.stderr)
        return 2
    job_id = argv[0]
    signal = "terminate"
    kill_after = 10
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--signal":
            i += 1
            if i >= len(argv):
                print("vanth stop: --signal requires a value", file=sys.stderr)
                return 2
            value = argv[i]
            if value not in {"terminate", "kill"}:
                print("vanth stop: --signal must be terminate or kill", file=sys.stderr)
                return 2
            signal = value
        elif arg == "--kill-after":
            i += 1
            if i >= len(argv):
                print("vanth stop: --kill-after requires a value", file=sys.stderr)
                return 2
            try:
                kill_after = int(argv[i])
            except ValueError:
                print(f"vanth stop: invalid --kill-after value {argv[i]!r}", file=sys.stderr)
                return 2
        else:
            print(f"vanth stop: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    client = VanthClient(home=home)
    try:
        client.ensure()
        result = client.post(f"/jobs/{job_id}/stop", {"signal": signal, "kill_after_seconds": kill_after})
    except Exception as exc:
        print(f"vanth stop: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        error = result.get("error") or ""
        if "not running" in error or "is not running" in error or "unknown" in error.lower():
            print(f"vanth stop: unknown or not running job {job_id}", file=sys.stderr)
            return 1
        print(f"vanth stop: daemon error: {error}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"vanth stop: requested stop for {job_id}")
    return 0


def cmd_artifacts(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    if not argv:
        print("vanth artifacts: missing job id", file=sys.stderr)
        return 2
    job_id = argv[0]
    limit = 50
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--limit":
            i += 1
            if i >= len(argv):
                print("vanth artifacts: --limit requires a value", file=sys.stderr)
                return 2
            try:
                limit = int(argv[i])
            except ValueError:
                print(f"vanth artifacts: invalid --limit value {argv[i]!r}", file=sys.stderr)
                return 2
        else:
            print(f"vanth artifacts: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    client = VanthClient(home=home)
    try:
        client.ensure()
        result = client.get(f"/jobs/{job_id}/artifacts", {"limit": limit})
    except Exception as exc:
        print(f"vanth artifacts: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        error = result.get("error") or ""
        if "unknown" in error.lower() or "job_id" in error.lower():
            print(f"vanth artifacts: unknown job {job_id}", file=sys.stderr)
            return 1
        print(f"vanth artifacts: daemon error: {error}", file=sys.stderr)
        return 1
    artifacts = result.get("artifacts") or []
    if json_out:
        print(json.dumps(result, indent=2, default=str))
        return 0
    if not artifacts:
        print(f"vanth artifacts: no artifacts for {job_id}")
        return 0
    for artifact in artifacts:
        size = artifact.get("size_bytes")
        size_text = _fmt_bytes(size) if size is not None else ""
        print(
            f"{(artifact.get('name') or ''):<24} {(artifact.get('kind') or ''):<12} "
            f"{size_text:<10} {(artifact.get('uri') or ''):<40} {artifact.get('created_at') or ''}"
        )
    return 0


def cmd_prune(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    older_than = 0
    dry_run: bool | None = None
    assume_yes = False
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--older-than":
            i += 1
            if i >= len(argv):
                print("vanth prune: --older-than requires a value", file=sys.stderr)
                return 2
            try:
                older_than = int(argv[i])
            except ValueError:
                print(f"vanth prune: invalid --older-than value {argv[i]!r}", file=sys.stderr)
                return 2
        elif arg == "--dry-run":
            dry_run = True
        elif arg == "--yes":
            assume_yes = True
            dry_run = False
        else:
            print(f"vanth prune: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    if dry_run is None:
        dry_run = not assume_yes
    client = VanthClient(home=home)
    try:
        client.ensure()
        result = client.post("/cleanup", {"older_than_seconds": older_than, "dry_run": dry_run})
    except Exception as exc:
        print(f"vanth prune: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        print(f"vanth prune: daemon error: {result.get('error') or result}", file=sys.stderr)
        return 1
    count = int(result.get("count") or 0)
    job_ids = result.get("jobs") or []
    if json_out:
        print(json.dumps(result, indent=2, default=str))
        return 0
    if dry_run:
        if count:
            print(f"vanth prune: would remove {count} job(s): {', '.join(job_ids)}")
        else:
            print("vanth prune: no jobs to remove")
        return 0
    if not assume_yes:
        if count == 0:
            print("vanth prune: no jobs to remove")
            return 0
        try:
            answer = input(f"Remove {count} job(s)? [y/N] ")
        except EOFError:
            answer = "n"
        if answer.strip().lower() not in {"y", "yes"}:
            print("vanth prune: aborted")
            return 0
    if count:
        print(f"vanth prune: removed {count} job(s)")
    else:
        print("vanth prune: no jobs to remove")
    return 0


def _usage() -> str:
    return (
        "usage: vanth <command> [options]\n"
        "\n"
        "Vanth is a local background-job daemon for AI agents (and humans).\n"
        "\n"
        "commands:\n"
        "  status         show daemon health, running jobs, and MCP client state\n"
        "  doctor         full health report (schema, deliveries, tools);\n"
        "                 --reap-orphans terminates orphaned MCP servers\n"
        "  list, ps       list jobs (running by default; --all for terminal)\n"
        "  logs, tail     show a job's stdout/stderr output (--grep filters lines)\n"
        "  diff           diff the run specs of two jobs\n"
        "  stop           stop a running job\n"
        "  artifacts      list a job's artifacts\n"
        "  prune          manually clean up terminal jobs (default dry-run)\n"
        "  restart        gracefully restart the daemon (jobs survive)\n"
        "  remote         pair/list/doctor/remove/pending/retry remote execution hosts\n"
        "  setup          register the MCP server in your clients' configs (one-shot)\n"
        "  autostart      enable/disable/status (daemon survives reboots)\n"
        "  version        print the installed package version\n"
        "\n"
        "options:\n"
        "  --help, -h     show this help\n"
        "  --json         machine-readable output (where supported)\n"
        "\n"
        "run `vanth setup` after installing to connect your MCP clients; run\n"
        "`vanth <command> --help` for command-specific options.\n"
    )


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    home = canonical_home()
    json_out = "--json" in argv
    argv = [arg for arg in argv if arg != "--json"]
    if not argv or argv[0] in {"--help", "-h", "help"}:
        print(_usage(), end="")
        return 0
    command = argv[0]
    if command in {"--version", "version"}:
        return cmd_version()
    if command == "status":
        return cmd_status(home, json_out=json_out)
    if command == "doctor":
        return cmd_doctor(argv[1:], home, json_out=json_out)
    if command == "restart":
        return cmd_restart(home, json_out=json_out)
    if command == "setup":
        return cmd_setup(argv[1:], home, json_out=json_out)
    if command == "autostart":
        return cmd_autostart(argv[1:], home, json_out=json_out)
    if command in {"list", "ps"}:
        return cmd_list(argv[1:], home, json_out=json_out)
    if command in {"logs", "tail"}:
        return cmd_logs(argv[1:], home, json_out=json_out)
    if command == "diff":
        return cmd_diff(argv[1:], home, json_out=json_out)
    if command == "stop":
        return cmd_stop(argv[1:], home, json_out=json_out)
    if command == "artifacts":
        return cmd_artifacts(argv[1:], home, json_out=json_out)
    if command == "prune":
        return cmd_prune(argv[1:], home, json_out=json_out)
    if command == "remote":
        return cmd_remote(argv[1:], home, json_out=json_out)
    print(f"vanth: unknown command {command!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
