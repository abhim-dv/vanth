"""Parent-liveness + idle watchdog for short-lived Vanth entrypoints.

The ``vanth`` MCP stdio server is launched by agent clients (opencode, codex,
etc.). Two failure modes leave these processes running forever:

1. **Orphaned parent**: the client session is killed/times out without sending
   an MCP ``notifications/exit`` or closing stdin. The stdio loop never ends.
2. **Cached worker leak**: some clients spawn a fresh ``vanth`` per operation
   and keep the process around (stdin stays open) even when it will never be
   used again. These accumulate without bound (observed: a new process every
   few minutes, none ever reaped).

This module gives the entrypoint a robust self-termination path:

- A daemon thread polls the parent PID; if the parent disappears, the process
  exits after a short grace period.
- If the parent stays alive but the process is idle (no stdin traffic for a
  configurable interval) and no tool call is in-flight, the process exits too.
  A tool call is "in-flight" whenever MCP has a request context, so a blocking
  ``job_wait`` / ``job_tail --follow`` is never killed mid-call.

All timeouts are env-overridable for tests and unusual launchers. Exiting uses
``os._exit(0)`` so nothing (open files, non-daemon threads, atexit) can hold
the process alive.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

# Environment override so tests and unusual launchers can point the watchdog
# at an explicit parent PID instead of the inferred process parent.
_PARENT_ENV = "VANTH_WATCH_PARENT_PID"

# Seconds between watchdog polls.
_DEFAULT_INTERVAL = 1.0
# Grace period after the parent disappears before exiting.
_DEFAULT_GRACE = 5.0
# Seconds of no stdin traffic (and no in-flight tool call) before exiting a
# cached worker. 0 disables the idle reaper entirely.
_DEFAULT_IDLE = 1800
# Safety ceiling: never idle-exit faster than this even if idle is tuned low.
_MIN_IDLE = 10

_NULL_PIDS = {0, 1}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def process_alive(pid: int | None) -> bool:
    """Return whether a process with the given PID is running.

    Cross-platform and permission-agnostic: on Windows uses ``tasklist``
    (OpenProcess access may be denied for other users' processes, while
    tasklist's CSV output still reports existence), on POSIX uses
    ``os.kill(pid, 0)``. On probe failure we conservatively report alive so a
    transient probe error never terminates a healthy server.
    """
    if not pid or pid in _NULL_PIDS:
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


def parent_pid() -> int | None:
    """Return the PID this process was launched by, if known and meaningful."""
    explicit = os.environ.get(_PARENT_ENV)
    if explicit:
        try:
            pid = int(explicit)
        except ValueError:
            pid = None
        if pid and pid not in _NULL_PIDS:
            return pid
    try:
        ppid = os.getppid()
    except (AttributeError, OSError):
        return None
    if ppid and ppid not in _NULL_PIDS:
        return ppid
    return None


def _exiting() -> None:
    """Hard-exit the process without unwinding; daemon threads cannot stop it."""
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    os._exit(0)


def _stdin_traffic() -> int:
    """Return a cheap proxy for stdin traffic: bytes buffered, or 0 if none.

    Uses ``select`` when available. This never consumes protocol bytes.
    """
    if not sys.stdin or not hasattr(sys.stdin, "buffer"):
        return 0
    try:
        fd = sys.stdin.buffer.fileno()
    except (OSError, ValueError):
        return 0
    try:
        import select as _select

        readable, _, _ = _select.select([fd], [], [], 0)
        return 1 if readable else 0
    except (ImportError, OSError, ValueError):
        return 0


class _InFlight:
    """Tracks whether a tool call is currently executing.

    The watchdog must not idle-exit while a blocking tool call (``job_wait``,
    ``job_tail --follow``) is running. We wrap ``FastMCP.call_tool`` so the
    counter is bumped around each call. The Desktop wake relay also bumps the
    counter around each poll so an active subscription is never idle-reaped
    while it is still delivering wake notifications (review rc37 P1) — the
    relay traffic is real work even though it never touches MCP stdio.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count = 0

    def __enter__(self) -> "_InFlight":
        with self._lock:
            self._count += 1
        return self

    def __exit__(self, *_: object) -> None:
        with self._lock:
            self._count -= 1

    def notify_activity(self) -> None:
        """Mark transient activity so the idle timer resets.

        Used by the Desktop wake relay around each poll/delivery so the
        watchdog does not idle-exit a process that is still actively
        delivering asynchronous wake notifications.
        """
        with self._lock:
            self._count += 1
            self._count -= 1

    @property
    def active(self) -> bool:
        with self._lock:
            return self._count > 0


def start_watchdog(
    *,
    interval: float | None = None,
    grace: float | None = None,
    idle: float | None = None,
    on_exit: callable = _exiting,  # type: ignore[assignment]
    in_flight: _InFlight | None = None,
) -> tuple[threading.Thread | None, _InFlight]:
    """Start a daemon watchdog thread; returns (thread, in_flight tracker).

    Exits the process when the parent dies (after ``grace``) or when the
    process is idle (no stdin traffic and no in-flight tool call) for ``idle``
    seconds. Returns ``(None, tracker)`` when there is nothing to watch (e.g. a
    console ``vanth`` with a TTY and no parent to monitor).

    Tuning is env-overridable for deployment and tests:

    - ``VANTH_WATCH_INTERVAL`` (seconds between polls, default 1.0)
    - ``VANTH_WATCH_GRACE`` (grace after parent death, default 5.0)
    - ``VANTH_WATCH_IDLE`` (idle seconds before self-termination, default 1800;
      0 disables idle reaping entirely)
    """
    interval = interval if interval is not None else _env_float("VANTH_WATCH_INTERVAL", _DEFAULT_INTERVAL)
    grace = grace if grace is not None else _env_float("VANTH_WATCH_GRACE", _DEFAULT_GRACE)
    idle = idle if idle is not None else _env_float("VANTH_WATCH_IDLE", _DEFAULT_IDLE)
    tracker = in_flight or _InFlight()
    parent = parent_pid()
    if parent is None:
        return None, tracker
    # A tiny idle is unsafe in production (could reap a live session mid-think),
    # but tests need small values. Clamp only here, at the production boundary.
    idle = max(idle, _MIN_IDLE) if idle > 0 else 0.0
    thread = threading.Thread(
        target=_watch_loop,
        args=(parent, interval, grace, idle, on_exit, tracker),
        name="vanth-watchdog",
        daemon=True,
    )
    thread.start()
    return thread, tracker


def _watch_loop(
    parent: int,
    interval: float,
    grace: float,
    idle: float,
    on_exit: callable,  # type: ignore[assignment]
    tracker: _InFlight,
    traffic: callable | None = None,  # type: ignore[assignment]
) -> None:
    traffic = traffic or _stdin_traffic
    dead_since: float | None = None
    idle_since: float | None = None
    while True:
        time.sleep(interval)
        now = time.monotonic()
        parent_alive = process_alive(parent)
        busy = tracker.active

        if not parent_alive:
            if dead_since is None:
                dead_since = now
            elif now - dead_since >= grace:
                on_exit()
                return
        else:
            dead_since = None

        if idle > 0 and parent_alive and not busy:
            if traffic():
                idle_since = None
            elif idle_since is None:
                idle_since = now
            elif now - idle_since >= idle:
                on_exit()
                return
        else:
            idle_since = None
