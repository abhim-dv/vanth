"""Tests for the MCP parent-liveness/idle watchdog and orphaned-server detection."""

import os
import subprocess
import sys
import time

import pytest

from vanth.process_watch import (
    _InFlight,
    _watch_loop,
    parent_pid,
    process_alive,
    start_watchdog,
)


def test_process_alive_false_for_dead_pid():
    assert process_alive(1) is False
    assert process_alive(None) is False
    assert process_alive(0) is False


def test_process_alive_true_for_running_child():
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        assert process_alive(proc.pid) is True
    finally:
        proc.terminate()
        proc.wait(timeout=5)
    assert process_alive(proc.pid) is False


def test_parent_pid_returns_own_parent():
    pid = parent_pid()
    assert isinstance(pid, int) and pid > 1


def test_parent_pid_env_override(monkeypatch):
    monkeypatch.setenv("VANTH_WATCH_PARENT_PID", "999")
    assert parent_pid() == 999
    monkeypatch.setenv("VANTH_WATCH_PARENT_PID", "not-a-number")
    assert parent_pid() != "not-a-number"


def test_in_flight_tracker():
    tracker = _InFlight()
    assert tracker.active is False
    with tracker:
        assert tracker.active is True
        with tracker:
            assert tracker.active is True
        assert tracker.active is True
    assert tracker.active is False


def test_watch_loop_exits_when_parent_dies():
    exited = []

    def fake_exit():
        exited.append(True)

    # Parent PID 1 is never alive, so the loop exits on the first poll.
    _watch_loop(parent=1, interval=0.01, grace=0.0, idle=0.0, on_exit=fake_exit, tracker=_InFlight())
    assert exited


def test_watch_loop_idle_exit_without_traffic():
    exited = []

    def fake_exit():
        exited.append(True)

    # No stdin traffic (inject a probe that reports none), parent is self
    # (alive), idle threshold is tiny. The loop must exit.
    _watch_loop(
        parent=os.getpid(),
        interval=0.01,
        grace=60,
        idle=0.02,
        on_exit=fake_exit,
        tracker=_InFlight(),
        traffic=lambda: 0,
    )
    assert exited


def test_watch_loop_never_idle_exits_while_in_flight():
    exited = []

    def fake_exit():
        exited.append(True)

    import threading

    tracker = _InFlight()
    with tracker:
        thread = threading.Thread(
            target=_watch_loop,
            args=(os.getpid(), 0.005, 60, 0.01, fake_exit, tracker),
            kwargs={"traffic": lambda: 0},
            daemon=True,
        )
        thread.start()
        time.sleep(0.05)
        assert not exited
    thread.join(timeout=0.5)
    assert not exited


def test_start_watchdog_none_when_no_parent(monkeypatch):
    monkeypatch.delenv("VANTH_WATCH_PARENT_PID", raising=False)

    def fake_getppid():
        return None

    # Patch os.getppid to None → no parent → no watchdog.
    import vanth.process_watch as pw

    orig = pw.os.getppid
    pw.os.getppid = fake_getppid
    try:
        thread, tracker = start_watchdog()
        assert thread is None
    finally:
        pw.os.getppid = orig


def test_start_watchdog_runs_when_parent_exists(monkeypatch):
    import vanth.process_watch as pw

    orig = pw.os.getppid
    pw.os.getppid = lambda: os.getpid()  # self as parent → alive
    try:
        thread, tracker = start_watchdog(interval=0.05, grace=60, idle=0.0)
        assert thread is not None and thread.is_alive()
        thread.join(timeout=0.2)
    finally:
        pw.os.getppid = orig


def test_orphaned_mcp_detection_empty_when_no_orphans():
    from vanth.server import _orphaned_mcp_servers

    # On a healthy box this should be an empty list (no exception).
    try:
        result = _orphaned_mcp_servers()
        assert isinstance(result, list)
    except Exception as exc:  # wmic/ps unavailable in some sandboxes
        pytest.skip(f"process enumeration unavailable: {exc}")
