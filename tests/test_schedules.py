"""Cron/interval schedules: parser, next-fire math, and the schedule loop."""

from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest

from vanth.server import JobManager
from vanth.schedules import (
    compute_next_fire,
    next_cron_fire,
    next_cron_fires,
    validate_cron,
    validate_schedule_spec,
    validate_timezone,
)


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def test_cron_basic_fields_and_steps():
    assert next_cron_fire("*/15 * * * *", after=_utc("2026-01-01T00:00:30Z")) == _utc("2026-01-01T00:15:00Z")
    assert next_cron_fire("0 9 * * 1-5", after=_utc("2026-01-02T10:00:00Z")) == _utc("2026-01-05T09:00:00Z")
    assert next_cron_fire("30 1,13 * * *", after=_utc("2026-01-01T02:00:00Z")) == _utc("2026-01-01T13:30:00Z")


def test_cron_accepts_dow_seven_as_sunday():
    # 2026-01-04 is a Sunday; cron accepts both 0 and 7 for Sunday.
    for dow in ("0", "7"):
        assert next_cron_fire(f"0 0 * * {dow}", after=_utc("2026-01-01T00:00:00Z")) == _utc("2026-01-04T00:00:00Z")
    # A 7 in a range normalizes to Sunday too (Fri..Sun -> includes Sunday).
    assert next_cron_fire("0 0 * * 5-7", after=_utc("2026-01-04T01:00:00Z")) == _utc("2026-01-09T00:00:00Z")


def test_cron_macros_and_validation():
    assert next_cron_fire("@daily", after=_utc("2026-01-01T05:00:00Z")) == _utc("2026-01-02T00:00:00Z")
    assert validate_cron("@hourly") == "@hourly"
    for bad in ("60 * * * *", "* * * *", "@bogus", "* * * * * *", "1-0 * * * *"):
        with pytest.raises(ValueError):
            validate_cron(bad)


def test_cron_dom_dow_or_semantics():
    # Both day fields restricted -> either matches (POSIX). 2026-01-01 is a
    # Thursday; day-of-month 1 OR Friday should match 2026-01-02.
    assert next_cron_fire("0 0 1 * 5", after=_utc("2026-01-01T01:00:00Z")) == _utc("2026-01-02T00:00:00Z")


def test_cron_dst_spring_forward_skips_nonexistent_time():
    # 2026-03-08 America/New_York: 02:00 jumps to 03:00, so 02:30 does not exist.
    nxt = next_cron_fire(
        "30 2 * * *",
        timezone_name="America/New_York",
        after=_utc("2026-03-08T00:00:00Z"),
    )
    assert nxt == _utc("2026-03-09T06:30:00Z")


def test_cron_dst_fall_back_matches_both_occurrences():
    # 2026-11-01 America/New_York: 01:30 occurs once as EDT (-4) and once as EST (-5).
    first = next_cron_fire(
        "30 1 * * *", timezone_name="America/New_York", after=_utc("2026-11-01T00:00:00Z")
    )
    second = next_cron_fire(
        "30 1 * * *", timezone_name="America/New_York", after=first
    )
    assert first == _utc("2026-11-01T05:30:00Z")
    assert second == _utc("2026-11-01T06:30:00Z")


def test_interval_and_validation():
    base = _utc("2026-01-01T00:00:00Z")
    assert compute_next_fire(interval_seconds=90, after=base) == base + timedelta(seconds=90)
    validate_schedule_spec(cron=None, interval_seconds=60)
    validate_schedule_spec(cron="* * * * *", interval_seconds=None)
    with pytest.raises(ValueError):
        validate_schedule_spec(cron=None, interval_seconds=None)
    with pytest.raises(ValueError):
        validate_schedule_spec(cron="* * * * *", interval_seconds=60)
    with pytest.raises(ValueError):
        validate_schedule_spec(cron=None, interval_seconds=0)
    assert validate_timezone("UTC") == "UTC"
    with pytest.raises(ValueError):
        validate_timezone("Not/AZone")


def test_next_cron_fires_is_ordered():
    fires = next_cron_fires("0 * * * *", after=_utc("2026-01-01T00:00:00Z"), count=3)
    assert fires == [_utc("2026-01-01T01:00:00Z"), _utc("2026-01-01T02:00:00Z"), _utc("2026-01-01T03:00:00Z")]


def _force_due(manager: JobManager, schedule_id: str) -> None:
    with manager.db_lock:
        manager.db.execute(
            "UPDATE schedules SET next_fire_at=? WHERE schedule_id=?",
            ("2000-01-01T00:00:00Z", schedule_id),
        )
        manager.db.commit()


def _wait_terminal(manager: JobManager, job_id: str, timeout: float = 20.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = manager.status(job_id)["status"]
        if status in {"completed", "failed", "timeout", "cancelled", "orphaned"}:
            return status
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not finish")


def test_schedule_fires_a_job_and_advances(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        schedule = manager.create_schedule(cmd("print('scheduled run')"), interval_seconds=60, name="nightly")
        assert schedule["next_fire_at"] is not None and schedule["enabled"] is True
        assert manager.schedule_next_fires(schedule["schedule_id"], count=1)["next_fires"]

        _force_due(manager, schedule["schedule_id"])
        manager._fire_due_schedules()

        jobs = manager.list()["jobs"]
        assert jobs, "schedule should have created a job"
        job = manager.status(jobs[0]["job_id"])
        assert job["schedule_id"] == schedule["schedule_id"]
        assert "scheduled" in job["tags"]
        assert _wait_terminal(manager, jobs[0]["job_id"]) == "completed"

        after = manager.get_schedule(schedule["schedule_id"])
        assert after["fire_count"] == 1
        assert after["next_fire_at"] > "2026-01-01"
    finally:
        manager.close()


def test_schedule_overlap_skip_holds_while_running(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        schedule = manager.create_schedule(cmd("import time; time.sleep(30)"), interval_seconds=60)
        _force_due(manager, schedule["schedule_id"])
        manager._fire_due_schedules()
        running = manager.list()["jobs"]
        assert len(running) == 1
        for _ in range(200):
            if manager.status(running[0]["job_id"])["status"] == "running":
                break
            time.sleep(0.05)

        _force_due(manager, schedule["schedule_id"])
        manager._fire_due_schedules()
        assert len(manager.list()["jobs"]) == 1, "overlap=skip must not stack a second run"
        assert manager.get_schedule(schedule["schedule_id"])["fire_count"] == 1

        manager.stop_sync(running[0]["job_id"])
    finally:
        manager.close()


def test_disabled_schedule_never_fires(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        schedule = manager.create_schedule(cmd("print('x')"), interval_seconds=60, enabled=False)
        assert schedule["next_fire_at"] is None
        _force_due(manager, schedule["schedule_id"])
        manager._fire_due_schedules()
        assert manager.list()["jobs"] == []
    finally:
        manager.close()


def test_schedule_update_and_delete(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        schedule = manager.create_schedule(cmd("echo a"), interval_seconds=60)
        updated = manager.update_schedule(schedule["schedule_id"], interval_seconds=120, name="renamed")
        assert updated["interval_seconds"] == 120 and updated["name"] == "renamed"
        with pytest.raises(ValueError):
            manager.update_schedule(schedule["schedule_id"], cron="* * * * *")  # both set
        with pytest.raises(ValueError, match="unknown schedule fields"):
            manager.update_schedule(schedule["schedule_id"], bogus=1)
        manager.delete_schedule(schedule["schedule_id"])
        with pytest.raises(ValueError):
            manager.get_schedule(schedule["schedule_id"])
    finally:
        manager.close()
