"""Review #6: duration, queue-time, and flakiness analytics."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from vanth.server import JobManager, _duration_trend, _percentile


def _iso(offset_seconds: float) -> str:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return (base + timedelta(seconds=offset_seconds)).isoformat().replace("+00:00", "Z")


def _seed(manager: JobManager, job_id: str, name: str, status: str, *, start: float, end: float, created: float | None = None):
    created = start if created is None else created
    with manager.db_lock:
        manager.db.execute(
            "INSERT INTO jobs(job_id, name, command, status, created_at, updated_at, started_at, ended_at, "
            "stdout_path, stderr_path, events_path) VALUES (?, ?, 'echo', ?, ?, ?, ?, ?, 'o', 'e', 'ev')",
            (job_id, name, status, _iso(created), _iso(end), _iso(start), _iso(end)),
        )
        manager.db.commit()


def test_percentile_interpolates():
    assert _percentile([], 0.5) is None
    assert _percentile([10.0], 0.95) == 10.0
    assert _percentile([0.0, 10.0], 0.5) == 5.0
    assert _percentile([1.0, 2.0, 3.0, 4.0], 0.5) == 2.5


def test_duration_trend_classifies():
    assert _duration_trend([1.0, 2.0, 3.0])["direction"] == "unknown"
    regressing = _duration_trend([100.0, 110.0, 120.0, 700.0, 720.0, 740.0])
    assert regressing["direction"] == "regressing" and regressing["factor"] > 1.5
    improving = _duration_trend([700.0, 720.0, 740.0, 100.0, 110.0, 120.0])
    assert improving["direction"] == "improving"
    stable = _duration_trend([100.0, 102.0, 101.0, 103.0, 99.0, 104.0])
    assert stable["direction"] == "stable"


def test_duration_stats_groups_and_stats(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        # "steady": 4 successes, runtimes 100/200/300/400.
        for i, duration in enumerate((100.0, 200.0, 300.0, 400.0)):
            _seed(manager, f"job_steady_{i}", "steady", "completed", start=i * 1000.0, end=i * 1000.0 + duration)
        # "flaky": completed, failed, completed -> the failed run is flaky.
        _seed(manager, "job_flaky_0", "flaky", "completed", start=0.0, end=10.0)
        _seed(manager, "job_flaky_1", "flaky", "failed", start=100.0, end=130.0)
        _seed(manager, "job_flaky_2", "flaky", "completed", start=200.0, end=210.0)
        # "creep": 8 runs, 100s baseline climbing to 800s -> regressing.
        for i, duration in enumerate((100.0, 110.0, 120.0, 130.0, 700.0, 720.0, 740.0, 760.0)):
            _seed(manager, f"job_creep_{i}", "creep", "completed", start=i * 1000.0, end=i * 1000.0 + duration)
        # queue time: a trigger wait of 50s.
        _seed(manager, "job_queue", "queued-ish", "completed", created=0.0, start=50.0, end=60.0)

        stats = manager.duration_stats(limit=20, slowest=5)
        groups = {group["key"]: group for group in stats["groups"]}

        steady = groups["steady"]
        assert steady["runs"] == 4 and steady["success_rate"] == 1.0
        assert steady["duration_seconds"]["p50"] == 250.0
        assert steady["duration_seconds"]["max"] == 400.0

        flaky = groups["flaky"]
        assert flaky["flaky_runs"] == 1
        assert abs(flaky["flaky_score"] - 1 / 3) < 1e-3
        assert flaky["success_rate"] == round(2 / 3, 4)

        creep = groups["creep"]
        assert creep["trend"]["direction"] == "regressing"

        queued = groups["queued-ish"]
        assert queued["queue_seconds"]["p50"] == 50.0

        # Global slowest is the longest run in the fixture.
        slowest = stats["slowest"]
        assert slowest[0]["duration_seconds"] == max(
            s["duration_seconds"] for s in slowest
        )
        assert slowest[0]["duration_seconds"] >= 760.0
    finally:
        manager.close()


def test_duration_stats_tag_filter(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        with manager.db_lock:
            manager.db.execute(
                "INSERT INTO jobs(job_id, name, command, status, created_at, updated_at, started_at, ended_at, "
                "tags_json, stdout_path, stderr_path, events_path) "
                "VALUES ('job_tagged', 'tagged', 'echo', 'completed', ?, ?, ?, ?, '[\"gpu\"]', 'o', 'e', 'ev')",
                (_iso(0), _iso(10), _iso(0), _iso(10)),
            )
            manager.db.execute(
                "INSERT INTO jobs(job_id, name, command, status, created_at, updated_at, started_at, ended_at, "
                "tags_json, stdout_path, stderr_path, events_path) "
                "VALUES ('job_untagged', 'untagged', 'echo', 'completed', ?, ?, ?, ?, '[\"cpu\"]', 'o', 'e', 'ev')",
                (_iso(0), _iso(20), _iso(0), _iso(20)),
            )
            manager.db.commit()
        stats = manager.duration_stats(tags=["gpu"])
        keys = {group["key"] for group in stats["groups"]}
        assert keys == {"tagged"}
    finally:
        manager.close()
