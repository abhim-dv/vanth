"""Deterministic release-workload regressions that run in ordinary CI.

The heavy chaos matrix (50x500 events, repeated daemon kills) lives in
scripts/chaos_matrix.py. These tests exercise the same guarantees at a
smaller, CI-safe scale:

- a concurrent event burst loses no structured events and preserves unique
  per-job sequence numbers;
- a slow wake adapter never delays stream parsing or terminal state;
- log caps bound each stream and cleanup is idempotent.
"""

import asyncio
import json
import subprocess
import sys
import threading
import time

from vanth.server import JobManager


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def wait_completed(manager: JobManager, job_id: str, timeout: float = 120) -> None:
    # 120s default: the concurrent burst test launches 10 jobs in parallel and
    # the full suite runs under load; 60s flaked when reader threads lagged.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if manager.status(job_id)["status"] in {"completed", "failed"}:
            return
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not finish in time")


def event_counts(manager: JobManager, job_id: str) -> dict[str, int]:
    rows = manager.db.execute(
        "SELECT type, COUNT(*) AS count FROM events WHERE job_id=? GROUP BY type", (job_id,)
    ).fetchall()
    return {row["type"]: row["count"] for row in rows}


def all_event_seqs(manager: JobManager, job_id: str) -> list[int]:
    rows = manager.db.execute(
        "SELECT seq FROM events WHERE job_id=? ORDER BY seq", (job_id,)
    ).fetchall()
    return [row["seq"] for row in rows]


def wait_event_counts(manager: JobManager, job_id: str, expected: dict[str, int], timeout: float = 30) -> None:
    """Wait until per-type event counts reach expectations.

    Status flips to terminal before the terminal EVENT row is necessarily
    observed by a separate reader connection; asserting counts immediately
    after wait_completed raced under load (review rc14 P1-11)."""
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        last = event_counts(manager, job_id)
        if all(last.get(t, 0) >= c for t, c in expected.items()):
            return
        time.sleep(0.05)
    raise AssertionError(f"events did not reach {expected} in time; last={last}")


def test_concurrent_burst_loses_no_events_and_keeps_unique_seq(tmp_path):
    jobs = 10
    per_job = 40
    manager = JobManager(tmp_path / "state")
    try:
        started = []
        for index in range(jobs):
            code = (
                "import json,sys;"
                f"f=lambda i:(print('AGENT_EVENT '+json.dumps({{'type':'metric','data':{{'i':i}}}}), flush=True),"
                f"print('AGENT_EVENT '+json.dumps({{'type':'metric','data':{{'i':i+{per_job}}}}}), file=sys.stderr, flush=True));"
                f"[f(i) for i in range({per_job})]"
            )
            started.append(
                asyncio.run(
                    manager.start(cmd(code), name=f"burst-{index}")
                )["job_id"]
            )
        for job_id in started:
            wait_completed(manager, job_id)
            wait_event_counts(manager, job_id, {
                "metric": per_job * 2,
                "started": 1,
                "completed": 1,
            })

        for job_id in started:
            counts = event_counts(manager, job_id)
            assert counts["metric"] == per_job * 2, (job_id, counts)
            assert counts["started"] == 1, (job_id, counts)
            assert counts["completed"] == 1, (job_id, counts)
            seqs = all_event_seqs(manager, job_id)
            assert seqs == list(range(1, per_job * 2 + 3)), job_id
    finally:
        manager.close()


def test_slow_wake_adapter_does_not_delay_stream_parsing(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        slow_command = [
            sys.executable,
            "-c",
            "import time; time.sleep(5)",
        ]
        code = (
            "import json,time;"
            "f=lambda i:(print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':i,'total':10}}), flush=True),"
            "time.sleep(0.05));"
            "[f(i) for i in range(1,11)]"
        )
        job_id = asyncio.run(
            manager.start(
                cmd(code),
                wake_targets=[
                    {"type": "local_command", "events": ["progress"], "command": slow_command}
                ],
            )
        )["job_id"]

        start = time.monotonic()
        wait_completed(manager, job_id, timeout=10)
        elapsed = time.monotonic() - start
        assert elapsed < 4, f"job completion waited on the slow adapter: {elapsed:.2f}s"
        assert event_counts(manager, job_id)["progress"] == 10
    finally:
        manager.close()


def test_log_caps_bound_streams_and_cleanup_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_MAX_LOG_BYTES", "1024")
    monkeypatch.setenv("VANTH_MAX_EVENT_LINE_BYTES", "2000")
    manager = JobManager(tmp_path / "state")
    try:
        code = (
            "import sys;"
            "[print('x'*200, flush=True) for _ in range(300)];"
            "[print('y'*200, file=sys.stderr, flush=True) for _ in range(300)]"
        )
        job_id = asyncio.run(manager.start(cmd(code)))["job_id"]
        wait_completed(manager, job_id, timeout=30)

        counts = event_counts(manager, job_id)
        assert counts["log_truncated"] == 2, counts
        row = manager.db.execute("SELECT stdout_path FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        stdout_path = row["stdout_path"]
        assert __import__("os").path.getsize(stdout_path) <= 1024
        first = manager.cleanup(0, dry_run=False)
        assert first["count"] == 1
        second = manager.cleanup(0, dry_run=False)
        assert second["count"] == 0
        assert manager.cleanup(0, dry_run=True)["count"] == 0
    finally:
        manager.close()


def test_cross_process_emits_keep_unique_seq_and_lose_no_events(tmp_path):
    manager = JobManager(tmp_path / "state")
    stamp = "2026-01-01T00:00:00Z"
    job_id = "job_seq_race"
    manager.db.execute(
        "INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, events_path) "
        "VALUES (?, 'x', 'running', ?, ?, ?, ?, ?)",
        (job_id, stamp, stamp,
         str(manager.logs / f"{job_id}.stdout.log"),
         str(manager.logs / f"{job_id}.stderr.log"),
         str(manager.events_dir / f"{job_id}.jsonl")),
    )
    manager.db.commit()
    manager.close()

    worker = (
        "from vanth.server import JobManager;"
        "m = JobManager(%r, recover=False);"
        "[m._emit(%r, 'metric', data={'i': i}, source='worker') for i in range(100)];"
        "m.close()"
    ) % (str(tmp_path / "state"), job_id)
    procs = [subprocess.Popen([sys.executable, "-c", worker], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(6)]
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, err

    restarted = JobManager(tmp_path / "state", recover=False)
    try:
        seqs = [row["seq"] for row in restarted.db.execute(
            "SELECT seq FROM events WHERE job_id=? ORDER BY seq", (job_id,)
        ).fetchall()]
        assert seqs == list(range(1, 601)), len(seqs)
    finally:
        restarted.close()
