import asyncio
import sqlite3
import subprocess
import sys

from vanth.server import JobManager, normalize_event_payload, now_iso, parse_agent_event_line


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def run(coro):
    async def wrapper():
        try:
            return await coro
        finally:
            await asyncio.sleep(0.1)

    return asyncio.run(wrapper())


def test_parse_valid_inline_event():
    event = parse_agent_event_line('AGENT_EVENT {"type":"checkpoint","message":"done","data":{"x":1}}')
    assert event == {"type": "checkpoint", "message": "done", "data": {"x": 1}}


def test_ignore_malformed_inline_event():
    assert parse_agent_event_line("AGENT_EVENT nope") is None
    assert parse_agent_event_line('AGENT_EVENT {"message":"missing type"}') is None
    assert parse_agent_event_line("hello") is None


def test_normalize_progress_percent():
    event = normalize_event_payload({"type": "progress", "data": {"current": 2, "total": 4}})
    assert event["data"]["percent"] == 50


def test_wait_returns_stored_event(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        started = await manager.start(cmd("print('hello')"))
        result = await manager.wait(started["job_id"], ["completed"], timeout_seconds=5)
        assert result["result"] == "event"
        assert result["event"]["type"] == "completed"

    run(main())


def test_checkpoint_progress_and_tail(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        code = (
            "import json,time;"
            "print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':1,'total':2,'unit':'step'}}), flush=True);"
            "print('plain log', flush=True);"
            "time.sleep(.1);"
            "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'done'}), flush=True)"
        )
        started = await manager.start(cmd(code))
        progress = await manager.wait(started["job_id"], ["progress"], timeout_seconds=5)
        checkpoint = await manager.wait(started["job_id"], ["checkpoint"], progress["event"]["event_id"], timeout_seconds=5)
        completed = await manager.wait(started["job_id"], ["completed"], checkpoint["event"]["event_id"], timeout_seconds=5)

        assert progress["event"]["data"]["percent"] == 50
        assert checkpoint["event"]["type"] == "checkpoint"
        assert completed["event"]["type"] == "completed"
        assert manager.status(started["job_id"])["progress"]["unit"] == "step"
        assert "plain log" in manager.tail(started["job_id"])["content"]
        tail = manager.tail(started["job_id"], max_bytes=4)
        assert tail["truncated"]
        assert len(tail["content"].encode()) <= 4

    run(main())


def test_restart_persists_completed_job(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        started = await manager.start(cmd("print('hello')"))
        completed = await manager.wait(started["job_id"], ["completed"], timeout_seconds=5)
        manager.close()

        restarted = JobManager(tmp_path)
        assert restarted.status(started["job_id"])["status"] == "completed"
        assert restarted.events(started["job_id"], types=["completed"])["events"][0]["event_id"] == completed["event"]["event_id"]
        assert "hello" in restarted.tail(started["job_id"])["content"]
        restarted.close()

    run(main())


def test_restart_marks_running_job_orphaned(tmp_path):
    manager = JobManager(tmp_path)
    manager.close()
    ts = now_iso()
    db = sqlite3.connect(tmp_path / "jobs.sqlite")
    db.execute(
        """
        INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, events_path)
        VALUES ('job_stale', 'sleep 30', 'running', ?, ?, ?, ?, ?)
        """,
        (
            ts,
            ts,
            str(tmp_path / "logs" / "job_stale.stdout.log"),
            str(tmp_path / "logs" / "job_stale.stderr.log"),
            str(tmp_path / "events" / "job_stale.jsonl"),
        ),
    )
    db.commit()
    db.close()

    restarted = JobManager(tmp_path)
    assert restarted.status("job_stale")["status"] == "orphaned"
    restarted.close()


def test_failed_and_cancelled(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        failed_job = await manager.start(cmd("import sys; sys.exit(3)"))
        failed = await manager.wait(failed_job["job_id"], ["failed"], timeout_seconds=5)
        assert failed["event"]["data"]["exit_code"] == 3

        slow_job = await manager.start(cmd("import time; time.sleep(30)"))
        waiter = asyncio.create_task(manager.wait(slow_job["job_id"], ["cancelled"], timeout_seconds=5))
        stopped = await manager.stop(slow_job["job_id"], kill_after_seconds=1)
        cancelled = await waiter
        assert stopped["status"] == "cancelled"
        assert cancelled["event"]["type"] == "cancelled"

    run(main())


def test_multiple_waiters(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        code = "import json,time; time.sleep(.2); print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"
        started = await manager.start(cmd(code))
        results = await asyncio.gather(
            manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=5),
            manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=5),
        )
        assert [r["event"]["type"] for r in results] == ["checkpoint", "checkpoint"]

    run(main())
