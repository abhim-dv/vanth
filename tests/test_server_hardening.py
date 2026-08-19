import asyncio
import inspect
import json
import subprocess
import sys
import time

import pytest

from vanth.server import JobManager


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def test_malformed_event_does_not_kill_reader(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        code = (
            "import json; "
            "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':{'bad':1}}), flush=True); "
            "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'after bad'}), flush=True)"
        )
        started = await manager.start(cmd(code))
        result = await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=5)
        assert result["event"]["message"] == "after bad"
        manager.close()

    asyncio.run(main())


def test_oversized_event_line_is_rejected_and_reader_continues(tmp_path, monkeypatch):
    async def main():
        monkeypatch.setenv("VANTH_MAX_EVENT_LINE_BYTES", "256")
        manager = JobManager(tmp_path)
        code = (
            "import json; "
            "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'x'*1000}), flush=True); "
            "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'after big'}), flush=True)"
        )
        started = await manager.start(cmd(code))
        rejected = await manager.wait(started["job_id"], ["event_rejected"], timeout_seconds=5)
        valid = await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=5)
        assert rejected["event"]["data"] == {"max_bytes": 256}
        assert valid["event"]["message"] == "after big"
        manager.close()

    asyncio.run(main())


@pytest.mark.parametrize(
    "target",
    [
        {"type": "local_command", "events": 7, "command": ["ignored"]},
        {"type": "unknown", "events": []},
        {"type": "local_command", "events": []},
        {"type": "local_command", "events": [], "command": ["ignored"], "max_attempts": "bad"},
    ],
)
def test_invalid_wake_targets_fail_before_launch(tmp_path, target):
    manager = JobManager(tmp_path)
    with pytest.raises(ValueError):
        asyncio.run(manager.start(cmd("pass"), wake_targets=[target]))
    assert manager.list()["jobs"] == []
    manager.close()


def test_recovery_does_not_overwrite_concurrent_completion(tmp_path):
    class SlowDeadCheck(JobManager):
        def _pid_alive(self, pid):
            time.sleep(1)
            return False

    async def start():
        manager = JobManager(tmp_path)
        started = await manager.start(cmd("import time; time.sleep(.3)"))
        await manager.wait(started["job_id"], ["started"], timeout_seconds=5)
        manager.close()
        return started["job_id"]

    job_id = asyncio.run(start())
    recovered = SlowDeadCheck(tmp_path)
    result = recovered.wait_sync(job_id, ["completed"], timeout_seconds=5)
    assert result["result"] == "event"
    assert recovered.status(job_id)["status"] == "completed"
    assert recovered.events(job_id, types=["orphaned"])["events"] == []
    recovered.close()


def test_unknown_job_events_is_an_error(tmp_path):
    manager = JobManager(tmp_path)
    with pytest.raises(ValueError, match="Unknown job_id"):
        manager.events("job_missing")
    manager.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows recovered process-tree behavior")
def test_stop_after_restart_kills_runner_and_workload(tmp_path):
    async def start():
        manager = JobManager(tmp_path)
        started = await manager.start(cmd("import time; time.sleep(30)"))
        await manager.wait(started["job_id"], ["started"], timeout_seconds=5)
        status = manager.status(started["job_id"])
        manager.close()
        return started["job_id"], status["worker_pid"], status["pid"]

    job_id, worker_pid, pid = asyncio.run(start())
    recovered = JobManager(tmp_path)
    stopped = recovered.stop_sync(job_id, kill_after_seconds=0)
    assert stopped["status"] == "cancelled"
    assert not recovered._pid_alive(worker_pid)
    assert not recovered._pid_alive(pid)
    recovered.close()


def test_live_manager_detects_runner_disappearance(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        started = await manager.start(cmd("import time; time.sleep(30)"))
        await manager.wait(started["job_id"], ["started"], timeout_seconds=5)
        status = manager.status(started["job_id"])
        manager._kill_pid(status["worker_pid"], force=True)
        orphaned = await manager.wait(started["job_id"], ["orphaned"], timeout_seconds=10)
        assert orphaned["result"] == "event"
        assert manager.status(started["job_id"])["status"] == "orphaned"
        assert not manager._pid_alive(status["pid"])
        manager.close()

    asyncio.run(main())


def test_runner_popen_failure_marks_job_failed(tmp_path, monkeypatch):
    real_popen = subprocess.Popen

    def failing_popen(argv, *args, **kwargs):
        if "-m" in argv and "vanth.runner" in argv:
            raise OSError("venv python missing")
        return real_popen(argv, *args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", failing_popen)
    manager = JobManager(tmp_path)

    async def main():
        started = await manager.start(cmd("import time; time.sleep(30)"))
        status = manager.status(started["job_id"])
        events = manager.events(started["job_id"], types=["failed"])["events"]
        assert status["status"] == "failed"
        assert status["exit_code"] == 1
        assert started["status"] == "failed"
        assert started["exit_code"] == 1
        assert started["worker_pid"] is None
        assert started["message"].startswith("Job runner failed to start:")
        assert events and events[0]["source"] == "server"
        assert events[0]["message"].startswith("Job runner failed to start:")
        assert not (manager.home / "specs" / f"{started['job_id']}.json").exists()
        assert started["job_id"] not in manager.processes

    asyncio.run(main())
    manager.close()


def test_job_start_mcp_tool_is_not_a_coroutine_function():
    from vanth.server import mcp

    tool = mcp._tool_manager._tools["job_start"]
    assert inspect.iscoroutinefunction(tool.fn) is False
