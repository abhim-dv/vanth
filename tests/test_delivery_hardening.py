import asyncio
import subprocess
import sys
import threading
import time

from vanth.server import JobManager


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def wait_for_delivery(manager: JobManager, job_id: str, status: str, timeout: float = 5):
    deadline = time.monotonic() + timeout
    delivery = None
    while time.monotonic() < deadline:
        deliveries = manager.deliveries(job_id)["deliveries"]
        if deliveries:
            delivery = deliveries[0]
            if delivery["status"] == status:
                return delivery
        time.sleep(0.05)
    return delivery


def test_quick_job_automatic_delivery_retry_succeeds(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    calls = tmp_path / "calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys; "
            "p=Path(sys.argv[1]); calls=p.read_text() if p.exists() else ''; "
            "p.write_text(calls+'x'); sys.exit(7 if not calls else 0)"
        ),
        str(calls),
    ]
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[
                {
                    "type": "local_command",
                    "events": ["checkpoint"],
                    "command": delivery_command,
                    "max_attempts": 2,
                    "retry_delay_seconds": 1,
                }
            ],
        )
    )

    delivery = wait_for_delivery(manager, started["job_id"], "delivered")

    assert delivery is not None and delivery["status"] == "delivered"
    assert delivery["attempts"] == 2
    assert calls.read_text() == "xx"


def test_concurrent_delivery_retries_dispatch_once(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    calls = tmp_path / "calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys,time; "
            "ready=Path(sys.argv[1]); calls=Path(sys.argv[2]); release=Path(sys.argv[3]); "
            "calls.open('a').write('success\\n' if ready.exists() else 'failed\\n'); "
            "end=time.monotonic()+2; "
            "ready.exists() and next((None for _ in iter(int,1) if time.sleep(.01) or release.exists() or time.monotonic()>=end),None); "
            "sys.exit(0 if ready.exists() else 7)"
        ),
        str(ready),
        str(calls),
        str(release),
    ]
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[{"type": "local_command", "events": ["checkpoint"], "command": delivery_command}],
        )
    )
    failed = wait_for_delivery(manager, started["job_id"], "failed")
    assert failed is not None
    ready.write_text("ok")

    barrier = threading.Barrier(3)

    def retry() -> None:
        barrier.wait()
        try:
            manager.retry_delivery(failed["delivery_id"])
        except ValueError:
            pass

    threads = [threading.Thread(target=retry) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and "success" not in calls.read_text():
        time.sleep(0.01)
    time.sleep(0.3)
    release.write_text("go")
    delivery = wait_for_delivery(manager, started["job_id"], "delivered")
    time.sleep(0.3)

    assert delivery is not None and delivery["status"] == "delivered"
    assert delivery["attempts"] == 2
    assert calls.read_text().splitlines() == ["failed", "success"]


def test_retry_due_after_manager_restart_is_dispatched(tmp_path):
    home = tmp_path / "state"
    calls = tmp_path / "restart-calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys; "
            "p=Path(sys.argv[1]); calls=p.read_text() if p.exists() else ''; "
            "p.write_text(calls+'x'); sys.exit(7 if not calls else 0)"
        ),
        str(calls),
    ]
    manager = JobManager(home)
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[
                {
                    "type": "local_command",
                    "events": ["checkpoint"],
                    "command": delivery_command,
                    "max_attempts": 2,
                    "retry_delay_seconds": 1,
                }
            ],
        )
    )
    retrying = wait_for_delivery(manager, started["job_id"], "retrying")
    assert retrying is not None
    manager.close()

    restarted = JobManager(home)
    try:
        delivered = wait_for_delivery(restarted, started["job_id"], "delivered")
        assert delivered is not None and delivered["attempts"] == 2
        assert calls.read_text() == "xx"
    finally:
        restarted.close()
