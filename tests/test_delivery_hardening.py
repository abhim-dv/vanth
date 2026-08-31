import asyncio
import json
import os
import subprocess
import sys
import threading
import time

import pytest

from vanth.opencode_bridge import OpenCodeSessionNotFound
from vanth.server import JobManager, now_iso


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


def test_notify_on_defaults_events_for_targets_without_events(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    calls = tmp_path / "notify_calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys; "
            "p=Path(sys.argv[1]); p.write_text(p.read_text()+'x') if p.exists() else p.write_text('x')"
        ),
        str(calls),
    ]
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            notify_on=["checkpoint"],
            wake_targets=[{"type": "local_command", "command": delivery_command}],
        )
    )
    delivery = wait_for_delivery(manager, started["job_id"], "delivered")
    assert delivery is not None and delivery["status"] == "delivered"
    assert calls.read_text() == "x"


def test_explicit_target_events_override_notify_on(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    calls = tmp_path / "override_calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys; "
            "p=Path(sys.argv[1]); p.write_text(p.read_text()+'x') if p.exists() else p.write_text('x')"
        ),
        str(calls),
    ]
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            notify_on=["completed"],
            wake_targets=[
                {
                    "type": "local_command",
                    "command": delivery_command,
                    "events": ["checkpoint"],
                }
            ],
        )
    )
    delivery = wait_for_delivery(manager, started["job_id"], "delivered")
    assert delivery is not None and delivery["status"] == "delivered"
    assert calls.read_text() == "x"


def test_notify_on_alone_without_targets_still_stores_value(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    started = asyncio.run(
        manager.start(cmd("import time; time.sleep(1)"), notify_on=["completed"])
    )
    assert json.loads(
        manager._row("SELECT notify_on FROM jobs WHERE job_id=?", (started["job_id"],))["notify_on"]
    ) == ["completed"]


def test_retry_delivery_force_advances_retrying_delivery(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    calls = tmp_path / "retry_calls.txt"
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
                    "retry_delay_seconds": 60,
                }
            ],
        )
    )
    retrying = wait_for_delivery(manager, started["job_id"], "retrying")
    assert retrying is not None
    assert retrying["next_attempt_at"] is not None
    manager.retry_delivery(retrying["delivery_id"])
    delivered = wait_for_delivery(manager, started["job_id"], "delivered")
    assert delivered is not None and delivered["status"] == "delivered"
    assert delivered["attempts"] == 2
    assert calls.read_text() == "xx"


def test_delivery_dispatch_respects_concurrency_cap(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_DELIVERY_MAX_CONCURRENT", "2")

    def active_threads() -> int:
        with manager._delivery_threads_lock:
            return len(manager._delivery_threads)

    def drain() -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and active_threads() > 0:
            time.sleep(0.05)

    manager = JobManager(tmp_path / "state", recover=False)
    started = asyncio.run(manager.start(cmd("import time; time.sleep(30)")))
    try:
        for _ in range(8):
            manager._insert_wake_targets(
                started["job_id"],
                [{"type": "local_command", "events": ["checkpoint"],
                  "command": [sys.executable, "-c", "import time; time.sleep(0.5)"], "timeout_seconds": 30}],
                now_iso(),
            )
        with manager.db_lock:
            manager.db.commit()
        manager._emit(started["job_id"], "checkpoint", message="burst")
        pending = manager.deliveries(started["job_id"], status="pending")["deliveries"]
        assert len(pending) == 8

        manager._dispatch_due_deliveries()
        assert active_threads() == 2
        drain()
        manager._dispatch_due_deliveries()
        assert active_threads() == 2
        drain()
        manager._dispatch_due_deliveries()
        assert active_threads() == 2
        drain()
        manager._dispatch_due_deliveries()
        assert active_threads() == 2
        drain()

        all_deliveries = manager.deliveries(started["job_id"])["deliveries"]
        assert len(all_deliveries) == 8
        assert all(delivery["status"] == "delivered" for delivery in all_deliveries)
    finally:
        manager.close()


def test_doctor_reports_dead_lettered_deliveries(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[
                {
                    "type": "local_command",
                    "events": ["checkpoint"],
                    "command": [sys.executable, "-c", "import sys; sys.exit(7)"],
                    "max_attempts": 2,
                    "retry_delay_seconds": 1,
                }
            ],
        )
    )
    failed = wait_for_delivery(manager, started["job_id"], "failed")
    assert failed is not None
    assert failed["attempts"] == 2
    doctor = manager.doctor()
    assert doctor["dead_letter_count"] >= 1
    entry = next(item for item in doctor["dead_lettered"] if item["delivery_id"] == failed["delivery_id"])
    assert entry["attempts"] == 2


def test_stale_opencode_session_skips_retries(tmp_path, request, monkeypatch):
    import vanth.server as server_module

    def raise_not_found(payload):
        raise OpenCodeSessionNotFound("opencode session not found: ses_x")

    monkeypatch.setattr(server_module, "send_delivery_to_opencode", raise_not_found)
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[
                {
                    "type": "opencode_thread",
                    "session_id": "ses_x",
                    "attach": "http://127.0.0.1:4096",
                    "events": ["checkpoint"],
                    "max_attempts": 3,
                    "retry_delay_seconds": 1,
                }
            ],
        )
    )
    failed = wait_for_delivery(manager, started["job_id"], "failed")
    assert failed is not None
    assert failed["status"] == "failed"
    assert failed["attempts"] == 1
    assert "session not found" in failed["last_error"]
