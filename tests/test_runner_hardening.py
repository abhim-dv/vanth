import json
from pathlib import Path

import pytest

from vanth.runner import run
from vanth.server import JobManager, now_iso


def seed_job(home: Path, job_id: str, spec: str | None) -> None:
    manager = JobManager(home, recover=False)
    timestamp = now_iso()
    manager.db.execute(
        """
        INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, events_path)
        VALUES (?, 'unused', 'running', ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            timestamp,
            timestamp,
            str(manager.logs / f"{job_id}.stdout.log"),
            str(manager.logs / f"{job_id}.stderr.log"),
            str(manager.events_dir / f"{job_id}.jsonl"),
        ),
    )
    manager.db.commit()
    if spec is not None:
        (manager.specs_dir / f"{job_id}.json").write_text(spec, encoding="utf-8")
    manager.close()


@pytest.mark.parametrize(
    "spec",
    [
        None,
        "{",
        json.dumps({"command": "echo ignored", "env": {"INVALID": None}}),
        json.dumps({"command": "echo ignored", "cwd": "missing-directory"}),
    ],
)
def test_runner_startup_failure_is_terminal(tmp_path, spec):
    job_id = "job_startup_failure"
    seed_job(tmp_path, job_id, spec)

    assert run(str(tmp_path), job_id) == 1

    manager = JobManager(tmp_path, recover=False)
    status = manager.status(job_id)
    events = manager.events(job_id, types=["failed"])["events"]
    assert status["status"] == "failed"
    assert status["exit_code"] == 1
    assert len(events) == 1
    assert events[0]["level"] == "error"
    assert events[0]["source"] == "runner"
    assert events[0]["message"].startswith("Job runner failed to start:")
    assert events[0]["data"]["error"]
    manager.close()
