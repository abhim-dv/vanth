from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from pathlib import Path

from .server import JobManager, now_iso


def _fail_start(manager: JobManager, job_id: str, exc: Exception) -> int:
    message = f"Job runner failed to start: {exc}"
    try:
        if manager._transition_terminal(job_id, "failed", 1):
            manager._emit(job_id, "failed", message=message, data={"error": str(exc)}, level="error", source="runner")
    finally:
        manager.close()
    return 1


def _publish_workload(manager: JobManager, job_id: str, pid: int) -> bool:
    def publish() -> int:
        with manager.db_lock:
            changed = manager.db.execute(
                "UPDATE jobs SET pid=?, runner_heartbeat_at=?, updated_at=? WHERE job_id=? AND status='running' AND stop_requested_at IS NULL",
                (pid, now_iso(), now_iso(), job_id),
            ).rowcount
            manager.db.commit()
        return changed

    return bool(manager._retry_locked(publish))


def _abort_workload(manager: JobManager, job_id: str, proc: subprocess.Popen[bytes]) -> int:
    manager._kill_process(proc, force=True)
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        manager._kill_process(proc, force=True)
        proc.wait()
    manager.close()
    return 1


def run(home: str, job_id: str) -> int:
    manager = JobManager(home, recover=False)
    try:
        spec = json.loads((Path(home) / "specs" / f"{job_id}.json").read_text(encoding="utf-8"))
        env = os.environ.copy()
        env.update(spec.get("env") or {})
        creationflags = 0
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0)
        proc = subprocess.Popen(
            spec["command"],
            cwd=spec.get("cwd"),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            creationflags=creationflags,
            start_new_session=sys.platform != "win32",
        )
    except Exception as exc:
        return _fail_start(manager, job_id, exc)
    try:
        published = _publish_workload(manager, job_id, proc.pid)
    except Exception as exc:
        manager.logger.exception("workload PID publication failed job_id=%s", job_id)
        return _abort_workload(manager, job_id, proc)
    if not published:
        return _abort_workload(manager, job_id, proc)
    try:
        (Path(home) / "specs" / f"{job_id}.json").unlink()
    except FileNotFoundError:
        pass
    manager._emit(job_id, "started")
    manager.processes[job_id] = proc
    manager.reader_threads[job_id] = [
        threading.Thread(
            target=manager._read_stream,
            args=(job_id, proc.stdout, Path(spec["stdout_path"]), "stdout"),
            daemon=True,
        ),
        threading.Thread(
            target=manager._read_stream,
            args=(job_id, proc.stderr, Path(spec["stderr_path"]), "stderr"),
            daemon=True,
        ),
    ]
    for thread in manager.reader_threads[job_id]:
        thread.start()
    heartbeat_stop = threading.Event()

    def heartbeat() -> None:
        def beat() -> None:
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET runner_heartbeat_at=?, updated_at=? WHERE job_id=? AND status='running'",
                    (now_iso(), now_iso(), job_id),
                )
                manager.db.commit()

        while not heartbeat_stop.wait(manager.heartbeat_interval):
            try:
                manager._retry_locked(beat)
            except Exception:
                manager.logger.exception("runner heartbeat update failed job_id=%s", job_id)

    heartbeat_thread = threading.Thread(target=heartbeat, name=f"vanth-heartbeat-{job_id}", daemon=True)
    heartbeat_thread.start()
    timeout_seconds = spec.get("timeout_seconds")
    try:
        exit_code = proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        manager._kill_process(proc, force=True)
        exit_code = proc.wait()
        manager._readers_done(job_id)
        manager._finish(job_id, "timeout", exit_code)
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=1)
        manager.close()
        return exit_code
    status = manager._row("SELECT status FROM jobs WHERE job_id=?", (job_id,))["status"]
    if status != "cancelled":
        manager._readers_done(job_id)
        manager._finish(job_id, "completed" if exit_code == 0 else "failed", exit_code)
    heartbeat_stop.set()
    heartbeat_thread.join(timeout=1)
    manager.close()
    return exit_code


def main() -> None:
    raise SystemExit(run(sys.argv[1], sys.argv[2]))


if __name__ == "__main__":
    main()
