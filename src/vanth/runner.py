from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from pathlib import Path

from .server import JobManager, now_iso


def run(home: str, job_id: str) -> int:
    manager = JobManager(home, recover=False)
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
    )
    with manager.db_lock:
        manager.db.execute("UPDATE jobs SET pid=?, updated_at=? WHERE job_id=?", (proc.pid, now_iso(), job_id))
        manager.db.commit()
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
    timeout_seconds = spec.get("timeout_seconds")
    try:
        exit_code = proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        manager._kill_process(proc, force=True)
        exit_code = proc.wait()
        manager._readers_done(job_id)
        manager._finish(job_id, "timeout", exit_code)
        manager.close()
        return exit_code
    status = manager._row("SELECT status FROM jobs WHERE job_id=?", (job_id,))["status"]
    if status != "cancelled":
        manager._readers_done(job_id)
        manager._finish(job_id, "completed" if exit_code == 0 else "failed", exit_code)
    manager.close()
    return exit_code


def main() -> None:
    raise SystemExit(run(sys.argv[1], sys.argv[2]))


if __name__ == "__main__":
    main()
