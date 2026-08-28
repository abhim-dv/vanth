from __future__ import annotations

import json
import os
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

from .server import JobManager, now_iso


def _fail_start(manager: JobManager, job_id: str, exc: Exception, claim_token: str | None = None) -> int:
    message = f"Job runner failed to start: {exc}"
    try:
        # The row may still be 'launching' (the parent claimed it and the runner
        # is mid-start). A claim-owned transition records the failure from
        # 'launching' OR 'running' so a run that fails before publishing is not
        # left stranded as 'launching' until stale-claim recovery.
        if manager._transition_terminal(job_id, "failed", 1, claim_token=claim_token):
            manager._emit(job_id, "failed", message=message, data={"error": str(exc)}, level="error", source="runner")
    finally:
        manager.close()
    return 1


def _publish_workload(
    manager: JobManager, job_id: str, pid: int, claim_token: str | None = None
) -> bool:
    def publish() -> int:
        with manager.db_lock:
            if claim_token:
                # Claim-owned promotion (review rc32 P1-3): the runner atomically
                # promotes the row it owns from 'launching' to 'running' ONLY if
                # it still holds claim_token. This is what makes the launching->running
                # transition race-free: a fast job can complete and record its
                # terminal state, and a parent can never resurrect a dead runner
                # with an unguarded 'running' write. If the claim was lost
                # (recovery orphaned it, or a newer launch owns the row), the
                # rowcount is 0 and the runner aborts its workload instead of
                # running an untracked process.
                changed = manager.db.execute(
                    "UPDATE jobs SET status='running', pid=?, worker_pid=?, started_at=?, runner_heartbeat_at=?, "
                    "updated_at=?, exit_code=NULL, ended_at=NULL "
                    "WHERE job_id=? AND claim_token=? AND status='launching' AND stop_requested_at IS NULL",
                    (pid, os.getpid(), now_iso(), now_iso(), now_iso(), job_id, claim_token),
                ).rowcount
            else:
                # start()-path jobs are inserted 'running' with worker_pid set
                # by the parent (the Popen pid). Record only the workload PID;
                # worker_pid stays the parent-observed process so _watch_runner's
                # pid guard keeps matching on platforms where the runner's
                # os.getpid() differs from the Popen pid (Windows launcher shim).
                changed = manager.db.execute(
                    "UPDATE jobs SET pid=?, runner_heartbeat_at=?, updated_at=? "
                    "WHERE job_id=? AND status='running' AND stop_requested_at IS NULL",
                    (pid, now_iso(), now_iso(), job_id),
                ).rowcount
            manager.db.commit()
        return changed

    return bool(manager._retry_locked(publish))


def _abort_workload(
    manager: JobManager, job_id: str, proc: subprocess.Popen[bytes], claim_token: str | None = None
) -> int:
    manager._kill_process(proc, force=True)
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        manager._kill_process(proc, force=True)
        proc.wait()
    if claim_token:
        # Record the failed publish if we still own the claim. If recovery
        # already moved the row to a terminal state, this is a guarded no-op.
        manager._transition_terminal(job_id, "failed", 1, claim_token=claim_token)
    manager.close()
    return 1


def _feed_stdin(job_id: str, channel: Path, stdin, feeder_stop: threading.Event) -> None:
    """Forward length-prefixed records from the job's stdin channel to the child."""
    if stdin is None:
        return
    offset = 0
    buffer = bytearray()
    eof_seen = False
    while not eof_seen and not feeder_stop.is_set():
        try:
            if not channel.exists():
                channel.parent.mkdir(parents=True, exist_ok=True)
                time.sleep(0.02)
                continue
            try:
                with channel.open("rb") as f:
                    f.seek(offset)
                    chunk = f.read()
                    offset = f.tell()
            except OSError:
                time.sleep(0.02)
                continue
            if chunk:
                buffer.extend(chunk)
                while len(buffer) >= 8:
                    (length,) = struct.unpack_from("<Q", buffer)
                    if length == 0:
                        eof_seen = True
                        del buffer[:8]
                        break
                    if len(buffer) < 8 + length:
                        break
                    record = bytes(buffer[8:8 + length])
                    del buffer[:8 + length]
                    try:
                        stdin.write(record)
                        stdin.flush()
                    except (BrokenPipeError, OSError):
                        return
            time.sleep(0.02)
        except Exception:
            time.sleep(0.02)
    if not feeder_stop.is_set():
        try:
            stdin.flush()
        except (BrokenPipeError, OSError):
            pass
    try:
        stdin.close()
    except OSError:
        pass


def run(home: str, job_id: str) -> int:
    manager = JobManager(home, recover=False)
    claim_token: str | None = None
    try:
        spec = json.loads((Path(home) / "specs" / f"{job_id}.json").read_text(encoding="utf-8"))
        claim_token = spec.get("claim_token")
        env = os.environ.copy()
        env.update(spec.get("env") or {})
        creationflags = 0
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0)
        proc = subprocess.Popen(
            spec["command"],
            cwd=spec.get("cwd"),
            env=env,
            stdin=subprocess.PIPE if spec.get("interactive") else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            creationflags=creationflags,
            start_new_session=sys.platform != "win32",
        )
    except Exception as exc:
        return _fail_start(manager, job_id, exc, claim_token)
    try:
        published = _publish_workload(manager, job_id, proc.pid, claim_token)
    except Exception as exc:
        manager.logger.exception("workload PID publication failed job_id=%s", job_id)
        return _abort_workload(manager, job_id, proc, claim_token)
    if not published:
        return _abort_workload(manager, job_id, proc, claim_token)
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
    feeder_stop = threading.Event()
    feeder_thread: threading.Thread | None = None
    if proc.stdin is not None:
        feeder_thread = threading.Thread(
            target=_feed_stdin,
            args=(job_id, Path(home) / "stdin" / f"{job_id}.in", proc.stdin, feeder_stop),
            name=f"vanth-stdin-{job_id}",
            daemon=True,
        )
        feeder_thread.start()
    timeout_seconds = spec.get("timeout_seconds")
    try:
        exit_code = proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        feeder_stop.set()
        if feeder_thread:
            feeder_thread.join(timeout=1)
        manager._kill_process(proc, force=True)
        exit_code = proc.wait()
        manager._readers_done(job_id)
        manager._finish(job_id, "timeout", exit_code, claim_token=claim_token)
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=1)
        manager.close()
        return exit_code
    if proc.stdin is not None:
        try:
            proc.stdin.close()
        except (BrokenPipeError, OSError):
            pass
    feeder_stop.set()
    if feeder_thread:
        feeder_thread.join(timeout=1)
    status = manager._row("SELECT status FROM jobs WHERE job_id=?", (job_id,))["status"]
    if status != "cancelled":
        manager._readers_done(job_id)
        manager._finish(job_id, "completed" if exit_code == 0 else "failed", exit_code, claim_token=claim_token)
    heartbeat_stop.set()
    heartbeat_thread.join(timeout=1)
    manager.close()
    return exit_code


def main() -> None:
    raise SystemExit(run(sys.argv[1], sys.argv[2]))


if __name__ == "__main__":
    main()
