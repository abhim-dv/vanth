"""Prometheus metrics exposition (review B2)."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

from vanth.server import JobManager, now_iso


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_metrics_text_contains_core_series(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.pool_configure("gpu", max_parallel=2)
        with manager.db_lock:
            for job_id, status in (("job_run", "running"), ("job_q", "queued"), ("job_bad", "failed")):
                manager.db.execute(
                    "INSERT INTO jobs(job_id, name, command, status, created_at, updated_at, stdout_path, "
                    "stderr_path, events_path, pool, priority) VALUES (?, ?, 'echo', ?, ?, ?, 'o', 'e', 'ev', 'gpu', 0)",
                    (job_id, job_id, status, now_iso(), now_iso()),
                )
            manager.db.commit()

        text = manager.metrics_text()
        assert "vanth_up 1" in text
        assert 'vanth_jobs{status="running_or_launching"} 1' in text
        assert 'vanth_jobs{status="queued"} 1' in text
        assert "vanth_jobs_total 3" in text
        assert 'vanth_pool_max_parallel{pool="gpu"} 2' in text
        assert 'vanth_pool_running{pool="gpu"} 1' in text
        assert 'vanth_pool_queued{pool="gpu"} 1' in text
        assert "vanth_schema_version " in text
        # Every non-comment line is a valid `name[labels] value`.
        for line in text.splitlines():
            if line and not line.startswith("#"):
                head, value = line.rsplit(" ", 1)
                float(value)
                assert head.startswith("vanth_")
    finally:
        manager.close()


def test_metrics_endpoint_requires_auth(tmp_path):
    port = _free_port()
    home = tmp_path / "state"
    env = {**os.environ, "VANTH_HOME": str(home), "VANTH_DAEMON_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2):
                    break
            except Exception:
                time.sleep(0.05)

        # Unauthorized cannot read metrics.
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5)
            raise AssertionError("metrics should require auth")
        except urllib.error.HTTPError as exc:
            assert exc.code == 401

        token = (home / "token").read_text(encoding="utf-8").strip()
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/metrics",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read().decode("utf-8")
            assert response.status == 200
            assert response.headers.get_content_type() == "text/plain"
        assert "vanth_up 1" in body
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
