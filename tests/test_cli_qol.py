import json
import os
import socket
import subprocess
import sys
import time

import pytest

from vanth.client import VanthClient


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


@pytest.fixture()
def daemon(tmp_path):
    """Start a real daemon on a temp home and yield the client."""
    port = free_port()
    env = {**os.environ, "VANTH_HOME": str(tmp_path / "state"), "VANTH_DAEMON_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = VanthClient(f"http://127.0.0.1:{port}", tmp_path / "state")
    deadline = time.monotonic() + 5
    while True:
        try:
            assert client.get("/health") == {"ok": True}
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)
    yield tmp_path, client, port
    try:
        client.post("/shutdown", {})
    except Exception:
        pass
    proc.wait(timeout=10)


def run_cli(home, *args, port=None):
    """Run `vanth <args>` as a subprocess against a specific home."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = {**os.environ, "VANTH_HOME": str(home)}
    if port:
        env["VANTH_DAEMON_PORT"] = str(port)
    return subprocess.run(
        [sys.executable, "-c", f"import sys; sys.path.insert(0,{root!r}); from vanth.cli import main; sys.exit(main())", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def wait_status(client, job_id, statuses, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payload = client.get(f"/jobs/{job_id}/status")
        if payload.get("status") in statuses:
            return payload.get("status")
        time.sleep(0.1)
    raise AssertionError(f"job {job_id} never reached {statuses}: {payload}")


def test_version_flag(daemon):
    result = run_cli(daemon[0] / "state", "--version")
    assert result.returncode == 0
    assert result.stdout.strip()


def test_version_subcommand(daemon):
    result = run_cli(daemon[0] / "state", "version")
    assert result.returncode == 0
    assert result.stdout.strip()


def test_list_shows_running_job(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    job_id = started["job_id"]
    result = run_cli(tmp_path / "state", "list", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "STATUS" in result.stdout
    assert job_id in result.stdout
    assert "running" in result.stdout
    try:
        client.post(f"/jobs/{job_id}/stop", {})
    except Exception:
        pass


def test_list_ps_alias(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    result = run_cli(tmp_path / "state", "ps", port=port)
    assert result.returncode == 0
    assert started["job_id"] in result.stdout
    try:
        client.post(f"/jobs/{started['job_id']}/stop", {})
    except Exception:
        pass


def test_list_json(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    result = run_cli(tmp_path / "state", "list", "--json", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    jobs = json.loads(result.stdout)
    assert isinstance(jobs, list)
    assert any(job["job_id"] == started["job_id"] for job in jobs)
    try:
        client.post(f"/jobs/{started['job_id']}/stop", {})
    except Exception:
        pass


def test_list_status_filter(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    result = run_cli(tmp_path / "state", "list", "--status", "queued", port=port)
    assert result.returncode == 0
    assert started["job_id"] not in result.stdout
    try:
        client.post(f"/jobs/{started['job_id']}/stop", {})
    except Exception:
        pass


def test_logs_shows_output(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("print('hello from job', flush=True); import time; time.sleep(0.2)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    result = run_cli(tmp_path / "state", "logs", job_id, port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "hello from job" in result.stdout


def test_logs_tail_alias(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("print('aliased tail', flush=True)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    result = run_cli(tmp_path / "state", "tail", job_id, port=port)
    assert result.returncode == 0
    assert "aliased tail" in result.stdout


def test_logs_unknown_job(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "logs", "job_nope", port=port)
    assert result.returncode == 1
    assert "unknown job" in result.stderr.lower()


def test_logs_stream_all(daemon):
    tmp_path, client, port = daemon
    code = "import sys; print('OUT-LINE', flush=True); print('ERR-LINE', file=sys.stderr, flush=True)"
    started = client.post("/jobs", {"command": cmd(code)})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    result = run_cli(tmp_path / "state", "logs", job_id, "--stream", "all", port=port)
    assert result.returncode == 0
    assert "OUT-LINE" in result.stdout
    assert "ERR-LINE" in result.stdout


def test_stop_job(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    job_id = started["job_id"]
    result = run_cli(tmp_path / "state", "stop", job_id, port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"requested stop for {job_id}" in result.stdout
    status = wait_status(client, job_id, ["cancelled", "stopped", "completed"])
    assert status == "cancelled"


def test_stop_json(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    result = run_cli(tmp_path / "state", "stop", started["job_id"], "--json", port=port)
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload.get("status") in {"cancelled", "running", "stopped"}
    try:
        client.post(f"/jobs/{started['job_id']}/stop", {})
    except Exception:
        pass


def test_artifacts(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    client.post(f"/jobs/{job_id}/artifacts", {"name": "a", "uri": "file:///tmp/x"})
    result = run_cli(tmp_path / "state", "artifacts", job_id, port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "a" in result.stdout
    assert "file:///tmp/x" in result.stdout


def test_artifacts_json(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    client.post(f"/jobs/{job_id}/artifacts", {"name": "b", "uri": "file:///tmp/y"})
    result = run_cli(tmp_path / "state", "artifacts", job_id, "--json", port=port)
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert any(artifact["name"] == "b" for artifact in payload["artifacts"])


def test_artifacts_unknown_job(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "artifacts", "job_nope", port=port)
    assert result.returncode == 1
    assert "unknown job" in result.stderr.lower()


def test_prune_dry_run(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    wait_status(client, started["job_id"], ["completed"])
    result = run_cli(tmp_path / "state", "prune", "--dry-run", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "would remove" in result.stdout
    assert started["job_id"] in result.stdout


def test_prune_dry_run_json(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    wait_status(client, started["job_id"], ["completed"])
    result = run_cli(tmp_path / "state", "prune", "--dry-run", "--json", port=port)
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload.get("count", 0) >= 1
    assert payload.get("dry_run") is True


def test_prune_yes(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    wait_status(client, started["job_id"], ["completed"])
    result = run_cli(tmp_path / "state", "prune", "--yes", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "removed" in result.stdout
    after = client.get(f"/jobs/{started['job_id']}/status")
    assert after.get("result") == "error"


def test_prune_defaults_to_dry_run(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    wait_status(client, started["job_id"], ["completed"])
    result = run_cli(tmp_path / "state", "prune", "--yes", port=port)
    assert result.returncode == 0
    client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    second = client.get("/jobs", {"limit": 50})["jobs"][-1]
    wait_status(client, second["job_id"], ["completed"])
    result = run_cli(tmp_path / "state", "prune", port=port)
    assert result.returncode == 0
    assert "would remove" in result.stdout
