import socket
import subprocess
import sys
import time

from vanth.client import VanthClient


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_daemon_http_job_flow(tmp_path):
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env={
            **__import__("os").environ,
            "VANTH_HOME": str(tmp_path / "state"),
            "VANTH_DAEMON_PORT": str(port),
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = VanthClient(f"http://127.0.0.1:{port}", tmp_path / "state")
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                assert client.get("/health") == {"ok": True}
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)

        job = client.post(
            "/jobs",
            {
                "command": subprocess.list2cmdline(
                    [
                        sys.executable,
                        "-c",
                        "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'daemon ok'}), flush=True)",
                    ]
                )
            },
        )
        waited = client.post(f"/jobs/{job['job_id']}/wait", {"filters": ["checkpoint"], "timeout_seconds": 5})
        assert waited["event"]["message"] == "daemon ok"
    finally:
        proc.terminate()
        proc.wait(timeout=5)
