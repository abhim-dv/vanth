import json
import os
import socket
import subprocess
import sys
import time

import pytest

from vanth.client import VanthClient
from vanth.migrations import LATEST_SCHEMA_VERSION


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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


def test_shutdown_route_gracefully_stops_daemon(daemon):
    tmp_path, client, port = daemon
    resp = client.post("/shutdown", {})
    assert resp["result"] == "shutting_down"
    # daemon.json should be removed on graceful shutdown
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and (tmp_path / "state" / "daemon.json").exists():
        time.sleep(0.1)
    assert not (tmp_path / "state" / "daemon.json").exists()


def test_status_reports_up(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "status", port=port)
    assert result.returncode == 0
    assert "UP" in result.stdout
    assert f"schema:   {LATEST_SCHEMA_VERSION}" in result.stdout


def test_status_json(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "status", "--json", port=port)
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["up"] is True
    assert payload["daemon_schema_version"] == LATEST_SCHEMA_VERSION


def test_status_down_when_no_daemon(tmp_path):
    result = run_cli(tmp_path, "status")
    assert result.returncode == 1
    assert "DOWN" in result.stdout


def test_doctor_ok(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "doctor", port=port)
    assert result.returncode == 0
    assert "OK" in result.stdout


def test_restart_starts_fresh_daemon(tmp_path):
    """Restart a running daemon and confirm a fresh process serves."""
    state = tmp_path / "state"
    port = free_port()
    env = {**os.environ, "VANTH_HOME": str(state), "VANTH_DAEMON_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = VanthClient(f"http://127.0.0.1:{port}", state)
    deadline = time.monotonic() + 5
    while True:
        try:
            if client.get("/health") == {"ok": True}:
                break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)
    try:
        result = run_cli(state, "restart", port=port)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "daemon" in result.stdout
        # A new daemon must be serving on the same port.
        deadline = time.monotonic() + 10
        while True:
            try:
                if client.get("/health") == {"ok": True}:
                    break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        disc = json.loads((state / "daemon.json").read_text(encoding="utf-8"))
        assert disc["schema_version"] == LATEST_SCHEMA_VERSION
        assert disc["pid"] is not None
    finally:
        try:
            client.post("/shutdown", {})
        except Exception:
            pass
        proc.wait(timeout=10)
