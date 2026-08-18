import json
import os
import signal
import socket
import subprocess
import sys
import time

from vanth.migrations import LATEST_SCHEMA_VERSION


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_daemon_writes_and_removes_discovery_metadata(tmp_path):
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env={**os.environ, "VANTH_HOME": str(tmp_path), "VANTH_DAEMON_PORT": str(port)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )
    meta_path = tmp_path / "daemon.json"
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not meta_path.exists():
            time.sleep(0.1)
        assert meta_path.exists(), "daemon.json not written"
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        assert payload["url"] == f"http://127.0.0.1:{port}"
        assert payload["home"] == str(tmp_path.resolve())
        assert isinstance(payload["pid"], int) and payload["pid"] > 0
        assert payload["schema_version"] == LATEST_SCHEMA_VERSION
        assert payload["started_at"]
    finally:
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        proc.wait(timeout=10)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and meta_path.exists():
        time.sleep(0.1)
    assert not meta_path.exists(), "daemon.json not removed on shutdown"
