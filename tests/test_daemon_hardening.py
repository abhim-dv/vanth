import concurrent.futures
import http.client
import json
import os
import socket
import signal
import subprocess
import sys
import threading
import time

import vanth.daemon as daemon


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start_daemon(tmp_path, max_request_bytes=1024 * 1024):
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env={
            **os.environ,
            "VANTH_HOME": str(tmp_path / "state"),
            "VANTH_DAEMON_PORT": str(port),
            "VANTH_MAX_REQUEST_BYTES": str(max_request_bytes),
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            status, payload = request(port, "GET", "/health")
            if status == 200 and payload == {"ok": True}:
                return proc, port
        except OSError:
            time.sleep(0.05)
    proc.terminate()
    raise AssertionError("daemon did not start")


def request(port, method, path, body=None, headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    connection.request(method, path, body=body, headers=headers or {})
    response = connection.getresponse()
    payload = json.loads(response.read())
    connection.close()
    return response.status, payload


def stop_daemon(proc):
    proc.terminate()
    proc.wait(timeout=5)
    return proc.stderr.read().decode(errors="replace")


def test_malformed_post_bodies_return_json_errors_and_daemon_stays_healthy(tmp_path):
    proc, port = start_daemon(tmp_path)
    try:
        cases = [
            (b'{"command":', {"Content-Length": "11"}),
            (b"\xff\xfe", {"Content-Length": "2"}),
            (b"[]", {"Content-Length": "2"}),
        ]
        for body, headers in cases:
            status, payload = request(port, "POST", "/jobs", body, headers)
            assert status == 400
            assert payload["result"] == "error"

        status, payload = request(
            port,
            "POST",
            "/jobs",
            b'{"command":"echo ok","bogus":1}',
            {"Content-Type": "application/json"},
        )
        assert status == 400
        assert payload["result"] == "error"
        assert request(port, "GET", "/health") == (200, {"ok": True})
    finally:
        stderr = stop_daemon(proc)
    assert "Traceback" not in stderr


def test_content_length_validation_and_body_cap(tmp_path):
    proc, port = start_daemon(tmp_path, max_request_bytes=16)
    try:
        for raw_length in (None, "wat", "-1"):
            sock = socket.create_connection(("127.0.0.1", port), timeout=5)
            header = "" if raw_length is None else f"Content-Length: {raw_length}\r\n"
            sock.sendall(f"POST /jobs HTTP/1.1\r\nHost: localhost\r\n{header}Connection: close\r\n\r\n".encode())
            response = sock.makefile("rb").read()
            sock.close()
            assert b" 400 " in response
            assert b'"result": "error"' in response

        status, payload = request(port, "POST", "/jobs", b"x" * 17, {"Content-Length": "17"})
        assert status == 413
        assert payload["result"] == "error"

        sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        sock.sendall(b"POST /jobs HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\nConnection: close\r\n\r\n{}")
        sock.shutdown(socket.SHUT_WR)
        response = sock.makefile("rb").read()
        sock.close()
        assert b" 400 " in response
        assert b"shorter than Content-Length" in response
    finally:
        stderr = stop_daemon(proc)
    assert "Traceback" not in stderr


def test_route_conversion_errors_return_json_instead_of_reset(tmp_path):
    proc, port = start_daemon(tmp_path)
    try:
        status, payload = request(port, "GET", "/jobs?limit=" + "9" * 100)
        assert status == 400
        assert payload["result"] == "error"
    finally:
        stderr = stop_daemon(proc)
    assert "Traceback" not in stderr


def test_concurrent_first_requests_initialize_one_manager(monkeypatch):
    created = []

    def make_manager():
        time.sleep(0.02)
        created.append(object())
        return created[0]

    monkeypatch.setattr(daemon, "manager", None)
    monkeypatch.setattr(daemon, "JobManager", make_manager)
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        managers = list(executor.map(lambda _: daemon.get_manager(), range(20)))

    assert len(created) == 1
    assert all(manager is created[0] for manager in managers)


def test_authenticated_routes_and_home_token_isolation(tmp_path):
    proc, port = start_daemon(tmp_path)
    try:
        token = (tmp_path / "state" / "token").read_text(encoding="utf-8")
        assert request(port, "GET", "/jobs")[0] == 401
        assert request(port, "GET", "/jobs", headers={"Authorization": "Bearer wrong"})[0] == 401
        status, payload = request(port, "GET", "/jobs", headers={"Authorization": f"Bearer {token}"})
        assert status == 200 and payload == {"jobs": []}
    finally:
        stop_daemon(proc)


def test_agent_bg_home_alias_cannot_bypass_daemon_lock(tmp_path):
    home = tmp_path / "state"
    first, port = start_daemon(tmp_path)
    second_port = free_port()
    env = {key: value for key, value in os.environ.items() if key not in {"VANTH_HOME", "AGENT_BG_HOME"}}
    env.update({"AGENT_BG_HOME": str(home), "VANTH_DAEMON_PORT": str(second_port)})
    second = subprocess.Popen([sys.executable, "-m", "vanth.daemon"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        assert second.wait(timeout=5) != 0
        assert "already owns" in second.stderr.read().decode(errors="replace")
    finally:
        if first.poll() is None:
            stop_daemon(first)


def test_tracking_server_counts_request_before_thread_entry():
    server = daemon.TrackingHTTPServer(("127.0.0.1", 0), daemon.Handler)
    entered = threading.Event()
    release = threading.Event()

    def paused_finish(request, client_address):
        entered.set()
        release.wait(timeout=2)

    server.finish_request = paused_finish
    request_socket = socket.socket()
    try:
        server.process_request(request_socket, ("127.0.0.1", 0))
        assert entered.wait(timeout=1)
        assert not server.wait_for_requests(0.05)
        release.set()
        assert server.wait_for_requests(2)
    finally:
        request_socket.close()
        server.server_close()


def test_shutdown_returns_controlled_result_to_active_wait(tmp_path):
    proc, port = start_daemon(tmp_path)
    token = (tmp_path / "state" / "token").read_text(encoding="utf-8")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        command = subprocess.list2cmdline([sys.executable, "-c", "import time; time.sleep(30)"])
        status, started = request(port, "POST", "/jobs", json.dumps({"command": command}).encode(), headers)
        assert status == 200
        result = []

        def wait_for_job():
            result.append(request(port, "POST", f"/jobs/{started['job_id']}/wait", json.dumps({"filters": ["completed"], "timeout_seconds": 30}).encode(), headers))

        thread = threading.Thread(target=wait_for_job)
        thread.start()
        time.sleep(0.2)
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        proc.wait(timeout=5)
        thread.join(timeout=5)
        assert result and result[0][1]["result"] == "shutdown"
    finally:
        if proc.poll() is None:
            stop_daemon(proc)
