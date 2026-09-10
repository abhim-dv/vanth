"""Edge-triggered operator alerts (review B3)."""

from __future__ import annotations

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from vanth.server import JobManager, now_iso


class _Sink(BaseHTTPRequestHandler):
    received: list[dict] = []

    def do_POST(self):  # noqa: N802 - stdlib handler API
        length = int(self.headers.get("Content-Length", "0"))
        _Sink.received.append(json.loads(self.rfile.read(length)))
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):
        return


def _start_sink() -> tuple[HTTPServer, int]:
    server = HTTPServer(("127.0.0.1", 0), _Sink)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1]


def test_alerts_edge_triggered_on_disk(tmp_path, monkeypatch):
    server, port = _start_sink()
    _Sink.received = []
    manager = JobManager(tmp_path, recover=False)
    try:
        monkeypatch.setenv("VANTH_ALERT_WEBHOOK", f"http://127.0.0.1:{port}/alert")
        monkeypatch.setenv("VANTH_ALERT_DISK_FREE_BYTES", str(10**18))  # always low

        manager._last_alert_check = None
        manager._check_alerts()
        assert len(_Sink.received) == 1
        assert _Sink.received[0]["condition"] == "disk_low"
        assert _Sink.received[0]["active"] is True

        # Edge-triggered: a second pass with the same state does not re-alert.
        manager._last_alert_check = None
        manager._check_alerts()
        assert len(_Sink.received) == 1

        # Recovery transition alerts once.
        monkeypatch.setenv("VANTH_ALERT_DISK_FREE_BYTES", "0")
        manager._last_alert_check = None
        manager._check_alerts()
        assert len(_Sink.received) == 2
        assert _Sink.received[1]["condition"] == "disk_low"
        assert _Sink.received[1]["active"] is False
    finally:
        manager.close()
        server.shutdown()
        server.server_close()


def test_alert_on_dead_letters(tmp_path, monkeypatch):
    server, port = _start_sink()
    _Sink.received = []
    manager = JobManager(tmp_path, recover=False)
    try:
        monkeypatch.setenv("VANTH_ALERT_WEBHOOK", f"http://127.0.0.1:{port}/alert")
        monkeypatch.setenv("VANTH_ALERT_DISK_FREE_BYTES", "0")
        with manager.db_lock:
            manager.db.execute(
                "INSERT INTO deliveries(delivery_id, event_id, target_id, job_id, target_type, status, "
                "attempts, payload_json, created_at, last_error) VALUES ('del_x','evt_x','tgt_x','job_x',"
                "'webhook','failed',1,'{}',?,'boom')",
                (now_iso(),),
            )
            manager.db.commit()
        manager._last_alert_check = None
        manager._check_alerts()
        assert any(item["condition"] == "dead_letters" and item["active"] for item in _Sink.received)
    finally:
        manager.close()
        server.shutdown()
        server.server_close()


def test_alert_send_failure_does_not_suppress_retry(tmp_path, monkeypatch):
    manager = JobManager(tmp_path, recover=False)
    server = None
    try:
        monkeypatch.setenv("VANTH_ALERT_DISK_FREE_BYTES", str(10**18))
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            dead_port = probe.getsockname()[1]
        monkeypatch.setenv("VANTH_ALERT_WEBHOOK", f"http://127.0.0.1:{dead_port}/alert")
        manager._last_alert_check = None
        manager._check_alerts()
        assert manager._alert_state.get("disk_low") is None, "a failed send must not advance the state"

        server, port = _start_sink()
        _Sink.received = []
        monkeypatch.setenv("VANTH_ALERT_WEBHOOK", f"http://127.0.0.1:{port}/alert")
        manager._last_alert_check = None
        manager._check_alerts()
        assert any(item["condition"] == "disk_low" and item["active"] for item in _Sink.received)
    finally:
        manager.close()
        if server is not None:
            server.shutdown()
            server.server_close()


def test_alerts_disabled_without_webhook(tmp_path, monkeypatch):
    monkeypatch.delenv("VANTH_ALERT_WEBHOOK", raising=False)
    manager = JobManager(tmp_path, recover=False)
    try:
        manager._last_alert_check = None
        manager._check_alerts()  # no destination configured: must be a no-op
        assert manager._alert_state == {}
    finally:
        manager.close()
