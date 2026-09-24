import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from vanth.client import VanthClient


def test_ensure_explains_a_daemon_using_another_home(tmp_path, monkeypatch):
    class OtherDaemon(BaseHTTPRequestHandler):
        def do_GET(self):
            payload = {"ok": True} if self.path == "/health" else {"result": "error", "error": "unauthorized"}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), OtherDaemon)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = VanthClient(url=f"http://127.0.0.1:{server.server_port}", home=tmp_path)
        monkeypatch.setattr("vanth.client.subprocess.Popen", lambda *args, **kwargs: pytest.fail("started a second daemon"))
        with pytest.raises(RuntimeError, match="VANTH_DAEMON_PORT"):
            client.ensure()
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
