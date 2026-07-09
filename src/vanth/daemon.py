from __future__ import annotations

import asyncio
import json
import os
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .server import JobManager


manager: JobManager | None = None


def get_manager() -> JobManager:
    global manager
    if manager is None:
        manager = JobManager()
    return manager


def ok(handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200) -> None:
    body = json.dumps(payload).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def error(handler: BaseHTTPRequestHandler, message: str, status: int = 400) -> None:
    ok(handler, {"result": "error", "error": message}, status)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        try:
            if parsed.path == "/health":
                ok(self, {"ok": True})
            elif parsed.path == "/jobs":
                statuses = query.get("status") or None
                ok(self, get_manager().list(statuses, int(query.get("limit", ["50"])[0])))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/status"):
                ok(self, get_manager().status(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/events"):
                ok(
                    self,
                    get_manager().events(
                        parsed.path.split("/")[2],
                        query.get("since_event_id", [None])[0],
                        query.get("types") or None,
                        int(query.get("limit", ["20"])[0]),
                    ),
                )
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/tail"):
                ok(
                    self,
                    get_manager().tail(
                        parsed.path.split("/")[2],
                        query.get("stream", ["stdout"])[0],
                        int(query.get("max_bytes", ["8192"])[0]),
                    ),
                )
            elif parsed.path == "/deliveries":
                ok(
                    self,
                    get_manager().deliveries(
                        query.get("job_id", [None])[0],
                        query.get("status", [None])[0],
                        int(query.get("limit", ["20"])[0]),
                    ),
                )
            else:
                error(self, "Not found", 404)
        except ValueError as exc:
            error(self, str(exc))

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length).decode() or "{}")
        parsed = urllib.parse.urlparse(self.path)
        try:
            if parsed.path == "/jobs":
                ok(self, asyncio.run(get_manager().start(**payload)))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/wait"):
                ok(self, get_manager().wait_sync(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/stop"):
                ok(self, get_manager().stop_sync(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/deliveries/") and parsed.path.endswith("/mark"):
                ok(self, get_manager().mark_delivery(parsed.path.split("/")[2], **payload))
            else:
                error(self, "Not found", 404)
        except ValueError as exc:
            error(self, str(exc))


def main() -> None:
    host = os.environ.get("VANTH_DAEMON_HOST", "127.0.0.1")
    port = int(os.environ.get("VANTH_DAEMON_PORT", "8765"))
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
