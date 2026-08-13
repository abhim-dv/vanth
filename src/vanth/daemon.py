from __future__ import annotations

import asyncio
import hmac
import http
import ipaddress
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import signal
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .client import auth_token_path, ensure_auth_token
from .paths import canonical_home
from .server import JobManager


manager: JobManager | None = None
manager_lock = threading.Lock()
shutdown_event = threading.Event()
DEFAULT_MAX_REQUEST_BYTES = 1024 * 1024
DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class RequestTooLarge(ValueError):
    pass


class TrackingHTTPServer(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._active_condition = threading.Condition()
        self._active_requests = 0

    def _request_finished(self) -> None:
        with self._active_condition:
            self._active_requests -= 1
            self._active_condition.notify_all()

    def process_request(self, request: Any, client_address: Any) -> None:
        with self._active_condition:
            self._active_requests += 1
        thread = threading.Thread(target=self.process_request_thread, args=(request, client_address))
        thread.daemon = self.daemon_threads
        try:
            thread.start()
        except BaseException:
            self._request_finished()
            self.close_request(request)
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_finished()

    def wait_for_requests(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._active_condition:
            while self._active_requests:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._active_condition.wait(remaining)
        return True


class DaemonLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+", encoding="utf-8")
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.handle.seek(0)
            self.handle.truncate()
            self.handle.write(str(os.getpid()))
            self.handle.flush()
            return True
        except (BlockingIOError, OSError):
            self.handle.close()
            self.handle = None
            return False

    def release(self) -> None:
        if not self.handle:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def get_manager() -> JobManager:
    global manager
    if manager is None:
        with manager_lock:
            if manager is None:
                manager = JobManager()
    return manager


def _max_response_bytes() -> int:
    try:
        return max(1024, int(os.environ.get("VANTH_MAX_RESPONSE_BYTES", DEFAULT_MAX_RESPONSE_BYTES)))
    except ValueError:
        return DEFAULT_MAX_RESPONSE_BYTES


def ok(handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200) -> None:
    body = json.dumps(payload).encode()
    if len(body) > _max_response_bytes():
        payload = {"result": "error", "error": "Response exceeds configured size limit"}
        body = json.dumps(payload).encode()
        status = 500
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)
    except (BrokenPipeError, ConnectionError):
        pass


def error(handler: BaseHTTPRequestHandler, message: str, status: int = 400) -> None:
    ok(handler, {"result": "error", "error": message[:4096]}, status)


class Handler(BaseHTTPRequestHandler):
    server_version = "vanthd/1"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _authorized(self) -> bool:
        # Keep the cheap liveness probe usable by process supervisors.
        if urllib.parse.urlparse(self.path).path == "/health":
            return True
        supplied = self.headers.get("Authorization", "")
        actual = supplied[7:] if supplied.startswith("Bearer ") else ""
        try:
            expected = Path(auth_token_path()).read_text(encoding="utf-8").strip()
        except OSError:
            expected = ""
        return bool(expected) and hmac.compare_digest(actual, expected)

    def do_GET(self) -> None:
        if shutdown_event.is_set():
            error(self, "Daemon is shutting down", 503)
            return
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        if not self._authorized():
            if parsed.path in {"/jobs", "/view", "/deliveries"} and "limit" in query:
                try:
                    value = int(query["limit"][0])
                    if value < 1 or value > 1000:
                        raise ValueError("limit must be an integer between 1 and 1000")
                except (ValueError, TypeError, OverflowError):
                    error(self, "limit must be an integer between 1 and 1000", 400)
                    return
            error(self, "Unauthorized", 401)
            return
        try:
            if parsed.path == "/health":
                ok(self, {"ok": True})
            elif parsed.path == "/ready":
                report = get_manager().doctor()
                ok(self, report, 200 if report["ok"] else 503)
            elif parsed.path == "/doctor":
                ok(self, get_manager().doctor())
            elif parsed.path == "/view":
                ok(self, get_manager().agent_view(query.get("thread_id", [None])[0], int(query.get("limit", ["50"])[0])))
            elif parsed.path == "/jobs":
                ok(self, get_manager().list(query.get("status") or None, int(query.get("limit", ["50"])[0]), query.get("thread_id", [None])[0]))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/status"):
                ok(self, get_manager().status(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/events"):
                ok(self, get_manager().events(parsed.path.split("/")[2], query.get("since_event_id", [None])[0], query.get("types") or None, int(query.get("limit", ["20"])[0])))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/tail"):
                ok(self, get_manager().tail(parsed.path.split("/")[2], query.get("stream", ["stdout"])[0], int(query.get("max_bytes", ["8192"])[0]), int(query["offset"][0]) if "offset" in query else None))
            elif parsed.path == "/deliveries":
                ok(self, get_manager().deliveries(query.get("job_id", [None])[0], query.get("status", [None])[0], int(query.get("limit", ["20"])[0])))
            elif parsed.path.startswith("/deliveries/") and parsed.path.endswith("/attempts"):
                ok(self, get_manager().delivery_attempts(parsed.path.split("/")[2], int(query.get("limit", ["20"])[0])))
            else:
                error(self, "Not found", 404)
        except (ValueError, TypeError, OverflowError) as exc:
            error(self, str(exc))
        except Exception:
            logging.getLogger("vanth.daemon").exception("GET request failed")
            error(self, "Internal server error", 500)

    def do_POST(self) -> None:
        if shutdown_event.is_set():
            error(self, "Daemon is shutting down", 503)
            return
        try:
            raw_length = self.headers.get("Content-Length")
            if raw_length is None:
                raise ValueError("Content-Length header is required")
            try:
                length = int(raw_length)
            except ValueError as exc:
                raise ValueError("Content-Length must be an integer") from exc
            if length < 0:
                raise ValueError("Content-Length must not be negative")
            try:
                max_request_bytes = int(os.environ.get("VANTH_MAX_REQUEST_BYTES", DEFAULT_MAX_REQUEST_BYTES))
            except ValueError:
                max_request_bytes = DEFAULT_MAX_REQUEST_BYTES
            if length > max_request_bytes:
                raise RequestTooLarge(f"Request body exceeds {max_request_bytes} bytes")
            body = self.rfile.read(length)
            if len(body) != length:
                raise ValueError("Request body is shorter than Content-Length")
            payload = json.loads(body.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("JSON body must be an object")
            if not self._authorized():
                allowed = {"command", "cwd", "name", "env", "timeout_seconds", "notify_on", "wake_targets", "origin_thread_id", "tags"}
                if self.path == "/jobs" and ("command" not in payload or set(payload) - allowed):
                    raise ValueError("invalid job request")
                error(self, "Unauthorized", 401)
                return
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path == "/jobs":
                ok(self, asyncio.run(get_manager().start(**payload)))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/wait"):
                ok(self, get_manager().wait_sync(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/stop"):
                ok(self, get_manager().stop_sync(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/deliveries/") and parsed.path.endswith("/mark"):
                ok(self, get_manager().mark_delivery(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/deliveries/") and parsed.path.endswith("/retry"):
                ok(self, get_manager().retry_delivery(parsed.path.split("/")[2]))
            elif parsed.path == "/cleanup":
                ok(self, get_manager().cleanup(**payload))
            else:
                error(self, "Not found", 404)
        except RequestTooLarge as exc:
            error(self, str(exc), 413)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, TypeError, OverflowError) as exc:
            error(self, str(exc))
        except Exception:
            logging.getLogger("vanth.daemon").exception("POST request failed")
            error(self, "Internal server error", 500)


def _loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _configure_logging(home: Path) -> logging.Logger:
    (home / "logs").mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("vanth.daemon")
    logger.setLevel(os.environ.get("VANTH_LOG_LEVEL", "INFO").upper())
    logger.propagate = False
    handler = RotatingFileHandler(
        home / "logs" / "daemon.log",
        maxBytes=int(os.environ.get("VANTH_LOG_MAX_BYTES", str(5 * 1024 * 1024))),
        backupCount=int(os.environ.get("VANTH_LOG_BACKUP_COUNT", "3")),
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s pid=%(process)d component=daemon %(message)s"))
    logger.addHandler(handler)
    return logger


def main() -> None:
    host = os.environ.get("VANTH_DAEMON_HOST", "127.0.0.1")
    if not _loopback(host):
        raise SystemExit("VANTH_DAEMON_HOST must be localhost or a loopback address")
    try:
        port = int(os.environ.get("VANTH_DAEMON_PORT", "8765"))
    except ValueError as exc:
        raise SystemExit("VANTH_DAEMON_PORT must be an integer") from exc
    home = canonical_home()
    ensure_auth_token(home)
    logger = _configure_logging(home)
    lock = DaemonLock(home / "daemon.lock")
    if not lock.acquire():
        raise SystemExit("another vanthd already owns this VANTH_HOME")
    httpd = TrackingHTTPServer((host, port), Handler)
    shutdown_event.clear()

    def stop(*_: Any) -> None:
        if shutdown_event.is_set():
            return
        shutdown_event.set()
        if manager is not None:
            manager.begin_shutdown()
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    previous = {}
    signal_names = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        signal_names.append(signal.SIGBREAK)
    for name in signal_names:
        try:
            previous[name] = signal.signal(name, stop)
        except ValueError:
            pass
    try:
        httpd.serve_forever()
    finally:
        if manager is not None:
            manager.begin_shutdown()
        httpd.wait_for_requests(float(os.environ.get("VANTH_SHUTDOWN_TIMEOUT", "10")))
        httpd.server_close()
        if manager is not None:
            manager.close()
        lock.release()
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        for name, handler in previous.items():
            try:
                signal.signal(name, handler)
            except ValueError:
                pass


if __name__ == "__main__":
    main()
