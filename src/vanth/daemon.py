from __future__ import annotations

import asyncio
import hmac
import http
import ipaddress
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import secrets
import signal
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .client import auth_token_path, ensure_auth_token
from .paths import canonical_home, secure_home_permissions
from .server import TERMINAL_STATUSES, JobManager


manager: JobManager | None = None
manager_lock = threading.Lock()
shutdown_event = threading.Event()
_httpd: "TrackingHTTPServer | None" = None
_remote_store = None
_remote_job_mgr = None
_remote_job_mgr_lock = threading.Lock()
DEFAULT_MAX_REQUEST_BYTES = 1024 * 1024
DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024


def get_remote_store():
    """Open (once) the controller-side RemoteStore on the shared home dir."""
    global _remote_store
    with manager_lock:
        if _remote_store is None:
            import sqlite3

            db = sqlite3.connect(canonical_home() / "remote.sqlite")
            db.row_factory = sqlite3.Row
            from .migrations import configure_connection
            from .remote.store import RemoteStore

            configure_connection(db)
            _remote_store = RemoteStore(db)
    return _remote_store


def get_remote_control():
    """Controller-side RemoteControl bound to the shared store (job routing)."""
    from .remote.control import RemoteControl

    return RemoteControl(get_remote_store())


_artifacts_ops = None


def get_artifacts():
    """Open (once) the artifact catalog, blob store, and operations engine."""
    global _artifacts_ops
    with manager_lock:
        if _artifacts_ops is None:
            from .artifacts.catalog import open_catalog
            from .artifacts.local_store import LocalBlobStore, default_store_root
            from .artifacts.operations import ArtifactOperations

            home = canonical_home()
            catalog = open_catalog(home)
            blobs = LocalBlobStore(default_store_root(home), catalog)
            _artifacts_ops = ArtifactOperations(catalog, blobs)
    return _artifacts_ops


_artifact_collections = None


def get_artifact_collections():
    """Open (once) the Phase 7 collections/aliases/lineage engine."""
    global _artifact_collections
    with manager_lock:
        if _artifact_collections is None:
            from .artifacts.collections import Collections

            _artifact_collections = Collections(get_artifacts().catalog, get_artifacts())
    return _artifact_collections


_artifact_lifecycle = None


def get_artifact_lifecycle():
    """Open (once) the Phase 7 lifecycle engine (delete/pin/GC/backup-restore)."""
    global _artifact_lifecycle
    with manager_lock:
        if _artifact_lifecycle is None:
            from .artifacts.lifecycle import Lifecycle

            ops = get_artifacts()
            _artifact_lifecycle = Lifecycle(ops.catalog, ops)
    return _artifact_lifecycle


def _remote_epoch(remote_id: str):
    """Best-effort expected state epoch for a remote (None when not yet seen)."""
    from .remote.ssh import VanthRemoteError

    try:
        return get_remote_store().get_remote(remote_id).get("state_epoch")
    except (ValueError, VanthRemoteError):
        return None


def _remote_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Translate a local job payload to a remote protocol payload."""
    return {
        key: value
        for key, value in payload.items()
        if key in {
            "command", "cwd", "name", "env", "timeout_seconds", "notify_on",
            "wake_targets", "origin_thread_id", "tags", "notes", "interactive",
            "trigger", "signal", "kill_after_seconds", "overrides",
        }
    }


def _remote_submit(remote_id: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Route a remote mutation through RemoteControl with a caller-supplied key."""
    control = get_remote_control()
    key = payload.pop("idempotency_key", None)
    if key is None:
        key = "ctr-" + secrets.token_hex(16)[:12]
    expected = _remote_epoch(remote_id)
    if method == "job.start":
        request = control.submit(remote_id, method, payload, idempotency_key=key, expected_state_epoch=expected)
        if request["status"] != "creating":
            return request
        return control.run_request(remote_id, request, expected_state_epoch=expected)
    if method == "job.stop":
        return control.stop(
            remote_id, payload["job_id"],
            signal=payload.get("signal", "terminate"),
            kill_after_seconds=payload.get("kill_after_seconds", 10),
            idempotency_key=key, expected_state_epoch=expected,
        )
    if method == "job.rerun":
        overrides = {k: v for k, v in payload.items() if k != "job_id"}
        return control.rerun(remote_id, payload["job_id"], overrides, idempotency_key=key, expected_state_epoch=expected)
    return control.submit(remote_id, method, payload, idempotency_key=key, expected_state_epoch=expected)


def _remote_wait(remote_id: str, remote_job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Wait on a remote job by polling RemoteControl.status every 0.2s.

    A real cross-machine event push is Phase 4; until then wait is a bounded
    poll of the remote's ``job.status`` method. Returns when the remote reports
    a terminal status or the timeout elapses.
    """
    timeout_seconds = float(payload.get("timeout_seconds", 3600))
    filters = payload.get("filters") or ["completed", "failed", "timeout", "cancelled", "orphaned"]
    deadline = time.monotonic() + timeout_seconds
    control = get_remote_control()
    while True:
        try:
            result = control.status(
                remote_id, remote_job_id,
                idempotency_key="wait-" + secrets.token_hex(16)[:12],
                expected_state_epoch=_remote_epoch(remote_id),
            )
        except Exception:
            result = {}
        status = None
        if isinstance(result, dict):
            response = result.get("response") or {}
            if isinstance(response, dict):
                status = response.get("status")
        if status and status in TERMINAL_STATUSES:
            return {"result": "status", "job_id": remote_job_id, "status": status, "response": result}
        if time.monotonic() >= deadline:
            return {"result": "timeout", "job_id": remote_job_id, "status": status, "message": "No terminal status before timeout"}
        time.sleep(min(0.2, deadline - time.monotonic()))


def _remote_job_manager():
    """Remote daemon-side RemoteJobManager singleton (POST /remote/helper).

    On a real remote host the daemon's ``jobs.sqlite`` is its local job store;
    the remote operation tables are created alongside it on the same connection
    so the acceptance transaction (operation + queued job + origin mapping +
    launch intent) commits atomically with the job row the dispatcher launches.
    """
    global _remote_job_mgr
    with _remote_job_mgr_lock:
        if _remote_job_mgr is None:
            import sqlite3
            from pathlib import Path

            from .migrations import configure_connection
            from .remote.remote import RemoteJobManager
            from .remote.store import RemoteOperationStore

            home = canonical_home()
            db = sqlite3.connect(home / "jobs.sqlite", check_same_thread=False)
            db.row_factory = sqlite3.Row
            configure_connection(db)
            _remote_job_mgr = RemoteJobManager(RemoteOperationStore(db), get_manager(), home=home)
            _remote_job_mgr.start()
    return _remote_job_mgr


class RequestTooLarge(ValueError):
    pass


class TrackingHTTPServer(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = False
    # Windows SO_REUSEADDR lets a second process bind the same loopback port
    # silently, producing a phantom listener that never receives traffic and
    # never exits cleanly. Disable it so a second daemon's bind fails loudly
    # instead (the OS home lock is the real guard anyway).
    allow_reuse_address = os.name != "nt"

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


def _set_httpd(server: "TrackingHTTPServer") -> None:
    global _httpd
    _httpd = server


def _stop_httpd(*args: Any) -> None:
    """Gracefully stop the HTTP server from a background thread.

    Used by the signal handler (which passes ``(signum, frame)`` on Unix) and
    the authenticated ``/shutdown`` route (no args). The server thread unwinds
    through ``main``'s finally block, which closes the manager, releases the
    daemon lock, and removes discovery metadata.
    """
    if shutdown_event.is_set():
        return
    shutdown_event.set()
    if manager is not None:
        manager.begin_shutdown()
    global _remote_job_mgr
    if _remote_job_mgr is not None:
        _remote_job_mgr.stop()
    server = _httpd
    if server is not None:
        threading.Thread(target=server.shutdown, daemon=True).start()


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
            elif parsed.path == "/remotes":
                from .remote.pairing import list_remotes

                ok(self, {"remotes": list_remotes(store=get_remote_store())})
            elif parsed.path == "/remotes/doctor":
                from .remote.pairing import doctor_remote

                ok(self, doctor_remote(remote_id=query.get("remote_id", [None])[0], store=get_remote_store()))
            elif parsed.path.startswith("/remotes/") and parsed.path.endswith("/jobs"):
                from .remote.readapi import projected_jobs

                ok(self, projected_jobs(get_manager(), get_remote_store(), parsed.path.split("/")[2],
                                        limit=int(query.get("limit", ["50"])[0])))
            elif parsed.path.startswith("/remotes/") and "/status/" in parsed.path:
                from .remote.readapi import projected_status

                parts = parsed.path.split("/")
                ok(self, projected_status(get_manager(), get_remote_store(), parts[2], parts[4]))
            elif parsed.path.startswith("/remotes/") and parsed.path.endswith("/dashboard"):
                from .remote.readapi import projected_dashboard

                ok(self, projected_dashboard(get_manager(), get_remote_store(), parsed.path.split("/")[2],
                                             limit=int(query.get("limit", ["5000"])[0])))
            elif parsed.path == "/view":
                ok(self, get_manager().agent_view(query.get("thread_id", [None])[0], int(query.get("limit", ["50"])[0])))
            elif parsed.path == "/jobs":
                ok(self, get_manager().list(query.get("status") or None, int(query.get("limit", ["50"])[0]),
                                            query.get("thread_id", [None])[0],
                                            query.get("name", [None])[0],
                                            query.get("tags") or None))
            elif parsed.path == "/status/batch":
                ids = query.get("job_ids", [""])[0]
                job_ids = [jid for jid in ids.split(",") if jid] if ids else []
                ok(self, get_manager().status_batch(job_ids, int(query.get("limit", ["500"])[0])))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/status"):
                remote_id = query.get("remote_id", [None])[0]
                if remote_id:
                    from .remote.ssh import VanthRemoteError

                    try:
                        ok(self, get_remote_control().status(
                            remote_id, parsed.path.split("/")[2],
                            idempotency_key="st-" + secrets.token_hex(16)[:12],
                            expected_state_epoch=_remote_epoch(remote_id),
                        ))
                    except VanthRemoteError as exc:
                        error(self, str(exc), 409)
                else:
                    ok(self, get_manager().status(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/events"):
                ok(self, get_manager().events(parsed.path.split("/")[2], query.get("since_event_id", [None])[0],
                                              query.get("types") or None, int(query.get("limit", ["20"])[0]),
                                              query.get("reverse", ["false"])[0].lower() in {"1", "true", "yes"}))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/tail"):
                ok(self, get_manager().tail(parsed.path.split("/")[2], query.get("stream", ["stdout"])[0], int(query.get("max_bytes", ["8192"])[0]), int(query["offset"][0]) if "offset" in query else None, query.get("follow", ["false"])[0] == "true", float(query.get("timeout_seconds", ["5"])[0]), query.get("grep", [None])[0]))
            elif parsed.path == "/deliveries":
                ok(self, get_manager().deliveries(query.get("job_id", [None])[0], query.get("status", [None])[0], int(query.get("limit", ["20"])[0])))
            elif parsed.path.startswith("/deliveries/") and parsed.path.endswith("/attempts"):
                ok(self, get_manager().delivery_attempts(parsed.path.split("/")[2], int(query.get("limit", ["20"])[0])))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/metrics"):
                ok(self, get_manager().metrics_query(
                    parsed.path.split("/")[2],
                    query.get("metric", [None])[0],
                    int(query["from_ms"][0]) if "from_ms" in query else None,
                    int(query["to_ms"][0]) if "to_ms" in query else None,
                    int(query.get("limit", ["1000"])[0]),
                ))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/summary"):
                ok(self, get_manager().run_summary(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/diff"):
                ok(self, get_manager().diff_spec(
                    parsed.path.split("/")[2],
                    query.get("other", [None])[0],
                ))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/artifacts"):
                ok(self, get_manager().artifacts(parsed.path.split("/")[2], int(query.get("limit", ["50"])[0])))
            elif parsed.path.startswith("/artifacts/") and parsed.path.endswith("/content"):
                ok(self, get_manager().artifact_read(parsed.path.split("/")[2], int(query.get("max_bytes", ["262144"])[0])))
            elif parsed.path == "/artifacts/resolve":
                ok(self, get_artifacts().resolve(
                    query.get("name", [""])[0],
                    alias=query.get("alias", [None])[0],
                    version_id=query.get("version_id", [None])[0],
                    idempotency_key=query.get("idempotency_key", [None])[0],
                ))
            elif parsed.path.startswith("/artifacts/info/"):
                ok(self, get_artifacts().info(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/artifacts/collections/"):
                ok(self, get_artifact_collections().get_collection(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/artifacts/lineage/"):
                ok(self, {"version_id": parsed.path.split("/")[2], "lineage": get_artifact_collections().lineage_for(parsed.path.split("/")[2])})
            elif parsed.path == "/cleanup/preview":
                ok(self, get_manager().cleanup_preview(int(query.get("older_than_seconds", ["0"])[0])))
            elif parsed.path == "/metrics/compare":
                ok(self, get_manager().metric_compare(
                    query.get("job_ids") or [],
                    query.get("metric", [None])[0],
                    query.get("aggregation", ["latest"])[0],
                    int(query["from_ms"][0]) if "from_ms" in query else None,
                    int(query["to_ms"][0]) if "to_ms" in query else None,
                ))
            elif parsed.path == "/dashboard":
                ok(self, get_manager().dashboard(
                    query.get("job_ids") or None,
                    int(query.get("limit", ["5000"])[0]),
                ))
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
                allowed = {"command", "cwd", "name", "env", "timeout_seconds", "notify_on", "wake_targets", "origin_thread_id", "tags", "notes", "interactive", "trigger"}
                if self.path == "/jobs" and ("command" not in payload or set(payload) - allowed):
                    raise ValueError("invalid job request")
                error(self, "Unauthorized", 401)
                return
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path == "/jobs":
                remote_id = payload.pop("remote_id", None)
                payload.pop("idempotency_key", None)
                if remote_id:
                    ok(self, _remote_submit(remote_id, "job.start", _remote_payload(payload)))
                else:
                    ok(self, asyncio.run(get_manager().start(**payload)))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/rerun"):
                remote_id = payload.pop("remote_id", None)
                if remote_id:
                    job_id = parsed.path.split("/")[2]
                    ok(self, _remote_submit(remote_id, "job.rerun", {"job_id": job_id, **payload}))
                else:
                    ok(self, asyncio.run(get_manager().rerun(parsed.path.split("/")[2], **payload)))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/wait"):
                remote_id = payload.pop("remote_id", None)
                if remote_id:
                    ok(self, _remote_wait(remote_id, parsed.path.split("/")[2], payload))
                else:
                    ok(self, get_manager().wait_sync(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/stop"):
                remote_id = payload.pop("remote_id", None)
                if remote_id:
                    ok(self, _remote_submit(remote_id, "job.stop", {"job_id": parsed.path.split("/")[2], **payload}))
                else:
                    ok(self, get_manager().stop_sync(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/send"):
                ok(self, get_manager().send_sync(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/deliveries/") and parsed.path.endswith("/mark"):
                ok(self, get_manager().mark_delivery(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/deliveries/") and parsed.path.endswith("/retry"):
                ok(self, get_manager().retry_delivery(parsed.path.split("/")[2]))
            elif parsed.path == "/cleanup":
                ok(self, get_manager().cleanup(**payload))
            elif parsed.path == "/reap-orphans":
                ok(self, get_manager().reap_orphans())
            elif parsed.path == "/remotes/pair":
                from .remote.pairing import pair_remote

                ok(self, pair_remote(
                    target=payload.get("target", ""),
                    name=payload.get("name"),
                    allow_root=bool(payload.get("allow_root", False)),
                    store=get_remote_store(),
                ))
            elif parsed.path == "/remotes/remove":
                from .remote.pairing import remove_remote

                ok(self, remove_remote(remote_id=payload.get("remote_id"), store=get_remote_store()))
            elif parsed.path == "/remote/helper":
                frame = payload.get("frame", payload)
                remote = _remote_job_manager()
                kind = frame.get("kind")
                if kind == "request":
                    response = remote.handle_request(frame)
                elif kind == "snapshot":
                    response = remote.handle_snapshot_request(frame)
                elif kind == "log_range":
                    response = remote.handle_log_range_request(frame)
                else:
                    from .remote.protocol import VanthRemoteProtocolError

                    raise VanthRemoteProtocolError("PROTOCOL_UNKNOWN_KIND", f"unknown frame kind: {frame.get('kind')!r}")
                ok(self, response)
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/metrics"):
                ok(self, get_manager().metric_ingest(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/wake"):
                ok(self, get_manager().add_wake_target(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/artifacts"):
                ok(self, get_manager().artifact_add(parsed.path.split("/")[2], **payload))
            elif parsed.path == "/artifacts/put":
                data = None
                if payload.get("data_b64") is not None:
                    import base64

                    data = base64.b64decode(payload["data_b64"])
                ok(self, get_artifacts().put_file(
                    payload.get("name"),
                    data=data,
                    source_path=payload.get("path"),
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/put-dir":
                ok(self, get_artifacts().put_dir(
                    payload.get("source_path"),
                    payload.get("name"),
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/materialize":
                ok(self, get_artifacts().materialize(
                    payload["version_id"],
                    payload["dest_path"],
                    overwrite=bool(payload.get("overwrite", False)),
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/verify":
                ok(self, get_artifacts().verify(payload["version_id"], idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/collections":
                ok(self, get_artifact_collections().create_collection(
                    payload.get("name"), idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/collections/append":
                ok(self, get_artifact_collections().append_version(
                    payload.get("collection") or payload.get("name"),
                    payload.get("version_id"),
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/alias-set":
                ok(self, get_artifact_collections().alias_set(
                    payload.get("alias_name"),
                    payload.get("root_id"),
                    payload.get("expected_version_id"),
                    payload.get("new_version_id"),
                    idempotency_key=payload.get("idempotency_key"),
                    updated_by=payload.get("updated_by"),
                ))
            elif parsed.path == "/artifacts/lineage":
                ok(self, get_artifact_collections().link_lineage(
                    payload.get("producer_kind"),
                    payload.get("producer_id"),
                    payload.get("consumer_kind"),
                    payload.get("consumer_id"),
                    payload.get("version_id"),
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/delete-request":
                ok(self, get_artifact_lifecycle().request_delete(
                    payload.get("version_id"), idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/restore-version":
                ok(self, get_artifact_lifecycle().restore(
                    payload.get("version_id"), idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/pin":
                ok(self, get_artifact_lifecycle().pin(
                    payload.get("version_id"), payload.get("hold_reason"),
                    idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/unpin":
                ok(self, get_artifact_lifecycle().unpin(
                    payload.get("version_id"), idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/gc":
                ok(self, get_artifact_lifecycle().gc(
                    dry_run=bool(payload.get("dry_run", True)),
                    idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/backup":
                ok(self, {"backup_path": str(get_artifact_lifecycle().backup())})
            elif parsed.path == "/artifacts/begin-restore":
                ok(self, get_artifact_lifecycle().begin_restore(payload.get("backup_path")))
            elif parsed.path == "/artifacts/complete-restore":
                ok(self, get_artifact_lifecycle().complete_restore())
            elif parsed.path == "/shutdown":
                _stop_httpd()
                ok(self, {"result": "shutting_down"})
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


def write_daemon_metadata(home: Path, url: str) -> None:
    """Atomically write daemon discovery metadata for clients and the monitor."""
    from .migrations import LATEST_SCHEMA_VERSION

    payload = {
        "url": url,
        "home": str(home),
        "pid": os.getpid(),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "schema_version": LATEST_SCHEMA_VERSION,
    }
    path = home / "daemon.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)


def remove_daemon_metadata(home: Path) -> None:
    try:
        (home / "daemon.json").unlink()
    except FileNotFoundError:
        pass


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
    secure_home_permissions(home)
    logger = _configure_logging(home)
    lock = DaemonLock(home / "daemon.lock")
    if not lock.acquire():
        raise SystemExit("another vanthd already owns this VANTH_HOME")
    try:
        httpd = None
        for attempt in range(6):
            try:
                httpd = TrackingHTTPServer((host, port), Handler)
                break
            except OSError:
                if attempt == 5:
                    raise
                time.sleep(0.1 * (attempt + 1))
    except OSError as exc:
        lock.release()
        raise SystemExit(f"cannot bind {host}:{port}: {exc}") from exc
    _set_httpd(httpd)
    shutdown_event.clear()
    daemon_url = f"http://{host}:{port}"
    write_daemon_metadata(home, daemon_url)

    previous = {}
    signal_names = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        signal_names.append(signal.SIGBREAK)
    for name in signal_names:
        try:
            previous[name] = signal.signal(name, _stop_httpd)
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
        remove_daemon_metadata(home)
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
