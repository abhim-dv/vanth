"""Remote-side accepted operations and dispatch (Phase 2).

:class:`RemoteJobManager` runs inside the remote daemon and handles protocol
frames forwarded by the forced-command helper over its authenticated loopback
API (``POST /remote/helper``).

- ``handle_request`` validates the frame, checks the state epoch, and commits
  the operation (``accepted``), the queued job row, the origin mapping, and the
  launch intent in **one** ``BEGIN IMMEDIATE`` transaction, then transitions the
  operation ``accepted -> queued`` (plan §Phase 2 "One transaction on the
  remote daemon that commits the operation, queued job, origin mapping, and
  launch intent before starting a runner").
- Replaying the same ``idempotency_key`` with the same digest returns the
  accepted operation (no second job); a different digest is rejected with
  ``PROTOCOL_REPLAY_MISMATCH``.
- ``_dispatch_loop`` is a daemon thread that polls ``remote_operations`` for
  ``queued`` rows and launches each underlying job through the refactored
  ``JobManager.prepare_launch`` / ``_launch_prepared`` path (never
  ``JobManager.start``), transitioning ``queued -> launched -> running`` and, on
  runner exit, ``completed`` / ``failed``. Because the operation, queued job,
  origin mapping, and launch intent all commit before the runner starts, the
  remote daemon can restart between acceptance and launch and the dispatcher
  resumes the ``queued`` row from the shared sqlite.

Remote rows live only in the remote-side tables (``remote_operations``,
``remote_replay_tombstones``, ``remote_job_origins``). The local ``JobManager``
never reads them: local PID, heartbeat, stdin, timeout, stop, recovery,
trigger, quota, and cleanup code stay entirely on the local ``jobs`` table.
"""

from __future__ import annotations

import json
import secrets
import threading
import time
from typing import Any

from .protocol import (
    VanthRemoteProtocolError,
    decode_frame,
    encode_frame,
    request_digest,
    validate_request,
)
from .store import RemoteOperationStore
from ..server import TERMINAL_STATUSES, now_iso


class _FakeSession:
    """A session-like object for the default in-process transport.

    The real helper forwards frames to the remote daemon over loopback HTTP;
    the daemon route uses the in-process transport below. A future concrete
    transport may talk over a real socket; the injectable seam is the same.
    """

    def __init__(self, handler) -> None:
        self._handler = handler

    def exchange(self, frame_bytes: bytes) -> str:
        frame = decode_frame(frame_bytes.decode("utf-8").rstrip("\n"))
        response = self._handler(frame)
        return encode_frame(response).decode("utf-8").rstrip("\n")


class RemoteJobManager:
    """Remote daemon side: accept operations, dispatch queued jobs."""

    def __init__(self, store: RemoteOperationStore, manager: Any, *, home: Any = None) -> None:
        self.store = store
        self.manager = manager
        self.home = home
        self.dispatcher_stop = threading.Event()
        self.dispatcher_thread: threading.Thread | None = None
        self.poll_interval = float(__import__("os").environ.get("VANTH_REMOTE_POLL_INTERVAL", "0.2"))
        self._shutdown = False

    def start(self) -> "RemoteJobManager":
        self.dispatcher_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        self.dispatcher_thread.start()
        return self

    def stop(self) -> None:
        self.dispatcher_stop.set()
        if self.dispatcher_thread and self.dispatcher_thread is not threading.current_thread():
            self.dispatcher_thread.join(timeout=2)

    def close(self) -> None:
        self.stop()
        self._shutdown = True

    # ------------------------------------------------------------------
    # Frame handling (invoked by the daemon's POST /remote/helper route)
    # ------------------------------------------------------------------

    def handle_request(self, frame: dict[str, Any]) -> dict[str, Any]:
        """Handle one request frame; returns a response or error frame."""
        try:
            method = frame["method"]
            payload = frame["payload"]
            idempotency_key = frame["idempotency_key"]
        except KeyError:
            return self._error_frame(frame, code="INVALID_REQUEST", message="missing request fields")
        digest = frame.get("digest")
        if digest is None:
            digest = request_digest(method, payload, idempotency_key)
        try:
            return self.handle_request_checked(frame, method, payload, idempotency_key, digest)
        except VanthRemoteProtocolError as exc:
            return self._error_frame(frame, code=exc.code, message=str(exc))
        except Exception as exc:
            return self._error_frame(frame, code="INVALID_REQUEST", message=str(exc))

    def handle_request_checked(
        self,
        frame: dict[str, Any],
        method: str,
        payload: dict[str, Any],
        idempotency_key: str,
        digest: str,
    ) -> dict[str, Any]:
        validate_request(method, payload)
        if method == "job.status":
            return self._handle_status(frame, payload, idempotency_key)
        return self._handle_mutation(frame, method, payload, idempotency_key, digest)

    def _handle_status(self, frame: dict[str, Any], payload: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        job_id = payload["job_id"]
        origin = self.store.get_remote_job_origin(job_id)
        status = None
        if origin:
            row = self.store.db.execute("SELECT status FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row:
                status = row["status"]
        result = {
            "job_id": job_id,
            "found": origin is not None,
            "origin": origin,
        }
        if status is not None:
            result["status"] = status
        return self._response_frame(frame, result)

    def _handle_mutation(
        self, frame: dict[str, Any], method: str, payload: dict[str, Any], idempotency_key: str, digest: str
    ) -> dict[str, Any]:
        self.store.db.execute("BEGIN IMMEDIATE")
        try:
            op = self.store.record_operation(
                idempotency_key=idempotency_key,
                method=method,
                payload=payload,
                digest=digest,
                commit=False,
            )
            replay = op["status"] != "accepted"
            job_id = None
            if not replay:
                job_id = "job_" + secrets.token_hex(16)[:12]
                self._insert_queued_job_uncommitted(job_id, payload)
                origin = "remote:" + idempotency_key
                self.store.record_queued_job(
                    op_id=op["op_id"],
                    remote_job_id=job_id,
                    origin=origin,
                )
                self.store.update_operation_status(op["op_id"], "queued", commit=False)
            self.store.db.commit()
        except BaseException:
            self.store.db.rollback()
            raise
        if replay:
            job_id = self.store.get_remote_job_origin_op(op["op_id"]) or op.get("job_id")
            return self._response_frame(frame, {
                "op_id": op["op_id"],
                "job_id": job_id,
                "status": op["status"],
                "replayed": True,
            })
        return self._response_frame(frame, {
            "op_id": op["op_id"],
            "job_id": job_id,
            "status": "queued",
        })

    def _insert_queued_job_uncommitted(self, job_id: str, payload: dict[str, Any]) -> None:
        """Insert the queued job row + origin mapping inside the caller's transaction."""
        command = payload.get("command") or "true"
        cwd = payload.get("cwd")
        name = payload.get("name")
        timeout_seconds = payload.get("timeout_seconds")
        env = payload.get("env") or {}
        tags = payload.get("tags") or []
        notes = payload.get("notes")
        interactive = payload.get("interactive")
        notify_on = payload.get("notify_on") or []
        trigger = payload.get("trigger")
        created_at = now_iso()
        self.store.db.execute(
            """
            INSERT INTO jobs(
              job_id, name, command, cwd, status, created_at, updated_at,
              timeout_seconds, notify_on, tags_json, env_json, notes, run_json,
              stdout_path, stderr_path, events_path, trigger_json
            )
            VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id, name, command, cwd, created_at, created_at,
                timeout_seconds,
                json.dumps(notify_on, separators=(",", ":")),
                json.dumps(tags, separators=(",", ":")),
                json.dumps(env, separators=(",", ":")),
                notes,
                json.dumps({"interactive": bool(interactive)}, separators=(",", ":")),
                str(self.manager.logs / f"{job_id}.stdout.log"),
                str(self.manager.logs / f"{job_id}.stderr.log"),
                str(self.manager.events_dir / f"{job_id}.jsonl"),
                json.dumps(trigger, separators=(",", ":")) if trigger else None,
            ),
        )

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------

    def _dispatch_loop(self) -> None:
        while not self.dispatcher_stop.wait(self.poll_interval):
            try:
                self._dispatch_queued_ops()
                self._sync_terminal_ops()
            except Exception:
                import logging

                logging.getLogger("vanth.remote").exception("remote dispatch iteration failed")

    def _dispatch_queued_ops(self) -> None:
        """Launch accepted queued jobs after commit.

        Polls operations in ``queued`` (and ``launched`` that never actually
        spawned, e.g. a crash between the ``launched`` commit and the process
        spawn). For each it launches the underlying job through the refactored
        ``JobManager.prepare_launch`` / ``_launch_prepared`` path and drives the
        operation ``queued -> launched -> running``. Because the operation, the
        queued job row, the origin mapping, and the launch intent all commit
        atomically before any spawn, a daemon restart between acceptance and
        launch resumes from the shared sqlite without losing the job.
        """
        rows = self.store.db.execute(
            """
            SELECT op_id, idempotency_key, method, payload_json, digest, status
            FROM remote_operations WHERE status IN ('queued', 'launched')
            """
        ).fetchall()
        for row in rows:
            try:
                op_id = row["op_id"]
                job_id = self.store.get_remote_job_origin_op(op_id)
                if not job_id:
                    self.store.update_operation_status(op_id, "failed")
                    continue
                # A `launched` row whose job is already running was spawned
                # before a crash; do not spawn a second process.
                if row["status"] == "launched":
                    job_status = self.store.db.execute(
                        "SELECT status FROM jobs WHERE job_id=?", (job_id,)
                    ).fetchone()
                    if job_status and job_status["status"] in TERMINAL_STATUSES:
                        self.store.update_operation_status(op_id, job_status["status"])
                        continue
                    if job_status and job_status["status"] == "running":
                        self.store.update_operation_status(op_id, "running")
                        continue
                launch = self.manager.prepare_launch(job_id)
                if launch is None:
                    self.store.update_operation_status(op_id, "failed")
                    continue
                if row["status"] == "queued":
                    self.store.update_operation_status(op_id, "launched")
                self.store.update_operation_status(op_id, "running")
                result = self.manager._launch_prepared(launch)
                if result.get("status") in TERMINAL_STATUSES:
                    self.store.update_operation_status(op_id, result.get("status"))
            except Exception:
                import logging

                logging.getLogger("vanth.remote").exception("remote dispatch of op %s failed", row["op_id"])

    def _sync_terminal_ops(self) -> None:
        """Converge ``running`` operations to the terminal status the runner
        recorded on the remote daemon's local ``jobs`` row."""
        rows = self.store.db.execute(
            "SELECT op_id FROM remote_operations WHERE status='running'"
        ).fetchall()
        for row in rows:
            job_id = self.store.get_remote_job_origin_op(row["op_id"])
            if not job_id:
                continue
            job_status = self.store.db.execute(
                "SELECT status FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if job_status and job_status["status"] in TERMINAL_STATUSES:
                try:
                    self.store.update_operation_status(row["op_id"], job_status["status"])
                except Exception:
                    import logging

                    logging.getLogger("vanth.remote").exception(
                        "operation terminal sync failed op=%s job=%s", row["op_id"], job_id
                    )

    def mark_terminal_from_job(self, job_id: str, status: str) -> None:
        """Called by the remote daemon when a local runner records a terminal
        status, so the accepted operation converges to the same terminal state."""
        if status not in {"completed", "failed", "cancelled", "timeout", "orphaned"}:
            return
        op_id = self.store.get_remote_operation_by_job(job_id)
        if not op_id:
            return
        try:
            self.store.update_operation_status(op_id, status)
        except Exception:
            import logging

            logging.getLogger("vanth.remote").exception("operation terminal sync failed op=%s job=%s", op_id, job_id)

    # ------------------------------------------------------------------
    # Unsupported (Phase 3/4) frames
    # ------------------------------------------------------------------

    def handle_snapshot_request(self, frame: dict[str, Any]) -> dict[str, Any]:
        return self._error_frame(frame, code="UNSUPPORTED_FEATURE", message="snapshot is not implemented yet")

    def handle_log_range_request(self, frame: dict[str, Any]) -> dict[str, Any]:
        return self._error_frame(frame, code="UNSUPPORTED_FEATURE", message="log_range is not implemented yet")

    # ------------------------------------------------------------------
    # Frame builders
    # ------------------------------------------------------------------

    def _response_frame(self, request: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        return {
            "version": "1",
            "kind": "response",
            "request_id": request.get("request_id"),
            "method": request.get("method"),
            "result": result,
            "sent_at": now_iso(),
        }

    def _error_frame(self, request: dict[str, Any], *, code: str, message: str) -> dict[str, Any]:
        from .helper import error_frame

        return error_frame(request, code=code, message=message)


def default_transport() -> Any:
    """A transport whose ``open_session`` talks to an in-process handler.

    Real deployments use the helper over loopback HTTP; this seam lets the
    daemon route and tests exercise the exact same handler.
    """
    return _InProcessTransport()


class _InProcessTransport:
    def __init__(self, handler: Any = None) -> None:
        self._handler = handler

    def open_session(self, remote_row: dict[str, Any], *, home: Any = None) -> Any:
        return _FakeSession(self._handler)
