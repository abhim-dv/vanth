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
from pathlib import Path
from typing import Any

from .feed import FeedStore
from .protocol import (
    VanthRemoteProtocolError,
    decode_frame,
    encode_frame,
    request_digest,
    validate_request,
)
from .store import RemoteOperationStore
from .transfer import TransferRegistry, _TRANSFER_METHODS
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
        self.feed = FeedStore(store.db)
        self.transfers = TransferRegistry(
            store.db,
            epoch_fn=self._remote_state_epoch,
            ops_factory=self._remote_artifact_ops,
            staging_dir=(Path(home) / "remote-transfers") if home else None,
        )
        self.dispatcher_stop = threading.Event()
        self.dispatcher_thread: threading.Thread | None = None
        self.poll_interval = float(__import__("os").environ.get("VANTH_REMOTE_POLL_INTERVAL", "0.2"))
        self._shutdown = False

    def _remote_artifact_ops(self) -> Any:
        """The remote host's own ArtifactOperations (Phase 9 publication)."""
        from ..artifacts.catalog import open_catalog
        from ..artifacts.local_store import LocalBlobStore, default_store_root
        from ..artifacts.operations import ArtifactOperations

        if not self.home:
            from ..paths import canonical_home

            home = canonical_home()
        else:
            home = Path(self.home)
        catalog = open_catalog(home)
        blobs = LocalBlobStore(default_store_root(home), catalog)
        return ArtifactOperations(catalog, blobs)

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
        if method in _TRANSFER_METHODS:
            return self._handle_transfer_frame(frame, method, payload)
        if method == "job.snapshot":
            return self.handle_snapshot_request(frame, payload)
        if method == "job.log_range":
            return self.handle_log_range_request(frame, payload)
        if method == "job.feed":
            return self.handle_feed_request(frame, payload)
        return self._handle_mutation(frame, method, payload, idempotency_key, digest)

    def _handle_transfer_frame(self, frame: dict[str, Any], method: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Route artifact transfer frames to the Phase 9 TransferRegistry."""
        try:
            if method == "artifact.transfer_init":
                result = self.transfers.init(payload)
            elif method == "artifact.blob_chunk":
                result = self.transfers.chunk(payload)
            else:
                result = self.transfers.complete(payload)
        except VanthRemoteProtocolError as exc:
            return self._error_frame(frame, code=exc.code, message=str(exc))
        except Exception as exc:
            return self._error_frame(frame, code="INVALID_REQUEST", message=str(exc))
        return self._response_frame(frame, result)

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
        # Outbox recording happens strictly AFTER the acceptance transaction
        # commits: a crash between the two leaves the job discoverable via
        # snapshot recovery, never a phantom feed row.
        if job_id:
            self._record_job_upsert(job_id)
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
                else:
                    self._record_job_upsert(job_id)

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
        else:
            self._record_job_upsert(job_id)

    # ------------------------------------------------------------------
    # Unsupported (Phase 3/4) frames
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Snapshot + log-range reads (Phase 3)
    # ------------------------------------------------------------------

    SNAPSHOT_PAGE_SIZE = 50
    SNAPSHOT_EVENT_LIMIT = 500

    def _remote_state_epoch(self) -> int:
        """The remote's own state epoch, from its remote_state singleton."""
        try:
            return self.store.get_state_epoch()
        except Exception:
            return 1

    def handle_snapshot_request(self, frame: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Build one paginated snapshot frame fixed to the remote state epoch.

        The cursor carries the page offset and the high-water event seq. Jobs
        come from the remote daemon's local ``jobs`` table; events are the
        bounded tail for the jobs on this page. ``has_more`` is true when more
        pages follow.
        """
        payload = payload if payload is not None else (frame.get("payload") or {})
        cursor = payload.get("cursor") or {}
        offset = int(cursor.get("offset", 0)) if isinstance(cursor, dict) else 0
        manager = self.manager
        rows = manager.db.execute(
            """
            SELECT job_id, name, command, status, created_at, updated_at, exit_code
            FROM jobs ORDER BY created_at ASC LIMIT ? OFFSET ?
            """,
            (self.SNAPSHOT_PAGE_SIZE + 1, offset),
        ).fetchall()
        has_more = len(rows) > self.SNAPSHOT_PAGE_SIZE
        rows = rows[: self.SNAPSHOT_PAGE_SIZE]
        jobs = [dict(row) for row in rows]
        events = []
        high_water = int(cursor.get("high_water", 0)) if isinstance(cursor, dict) else 0
        for job in jobs:
            for event_row in manager.db.execute(
                "SELECT event_id, job_id, seq, type, level, message, data_json, source, created_at "
                "FROM events WHERE job_id=? AND seq>? ORDER BY seq ASC LIMIT ?",
                (job["job_id"], high_water, self.SNAPSHOT_EVENT_LIMIT),
            ).fetchall():
                events.append(dict(event_row))
                high_water = max(high_water, int(event_row["seq"]))
        next_cursor = {
            "offset": offset + len(jobs),
            "high_water": high_water,
        }
        # Reads travel in a standard response frame's result so the controller
        # run_request path handles them uniformly; the inner kind identifies
        # the payload shape.
        return self._response_frame(frame, {
            "kind": "snapshot",
            "state_epoch": self._remote_state_epoch(),
            "cursor": next_cursor,
            "jobs": jobs,
            "events": events,
            "has_more": has_more,
        })

    # ------------------------------------------------------------------
    # Change feed (Phase 4)
    # ------------------------------------------------------------------

    FEED_DEFAULT_LIMIT = 100
    FEED_MAX_LIMIT = 500
    FEED_MAX_WAIT_MS = 10000
    FEED_POLL_INTERVAL = 0.025

    def _record_job_upsert(self, job_id: str, status: str | None = None) -> None:
        """Append a ``job.upsert`` outbox row with the minimal job row.

        Best-effort: feed recording must never break dispatch or acceptance.
        """
        try:
            payload: dict[str, Any] = {"job_id": job_id}
            row = self.manager.db.execute(
                "SELECT job_id, name, command, status, created_at, updated_at, exit_code "
                "FROM jobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
            if row:
                payload = dict(row)
            elif status:
                payload["status"] = status
            self.feed.append("job.upsert", job_id=job_id, payload=payload)
        except Exception:
            import logging

            logging.getLogger("vanth.remote").exception("feed append failed job=%s", job_id)

    def handle_feed_request(self, frame: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Serve one bounded batch of outbox changes after the caller's cursor.

        Reads rows strictly after ``cursor.seq`` within the CURRENT
        ``(state_epoch, feed_epoch)``; when no rows exist and ``wait_ms`` is
        requested, long-polls up to that duration before returning an empty
        batch. The result carries ``oldest_seq``/``high_water_seq`` so the
        controller can detect cursor gaps and reset its cursor to the
        high-water mark.
        """
        from .protocol import (
            FEED_DEFAULT_LIMIT,
            FEED_MAX_LIMIT,
            FEED_MAX_WAIT_MS,
        )

        payload = payload if payload is not None else (frame.get("payload") or {})
        limit = int(payload.get("limit") or FEED_DEFAULT_LIMIT)
        if limit < 1:
            return self._error_frame(frame, code="INVALID_REQUEST", message="limit must be >= 1")
        limit = min(limit, FEED_MAX_LIMIT)
        wait_ms = int(payload.get("wait_ms") or 0)
        if wait_ms < 0:
            return self._error_frame(frame, code="INVALID_REQUEST", message="wait_ms must be >= 0")
        wait_ms = min(wait_ms, FEED_MAX_WAIT_MS)
        cursor = payload.get("cursor")
        if cursor is not None and not isinstance(cursor, dict):
            return self._error_frame(frame, code="INVALID_REQUEST", message="cursor must be an object")

        deadline = time.monotonic() + (wait_ms / 1000.0)
        while True:
            batch = self.feed.read(cursor, limit=limit)
            if batch["changes"] or wait_ms <= 0 or time.monotonic() >= deadline:
                break
            time.sleep(self.FEED_POLL_INTERVAL)

        last_seq = int(cursor.get("seq") or 0) if isinstance(cursor, dict) else 0
        for change in batch["changes"]:
            last_seq = max(last_seq, change["seq"])
        next_cursor = {
            "state_epoch": batch["state_epoch"],
            "feed_epoch": batch["feed_epoch"],
            "seq": last_seq,
        }
        return self._response_frame(frame, {
            "kind": "feed",
            "state_epoch": batch["state_epoch"],
            "feed_epoch": batch["feed_epoch"],
            "cursor": next_cursor,
            "changes": batch["changes"],
            "has_more": batch["has_more"],
            "oldest_seq": batch["oldest_seq"],
            "high_water_seq": batch["high_water_seq"],
        })

    def handle_log_range_request(self, frame: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Read a byte range of a remote job's stdout/stderr log.

        Returns a ``log_range`` frame with base64 content; ``truncated`` is set
        when the requested window was clipped to the file size. Arbitrary bytes
        round-trip exactly through base64.
        """
        import base64
        import os as _os

        from .protocol import VanthRemoteProtocolError as _VPE

        payload = payload if payload is not None else (frame.get("payload") or {})
        remote_job_id = payload["remote_job_id"]
        stream = payload.get("stream") or "stdout"
        if stream not in ("stdout", "stderr"):
            return self._error_frame(frame, code="INVALID_REQUEST", message="stream must be 'stdout' or 'stderr'")
        offset = int(payload.get("offset", 0))
        size = int(payload.get("size", 65536))
        if offset < 0 or size <= 0:
            return self._error_frame(frame, code="INVALID_REQUEST", message="offset must be >= 0 and size must be > 0")
        path = Path(str(self.manager.logs / f"{remote_job_id}.{stream}.log"))
        if not path.exists():
            return self._error_frame(frame, code="INVALID_REQUEST", message=f"unknown remote job log: {remote_job_id}")
        file_size = path.stat().st_size
        with path.open("rb") as handle:
            handle.seek(offset)
            content = handle.read(size)
        truncated = (offset + len(content)) < file_size
        return self._response_frame(frame, {
            "kind": "log_range",
            "remote_job_id": remote_job_id,
            "stream": stream,
            "offset": offset,
            "size": file_size,
            "content": base64.b64encode(content).decode("ascii"),
            "truncated": truncated,
        })

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
