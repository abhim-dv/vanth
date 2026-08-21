"""Controller-side durable request execution (Phase 2).

:class:`RemoteControl` executes protocol requests over SSH with full
durability:

- ``submit`` records the request row (status ``creating``) **and** the
  ``submitting`` shadow in **one** ``BEGIN IMMEDIATE`` transaction before any
  SSH I/O (plan §Phase 2 "One transaction on the controller that creates the
  request and ``submitting`` shadow before SSH").
- Replaying the same ``idempotency_key`` with the same digest returns the
  original request and never starts a second job; a changed method or
  normalized payload for the same key is rejected with
  ``PROTOCOL_REPLAY_MISMATCH``.
- ``run_request`` opens **one** SSH session per request (injectable transport,
  no connection pooling), sends the request frame, and reads exactly one
  response frame.
- A transport failure leaves the request ``submitting`` (durably lost): replay
  returns the same request and calling ``submit`` again does not create a
  second row, because the remote's operation idempotency guarantees at most one
  acceptance per key.
- Every mutation first verifies the remote's ``state_epoch`` against the
  controller's stored expectation so a stale epoch cannot mutate a restored
  timeline.

The transport interface mirrors the injectable pattern from ``pairing.py``: a
``run_session``-style callable that takes the encoded frame bytes and returns
the response line. Tests use a fake transport; no real network is touched.
"""

from __future__ import annotations

import json
import os
import secrets
from pathlib import Path
from typing import Any

from . import ssh
from .protocol import (
    IDEMPOTENCY_KEY_RE,
    VanthRemoteProtocolError,
    decode_frame,
    encode_frame,
    request_digest,
    validate_request,
)
from .store import RemoteStore
from ..server import now_iso


class _SSHSession:
    """One SSH process bound to a paired remote; exchange() runs the helper."""

    def __init__(self, remote_row: dict[str, Any], *, config: str, config_dir: Path, timeout: float) -> None:
        self.remote_row = remote_row
        self.config = config
        self.config_dir = config_dir
        self.timeout = timeout

    def exchange(self, frame_bytes: bytes) -> str | None:
        """Run one ``ssh <target>`` with the helper frame on stdin.

        Returns the decoded stdout line, or None on transport failure.
        """
        result = ssh.run_ssh(
            [self.remote_row.get("target", "")],
            config_dir=self.config_dir,
            config=self.config,
            timeout=self.timeout,
            stdin=frame_bytes,
        )
        if result.returncode != 0:
            return None
        return result.stdout.decode("utf-8", errors="replace").strip() or None


class DefaultSessionTransport:
    """One SSH process per request, using the paired remote's allowlist config."""

    def open_session(self, remote_row: dict[str, Any], *, home: Path) -> Any:
        """Return a session object with ``exchange(frame_bytes) -> line``.

        ``exchange`` runs ``ssh <target>`` with the generated allowlist config,
        feeds the encoded frame to the forced-command helper's stdin, and
        returns the decoded response line (or None on transport failure).
        """
        remote_id = remote_row["remote_id"]
        remote_dir = home / "remote" / remote_id
        config = ssh.allowlist_config(
            hostname=remote_row.get("target", ""),
            user=None,
            port=None,
            identity_file=str(remote_dir / "id_ed25519"),
            known_hosts=str(remote_dir / "known_hosts"),
        )
        timeout = float(os.environ.get("VANTH_REMOTE_REQUEST_TIMEOUT", "60"))
        return _SSHSession(remote_row, config=config, config_dir=remote_dir, timeout=timeout)


class RemoteControl:
    """Controller-side durable request execution against a paired remote."""

    def __init__(self, store: RemoteStore, *, transport: Any | None = None, home: str | Path | None = None) -> None:
        self.store = store
        self.transport = transport or DefaultSessionTransport()
        if home is None:
            from ..paths import canonical_home

            home = canonical_home()
        self.home = Path(home)

    # ------------------------------------------------------------------
    # submit / run_request / replay
    # ------------------------------------------------------------------

    def submit(self, remote_id: str, method: str, payload: dict[str, Any], *, idempotency_key: str, expected_state_epoch: int | None = None) -> dict[str, Any]:
        """Record a durable request and (on first use) a ``submitting`` shadow.

        ``idempotency_key`` is required and validated by the protocol. Returns
        the durable request row. On replay (same key + same digest) returns the
        original request without creating a second row. A different digest for
        the same key raises ``PROTOCOL_REPLAY_MISMATCH``. The caller supplies
        ``expected_state_epoch`` from the most recent ``hello`` frame.
        """
        if not isinstance(idempotency_key, str) or not IDEMPOTENCY_KEY_RE.match(idempotency_key):
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "idempotency_key must be 8..128 chars in [A-Za-z0-9_-]"
            )
        payload = self._validate_payload(method, payload)
        digest = request_digest(method, payload, idempotency_key)
        self.store.check_state_epoch(remote_id, expected_state_epoch)
        self.store.db.execute("BEGIN IMMEDIATE")
        try:
            request = self.store.record_request(
                remote_id=remote_id,
                idempotency_key=idempotency_key,
                method=method,
                payload=payload,
                digest=digest,
                commit=False,
            )
            if request["status"] == "creating":
                self.store.record_submitting_shadow(remote_id, request)
            self.store.db.commit()
        except BaseException:
            self.store.db.rollback()
            raise
        return request

    def run_request(self, remote_id: str, request: dict[str, Any], *, expected_state_epoch: int | None = None) -> dict[str, Any]:
        """Send one request over one SSH session and record the outcome.

        The request is durably transitioned ``creating -> submitting`` before
        any SSH I/O. On a response frame it then goes ``submitting -> accepted``
        then ``accepted -> completed`` (with the response stored) and the remote
        job shadow is upserted. On an error frame the request goes ``failed``.
        On transport failure the request stays ``submitting`` (durably lost) and
        is replayed by key without starting a second job.
        """
        remote_row = self.store.get_remote(remote_id)
        self.store.check_state_epoch(remote_id, expected_state_epoch)
        try:
            self.store.update_request_status(request["request_id"], "submitting")
        except ValueError:
            return self.store.get_request_by_key(remote_id, request["idempotency_key"])
        frame = self._build_request_frame(remote_id, request)
        try:
            session = self.transport.open_session(remote_row, home=self.home)
            if session is None:
                return self._lost(remote_id, request)
            response_line = session.exchange(encode_frame(frame))
            if not response_line:
                return self._lost(remote_id, request)
            response = decode_frame(response_line)
        except Exception:
            return self._lost(remote_id, request)

        self.store.db.execute("BEGIN IMMEDIATE")
        try:
            self.store.update_request_status(request["request_id"], "accepted", commit=False)
            self.store.db.commit()
        except ValueError:
            return self._lost(remote_id, request)
        if response.get("kind") == "error":
            return self._finish_error(remote_id, request, response)
        if response.get("kind") != "response":
            return self._lost(remote_id, request)
        return self._finish_response(remote_id, request, response)

    def replay(self, remote_id: str, idempotency_key: str) -> dict[str, Any]:
        """Return the durable request for a key, or a replay tombstone result."""
        try:
            return self.store.get_request_by_key(remote_id, idempotency_key)
        except ValueError:
            pass
        try:
            tombstone = self.store.get_replay_tombstone(remote_id, idempotency_key)
            return {
                "replayed": True,
                "tombstone": tombstone,
                "message": "request is no longer retained; replay identity preserved by tombstone",
            }
        except ValueError:
            raise ValueError(f"no durable request for key {idempotency_key!r} on remote {remote_id!r}") from None

    # ------------------------------------------------------------------
    # status / stop / rerun helpers
    # ------------------------------------------------------------------

    def status(self, remote_id: str, remote_job_id: str, *, idempotency_key: str, expected_state_epoch: int | None = None) -> dict[str, Any]:
        request = self.submit(
            remote_id, "job.status", {"job_id": remote_job_id},
            idempotency_key=idempotency_key, expected_state_epoch=expected_state_epoch,
        )
        if request["status"] != "creating":
            return request
        return self.run_request(remote_id, request, expected_state_epoch=expected_state_epoch)

    def stop(self, remote_id: str, remote_job_id: str, *, signal: str = "terminate", kill_after_seconds: int = 10,
             idempotency_key: str, expected_state_epoch: int | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"job_id": remote_job_id}
        if signal != "terminate":
            payload["signal"] = signal
        if kill_after_seconds != 10:
            payload["kill_after_seconds"] = kill_after_seconds
        request = self.submit(
            remote_id, "job.stop", payload,
            idempotency_key=idempotency_key, expected_state_epoch=expected_state_epoch,
        )
        if request["status"] != "creating":
            return request
        return self.run_request(remote_id, request, expected_state_epoch=expected_state_epoch)

    def rerun(self, remote_id: str, remote_job_id: str, overrides: dict[str, Any] | None = None, *,
              idempotency_key: str, expected_state_epoch: int | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"job_id": remote_job_id}
        payload.update({key: value for key, value in (overrides or {}).items() if value is not None})
        request = self.submit(
            remote_id, "job.rerun", payload,
            idempotency_key=idempotency_key, expected_state_epoch=expected_state_epoch,
        )
        if request["status"] != "creating":
            return request
        return self.run_request(remote_id, request, expected_state_epoch=expected_state_epoch)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_payload(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        validate_request(method, payload)
        return dict(payload)

    def _build_request_frame(self, remote_id: str, request: dict[str, Any]) -> dict[str, Any]:
        return {
            "version": "1",
            "kind": "request",
            "request_id": request["request_id"],
            "idempotency_key": request["idempotency_key"],
            "method": request["method"],
            "payload": request["payload"],
            "digest": request["digest"],
            "sent_at": now_iso(),
        }

    def _lost(self, remote_id: str, request: dict[str, Any]) -> dict[str, Any]:
        """Transport failure: keep the request durably ``submitting`` (lost)."""
        self.store.db.execute("BEGIN IMMEDIATE")
        try:
            current = self.store.db.execute(
                "SELECT status FROM remote_requests WHERE request_id=?", (request["request_id"],)
            ).fetchone()
            if current and current["status"] == "submitting":
                # Leave status as-is so replay returns the same in-flight request
                # and never starts a second job; the remote operation is keyed by
                # idempotency_key and will accept only the first submission.
                pass
            self.store.db.commit()
        except BaseException:
            self.store.db.rollback()
        return self.store.get_request_by_key(remote_id, request["idempotency_key"])

    def _finish_response(self, remote_id: str, request: dict[str, Any], response: dict[str, Any]) -> dict[str, Any]:
        result = response.get("result") or {}
        completed = self.store.update_request_status(
            request["request_id"], "completed", response=result
        )
        remote_job_id = result.get("job_id")
        if remote_job_id:
            self.store.upsert_shadow(
                remote_id=remote_id,
                remote_job_id=str(remote_job_id),
                status=(result.get("status") or "running"),
                payload=result,
            )
        return completed

    def _finish_error(self, remote_id: str, request: dict[str, Any], response: dict[str, Any]) -> dict[str, Any]:
        code = response.get("code") or "INVALID_REQUEST"
        message = response.get("message") or ""
        failed = self.store.update_request_status(
            request["request_id"], "failed", error=f"{code}: {message}"
        )
        self.store.record_replay_tombstone(remote_id, request["idempotency_key"], request["digest"])
        return failed
