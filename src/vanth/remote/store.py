"""Durable stores for the remote execution protocol (Phase 0).

Two stores share one idempotency contract:

- :class:`RemoteStore` lives on the **controller** (local) side and records
  remote instances, durable requests keyed by ``(remote_id,
  idempotency_key)``, replay tombstones, and minimal remote job shadows.
- :class:`RemoteOperationStore` lives on the **remote** side and records
  accepted operations keyed by ``idempotency_key`` plus replay tombstones.

Both reuse ``configure_connection`` from ``vanth.migrations`` (WAL, 30s busy
timeout, foreign keys) and the ``now_iso()`` timestamp convention from
``vanth.server``. All request mutations run in an explicit transaction so a
crash before commit leaves no trace and a crash after commit is replay-safe.
"""

from __future__ import annotations

import json
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..migrations import configure_connection
from ..server import now_iso
from .protocol import VanthRemoteProtocolError

CONTROLLER_DDL = """
CREATE TABLE IF NOT EXISTS remotes (
  remote_id TEXT PRIMARY KEY,
  name TEXT,
  target TEXT NOT NULL,
  state TEXT NOT NULL,
  state_epoch INTEGER NOT NULL DEFAULT 1,
  controller_id TEXT,
  credential_state TEXT,
  pairing_state TEXT,
  key_path TEXT,
  known_hosts_path TEXT,
  installed_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS remote_requests (
  request_id TEXT PRIMARY KEY,
  remote_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  method TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  status TEXT NOT NULL,
  response_json TEXT,
  error_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(remote_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS remote_replay_tombstones (
  tombstone_id TEXT PRIMARY KEY,
  remote_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(remote_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS remote_shadows (
  shadow_id TEXT PRIMARY KEY,
  remote_id TEXT NOT NULL,
  remote_job_id TEXT NOT NULL,
  status TEXT NOT NULL,
  payload_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_remote_requests_remote ON remote_requests(remote_id, created_at);
CREATE INDEX IF NOT EXISTS idx_remote_shadows_remote ON remote_shadows(remote_id, created_at);
"""

REMOTE_OPERATION_DDL = """
CREATE TABLE IF NOT EXISTS remote_operations (
  op_id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL,
  method TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(idempotency_key)
);
CREATE TABLE IF NOT EXISTS remote_replay_tombstones (
  tombstone_id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(idempotency_key)
);
"""


class RemoteStore:
    """Controller-side durable store for remotes and requests."""

    def __init__(self, db) -> None:
        self.db = db
        self.db.executescript(CONTROLLER_DDL)
        self.db.commit()

    # ------------------------------------------------------------------
    # Remotes
    # ------------------------------------------------------------------

    def create_remote(
        self,
        *,
        name: str | None = None,
        target: str,
        state: str = "unpaired",
        state_epoch: int = 1,
        controller_id: str | None = None,
        credential_state: str | None = None,
        pairing_state: str = "idle",
        key_path: str | None = None,
        known_hosts_path: str | None = None,
        installed_at: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(target, str) or not target.strip():
            raise ValueError("target must be a non-empty string")
        remote_id = "rmt_" + secrets.token_hex(16)
        stamp = now_iso()
        self.db.execute(
            """
            INSERT INTO remotes(remote_id, name, target, state, state_epoch, controller_id,
              credential_state, pairing_state, key_path, known_hosts_path, installed_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                remote_id, name, target, state, state_epoch, controller_id,
                credential_state, pairing_state, key_path, known_hosts_path, installed_at, stamp, stamp,
            ),
        )
        self.db.commit()
        return self._remote_dict(self.db.execute(
            "SELECT * FROM remotes WHERE remote_id=?", (remote_id,)
        ).fetchone())

    def get_remote(self, remote_id: str) -> dict[str, Any]:
        row = self.db.execute("SELECT * FROM remotes WHERE remote_id=?", (remote_id,)).fetchone()
        if not row:
            raise ValueError(f"Unknown remote_id: {remote_id}")
        return self._remote_dict(row)

    def list_remotes(self) -> list[dict[str, Any]]:
        return [
            self._remote_dict(row)
            for row in self.db.execute("SELECT * FROM remotes ORDER BY created_at ASC").fetchall()
        ]

    def update_remote_state(self, remote_id: str, state: str) -> dict[str, Any]:
        transition(self._current_remote_state(remote_id), state, machine="pairing")
        self.db.execute(
            "UPDATE remotes SET state=?, updated_at=? WHERE remote_id=?",
            (state, now_iso(), remote_id),
        )
        self.db.commit()
        return self.get_remote(remote_id)

    def _current_remote_state(self, remote_id: str) -> str:
        row = self.db.execute("SELECT state FROM remotes WHERE remote_id=?", (remote_id,)).fetchone()
        if not row:
            raise ValueError(f"Unknown remote_id: {remote_id}")
        return row[0]

    def check_state_epoch(self, remote_id: str, expected_epoch: int | None) -> None:
        """Refuse to mutate through a stale state epoch.

        ``expected_epoch`` is the remote's current ``state_epoch`` as reported
        by its latest ``hello`` frame. When it is ``None`` (no handshake yet)
        nothing is enforced; when it disagrees with the controller's stored
        expectation the call raises ``VanthRemoteError`` so a restored or
        rotated timeline can never be mutated through a stale snapshot.
        """
        if expected_epoch is None:
            return
        row = self.db.execute("SELECT state_epoch FROM remotes WHERE remote_id=?", (remote_id,)).fetchone()
        if not row:
            raise ValueError(f"Unknown remote_id: {remote_id}")
        if int(row["state_epoch"]) != expected_epoch:
            from .ssh import VanthRemoteError

            raise VanthRemoteError(
                f"state_epoch mismatch: controller expects {row['state_epoch']}, remote reported {expected_epoch}"
            )

    def set_state_epoch(self, remote_id: str, state_epoch: int) -> dict[str, Any]:
        if isinstance(state_epoch, bool) or not isinstance(state_epoch, int) or state_epoch < 1:
            raise ValueError("state_epoch must be an integer >= 1")
        self.db.execute(
            "UPDATE remotes SET state_epoch=?, updated_at=? WHERE remote_id=?",
            (state_epoch, now_iso(), remote_id),
        )
        self.db.commit()
        return self.get_remote(remote_id)

    @staticmethod
    def _remote_dict(row) -> dict[str, Any]:
        return dict(row)

    # ------------------------------------------------------------------
    # Durable requests
    # ------------------------------------------------------------------

    def record_request(
        self,
        *,
        remote_id: str,
        idempotency_key: str,
        method: str,
        payload: dict[str, Any],
        digest: str,
        commit: bool = True,
    ) -> dict[str, Any]:
        if not self.db.execute("SELECT remote_id FROM remotes WHERE remote_id=?", (remote_id,)).fetchone():
            raise ValueError(f"Unknown remote_id: {remote_id}")
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM remote_requests WHERE remote_id=? AND idempotency_key=?",
                (remote_id, idempotency_key),
            ).fetchone()
            if existing:
                if existing["digest"] != digest:
                    raise VanthRemoteProtocolError("PROTOCOL_REPLAY_MISMATCH")
                result = self._request_dict(existing)
                if commit:
                    self.db.commit()
                return result
            request_id = "req_" + secrets.token_hex(16)
            stamp = now_iso()
            self.db.execute(
                """
                INSERT INTO remote_requests(request_id, remote_id, idempotency_key, method, payload_json,
                  digest, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'creating', ?, ?)
                """,
                (
                    request_id, remote_id, idempotency_key, method,
                    json.dumps(payload, separators=(",", ":")), digest, stamp, stamp,
                ),
            )
            result = self._request_dict(self.db.execute(
                "SELECT * FROM remote_requests WHERE request_id=?", (request_id,)
            ).fetchone())
            if commit:
                self.db.commit()
            return result
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def get_request_by_key(self, remote_id: str, idempotency_key: str) -> dict[str, Any]:
        row = self.db.execute(
            "SELECT * FROM remote_requests WHERE remote_id=? AND idempotency_key=?",
            (remote_id, idempotency_key),
        ).fetchone()
        if not row:
            raise ValueError(f"no request found for key {idempotency_key!r} on remote {remote_id!r}")
        return self._request_dict(row)

    def record_submitting_shadow(self, remote_id: str, request: dict[str, Any]) -> dict[str, Any]:
        """Create the ``submitting`` shadow in the same transaction as the request.

        Called inside the caller's ``BEGIN IMMEDIATE``; never commits. The
        shadow's ``remote_job_id`` is not yet known, so it is keyed by the
        request id. ``update_request_status`` (which commits) also persists the
        final shadow once the remote returns a job id.
        """
        remote_job_id = request["request_id"]
        payload = {
            "request_id": request["request_id"],
            "idempotency_key": request["idempotency_key"],
            "method": request["method"],
            "status": "submitting",
        }
        if not self.db.execute(
            "SELECT shadow_id FROM remote_shadows WHERE remote_id=? AND remote_job_id=?",
            (remote_id, remote_job_id),
        ).fetchone():
            stamp = now_iso()
            self.db.execute(
                """
                INSERT INTO remote_shadows(shadow_id, remote_id, remote_job_id, status, payload_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("shd_" + secrets.token_hex(16), remote_id, remote_job_id, "submitting",
                 json.dumps(payload, separators=(",", ":")), stamp, stamp),
            )
        return self._shadow_dict(self.db.execute(
            "SELECT * FROM remote_shadows WHERE remote_id=? AND remote_job_id=?",
            (remote_id, remote_job_id),
        ).fetchone())

    def update_request_status(
        self, request_id: str, status: str, *, response: dict[str, Any] | None = None, error: str | None = None,
        commit: bool = True
    ) -> dict[str, Any]:
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT status FROM remote_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if not row:
                raise ValueError(f"Unknown request_id: {request_id}")
            transition(row[0], status, machine="request")
            response_json = json.dumps(response, separators=(",", ":")) if response is not None else None
            error_json = json.dumps(error, separators=(",", ":")) if error is not None else None
            self.db.execute(
                """
                UPDATE remote_requests SET status=?, response_json=COALESCE(?, response_json),
                  error_json=COALESCE(?, error_json), updated_at=?
                WHERE request_id=?
                """,
                (status, response_json, error_json, now_iso(), request_id),
            )
            if commit:
                self.db.commit()
            return self._request_dict(self.db.execute(
                "SELECT * FROM remote_requests WHERE request_id=?", (request_id,)
            ).fetchone())
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    @staticmethod
    def _request_dict(row) -> dict[str, Any]:
        result = dict(row)
        payload = result.pop("payload_json", None)
        result["payload"] = json.loads(payload) if payload is not None else None
        result["response"] = json.loads(result["response_json"]) if result.get("response_json") else None
        result["error"] = json.loads(result["error_json"]) if result.get("error_json") else None
        return result

    # ------------------------------------------------------------------
    # Replay tombstones
    # ------------------------------------------------------------------

    def record_replay_tombstone(self, remote_id: str, idempotency_key: str, digest: str,
                                commit: bool = True) -> dict[str, Any]:
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM remote_replay_tombstones WHERE remote_id=? AND idempotency_key=?",
                (remote_id, idempotency_key),
            ).fetchone()
            if existing:
                result = self._tombstone_dict(existing)
                if commit:
                    self.db.commit()
                return result
            tombstone_id = "tomb_" + secrets.token_hex(16)
            self.db.execute(
                """
                INSERT INTO remote_replay_tombstones(tombstone_id, remote_id, idempotency_key, digest, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (tombstone_id, remote_id, idempotency_key, digest, now_iso()),
            )
            result = self._tombstone_dict(self.db.execute(
                "SELECT * FROM remote_replay_tombstones WHERE tombstone_id=?", (tombstone_id,)
            ).fetchone())
            if commit:
                self.db.commit()
            return result
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def get_replay_tombstone(self, remote_id: str, idempotency_key: str) -> dict[str, Any]:
        row = self.db.execute(
            "SELECT * FROM remote_replay_tombstones WHERE remote_id=? AND idempotency_key=?",
            (remote_id, idempotency_key),
        ).fetchone()
        if not row:
            raise ValueError(f"no replay tombstone for key {idempotency_key!r} on remote {remote_id!r}")
        return self._tombstone_dict(row)

    @staticmethod
    def _tombstone_dict(row) -> dict[str, Any]:
        return dict(row)

    # ------------------------------------------------------------------
    # Remote job shadows
    # ------------------------------------------------------------------

    def upsert_shadow(
        self,
        *,
        remote_id: str,
        remote_job_id: str,
        status: str,
        payload: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> dict[str, Any]:
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT shadow_id FROM remote_shadows WHERE remote_id=? AND remote_job_id=?",
                (remote_id, remote_job_id),
            ).fetchone()
            stamp = now_iso()
            payload_json = json.dumps(payload, separators=(",", ":")) if payload is not None else None
            if existing:
                self.db.execute(
                    "UPDATE remote_shadows SET status=?, payload_json=?, updated_at=? WHERE shadow_id=?",
                    (status, payload_json, stamp, existing[0]),
                )
            else:
                self.db.execute(
                    """
                    INSERT INTO remote_shadows(shadow_id, remote_id, remote_job_id, status, payload_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("shd_" + secrets.token_hex(16), remote_id, remote_job_id, status, payload_json, stamp, stamp),
                )
            result = self._shadow_dict(self.db.execute(
                "SELECT * FROM remote_shadows WHERE remote_id=? AND remote_job_id=?",
                (remote_id, remote_job_id),
            ).fetchone())
            if commit:
                self.db.commit()
            return result
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    @staticmethod
    def _shadow_dict(row) -> dict[str, Any]:
        result = dict(row)
        result["payload"] = json.loads(result["payload_json"]) if result.get("payload_json") else None
        return result


class RemoteOperationStore:
    """Remote-side durable store for accepted operations and tombstones."""

    def __init__(self, db) -> None:
        self.db = db
        self.db.executescript(REMOTE_OPERATION_DDL)
        self.db.commit()

    def record_operation(
        self, *, idempotency_key: str, method: str, payload: dict[str, Any], digest: str,
        commit: bool = True,
    ) -> dict[str, Any]:
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM remote_operations WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing:
                if existing["digest"] != digest:
                    raise VanthRemoteProtocolError("PROTOCOL_REPLAY_MISMATCH")
                result = self._operation_dict(existing)
                if commit:
                    self.db.commit()
                return result
            op_id = "op_" + secrets.token_hex(16)
            stamp = now_iso()
            self.db.execute(
                """
                INSERT INTO remote_operations(op_id, idempotency_key, method, payload_json, digest, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'accepted', ?, ?)
                """,
                (
                    op_id, idempotency_key, method,
                    json.dumps(payload, separators=(",", ":")), digest, stamp, stamp,
                ),
            )
            result = self._operation_dict(self.db.execute(
                "SELECT * FROM remote_operations WHERE op_id=?", (op_id,)
            ).fetchone())
            if commit:
                self.db.commit()
            return result
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def get_operation(self, idempotency_key: str) -> dict[str, Any]:
        row = self.db.execute(
            "SELECT * FROM remote_operations WHERE idempotency_key=?", (idempotency_key,)
        ).fetchone()
        if not row:
            raise ValueError(f"no operation found for key {idempotency_key!r}")
        return self._operation_dict(row)

    def update_operation_status(
        self, op_id: str, status: str, *, payload: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> dict[str, Any]:
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT status FROM remote_operations WHERE op_id=?", (op_id,)
            ).fetchone()
            if not row:
                raise ValueError(f"Unknown op_id: {op_id}")
            transition(row[0], status, machine="operation")
            payload_json = json.dumps(payload, separators=(",", ":")) if payload is not None else None
            self.db.execute(
                """
                UPDATE remote_operations SET status=?, payload_json=COALESCE(?, payload_json), updated_at=?
                WHERE op_id=?
                """,
                (status, payload_json, now_iso(), op_id),
            )
            if commit:
                self.db.commit()
            return self._operation_dict(self.db.execute(
                "SELECT * FROM remote_operations WHERE op_id=?", (op_id,)
            ).fetchone())
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def record_queued_job(self, *, op_id: str, remote_job_id: str, origin: str) -> None:
        """Persist the job-origin mapping that keeps a remote job recoverable.

        Runs inside the caller's ``BEGIN IMMEDIATE`` (the acceptance
        transaction); never commits. The mapping maps an operation's ``op_id``
        to the ``job_`` row the remote daemon inserted plus a durable launch
        intent (status ``queued``). It is a separate table from the local
        ``jobs`` table so remote machinery never feeds local PID/heartbeat/
        cleanup code.
        """
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS remote_job_origins (
              op_id TEXT PRIMARY KEY,
              remote_job_id TEXT NOT NULL,
              origin TEXT NOT NULL,
              launch_intent TEXT NOT NULL,
              created_at TEXT NOT NULL,
              UNIQUE(remote_job_id)
            )
            """,
        )
        self.db.execute(
            """
            INSERT OR REPLACE INTO remote_job_origins(op_id, remote_job_id, origin, launch_intent, created_at)
            VALUES (?, ?, ?, 'queued', ?)
            """,
            (op_id, remote_job_id, origin, now_iso()),
        )

    def record_replay_tombstone(self, idempotency_key: str, digest: str) -> dict[str, Any]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM remote_replay_tombstones WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing:
                result = self._tombstone_dict(existing)
                self.db.commit()
                return result
            tombstone_id = "tomb_" + secrets.token_hex(16)
            self.db.execute(
                """
                INSERT INTO remote_replay_tombstones(tombstone_id, idempotency_key, digest, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (tombstone_id, idempotency_key, digest, now_iso()),
            )
            result = self._tombstone_dict(self.db.execute(
                "SELECT * FROM remote_replay_tombstones WHERE tombstone_id=?", (tombstone_id,)
            ).fetchone())
            self.db.commit()
            return result
        except BaseException:
            self.db.rollback()
            raise

    def get_replay_tombstone(self, idempotency_key: str) -> dict[str, Any]:
        row = self.db.execute(
            "SELECT * FROM remote_replay_tombstones WHERE idempotency_key=?", (idempotency_key,)
        ).fetchone()
        if not row:
            raise ValueError(f"no replay tombstone for key {idempotency_key!r}")
        return self._tombstone_dict(row)

    def get_remote_job_origin(self, remote_job_id: str) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT * FROM remote_job_origins WHERE remote_job_id=?", (remote_job_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_remote_job_origin_op(self, op_id: str) -> str | None:
        row = self.db.execute(
            "SELECT remote_job_id FROM remote_job_origins WHERE op_id=?", (op_id,)
        ).fetchone()
        return row[0] if row else None

    def get_remote_operation_by_job(self, remote_job_id: str) -> str | None:
        row = self.db.execute(
            "SELECT op_id FROM remote_job_origins WHERE remote_job_id=?", (remote_job_id,)
        ).fetchone()
        return row[0] if row else None

    @staticmethod
    def _operation_dict(row) -> dict[str, Any]:
        result = dict(row)
        payload = result.pop("payload_json", None)
        result["payload"] = json.loads(payload) if payload is not None else None
        return result

    @staticmethod
    def _tombstone_dict(row) -> dict[str, Any]:
        return dict(row)


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------

TRANSITION_TABLES: dict[str, dict[str, dict[str, str]]] = {
    "pairing": {
        "unpaired": {"pairing": "pairing"},
        "pairing": {"paired": "paired", "error": "error"},
        "paired": {},
        "error": {},
    },
    "request": {
        "creating": {"submitting": "submitting"},
        "submitting": {"accepted": "accepted"},
        "accepted": {"completed": "completed", "failed": "failed", "lost": "lost"},
        "completed": {},
        "failed": {},
        "lost": {},
    },
    "operation": {
        "accepted": {"queued": "queued"},
        "queued": {"launched": "launched"},
        "launched": {"running": "running"},
        "running": {"completed": "completed", "failed": "failed"},
        "completed": {},
        "failed": {},
    },
}


def transition(current: str, event: str, *, machine: str) -> str:
    """Return the next state for ``event`` from ``current``, or raise ValueError."""
    table = TRANSITION_TABLES.get(machine)
    if table is None:
        raise ValueError(f"unknown state machine: {machine}")
    if current not in table:
        raise ValueError(f"invalid state {current!r} for {machine} state machine")
    if event not in table[current]:
        raise ValueError(
            f"invalid transition {current!r} --({event})--> ? for {machine} state machine"
        )
    return table[current][event]
