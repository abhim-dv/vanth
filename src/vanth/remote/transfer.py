"""Brokered artifact transfer over the bulk SSH session seam (Phase 9).

Two sides, one module:

- :class:`TransferRegistry` runs on the REMOTE side (wired into
  ``RemoteJobManager.handle_request``) and implements the
  ``artifact.transfer_init`` / ``artifact.blob_chunk`` /
  ``artifact.transfer_complete`` methods. Transfers live in the
  ``remote_transfers`` table on the remote database; chunks are verified for
  strict offset continuity and per-chunk sha256 before they are appended to a
  staging file and acknowledged in ONE transaction. Publication happens only
  at ``transfer_complete``, riding the EXISTING fenced durable-operation
  pattern (:meth:`ArtifactOperations.put_file`) keyed by an idempotency key
  derived from the transfer id — no parallel durability mechanism.

- :class:`RemoteArtifactBroker` runs on the CONTROLLER side and brokers
  publication/materialization over ONE bulk SSH session per transfer
  (``control.transport.open_session``). Controller-side resume state lives in
  the ``controller_transfers`` table on the ARTIFACTS CATALOG database
  (chosen because the broker already holds ``ops.catalog.db`` there and the
  transfer is an artifact-lifecycle concern, not a request-row concern).
  Every acknowledged offset is persisted durably before the next chunk is
  sent; a retry with the same idempotency key resumes from the acknowledged
  offset and never re-sends acked bytes.

Security invariants: nothing but artifact bytes (base64) and content
identifiers crosses the wire — no storage-profile credentials, tokens, or
config ever enter a transfer frame. Byte counts are exact (every chunk is
verified; total == manifest size before completion), and BOTH sides hash the
content: the controller knows the expected whole-blob sha256 up front and the
remote hashes what it actually received before publishing.

State-epoch invariant: every init/chunk/complete response carries the
remote's CURRENT state epoch. If it changes mid-transfer the transfer is
aborted permanently ("epoch changed; transfer stopped rather than rebound") —
never rebound onto the new timeline.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
from pathlib import Path
from typing import Any, Callable

from .protocol import (
    IDEMPOTENCY_KEY_RE,
    VanthRemoteProtocolError,
    decode_frame,
    encode_frame,
    request_digest,
)
from ..server import now_iso

__all__ = [
    "TransferRegistry",
    "RemoteArtifactBroker",
    "TransferInterrupted",
    "TransferAborted",
    "EPOCH_STOP_MESSAGE",
    "DEFAULT_CHUNK_BYTES",
    "_TRANSFER_METHODS",
]

_TRANSFER_METHODS = frozenset(
    {"artifact.transfer_init", "artifact.blob_chunk", "artifact.transfer_complete"}
)

REMOTE_TRANSFER_DDL = """
CREATE TABLE IF NOT EXISTS remote_transfers (
  transfer_id TEXT PRIMARY KEY,
  direction TEXT NOT NULL,
  root_name TEXT NOT NULL,
  manifest_digest TEXT NOT NULL,
  total_bytes INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  acked_offset INTEGER NOT NULL DEFAULT 0,
  staging_path TEXT NOT NULL,
  state_epoch INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""

# Mirrors remote_transfers plus op_claim_token (the fence token that gates
# every controller-side offset/completion update). Lives on the artifacts
# CATALOG database (ops.catalog.db): the transfer is an artifact-lifecycle
# concern and the broker already owns that connection.
CONTROLLER_TRANSFER_DDL = """
CREATE TABLE IF NOT EXISTS controller_transfers (
  transfer_id TEXT PRIMARY KEY,
  remote_id TEXT NOT NULL,
  direction TEXT NOT NULL,
  root_name TEXT NOT NULL,
  manifest_digest TEXT NOT NULL,
  total_bytes INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  acked_offset INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT NOT NULL UNIQUE,
  op_claim_token TEXT,
  state_epoch INTEGER,
  status TEXT NOT NULL DEFAULT 'active',
  error TEXT,
  result_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""

EPOCH_STOP_MESSAGE = "epoch changed; transfer stopped rather than rebound"

DEFAULT_CHUNK_BYTES = 256 * 1024

_EXPECTED_OFFSET_RE = re.compile(r"expected_offset=(\d+)")


def _mint_key(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(12)}"


def _valid_key(key: str | None) -> bool:
    return isinstance(key, str) and bool(IDEMPOTENCY_KEY_RE.match(key))


class TransferInterrupted(RuntimeError):
    """Transport-level interruption; the transfer resumes from its recorded
    acknowledged offset when retried with the SAME idempotency key."""

    def __init__(self, transfer_id: str, acked_offset: int, message: str = "transfer interrupted") -> None:
        super().__init__(message)
        self.transfer_id = transfer_id
        self.acked_offset = int(acked_offset)


class TransferAborted(RuntimeError):
    """Permanent failure (epoch change, tamper); retrying must not resume."""


class TransferRegistry:
    """Remote-side registry of bulk artifact transfers (Phase 9)."""

    def __init__(self, db, *, epoch_fn: Callable[[], int], ops_factory: Callable[[], Any] | None = None,
                 staging_dir: str | Path | None = None) -> None:
        self.db = db
        self.db.executescript(REMOTE_TRANSFER_DDL)
        self.db.commit()
        self._epoch_fn = epoch_fn
        self._ops_factory = ops_factory
        self.staging_dir = Path(staging_dir) if staging_dir else None
        self._ops: Any = None

    # ------------------------------------------------------------------
    # Remote-side ArtifactOperations (lazy: only publication needs it)
    # ------------------------------------------------------------------

    def ops(self) -> Any:
        if self._ops is None:
            if self._ops_factory is None:
                raise RuntimeError("remote artifact operations are unavailable")
            self._ops = self._ops_factory()
        return self._ops

    def _staging_path(self, transfer_id: str) -> str:
        base = self.staging_dir
        if base is None:
            from ..paths import canonical_home

            base = canonical_home() / "remote-transfers"
        base.mkdir(parents=True, exist_ok=True)
        return str(base / f"{transfer_id}.part")

    # ------------------------------------------------------------------
    # Handlers (called from RemoteJobManager routing)
    # ------------------------------------------------------------------

    def init(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Register or resume a durable transfer. Returns the resume point."""
        transfer_id = payload["transfer_id"]
        direction = payload["direction"]
        now = now_iso()
        epoch = int(self._epoch_fn())
        staging_path = self._staging_path(transfer_id)
        root_name = str(payload.get("root_name") or "")
        manifest_digest = str(payload.get("manifest_digest") or "")
        total_bytes = int(payload.get("total_bytes") or 0)
        sha256 = str(payload.get("sha256") or "")
        version_id = payload.get("version_id")
        if direction == "pull":
            # The remote's own catalog is authoritative for pull transfers.
            row_v = self.ops().catalog.db.execute(
                "SELECT * FROM versions WHERE version_id=?", (version_id,)
            ).fetchone()
            if not row_v:
                raise VanthRemoteProtocolError("INVALID_REQUEST", f"unknown remote version: {version_id!r}")
            import json as _json

            manifest = _json.loads(row_v["manifest_json"])
            root_row = self.ops().catalog.db.execute(
                "SELECT name FROM roots WHERE root_id=?", (row_v["root_id"],)
            ).fetchone()
            root_name = root_row["name"] if root_row else ""
            manifest_digest = row_v["manifest_digest"]
            total_bytes = int(row_v["size_bytes"])
            sha256 = manifest["sha256"]
            blob_path = self.ops().blobs.blob_path(sha256)
            if not blob_path.is_file():
                raise VanthRemoteProtocolError(
                    "INVALID_REQUEST", f"remote blob missing for version {version_id!r}: {sha256}"
                )
            staging_path = str(blob_path)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM remote_transfers WHERE transfer_id=?", (transfer_id,)
            ).fetchone()
            if existing:
                if (
                    existing["direction"] != direction
                    or existing["root_name"] != root_name
                    or existing["manifest_digest"] != manifest_digest
                    or int(existing["total_bytes"]) != total_bytes
                    or existing["sha256"] != sha256
                ):
                    raise VanthRemoteProtocolError(
                        "PROTOCOL_REPLAY_MISMATCH",
                        "transfer_id was reused with different content identity",
                    )
                result = {
                    "transfer_id": transfer_id,
                    "direction": direction,
                    "root_name": root_name,
                    "manifest_digest": manifest_digest,
                    "total_bytes": total_bytes,
                    "sha256": sha256,
                    "acked_offset": int(existing["acked_offset"]),
                    "resumed": True,
                    "state_epoch": int(self._epoch_fn()),
                }
                self.db.execute(
                    "UPDATE remote_transfers SET updated_at=? WHERE transfer_id=?", (now, transfer_id)
                )
                self.db.commit()
                return result
            self.db.execute(
                """
                INSERT INTO remote_transfers(transfer_id, direction, root_name, manifest_digest,
                  total_bytes, sha256, acked_offset, staging_path, state_epoch, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                """,
                (transfer_id, direction, root_name, manifest_digest, total_bytes, sha256,
                 staging_path, epoch, now, now),
            )
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {
            "transfer_id": transfer_id,
            "direction": direction,
            "root_name": root_name,
            "manifest_digest": manifest_digest,
            "total_bytes": total_bytes,
            "sha256": sha256,
            "acked_offset": 0,
            "resumed": False,
            "state_epoch": epoch,
        }

    def chunk(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Handle one chunk frame in the transfer's registered direction.

        Push: verify strict offset continuity + chunk sha256, append to the
        staging file at exactly that offset, advance acked_offset in one tx.
        Pull: serve the requested byte window back to the controller.
        """
        transfer_id = payload["transfer_id"]
        row = self._get(transfer_id)
        if not row:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"unknown transfer: {transfer_id!r}")
        current_epoch = int(self._epoch_fn())
        if current_epoch != int(row["state_epoch"]):
            raise VanthRemoteProtocolError("INVALID_REQUEST", EPOCH_STOP_MESSAGE)
        if row["direction"] == "pull":
            return self._serve_chunk(row, payload)
        return self._receive_chunk(row, payload)

    def _receive_chunk(self, row, payload: dict[str, Any]) -> dict[str, Any]:
        data_b64 = payload.get("data_b64")
        chunk_sha = payload.get("sha256")
        if not isinstance(data_b64, str) or chunk_sha is None:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "push chunk requires data_b64 and sha256"
            )
        try:
            data = base64.b64decode(data_b64, validate=True)
        except Exception:
            raise VanthRemoteProtocolError("INVALID_REQUEST", "data_b64 is not valid base64") from None
        offset = int(payload["offset"])
        acked_offset = int(row["acked_offset"])
        if offset != acked_offset:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST",
                f"chunk offset discontinuity: expected_offset={acked_offset}",
            )
        if hashlib.sha256(data).hexdigest() != chunk_sha:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", f"chunk sha256 mismatch at offset {offset}"
            )
        total = int(row["total_bytes"])
        if offset + len(data) > total:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "chunk exceeds declared total_bytes"
            )
        # Seek+write at exactly the acknowledged offset (never O_APPEND: the
        # write MUST land at `offset`). A crash between the write and the DB
        # update makes a resume rewrite the same bytes — never a duplicate
        # append.
        staged = Path(row["staging_path"])
        if not staged.exists():
            staged.parent.mkdir(parents=True, exist_ok=True)
            staged.touch()
        with staged.open("r+b") as handle:
            handle.seek(offset)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                "UPDATE remote_transfers SET acked_offset=?, updated_at=? "
                "WHERE transfer_id=? AND acked_offset=?",
                (offset + len(data), now_iso(), row["transfer_id"], acked_offset),
            ).rowcount
            if not changed:
                raise ValueError("concurrent chunk writer lost the ack race")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {
            "transfer_id": row["transfer_id"],
            "acked_offset": offset + len(data),
            "state_epoch": int(self._epoch_fn()),
        }

    def _serve_chunk(self, row, payload: dict[str, Any]) -> dict[str, Any]:
        offset = int(payload["offset"])
        size = int(payload.get("size") or DEFAULT_CHUNK_BYTES)
        if size <= 0:
            raise VanthRemoteProtocolError("INVALID_REQUEST", "size must be > 0 for pull chunks")
        total = int(row["total_bytes"])
        # Pull chunks are replayable (review P1-14): the controller may request
        # any in-range offset — including one it already holds — because its
        # ledger is authoritative. Enforce range, not continuity.
        if offset < 0 or offset >= total:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", f"chunk offset out of range: {offset} (total={total})"
            )
        with open(row["staging_path"], "rb") as handle:
            handle.seek(offset)
            data = handle.read(min(size, max(0, total - offset)))
        # Pull chunks are REPLAYABLE: the durable resume point is NOT advanced
        # here. Advancing on serve meant a lost response left the remote
        # believing bytes were delivered that the controller never received,
        # and retrying the prior offset failed (review P1-14). The
        # controller-side ledger is authoritative for pull resume.
        return {
            "transfer_id": row["transfer_id"],
            "offset": offset,
            "size": len(data),
            "data_b64": base64.b64encode(data).decode("ascii"),
            "sha256": hashlib.sha256(data).hexdigest(),
            # Replayable chunks: the served window end is informational only;
            # the DURABLE remote offset advances solely via push-style chunk
            # acceptance, never on serve (review P1-14).
            "served_to": offset + len(data),
            "acked_offset": int(row["acked_offset"]),
            "state_epoch": int(self._epoch_fn()),
        }

    def complete(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Finalize a transfer after both sides agree on the whole-content sha.

        Push: the remote hashes ITS staged file, refuses unless size and hash
        match the registered identity, then publishes through the standard
        fenced durable operation (:meth:`ArtifactOperations.put_file`) under
        an idempotency key derived from the transfer id — replays collapse to
        the same version, so a duplicate completion can never publish twice.
        Pull: verifies source stability and returns the existing version id.
        """
        transfer_id = payload["transfer_id"]
        row = self._get(transfer_id)
        if not row:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"unknown transfer: {transfer_id!r}")
        current_epoch = int(self._epoch_fn())
        if current_epoch != int(row["state_epoch"]):
            raise VanthRemoteProtocolError("INVALID_REQUEST", EPOCH_STOP_MESSAGE)
        staged = Path(row["staging_path"])
        if not staged.is_file():
            raise VanthRemoteProtocolError("INVALID_REQUEST", "staged content is missing")
        actual_size = staged.stat().st_size
        if actual_size != int(row["total_bytes"]):
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST",
                f"incomplete transfer: expected {int(row['total_bytes'])} bytes, staged {actual_size}",
            )
        digest = hashlib.sha256()
        with staged.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        actual_sha = digest.hexdigest()
        expected_sha = str(payload.get("sha256") or row["sha256"])
        if actual_sha != expected_sha or actual_sha != row["sha256"]:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST",
                f"whole-content sha256 mismatch: expected {row['sha256']}, remote hashed {actual_sha}",
            )
        if row["direction"] == "pull":
            vrow = self.ops().catalog.db.execute(
                "SELECT version_id FROM versions WHERE manifest_digest=?", (row["manifest_digest"],)
            ).fetchone()
            if not vrow:
                raise VanthRemoteProtocolError(
                    "INVALID_REQUEST", f"no published remote version for digest {row['manifest_digest']}"
                )
            return {
                "transfer_id": transfer_id,
                "version_id": vrow["version_id"],
                "sha256": actual_sha,
                "total_bytes": int(row["total_bytes"]),
                "verified": True,
                "state_epoch": current_epoch,
            }
        # Publication rides the EXISTING fenced durable-op pattern: put_file
        # begins/replays by this key, claims with lease+generation, and only
        # the live claim can commit the version row — so a duplicated
        # completion (crash between publish and ack) collapses onto the same
        # immutable version.
        data = staged.read_bytes()
        result = self.ops().put_file(
            row["root_name"], data=data, idempotency_key="xfer-" + transfer_id
        )
        if result.get("sha256") != row["sha256"]:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "published version does not match the transfer identity"
            )
        # The staged file is intentionally retained: a replayed completion
        # (crash after publish, before the controller's ack) must be able to
        # re-verify and collapse onto the same version. Reclaiming staging
        # files is GC territory (deferred).
        return {
            "transfer_id": transfer_id,
            "version_id": result["version_id"],
            "deduplicated": bool(result.get("deduplicated")),
            "sha256": row["sha256"],
            "total_bytes": int(row["total_bytes"]),
            "state_epoch": current_epoch,
        }

    # ------------------------------------------------------------------

    def _get(self, transfer_id: str):
        return self.db.execute(
            "SELECT * FROM remote_transfers WHERE transfer_id=?", (transfer_id,)
        ).fetchone()


class RemoteArtifactBroker:
    """Controller-side broker for push/pull artifact transfers (Phase 9).

    Opens ONE bulk session per attempt via ``control.transport.open_session``,
    streams sequentially, persists every acknowledged offset durably before
    sending the next chunk, and stops permanently when the remote's state
    epoch changes mid-transfer.
    """

    def __init__(self, control: Any, ops: Any) -> None:
        self.control = control
        self.ops = ops
        db = ops.catalog.db
        db.executescript(CONTROLLER_TRANSFER_DDL)
        db.commit()
        self.db = db

    @property
    def chunk_bytes(self) -> int:
        try:
            value = int(os.environ.get("VANTH_TRANSFER_CHUNK_BYTES", str(DEFAULT_CHUNK_BYTES)))
        except ValueError:
            value = DEFAULT_CHUNK_BYTES
        return max(1024, value)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push_blob(self, remote_id: str, local_version_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        """Brokered PUBLICATION of a local managed version to a paired remote."""
        key = idempotency_key or _mint_key("push")
        if not _valid_key(key):
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "idempotency_key must be 8..128 chars in [A-Za-z0-9_-]"
            )
        verify = self.ops.verify(local_version_id)
        if not verify.get("ok"):
            raise TransferAborted(
                f"source version failed verification before transfer: {local_version_id}"
            )
        vrow = self.ops.catalog.db.execute(
            "SELECT * FROM versions WHERE version_id=?", (local_version_id,)
        ).fetchone()
        if not vrow:
            raise ValueError(f"Unknown version_id: {local_version_id}")
        import json as _json

        manifest = _json.loads(vrow["manifest_json"])
        if manifest.get("kind") != "file":
            raise ValueError("push_blob supports kind='file' versions only")
        sha256 = manifest["sha256"]
        total = int(vrow["size_bytes"])
        mdigest = vrow["manifest_digest"]
        root_row = self.ops.catalog.db.execute(
            "SELECT name FROM roots WHERE root_id=?", (vrow["root_id"],)
        ).fetchone()
        root_name = root_row["name"] if root_row else local_version_id
        if not (self.ops.blobs.has_blob(sha256) and self.ops.blobs.verify_blob(sha256)):
            raise TransferAborted(f"source blob missing or corrupt: {sha256}")

        # Identity binds REMOTE + direction + caller key + immutable version:
        # the same key/version against a DIFFERENT remote (or destination for
        # pull) must never replay the first ledger entry (review P1-15).
        transfer_id = "xfr_" + hashlib.sha256(
            f"push:{remote_id}:{key}:{local_version_id}".encode("utf-8")
        ).hexdigest()[:32]
        record = self._begin_transfer(
            transfer_id, remote_id, "push", root_name, mdigest, total, sha256, key
        )
        if record["status"] == "completed":
            stored = json.loads(record["result_json"]) if record["result_json"] else {}
            return {**stored, "replayed": True}
        if record["status"] == "failed":
            raise TransferAborted(record["error"] or "transfer previously aborted")

        session, response = self._open_and_init(remote_id, transfer_id, {
            "transfer_id": transfer_id,
            "direction": "push",
            "root_name": root_name,
            "manifest_digest": mdigest,
            "total_bytes": total,
            "sha256": sha256,
        }, record)
        init_result = response["result"]
        epoch = int(init_result["state_epoch"])
        offset = min(int(init_result["acked_offset"]), total)
        # The remote's acknowledged offset is authoritative: adopt it so we
        # never resend bytes it already holds.
        self._persist_progress(transfer_id, record["op_claim_token"],
                               acked_offset=offset, state_epoch=epoch)

        sent_this_attempt = 0
        with self.ops.blobs.blob_path(sha256).open("rb") as blob:
            while offset < total:
                self._check_epoch_record(transfer_id, epoch)
                blob.seek(offset)
                chunk = blob.read(min(self.chunk_bytes, total - offset))
                if not chunk:
                    break
                frame = self._frame("artifact.blob_chunk", {
                    "transfer_id": transfer_id,
                    "offset": offset,
                    "data_b64": base64.b64encode(chunk).decode("ascii"),
                    "sha256": hashlib.sha256(chunk).hexdigest(),
                })
                resp = self._exchange(session, frame)
                outcome = self._classify(resp, epoch, transfer_id)
                if outcome == "retry-offset":
                    match = _EXPECTED_OFFSET_RE.search(resp.get("message") or "")
                    expected = int(match.group(1)) if match else offset
                    offset = min(expected, total)
                    self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=offset)
                    continue
                if outcome == "epoch-stop":
                    self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
                    raise TransferAborted(EPOCH_STOP_MESSAGE)
                if outcome == "error":
                    message = str(resp.get("message") or "chunk rejected")
                    self._mark_failed(transfer_id, record["op_claim_token"], message)
                    raise TransferAborted(message)
                result = resp["result"]
                new_epoch = int(result.get("state_epoch", epoch))
                if new_epoch != epoch:
                    self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
                    raise TransferAborted(EPOCH_STOP_MESSAGE)
                acked = int(result["acked_offset"])
                if acked <= offset:
                    raise TransferInterrupted(transfer_id, offset, "remote ack did not advance")
                offset = acked
                sent_this_attempt += 1
                self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=offset)

        if offset != total:
            self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=offset)
            raise TransferInterrupted(transfer_id, offset)

        complete_frame = self._frame("artifact.transfer_complete", {
            "transfer_id": transfer_id, "sha256": sha256,
        })
        resp = self._exchange(session, complete_frame)
        outcome = self._classify(resp, epoch, transfer_id)
        if outcome == "epoch-stop":
            self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
            raise TransferAborted(EPOCH_STOP_MESSAGE)
        if resp.get("kind") != "response":
            raise TransferInterrupted(transfer_id, offset, str(resp.get("message") or "completion failed"))
        remote_version_id = resp["result"]["version_id"]

        # Source stability at the END: the content-addressed source blob must
        # be byte-for-byte unchanged after the transfer too.
        if not self.ops.blobs.verify_blob(sha256):
            self._mark_failed(transfer_id, record["op_claim_token"], "source blob changed during transfer")
            raise TransferAborted("source blob changed during transfer")

        final = {
            "transfer_id": transfer_id,
            "direction": "push",
            "remote_id": remote_id,
            "version_id": remote_version_id,
            "sha256": sha256,
            "manifest_digest": mdigest,
            "total_bytes": total,
            "bytes_sent_total": total,
            "chunks_sent_this_attempt": sent_this_attempt,
            "completed": True,
        }
        self._mark_completed(transfer_id, record["op_claim_token"], final)
        return final

    def pull_blob(self, remote_id: str, remote_version_id: str, dest_path: str | os.PathLike[str],
                  *, idempotency_key: str | None = None) -> dict[str, Any]:
        """Brokered MATERIALIZATION of a remote version onto this machine."""
        dest = Path(dest_path)
        key = idempotency_key or _mint_key("pull")
        if not _valid_key(key):
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "idempotency_key must be 8..128 chars in [A-Za-z0-9_-]"
            )
        # Destination identity is part of the transfer identity (review P1-15).
        import hashlib as _h

        dest_token = _h.sha256(str(dest.resolve()).encode("utf-8")).hexdigest()[:16]
        transfer_id = "xfr_" + hashlib.sha256(
            f"pull:{remote_id}:{key}:{remote_version_id}:{dest_token}".encode("utf-8")
        ).hexdigest()[:32]
        record = self._begin_transfer(
            transfer_id, remote_id, "pull", "", "", 0, "0" * 64, key
        )
        if record["status"] == "completed":
            stored = json.loads(record["result_json"]) if record["result_json"] else {}
            return {**stored, "replayed": True}
        if record["status"] == "failed":
            raise TransferAborted(record["error"] or "transfer previously aborted")

        session, response = self._open_and_init(remote_id, transfer_id, {
            "transfer_id": transfer_id,
            "direction": "pull",
            "version_id": remote_version_id,
        }, record)
        init_result = response["result"]
        total = int(init_result["total_bytes"])
        sha256 = init_result["sha256"]
        root_name = init_result["root_name"]
        mdigest = init_result["manifest_digest"]
        epoch = int(init_result["state_epoch"])
        offset = int(init_result["acked_offset"])
        self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=offset,
                               state_epoch=epoch, root_name=root_name,
                               manifest_digest=mdigest, total_bytes=total, sha256=sha256)

        staging = dest.parent / f".{dest.name}.pulling-{transfer_id[:12]}"
        staging.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(staging, "a+b") as out:
                out.truncate(offset)
                # On resume the prefix was already verified chunk-by-chunk;
                # re-hash it so the final whole-content check stays exact.
                received_hash = hashlib.sha256()
                out.seek(0)
                for block in iter(lambda: out.read(1024 * 1024), b""):
                    received_hash.update(block)
                while offset < total:
                    self._check_epoch_record(transfer_id, epoch)
                    frame = self._frame("artifact.blob_chunk", {
                        "transfer_id": transfer_id,
                        "offset": offset,
                        "data_b64": "",
                        "sha256": hashlib.sha256(b"").hexdigest(),
                        "size": self.chunk_bytes,
                    })
                    resp = self._exchange(session, frame)
                    outcome = self._classify(resp, epoch, transfer_id)
                    if outcome == "epoch-stop":
                        self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
                        raise TransferAborted(EPOCH_STOP_MESSAGE)
                    if resp.get("kind") != "response":
                        raise TransferInterrupted(transfer_id, offset, str(resp.get("message")))
                    result = resp["result"]
                    new_epoch = int(result.get("state_epoch", epoch))
                    if new_epoch != epoch:
                        self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
                        raise TransferAborted(EPOCH_STOP_MESSAGE)
                    data = base64.b64decode(result["data_b64"], validate=True)
                    if hashlib.sha256(data).hexdigest() != result["sha256"]:
                        self._mark_failed(transfer_id, record["op_claim_token"],
                                          f"pull chunk sha mismatch at offset {offset}")
                        raise TransferAborted(f"pull chunk sha mismatch at offset {offset}")
                    out.seek(offset)
                    out.write(data)
                    received_hash.update(data)
                    # Pull chunks are replayable: advance locally by the
                    # served window; the remote's durable offset does not
                    # move on serve (review P1-14).
                    offset = int(result.get("served_to", offset + len(data)))
                    self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=offset)
            if offset != total or received_hash.hexdigest() != sha256:
                raise TransferInterrupted(transfer_id, offset, "pull did not assemble the full content")
        finally:
            if offset != total:
                try:
                    staging.unlink(missing_ok=True)
                except OSError:
                    pass

        complete_frame = self._frame("artifact.transfer_complete", {
            "transfer_id": transfer_id, "sha256": sha256,
        })
        resp = self._exchange(session, complete_frame)
        if resp.get("kind") != "response":
            raise TransferInterrupted(transfer_id, offset, str(resp.get("message")))

        # Materialize locally through the standard durable-op pattern, then
        # write dest_path atomically via materialize().
        with staging.open("rb") as handle:
            local_bytes = handle.read()
        put = self.ops.put_file(root_name, data=local_bytes, idempotency_key=key + "-localpub")
        mat = self.ops.materialize(put["version_id"], dest, overwrite=True,
                                   idempotency_key=key + "-localmat")
        try:
            staging.unlink(missing_ok=True)
        except OSError:
            pass
        final = {
            "transfer_id": transfer_id,
            "direction": "pull",
            "remote_id": remote_id,
            "remote_version_id": remote_version_id,
            "version_id": put["version_id"],
            "dest_path": str(mat["dest_path"]),
            "sha256": sha256,
            "total_bytes": total,
            "completed": True,
        }
        self._mark_completed(transfer_id, record["op_claim_token"], final)
        return final

    # ------------------------------------------------------------------
    # Session/frame plumbing
    # ------------------------------------------------------------------

    def _session(self, remote_row: dict[str, Any]) -> Any:
        return self.control.transport.open_session(remote_row, home=self.control.home)

    def _open_and_init(self, remote_id: str, transfer_id: str, init_payload: dict[str, Any],
                       record) -> tuple[Any, dict[str, Any]]:
        remote_row = self.control.store.get_remote(remote_id)
        session = self._session(remote_row)
        if session is None:
            raise TransferInterrupted(transfer_id, int(record["acked_offset"]), "cannot open transport session")
        resp = self._exchange(session, self._frame("artifact.transfer_init", init_payload))
        if resp.get("kind") != "response":
            message = str(resp.get("message") or "transfer_init failed")
            if "epoch changed" in message:
                self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
                raise TransferAborted(EPOCH_STOP_MESSAGE)
            raise TransferInterrupted(transfer_id, int(record["acked_offset"]), message)
        if record["state_epoch"] is not None and int(resp["result"]["state_epoch"]) != int(record["state_epoch"]):
            self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
            raise TransferAborted(EPOCH_STOP_MESSAGE)
        return session, resp

    @staticmethod
    def _frame(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        key = _mint_key("trn")
        return {
            "version": "1",
            "kind": "request",
            "request_id": "req_" + secrets.token_hex(16),
            "idempotency_key": key,
            "method": method,
            "payload": payload,
            "digest": request_digest(method, payload, key),
            "sent_at": now_iso(),
        }

    @staticmethod
    def _exchange(session: Any, frame: dict[str, Any]) -> dict[str, Any]:
        line = session.exchange(encode_frame(frame))
        if not line:
            raise ConnectionError("transport returned no response frame")
        return decode_frame(line)

    @staticmethod
    def _classify(resp: dict[str, Any], known_epoch: int, transfer_id: str) -> str:
        """Bucket a response frame: ok / retry-offset / epoch-stop."""
        if resp.get("kind") == "error":
            message = str(resp.get("message") or "")
            if "epoch changed" in message:
                return "epoch-stop"
            if _EXPECTED_OFFSET_RE.search(message):
                return "retry-offset"
            return "error"
        return "ok"

    # ------------------------------------------------------------------
    # Durable controller-side ledger (fenced by op_claim_token)
    # ------------------------------------------------------------------

    def _begin_transfer(self, transfer_id: str, remote_id: str, direction: str, root_name: str,
                        manifest_digest: str, total: int, sha256: str, key: str) -> dict[str, Any]:
        now = now_iso()
        token = secrets.token_hex(16)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                """
                INSERT OR IGNORE INTO controller_transfers(transfer_id, remote_id, direction, root_name,
                  manifest_digest, total_bytes, sha256, acked_offset, idempotency_key, op_claim_token,
                  status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 'active', ?, ?)
                """,
                (transfer_id, remote_id, direction, root_name, manifest_digest, total, sha256,
                 key, token, now, now),
            )
            # Takeover: this process now owns the fence token (single-writer
            # assumption per transfer; the token gate below fences stale
            # writers from committing progress after a takeover).
            self.db.execute(
                "UPDATE controller_transfers SET op_claim_token=?, status='active', error=NULL, updated_at=? "
                "WHERE transfer_id=? AND status IN ('active','interrupted')",
                (token, now, transfer_id),
            )
            row = self.db.execute(
                "SELECT * FROM controller_transfers WHERE transfer_id=?", (transfer_id,)
            ).fetchone()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return dict(row)

    def _persist_progress(self, transfer_id: str, token: str | None, *, acked_offset: int,
                          state_epoch: int | None = None, **identity: Any) -> None:
        sets = ["acked_offset=?", "updated_at=?"]
        params: list[Any] = [int(acked_offset), now_iso()]
        if state_epoch is not None:
            sets.append("state_epoch=?")
            params.append(int(state_epoch))
        for column in ("root_name", "manifest_digest", "total_bytes", "sha256"):
            if column in identity:
                sets.append(f"{column}=?")
                params.append(identity[column])
        params.extend([transfer_id, token])
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                f"UPDATE controller_transfers SET {', '.join(sets)} "
                "WHERE transfer_id=? AND op_claim_token=?",
                params,
            ).rowcount
            if not changed:
                raise ValueError(f"stale worker: transfer {transfer_id} claim lost")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _check_epoch_record(self, transfer_id: str, known_epoch: int) -> None:
        """Stop BEFORE sending when the recorded epoch was already superseded."""
        row = self.db.execute(
            "SELECT status, error FROM controller_transfers WHERE transfer_id=?", (transfer_id,)
        ).fetchone()
        if row and row["status"] == "failed":
            raise TransferAborted(row["error"] or "transfer aborted")

    def _mark_completed(self, transfer_id: str, token: str | None, result: dict[str, Any]) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                "UPDATE controller_transfers SET status='completed', result_json=?, acked_offset=?, "
                "updated_at=?, op_claim_token=NULL WHERE transfer_id=? AND op_claim_token=? AND status='active'",
                (json.dumps(result, separators=(",", ":")), int(result.get("total_bytes", 0)),
                 now_iso(), transfer_id, token),
            ).rowcount
            if not changed:
                raise ValueError(f"stale worker: transfer {transfer_id} cannot be completed with this claim")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _mark_aborted(self, transfer_id: str, token: str | None, error: str) -> None:
        self._mark_failed(transfer_id, token, error)

    def _mark_failed(self, transfer_id: str, token: str | None, error: str) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "UPDATE controller_transfers SET status='failed', error=?, updated_at=? "
                "WHERE transfer_id=? AND (op_claim_token=? OR ? IS NULL)",
                (error, now_iso(), transfer_id, token, token),
            )
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
