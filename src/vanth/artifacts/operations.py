"""Durable artifact operations: idempotent, leased, fenced (Phase 5).

Every public method goes through the same durable-operation pattern:

1. ``_begin`` computes ``request_digest(method, payload, idempotency_key)``
   (reusing :func:`vanth.remote.protocol.request_digest`) and inserts the
   operation row in one transaction. Replays by idempotency key return the
   existing row; a different digest for the same key raises a
   ``PROTOCOL_REPLAY_MISMATCH`` error.
2. A worker claims the op: fresh ``claim_token``, ``claimed_at``,
   ``lease_expires_at`` (now + lease seconds), ``attempts += 1`` and
   ``lease_generation += 1``.
3. Completion is **fenced**: only the claim token recorded on the row may
   move the op to a terminal status, and only while its lease is unexpired.
   ``reclaim_expired()`` re-claims expired ops under a new generation and
   token, so a stale worker's token can never commit afterwards.

Publication transaction boundary (``put_file``): bytes are staged, hashed,
and made durable in the blob store *first*; then one catalog transaction
inserts the immutable version row, moves the root's latest pointer, and marks
the operation completed. A crash before that transaction leaves only
discoverable staging state (no version row); a crash after it replays to the
same version via the stored result.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import stat
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..remote.protocol import request_digest
from .catalog import Catalog, is_recovery_required, new_id, now_iso
from .local_store import LocalBlobStore
from .manifest import build_manifest, build_manifest_from_tree, manifest_digest

__all__ = ["ArtifactOperations", "DEFAULT_LEASE_SECONDS", "MUTATING_METHODS"]

DEFAULT_LEASE_SECONDS = 300

# Durable-op methods that mutate catalog state. While the ``recovery_required``
# marker is set (a backup restore is in progress), every one of these is
# refused until recovery completes; read-style ops stay available.
MUTATING_METHODS = frozenset(
    {
        "artifact.put_file",
        "artifact.put_dir",
        "collection.create",
        "collection.append_version",
        "collection.alias_set",
        "collection.link_lineage",
        "lifecycle.request_delete",
        "lifecycle.restore",
        "lifecycle.pin",
        "lifecycle.unpin",
        "lifecycle.gc",
    }
)

DEFAULT_LEASE_SECONDS = 300


def _mint_key(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(12)}"


class ArtifactOperations:
    def __init__(
        self,
        catalog: Catalog,
        blobs: LocalBlobStore,
        journalless: bool = True,
        *,
        lease_seconds: int | None = None,
    ) -> None:
        self.catalog = catalog
        self.blobs = blobs
        self.journalless = journalless
        if lease_seconds is None:
            try:
                lease_seconds = max(30, int(os.environ.get("VANTH_ARTIFACT_LEASE_SECONDS", DEFAULT_LEASE_SECONDS)))
            except ValueError:
                lease_seconds = DEFAULT_LEASE_SECONDS
        self.lease_seconds = int(lease_seconds)
        # Test hook: called between staging and blob publication so crash cases
        # can inject a failure while staging state is still discoverable.
        self.crash_hook = None

    # ------------------------------------------------------------------
    # Durable operation plumbing
    # ------------------------------------------------------------------

    def _op_dict(self, row) -> dict[str, Any]:
        return {
            "op_id": row["op_id"],
            "idempotency_key": row["idempotency_key"],
            "method": row["method"],
            "request_digest": row["request_digest"],
            "payload": json.loads(row["payload_json"] or "{}"),
            "status": row["status"],
            "claim_token": row["claim_token"],
            "lease_expires_at": row["lease_expires_at"],
            "lease_generation": int(row["lease_generation"]),
            "attempts": int(row["attempts"]),
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "error": row["error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _begin(self, method: str, payload: dict[str, Any], idempotency_key: str) -> tuple[dict[str, Any], bool]:
        """Insert or replay the operation row. Returns ``(op, replayed)``.

        Mutating methods are refused while ``recovery_required`` is set —
        a restored catalog stays locked out of every mutation until
        ``complete_restore`` clears the marker (replays of already-completed
        operations still return their recorded result).
        """
        digest = request_digest(method, payload, idempotency_key)
        db = self.catalog.db
        with self.catalog.lock:
            if method in MUTATING_METHODS and is_recovery_required(db):
                raise ValueError("recovery_required: complete restore first")
            db.execute("BEGIN IMMEDIATE")
            try:
                existing = db.execute(
                    "SELECT * FROM operations WHERE idempotency_key=?", (idempotency_key,)
                ).fetchone()
                if existing:
                    if existing["request_digest"] != digest:
                        raise ValueError(
                            "PROTOCOL_REPLAY_MISMATCH: idempotency key was reused with a different request"
                        )
                    op = self._op_dict(existing)
                    db.commit()
                    return op, True
                stamp = now_iso()
                db.execute(
                    """
                    INSERT INTO operations(op_id, idempotency_key, method, request_digest, payload_json,
                      status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                    """,
                    (
                        new_id("aop"), idempotency_key, method, digest,
                        json.dumps(payload, separators=(",", ":")), stamp, stamp,
                    ),
                )
                row = db.execute(
                    "SELECT * FROM operations WHERE idempotency_key=?", (idempotency_key,)
                ).fetchone()
                db.commit()
                return self._op_dict(row), False
            except BaseException:
                db.rollback()
                raise

    def _claim(self, op_id: str) -> dict[str, Any]:
        """Claim (or re-claim an EXPIRED op): new token, generation, and lease.

        A ``running`` op with a LIVE lease can never be claimed — concurrent
        retries of one idempotency key must not steal the original worker's
        claim or execute side effects twice (review P1-8).
        """
        db = self.catalog.db
        token = secrets.token_hex(16)
        now = datetime.now(timezone.utc)
        expires = (now + timedelta(seconds=self.lease_seconds)).isoformat().replace("+00:00", "Z")
        now_text = now_iso()
        with self.catalog.lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                changed = db.execute(
                    """
                    UPDATE operations SET status='running', claim_token=?, claimed_at=?, lease_expires_at=?,
                      attempts=attempts+1, lease_generation=lease_generation+1, updated_at=?
                    WHERE op_id=? AND (
                      status='pending' OR status='failed'
                      OR (status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
                    )
                    """,
                    (token, now.isoformat().replace("+00:00", "Z"), expires, now_text,
                     op_id, now_text),
                ).rowcount
                if not changed:
                    db.rollback()
                    raise ValueError(f"operation cannot be claimed: {op_id}")
                row = db.execute("SELECT * FROM operations WHERE op_id=?", (op_id,)).fetchone()
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return self._op_dict(row)

    def _finish(
        self,
        op_id: str,
        claim_token: str,
        *,
        status: str,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """Fenced terminal transition: token must match and the lease must be live.

        A reclaim assigns a new token (and bumps ``lease_generation``), so a
        matching token implies the worker still owns the current generation;
        an expired lease can never be committed even before reclaim happens.
        """
        db = self.catalog.db
        with self.catalog.lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                changed = db.execute(
                    """
                    UPDATE operations SET status=?, result_json=?, error=?, updated_at=?,
                      claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL
                    WHERE op_id=? AND claim_token=? AND status='running'
                      AND (lease_expires_at IS NULL OR lease_expires_at > ?)
                    """,
                    (
                        status,
                        json.dumps(result, separators=(",", ":")) if result is not None else None,
                        error,
                        now_iso(),
                        op_id,
                        claim_token,
                        now_iso(),
                    ),
                ).rowcount
                if not changed:
                    db.rollback()
                    raise ValueError(
                        f"stale worker: operation {op_id} cannot be finished with this claim "
                        "(expired lease or reclaimed generation)"
                    )
                row = db.execute("SELECT * FROM operations WHERE op_id=?", (op_id,)).fetchone()
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return self._op_dict(row)

    def complete(self, op_id: str, claim_token: str, result: dict[str, Any]) -> dict[str, Any]:
        return self._finish(op_id, claim_token, status="completed", result=result)

    def fail(self, op_id: str, claim_token: str, error: str) -> dict[str, Any]:
        return self._finish(op_id, claim_token, status="failed", error=error)

    def reclaim_expired(self) -> list[dict[str, Any]]:
        """Re-claim every expired running op under a fresh generation."""
        db = self.catalog.db
        with self.catalog.lock:
            rows = db.execute(
                "SELECT op_id FROM operations WHERE status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?",
                (now_iso(),),
            ).fetchall()
        return [self._claim(row["op_id"]) for row in rows]

    def get_operation(self, op_id: str) -> dict[str, Any]:
        row = self.catalog.db.execute("SELECT * FROM operations WHERE op_id=?", (op_id,)).fetchone()
        if not row:
            raise ValueError(f"Unknown op_id: {op_id}")
        return self._op_dict(row)

    # ------------------------------------------------------------------
    # Root / version helpers
    # ------------------------------------------------------------------

    def _get_or_create_root(self, name: str) -> dict[str, Any]:
        db = self.catalog.db
        with self.catalog.lock:
            row = db.execute("SELECT * FROM roots WHERE name=?", (name,)).fetchone()
            if row:
                return dict(row)
            stamp = now_iso()
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    "INSERT INTO roots(root_id, name, latest_version_id, created_at, updated_at) VALUES (?, ?, NULL, ?, ?)",
                    (new_id("rot"), name, stamp, stamp),
                )
                db.commit()
            except BaseException:
                db.rollback()
                row = db.execute("SELECT * FROM roots WHERE name=?", (name,)).fetchone()
                if not row:
                    raise
                return dict(row)
            row = db.execute("SELECT * FROM roots WHERE name=?", (name,)).fetchone()
            return dict(row)

    def _get_version(self, version_id: str):
        return self.catalog.db.execute("SELECT * FROM versions WHERE version_id=?", (version_id,)).fetchone()

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------

    def put_file(
        self,
        name: str,
        *,
        data: bytes | bytearray | memoryview | None = None,
        source_path: str | os.PathLike[str] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Publish file content as an immutable version of root ``name``.

        Identical content published to the same root collapses onto the
        existing ``(root_id, manifest_digest)`` version with
        ``deduplicated=True``; identical bytes also deduplicate physically in
        the blob store. Publishing different content to the same root creates
        a new immutable version and advances the root's latest pointer.

        Tombstone note (Phase 7): blobs are content-addressed files with no
        catalog rows, so a delete-requested (logically deleted) version's
        ``(root_id, manifest_digest)`` match is treated as a NORMAL dedup
        hit — the version row still exists, which by definition means it was
        not hard-deleted (GC removes rows outright, so any row found here is
        live). Re-publication therefore never resurrects hard-deleted
        content; it simply re-publishes the bytes as a fresh version.
        """
        if isinstance(name, bool) or not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")
        if (data is None) == (source_path is None):
            raise ValueError("provide exactly one of data or source_path")
        if source_path is not None:
            data = Path(source_path).read_bytes()
        data = bytes(data)
        manifest = build_manifest(data, name)
        mdigest = manifest_digest(manifest)
        key = idempotency_key or _mint_key("aput")
        payload = {"name": name, "manifest_digest": mdigest}
        op, replayed = self._begin("artifact.put_file", payload, key)
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self._claim(op["op_id"])
        token = claimed["claim_token"]
        try:
            root = self._get_or_create_root(name)
            existing = self.catalog.db.execute(
                "SELECT * FROM versions WHERE root_id=? AND manifest_digest=?", (root["root_id"], mdigest)
            ).fetchone()
            if existing:
                result = {
                    "version_id": existing["version_id"],
                    "root_id": root["root_id"],
                    "name": name,
                    "manifest_digest": mdigest,
                    "size_bytes": int(existing["size_bytes"]),
                    "sha256": manifest["sha256"],
                    "deduplicated": True,
                }
                self.complete(op["op_id"], token, result)
                return result
            staged = self.blobs.stage(data)
            if self.crash_hook is not None:
                self.crash_hook(op["op_id"])
            self.blobs.publish_staged(staged, manifest["sha256"])
            # Publication transaction boundary: the blob is durable above; the
            # single transaction below commits version + root pointer + op result.
            db = self.catalog.db
            with self.catalog.lock:
                db.execute("BEGIN IMMEDIATE")
                try:
                    row = db.execute(
                        "SELECT version_id FROM versions WHERE root_id=? AND manifest_digest=?",
                        (root["root_id"], mdigest),
                    ).fetchone()
                    deduplicated = bool(row)
                    version_id = row["version_id"] if row else self.catalog.new_version_id()
                    if not row:
                        db.execute(
                            "INSERT INTO versions(version_id, root_id, manifest_digest, manifest_json, size_bytes, created_at)"
                            " VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                version_id,
                                root["root_id"],
                                mdigest,
                                json.dumps(manifest, separators=(",", ":")),
                                manifest["size_bytes"],
                                now_iso(),
                            ),
                        )
                    db.execute(
                        "UPDATE roots SET latest_version_id=?, updated_at=? WHERE root_id=?",
                        (version_id, now_iso(), root["root_id"]),
                    )
                    result = {
                        "version_id": version_id,
                        "root_id": root["root_id"],
                        "name": name,
                        "manifest_digest": mdigest,
                        "size_bytes": manifest["size_bytes"],
                        "sha256": manifest["sha256"],
                        "deduplicated": deduplicated,
                    }
                    changed = db.execute(
                        """
                        UPDATE operations SET status='completed', result_json=?, updated_at=?,
                          claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL
                        WHERE op_id=? AND claim_token=? AND status='running'
                          AND (lease_expires_at IS NULL OR lease_expires_at > ?)
                        """,
                        (
                            json.dumps(result, separators=(",", ":")),
                            now_iso(),
                            op["op_id"],
                            token,
                            now_iso(),
                        ),
                    ).rowcount
                    if not changed:
                        raise ValueError(
                            f"stale worker: operation {op['op_id']} cannot be completed with this claim "
                            "(expired lease or reclaimed generation)"
                        )
                    db.commit()
                except BaseException:
                    db.rollback()
                    raise
            return result
        except BaseException as exc:
            try:
                self.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise

    def put_dir(
        self,
        source_dir: str | os.PathLike[str],
        name: str,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Publish a directory tree as an immutable v1 (kind="dir") version.

        Mirrors the ``put_file`` durable-operation pattern exactly: capture a
        canonical manifest (secure capture refuses symlinks, reparse points,
        special files, portability collisions, and source mutation), then
        stage + publish every unique blob (each verified against its manifest
        sha256 by ``publish_staged`` before it becomes visible), and finally
        commit version + root pointer + op result in ONE catalog transaction.
        A crash before that transaction leaves only staging state; the
        committed digest therefore always describes the exact transferred
        bytes. Identical content re-published to the same root deduplicates
        onto the existing version.
        """
        if isinstance(name, bool) or not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")
        source = Path(source_dir)
        manifest = build_manifest_from_tree(source, name)
        mdigest = manifest_digest(manifest)
        key = idempotency_key or _mint_key("aput")
        payload = {"name": name, "manifest_digest": mdigest}
        op, replayed = self._begin("artifact.put_dir", payload, key)
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self._claim(op["op_id"])
        token = claimed["claim_token"]
        try:
            root = self._get_or_create_root(name)
            existing = self.catalog.db.execute(
                "SELECT * FROM versions WHERE root_id=? AND manifest_digest=?", (root["root_id"], mdigest)
            ).fetchone()
            if existing:
                result = {
                    "version_id": existing["version_id"],
                    "root_id": root["root_id"],
                    "name": name,
                    "manifest_digest": mdigest,
                    "size_bytes": int(existing["size_bytes"]),
                    "file_count": sum(1 for e in manifest["entries"] if e["kind"] == "file"),
                    "deduplicated": True,
                }
                self.complete(op["op_id"], token, result)
                return result
            # Stage every unique blob first (dedup by construction via the
            # content-addressed set of sha256s), then publish each one —
            # publish_staged re-hashes the staged bytes against the manifest
            # digest before the blob path exists.
            unique_shas: dict[str, str] = {}
            for entry in manifest["entries"]:
                if entry["kind"] != "file":
                    continue
                unique_shas.setdefault(entry["sha256"], str(source / str(entry["path"]).replace("/", os.sep)))
            staged_paths: list[tuple[Path, str]] = []
            for sha256, file_path in unique_shas.items():
                staged_paths.append((self.blobs.stage(file_path), sha256))
            if self.crash_hook is not None:
                self.crash_hook(op["op_id"])
            for staged, sha256 in staged_paths:
                self.blobs.publish_staged(staged, sha256)
            # Publication transaction boundary (same as put_file): blobs are
            # durable above; this single transaction commits version + root
            # pointer + fenced op completion.
            db = self.catalog.db
            total_size = sum(int(e["size_bytes"]) for e in manifest["entries"] if e["kind"] == "file")
            with self.catalog.lock:
                db.execute("BEGIN IMMEDIATE")
                try:
                    row = db.execute(
                        "SELECT version_id FROM versions WHERE root_id=? AND manifest_digest=?",
                        (root["root_id"], mdigest),
                    ).fetchone()
                    deduplicated = bool(row)
                    version_id = row["version_id"] if row else self.catalog.new_version_id()
                    if not row:
                        db.execute(
                            "INSERT INTO versions(version_id, root_id, manifest_digest, manifest_json, size_bytes, created_at)"
                            " VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                version_id,
                                root["root_id"],
                                mdigest,
                                json.dumps(manifest, separators=(",", ":")),
                                total_size,
                                now_iso(),
                            ),
                        )
                    db.execute(
                        "UPDATE roots SET latest_version_id=?, updated_at=? WHERE root_id=?",
                        (version_id, now_iso(), root["root_id"]),
                    )
                    result = {
                        "version_id": version_id,
                        "root_id": root["root_id"],
                        "name": name,
                        "manifest_digest": mdigest,
                        "size_bytes": total_size,
                        "file_count": sum(1 for e in manifest["entries"] if e["kind"] == "file"),
                        "deduplicated": deduplicated,
                    }
                    changed = db.execute(
                        """
                        UPDATE operations SET status='completed', result_json=?, updated_at=?,
                          claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL
                        WHERE op_id=? AND claim_token=? AND status='running'
                          AND (lease_expires_at IS NULL OR lease_expires_at > ?)
                        """,
                        (
                            json.dumps(result, separators=(",", ":")),
                            now_iso(),
                            op["op_id"],
                            token,
                            now_iso(),
                        ),
                    ).rowcount
                    if not changed:
                        raise ValueError(
                            f"stale worker: operation {op['op_id']} cannot be completed with this claim "
                            "(expired lease or reclaimed generation)"
                        )
                    db.commit()
                except BaseException:
                    db.rollback()
                    raise
            return result
        except BaseException as exc:
            try:
                self.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise

    def resolve(
        self,
        name_or_root_id: str,
        *,
        alias: str | None = None,
        version_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Resolve to one immutable version: explicit version, alias pin, or root latest."""
        key = idempotency_key or _mint_key("ares")
        payload = {"target": name_or_root_id, "alias": alias, "version_id": version_id}
        op, replayed = self._begin("artifact.resolve", payload, key)
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self._claim(op["op_id"])
        token = claimed["claim_token"]
        try:
            if version_id is not None:
                row = self._get_version(version_id)
                if not row:
                    raise ValueError(f"Unknown version_id: {version_id}")
                resolved_via = "version"
            elif alias is not None:
                alias_row = self.catalog.db.execute(
                    """
                    SELECT v.* FROM aliases a JOIN versions v ON v.version_id=a.version_id
                    WHERE a.alias_name=?
                    """,
                    (alias,),
                ).fetchone()
                if not alias_row:
                    raise ValueError(f"Unknown alias: {alias}")
                row = alias_row
                resolved_via = "alias"
            else:
                root_row = self.catalog.db.execute(
                    "SELECT * FROM roots WHERE name=? OR root_id=?", (name_or_root_id, name_or_root_id)
                ).fetchone()
                if not root_row:
                    raise ValueError(f"Unknown root: {name_or_root_id}")
                if not root_row["latest_version_id"]:
                    raise ValueError(f"Root has no versions yet: {name_or_root_id}")
                row = self._get_version(root_row["latest_version_id"])
                resolved_via = "latest"
            root_row = self.catalog.db.execute(
                "SELECT * FROM roots WHERE root_id=?", (row["root_id"],)
            ).fetchone()
            result = {
                "root_id": row["root_id"],
                "root_name": root_row["name"] if root_row else None,
                "version_id": row["version_id"],
                "manifest_digest": row["manifest_digest"],
                "size_bytes": int(row["size_bytes"]),
                "resolved_via": resolved_via,
            }
            self.complete(op["op_id"], token, result)
            return result
        except BaseException as exc:
            try:
                self.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise

    def info(self, version_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        """Manifest plus blob existence and verification flag for one version."""
        key = idempotency_key or _mint_key("ainf")
        payload = {"version_id": version_id}
        op, replayed = self._begin("artifact.info", payload, key)
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self._claim(op["op_id"])
        token = claimed["claim_token"]
        try:
            row = self._get_version(version_id)
            if not row:
                raise ValueError(f"Unknown version_id: {version_id}")
            manifest = json.loads(row["manifest_json"])
            sha256 = manifest.get("sha256")
            if manifest.get("kind") == "dir":
                shas = {e["sha256"] for e in manifest["entries"] if e["kind"] == "file"}
                blob_exists = all(self.blobs.has_blob(s) for s in shas)
                verified = all(self.blobs.verify_blob(s) for s in shas)
            else:
                blob_exists = self.blobs.has_blob(sha256)
                verified = self.blobs.verify_blob(sha256)
            result = {
                "version_id": version_id,
                "root_id": row["root_id"],
                "manifest_digest": row["manifest_digest"],
                "manifest": manifest,
                "size_bytes": int(row["size_bytes"]),
                "blob_exists": blob_exists,
                "verified": verified,
            }
            self.complete(op["op_id"], token, result)
            return result
        except BaseException as exc:
            try:
                self.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise

    def materialize(
        self,
        version_id: str,
        dest_path: str | os.PathLike[str],
        *,
        idempotency_key: str | None = None,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        """Write a version's blob to ``dest_path`` atomically; refuses to clobber."""
        dest = Path(dest_path)
        key = idempotency_key or _mint_key("amat")
        payload = {"version_id": version_id, "dest_path": str(dest), "overwrite": bool(overwrite)}
        op, replayed = self._begin("artifact.materialize", payload, key)
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self._claim(op["op_id"])
        token = claimed["claim_token"]
        try:
            row = self._get_version(version_id)
            if not row:
                raise ValueError(f"Unknown version_id: {version_id}")
            manifest = json.loads(row["manifest_json"])
            if manifest.get("kind") == "dir":
                result = self._materialize_dir(manifest, dest, version_id, int(row["size_bytes"]))
                self.complete(op["op_id"], token, result)
                return result
            sha256 = manifest["sha256"]
            if not self.blobs.has_blob(sha256):
                raise ValueError(f"blob missing for version {version_id}: {sha256}")
            existed = dest.exists()
            if existed and not overwrite:
                raise ValueError(f"destination already exists: {dest}")
            staged = self.blobs.stage(self.blobs.blob_path(sha256))
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, dest)
            result = {
                "version_id": version_id,
                "dest_path": str(dest),
                "size_bytes": int(row["size_bytes"]),
                "sha256": sha256,
                "overwritten": existed,
            }
            self.complete(op["op_id"], token, result)
            return result
        except BaseException as exc:
            try:
                self.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise

    # -- directory materialization ------------------------------------------

    @staticmethod
    def _is_reparse(info: os.stat_result) -> bool:
        """True for symlinks and Windows reparse points (junctions, mounts).

        On Windows CPython ``st_reparse_tag`` is exposed by lstat; a junction
        is NOT reported by ``os.path.islink``, so the tag check is what
        catches them. On POSIX the attribute is absent and plain symlinks are
        caught via mode bits.
        """
        if stat.S_ISLNK(info.st_mode):
            return True
        return bool(getattr(info, "st_reparse_tag", 0))

    def _check_component_chain(self, path: Path) -> None:
        """Refuse any existing component of ``path`` that is a symlink or
        reparse point (symlink-swap defense). Components that do not exist
        yet pass; each one is still checked again immediately before use."""
        absolute = Path(os.path.abspath(path))
        parts = absolute.parts
        if len(parts) < 2:
            return
        probe = Path(parts[0])
        for part in parts[1:]:
            probe = probe / part
            try:
                info = os.lstat(probe)
            except FileNotFoundError:
                continue
            except OSError:
                continue
            if self._is_reparse(info):
                raise ValueError(
                    f"refusing to materialize through a symlink/reparse-point path component: {probe}"
                )

    def _build_tree_into_staging(self, staging: Path, entries: list[dict[str, Any]]) -> None:
        """Create every manifest entry under the fresh staging directory.

        Each path component is lstat-checked immediately before use and
        refused when it is a symlink/reparse point; each file blob is
        re-verified against its manifest sha256 while copying. A final sweep
        re-verifies every created component right before the caller renames
        the staging tree into place.
        """
        created_dirs: set[Path] = set()
        for entry in entries:
            target = staging / str(entry["path"])
            current = staging
            for component in target.parent.relative_to(staging).parts:
                nxt = current / component
                # lstat EVERY time, even components we created ourselves:
                # refuse anything that became a symlink/reparse point between
                # use and use (TOCTOU mitigation on all platforms; junctions
                # are caught via st_reparse_tag on Windows).
                try:
                    info = os.lstat(nxt)
                    if self._is_reparse(info):
                        raise ValueError(
                            f"refusing to create under symlink/reparse-point component: {nxt}"
                        )
                    if not stat.S_ISDIR(info.st_mode):
                        raise ValueError(f"path component exists and is not a directory: {nxt}")
                except FileNotFoundError:
                    nxt.mkdir(exist_ok=False)
                    created_dirs.add(nxt)
                current = nxt
            if entry["kind"] == "dir":
                if os.path.lexists(target):
                    raise ValueError(f"unexpected existing entry while creating tree: {target}")
                target.mkdir()
                continue
            digest = hashlib.sha256()
            blob = self.blobs.blob_path(str(entry["sha256"]))
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            fd = os.open(target, flags, 0o600)
            try:
                with blob.open("rb") as src, os.fdopen(fd, "wb", closefd=True) as out:
                    for chunk in iter(lambda: src.read(1024 * 1024), b""):
                        digest.update(chunk)
                        out.write(chunk)
            except BaseException:
                try:
                    target.unlink()
                except OSError:
                    pass
                raise
            if digest.hexdigest() != entry["sha256"]:
                raise ValueError(
                    f"blob content does not match manifest entry during materialization: "
                    f"{entry['path']} ({entry['sha256']})"
                )
            if entry["executable"] and os.name != "nt":
                os.chmod(target, 0o755)
        # Final anti-swap sweep: every component we created must still be a
        # real directory immediately before the atomic rename into place.
        for dir_path in sorted(created_dirs):
            info = os.lstat(dir_path)
            if self._is_reparse(info) or not stat.S_ISDIR(info.st_mode):
                raise ValueError(
                    f"path component changed during materialization; refusing: {dir_path}"
                )

    def _materialize_dir(
        self,
        manifest: dict[str, Any],
        dest: Path,
        version_id: str,
        size_bytes: int,
    ) -> dict[str, Any]:
        from .manifest import validate_manifest_v1

        validate_manifest_v1(manifest)
        # Never merge directories: an existing destination is refused even
        # with overwrite=True (overwrite applies to file roots only).
        if os.path.lexists(dest):
            raise ValueError(
                f"destination already exists: {dest}; directory materialization never merges "
                "(overwrite does not apply to directories)"
            )
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Refuse a planted symlink/junction anywhere along the parent chain.
        self._check_component_chain(dest.parent)

        entries = sorted(manifest["entries"], key=lambda e: str(e["path"]).encode("utf-8"))
        file_entries = [e for e in entries if e["kind"] == "file"]
        missing = sorted({str(e["sha256"]) for e in file_entries if not self.blobs.has_blob(e["sha256"])})
        if missing:
            raise ValueError(f"blobs missing for dir version {version_id}: {', '.join(missing)}")

        staging = dest.parent / f".{dest.name}.materializing-{uuid.uuid4().hex}"
        staging.mkdir()
        try:
            try:
                self._build_tree_into_staging(staging, entries)
                # Atomic swap into place: rename fails rather than merges if
                # a destination raced into existence.
                os.rename(staging, dest)
            except OSError as exc:
                raise ValueError(
                    f"destination already exists or became unusable: {dest} ({exc})"
                ) from None
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        return {
            "version_id": version_id,
            "dest_path": str(dest),
            "size_bytes": size_bytes,
            "file_count": len(file_entries),
            "overwritten": False,
        }

    def verify(self, version_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        key = idempotency_key or _mint_key("aver")
        payload = {"version_id": version_id}
        op, replayed = self._begin("artifact.verify", payload, key)
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self._claim(op["op_id"])
        token = claimed["claim_token"]
        try:
            row = self._get_version(version_id)
            if not row:
                raise ValueError(f"Unknown version_id: {version_id}")
            manifest = json.loads(row["manifest_json"])
            if manifest.get("kind") == "dir":
                # Verify EVERY referenced blob; empty-dir entries carry the
                # well-known empty-content digest and no blob of their own.
                entry_reports = []
                offending = []
                for entry in manifest["entries"]:
                    if entry["kind"] == "dir":
                        # Empty dirs carry the empty-content digest by
                        # definition and have no blob of their own.
                        continue
                    expected = str(entry["sha256"])
                    actual = None
                    if self.blobs.has_blob(expected):
                        digest = hashlib.sha256()
                        with self.blobs.blob_path(expected).open("rb") as handle:
                            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                                digest.update(chunk)
                        actual = digest.hexdigest()
                    entry_ok = actual == expected
                    report = {
                        "path": entry["path"],
                        "kind": entry["kind"],
                        "expected_sha256": expected,
                        "actual_sha256": actual,
                        "ok": entry_ok,
                    }
                    if not entry_ok:
                        offending.append(entry["path"])
                    entry_reports.append(report)
                ok = not offending
                result = {
                    "ok": ok,
                    "ok_bool": ok,
                    "expected_sha256": None,
                    "actual_sha256": None,
                    "entries": entry_reports,
                    "offending_paths": offending,
                }
                self.complete(op["op_id"], token, result)
                return result
            expected = manifest["sha256"]
            actual = None
            if self.blobs.has_blob(expected):
                digest = hashlib.sha256()
                with self.blobs.blob_path(expected).open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                actual = digest.hexdigest()
            ok = actual == expected
            result = {
                "ok": ok,
                "ok_bool": ok,
                "expected_sha256": expected,
                "actual_sha256": actual,
            }
            self.complete(op["op_id"], token, result)
            return result
        except BaseException as exc:
            try:
                self.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise
