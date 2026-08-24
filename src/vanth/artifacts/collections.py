"""Collections, compare-and-swap aliases, and lineage (Phase 7).

All mutating methods reuse the durable-operation pattern from
:class:`~vanth.artifacts.operations.ArtifactOperations` (``_begin`` digest +
replay by idempotency key, lease claim, fenced completion), so every change is
idempotent under retries and crashes.

- Collections are append-only: membership ordinals are monotonic and never
  rewritten; appending a version that is already a member is a no-op that
  returns the current state.
- Alias movement requires an explicit compare-and-swap: ``alias_set`` only
  moves an alias when it currently points at the caller-supplied
  ``expected_version_id`` (or, with ``expected_version_id=None``, when the
  alias does not exist yet). There is NO silent last-write-wins movement.
- Lineage links a producer/consumer identity ('job' | 'remote_job' |
  'version' | 'alias') to one resolved immutable version.
"""

from __future__ import annotations

import json
from typing import Any

from .catalog import new_id, now_iso

__all__ = ["Collections", "LINEAGE_KINDS"]

LINEAGE_KINDS = frozenset({"job", "remote_job", "version", "alias"})


class Collections:
    def __init__(self, catalog, ops) -> None:
        self.catalog = catalog
        self.ops = ops

    # ------------------------------------------------------------------
    # Lookups (reads)
    # ------------------------------------------------------------------

    def _collection_row(self, name_or_id: str):
        return self.catalog.db.execute(
            "SELECT * FROM collections WHERE name=? OR collection_id=?", (name_or_id, name_or_id)
        ).fetchone()

    def _require_collection(self, name_or_id: str):
        if not isinstance(name_or_id, str) or not name_or_id.strip():
            raise ValueError("collection must be a non-empty name or id")
        row = self._collection_row(name_or_id)
        if not row:
            raise ValueError(f"Unknown collection: {name_or_id}")
        return row

    def get_collection(self, name_or_id: str) -> dict[str, Any]:
        """Ordered versions of a collection (monotonic ordinal order)."""
        row = self._require_collection(name_or_id)
        versions = [
            {
                "ordinal": int(v["ordinal"]),
                "version_id": v["version_id"],
                "created_at": v["created_at"],
                "root_id": v["root_id"],
                "manifest_digest": v["manifest_digest"],
                "size_bytes": int(v["size_bytes"]),
            }
            for v in self.catalog.db.execute(
                """
                SELECT cv.ordinal, cv.version_id, cv.created_at,
                       v.root_id, v.manifest_digest, v.size_bytes
                FROM collection_versions cv JOIN versions v ON v.version_id=cv.version_id
                WHERE cv.collection_id=? ORDER BY cv.ordinal
                """,
                (row["collection_id"],),
            )
        ]
        return {
            "collection_id": row["collection_id"],
            "name": row["name"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "versions": versions,
        }

    def lineage_for(self, version_id: str) -> list[dict[str, Any]]:
        """All lineage links recorded against one immutable version."""
        if not isinstance(version_id, str) or not version_id:
            raise ValueError("version_id must be a non-empty string")
        return [
            dict(row)
            for row in self.catalog.db.execute(
                """
                SELECT lin_id, producer_kind, producer_id, consumer_kind, consumer_id,
                       version_id, created_at
                FROM lineage WHERE version_id=? ORDER BY created_at, lin_id
                """,
                (version_id,),
            )
        ]

    # ------------------------------------------------------------------
    # Mutations (durable-operation pattern)
    # ------------------------------------------------------------------

    def create_collection(self, name: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        """Create a named collection. Duplicate names are refused."""
        if isinstance(name, bool) or not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")
        payload = {"name": name}
        op, replayed = self.ops._begin("collection.create", payload, idempotency_key or new_id("akey"))
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self.ops._claim(op["op_id"])
        token = claimed["claim_token"]
        db = self.catalog.db
        try:
            stamp = now_iso()
            with self.catalog.lock:
                db.execute("BEGIN IMMEDIATE")
                try:
                    existing = db.execute("SELECT * FROM collections WHERE name=?", (name,)).fetchone()
                    if existing:
                        raise ValueError(f"collection already exists: {name}")
                    collection_id = new_id("col")
                    db.execute(
                        "INSERT INTO collections(collection_id, name, created_at, updated_at)"
                        " VALUES (?, ?, ?, ?)",
                        (collection_id, name, stamp, stamp),
                    )
                    result = {"collection_id": collection_id, "name": name, "created_at": stamp}
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
                self.ops.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise

    def append_version(
        self, collection_id_or_name: str, version_id: str, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        """Append an immutable version to a collection with a monotonic ordinal.

        Appending a version that is already a member is a no-op returning the
        current ordered state. Ordinals are assigned as max(existing)+1 inside
        one transaction, so they are strictly monotonic per collection.
        """
        if not isinstance(version_id, str) or not version_id.strip():
            raise ValueError("version_id must be a non-empty string")
        payload = {"collection": str(collection_id_or_name), "version_id": version_id}
        op, replayed = self.ops._begin("collection.append_version", payload, idempotency_key or new_id("akey"))
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self.ops._claim(op["op_id"])
        token = claimed["claim_token"]
        db = self.catalog.db
        try:
            with self.catalog.lock:
                db.execute("BEGIN IMMEDIATE")
                try:
                    coll = db.execute(
                        "SELECT * FROM collections WHERE name=? OR collection_id=?",
                        (collection_id_or_name, collection_id_or_name),
                    ).fetchone()
                    if not coll:
                        raise ValueError(f"Unknown collection: {collection_id_or_name}")
                    version = db.execute(
                        "SELECT version_id, root_id FROM versions WHERE version_id=?", (version_id,)
                    ).fetchone()
                    if not version:
                        raise ValueError(f"Unknown version_id: {version_id}")
                    current = db.execute(
                        """
                        SELECT cv.version_id, cv.ordinal, cv.created_at FROM collection_versions cv
                        WHERE cv.collection_id=? ORDER BY cv.ordinal
                        """,
                        (coll["collection_id"],),
                    ).fetchall()
                    already = next((r for r in current if r["version_id"] == version_id), None)
                    appended = already is None
                    ordinal = int(already["ordinal"]) if already else (
                        int(current[-1]["ordinal"]) + 1 if current else 1
                    )
                    if appended:
                        db.execute(
                            "INSERT INTO collection_versions(collection_id, version_id, ordinal, created_at)"
                            " VALUES (?, ?, ?, ?)",
                            (coll["collection_id"], version_id, ordinal, now_iso()),
                        )
                        # Capture the persisted stamp once and reuse it in the
                        # result (review rc14 P2-6): the response previously
                        # generated a DIFFERENT timestamp than the stored row.
                        appended_created_at = db.execute(
                            "SELECT created_at FROM collection_versions WHERE collection_id=? AND version_id=?",
                            (coll["collection_id"], version_id),
                        ).fetchone()["created_at"]
                        db.execute(
                            "UPDATE collections SET updated_at=? WHERE collection_id=?",
                            (now_iso(), coll["collection_id"]),
                        )
                    result = {
                        "collection_id": coll["collection_id"],
                        "name": coll["name"],
                        "version_id": version_id,
                        "ordinal": ordinal,
                        "appended": appended,
                        "versions": [
                            {"version_id": r["version_id"], "ordinal": int(r["ordinal"]), "created_at": r["created_at"]}
                            for r in current
                        ]
                        + ([{"version_id": version_id, "ordinal": ordinal,
                             "created_at": appended_created_at}] if appended else []),
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
                self.ops.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise

    def alias_set(
        self,
        alias_name: str,
        root_id: str,
        expected_version_id: str | None,
        new_version_id: str,
        *,
        idempotency_key: str | None = None,
        updated_by: str | None = None,
    ) -> dict[str, Any]:
        """Compare-and-swap alias movement — never a silent overwrite.

        With ``expected_version_id=None`` the alias must NOT exist (create).
        Otherwise the alias must currently point at exactly
        ``expected_version_id``. Any mismatch raises
        ``ALIAS_CAS_MISMATCH`` and leaves the alias untouched.
        """
        for label, value in (("alias_name", alias_name), ("root_id", root_id)):
            if isinstance(value, bool) or not isinstance(value, str) or not value.strip():
                raise ValueError(f"{label} must be a non-empty string")
        if not isinstance(new_version_id, str) or not new_version_id.strip():
            raise ValueError("new_version_id must be a non-empty string")
        payload = {
            "alias_name": alias_name,
            "root_id": root_id,
            "expected_version_id": expected_version_id,
            "new_version_id": new_version_id,
        }
        op, replayed = self.ops._begin("collection.alias_set", payload, idempotency_key or new_id("akey"))
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self.ops._claim(op["op_id"])
        token = claimed["claim_token"]
        db = self.catalog.db
        try:
            with self.catalog.lock:
                db.execute("BEGIN IMMEDIATE")
                try:
                    version = db.execute(
                        "SELECT root_id FROM versions WHERE version_id=?", (new_version_id,)
                    ).fetchone()
                    if not version:
                        raise ValueError(f"Unknown version_id: {new_version_id}")
                    if version["root_id"] != root_id:
                        raise ValueError(
                            f"version {new_version_id} does not belong to root {root_id}"
                        )
                    current = db.execute(
                        "SELECT * FROM aliases WHERE alias_name=?", (alias_name,)
                    ).fetchone()
                    previous = current["version_id"] if current else None
                    if expected_version_id is None:
                        if current:
                            raise ValueError(
                                f"ALIAS_CAS_MISMATCH: alias '{alias_name}' already exists "
                                f"(expected absent); points at {previous}"
                            )
                        created = True
                        stamp = now_iso()
                        db.execute(
                            "INSERT INTO aliases(alias_name, root_id, version_id, updated_at, pinned_at, updated_by)"
                            " VALUES (?, ?, ?, ?, ?, ?)",
                            (alias_name, root_id, new_version_id, stamp, stamp, updated_by),
                        )
                    else:
                        if not current or current["version_id"] != expected_version_id:
                            raise ValueError(
                                f"ALIAS_CAS_MISMATCH: alias '{alias_name}' points at "
                                f"{previous!r}, expected {expected_version_id!r}"
                            )
                        # Cross-root movement is a separate, explicit decision:
                        # silently rebasing an alias onto another root would
                        # change the meaning of every consumer that resolved it
                        # (review P2-14). Remove and re-create deliberately.
                        if current["root_id"] != root_id:
                            raise ValueError(
                                f"ALIAS_CROSS_ROOT_MOVE: alias '{alias_name}' belongs to root "
                                f"{current['root_id']}; refusing move to {root_id}"
                            )
                        created = False
                        db.execute(
                            "UPDATE aliases SET root_id=?, version_id=?, updated_at=?, updated_by=?"
                            " WHERE alias_name=?",
                            (root_id, new_version_id, now_iso(), updated_by, alias_name),
                        )
                    result = {
                        "alias_name": alias_name,
                        "root_id": root_id,
                        "previous_version_id": previous,
                        "version_id": new_version_id,
                        "created": created,
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
                self.ops.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise

    def link_lineage(
        self,
        producer_kind: str,
        producer_id: str,
        consumer_kind: str,
        consumer_id: str,
        version_id: str,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Record a producer/consumer lineage link to one resolved version."""
        for label, kind in (("producer_kind", producer_kind), ("consumer_kind", consumer_kind)):
            if kind not in LINEAGE_KINDS:
                raise ValueError(f"{label} must be one of {sorted(LINEAGE_KINDS)}, got {kind!r}")
        for label, value in (("producer_id", producer_id), ("consumer_id", consumer_id)):
            if isinstance(value, bool) or not isinstance(value, str) or not value.strip():
                raise ValueError(f"{label} must be a non-empty string")
        if not isinstance(version_id, str) or not version_id.strip():
            raise ValueError("version_id must be a non-empty string")
        payload = {
            "producer_kind": producer_kind,
            "producer_id": producer_id,
            "consumer_kind": consumer_kind,
            "consumer_id": consumer_id,
            "version_id": version_id,
        }
        op, replayed = self.ops._begin("collection.link_lineage", payload, idempotency_key or new_id("akey"))
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self.ops._claim(op["op_id"])
        token = claimed["claim_token"]
        db = self.catalog.db
        try:
            with self.catalog.lock:
                db.execute("BEGIN IMMEDIATE")
                try:
                    if not db.execute(
                        "SELECT 1 FROM versions WHERE version_id=?", (version_id,)
                    ).fetchone():
                        raise ValueError(f"Unknown version_id: {version_id}")
                    existing = db.execute(
                        """
                        SELECT * FROM lineage WHERE producer_kind=? AND producer_id=?
                          AND consumer_kind=? AND consumer_id=? AND version_id=?
                        """,
                        (producer_kind, producer_id, consumer_kind, consumer_id, version_id),
                    ).fetchone()
                    if existing:
                        lin_id = existing["lin_id"]
                        deduplicated = True
                    else:
                        lin_id = new_id("lin")
                        stamp = now_iso()
                        db.execute(
                            "INSERT INTO lineage(lin_id, producer_kind, producer_id, consumer_kind,"
                            " consumer_id, version_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (lin_id, producer_kind, producer_id, consumer_kind, consumer_id, version_id, stamp),
                        )
                        deduplicated = False
                    result = {
                        "lin_id": lin_id,
                        "producer_kind": producer_kind,
                        "producer_id": producer_id,
                        "consumer_kind": consumer_kind,
                        "consumer_id": consumer_id,
                        "version_id": version_id,
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
                self.ops.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise
