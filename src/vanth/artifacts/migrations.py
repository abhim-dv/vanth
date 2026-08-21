"""Ordered SQLite migrations for the separate artifacts catalog (``artifacts.sqlite``).

Mirrors :mod:`vanth.migrations`: a single ``PRAGMA user_version`` counter, a
backup before any upgrade, and a transactional v0 -> latest path. Connection
tuning is reused verbatim from ``vanth.migrations.configure_connection`` so
both databases share WAL, foreign keys, and the busy timeout.

Schema v2 (Phase 7) adds:

- ``collections`` / ``collection_versions``: named collections with
  monotonic, append-only, immutable version membership.
- ``aliases.pinned_at`` / ``aliases.updated_by``: alias CAS provenance.
- ``lineage``: producer/consumer links ('job' | 'remote_job' | 'version' |
  'alias') to resolved immutable versions.
- ``versions.deleted_at`` / ``delete_requested_at`` / ``pin_hold``:
  lifecycle columns (logical delete + pin/hold) used by GC.
- ``catalog_state``: small key/value table holding the
  ``recovery_required`` marker that locks a restored catalog out of
  mutations until recovery completes.

Schema v3 (Phase 8) adds:

- ``storage_profiles``: immutable storage-profile revisions (a profile is
  never updated in place; changing config inserts a new row with
  ``revision+1``, keyed ``UNIQUE(profile_id, revision)``).
- ``writer_leases``: catalog and root writer leases (keys like ``catalog``
  and ``root:<root_id>``) acquired via conditional upserts, fencing remote
  writers the way local ops are fenced by claim tokens.
- ``roots.profile_id``: optional pin of a root to a storage profile.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from ..migrations import busy_timeout_ms, configure_connection

__all__ = [
    "ARTIFACTS_LATEST_SCHEMA_VERSION",
    "busy_timeout_ms",
    "configure_connection",
    "migrate_artifacts",
]

ARTIFACTS_LATEST_SCHEMA_VERSION = 3


def _backup_before_migration(db: sqlite3.Connection, home: Path) -> Path:
    backups = home / "backups"
    backups.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = backups / f"artifacts-{stamp}.sqlite"
    destination = sqlite3.connect(path)
    try:
        db.backup(destination)
    finally:
        destination.close()
    return path


def _ensure_columns(db: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    """Additive ALTER TABLE for columns missing on an older layout."""
    existing = {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}
    for name, ddl in columns.items():
        if name not in existing:
            db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


V2_STATEMENTS = (
    """CREATE TABLE IF NOT EXISTS collections (
         collection_id TEXT PRIMARY KEY,
         name TEXT NOT NULL UNIQUE,
         created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS collection_versions (
         collection_id TEXT NOT NULL, version_id TEXT NOT NULL,
         ordinal INTEGER NOT NULL, created_at TEXT NOT NULL,
         PRIMARY KEY(collection_id, version_id))""",
    "CREATE INDEX IF NOT EXISTS idx_collection_versions_ord ON collection_versions(collection_id, ordinal)",
    """CREATE TABLE IF NOT EXISTS lineage (
         lin_id TEXT PRIMARY KEY,
         producer_kind TEXT NOT NULL, producer_id TEXT NOT NULL,
         consumer_kind TEXT NOT NULL, consumer_id TEXT NOT NULL,
         version_id TEXT NOT NULL, created_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS idx_lineage_version ON lineage(version_id)",
    "CREATE TABLE IF NOT EXISTS catalog_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
)

V2_VERSION_COLUMNS = {
    "deleted_at": "TEXT",
    "delete_requested_at": "TEXT",
    "pin_hold": "TEXT",
}

V2_ALIAS_COLUMNS = {
    "pinned_at": "TEXT",
    "updated_by": "TEXT",
}

V3_STATEMENTS = (
    # NOTE: the natural key here is (profile_id, revision), not profile_id
    # alone — a profile is never updated in place, so every revision of one
    # profile_id is its own row.
    """CREATE TABLE IF NOT EXISTS storage_profiles (
         profile_id TEXT NOT NULL,
         revision INTEGER NOT NULL,
         kind TEXT NOT NULL,
         config_json TEXT NOT NULL,
         capabilities_json TEXT NOT NULL,
         created_at TEXT NOT NULL,
         PRIMARY KEY(profile_id, revision))""",
    """CREATE TABLE IF NOT EXISTS writer_leases (
         lease_key TEXT PRIMARY KEY,
         owner_instance_id TEXT NOT NULL,
         claim_token TEXT NOT NULL,
         lease_expires_at TEXT NOT NULL,
         generation INTEGER NOT NULL DEFAULT 0,
         updated_at TEXT NOT NULL)""",
)

V3_ROOT_COLUMNS = {
    "profile_id": "TEXT",
}


def _create_latest_schema(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS catalog (
          id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
          state_epoch INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS versions (
          version_id TEXT PRIMARY KEY, root_id TEXT NOT NULL,
          manifest_digest TEXT NOT NULL, manifest_json TEXT NOT NULL,
          size_bytes INTEGER NOT NULL,
          deleted_at TEXT, delete_requested_at TEXT, pin_hold TEXT,
          created_at TEXT NOT NULL,
          UNIQUE(root_id, manifest_digest)
        );
        CREATE INDEX IF NOT EXISTS idx_versions_root ON versions(root_id, created_at);
        CREATE TABLE IF NOT EXISTS roots (
          root_id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
          latest_version_id TEXT,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        -- Phase 7: alias movement requires compare-and-swap via
        -- Collections.alias_set; there is no silent last-write-wins path.
        CREATE TABLE IF NOT EXISTS aliases (
          alias_name TEXT PRIMARY KEY, root_id TEXT NOT NULL,
          version_id TEXT NOT NULL, updated_at TEXT NOT NULL,
          pinned_at TEXT, updated_by TEXT
        );
        CREATE TABLE IF NOT EXISTS operations (
          op_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
          method TEXT NOT NULL, request_digest TEXT NOT NULL,
          payload_json TEXT NOT NULL, status TEXT NOT NULL,
          claim_token TEXT, claimed_at TEXT, lease_expires_at TEXT,
          lease_generation INTEGER NOT NULL DEFAULT 0,
          attempts INTEGER NOT NULL DEFAULT 0,
          progress_json TEXT, result_json TEXT, error TEXT,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_operations_status ON operations(status, lease_expires_at);
        """
    )
    for statement in V2_STATEMENTS:
        db.execute(statement)
    for statement in V3_STATEMENTS:
        db.execute(statement)
    # Fresh layouts already carry the v2 columns; ensure-columns keeps this
    # idempotent for partial/older layouts that reach this path.
    _ensure_columns(db, "aliases", V2_ALIAS_COLUMNS)
    _ensure_columns(db, "versions", V2_VERSION_COLUMNS)
    _ensure_columns(db, "roots", V3_ROOT_COLUMNS)
    db.execute(f"PRAGMA user_version={ARTIFACTS_LATEST_SCHEMA_VERSION}")


def migrate_artifacts(db: sqlite3.Connection, home: str | Path) -> Path | None:
    """Create the latest artifacts schema or upgrade an existing database.

    Returns the backup path when a migration backup was taken, else ``None``.
    """
    home = Path(home)
    version = int(db.execute("PRAGMA user_version").fetchone()[0])
    if version > ARTIFACTS_LATEST_SCHEMA_VERSION:
        raise RuntimeError(
            f"artifacts schema version {version} is newer than this Vanth binary supports "
            f"({ARTIFACTS_LATEST_SCHEMA_VERSION})"
        )
    has_catalog = bool(
        db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='catalog'").fetchone()
    )
    if not has_catalog:
        _create_latest_schema(db)
        db.commit()
        return None

    backup = None
    if version < ARTIFACTS_LATEST_SCHEMA_VERSION:
        backup = _backup_before_migration(db, home)
        try:
            db.execute("BEGIN IMMEDIATE")
            if version < 1:
                # v0 -> v1: create any tables/indexes missing from a partial or
                # older layout. CREATE ... IF NOT EXISTS keeps this idempotent.
                for statement in (
                    """CREATE TABLE IF NOT EXISTS catalog (
                         id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,
                         state_epoch INTEGER NOT NULL DEFAULT 1,
                         created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
                    """CREATE TABLE IF NOT EXISTS versions (
                         version_id TEXT PRIMARY KEY, root_id TEXT NOT NULL,
                         manifest_digest TEXT NOT NULL, manifest_json TEXT NOT NULL,
                         size_bytes INTEGER NOT NULL, created_at TEXT NOT NULL,
                         UNIQUE(root_id, manifest_digest))""",
                    "CREATE INDEX IF NOT EXISTS idx_versions_root ON versions(root_id, created_at)",
                    """CREATE TABLE IF NOT EXISTS roots (
                         root_id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
                         latest_version_id TEXT,
                         created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
                    """CREATE TABLE IF NOT EXISTS aliases (
                         alias_name TEXT PRIMARY KEY, root_id TEXT NOT NULL,
                         version_id TEXT NOT NULL, updated_at TEXT NOT NULL)""",
                    """CREATE TABLE IF NOT EXISTS operations (
                         op_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
                         method TEXT NOT NULL, request_digest TEXT NOT NULL,
                         payload_json TEXT NOT NULL, status TEXT NOT NULL,
                         claim_token TEXT, claimed_at TEXT, lease_expires_at TEXT,
                         lease_generation INTEGER NOT NULL DEFAULT 0,
                         attempts INTEGER NOT NULL DEFAULT 0,
                         progress_json TEXT, result_json TEXT, error TEXT,
                         created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
                    "CREATE INDEX IF NOT EXISTS idx_operations_status ON operations(status, lease_expires_at)",
                ):
                    db.execute(statement)
            if version < 2:
                # v1 -> v2: collections, lineage, lifecycle columns, and the
                # catalog_state key/value table (recovery_required marker).
                for statement in V2_STATEMENTS:
                    db.execute(statement)
                _ensure_columns(db, "aliases", V2_ALIAS_COLUMNS)
                _ensure_columns(db, "versions", V2_VERSION_COLUMNS)
            if version < 3:
                # v2 -> v3: storage-profile revisions, writer leases, and the
                # roots.profile_id pin column (Phase 8 S3 storage).
                for statement in V3_STATEMENTS:
                    db.execute(statement)
                _ensure_columns(db, "roots", V3_ROOT_COLUMNS)
            db.execute(f"PRAGMA user_version={ARTIFACTS_LATEST_SCHEMA_VERSION}")
            db.commit()
        except BaseException:
            db.rollback()
            raise
    return backup
