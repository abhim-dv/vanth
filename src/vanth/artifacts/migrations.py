"""Ordered SQLite migrations for the separate artifacts catalog (``artifacts.sqlite``).

Mirrors :mod:`vanth.migrations`: a single ``PRAGMA user_version`` counter, a
backup before any upgrade, and a transactional v0 -> latest path. Connection
tuning is reused verbatim from ``vanth.migrations.configure_connection`` so
both databases share WAL, foreign keys, and the busy timeout.
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

ARTIFACTS_LATEST_SCHEMA_VERSION = 1


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
          created_at TEXT NOT NULL,
          UNIQUE(root_id, manifest_digest)
        );
        CREATE INDEX IF NOT EXISTS idx_versions_root ON versions(root_id, created_at);
        CREATE TABLE IF NOT EXISTS roots (
          root_id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
          latest_version_id TEXT,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        -- Aliases are last-write-wins in Phase 5: an upsert moves the alias to
        -- a new pinned version. Compare-and-swap alias movement arrives in
        -- Phase 7; until then readers must treat aliases as advisory pins.
        CREATE TABLE IF NOT EXISTS aliases (
          alias_name TEXT PRIMARY KEY, root_id TEXT NOT NULL,
          version_id TEXT NOT NULL, updated_at TEXT NOT NULL
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
        PRAGMA user_version=1;
        """
    )


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
            db.execute(f"PRAGMA user_version={ARTIFACTS_LATEST_SCHEMA_VERSION}")
            db.commit()
        except BaseException:
            db.rollback()
            raise
    return backup
