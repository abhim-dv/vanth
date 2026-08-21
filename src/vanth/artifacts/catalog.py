"""The single artifacts catalog row: identity, epoch, and id minting (Phase 5).

``artifacts.sqlite`` holds exactly one ``catalog`` row. Its ``instance_id``
changes whenever a backup is restored, which (together with the blob-store
ownership marker) locks a restored catalog out of foreign content until
explicit recovery. ``state_epoch`` is bumped on restore-style events so stale
workers can detect they are talking to a different catalog generation.
"""

from __future__ import annotations

import secrets
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from ..migrations import configure_connection
from .migrations import migrate_artifacts

__all__ = ["Catalog", "open_catalog", "new_id"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_id(prefix: str) -> str:
    """A fresh ``<prefix>_<32 hex>`` identifier per repo convention."""
    return f"{prefix}_{secrets.token_hex(16)}"


class Catalog:
    """Typed handle over the artifacts catalog database."""

    def __init__(self, db: sqlite3.Connection, home: str | Path) -> None:
        self.db = db
        self.home = Path(home)
        self.lock = threading.RLock()
        self._ensure_catalog_row()

    def _ensure_catalog_row(self) -> None:
        with self.lock:
            existing = self.db.execute("SELECT id FROM catalog LIMIT 1").fetchone()
            if existing:
                return
            stamp = now_iso()
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self.db.execute(
                    "INSERT INTO catalog(id, instance_id, state_epoch, created_at, updated_at)"
                    " VALUES (?, ?, 1, ?, ?)",
                    (new_id("cat"), new_id("cit"), stamp, stamp),
                )
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise

    def _row(self) -> sqlite3.Row:
        return self.db.execute("SELECT * FROM catalog LIMIT 1").fetchone()

    def identity(self) -> dict[str, object]:
        with self.lock:
            row = self._row()
            return {
                "catalog_id": row["id"],
                "instance_id": row["instance_id"],
                "state_epoch": int(row["state_epoch"]),
            }

    def bump_epoch(self) -> int:
        """Increment and return the catalog state epoch (restore lockout hook)."""
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self.db.execute(
                    "UPDATE catalog SET state_epoch=state_epoch+1, updated_at=? WHERE id=(SELECT id FROM catalog LIMIT 1)",
                    (now_iso(),),
                )
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
            return int(self._row()["state_epoch"])

    @staticmethod
    def new_version_id() -> str:
        return new_id("ver")

    @staticmethod
    def new_root_id() -> str:
        return new_id("rot")

    @staticmethod
    def new_op_id() -> str:
        return new_id("aop")


def open_catalog(home: str | Path) -> Catalog:
    """Open (creating/migrating as needed) the catalog at ``<home>/artifacts.sqlite``."""
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(home / "artifacts.sqlite", check_same_thread=False)
    db.row_factory = sqlite3.Row
    configure_connection(db)
    migrate_artifacts(db, home)
    return Catalog(db, home)
