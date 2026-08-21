"""Remote-side durable change-feed outbox (Phase 4).

:class:`FeedStore` records every change controllers need to observe — job
upserts and tombstones — as append-only outbox rows in ``remote_feed`` on the
REMOTE database. Rows carry ``(state_epoch, feed_epoch, feed_seq)`` so a
controller cursor can resume exactly where it left off and detect gaps:

- ``state_epoch`` is the remote's timeline version (bumped when its database
  is restored). A restore also bumps ``feed_epoch``, so any batch from a
  previous timeline is never served against a stale cursor.
- ``feed_seq`` is a monotonic AUTOINCREMENT sequence within the table.

Retention: Phase 4 keeps FULL history (no compaction); the plan explicitly
defers soft/emergency byte targets. :meth:`FeedStore.feed_high_water` returns
the oldest retained seq so gap detection can compare a caller's cursor against
available history.

Deferred (documented, not implemented): direct-response/feed mapping
agreement enforcement (a shadow whose status conflicts with a fresh
``job.status`` round-trip putting the remote into ``sync_blocked``).
"""

from __future__ import annotations

import json
from typing import Any

from ..server import now_iso

FEED_DDL = """
CREATE TABLE IF NOT EXISTS remote_feed (
  feed_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  state_epoch INTEGER NOT NULL,
  feed_epoch INTEGER NOT NULL DEFAULT 1,
  kind TEXT NOT NULL,
  job_id TEXT,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_remote_feed_cursor
  ON remote_feed(state_epoch, feed_epoch, feed_seq);
"""


class FeedStore:
    """Append-only outbox over the same sqlite connection as the remote store."""

    def __init__(self, db) -> None:
        self.db = db
        self.db.executescript(FEED_DDL)
        self.db.commit()

    # ------------------------------------------------------------------
    # Epochs (stored alongside state_epoch in remote_state)
    # ------------------------------------------------------------------

    def epochs(self) -> tuple[int, int]:
        row = self.db.execute(
            "SELECT state_epoch, feed_epoch FROM remote_state WHERE id=1"
        ).fetchone()
        return (
            int(row["state_epoch"]) if row else 1,
            int(row["feed_epoch"]) if row and row["feed_epoch"] else 1,
        )

    # ------------------------------------------------------------------
    # Append / read
    # ------------------------------------------------------------------

    def append(self, kind: str, *, job_id: str | None = None,
               payload: Any | None = None) -> int:
        """Append one outbox row pinned to the CURRENT epochs; returns its seq."""
        state_epoch, feed_epoch = self.epochs()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.db.execute(
                """
                INSERT INTO remote_feed(state_epoch, feed_epoch, kind, job_id, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    state_epoch, feed_epoch, kind, job_id,
                    json.dumps(payload if payload is not None else {},
                               separators=(",", ":")),
                    now_iso(),
                ),
            )
            seq = int(cursor.lastrowid)
            self.db.commit()
            return seq
        except BaseException:
            self.db.rollback()
            raise

    def read(self, cursor: dict[str, Any] | None = None, *, limit: int = 100) -> dict[str, Any]:
        """Read rows strictly after ``cursor['seq']`` within the CURRENT epochs."""
        state_epoch, feed_epoch = self.epochs()
        seq = 0
        if isinstance(cursor, dict):
            try:
                seq = int(cursor.get("seq") or 0)
            except (TypeError, ValueError):
                seq = 0
        rows = self.db.execute(
            """
            SELECT feed_seq, state_epoch, feed_epoch, kind, job_id, payload_json, created_at
            FROM remote_feed WHERE state_epoch=? AND feed_epoch=? AND feed_seq>?
            ORDER BY feed_seq ASC LIMIT ?
            """,
            (state_epoch, feed_epoch, seq, max(0, limit)),
        ).fetchall()
        oldest = self.db.execute(
            "SELECT MIN(feed_seq) FROM remote_feed WHERE state_epoch=? AND feed_epoch=?",
            (state_epoch, feed_epoch),
        ).fetchone()[0]
        high_water = self.db.execute(
            "SELECT MAX(feed_seq) FROM remote_feed WHERE state_epoch=? AND feed_epoch=?",
            (state_epoch, feed_epoch),
        ).fetchone()[0]
        changes = [
            {
                "seq": int(row["feed_seq"]),
                "kind": row["kind"],
                "job_id": row["job_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]
        return {
            "state_epoch": state_epoch,
            "feed_epoch": feed_epoch,
            "changes": changes,
            "has_more": len(changes) == limit and len(changes) > 0,
            "oldest_seq": int(oldest) if oldest is not None else None,
            "high_water_seq": int(high_water) if high_water is not None else None,
        }

    def feed_high_water(self) -> int | None:
        """Oldest retained seq for the current epochs (None when empty).

        With full history retention this is the first seq ever recorded;
        once compaction lands it becomes the compaction floor, letting a
        controller detect that its cursor predates available history.
        """
        state_epoch, feed_epoch = self.epochs()
        row = self.db.execute(
            "SELECT MIN(feed_seq) FROM remote_feed WHERE state_epoch=? AND feed_epoch=?",
            (state_epoch, feed_epoch),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None
