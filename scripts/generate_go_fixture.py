"""Generate a deterministic database fixture for Go conformance.

The fixture is checked in at testdata/state/jobs.sqlite so the Go test suite
can run without Python present. Values are fixed (no timestamps or absolute
paths) so regeneration is byte-stable across OSes and machines. The Go side
opens the same file with modernc.org/sqlite to prove cross-language
compatibility, and a round-trip test has Go write rows that Python verifies.

The schema is built through the SAME ``migrate()`` path the daemon uses, so the
fixture can never drift from ``LATEST_SCHEMA_VERSION`` again (a frozen DDL once
left the fixture at v9 while the daemon reached v13).

Regenerate (keep in sync with any schema change):
    uv run python scripts/generate_go_fixture.py testdata
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

from vanth.migrations import LATEST_SCHEMA_VERSION, migrate


def seed(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        # Idempotent regeneration: drop the previous fixture so re-running
        # the generator (e.g. from CI on a checked-out fixture) produces a
        # fresh, byte-stable database instead of failing on existing tables.
        db_path.unlink()
    db = sqlite3.connect(db_path)
    try:
        # Build the schema through the SAME migration path the daemon uses so the
        # fixture can never drift from LATEST_SCHEMA_VERSION again (a frozen DDL
        # once left the fixture at v9 while the daemon reached v13). migrate()
        # creates the full latest schema on a fresh database.
        migrate(db, db_path.parent)
        stamp = "2026-01-01T00:00:00Z"
        with db:
            db.execute(
                "INSERT INTO jobs(job_id, name, command, status, pid, worker_pid, runner_heartbeat_at, "
                "created_at, updated_at, started_at, ended_at, exit_code, tags_json, env_json, notes, run_json, "
                "stdout_path, stderr_path, events_path) "
                "VALUES (?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "job_completed",
                    "conformance complete",
                    "echo done",
                    1234,
                    1235,
                    stamp,
                    stamp,
                    stamp,
                    stamp,
                    stamp,
                    0,
                    json.dumps(["fixture"]),
                    json.dumps({"FIXTURE_ENV": "1"}),
                    "conformance notes",
                    json.dumps({"author": "fixture", "hostname": "test-host", "os": "test"}),
                    "logs/job_completed.stdout.log",
                    "logs/job_completed.stderr.log",
                    "events/job_completed.jsonl",
                ),
            )
            db.execute(
                "INSERT INTO jobs(job_id, name, command, status, pid, worker_pid, runner_heartbeat_at, "
                "created_at, updated_at, started_at, tags_json, env_json, notes, run_json, "
                "stdout_path, stderr_path, events_path) "
                "VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "job_running",
                    "conformance running",
                    "sleep 30",
                    2345,
                    2346,
                    stamp,
                    stamp,
                    stamp,
                    stamp,
                    json.dumps(["live"]),
                    json.dumps({"FIXTURE_ENV": "2"}),
                    "running notes",
                    json.dumps({"author": "fixture", "hostname": "test-host", "os": "test"}),
                    "logs/job_running.stdout.log",
                    "logs/job_running.stderr.log",
                    "events/job_running.jsonl",
                ),
            )
            db.execute(
                "INSERT INTO events(event_id, job_id, seq, type, level, message, data_json, source, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("evt_complete", "job_completed", 1, "completed", "info", None, "{}", "server", stamp),
            )
            db.execute(
                "INSERT INTO events(event_id, job_id, seq, type, level, message, data_json, source, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("evt_progress", "job_running", 1, "progress", "info", "epoch 1",
                 json.dumps({"current": 1, "total": 3, "percent": 33.33}), "stdout", stamp),
            )
            db.execute(
                "INSERT INTO wake_targets(target_id, job_id, type, events_json, config_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("target_1", "job_completed", "codex_thread",
                 json.dumps(["completed"]), json.dumps({"thread_id": "thread_fixture"}), stamp),
            )
            db.execute(
                "INSERT INTO deliveries(delivery_id, event_id, target_id, job_id, target_type, status, attempts, payload_json, "
                "created_at, delivered_at) VALUES (?, ?, ?, ?, ?, 'delivered', 1, ?, ?, ?)",
                ("del_1", "evt_complete", "target_1", "job_completed", "codex_thread",
                 json.dumps({"target": {"type": "codex_thread", "thread_id": "thread_fixture"},
                             "event": {"type": "completed"}, "delivery_id": "del_1"}), stamp, stamp),
            )
            db.execute(
                "INSERT INTO delivery_attempts(attempt_id, delivery_id, attempt, claim_token, target_type, "
                "started_at, ended_at, status, reclaimed, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'delivered', 0, ?)",
                ("att_1", "del_1", 1, "tok_1", "codex_thread", stamp, stamp, stamp),
            )
            db.execute(
                "INSERT INTO cleanup_tombstones(tombstone_id, job_id, artifacts_json, created_at) "
                "VALUES (?, ?, ?, ?)",
                ("clean_1", "job_completed", json.dumps(["logs/gone.log"]), stamp),
            )
            db.execute(
                "INSERT INTO metric_series(series_id, job_id, metric, x, y, stage, event_id, seq, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("ser_1", "job_running", "loss", 1.0, 0.42, "train", "evt_progress", 1, stamp),
            )
            db.execute(
                "INSERT INTO artifacts(artifact_id, job_id, name, uri, kind, size_bytes, sha256, meta_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("art_1", "job_completed", "best.pt", "file:///tmp/best.pt", "checkpoint",
                 123, "abc123", json.dumps({"epoch": 5}), stamp),
            )
        schema = db.execute("PRAGMA user_version").fetchone()[0]
        assert schema == LATEST_SCHEMA_VERSION, schema
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        db.close()
    for suffix in ("-wal", "-shm"):
        try:
            Path(str(db_path) + suffix).unlink()
        except FileNotFoundError:
            pass
    print(f"fixture written: {db_path} (schema v{schema})")


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    seed(Path(sys.argv[1]) / "state" / "jobs.sqlite")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
