"""Verify a database after Go wrote to it (cross-language conformance).

Usage: uv run python scripts/verify_go_write.py <db_path>

Expected: a 'job_go_written' row written by Go with status 'completed', plus
the Go-written event 'evt_go' present with type 'checkpoint'.
"""

from __future__ import annotations

import sqlite3
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    db = sqlite3.connect(sys.argv[1])
    try:
        row = db.execute("SELECT status FROM jobs WHERE job_id='job_go_written'").fetchone()
        if not row or row[0] != "completed":
            print("FAIL: job_go_written missing or wrong status:", row)
            return 1
        event = db.execute("SELECT type FROM events WHERE event_id='evt_go'").fetchone()
        if not event or event[0] != "checkpoint":
            print("FAIL: evt_go missing or wrong type:", event)
            return 1
    finally:
        db.close()
    print("OK: Go-written rows readable by Python sqlite3")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
