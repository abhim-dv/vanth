"""Whole-state backup/restore (review B1)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from vanth.backup import create_backup, restore_backup
from vanth.cli import cmd_backup, cmd_restore
from vanth.migrations import LATEST_SCHEMA_VERSION
from vanth.server import JobManager


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def _seed(home: Path) -> None:
    manager = JobManager(home, recover=False)
    try:
        asyncio.run(manager.start(cmd("print('hi')")))
        blob = home / "artifacts-store" / "blobs" / "aa" / "bb" / "deadbeef"
        blob.parent.mkdir(parents=True, exist_ok=True)
        blob.write_bytes(b"artifact-bytes")
    finally:
        manager.close()


def _job_count(home: Path) -> int:
    connection = sqlite3.connect(home / "jobs.sqlite")
    try:
        return connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    finally:
        connection.close()


def test_backup_restore_round_trip(tmp_path):
    home = tmp_path / "state"
    _seed(home)
    archive = create_backup(home)
    assert archive.is_file()

    # Destroy live state, then restore it.
    connection = sqlite3.connect(home / "jobs.sqlite")
    connection.execute("DELETE FROM jobs")
    connection.commit()
    connection.close()
    (home / "artifacts-store" / "blobs" / "aa" / "bb" / "deadbeef").unlink()
    assert _job_count(home) == 0

    result = restore_backup(home, archive)
    assert result["result"] == "ok"
    assert _job_count(home) == 1
    assert (home / "artifacts-store" / "blobs" / "aa" / "bb" / "deadbeef").exists()


def test_restore_refuses_tampered_archive(tmp_path):
    archive = tmp_path / "tampered.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("jobs.sqlite", b"not a real database")
        bundle.writestr(
            "manifest.json",
            json.dumps(
                {
                    "format": "vanth-backup/1",
                    "schema_version": LATEST_SCHEMA_VERSION,
                    "files": [{"path": "jobs.sqlite", "sha256": "0" * 64, "size": 21}],
                }
            ),
        )
    with pytest.raises(ValueError, match="integrity"):
        restore_backup(tmp_path / "home", archive)


def test_restore_refuses_newer_schema(tmp_path):
    archive = tmp_path / "future.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("jobs.sqlite", b"x")
        import hashlib

        bundle.writestr(
            "manifest.json",
            json.dumps(
                {
                    "format": "vanth-backup/1",
                    "schema_version": LATEST_SCHEMA_VERSION + 1,
                    "files": [{"path": "jobs.sqlite", "sha256": hashlib.sha256(b"x").hexdigest(), "size": 1}],
                }
            ),
        )
    with pytest.raises(ValueError, match="newer than"):
        restore_backup(tmp_path / "home", archive)


def test_restore_rejects_zipslip_before_mutation(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    (home / "sentinel.txt").write_text("keep", encoding="utf-8")
    archive = tmp_path / "slip.zip"
    good, evil = b"good", b"evil"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("good.sqlite", good)
        bundle.writestr("../escaped.txt", evil)
        bundle.writestr(
            "manifest.json",
            json.dumps(
                {
                    "format": "vanth-backup/1",
                    "schema_version": LATEST_SCHEMA_VERSION,
                    "files": [
                        {"path": "good.sqlite", "sha256": hashlib.sha256(good).hexdigest(), "size": 4},
                        {"path": "../escaped.txt", "sha256": hashlib.sha256(evil).hexdigest(), "size": 4},
                    ],
                }
            ),
        )
    with pytest.raises(ValueError, match="unsafe path|escapes"):
        restore_backup(home, archive)
    assert not (tmp_path / "escaped.txt").exists()
    assert not (home / "good.sqlite").exists(), "no member may be written before full validation"
    assert (home / "sentinel.txt").read_text(encoding="utf-8") == "keep"


def test_restore_removes_stale_managed_files(tmp_path):
    home = tmp_path / "state"
    _seed(home)
    archive = create_backup(home)
    stale = home / "artifacts-store" / "blobs" / "cc" / "dd" / "stale"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_bytes(b"stale")
    restore_backup(home, archive)
    assert not stale.exists(), "stale managed files must not survive a restore"


def test_cli_backup_and_restore_guard(tmp_path, capsys):
    home = tmp_path / "state"
    _seed(home)
    assert cmd_backup([], home) == 0
    out = capsys.readouterr().out
    assert "backup written:" in out
    archive = out.strip().split("backup written: ", 1)[1]
    assert Path(archive).is_file()
    # Restore without --yes is refused.
    assert cmd_restore([archive], home) == 2
    assert "refusing without --yes" in capsys.readouterr().err
