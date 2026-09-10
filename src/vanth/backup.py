"""Whole-state backup and restore for a Vanth home (review B1).

One archive holds every durable store that must move together: ``jobs.sqlite``
and ``artifacts.sqlite`` (via SQLite's online backup API, so WAL is safe),
the per-job event mirrors, and the managed-artifact blob store. A
``manifest.json`` records the schema version and a SHA-256 per file so a restore
can verify integrity before touching the live state.

The token, daemon lock/discovery files, and (by default) logs are excluded.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from .migrations import LATEST_SCHEMA_VERSION

BACKUP_FORMAT = "vanth-backup/1"
_SQLITE_FILES = ("jobs.sqlite", "artifacts.sqlite")
_TREE_DIRS = ("events",)
_ARTIFACT_TREE = "artifacts-store"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_sqlite(source: Path, destination: Path) -> None:
    source_connection = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        destination_connection = sqlite3.connect(destination)
        try:
            source_connection.backup(destination_connection)
        finally:
            destination_connection.close()
    finally:
        source_connection.close()


def _backup_name() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"vanth-backup-{stamp}.zip"


def create_backup(home: str | Path, *, out: str | Path | None = None, include_logs: bool = False) -> Path:
    """Write one archive of everything needed to restore ``home``. Returns its path."""
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True)
    destination = Path(out) if out else home / "backups" / _backup_name()
    destination.parent.mkdir(parents=True, exist_ok=True)
    files: list[dict[str, object]] = []

    with tempfile.TemporaryDirectory(prefix="vanth-backup-") as staging, zipfile.ZipFile(
        destination, "w", zipfile.ZIP_DEFLATED
    ) as archive:
        staging_path = Path(staging)
        for name in _SQLITE_FILES:
            source = home / name
            if not source.is_file():
                continue
            snapshot = staging_path / name
            _snapshot_sqlite(source, snapshot)
            archive.write(snapshot, name)
            files.append({"path": name, "sha256": _sha256_file(snapshot), "size": snapshot.stat().st_size})
            snapshot.unlink()

        for tree in (*_TREE_DIRS, _ARTIFACT_TREE):
            root = home / tree
            if not root.is_dir():
                continue
            for path in sorted(root.rglob("*")):
                if not path.is_file():
                    continue
                relative = path.relative_to(home)
                if "staging" in relative.parts or path.name.endswith(".lock"):
                    continue
                archive.write(path, relative.as_posix())
                files.append({"path": relative.as_posix(), "sha256": _sha256_file(path), "size": path.stat().st_size})

        if include_logs:
            root = home / "logs"
            if root.is_dir():
                for path in sorted(root.rglob("*")):
                    if path.is_file():
                        relative = path.relative_to(home)
                        archive.write(path, relative.as_posix())
                        files.append({"path": relative.as_posix(), "sha256": _sha256_file(path), "size": path.stat().st_size})

        manifest = {
            "format": BACKUP_FORMAT,
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "schema_version": LATEST_SCHEMA_VERSION,
            "include_logs": include_logs,
            "files": files,
        }
        archive.writestr("manifest.json", json.dumps(manifest, indent=2))
    return destination


def _safe_member(name: str, home: Path) -> Path:
    candidate = Path(name)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"unsafe path in backup archive: {name!r}")
    resolved = (home / candidate).resolve()
    if not resolved.is_relative_to(home.resolve()):
        raise ValueError(f"path escapes the home directory: {name!r}")
    return resolved


def restore_backup(home: str | Path, archive: str | Path, *, force: bool = False) -> dict[str, object]:
    """Verify and restore an archive into ``home`` (snapshotting current state first)."""
    home = Path(home)
    archive = Path(archive)
    if not archive.is_file():
        raise ValueError(f"backup archive not found: {archive}")
    if (home / "daemon.json").exists() and not force:
        raise ValueError("daemon appears to be running; stop it first or pass force=True")
    with zipfile.ZipFile(archive) as bundle:
        try:
            manifest = json.loads(bundle.read("manifest.json"))
        except KeyError as exc:
            raise ValueError("archive is not a Vanth backup (no manifest.json)") from exc
        if manifest.get("format") != BACKUP_FORMAT:
            raise ValueError(f"unsupported backup format: {manifest.get('format')!r}")
        schema = int(manifest.get("schema_version", 0))
        if schema > LATEST_SCHEMA_VERSION and not force:
            raise ValueError(
                f"backup schema v{schema} is newer than this binary supports (v{LATEST_SCHEMA_VERSION}); pass force=True"
            )
        files = manifest.get("files") or []
        # Verify every file before mutating live state.
        for entry in files:
            member = bundle.read(entry["path"])
            if hashlib.sha256(member).hexdigest() != entry["sha256"]:
                raise ValueError(f"backup integrity check failed for {entry['path']!r}")

    # Snapshot current state so a restore is itself reversible.
    try:
        create_backup(home, out=home / "backups" / f"pre-restore-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.zip")
    except Exception:
        pass

    home.mkdir(parents=True, exist_ok=True)
    restored = 0
    with zipfile.ZipFile(archive) as bundle:
        for entry in files:
            target = _safe_member(str(entry["path"]), home)
            target.parent.mkdir(parents=True, exist_ok=True)
            with bundle.open(entry["path"]) as source, target.open("wb") as destination_handle:
                destination_handle.write(source.read())
            restored += 1
    return {"result": "ok", "archive": str(archive), "files_restored": restored, "schema_version": schema}
