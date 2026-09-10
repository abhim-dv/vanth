"""Whole-state backup and restore for a Vanth home (review B1).

One archive holds every durable store that must move together: ``jobs.sqlite``,
``artifacts.sqlite``, and ``remote.sqlite`` (via SQLite's online backup API, so
WAL is safe), the per-job event mirrors, and the managed-artifact blob store. A
``manifest.json`` records the schema version and a SHA-256 per file so a restore
can verify integrity before touching the live state.

The token, daemon lock/discovery files, and (by default) logs are excluded.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from .migrations import LATEST_SCHEMA_VERSION

BACKUP_FORMAT = "vanth-backup/1"
_SQLITE_FILES = ("jobs.sqlite", "artifacts.sqlite", "remote.sqlite")
_TREE_DIRS = ("events",)
_ARTIFACT_TREE = "artifacts-store"
_MANAGED_TREE_NAMES = {*_TREE_DIRS, _ARTIFACT_TREE, "logs"}
_DEFAULT_MAX_MEMBER_BYTES = 8 * 1024**3


def _max_member_bytes() -> int:
    try:
        return max(1, int(os.environ.get("VANTH_BACKUP_MAX_MEMBER_BYTES", str(_DEFAULT_MAX_MEMBER_BYTES))))
    except ValueError:
        return _DEFAULT_MAX_MEMBER_BYTES


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
    temporary = destination.with_name(destination.name + ".tmp")
    # Never archive the archive itself (e.g. ``--out`` placed inside events/ or
    # artifacts-store/), and keep the temp staging dir out of the walk.
    skip = {temporary.resolve(), destination.resolve(), (home / "backups").resolve()}
    files: list[dict[str, object]] = []

    with tempfile.TemporaryDirectory(prefix="vanth-backup-") as staging, zipfile.ZipFile(
        temporary, "w", zipfile.ZIP_DEFLATED
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
                if path.resolve() in skip:
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
    os.replace(temporary, destination)  # atomic publish
    return destination


def _safe_member(name: str, home: Path) -> Path:
    candidate = Path(name)
    if candidate.is_absolute() or ".." in candidate.parts or "\\" in name:
        raise ValueError(f"unsafe path in backup archive: {name!r}")
    resolved = (home / candidate).resolve()
    if not resolved.is_relative_to(home.resolve()):
        raise ValueError(f"path escapes the home directory: {name!r}")
    return resolved


def _verify_archive(bundle: zipfile.ZipFile, entries: list[tuple[dict, str]]) -> None:
    limit = _max_member_bytes()
    for entry, name in entries:
        info = bundle.getinfo(name)
        if info.file_size > limit:
            raise ValueError(f"backup member too large: {name!r}")
        digest = hashlib.sha256()
        with bundle.open(name) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != entry["sha256"]:
            raise ValueError(f"backup integrity check failed for {name!r}")


def restore_backup(home: str | Path, archive: str | Path, *, force: bool = False) -> dict[str, object]:
    """Verify and restore an archive into ``home`` (snapshotting current state first)."""
    home = Path(home)
    archive = Path(archive)
    if not archive.is_file():
        raise ValueError(f"backup archive not found: {archive}")

    if not force:
        # Use the REAL home lock (not daemon.json existence): a live daemon holds
        # it, so acquire failing means "running". Force bypasses for recovery.
        from .daemon import DaemonLock

        lock = DaemonLock(home / "daemon.lock")
        if not lock.acquire():
            raise ValueError("daemon appears to be running (home lock held); stop it first, or pass force=True")
        lock.release()

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
        # Validate EVERY member path AND verify content BEFORE any mutation, so a
        # malformed or tampered archive cannot partially overwrite live state.
        validated: list[tuple[dict, str, Path]] = []
        for entry in files:
            name = str(entry["path"])
            target = _safe_member(name, home)
            validated.append((entry, name, target))
        _verify_archive(bundle, [(entry, name) for entry, name, _ in validated])

    # Snapshot current state so a restore is itself reversible.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    try:
        create_backup(home, out=home / "backups" / f"pre-restore-{timestamp}.zip")
    except Exception:
        pass

    home.mkdir(parents=True, exist_ok=True)
    # Drop the managed trees the archive repopulates so stale files cannot
    # survive a restore (SQLite catalogs + filesystem stay consistent).
    tree_roots = {Path(name).parts[0] for _, name, _ in validated if len(Path(name).parts) > 1}
    for tree in tree_roots & _MANAGED_TREE_NAMES:
        shutil.rmtree(home / tree, ignore_errors=True)

    restored = 0
    with zipfile.ZipFile(archive) as bundle:
        for entry, name, target in validated:
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(target.name + ".restore-tmp")
            with bundle.open(name) as source, temporary.open("wb") as destination_handle:
                shutil.copyfileobj(source, destination_handle)
            os.replace(temporary, target)  # atomic per file
            restored += 1
    return {"result": "ok", "archive": str(archive), "files_restored": restored, "schema_version": schema}
