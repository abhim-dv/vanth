"""Directory materialization: round-trip, never-merge, symlink defense (Phase 6)."""

from __future__ import annotations

import hashlib
import os
import stat as stat_module
import sys
from pathlib import Path

import pytest

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.local_store import LocalBlobStore, default_store_root
from vanth.artifacts.operations import ArtifactOperations


@pytest.fixture()
def ops(tmp_path):
    home = tmp_path / "state"
    catalog = open_catalog(home)
    return ArtifactOperations(catalog, LocalBlobStore(default_store_root(home), catalog))


def make_source(base: Path) -> Path:
    src = base / "src"
    (src / "a" / "b").mkdir(parents=True)
    (src / "a" / "b" / "hello.txt").write_bytes(b"hello world\n")
    (src / "dup1.bin").write_bytes(b"same")
    (src / "dup2.bin").write_bytes(b"same")
    (src / "emptydir").mkdir()
    script = src / "a" / "run.sh"
    script.write_bytes(b"#!/bin/sh\necho hi\n")
    if os.name != "nt":
        os.chmod(script, 0o755)
    return src


def test_round_trip_byte_exact(tmp_path, ops):
    src = make_source(tmp_path)
    published = ops.put_dir(src, "tree", idempotency_key="rt-put-1")
    assert published["deduplicated"] is False
    dest = tmp_path / "out" / "tree"

    result = ops.materialize(published["version_id"], dest, idempotency_key="rt-mat-1")
    assert result["overwritten"] is False

    # Every file byte-exact.
    for path in src.rglob("*"):
        rel = path.relative_to(src)
        target = dest / rel
        if path.is_dir():
            assert target.is_dir()
        else:
            assert target.read_bytes() == path.read_bytes()
    # Empty dir exists.
    assert (dest / "emptydir").is_dir()
    # Executable bit restored on POSIX.
    if os.name != "nt":
        restored = dest / "a" / "run.sh"
        assert restored.stat().st_mode & 0o111


def test_existing_destination_refused_never_merges(tmp_path, ops):
    src = make_source(tmp_path)
    published = ops.put_dir(src, "tree", idempotency_key="nm-put-1")
    dest = tmp_path / "out" / "tree"
    dest.mkdir(parents=True)
    sentinel = dest / "sentinel.txt"
    sentinel.write_bytes(b"do not touch")

    with pytest.raises(ValueError, match="never merges"):
        ops.materialize(published["version_id"], dest, idempotency_key="nm-mat-1")
    # overwrite=True is also refused for directories: never merge.
    with pytest.raises(ValueError, match="never merges"):
        ops.materialize(published["version_id"], dest, overwrite=True, idempotency_key="nm-mat-2")
    assert sentinel.read_bytes() == b"do not touch"


def test_atomic_directory_publication_never_replaces_empty_destination(tmp_path, ops):
    src = tmp_path / "staging"
    dest = tmp_path / "destination"
    src.mkdir()
    dest.mkdir()
    with pytest.raises(OSError):
        ops._rename_noreplace(src, dest)
    assert src.is_dir()
    assert dest.is_dir()


def test_symlinked_dest_parent_component_refused(tmp_path, ops):
    """A planted symlink/junction at a destination parent component must be
    refused without touching anything beyond it."""
    real = tmp_path / "real"
    real.mkdir()
    link_parent = tmp_path / "link"
    created = False
    if hasattr(os, "symlink"):
        try:
            os.symlink(str(real), str(link_parent), target_is_directory=True)
            created = link_parent.exists()
        except (OSError, NotImplementedError):
            created = False
    if not created and sys.platform == "win32":
        try:
            import _winapi

            _winapi.CreateJunction(str(real), str(link_parent))
            created = True
        except (ImportError, OSError):
            created = False
    if not created:
        pytest.skip("symlink/junction creation unavailable on this platform/account")

    src = make_source(tmp_path)
    published = ops.put_dir(src, "tree", idempotency_key="sp-put-1")
    with pytest.raises(ValueError, match="symlink|reparse"):
        ops.materialize(published["version_id"], link_parent / "inner", idempotency_key="sp-mat-1")
    # Nothing was written through the link.
    assert list(real.iterdir()) == []


def test_component_swap_inside_staging_refused_and_cleaned(tmp_path, ops, monkeypatch):
    """A component that turns into a symlink between creation and use is
    caught by the lstat-before-use check; staging is removed."""
    src = tmp_path / "src"
    (src / "deep").mkdir(parents=True)
    (src / "deep" / "f.txt").write_bytes(b"data")
    published = ops.put_dir(src, "t", idempotency_key="sw-put-1")
    dest = tmp_path / "out" / "tree"
    dest.parent.mkdir(parents=True)

    import vanth.artifacts.operations as operations_module

    original_lstat = os.lstat
    state = {"armed": True}

    def poisoned_lstat(path, *args, **kwargs):
        info = original_lstat(path, *args, **kwargs)
        p = Path(path)
        # When materialization first lstats a staging dir it just made,
        # plant a reparse-style failure: replace it with a symlink so the
        # next use must be refused by the lstat-before-use check.
        if state["armed"] and ".materializing-" in str(p) and p.name == "deep":
            state["armed"] = False
            p.rmdir()
            if hasattr(os, "symlink"):
                try:
                    os.symlink(str(dest.parent), str(p), target_is_directory=True)
                except (OSError, NotImplementedError):
                    pass
        return info

    monkeypatch.setattr(operations_module.os, "lstat", poisoned_lstat)
    if hasattr(os, "symlink"):
        with pytest.raises(ValueError):
            ops.materialize(published["version_id"], dest, idempotency_key="sw-mat-1")
    else:
        # Without symlink support the poison step no-ops; materialization
        # still succeeds via the same code path.
        ops.materialize(published["version_id"], dest, idempotency_key="sw-mat-2")
    monkeypatch.undo()
    # No staging leftovers survive a refused materialization.
    assert [p for p in dest.parent.iterdir() if ".materializing-" in p.name] == []


def test_verify_reports_offending_entry_for_tampered_blob(tmp_path, ops):
    src = make_source(tmp_path)
    published = ops.put_dir(src, "tree", idempotency_key="vt-put-1")

    ok_report = ops.verify(published["version_id"], idempotency_key="vt-ver-1")
    assert ok_report["ok"] is True
    assert ok_report["offending_paths"] == []

    manifest = ops.info(published["version_id"], idempotency_key="vt-info-1")["manifest"]
    victim = next(e for e in manifest["entries"] if e["path"] == "dup2.bin")
    ops.blobs.blob_path(victim["sha256"]).write_bytes(b"TAMPERED!")

    report = ops.verify(published["version_id"], idempotency_key="vt-ver-2")
    assert report["ok"] is False
    # dup1.bin and dup2.bin share one blob (repeated reference), so both
    # entries are listed as offending.
    assert report["offending_paths"] == ["dup1.bin", "dup2.bin"]
    entry_report = {r["path"]: r for r in report["entries"]}
    assert entry_report["dup1.bin"]["actual_sha256"] == hashlib.sha256(b"TAMPERED!").hexdigest()

    op = ops.catalog.db.execute(
        "SELECT status FROM operations WHERE idempotency_key='vt-ver-2'"
    ).fetchone()
    assert op["status"] == "completed"


def test_verify_empty_dirs_do_not_count_as_offenders(tmp_path, ops):
    src = tmp_path / "src"
    src.mkdir()
    (src / "only-empty").mkdir()
    published = ops.put_dir(src, "bare", idempotency_key="ve-put-1")
    report = ops.verify(published["version_id"], idempotency_key="ve-ver-1")
    assert report["ok"] is True


def test_missing_blob_refuses_materialization_without_touching_dest(tmp_path, ops):
    src = tmp_path / "src"
    src.mkdir()
    (src / "f.txt").write_bytes(b"payload")
    published = ops.put_dir(src, "t", idempotency_key="mb-put-1")
    manifest = ops.info(published["version_id"], idempotency_key="mb-info-1")["manifest"]
    sha = manifest["entries"][0]["sha256"]
    os.unlink(ops.blobs.blob_path(sha))

    dest = tmp_path / "out" / "tree"
    with pytest.raises(ValueError, match="blobs missing"):
        ops.materialize(published["version_id"], dest, idempotency_key="mb-mat-1")
    assert not dest.exists()


def test_file_root_materialization_unchanged(tmp_path, ops):
    data = b"still one file"
    published = ops.put_file("f.bin", data=data, idempotency_key="fr-put-1")
    dest = tmp_path / "out" / "copy.bin"
    ops.materialize(published["version_id"], dest, idempotency_key="fr-mat-1")
    assert dest.read_bytes() == data
