"""Capture security: symlinks, special files, source mutation (Phase 6).

These tests run on Windows too: real symlinks are attempted and skipped
gracefully when the platform refuses to create them; junctions are exercised
via ``_winapi.CreateJunction`` where available.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import vanth.artifacts.manifest as manifest_module
from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.local_store import LocalBlobStore, default_store_root
from vanth.artifacts.manifest import build_manifest_from_tree
from vanth.artifacts.operations import ArtifactOperations


@pytest.fixture()
def ops(tmp_path):
    home = tmp_path / "state"
    catalog = open_catalog(home)
    return ArtifactOperations(catalog, LocalBlobStore(default_store_root(home), catalog))


def version_count(operations: ArtifactOperations) -> int:
    return int(operations.catalog.db.execute("SELECT COUNT(*) FROM versions").fetchone()[0])


def plain_tree(base: Path) -> Path:
    root = base / "src"
    root.mkdir()
    (root / "keep.txt").write_bytes(b"legit content")
    return root


def try_symlink(target: Path, link: Path) -> bool:
    """Create a symlink; returns False when the platform forbids it."""
    try:
        os.symlink(str(target), str(link))
        return True
    except (OSError, NotImplementedError):
        return False


def test_symlink_file_inside_source_refused(tmp_path, ops):
    root = plain_tree(tmp_path)
    if not try_symlink(root / "keep.txt", root / "link.txt"):
        pytest.skip("symlink creation unavailable on this platform/account")
    with pytest.raises(ValueError, match="symlink"):
        build_manifest_from_tree(root, "t")
    with pytest.raises(ValueError, match="symlink"):
        ops.put_dir(root, "t", idempotency_key="sym-file-1")
    assert version_count(ops) == 0


def test_symlink_directory_inside_source_refused(tmp_path, ops):
    root = plain_tree(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_bytes(b"should never be captured")
    if not try_symlink(outside, root / "linked_dir"):
        pytest.skip("symlink creation unavailable on this platform/account")
    with pytest.raises(ValueError, match="symlink"):
        build_manifest_from_tree(root, "t")
    assert version_count(ops) == 0


def test_junction_inside_source_refused(tmp_path):
    """Windows directory junctions are reparse points, not symlinks; they must
    still be refused via st_reparse_tag."""
    if os.name != "nt":
        pytest.skip("junctions are Windows-specific")
    root = plain_tree(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        import _winapi

        _winapi.CreateJunction(str(outside), str(root / "junction"))
    except (ImportError, OSError):
        pytest.skip("CreateJunction unavailable on this platform/account")
    # A junction must NOT look like an ordinary directory to capture.
    with pytest.raises(ValueError, match="reparse"):
        build_manifest_from_tree(root, "t")


def test_special_file_refused(tmp_path, ops):
    if not hasattr(os, "mkfifo"):
        pytest.skip("mkfifo is POSIX-only")
    root = plain_tree(tmp_path)
    os.mkfifo(root / "pipe")
    with pytest.raises(ValueError, match="special"):
        build_manifest_from_tree(root, "t")
    with pytest.raises(ValueError, match="special"):
        ops.put_dir(root, "t", idempotency_key="fifo-1")
    assert version_count(ops) == 0


def test_source_mutated_mid_capture_refused(tmp_path, ops, monkeypatch):
    root = plain_tree(tmp_path)
    victim = root / "sub" / "data.bin"
    victim.parent.mkdir()
    victim.write_bytes(b"0123456789")

    original_hasher = manifest_module._hash_file_streaming

    def shrinking_hasher(path):
        digest = original_hasher(path)
        Path(path).write_bytes(b"tiny")  # mutate after hashing
        return digest

    monkeypatch.setattr(manifest_module, "_hash_file_streaming", shrinking_hasher)
    with pytest.raises(ValueError, match="source mutated during capture"):
        build_manifest_from_tree(root, "t")
    with pytest.raises(ValueError, match="source mutated during capture"):
        ops.put_dir(root, "t", idempotency_key="mutate-1")
    assert version_count(ops) == 0


def test_vanishing_file_refused(tmp_path, monkeypatch):
    original_hasher = manifest_module._hash_file_streaming

    def vanishing_hasher(path):
        digest = original_hasher(path)
        Path(path).unlink()  # vanish between read and verification
        return digest

    for attempt in ("vanish-1", "vanish-2"):
        root = tmp_path / f"src-{attempt}"
        root.mkdir(parents=True)
        (root / "keep.txt").write_bytes(b"legit content")
        victim = root / "gone.bin"
        victim.write_bytes(b"vanish me")
        monkeypatch.setattr(manifest_module, "_hash_file_streaming", vanishing_hasher)
        with pytest.raises(ValueError, match="vanished mid-walk"):
            build_manifest_from_tree(root, "t")
        # Restore the vanished file so the durable-op path sees the same race.
        victim.write_bytes(b"vanish me")
        home = tmp_path / f"state-{attempt}"
        catalog = open_catalog(home)
        operations = ArtifactOperations(catalog, LocalBlobStore(default_store_root(home), catalog))
        with pytest.raises(ValueError, match="vanished mid-walk"):
            operations.put_dir(root, "t", idempotency_key=attempt)
        assert version_count(operations) == 0
        monkeypatch.undo()


def test_case_collision_in_tree_refused_at_capture(tmp_path):
    """A tree that would collide on a case-insensitive filesystem fails at
    capture time rather than publishing an un-materializable manifest."""
    root = tmp_path / "src"
    root.mkdir()
    (root / "Flag.txt").write_bytes(b"A")
    if (root / "flag.txt").exists():
        pytest.skip("host filesystem is case-insensitive; the colliding pair cannot coexist to be captured")
    (root / "flag.txt").write_bytes(b"B")
    with pytest.raises(ValueError, match="portability"):
        build_manifest_from_tree(root, "t")


def test_symlink_swap_after_check_fails_without_publishing(tmp_path, ops, monkeypatch):
    """Simulate a TOCTOU swap: by the time capture verifies stats, the entry
    has been replaced. The re-stat pass must catch the change."""
    root = plain_tree(tmp_path)
    target = root / "swap.bin"
    target.write_bytes(b"original bytes here")

    original_stat = manifest_module._stat_capture
    state = {"calls": 0}

    def swapping_stat(path, **kwargs):
        info = original_stat(path, **kwargs)
        if str(path).endswith("swap.bin"):
            state["calls"] += 1
            # Call 1 is the safety pre-stat, call 2 records (mtime_ns, size)
            # while reading; mutating right after call 2 simulates an
            # attacker swapping content before the verification re-stat.
            if state["calls"] == 2:
                target.write_bytes(b"replaced!")
        return info

    monkeypatch.setattr(manifest_module, "_stat_capture", swapping_stat)
    with pytest.raises(ValueError, match="source mutated during capture"):
        build_manifest_from_tree(root, "t")
    assert version_count(ops) == 0
