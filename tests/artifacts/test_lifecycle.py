"""Phase 7: lifecycle — logical delete, pin/hold, fenced GC, backup/restore."""

from __future__ import annotations

import pytest

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.collections import Collections
from vanth.artifacts.lifecycle import Lifecycle
from vanth.artifacts.local_store import LocalBlobStore, default_store_root
from vanth.artifacts.operations import ArtifactOperations


@pytest.fixture()
def home(tmp_path):
    return tmp_path / "state"


@pytest.fixture()
def ops(home) -> ArtifactOperations:
    catalog = open_catalog(home)
    blobs = LocalBlobStore(default_store_root(home), catalog)
    return ArtifactOperations(catalog, blobs)


@pytest.fixture()
def lifecycle(ops) -> Lifecycle:
    return Lifecycle(ops.catalog, ops)


@pytest.fixture()
def collections(ops) -> Collections:
    return Collections(ops.catalog, ops)


def _row(ops, version_id):
    return ops.catalog.db.execute(
        "SELECT * FROM versions WHERE version_id=?", (version_id,)
    ).fetchone()


# ---------------------------------------------------------------------------
# request_delete / restore / pin / unpin round trips
# ---------------------------------------------------------------------------


def test_request_delete_and_restore_round_trip(ops, lifecycle):
    v1 = ops.put_file("r.bin", data=b"roundtrip", idempotency_key="lc-rt-key-01")
    out = lifecycle.request_delete(v1["version_id"], idempotency_key="lc-rt-del")
    assert out["delete_requested"] is True
    assert _row(ops, v1["version_id"])["delete_requested_at"] is not None
    # content stays (logical delete only)
    assert ops.blobs.has_blob(v1["sha256"])

    back = lifecycle.restore(v1["version_id"], idempotency_key="lc-rt-res")
    assert back["delete_requested"] is False
    assert _row(ops, v1["version_id"])["delete_requested_at"] is None


def test_pin_unpin_round_trip(ops, lifecycle):
    v1 = ops.put_file("p.bin", data=b"pinned", idempotency_key="lc-pin-key-01")
    assert _row(ops, v1["version_id"])["pin_hold"] is None
    out = lifecycle.pin(v1["version_id"], "legal-hold", idempotency_key="lc-pin-set")
    assert out["pin_hold"] == "legal-hold"
    assert _row(ops, v1["version_id"])["pin_hold"] == "legal-hold"
    lifecycle.unpin(v1["version_id"], idempotency_key="lc-pin-clear")
    assert _row(ops, v1["version_id"])["pin_hold"] is None


# ---------------------------------------------------------------------------
# GC: dry run touches nothing; real run frees only unreachable content
# ---------------------------------------------------------------------------


def test_gc_dry_run_lists_candidates_without_touching_anything(ops, lifecycle):
    v1 = ops.put_file("d.bin", data=b"old", idempotency_key="gc-dry-key-01")
    v2 = ops.put_file("d.bin", data=b"new", idempotency_key="gc-dry-key-02")

    versions_before = int(ops.catalog.db.execute("SELECT COUNT(*) FROM versions").fetchone()[0])
    report = lifecycle.gc(dry_run=True, idempotency_key="gc-dry-run-01")
    assert report["dry_run"] is True
    assert v1["version_id"] in report["candidates"]
    assert v2["version_id"] not in report["candidates"]
    assert v1["sha256"] in report["blobs_freed"]

    assert int(ops.catalog.db.execute("SELECT COUNT(*) FROM versions").fetchone()[0]) == versions_before
    assert _row(ops, v1["version_id"]) is not None
    assert ops.blobs.has_blob(v1["sha256"])
    assert ops.resolve("d.bin")["version_id"] == v2["version_id"]


def test_gc_keeps_pinned_aliased_in_collection_and_delete_requested_latest(ops, lifecycle, collections):
    root = None
    # unreachable but pinned -> kept
    p1 = ops.put_file("pin.bin", data=b"p0", idempotency_key="gc-pin-key-01")
    root = p1["root_id"]
    p2 = ops.put_file("pin.bin", data=b"p1", idempotency_key="gc-pin-key-02")
    lifecycle.pin(p1["version_id"], "hold", idempotency_key="gc-pin-hold")

    # unreachable but aliased -> kept
    a1 = ops.put_file("alias.bin", data=b"a0", idempotency_key="gc-alias-key-01")
    a2 = ops.put_file("alias.bin", data=b"a1", idempotency_key="gc-alias-key-02")
    collections.alias_set("keep", a1["root_id"], None, a1["version_id"], idempotency_key="gc-alias-pin")

    # collection member (not latest) -> kept
    c1 = ops.put_file("coll.bin", data=b"c0", idempotency_key="gc-coll-key-01")
    c2 = ops.put_file("coll.bin", data=b"c1", idempotency_key="gc-coll-key-02")
    collections.create_collection("keepers", idempotency_key="gc-coll-create")
    collections.append_version("keepers", c1["version_id"], idempotency_key="gc-coll-app-01")

    # delete-requested but still root-latest -> kept by the latest pointer
    d1 = ops.put_file("del.bin", data=b"d0", idempotency_key="gc-del-key-01")
    lifecycle.request_delete(d1["version_id"], idempotency_key="gc-del-req")

    # plain unreachable candidate
    u1 = ops.put_file("free.bin", data=b"f0", idempotency_key="gc-free-key-01")
    u2 = ops.put_file("free.bin", data=b"f1", idempotency_key="gc-free-key-02")

    dry = lifecycle.gc(dry_run=True, idempotency_key="gc-mixed-dry")
    assert dry["candidates"] == [u1["version_id"]]
    for kept in (p1, a1, c1, d1, p2, a2, c2, u2):
        assert kept["version_id"] not in dry["candidates"]

    real = lifecycle.gc(dry_run=False, idempotency_key="gc-mixed-real")
    assert real["candidates"] == [u1["version_id"]]
    for kept in (p1, a1, c1, d1, p2, a2, c2, u2):
        assert _row(ops, kept["version_id"]) is not None
    assert not ops.blobs.has_blob(u1["sha256"])
    assert u1["sha256"] in real["blobs_freed"]
    for kept in (p1, a1, c1, d1):
        assert ops.blobs.has_blob(kept["sha256"])


def test_gc_frees_blob_only_when_no_remaining_version_references_it(ops, lifecycle):
    shared = b"shared-bytes"
    s1 = ops.put_file("s1.bin", data=shared, idempotency_key="gc-share-key-01")
    s2 = ops.put_file("s2.bin", data=shared, idempotency_key="gc-share-key-02")
    assert s1["sha256"] == s2["sha256"]
    # make s1's version unreachable while s2's version still references the sha
    s1b = ops.put_file("s1.bin", data=b"replacement", idempotency_key="gc-share-key-03")

    real = lifecycle.gc(dry_run=False, idempotency_key="gc-share-real")
    assert s1["version_id"] in real["candidates"]
    assert s2["version_id"] not in real["candidates"]
    # blob survives: the surviving s2 version references it
    assert ops.blobs.has_blob(s2["sha256"])
    assert s2["sha256"] not in real["blobs_freed"]

    # now remove the last reference and gc again: blob goes away
    s3 = ops.put_file("s2.bin", data=b"s2-new", idempotency_key="gc-share-key-04")
    real2 = lifecycle.gc(dry_run=False, idempotency_key="gc-share-real-2")
    assert s2["version_id"] in real2["candidates"]
    assert s1b["version_id"] not in real2["candidates"]  # still root-latest
    assert not ops.blobs.has_blob(s2["sha256"])
    assert s2["sha256"] in real2["blobs_freed"]
    assert ops.blobs.has_blob(s3["sha256"])
    assert s3["sha256"] not in real2["blobs_freed"]


def test_publication_after_gc_still_works_and_dedup_surviving_content(ops, lifecycle):
    v1 = ops.put_file("after.bin", data=b"first", idempotency_key="gc-after-key-01")
    v2 = ops.put_file("after.bin", data=b"second", idempotency_key="gc-after-key-02")
    report = lifecycle.gc(dry_run=False, idempotency_key="gc-after-real")
    assert v1["version_id"] in report["candidates"]

    # dedup hit on surviving content still resolves to the live version row
    again = ops.put_file("after.bin", data=b"second", idempotency_key="gc-after-key-03")
    assert again["deduplicated"] is True
    assert again["version_id"] == v2["version_id"]

    fresh = ops.put_file("after.bin", data=b"third", idempotency_key="gc-after-key-04")
    assert fresh["deduplicated"] is False
    assert ops.resolve("after.bin")["version_id"] == fresh["version_id"]
    materialized = ops.materialize(fresh["version_id"], ops.catalog.home.parent / "after-out.bin")
    assert materialized["size_bytes"] == len(b"third")


# ---------------------------------------------------------------------------
# Backup / restore with epoch rotation + recovery_required gating
# ---------------------------------------------------------------------------


def test_backup_creates_manual_snapshot(home, ops, lifecycle):
    ops.put_file("b.bin", data=b"backup me", idempotency_key="bk-key-01")
    path = lifecycle.backup()
    assert path.exists()
    assert path.parent == home / "backups"
    assert "-manual.sqlite" in path.name
    import sqlite3

    conn = sqlite3.connect(path)
    try:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        count = int(conn.execute("SELECT COUNT(*) FROM versions").fetchone()[0])
    finally:
        conn.close()
    assert {"versions", "collections", "lineage", "catalog_state"} <= names
    assert count == 1


def test_begin_restore_rotates_instance_id_and_bumps_epoch(home, ops, lifecycle):
    ops.put_file("br.bin", data=b"before backup", idempotency_key="br-key-01")
    before = ops.catalog.identity()
    backup_path = lifecycle.backup()

    ops.put_file("br.bin", data=b"changed after backup", idempotency_key="br-key-02")
    after_more = ops.catalog.identity()
    assert after_more["state_epoch"] == before["state_epoch"]

    result = lifecycle.begin_restore(backup_path)
    assert result["recovery_required"] is True
    identity = ops.catalog.identity()
    assert identity["instance_id"] != before["instance_id"]
    assert identity["state_epoch"] == before["state_epoch"] + 1
    # restored content: only the pre-backup version exists
    rows = int(ops.catalog.db.execute("SELECT COUNT(*) FROM versions").fetchone()[0])
    assert rows == 1


MUTATING_CALLS = [
    (
        lambda ops, collections, lifecycle: ops.put_file(
            "locked.bin", data=b"x", idempotency_key="rec-lock-put"
        ),
        "put_file",
    ),
    (
        lambda ops, collections, lifecycle: ops.put_dir(
            _make_dir(), "locked-dir", idempotency_key="rec-lock-putdir"
        ),
        "put_dir",
    ),
    (
        lambda ops, collections, lifecycle: collections.create_collection(
            "locked-col", idempotency_key="rec-lock-col"
        ),
        "create_collection",
    ),
    (
        lambda ops, collections, lifecycle: collections.append_version(
            "any", "ver_" + "0" * 32, idempotency_key="rec-lock-app"
        ),
        "append_version",
    ),
    (
        lambda ops, collections, lifecycle: collections.alias_set(
            "a", "rot_" + "0" * 32, None, "ver_" + "0" * 32, idempotency_key="rec-lock-alias"
        ),
        "alias_set",
    ),
    (
        lambda ops, collections, lifecycle: collections.link_lineage(
            "job", "j", "job", "c", "ver_" + "0" * 32, idempotency_key="rec-lock-lin"
        ),
        "link_lineage",
    ),
    (
        lambda ops, collections, lifecycle: lifecycle.request_delete(
            "ver_" + "0" * 32, idempotency_key="rec-lock-del"
        ),
        "request_delete",
    ),
    (
        lambda ops, collections, lifecycle: lifecycle.pin(
            "ver_" + "0" * 32, "hold", idempotency_key="rec-lock-pin"
        ),
        "pin",
    ),
    (
        lambda ops, collections, lifecycle: lifecycle.unpin(
            "ver_" + "0" * 32, idempotency_key="rec-lock-unpin"
        ),
        "unpin",
    ),
    (
        lambda ops, collections, lifecycle: lifecycle.gc(dry_run=False, idempotency_key="rec-lock-gc"),
        "gc",
    ),
]


def _make_dir():
    import tempfile
    from pathlib import Path

    tmp = Path(tempfile.mkdtemp(prefix="vanth-gc-dir-"))
    (tmp / "nested").mkdir()
    (tmp / "nested" / "f.txt").write_bytes(b"dir content")
    return tmp


def test_recovery_required_blocks_all_mutations_until_complete_restore(ops, lifecycle, collections):
    ops.put_file("rec.bin", data=b"pre-restore", idempotency_key="rec-key-01")
    backup_path = lifecycle.backup()

    restore_result = lifecycle.begin_restore(backup_path)
    assert restore_result["recovery_required"] is True

    # reads keep working
    assert ops.resolve("rec.bin")["root_name"] == "rec.bin"

    for call, label in MUTATING_CALLS:
        with pytest.raises(ValueError, match="recovery_required"):
            call(ops, collections, lifecycle)

    complete = lifecycle.complete_restore()
    assert complete["recovery_required"] is False
    # mutations work again
    out = ops.put_file("post.bin", data=b"post-restore", idempotency_key="rec-post-put")
    assert out["deduplicated"] is False
    created = collections.create_collection("post-col", idempotency_key="rec-post-col")
    assert created["collection_id"].startswith("col_")


def test_begin_restore_requires_existing_backup(lifecycle, tmp_path):
    with pytest.raises(ValueError, match="backup not found"):
        lifecycle.begin_restore(tmp_path / "missing.sqlite")
