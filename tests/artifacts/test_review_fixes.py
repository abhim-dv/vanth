"""Review-fix regressions: claim fencing (P1-8) and helper framing (P0-5)."""

import json
import os
import sqlite3
from pathlib import Path

import pytest

from vanth.artifacts.operations import ArtifactOperations
from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.local_store import LocalBlobStore


def make_ops(tmp_path):
    catalog = open_catalog(tmp_path)
    blobs = LocalBlobStore(tmp_path / "artifacts-store", catalog)
    return ArtifactOperations(catalog, blobs)


def test_concurrent_retry_cannot_steal_live_claim(tmp_path):
    """Review P1-8: a second caller claiming a running op with a live lease
    must fail loudly instead of fencing out the original worker."""
    ops = make_ops(tmp_path)
    op, _ = ops._begin("artifact.put_file", {"name": "x"}, "key-claim-0001")
    first = ops._claim(op["op_id"])
    assert first["status"] == "running"
    with pytest.raises(ValueError, match="cannot be claimed"):
        ops._claim(op["op_id"])
    # Original worker still owns the fence: completion succeeds.
    ops.complete(op["op_id"], first["claim_token"], {"ok": True})


def test_expired_lease_can_be_reclaimed_with_new_generation(tmp_path):
    ops = make_ops(tmp_path)
    op, _ = ops._begin("artifact.put_file", {"name": "y"}, "key-claim-0002")
    first = ops._claim(op["op_id"])
    gen = int(first["lease_generation"])
    # Expire the lease behind the original worker's back.
    ops.catalog.db.execute(
        "UPDATE operations SET lease_expires_at='2000-01-01T00:00:00Z' WHERE op_id=?",
        (op["op_id"],),
    )
    ops.catalog.db.commit()
    second = ops._claim(op["op_id"])
    assert int(second["lease_generation"]) == gen + 1
    assert second["claim_token"] != first["claim_token"]
    # Old token can no longer commit.
    with pytest.raises(ValueError, match="stale worker"):
        ops.complete(op["op_id"], first["claim_token"], {})


# --- Sol review follow-ups -------------------------------------------------

def test_materialize_refuses_symlink_ancestor_without_creating_through_it(tmp_path):
    """Sol review: the parent sweep must run BEFORE mkdir — creating
    directories through a symlink ancestor that the sweep then rejects is
    exactly the hazard the sweep exists to prevent."""
    real = tmp_path / "real"
    link = tmp_path / "linkdir"
    real.mkdir()
    if os.name == "nt":
        pytest.skip("symlink creation requires privileges")
    os.symlink(real, link)
    ops = make_ops(tmp_path)
    put = ops.put_file("m.bin", data=b"hello", idempotency_key="key-symlink-01")
    dest = link / "sub" / "child.bin"
    with pytest.raises(ValueError, match="symlink|reparse"):
        ops.materialize(put["version_id"], dest, overwrite=True, idempotency_key="key-symlink-02")
    # Nothing was created THROUGH the symlink.
    assert not (real / "sub").exists()


def test_portable_rename_noreplace(tmp_path):
    """Sol review: the non-renameat2 fallback publishes atomically without
    replacing an existing target (macOS/BSD path, exercised directly)."""
    ops = make_ops(tmp_path)
    src = tmp_path / "staged.bin"
    src.write_bytes(b"payload")
    dst = tmp_path / "out.bin"

    ops._rename_noreplace_portable(str(src), str(dst))
    assert dst.read_bytes() == b"payload"
    assert not src.exists()

    src2 = tmp_path / "staged2.bin"
    src2.write_bytes(b"other")
    with pytest.raises(FileExistsError):
        ops._rename_noreplace_portable(str(src2), str(dst))
    assert dst.read_bytes() == b"payload"
    src2.unlink()

    # Directories take the checked-rename branch (link(2) refuses dirs).
    if os.name == "nt":
        pytest.skip("directory rename fallback is POSIX-only; Windows never reaches it")
    src_dir = tmp_path / "src-dir"
    src_dir.mkdir()
    dst_dir = tmp_path / "out-dir"
    ops._rename_noreplace_portable(str(src_dir), str(dst_dir))
    assert dst_dir.is_dir()


def test_restore_temp_db_name_is_collision_free(tmp_path):
    """Sol review: concurrent restores in one process must not share a PID-only temp name."""
    import re
    from vanth.artifacts import lifecycle as lifecycle_mod

    ops = make_ops(tmp_path)
    lc = lifecycle_mod.Lifecycle(ops.catalog, ops.blobs)
    backup = tmp_path / "backup.sqlite"
    backup.write_bytes(b"x")
    captured = {}

    real_copyfile = lifecycle_mod.shutil.copyfile

    def spy_copy(src, dst, *a, **kw):
        captured["name"] = Path(dst).name
        return real_copyfile(src, dst, *a, **kw)

    lifecycle_mod.shutil.copyfile = spy_copy
    try:
        with pytest.raises(Exception):
            lc.begin_restore(str(backup), home=tmp_path / "home")
    finally:
        lifecycle_mod.shutil.copyfile = real_copyfile
    name = captured.get("name", "")
    assert re.match(r"restore-prepared-\d+-[0-9a-f]{12}\.sqlite", name), name
