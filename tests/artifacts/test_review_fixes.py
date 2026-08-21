"""Review-fix regressions: claim fencing (P1-8) and helper framing (P0-5)."""

import json
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
