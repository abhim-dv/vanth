"""Phase 7: collections, alias compare-and-swap, and lineage."""

from __future__ import annotations

import pytest

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.collections import Collections
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
def collections(ops) -> Collections:
    return Collections(ops.catalog, ops)


def _version(ops, name, data, key):
    return ops.put_file(name, data=data, idempotency_key=key)


# ---------------------------------------------------------------------------
# create / append monotonic ordinals
# ---------------------------------------------------------------------------


def test_create_collection_and_append_monotonic_ordinals(ops, collections):
    created = collections.create_collection("nightlies", idempotency_key="col-create-001")
    assert created["collection_id"].startswith("col_")
    v1 = _version(ops, "m.bin", b"one", "col-a-key-01")
    v2 = _version(ops, "m.bin", b"two", "col-a-key-02")
    v3 = _version(ops, "m.bin", b"three", "col-a-key-03")
    r1 = collections.append_version("nightlies", v1["version_id"], idempotency_key="col-app-01")
    r2 = collections.append_version("nightlies", v2["version_id"], idempotency_key="col-app-02")
    r3 = collections.append_version("nightlies", v3["version_id"], idempotency_key="col-app-03")
    assert (r1["ordinal"], r2["ordinal"], r3["ordinal"]) == (1, 2, 3)

    state = collections.get_collection("nightlies")
    assert [v["version_id"] for v in state["versions"]] == [
        v1["version_id"], v2["version_id"], v3["version_id"],
    ]
    assert [v["ordinal"] for v in state["versions"]] == [1, 2, 3]
    # lookup by collection_id works too
    by_id = collections.get_collection(created["collection_id"])
    assert by_id["collection_id"] == created["collection_id"]
    assert len(by_id["versions"]) == 3


def test_duplicate_append_is_no_op(ops, collections):
    collections.create_collection("dupes", idempotency_key="col-dup-create")
    v1 = _version(ops, "d.bin", b"once", "col-dup-key-01")
    first = collections.append_version("dupes", v1["version_id"], idempotency_key="col-dup-app-1")
    second = collections.append_version("dupes", v1["version_id"], idempotency_key="col-dup-app-2")
    assert first["appended"] is True
    assert second["appended"] is False
    assert second["ordinal"] == first["ordinal"] == 1
    rows = ops.catalog.db.execute(
        "SELECT COUNT(*) FROM collection_versions WHERE collection_id=?",
        (first["collection_id"],),
    ).fetchone()[0]
    assert rows == 1


def test_append_unknown_collection_or_version_fails(ops, collections):
    v1 = _version(ops, "x.bin", b"x", "col-x-key-01")
    with pytest.raises(ValueError, match="Unknown collection"):
        collections.append_version("nope", v1["version_id"])
    collections.create_collection("real", idempotency_key="col-x-create")
    with pytest.raises(ValueError, match="Unknown version_id"):
        collections.append_version("real", "ver_" + "0" * 32)


def test_create_duplicate_name_refused(ops, collections):
    collections.create_collection("solo", idempotency_key="col-solo-1")
    with pytest.raises(ValueError, match="already exists"):
        collections.create_collection("solo", idempotency_key="col-solo-2")


# ---------------------------------------------------------------------------
# Alias compare-and-swap
# ---------------------------------------------------------------------------


def test_alias_set_cas_move_success_and_mismatch_failure(ops, collections):
    root = _version(ops, "a.bin", b"v1", "alias-cas-key-01")["root_id"]
    v1 = _version(ops, "a.bin", b"v1", "alias-cas-key-01")
    v2 = _version(ops, "a.bin", b"v2", "alias-cas-key-02")

    created = collections.alias_set(
        "stable", root, None, v1["version_id"], idempotency_key="alias-cas-01"
    )
    assert created["created"] is True
    assert created["previous_version_id"] is None

    # create-if-absent again must fail now that the alias exists
    with pytest.raises(ValueError, match="ALIAS_CAS_MISMATCH"):
        collections.alias_set("stable", root, None, v2["version_id"], idempotency_key="alias-cas-02")

    # wrong expectation refuses to move and leaves the pin untouched
    with pytest.raises(ValueError, match="ALIAS_CAS_MISMATCH"):
        collections.alias_set(
            "stable", root, v2["version_id"], v2["version_id"], idempotency_key="alias-cas-03"
        )
    resolved = ops.resolve("a.bin", alias="stable")
    assert resolved["version_id"] == v1["version_id"]

    moved = collections.alias_set(
        "stable", root, v1["version_id"], v2["version_id"], idempotency_key="alias-cas-04"
    )
    assert moved["created"] is False
    assert moved["previous_version_id"] == v1["version_id"]
    resolved = ops.resolve("a.bin", alias="stable")
    assert resolved["version_id"] == v2["version_id"]

    row = ops.catalog.db.execute("SELECT * FROM aliases WHERE alias_name='stable'").fetchone()
    assert row["pinned_at"] is not None


def test_destructive_delete_of_aliased_version_rejected_until_alias_removed(ops, collections):
    root = _version(ops, "prot.bin", b"p0", "del-key-00")["root_id"]
    v1 = _version(ops, "prot.bin", b"p0", "del-key-00")
    v2 = _version(ops, "prot.bin", b"p1", "del-key-01")
    collections.alias_set("pin-alias", root, None, v1["version_id"], idempotency_key="del-alias-01")
    with pytest.raises(ValueError, match="alias"):
        lifecycle_request_delete(collections.ops, v1["version_id"])

    # move the alias away, then the delete request goes through
    collections.alias_set(
        "pin-alias", root, v1["version_id"], v2["version_id"], idempotency_key="del-alias-02"
    )
    from vanth.artifacts.lifecycle import Lifecycle

    lifecycle = Lifecycle(ops.catalog, ops)
    result = lifecycle.request_delete(v1["version_id"], idempotency_key="del-key-02")
    assert result["delete_requested"] is True
    row = ops.catalog.db.execute(
        "SELECT delete_requested_at FROM versions WHERE version_id=?", (v1["version_id"],)
    ).fetchone()
    assert row["delete_requested_at"] is not None


def lifecycle_request_delete(ops, version_id):
    from vanth.artifacts.lifecycle import Lifecycle

    Lifecycle(ops.catalog, ops).request_delete(version_id)


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


def test_link_lineage_and_lookup(ops, collections):
    v1 = _version(ops, "lin.bin", b"linked", "lin-key-01")
    job_id = "job_abc123"
    link = collections.link_lineage(
        "job", job_id, "version", v1["version_id"], v1["version_id"], idempotency_key="lin-link-01"
    )
    assert link["lin_id"].startswith("lin_")
    assert link["deduplicated"] is False
    dup = collections.link_lineage(
        "job", job_id, "version", v1["version_id"], v1["version_id"], idempotency_key="lin-link-02"
    )
    assert dup["deduplicated"] is True
    assert dup["lin_id"] == link["lin_id"]

    links = collections.lineage_for(v1["version_id"])
    assert len(links) == 1
    assert links[0]["producer_kind"] == "job"
    assert links[0]["producer_id"] == job_id
    assert links[0]["consumer_kind"] == "version"

    remote_link = collections.link_lineage(
        "remote_job", "rjob_1", "job", job_id, v1["version_id"], idempotency_key="lin-link-03"
    )
    assert len(collections.lineage_for(v1["version_id"])) == 2
    assert remote_link["producer_kind"] == "remote_job"


def test_link_lineage_validates_kinds_and_versions(ops, collections):
    v1 = _version(ops, "link2.bin", b"l2", "lin-key-02")
    with pytest.raises(ValueError, match="producer_kind"):
        collections.link_lineage("robot", "x", "job", "y", v1["version_id"])
    with pytest.raises(ValueError, match="Unknown version_id"):
        collections.link_lineage("job", "j", "alias", "a", "ver_" + "f" * 32)


def test_lineage_rows_removed_with_gc_of_their_version(ops, collections):
    from vanth.artifacts.lifecycle import Lifecycle

    lifecycle = Lifecycle(ops.catalog, ops)
    v1 = _version(ops, "gcl.bin", b"g1", "gcl-key-01")
    v2 = _version(ops, "gcl.bin", b"g2", "gcl-key-02")
    collections.link_lineage("job", "j1", "version", v1["version_id"], v1["version_id"])
    report = lifecycle.gc(dry_run=False, idempotency_key="gc-lin-run-01")
    assert v1["version_id"] in report["candidates"]
    assert collections.lineage_for(v1["version_id"]) == []
    assert len(collections.lineage_for(v2["version_id"])) == 0
