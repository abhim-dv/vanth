"""Phase 8: storage profiles — immutable revisions, capability probing, and
the targeted missing-extra error path."""

from __future__ import annotations

import json
import sys

import pytest

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.s3 import (
    InMemoryProvider,
    ProviderError,
    StorageProfiles,
)


@pytest.fixture()
def home(tmp_path):
    return tmp_path / "state"


@pytest.fixture()
def catalog(home):
    return open_catalog(home)


@pytest.fixture()
def profiles(catalog):
    return StorageProfiles(catalog)


# ---------------------------------------------------------------------------
# immutable revisions
# ---------------------------------------------------------------------------


def test_create_makes_revision_one(profiles):
    created = profiles.create("s3", {"bucket": "bkt", "prefix": "vanth/", "region": "us-east-1"})
    assert created["profile_id"].startswith("spr_")
    assert created["revision"] == 1
    assert created["kind"] == "s3"
    assert created["config"]["bucket"] == "bkt"
    assert created["capabilities"] == {}


def test_update_inserts_new_revision_and_old_stays_queryable(profiles):
    created = profiles.create("s3", {"bucket": "old-bucket"})
    updated = profiles.update(created["profile_id"], {"bucket": "new-bucket"})
    assert updated["revision"] == 2
    assert updated["config"] == {"bucket": "new-bucket"}
    revs = profiles.revisions(created["profile_id"])
    assert [r["revision"] for r in revs] == [1, 2]
    assert revs[0]["config"] == {"bucket": "old-bucket"}  # immutable
    assert revs[1]["config"] == {"bucket": "new-bucket"}
    # get() always serves the latest revision
    assert profiles.get(created["profile_id"])["config"] == {"bucket": "new-bucket"}


def test_profile_rows_are_never_updated_in_place_on_config_change(profiles):
    created = profiles.create("s3", {"bucket": "one"})
    stamp = created["created_at"]
    profiles.update(created["profile_id"], {"bucket": "two"})
    row = profiles.catalog.db.execute(
        "SELECT * FROM storage_profiles WHERE profile_id=? AND revision=1",
        (created["profile_id"],),
    ).fetchone()
    assert json.loads(row["config_json"]) == {"bucket": "one"}
    assert row["created_at"] == stamp


def test_unknown_profile_raises(profiles):
    with pytest.raises(ValueError, match="Unknown storage profile"):
        profiles.get("spr_" + "0" * 32)
    with pytest.raises(ValueError, match="Unknown storage profile"):
        profiles.update("spr_" + "0" * 32, {"bucket": "x"})


def test_kind_and_config_validation(profiles):
    with pytest.raises(ValueError, match="unsupported storage-profile kind"):
        profiles.create("gcs", {"bucket": "x"})
    with pytest.raises(ValueError, match="bucket"):
        profiles.create("s3", {})


# ---------------------------------------------------------------------------
# capability probing
# ---------------------------------------------------------------------------


def test_probe_stores_capabilities_on_latest_revision(profiles):
    created = profiles.create("s3", {"bucket": "bkt"})
    caps = {"conditional_put": True, "versioning": True, "multipart": True}
    out = profiles.probe(
        created["profile_id"], provider=InMemoryProvider(bucket="bkt")
    )
    assert out["capabilities"] == caps
    stored = json.loads(
        profiles.catalog.db.execute(
            "SELECT capabilities_json FROM storage_profiles WHERE profile_id=? AND revision=?",
            (created["profile_id"], 1),
        ).fetchone()["capabilities_json"]
    )
    assert stored == caps
    # after an update, probe lands on the new latest revision
    profiles.update(created["profile_id"], {"bucket": "bkt2"})
    probed = profiles.probe(created["profile_id"], provider=InMemoryProvider())
    assert probed["revision"] == 2
    assert probed["capabilities"] == caps
    old_row = profiles.catalog.db.execute(
        "SELECT capabilities_json FROM storage_profiles WHERE profile_id=? AND revision=1",
        (created["profile_id"],),
    ).fetchone()
    assert json.loads(old_row["capabilities_json"]) == caps


# ---------------------------------------------------------------------------
# missing-extra error path (boto3 is an optional extra, never a hard dep)
# ---------------------------------------------------------------------------


def test_probe_without_boto3_names_the_extra(profiles, monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", None)  # simulate absent extra
    created = profiles.create("s3", {"bucket": "bkt"})
    with pytest.raises(ProviderError) as excinfo:
        profiles.probe(created["profile_id"])
    assert "vanth[artifact-s3]" in str(excinfo.value)
    assert "missing-extra" in str(excinfo.value)


def test_require_boto3_missing_extra_message(monkeypatch):
    from vanth.artifacts.s3 import _require_boto3

    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(ProviderError, match=r"vanth\[artifact-s3\]"):
        _require_boto3()


def test_schema_v3_tables_and_roots_column(catalog):
    tables = {
        row[0]
        for row in catalog.db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert {"storage_profiles", "writer_leases"} <= tables
    root_cols = {row[1] for row in catalog.db.execute("PRAGMA table_info(roots)").fetchall()}
    assert "profile_id" in root_cols
