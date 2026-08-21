"""Phase 8: writer leases — exclusivity, expiry, renew, and the post-call
fence that keeps a provider call outliving a lease from double-committing."""

from __future__ import annotations

import pytest

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.s3 import DEFAULT_WRITER_LEASE_SECONDS, WriterLeases


@pytest.fixture()
def home(tmp_path):
    return tmp_path / "state"


@pytest.fixture()
def catalog(home):
    return open_catalog(home)


@pytest.fixture()
def leases(catalog):
    return WriterLeases(catalog)


def _expire(leases, lease_key, in_the_past="2000-01-01T00:00:00Z"):
    """Simulate clock passage: force a live lease into the past."""
    with leases.catalog.lock:
        leases.db.execute("BEGIN IMMEDIATE")
        try:
            leases.db.execute(
                "UPDATE writer_leases SET lease_expires_at=? WHERE lease_key=?",
                (in_the_past, lease_key),
            )
            leases.db.commit()
        except BaseException:
            leases.db.rollback()
            raise


# ---------------------------------------------------------------------------
# invariant 1: two catalog instances cannot both mutate one logical catalog
# ---------------------------------------------------------------------------


def test_exclusive_acquisition_blocks_second_instance(leases):
    instance_a = "cit_" + "a" * 32
    instance_b = "cit_" + "b" * 32
    token = leases.acquire("catalog", instance_a)
    assert token
    # B (a different catalog instance) is blocked while A holds the lease.
    assert leases.acquire("catalog", instance_b) is None
    # A releasing lets B in.
    assert leases.release("catalog", token) is True
    token_b = leases.acquire("catalog", instance_b)
    assert token_b


def test_release_is_token_fenced(leases):
    token = leases.acquire("catalog", "cit_" + "a" * 32)
    assert leases.release("catalog", "wrong-token") is False
    assert leases.holds("catalog", token) is True
    assert leases.release("catalog", token) is True
    assert leases.holds("catalog", token) is False


def test_expiry_lets_other_instance_in(leases):
    instance_a = "cit_" + "a" * 32
    instance_b = "cit_" + "b" * 32
    token = leases.acquire("catalog", instance_a)
    _expire(leases, "catalog")
    assert leases.reclaim_expired() != []
    token_b = leases.acquire("catalog", instance_b)
    assert token_b
    # A's stale token can no longer release or revalidate.
    assert leases.release("catalog", token) is False
    assert leases.holds("catalog", token) is False


def test_renew_extends_live_lease_only(leases):
    token = leases.acquire("catalog", "cit_" + "a" * 32, ttl_seconds=1)
    before = leases.inspect("catalog")
    renewed = leases.renew("catalog", token, ttl_seconds=DEFAULT_WRITER_LEASE_SECONDS)
    assert renewed and renewed["lease_expires_at"] > before["lease_expires_at"]
    _expire(leases, "catalog")
    assert leases.renew("catalog", token) is None


def test_same_owner_may_reacquire_own_lease(leases):
    instance_a = "cit_" + "a" * 32
    first = leases.acquire("catalog", instance_a)
    second = leases.acquire("catalog", instance_a)
    assert second  # owner_instance_id = me takeover path
    assert second != first
    assert not leases.holds("catalog", first)


# ---------------------------------------------------------------------------
# post-call fence: provider call outliving the lease converges without a
# duplicate catalog commit
# ---------------------------------------------------------------------------


class FakeProviderWork:
    """Stands in for a remote provider upload; the hook expires the writer
    lease after work starts but before the commit transaction begins."""

    def __init__(self, leases, lease_key, digest, *, expire_mid_flight=False):
        self.leases = leases
        self.lease_key = lease_key
        self.digest = digest
        self.expire_mid_flight = expire_mid_flight
        self.uploaded = False

    def __call__(self, claim_token):
        if self.expire_mid_flight:
            _expire(self.leases, self.lease_key)
        self.uploaded = True
        return {"manifest_digest": self.digest, "uploaded_by": claim_token}


def test_lost_lease_converts_commit_to_replay_without_new_row(catalog, leases):
    root_id = "rot_" + "1" * 32
    digest = "sha256:" + "f" * 64
    versions_before = catalog.db.execute("SELECT COUNT(*) FROM versions").fetchone()[0]

    work = FakeProviderWork(leases, f"root:{root_id}", digest, expire_mid_flight=True)

    def commit(db, result):
        db.execute(
            "INSERT INTO versions(version_id, root_id, manifest_digest, manifest_json, "
            "size_bytes, created_at) VALUES (?, ?, ?, '{}', 0, ?)",
            ("ver_fenced", root_id, result["manifest_digest"], "2026-01-01T00:00:00Z"),
        )
        return {"version_id": "ver_fenced"}

    def replay(db, result):
        row = db.execute(
            "SELECT version_id FROM versions WHERE root_id=? AND manifest_digest=?",
            (root_id, result["manifest_digest"]),
        ).fetchone()
        return {"version_id": row["version_id"]} if row else None

    outcome = leases.with_writer_lease(
        f"root:{root_id}",
        "cit_" + "a" * 32,
        work=work,
        commit=commit,
        replay=replay,
    )
    assert work.uploaded is True
    assert outcome["committed"] is False
    assert outcome["fenced"] is True
    assert outcome["replayed"] is False
    versions_after = catalog.db.execute("SELECT COUNT(*) FROM versions").fetchone()[0]
    assert versions_after == versions_before  # NO duplicate catalog commit


def test_valid_lease_commits_exactly_once(catalog, leases):
    root_id = "rot_" + "2" * 32
    digest = "sha256:" + "e" * 64
    calls = []

    def commit(db, result):
        calls.append(result)
        db.execute(
            "INSERT INTO versions(version_id, root_id, manifest_digest, manifest_json, "
            "size_bytes, created_at) VALUES (?, ?, ?, '{}', 0, ?)",
            ("ver_ok", root_id, result["manifest_digest"], "2026-01-01T00:00:00Z"),
        )
        return {"version_id": "ver_ok"}

    outcome = leases.with_writer_lease(
        f"root:{root_id}",
        "cit_" + "a" * 32,
        work=lambda token: {"manifest_digest": digest},
        commit=commit,
        replay=lambda db, result: None,
    )
    assert outcome == {
        "committed": True,
        "fenced": False,
        "replayed": False,
        "lease_key": f"root:{root_id}",
        "claim_token": outcome["claim_token"],
        "result": {"version_id": "ver_ok"},
    }
    assert len(calls) == 1
    count = catalog.db.execute(
        "SELECT COUNT(*) FROM versions WHERE root_id=?", (root_id,)
    ).fetchone()[0]
    assert count == 1


def test_replay_finds_existing_state_after_lost_lease(catalog, leases):
    """The idempotent half of the fence: a lost lease converts the outcome
    into a replay lookup, which returns the already-committed version."""
    root_id = "rot_" + "3" * 32
    digest = "sha256:" + "d" * 64
    catalog.db.execute(
        "INSERT INTO versions(version_id, root_id, manifest_digest, manifest_json, "
        "size_bytes, created_at) VALUES (?, ?, ?, '{}', 0, ?)",
        ("ver_existing", root_id, digest, "2026-01-01T00:00:00Z"),
    )
    catalog.db.commit()

    def replay(db, result):
        row = db.execute(
            "SELECT version_id FROM versions WHERE root_id=? AND manifest_digest=?",
            (root_id, result["manifest_digest"]),
        ).fetchone()
        return {"version_id": row["version_id"]} if row else None

    outcome = leases.with_writer_lease(
        f"root:{root_id}",
        "cit_" + "b" * 32,
        work=lambda token: (_expire(leases, f"root:{root_id}"), {"manifest_digest": digest})[1],
        commit=lambda db, result: {"version_id": "ver_duplicate_must_not_appear"},
        replay=replay,
        ttl_seconds=1,
    )
    assert outcome["committed"] is False
    assert outcome["fenced"] is True
    assert outcome["replayed"] is True
    assert outcome["result"] == {"version_id": "ver_existing"}
    count = catalog.db.execute(
        "SELECT COUNT(*) FROM versions WHERE root_id=?", (root_id,)
    ).fetchone()[0]
    assert count == 1  # no duplicate row from the fenced attempt


def test_lease_unavailable_raises(catalog, leases):
    holder = "cit_" + "c" * 32
    other = "cit_" + "d" * 32
    token = leases.acquire("root:X", holder)
    with pytest.raises(ValueError, match="writer lease unavailable"):
        leases.with_writer_lease(
            "root:X",
            other,
            work=lambda t: {},
            commit=lambda db, r: {},
        )
    assert leases.inspect("root:X")["owner_instance_id"] == holder


# ---------------------------------------------------------------------------
# invariant 2: two catalogs cannot claim the same writable root
# ---------------------------------------------------------------------------


def test_two_catalogs_cannot_claim_same_root(leases):
    root_id = "rot_shared"
    instance_a = "cat_" + "a" * 30
    instance_b = "cat_" + "b" * 30
    token_a = leases.acquire(f"root:{root_id}", instance_a)
    assert token_a
    # B blocked while A alive...
    assert leases.acquire(f"root:{root_id}", instance_b) is None
    # ...and still blocked after A merely RENEWS.
    assert leases.renew(f"root:{root_id}", token_a) is not None
    assert leases.acquire(f"root:{root_id}", instance_b) is None
    # ...only release/expiry frees the root.
    leases.release(f"root:{root_id}", token_a)
    assert leases.acquire(f"root:{root_id}", instance_b) is not None


def test_lease_rows_persist_schema_shape(leases, catalog):
    token = leases.acquire("catalog", "cit_" + "e" * 32, ttl_seconds=60)
    row = catalog.db.execute("SELECT * FROM writer_leases WHERE lease_key='catalog'").fetchone()
    assert set(row.keys()) == {
        "lease_key", "owner_instance_id", "claim_token",
        "lease_expires_at", "generation", "updated_at",
    }
    assert row["claim_token"] == token
    assert int(row["generation"]) >= 0
