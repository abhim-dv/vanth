"""Phase 8: InMemoryProvider conditional-op contract."""

from __future__ import annotations

import pytest

from vanth.artifacts.s3 import (
    ConditionFailed,
    InMemoryProvider,
    NoSuchKey,
    ProviderError,
    capability_probe,
)


@pytest.fixture()
def provider():
    return InMemoryProvider()


# ---------------------------------------------------------------------------
# conditional put semantics
# ---------------------------------------------------------------------------


def test_put_if_absent_create_ok(provider):
    out = provider.put_if_absent("k/a", b"one")
    assert set(out) == {"etag", "version_id"}
    assert out["etag"] and out["version_id"]


def test_put_if_absent_second_create_without_if_match_fails(provider):
    provider.put_if_absent("k/a", b"one")
    with pytest.raises(ConditionFailed):
        provider.put_if_absent("k/a", b"two")


def test_put_if_absent_correct_if_match_overwrites(provider):
    first = provider.put_if_absent("k/a", b"one")
    second = provider.put_if_absent("k/a", b"two", if_match=first["etag"])
    assert second["etag"] != first["etag"]
    data, meta = provider.get("k/a")
    assert data == b"two"
    assert meta["etag"] == second["etag"]
    assert meta["version_id"] == second["version_id"]


def test_put_if_absent_wrong_if_match_raises_condition_failed(provider):
    first = provider.put_if_absent("k/a", b"one")
    with pytest.raises(ConditionFailed):
        provider.put_if_absent("k/a", b"two", if_match="deadbeef")
    # original content untouched
    data, meta = provider.get("k/a")
    assert data == b"one"
    assert meta["etag"] == first["etag"]


def test_put_if_absent_if_match_on_missing_key_fails(provider):
    with pytest.raises(ConditionFailed):
        provider.put_if_absent("missing", b"x", if_match="whatever")


def test_etags_deterministic_and_version_ids_monotonic(provider):
    a = provider.put_if_absent("a", b"data")
    b = provider.put_if_absent("b", b"other")
    assert a["etag"] != b["etag"]
    assert int(b["version_id"].split("-")[1]) > int(a["version_id"].split("-")[1])
    overwrite = provider.put_if_absent("a", b"data2", if_match=a["etag"])
    assert int(overwrite["version_id"].split("-")[1]) > int(b["version_id"].split("-")[1])
    # determinism: an identical operation sequence on a fresh provider yields
    # identical etags/version ids
    replay = InMemoryProvider()
    r1 = replay.put_if_absent("a", b"data")
    r2 = replay.put_if_absent("b", b"other")
    assert (r1["etag"], r2["etag"]) == (a["etag"], b["etag"])
    assert (r1["version_id"], r2["version_id"]) == (a["version_id"], b["version_id"])


# ---------------------------------------------------------------------------
# get / head / delete round-trip
# ---------------------------------------------------------------------------


def test_get_head_round_trip(provider):
    put = provider.put_if_absent("x/y", b"\x00payload\xff")
    data, meta = provider.get("x/y")
    assert data == b"\x00payload\xff"
    assert meta["etag"] == put["etag"]
    assert meta["size_bytes"] == len(data)
    head = provider.head("x/y")
    assert head == meta


def test_head_missing_returns_none(provider):
    assert provider.head("nope") is None


def test_get_missing_raises_no_such_key(provider):
    with pytest.raises(NoSuchKey):
        provider.get("nope")


def test_delete_removes_then_idempotent(provider):
    provider.put_if_absent("d/1", b"bye")
    provider.delete("d/1")
    assert provider.head("d/1") is None
    provider.delete("d/1")  # idempotent on missing key


def test_delete_wrong_if_match_raises(provider):
    put = provider.put_if_absent("d/2", b"keep")
    with pytest.raises(ConditionFailed):
        provider.delete("d/2", if_match="wrong")
    provider.delete("d/2", if_match=put["etag"])
    assert provider.head("d/2") is None


def test_list_prefix(provider):
    provider.put_if_absent("p/1", b"a")
    provider.put_if_absent("p/2", b"b")
    provider.put_if_absent("q/1", b"c")
    assert provider.list_prefix("p/") == ["p/1", "p/2"]


# ---------------------------------------------------------------------------
# multipart
# ---------------------------------------------------------------------------


def test_multipart_assembles_parts_in_order(provider):
    upload_id = provider.init_multipart("big.bin")
    parts = [
        provider.upload_part("big.bin", upload_id, n, bytes([n]) * 4)
        for n in (1, 2, 3)
    ]
    out = provider.complete_multipart("big.bin", upload_id, parts)
    assert out["size_bytes"] == 12
    data, _ = provider.get("big.bin")
    assert data == bytes([1]) * 4 + bytes([2]) * 4 + bytes([3]) * 4


def test_multipart_complete_orders_by_part_number_not_list_order(provider):
    upload_id = provider.init_multipart("shuffled.bin")
    p2 = provider.upload_part("shuffled.bin", upload_id, 2, b"BB")
    p1 = provider.upload_part("shuffled.bin", upload_id, 1, b"AA")
    provider.complete_multipart("shuffled.bin", upload_id, [p2, p1])
    data, _ = provider.get("shuffled.bin")
    assert data == b"AABB"


def test_multipart_abort_discards(provider):
    upload_id = provider.init_multipart("gone.bin")
    provider.upload_part("gone.bin", upload_id, 1, b"junk")
    provider.abort_multipart("gone.bin", upload_id)
    with pytest.raises(ProviderError):
        provider.complete_multipart(
            "gone.bin", upload_id,
            [{"part_number": 1, "etag": "e"}],
        )


def test_upload_part_unknown_upload_fails(provider):
    with pytest.raises(ProviderError):
        provider.upload_part("none.bin", "mpu-nope", 1, b"x")


# ---------------------------------------------------------------------------
# capability probe
# ---------------------------------------------------------------------------


def test_capability_probe_reports_all_true_for_in_memory():
    caps = capability_probe(InMemoryProvider())
    assert caps == {"conditional_put": True, "versioning": True, "multipart": True}


def test_capability_probe_cleans_up_probe_keys():
    provider = InMemoryProvider()
    capability_probe(provider)
    assert provider.list_prefix(".vanth-probe/") == []
