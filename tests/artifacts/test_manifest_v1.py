"""Manifest v1: build from tree, deterministic ordering, validator, vectors (Phase 6)."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

from vanth.artifacts.manifest import (
    EMPTY_SHA256,
    build_manifest_from_tree,
    canonical_manifest,
    manifest_digest,
    validate_manifest_v1,
)

VECTORS_PATH = Path(__file__).resolve().parents[2] / "docs" / "spec" / "artifact-manifest-v1-vectors.json"

CHMOD_SUPPORTS_EXEC = os.name != "nt"


def make_tree(base: Path) -> Path:
    root = base / "src"
    (root / "a" / "b").mkdir(parents=True)
    (root / "a" / "b" / "hello.txt").write_bytes(b"hello world\n")
    (root / "dup1.bin").write_bytes(b"same")
    (root / "dup2.bin").write_bytes(b"same")
    (root / "empty").mkdir()
    return root


# ---------------------------------------------------------------------------
# Build from tree
# ---------------------------------------------------------------------------


def test_build_from_tree_deterministic_order_empty_dirs_repeated_blob(tmp_path):
    root = make_tree(tmp_path)
    manifest = build_manifest_from_tree(root, "tree-one")
    validate_manifest_v1(manifest)
    paths = [entry["path"] for entry in manifest["entries"]]
    assert paths == sorted(paths, key=lambda p: p.encode("utf-8"))
    assert paths == ["a/b/hello.txt", "dup1.bin", "dup2.bin", "empty"]
    # Empty dir is explicit with the empty-content digest.
    empty_entry = manifest["entries"][-1]
    assert empty_entry["kind"] == "dir"
    assert empty_entry["size_bytes"] == 0
    assert empty_entry["sha256"] == EMPTY_SHA256
    assert empty_entry["executable"] is False
    # Repeated blob references are allowed.
    shas = {e["sha256"] for e in manifest["entries"] if e["kind"] == "file"}
    dup_shas = [e["sha256"] for e in manifest["entries"] if e["path"].startswith("dup")]
    assert len(dup_shas) == 2 and len(shas) < 3


def test_build_is_deterministic_across_calls(tmp_path):
    root = make_tree(tmp_path)
    first = build_manifest_from_tree(root, "t")
    second = build_manifest_from_tree(root, "t")
    assert manifest_digest(first) == manifest_digest(second)
    # Reordering entries must not change the digest (canonicalization).
    shuffled = dict(first, entries=list(reversed(first["entries"])))
    with pytest.raises(ValueError):
        # Unsorted entries are invalid, so digest must refuse them too.
        manifest_digest(shuffled)


def test_executable_bit_captured(tmp_path):
    if not CHMOD_SUPPORTS_EXEC:
        pytest.skip("filesystem does not support the POSIX executable bit")
    root = tmp_path / "src"
    root.mkdir()
    script = root / "run.sh"
    script.write_bytes(b"#!/bin/sh\necho hi\n")
    os.chmod(script, 0o755)
    plain = root / "plain.txt"
    plain.write_bytes(b"plain")
    manifest = build_manifest_from_tree(root, "exec")
    by_path = {e["path"]: e for e in manifest["entries"]}
    assert by_path["run.sh"]["executable"] is True
    assert by_path["plain.txt"]["executable"] is False


def test_empty_root_tree_and_missing_source(tmp_path):
    root = tmp_path / "src"
    root.mkdir()
    manifest = build_manifest_from_tree(root, "bare")
    assert manifest["entries"] == []
    validate_manifest_v1(manifest)
    with pytest.raises(OSError):
        build_manifest_from_tree(tmp_path / "nope", "ghost")


# ---------------------------------------------------------------------------
# Validator rejections
# ---------------------------------------------------------------------------


def good_manifest(entries) -> dict:
    return {"manifest_version": 1, "kind": "dir", "name": "x",
            "entries": sorted(entries, key=lambda e: e["path"].encode("utf-8"))}


def file_entry(path, sha=None, size=1):
    return {"path": path, "kind": "file", "size_bytes": size,
            "sha256": sha or hashlib.sha256(b"x").hexdigest(), "executable": False}


@pytest.mark.parametrize("path", [
    "a/../b.txt",          # traversal component
    "a\\b.txt",            # backslash
    "/abs/path.txt",       # absolute
    "trailing/",           # trailing slash
    "a//b.txt",            # empty component
    "./here.txt",          # dot component
    "con.txt"[:3],         # reserved device name CON as whole component
    "COM1",                # reserved device name
    "lpt9",                # reserved device name, case-insensitive
    "bad\x01name.txt",     # control char
    "illegal<char>.txt",   # Windows-illegal character
])
def test_validator_rejects_bad_paths(path):
    with pytest.raises(ValueError):
        validate_manifest_v1(good_manifest([file_entry(path)]))


def test_validator_rejects_duplicate_paths():
    entry = file_entry("same.txt")
    with pytest.raises(ValueError, match="duplicate"):
        validate_manifest_v1(good_manifest([entry, dict(entry)]))


def test_validator_rejects_case_collision():
    with pytest.raises(ValueError, match="portability"):
        validate_manifest_v1(good_manifest([
            file_entry("A.txt"),
            file_entry("a.txt"),
        ]))


def test_validator_rejects_nfc_nfd_collision():
    nfc = "caf\u00e9.txt"
    nfd = "cafe\u0301.txt"
    assert nfc != nfd
    with pytest.raises(ValueError, match="portability"):
        validate_manifest_v1(good_manifest([file_entry(nfc), file_entry(nfd)]))


def test_validator_accepts_distinct_unicode_paths():
    # Distinct NFC-normalized, lowercase-distinct names are fine.
    validate_manifest_v1(good_manifest([
        file_entry("caf\u00e9.txt"),
        file_entry("cafe.txt"),
        file_entry("CAF\u00c9.txt".lower().replace("\u00e9", "\u00ea")),  # caf\xea.txt
    ]))


def test_validator_rejects_bad_dir_entries():
    bad_size = {"path": "d", "kind": "dir", "size_bytes": 5, "sha256": EMPTY_SHA256, "executable": False}
    with pytest.raises(ValueError, match="size_bytes"):
        validate_manifest_v1(good_manifest([bad_size]))
    bad_sha = {"path": "d", "kind": "dir", "size_bytes": 0, "sha256": "0" * 64, "executable": False}
    with pytest.raises(ValueError, match="sha256"):
        validate_manifest_v1(good_manifest([bad_sha]))
    bad_exec = {"path": "d", "kind": "dir", "size_bytes": 0, "sha256": EMPTY_SHA256, "executable": True}
    with pytest.raises(ValueError, match="executable"):
        validate_manifest_v1(good_manifest([bad_exec]))


def test_validator_rejects_unsorted_entries_bad_version_kind_and_fields():
    # Constructed directly (no sorting helper) to keep the entries unsorted.
    m = {"manifest_version": 1, "kind": "dir", "name": "x",
         "entries": [file_entry("b.txt"), file_entry("a.txt")]}
    with pytest.raises(ValueError, match="sorted"):
        validate_manifest_v1(m)
    with pytest.raises(ValueError, match="manifest_version"):
        validate_manifest_v1(good_manifest([file_entry("a.txt")]) | {"manifest_version": 0})
    with pytest.raises(ValueError, match="kind"):
        validate_manifest_v1(good_manifest([file_entry("a.txt")]) | {"kind": "file"})
    extra = good_manifest([file_entry("a.txt")]) | {"extra": 1}
    with pytest.raises(ValueError, match="unknown fields"):
        validate_manifest_v1(extra)


# ---------------------------------------------------------------------------
# Golden vectors (byte-exact from docs/spec)
# ---------------------------------------------------------------------------


def test_golden_vectors_byte_exact():
    doc = json.loads(VECTORS_PATH.read_text(encoding="utf-8"))
    for vector in doc["vectors"]:
        manifest = vector["manifest"]
        validate_manifest_v1(manifest)
        assert canonical_manifest(manifest) == vector["canonical"]
        assert manifest_digest(manifest) == vector["digest_sha256"]


def test_golden_rejected_cases_are_rejected():
    doc = json.loads(VECTORS_PATH.read_text(encoding="utf-8"))
    assert len(doc["vectors"]) >= 3
    for case in doc["rejected_cases"]:
        with pytest.raises(ValueError, match=case["error_contains"]):
            validate_manifest_v1(case["input"])
