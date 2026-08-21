"""Manifest v0 build/canonicalize/digest/validate plus golden vectors (Phase 5)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from vanth.artifacts.manifest import (
    MANIFEST_VERSION,
    build_manifest,
    canonical_manifest,
    manifest_digest,
    validate_manifest,
)

VECTORS_PATH = Path(__file__).resolve().parents[2] / "docs" / "spec" / "artifact-manifest-v0-vectors.json"


def test_build_manifest_fields():
    data = b"hello world\n"
    manifest = build_manifest(data, "hello.txt")
    assert manifest == {
        "manifest_version": 0,
        "kind": "file",
        "size_bytes": 12,
        "sha256": hashlib.sha256(data).hexdigest(),
        "name": "hello.txt",
    }
    validate_manifest(manifest)


def test_canonical_form_is_sorted_and_compact():
    manifest = build_manifest(b"x", "a.bin")
    canonical = canonical_manifest(manifest)
    assert canonical == (
        '{"kind":"file","manifest_version":0,"name":"a.bin",'
        f'"sha256":"{manifest["sha256"]}","size_bytes":1}}'
    )
    assert " " not in canonical


def test_digest_is_sha256_of_canonical():
    manifest = build_manifest(b"y", "b.bin")
    expected = hashlib.sha256(canonical_manifest(manifest).encode("utf-8")).hexdigest()
    assert manifest_digest(manifest) == expected
    # Key insertion order must not matter.
    reordered = {k: manifest[k] for k in reversed(list(manifest.keys()))}
    assert manifest_digest(reordered) == expected


def test_golden_vectors_byte_exact():
    vectors = json.loads(VECTORS_PATH.read_text(encoding="utf-8"))["vectors"]
    assert len(vectors) >= 3
    for vector in vectors:
        manifest = vector["manifest"]
        validate_manifest(manifest)
        assert canonical_manifest(manifest) == vector["canonical"]
        assert manifest_digest(manifest) == vector["digest_sha256"]
        content_sha = manifest["sha256"]
        assert len(content_sha) == 64


def test_rejects_bad_kind():
    manifest = build_manifest(b"x", "a.bin")
    bad = dict(manifest, kind="directory")
    with pytest.raises(ValueError, match="kind"):
        validate_manifest(bad)
    with pytest.raises(ValueError):
        manifest_digest(bad)


def test_rejects_bad_version():
    manifest = build_manifest(b"x", "a.bin")
    with pytest.raises(ValueError, match="manifest_version"):
        validate_manifest(dict(manifest, manifest_version=MANIFEST_VERSION + 1))
    with pytest.raises(ValueError, match="manifest_version"):
        validate_manifest(dict(manifest, manifest_version="0"))


def test_rejects_bad_sha_format():
    manifest = build_manifest(b"x", "a.bin")
    for bad in ("XYZ", manifest["sha256"].upper(), manifest["sha256"][:-1], 123):
        with pytest.raises(ValueError, match="sha256"):
            validate_manifest(dict(manifest, sha256=bad))


def test_rejects_missing_unknown_fields_and_bad_size_name():
    manifest = build_manifest(b"x", "a.bin")
    missing = {k: v for k, v in manifest.items() if k != "sha256"}
    with pytest.raises(ValueError, match="missing required fields"):
        validate_manifest(missing)
    with pytest.raises(ValueError, match="unknown fields"):
        validate_manifest(dict(manifest, extra=1))
    with pytest.raises(ValueError, match="size_bytes"):
        validate_manifest(dict(manifest, size_bytes=-1))
    with pytest.raises(ValueError, match="name"):
        validate_manifest(dict(manifest, name=""))
    with pytest.raises(ValueError):
        validate_manifest("not a dict")


def test_build_rejects_bad_inputs():
    with pytest.raises(ValueError):
        build_manifest(b"x", "")
    with pytest.raises(ValueError):
        build_manifest("not bytes", "a.bin")  # type: ignore[arg-type]
