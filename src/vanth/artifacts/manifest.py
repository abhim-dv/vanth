"""Minimal file-root artifact manifest, version 0 (Phase 5).

Manifest v0 is a **file-only preview** format; it deliberately does not claim
v1. Version 1 requires canonical directory manifests and pinned Unicode
normalization/case-fold vectors (Phase 6) before it may be named.

Schema::

    {"manifest_version": 0,
     "kind": "file",
     "size_bytes": <int >= 0>,
     "sha256": "<64 lowercase hex>",
     "name": "<non-empty string>"}

Canonical form reuses the RFC 8785 serializer from
:mod:`vanth.remote.protocol`; the manifest digest is the lowercase hex
SHA-256 of the UTF-8 bytes of that canonical string.
"""

from __future__ import annotations

import hashlib
import re

from ..remote.protocol import VanthRemoteProtocolError, canonical_json

__all__ = [
    "MANIFEST_VERSION",
    "build_manifest",
    "canonical_manifest",
    "manifest_digest",
    "validate_manifest",
]

MANIFEST_VERSION = 0

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FIELDS = {"manifest_version", "kind", "size_bytes", "sha256", "name"}


def build_manifest(data: bytes, name: str) -> dict[str, object]:
    """Build a v0 file manifest for ``data`` named ``name``."""
    if isinstance(name, bool) or not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string")
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise ValueError("data must be bytes")
    return {
        "manifest_version": MANIFEST_VERSION,
        "kind": "file",
        "size_bytes": len(data),
        "sha256": hashlib.sha256(bytes(data)).hexdigest(),
        "name": name,
    }


def validate_manifest(manifest: object) -> None:
    """Validate a v0 file manifest; raise ValueError on any violation."""
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
    missing = sorted(_FIELDS - set(manifest.keys()))
    if missing:
        raise ValueError(f"manifest missing required fields: {', '.join(missing)}")
    unknown = sorted(set(manifest.keys()) - _FIELDS)
    if unknown:
        raise ValueError(f"manifest has unknown fields: {', '.join(unknown)}")
    version = manifest["manifest_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        raise ValueError("manifest_version must be an integer")
    if version != MANIFEST_VERSION:
        raise ValueError(f"unsupported manifest_version: {version} (this build supports {MANIFEST_VERSION})")
    kind = manifest["kind"]
    if not isinstance(kind, str):
        raise ValueError("kind must be a string")
    if kind != "file":
        raise ValueError(f"unsupported manifest kind: {kind!r} (v0 supports 'file' only)")
    size = manifest["size_bytes"]
    if isinstance(size, bool) or not isinstance(size, int):
        raise ValueError("size_bytes must be an integer")
    if size < 0:
        raise ValueError("size_bytes must be >= 0")
    sha256 = manifest["sha256"]
    if not isinstance(sha256, str) or not _SHA256_RE.match(sha256):
        raise ValueError("sha256 must be a 64-char lowercase hex string")
    name = manifest["name"]
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string")


def canonical_manifest(manifest: dict[str, object]) -> str:
    """RFC 8785 canonical JSON of a validated manifest."""
    validate_manifest(manifest)
    try:
        return canonical_json(manifest)
    except VanthRemoteProtocolError as exc:
        raise ValueError(str(exc)) from None


def manifest_digest(manifest: dict[str, object]) -> str:
    """Lowercase hex SHA-256 of the canonical manifest string."""
    return hashlib.sha256(canonical_manifest(manifest).encode("utf-8")).hexdigest()
