# Artifact Manifest v0 (file-only preview)

Status: **preview**. This format deliberately does **not** claim v1; version 1
requires canonical directory manifests and pinned Unicode
normalization/case-fold portability vectors (Phase 6) before it may be named.

## Schema

A manifest is a JSON object with exactly these fields:

| Field              | Type   | Constraints                                   |
|--------------------|--------|-----------------------------------------------|
| `manifest_version` | int    | must be `0`                                    |
| `kind`             | string | must be `"file"`                               |
| `size_bytes`       | int    | `>= 0`, byte length of the content             |
| `sha256`           | string | 64 lowercase hex chars, digest of the content  |
| `name`             | string | non-empty display/logical name                 |

Missing or unknown fields are rejected by
`vanth.artifacts.manifest.validate_manifest`.

## Canonicalization and digest

Canonical form reuses the RFC 8785 canonical JSON serializer from
`vanth.remote.protocol.canonical_json` (sorted keys in UTF-16 code-unit
order, minimal escapes, ES6 number formatting, no whitespace). The **manifest
digest** is the lowercase hex SHA-256 of the UTF-8 bytes of the canonical
string:

```
manifest_digest = SHA256(UTF8(canonical_json(manifest)))
```

The same canonicalizer backs the remote request digest, so one implementation
is authoritative for both contracts.

## Version identity and dedup semantics

- Versions are immutable rows keyed `(root_id, manifest_digest)` with a
  UNIQUE constraint: identical content published to the **same root**
  collapses onto the existing version (`deduplicated=true`).
- Different roots give separate-version semantics even for identical bytes.
- Identical bytes always deduplicate physically in the blob store
  (`blobs/<aa>/<bb>/<sha256>`), regardless of root.

## Golden vectors

See `artifact-manifest-v0-vectors.json`. Each vector lists the input
manifest, its canonical string, and `digest_sha256` (hex SHA-256 of the
canonical string's UTF-8 bytes), produced by the reference implementation
(`src/vanth/artifacts/manifest.py`) and hand-checked against known SHA-256
values where applicable.
