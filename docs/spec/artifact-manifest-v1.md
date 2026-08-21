# Artifact Manifest v1 (directory trees)

Status: **pinned**. v1 names the canonical directory-manifest format with a
pinned Unicode/portability contract and pinned capture/materialization
security rules. The file-only preview remains manifest v0
(`artifact-manifest-v0.md`).

## Schema

A manifest is a JSON object with exactly these fields:

| Field              | Type   | Constraints                                   |
|--------------------|--------|-----------------------------------------------|
| `manifest_version` | int    | must be `1`                                    |
| `kind`             | string | must be `"dir"`                                |
| `name`             | string | non-empty display/logical name                 |
| `entries`          | array  | entry objects, ordered (see Ordering)          |

Each entry has exactly:

| Field        | Type   | Constraints                                              |
|--------------|--------|-----------------------------------------------------------|
| `path`       | string | relative POSIX path (see Path rules)                      |
| `kind`       | string | `"file"` or `"dir"`                                       |
| `size_bytes` | int    | `>= 0`; always `0` for dir entries                        |
| `sha256`     | string | 64 lowercase hex; empty-content digest for dir entries    |
| `executable` | bool   | always `false` for dir entries                            |

Empty directories are explicit entries: `{"kind": "dir", "size_bytes": 0,
"sha256": "e3b0c442...b855", "executable": false}` (the SHA-256 of zero
bytes). Files always carry their real size and content digest.

Parent-directory entries are **implicit** except when genuinely empty: a file
at `a/b/c.txt` implies dirs `a` and `a/b` without entries for them.
`validate_manifest_v1` accepts either form (explicit non-empty parents are
legal), but `build_manifest_from_tree` emits explicit entries only for empty
directories.

## Path rules

Enforced per entry by `validate_manifest_v1` / `validate_v1_path`:

- Relative, POSIX-style forward slashes only. Backslashes are rejected so a
  manifest parses identically on Windows and POSIX.
- No leading `/`, no trailing `/`, no empty components (`//`).
- No component equal to `.` or `..`.
- No NUL bytes, no control characters `< 0x20`.
- No characters illegal on Windows: `< > : " | ? *`.
- No Windows-reserved device name as a whole component, case-insensitive:
  `CON`, `PRN`, `AUX`, `NUL`, `COM1-9`, `LPT1-9`.

## Deterministic ordering

Entries are sorted by the **UTF-8 byte sequence** of `path`. Duplicate paths
are rejected. This makes the canonical form byte-stable across platforms,
locales, and filesystem enumeration orders.

## Pinned Unicode / portability behavior

- Paths are stored **byte-exact as UTF-8**; Vanth never normalizes them.
- However, to stay portable onto case-insensitive or normalizing filesystems
  (Windows NTFS semantics, macOS HFS+/APFS defaults, many SMB/NFS mounts),
  each path maps to a collision key:

  ```
  portability_key(path) = unicodedata.normalize("NFC", path).lower()
  ```

- Within one manifest, two distinct paths sharing a collision key are
  **rejected at build time AND validation time**. This covers case
  collisions (`A.txt` vs `a.txt`) and NFC/NFD collisions
  (`cafe\u0301.txt` vs `caf\u00e9.txt`). Such a manifest could never be
  materialized faithfully on those filesystems, so it is refused before it
  can be published — this pin is what unlocks calling the format v1.

## Canonicalization and digest

Canonical form reuses the RFC 8785 canonical JSON serializer from
`vanth.remote.protocol.canonical_json` (sorted keys in UTF-16 code-unit
order, minimal escapes, ES6 number formatting, no whitespace). The **manifest
digest** is the lowercase hex SHA-256 of the UTF-8 bytes of the canonical
string:

```
manifest_digest = SHA256(UTF8(canonical_json(manifest)))
```

One implementation backs v0 digests, v1 digests, and the remote request
digest.

## Secure capture (`build_manifest_from_tree`)

- Refuses symlinks anywhere in the source tree (checked via
  `os.scandir(...)` + `entry.is_symlink()`).
- Refuses Windows reparse points (junctions, mount points) via
  `st_reparse_tag` on lstat results; refuses any non-regular, non-directory
  special file (FIFOs, devices).
- Hashes every file streaming; records `(mtime_ns, size)` while reading and
  re-stats every captured file afterwards: any change or a vanished file
  raises `ValueError("source mutated during capture")` and nothing is
  published.
- Applies the portability-collision check within the tree.
- Executable bit: POSIX uses `os.access(p, os.X_OK)` / mode `& 0o111`;
  Windows has no real exec bit, so capture pins `executable=false`
  (best-effort documented default).

## Secure materialization (dir roots)

- Refuses an existing destination **always**, including `overwrite=true`:
  directory materialization never merges. `overwrite` applies to file roots
  only.
- Creates the tree under a staging sibling directory, then atomically renames
  it into place; a destination that races into existence fails the rename
  rather than merging.
- Every existing path component along the destination parent chain and every
  staging-internal component is checked with `lstat`
  (`follow_symlinks=False`) immediately before use and refused if it is a
  symlink or reparse point (defense against symlink-swap attacks between
  check and use). On Windows, junctions are caught via `st_reparse_tag`
  because `os.path.islink` does not report them.
- Each file blob is verified against its manifest sha256 while copying into
  the staging tree; a mismatch aborts and removes the staging tree.
- The executable bit is restored on POSIX where `entry.executable` is true.
- File-root materialization is unchanged: exactly one file.

## Publication boundary (`put_dir`)

Same durable-operation pattern as `put_file`: capture manifest → stage +
publish every unique blob (`publish_staged` re-hashes staged bytes against
the manifest sha256 before publication) → ONE catalog transaction inserting
the version row, moving the root pointer, and fencing the op completion. The
committed digest therefore always describes the exact transferred bytes; a
crash before that transaction leaves only discoverable staging state.

## Golden vectors

See `artifact-manifest-v1-vectors.json`. Accepted vectors list the input tree
description (or inline manifest), canonical string, and `digest_sha256`;
rejected cases document the error class. Tests assert all of these
byte-exactly.
