"""Artifact manifests: v0 file-only preview and v1 directory format.

Manifest v0 is a **file-only preview** format (Phase 5). Manifest v1
(Phase 6) adds canonical **directory** manifests with deterministic entry
ordering, empty directories, repeated blob references, executable-bit
capture, and a pinned Unicode/portability contract. See
``docs/spec/artifact-manifest-v1.md`` for the authoritative spec and golden
vectors.

v0 schema::

    {"manifest_version": 0,
     "kind": "file",
     "size_bytes": <int >= 0>,
     "sha256": "<64 lowercase hex>",
     "name": "<non-empty string>"}

v1 schema::

    {"manifest_version": 1,
     "kind": "dir",
     "name": "<non-empty string>",
     "entries": [{"path": "<relative POSIX path>", "kind": "file"|"dir",
                  "size_bytes": <int >= 0>, "sha256": "<64 lowercase hex>",
                  "executable": <bool>}, ...]}

Canonical form reuses the RFC 8785 serializer from
:mod:`vanth.remote.protocol`; the manifest digest is the lowercase hex
SHA-256 of the UTF-8 bytes of that canonical string.
"""

from __future__ import annotations

import hashlib
import os
import re
import stat
import unicodedata
from pathlib import Path

from ..remote.protocol import VanthRemoteProtocolError, canonical_json

__all__ = [
    "EMPTY_SHA256",
    "MANIFEST_VERSION",
    "MANIFEST_VERSION_V1",
    "build_manifest",
    "build_manifest_from_tree",
    "canonical_manifest",
    "manifest_digest",
    "portability_key",
    "validate_manifest",
    "validate_manifest_v1",
]

MANIFEST_VERSION = 0
MANIFEST_VERSION_V1 = 1

# SHA-256 of zero bytes; the content digest of every empty-directory entry.
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FIELDS = {"manifest_version", "kind", "size_bytes", "sha256", "name"}
_FIELDS_V1 = {"manifest_version", "kind", "name", "entries"}
_ENTRY_FIELDS = {"path", "kind", "size_bytes", "sha256", "executable"}

# Characters illegal in any path component on Windows, plus control chars.
_WINDOWS_ILLEGAL_CHARS = set('<>:"|?*')
# Whole-component reserved device names on Windows (case-insensitive).
_WINDOWS_RESERVED = {"CON", "PRN", "AUX", "NUL"} | {
    f"{prefix}{n}" for prefix in ("COM", "LPT") for n in range(1, 10)
}


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and bool(_SHA256_RE.match(value))


def portability_key(path: str) -> str:
    """The pinned collision key for a path.

    Two distinct paths collide when they share this key: NFC normalization
    collapses Unicode-equivalent spellings (precomposed vs decomposing
    sequences) and ``str.lower()`` collapses case differences. Both matter
    because materialization targets may live on case-insensitive or
    normalizing filesystems (macOS HFS+, default Windows NTFS semantics,
    many SMB/NFS mounts). Publishing a manifest whose entries would collide
    after either transform could never be materialized faithfully there, so
    it is rejected at build AND validation time (the *portability pin*).
    """
    return unicodedata.normalize("NFC", path).lower()


def _validate_portability_collisions(paths: list[str]) -> None:
    seen: dict[str, str] = {}
    for path in paths:  # already ordered by UTF-8 bytes
        key = portability_key(path)
        other = seen.get(key)
        if other is not None:
            raise ValueError(
                f"paths {other!r} and {path!r} collide under the v1 portability pin "
                "(case-insensitive or NFC/NFD-normalization collision)"
            )
        seen[key] = path


def validate_v1_path(path: object) -> str:
    """Validate one relative POSIX-style path; returns it or raises ValueError."""
    if not isinstance(path, str) or not path:
        raise ValueError("entry path must be a non-empty string")
    if "\x00" in path:
        raise ValueError("entry path must not contain NUL")
    if "\\" in path:
        raise ValueError("entry path must use forward slashes only (backslash rejected)")
    if path.startswith("/"):
        raise ValueError("entry path must be relative (no leading '/')")
    if path.endswith("/"):
        raise ValueError("entry path must not have a trailing slash")
    components = path.split("/")
    if any(component == "" for component in components):
        raise ValueError("entry path must not contain empty components ('//')")
    for component in components:
        if component in (".", ".."):
            raise ValueError(f"entry path must not contain '.' or '..' components: {path!r}")
        for char in component:
            if ord(char) < 0x20:
                raise ValueError(f"entry path contains a control character (< 0x20): {path!r}")
            if char in _WINDOWS_ILLEGAL_CHARS:
                raise ValueError(
                    f"entry path component contains a character illegal on Windows "
                    f"({char!r}): {path!r}"
                )
        # Windows strips trailing dots/spaces before resolving, and applies
        # device-name semantics to the BASENAME before any extension: "CON.txt"
        # and "NUL." cannot materialize faithfully (review P2-13).
        stem = component.split(".", 1)[0].rstrip(" .")
        if stem.upper() in _WINDOWS_RESERVED:
            raise ValueError(
                f"entry path uses a Windows-reserved device name (before extension "
                f"and after trailing dot/space trimming): {component!r}"
            )
    return path


def _validate_entries(entries: object) -> list[dict[str, object]]:
    if not isinstance(entries, list):
        raise ValueError("entries must be an array")
    parsed: list[dict[str, object]] = []
    previous: bytes | None = None
    seen_paths: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("each entry must be an object")
        missing = sorted(_ENTRY_FIELDS - set(entry.keys()))
        if missing:
            raise ValueError(f"entry missing required fields: {', '.join(missing)}")
        unknown = sorted(set(entry.keys()) - _ENTRY_FIELDS)
        if unknown:
            raise ValueError(f"entry has unknown fields: {', '.join(unknown)}")
        path = validate_v1_path(entry["path"])
        kind = entry["kind"]
        if kind not in ("file", "dir"):
            raise ValueError(f"entry kind must be 'file' or 'dir', got {kind!r}")
        size = entry["size_bytes"]
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ValueError("entry size_bytes must be an integer >= 0")
        sha256 = entry["sha256"]
        if not _is_sha256(sha256):
            raise ValueError("entry sha256 must be a 64-char lowercase hex string")
        executable = entry["executable"]
        if not isinstance(executable, bool):
            raise ValueError("entry executable must be a boolean")
        if kind == "dir":
            # Empty directories are pure structure: zero bytes, empty-content
            # digest, no exec bit. Files always carry their real size/digest.
            if size != 0:
                raise ValueError("dir entries must have size_bytes == 0")
            if sha256 != EMPTY_SHA256:
                raise ValueError("dir entries must use the empty-content sha256")
            if executable:
                raise ValueError("dir entries must have executable=false")
        encoded = path.encode("utf-8")
        if previous is not None and encoded <= previous:
            if path in seen_paths:
                raise ValueError(f"duplicate entry path: {path!r}")
            raise ValueError("entries must be sorted by UTF-8 bytes of path")
        previous = encoded
        seen_paths.add(path)
        parsed.append(dict(entry))
    _validate_portability_collisions(sorted(seen_paths))
    return parsed


def validate_manifest_v1(manifest: object) -> None:
    """Validate a v1 directory manifest; raise ValueError on any violation."""
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
    missing = sorted(_FIELDS_V1 - set(manifest.keys()))
    if missing:
        raise ValueError(f"manifest missing required fields: {', '.join(missing)}")
    unknown = sorted(set(manifest.keys()) - _FIELDS_V1)
    if unknown:
        raise ValueError(f"manifest has unknown fields: {', '.join(unknown)}")
    version = manifest["manifest_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        raise ValueError("manifest_version must be an integer")
    if version != MANIFEST_VERSION_V1:
        raise ValueError(f"unsupported manifest_version: {version} (expected {MANIFEST_VERSION_V1})")
    kind = manifest["kind"]
    if not isinstance(kind, str) or kind != "dir":
        raise ValueError(f"unsupported manifest kind for v1: {kind!r} (must be 'dir')")
    name = manifest["name"]
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string")
    _validate_entries(manifest["entries"])


def sort_key_utf8(path: str) -> bytes:
    return path.encode("utf-8")


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
    """RFC 8785 canonical JSON of a validated v0 or v1 manifest."""
    version = manifest.get("manifest_version") if isinstance(manifest, dict) else None
    if version == MANIFEST_VERSION_V1:
        validate_manifest_v1(manifest)
    else:
        validate_manifest(manifest)
    try:
        return canonical_json(manifest)
    except VanthRemoteProtocolError as exc:
        raise ValueError(str(exc)) from None


def manifest_digest(manifest: dict[str, object]) -> str:
    """Lowercase hex SHA-256 of the canonical manifest string."""
    return hashlib.sha256(canonical_manifest(manifest).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# v1: secure capture from a source tree
# ---------------------------------------------------------------------------


def _stat_capture(path: str | os.PathLike[str]) -> os.stat_result:
    """Stat hook used by capture; tests monkeypatch this to inject mutation.

    Always uses ``follow_symlinks=False`` so symlinked/reparse intermediates
    are never followed.
    """
    return os.stat(path, follow_symlinks=False)


def _is_reparse_point(info: os.stat_result) -> bool:
    """True when the stat result describes a Windows reparse point (junction,
    mount point, symlink, ...). ``st_reparse_tag`` exists on Windows CPython;
    elsewhere it is always absent and symlinks are caught by is_symlink."""
    tag = getattr(info, "st_reparse_tag", 0)
    return bool(tag)


def _refuse_unsafe_entry(entry: os.DirEntry[str], where: str) -> None:
    if entry.is_symlink():
        raise ValueError(f"source tree contains a symlink; refused: {where}")
    info = _stat_capture(entry.path)
    if _is_reparse_point(info):
        raise ValueError(f"source tree contains a reparse point; refused: {where}")
    if stat.S_ISDIR(info.st_mode):
        return
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"source tree contains a special (non-regular) file; refused: {where}")


def _is_executable(path: str, info: os.stat_result) -> bool:
    # Best-effort executable-bit capture. On POSIX the mode's x-bits are
    # authoritative; on Windows there is no real exec bit and mode bits are
    # an artifact of attribute mapping, so the pinned default is False.
    if os.name == "nt":
        return False
    return os.access(path, os.X_OK) or bool(info.st_mode & 0o111)


def _hash_file_streaming(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest_from_tree(source_dir: str | os.PathLike[str], name: str) -> dict[str, object]:
    """Securely capture a directory tree into a canonical v1 manifest.

    Refuses symlinks, reparse points, and non-regular files anywhere in the
    tree; detects source mutation (any captured file whose ``(mtime_ns,
    size)`` changed between read and verification, or that vanished); and
    rejects trees that would collide under the portability pin. Entries are
    ordered by UTF-8 bytes of path; genuinely empty directories get explicit
    entries; parent directories of files stay implicit.
    """
    if isinstance(name, bool) or not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string")
    root = Path(source_dir)
    root_info = _stat_capture(root)
    if not stat.S_ISDIR(root_info.st_mode) or _is_reparse_point(root_info):
        raise ValueError(f"source_dir must be a real directory: {root}")

    file_records: list[dict[str, object]] = []
    empty_dirs: list[str] = []

    def walk(dir_path: Path, rel_prefix: str) -> bool:
        """Returns True when the directory contained no children at all."""
        had_children = False
        with os.scandir(dir_path) as iterator:
            children = sorted(iterator, key=lambda e: e.name.encode("utf-8"))
        for child in children:
            had_children = True
            _refuse_unsafe_entry(child, child.path)
            child_rel = f"{rel_prefix}{child.name}"
            if child.is_dir(follow_symlinks=False):
                if walk(Path(child.path), f"{child_rel}/"):
                    empty_dirs.append(child_rel)
            else:
                info = _stat_capture(child.path)
                record = {
                    "path": child_rel,
                    "kind": "file",
                    "size_bytes": int(info.st_size),
                    "sha256": _hash_file_streaming(child.path),
                    "executable": _is_executable(child.path, info),
                    "_stat": (info.st_mtime_ns, int(info.st_size)),
                }
                file_records.append(record)
        return not had_children

    # The root itself counts as content-bearing even when empty only if named
    # by the caller; an empty *root* produces zero entries, which is valid.
    walk(root, "")

    # Source-mutation rejection: every captured file must still have the same
    # (mtime_ns, size) observed during reading, and must still exist.
    for record in file_records:
        path = record.pop("_stat")  # type: ignore[arg-type]
        try:
            fresh = _stat_capture(os.path.join(str(root), str(record["path"])))
        except OSError:
            raise ValueError(
                f"source mutated during capture: {record['path']!r} vanished mid-walk"
            ) from None
        if (fresh.st_mtime_ns, int(fresh.st_size)) != path:
            raise ValueError("source mutated during capture")

    entries = [
        {k: record[k] for k in ("path", "kind", "size_bytes", "sha256", "executable")}
        for record in file_records
    ]
    entries.extend(
        {
            "path": path,
            "kind": "dir",
            "size_bytes": 0,
            "sha256": EMPTY_SHA256,
            "executable": False,
        }
        for path in empty_dirs
    )
    entries.sort(key=lambda e: str(e["path"]).encode("utf-8"))
    manifest = {"manifest_version": MANIFEST_VERSION_V1, "kind": "dir", "name": name, "entries": entries}
    validate_manifest_v1(manifest)
    return manifest
