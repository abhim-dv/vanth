"""Content-addressed local blob store with atomic publication (Phase 5).

Layout under the store root::

    vanth-artifacts-owner.json   ownership marker {catalog_id, instance_id, created_at}
    staging/<uuid>.tmp           same-filesystem staging area (crash-visible)
    blobs/<aa>/<bb>/<sha256>     content-addressed, dedup by construction

All writes land in ``staging`` first and move into place with a single
``os.replace`` on the same filesystem, so a blob path never holds partial
content. The root is refused when its marker belongs to a different catalog
instance (e.g. after a backup restore regenerated ``instance_id``).
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from pathlib import Path

from .catalog import Catalog, now_iso

__all__ = ["LocalBlobStore", "OwnershipError", "default_store_root", "OWNER_MARKER_NAME"]

OWNER_MARKER_NAME = "vanth-artifacts-owner.json"


class OwnershipError(ValueError):
    """The store root's ownership marker belongs to a different catalog instance."""


def default_store_root(home: str | Path) -> Path:
    return Path(home) / "artifacts-store"


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class LocalBlobStore:
    def __init__(self, root: str | Path, catalog: Catalog) -> None:
        self.root = Path(root)
        self.staging_dir = self.root / "staging"
        self.blobs_dir = self.root / "blobs"
        for directory in (self.root, self.staging_dir, self.blobs_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self.catalog = catalog
        self._claim_ownership()

    # -- publication / GC fence --------------------------------------------

    class _Fence:
        """OS-level exclusion between blob publication and GC deletion.

        Both windows hold the same O_CREAT|O_EXCL lock file under the store
        root: publication holds it from the first ``os.replace`` into blobs
        until its catalog commit; GC holds it across unlink. This closes the
        race where GC deleted a blob a concurrent publisher had just made
        reachable (review P1-9). A stale fence older than ``stale_seconds``
        is broken once (crashed holder)."""

        def __init__(self, path: Path, *, timeout: float = 30.0, stale_seconds: float = 900.0):
            self.path = Path(path)
            self.timeout = timeout
            self.stale_seconds = stale_seconds
            self._fd = None

        def __enter__(self) -> "_Fence":
            import time as _time

            deadline = _time.monotonic() + self.timeout
            while True:
                try:
                    self._fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    os.write(self._fd, str(os.getpid()).encode())
                    return self
                except FileExistsError:
                    try:
                        age = _time.time() - self.path.stat().st_mtime
                        if age > self.stale_seconds:
                            self.path.unlink(missing_ok=True)
                            continue
                    except OSError:
                        pass
                    if _time.monotonic() > deadline:
                        raise TimeoutError(f"artifact gc fence held too long: {self.path}")
                    _time.sleep(0.05)

        def __exit__(self, *exc) -> None:
            if self._fd is not None:
                try:
                    os.close(self._fd)
                finally:
                    self.path.unlink(missing_ok=True)
                self._fd = None

    def gc_fence(self) -> "LocalBlobStore._Fence":
        return LocalBlobStore._Fence(self.root / ".vanth-gc-fence.lock")

    # -- ownership ---------------------------------------------------------

    def _claim_ownership(self) -> None:
        marker = self.root / OWNER_MARKER_NAME
        identity = self.catalog.identity()
        if marker.exists():
            try:
                data = json.loads(marker.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise OwnershipError(f"artifact store owner marker is unreadable: {exc}") from None
            if (
                data.get("catalog_id") != identity["catalog_id"]
                or data.get("instance_id") != identity["instance_id"]
            ):
                raise OwnershipError(
                    "artifact store belongs to a different catalog instance "
                    f"(marker catalog_id={data.get('catalog_id')!r}, instance_id={data.get('instance_id')!r}; "
                    f"this catalog has catalog_id={identity['catalog_id']!r}, instance_id={identity['instance_id']!r})"
                )
            return
        payload = {
            "catalog_id": identity["catalog_id"],
            "instance_id": identity["instance_id"],
            "created_at": now_iso(),
        }
        tmp = self.root / (OWNER_MARKER_NAME + ".tmp")
        tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp, marker)

    # -- staging + publication ----------------------------------------------

    def stage(self, source: bytes | bytearray | memoryview | str | os.PathLike[str]) -> Path:
        """Copy content into staging (same filesystem); returns the staged path."""
        staged = self.staging_dir / (uuid.uuid4().hex + ".tmp")
        if isinstance(source, (bytes, bytearray, memoryview)):
            staged.write_bytes(bytes(source))
        else:
            shutil.copyfile(source, staged)
        return staged

    def publish_staged(self, staging_path: str | os.PathLike[str], expected_sha256: str) -> dict[str, object]:
        """Verify the staged bytes then atomically rename them into the blob layout.

        Returns ``{"sha256", "size_bytes", "blob_path"}``. Identical bytes
        deduplicate: an existing blob at the target path wins and the staged
        copy is discarded.
        """
        if not _is_sha256(expected_sha256):
            raise ValueError("expected_sha256 must be a 64-char lowercase hex string")
        staged = Path(staging_path)
        actual = _hash_file(staged)
        if actual != expected_sha256:
            staged.unlink(missing_ok=True)
            raise ValueError(f"staged content hash mismatch: expected {expected_sha256}, got {actual}")
        size = staged.stat().st_size
        target = self.blob_path(expected_sha256)
        if target.exists():
            staged.unlink(missing_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, target)
        return {"sha256": expected_sha256, "size_bytes": size, "blob_path": target}

    # -- blob access ---------------------------------------------------------

    def blob_path(self, sha256: str) -> Path:
        if not _is_sha256(sha256):
            raise ValueError("sha256 must be a 64-char lowercase hex string")
        return self.blobs_dir / sha256[:2] / sha256[2:4] / sha256

    def has_blob(self, sha256: str) -> bool:
        try:
            return self.blob_path(sha256).is_file()
        except ValueError:
            return False

    def read_blob(self, sha256: str) -> bytes:
        return self.blob_path(sha256).read_bytes()

    def verify_blob(self, sha256: str) -> bool:
        """Re-hash the stored blob; detects changed or truncated content."""
        path = self.blob_path(sha256)
        if not path.is_file():
            return False
        return _hash_file(path) == sha256

    def list_staging(self) -> list[Path]:
        """Discoverable staging state (crash recovery visibility)."""
        if not self.staging_dir.is_dir():
            return []
        return sorted(p for p in self.staging_dir.iterdir() if p.is_file())
