"""Managed artifacts: content-addressed blob store, catalog, and durable operations (Phase 5)."""

from .catalog import Catalog, open_catalog
from .collections import Collections
from .lifecycle import Lifecycle
from .local_store import LocalBlobStore, OwnershipError, default_store_root
from .manifest import (
    build_manifest,
    canonical_manifest,
    manifest_digest,
    validate_manifest,
)
from .operations import ArtifactOperations

__all__ = [
    "ArtifactOperations",
    "Catalog",
    "Collections",
    "Lifecycle",
    "LocalBlobStore",
    "OwnershipError",
    "open_catalog",
    "default_store_root",
    "build_manifest",
    "canonical_manifest",
    "manifest_digest",
    "validate_manifest",
]
