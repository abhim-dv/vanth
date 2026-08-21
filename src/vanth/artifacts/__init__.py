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
from .s3 import (
    Boto3Provider,
    ConditionFailed,
    InMemoryProvider,
    NoSuchKey,
    ProviderError,
    S3Provider,
    StorageProfiles,
    WriterLeases,
)

__all__ = [
    "ArtifactOperations",
    "Boto3Provider",
    "Catalog",
    "Collections",
    "ConditionFailed",
    "InMemoryProvider",
    "Lifecycle",
    "LocalBlobStore",
    "NoSuchKey",
    "OwnershipError",
    "ProviderError",
    "S3Provider",
    "StorageProfiles",
    "WriterLeases",
    "open_catalog",
    "default_store_root",
    "build_manifest",
    "canonical_manifest",
    "manifest_digest",
    "validate_manifest",
]
