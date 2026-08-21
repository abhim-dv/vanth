"""S3 storage: provider abstraction with conditional operations, writer
leases, and immutable storage-profile revisions (Phase 8).

Providers expose EXACTLY the conditional primitives the catalog writer
needs — no generic object-store interface (the plan explicitly defers
GCS/Azure/fsspec backends until a second backend is scheduled).

Conditional contract (mirrored at the SQLite level by :class:`WriterLeases`):

- ``put_if_absent(key, data)``            create-only; ``ConditionFailed``
  if the key already exists (S3 ``IfNoneMatch="*"``).
- ``put_if_absent(key, data, if_match=e)`` optimistic overwrite; succeeds
  only when the current etag equals ``e``.
- ``delete(key, if_match=e)``             conditional delete.

Writer leases fence remote writers the way local ops are fenced by claim
tokens: acquisition is a conditional upsert
(``INSERT ... ON CONFLICT DO UPDATE ... WHERE``), never a read-then-write,
so two racing processes can never both win regardless of interleaving.

The post-call fence (:meth:`WriterLeases.with_writer_lease`) closes the
"provider request outlives the lease" hole: the catalog commit transaction
is only attempted while the lease row still names our claim token with an
unexpired deadline, checked inside ``BEGIN IMMEDIATE`` (the same fencing
style as :mod:`vanth.artifacts.lifecycle` GC). A lost lease converts the
outcome into an idempotent replay lookup so no duplicate catalog row can
appear; a later retry converges through the durable-op idempotency key.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Protocol, runtime_checkable

from .catalog import Catalog, new_id, now_iso

__all__ = [
    "ProviderError",
    "ConditionFailed",
    "NoSuchKey",
    "S3Provider",
    "InMemoryProvider",
    "Boto3Provider",
    "capability_probe",
    "WriterLeases",
    "StorageProfiles",
    "PROBE_PREFIX",
    "MISSING_EXTRA_MESSAGE",
    "DEFAULT_WRITER_LEASE_SECONDS",
]

PROBE_PREFIX = ".vanth-probe/"
DEFAULT_WRITER_LEASE_SECONDS = 300

MISSING_EXTRA_MESSAGE = (
    "missing-extra: install vanth[artifact-s3] to use S3 storage profiles"
)


class ProviderError(RuntimeError):
    """Any provider-level failure (transport, missing extra, bad upload)."""


class ConditionFailed(ProviderError):
    """A conditional operation's precondition did not hold."""


class NoSuchKey(ProviderError):
    """The requested key does not exist."""


def _require_boto3():
    try:
        import boto3
    except ImportError:
        raise ProviderError(MISSING_EXTRA_MESSAGE) from None
    return boto3


@runtime_checkable
class S3Provider(Protocol):
    """The exact set of conditional primitives the catalog writer uses."""

    def put_if_absent(self, key: str, data: bytes, *, if_match: str | None = None) -> dict[str, Any]:
        """Create-only without ``if_match``; optimistic overwrite with it."""
        ...

    def get(self, key: str) -> tuple[bytes, dict[str, Any]]: ...

    def head(self, key: str) -> dict[str, Any] | None: ...

    def delete(self, key: str, *, if_match: str | None = None) -> None: ...

    def list_prefix(self, prefix: str) -> list[str]: ...

    def init_multipart(self, key: str) -> str: ...

    def upload_part(self, key: str, upload_id: str, part_number: int, data: bytes) -> dict[str, Any]: ...

    def complete_multipart(self, key: str, upload_id: str, parts: list[dict[str, Any]]) -> dict[str, Any]: ...

    def abort_multipart(self, key: str, upload_id: str) -> None: ...


def _meta(record: dict[str, Any], size: int | None = None) -> dict[str, Any]:
    out = {"etag": record["etag"], "version_id": record["version_id"]}
    if size is not None:
        out["size_bytes"] = size
    return out


class InMemoryProvider:
    """Deterministic in-memory implementation of :class:`S3Provider`.

    Etags are sha256 over a monotonic counter plus content; version ids are
    monotonic per provider instance. This is what tests run against — no
    credentials or network required.
    """

    def __init__(self, bucket: str = "vanth-test") -> None:
        self.bucket = bucket
        self._objects: dict[str, dict[str, Any]] = {}
        self._uploads: dict[tuple[str, str], dict[int, bytes]] = {}
        self._lock = threading.Lock()
        self._etag_counter = 0
        self._version_counter = 0

    # -- deterministic minting -------------------------------------------------

    def _next_etag(self, data: bytes) -> str:
        self._etag_counter += 1
        return hashlib.sha256(f"{self._etag_counter}:".encode() + data).hexdigest()

    def _next_version_id(self) -> str:
        self._version_counter += 1
        return f"vid-{self._version_counter:08d}"

    def _store(self, key: str, data: bytes) -> dict[str, Any]:
        record = {
            "data": bytes(data),
            "etag": self._next_etag(data),
            "version_id": self._next_version_id(),
        }
        self._objects[key] = record
        return record

    # -- conditional primitives ------------------------------------------------

    def put_if_absent(self, key: str, data: bytes, *, if_match: str | None = None) -> dict[str, Any]:
        data = bytes(data)
        with self._lock:
            current = self._objects.get(key)
            if current is None:
                if if_match is not None:
                    raise ConditionFailed(f"precondition failed: {key} does not exist")
            else:
                if if_match is None:
                    raise ConditionFailed(f"precondition failed: {key} already exists")
                if if_match != current["etag"]:
                    raise ConditionFailed(
                        f"precondition failed: etag mismatch for {key}"
                    )
            record = self._store(key, data)
            return _meta(record)

    def get(self, key: str) -> tuple[bytes, dict[str, Any]]:
        with self._lock:
            record = self._objects.get(key)
            if record is None:
                raise NoSuchKey(f"NoSuchKey: {key}")
            return record["data"], _meta(record, size=len(record["data"]))

    def head(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            record = self._objects.get(key)
            if record is None:
                return None
            return _meta(record, size=len(record["data"]))

    def delete(self, key: str, *, if_match: str | None = None) -> None:
        with self._lock:
            record = self._objects.get(key)
            if record is None:
                return
            if if_match is not None and if_match != record["etag"]:
                raise ConditionFailed(f"precondition failed: etag mismatch for {key}")
            del self._objects[key]

    def list_prefix(self, prefix: str) -> list[str]:
        with self._lock:
            return sorted(k for k in self._objects if k.startswith(prefix))

    # -- multipart --------------------------------------------------------------

    def init_multipart(self, key: str) -> str:
        upload_id = f"mpu-{secrets.token_hex(8)}"
        with self._lock:
            self._uploads[(key, upload_id)] = {}
        return upload_id

    def upload_part(
        self, key: str, upload_id: str, part_number: int, data: bytes
    ) -> dict[str, Any]:
        data = bytes(data)
        with self._lock:
            stored = self._uploads.get((key, upload_id))
            if stored is None:
                raise ProviderError(f"NoSuchUpload: {upload_id}")
            stored[int(part_number)] = data
        return {
            "part_number": int(part_number),
            "etag": self._next_etag(data),
            "size_bytes": len(data),
        }

    def complete_multipart(
        self, key: str, upload_id: str, parts: list[dict[str, Any]]
    ) -> dict[str, Any]:
        with self._lock:
            stored = self._uploads.pop((key, upload_id), None)
            if stored is None:
                raise ProviderError(f"NoSuchUpload: {upload_id}")
            ordered = sorted(parts, key=lambda p: int(p["part_number"]))
            missing = [int(p["part_number"]) for p in ordered if int(p["part_number"]) not in stored]
            if missing:
                raise ProviderError(f"InvalidPart: missing parts {missing}")
            data = b"".join(stored[int(p["part_number"])] for p in ordered)
            record = self._store(key, data)
            return {**_meta(record), "size_bytes": len(data)}

    def abort_multipart(self, key: str, upload_id: str) -> None:
        with self._lock:
            self._uploads.pop((key, upload_id), None)


class Boto3Provider:
    """Thin real-endpoint implementation mapping the conditional primitives
    onto boto3. Never exercised by tests (no credentials/network); kept
    import-light via the lazy ``_require_boto3()`` guard.

    Conditional-op mapping (best the S3 API allows):

    - ``put_if_absent(if_match=None)``  -> ``put_object(IfNoneMatch="*")``
      create-only; HTTP 412 / ``PreconditionFailed`` maps to
      ``ConditionFailed`` (exactly our create-only semantics).
    - ``put_if_absent(if_match=etag)``  -> ``put_object(IfMatch=etag)``
      optimistic overwrite; 412 -> ``ConditionFailed``, 404 -> ``ConditionFailed``
      (nothing to match).
    - ``get``                           -> ``get_object``; ``NoSuchKey`` -> ``NoSuchKey``.
    - ``head``                          -> ``head_object``; 404 -> ``None``.
    - ``delete(if_match=None)``         -> ``delete_object`` (idempotent).
    - ``delete(if_match=etag)``         -> ``delete_object(IfMatch=etag)``
      conditional delete (S3 supports If-Match on DeleteObject since the
      Dec 2024 conditional-write GA); 412 -> ``ConditionFailed``.
    - ``list_prefix``                   -> paginated ``list_objects_v2``.
    - multipart                         -> ``create_multipart_upload`` /
      ``upload_part`` / ``complete_multipart_upload(MultipartUpload={"Parts":
      [...]})`` / ``abort_multipart_upload``.
    """

    def __init__(
        self,
        bucket: str,
        *,
        region: str | None = None,
        endpoint_url: str | None = None,
        client: Any = None,
    ) -> None:
        self.bucket = bucket
        if client is None:
            client = _require_boto3().client("s3", region_name=region, endpoint_url=endpoint_url)
        self._client = client

    @staticmethod
    def _error_code(exc: Exception) -> str:
        response = getattr(exc, "response", None) or {}
        error = response.get("Error") or {}
        return str(error.get("Code", ""))

    @staticmethod
    def _raise_condition(code: str, key: str) -> None:
        if code in {"PreconditionFailed", "412", "ConditionalRequestConflict"}:
            raise ConditionFailed(f"precondition failed for {key}") from None
        if code in {"NoSuchKey", "NotFound", "404"}:
            raise ConditionFailed(f"precondition failed for {key}: object absent") from None

    def put_if_absent(self, key: str, data: bytes, *, if_match: str | None = None) -> dict[str, Any]:
        from botocore.exceptions import ClientError

        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Key": key, "Body": bytes(data)}
        if if_match is None:
            kwargs["IfNoneMatch"] = "*"
        else:
            kwargs["IfMatch"] = if_match
        try:
            resp = self._client.put_object(**kwargs)
        except ClientError as exc:
            code = self._error_code(exc)
            self._raise_condition(code, key)
            raise ProviderError(f"put failed for {key}: {code}") from exc
        return {
            "etag": str(resp.get("ETag", "")).strip('"'),
            "version_id": resp.get("VersionId"),
        }

    def get(self, key: str) -> tuple[bytes, dict[str, Any]]:
        from botocore.exceptions import ClientError

        try:
            resp = self._client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if self._error_code(exc) in {"NoSuchKey", "NotFound", "404"}:
                raise NoSuchKey(f"NoSuchKey: {key}") from exc
            raise
        body = resp["Body"].read()
        meta = {
            "etag": str(resp.get("ETag", "")).strip('"'),
            "version_id": resp.get("VersionId"),
            "size_bytes": int(resp.get("ContentLength", len(body))),
        }
        return body, meta

    def head(self, key: str) -> dict[str, Any] | None:
        from botocore.exceptions import ClientError

        try:
            resp = self._client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if self._error_code(exc) in {"NoSuchKey", "NotFound", "404"}:
                return None
            raise
        return {
            "etag": str(resp.get("ETag", "")).strip('"'),
            "version_id": resp.get("VersionId"),
            "size_bytes": int(resp.get("ContentLength", 0)),
        }

    def delete(self, key: str, *, if_match: str | None = None) -> None:
        from botocore.exceptions import ClientError

        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Key": key}
        if if_match is not None:
            kwargs["IfMatch"] = if_match
        try:
            self._client.delete_object(**kwargs)
        except ClientError as exc:
            code = self._error_code(exc)
            self._raise_condition(code, key)
            raise ProviderError(f"delete failed for {key}: {code}") from exc

    def list_prefix(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self._client.list_objects_v2(**kwargs)
            keys.extend(item["Key"] for item in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                return sorted(keys)
            token = resp["NextContinuationToken"]

    def init_multipart(self, key: str) -> str:
        resp = self._client.create_multipart_upload(Bucket=self.bucket, Key=key)
        return resp["UploadId"]

    def upload_part(
        self, key: str, upload_id: str, part_number: int, data: bytes
    ) -> dict[str, Any]:
        resp = self._client.upload_part(
            Bucket=self.bucket, Key=key, UploadId=upload_id,
            PartNumber=int(part_number), Body=bytes(data),
        )
        return {
            "part_number": int(part_number),
            "etag": str(resp.get("ETag", "")).strip('"'),
            "size_bytes": len(data),
        }

    def complete_multipart(
        self, key: str, upload_id: str, parts: list[dict[str, Any]]
    ) -> dict[str, Any]:
        payload = {
            "Parts": [
                {"PartNumber": int(p["part_number"]), "ETag": p["etag"]}
                for p in sorted(parts, key=lambda p: int(p["part_number"]))
            ]
        }
        resp = self._client.complete_multipart_upload(
            Bucket=self.bucket, Key=key, UploadId=upload_id, MultipartUpload=payload,
        )
        return {
            "etag": str(resp.get("ETag", "")).strip('"'),
            "version_id": resp.get("VersionId"),
        }

    def abort_multipart(self, key: str, upload_id: str) -> None:
        self._client.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)


def capability_probe(provider: S3Provider, bucket: str = "") -> dict[str, bool]:
    """Cheaply discover what a provider/bucket actually supports.

    Runs under a reserved probe prefix (``.vanth-probe/``): one conditional
    put (create-only), one read-back for version ids, one minimal multipart
    round-trip, then cleanup. ``bucket`` matters only for providers whose
    client is bound per-call (the in-memory and pre-bound clients ignore it);
    InMemory probes report all-True by construction.
    """
    capabilities = {"conditional_put": False, "versioning": False, "multipart": False}
    probe_key = f"{PROBE_PREFIX}probe-{secrets.token_hex(8)}"
    try:
        try:
            provider.put_if_absent(probe_key, b"vanth-capability-probe")
            capabilities["conditional_put"] = True
        except ConditionFailed:
            pass
        try:
            _, meta = provider.get(probe_key)
            capabilities["versioning"] = bool(meta.get("version_id"))
        except ProviderError:
            pass
        upload_id = ""
        try:
            upload_id = provider.init_multipart(probe_key)
            part = provider.upload_part(probe_key, upload_id, 1, b"vanth-capability-probe")
            provider.complete_multipart(probe_key, upload_id, [part])
            capabilities["multipart"] = True
        except ProviderError:
            if upload_id:
                try:
                    provider.abort_multipart(probe_key, upload_id)
                except ProviderError:
                    pass
    finally:
        try:
            provider.delete(probe_key)
        except ProviderError:
            pass
    return capabilities


# ---------------------------------------------------------------------------
# Writer leases (schema v3 ``writer_leases`` table)
# ---------------------------------------------------------------------------


class WriterLeases:
    """Catalog/root writer leases keyed like ``'catalog'`` / ``root:<id>``.

    Acquisition is fully conditional at the SQLite level — an UPSERT whose
    ``DO UPDATE ... WHERE`` clause requires either an expired lease or the
    same owner — mirroring the provider conditional-write contract. Two
    processes racing on one lease key can never both win regardless of
    interleaving because there is no read-then-write window anywhere.
    """

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    @property
    def db(self):
        return self.catalog.db

    def acquire(
        self, lease_key: str, instance_id: str, *, ttl_seconds: int = DEFAULT_WRITER_LEASE_SECONDS
    ) -> str | None:
        """Try to take ``lease_key``; returns a fresh claim token or ``None``."""
        token = secrets.token_hex(16)
        now = datetime.now(timezone.utc)
        expires = (
            now + timedelta(seconds=max(1, int(ttl_seconds)))
        ).isoformat().replace("+00:00", "Z")
        stamp = now_iso()
        db = self.db
        with self.catalog.lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                changed = db.execute(
                    """
                    INSERT INTO writer_leases(lease_key, owner_instance_id, claim_token,
                      lease_expires_at, generation, updated_at)
                    VALUES (?, ?, ?, ?, 0, ?)
                    ON CONFLICT(lease_key) DO UPDATE SET
                      owner_instance_id=excluded.owner_instance_id,
                      claim_token=excluded.claim_token,
                      lease_expires_at=excluded.lease_expires_at,
                      generation=writer_leases.generation+1,
                      updated_at=excluded.updated_at
                    WHERE writer_leases.lease_expires_at <= excluded.updated_at
                       OR writer_leases.owner_instance_id = excluded.owner_instance_id
                    """,
                    (lease_key, instance_id, token, expires, stamp),
                ).rowcount
                if not changed:
                    db.rollback()
                    return None
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return token

    def renew(
        self,
        lease_key: str,
        claim_token: str,
        *,
        ttl_seconds: int = DEFAULT_WRITER_LEASE_SECONDS,
    ) -> dict[str, Any] | None:
        """Extend a live lease we still own; ``None`` when lost/expired."""
        now = datetime.now(timezone.utc)
        expires = (
            now + timedelta(seconds=max(1, int(ttl_seconds)))
        ).isoformat().replace("+00:00", "Z")
        stamp = now_iso()
        db = self.db
        with self.catalog.lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                changed = db.execute(
                    """
                    UPDATE writer_leases SET lease_expires_at=?, generation=generation+1, updated_at=?
                    WHERE lease_key=? AND claim_token=? AND lease_expires_at > ?
                    """,
                    (expires, stamp, lease_key, claim_token, now_iso()),
                ).rowcount
                if not changed:
                    db.rollback()
                    return None
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return {"lease_key": lease_key, "claim_token": claim_token, "lease_expires_at": expires}

    def release(self, lease_key: str, claim_token: str) -> bool:
        """Release a lease only if we still own it (token-fenced delete)."""
        db = self.db
        with self.catalog.lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                changed = db.execute(
                    "DELETE FROM writer_leases WHERE lease_key=? AND claim_token=?",
                    (lease_key, claim_token),
                ).rowcount
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return bool(changed)

    def holds(self, lease_key: str, claim_token: str) -> bool:
        """True while ``claim_token`` names the live unexpired lease."""
        db = self.db
        with self.catalog.lock:
            row = db.execute(
                "SELECT 1 FROM writer_leases WHERE lease_key=? AND claim_token=? AND lease_expires_at > ?",
                (lease_key, claim_token, now_iso()),
            ).fetchone()
        return bool(row)

    def inspect(self, lease_key: str) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT * FROM writer_leases WHERE lease_key=?", (lease_key,)
        ).fetchone()
        if not row:
            return None
        return {
            "lease_key": row["lease_key"],
            "owner_instance_id": row["owner_instance_id"],
            "claim_token": row["claim_token"],
            "lease_expires_at": row["lease_expires_at"],
            "generation": int(row["generation"]),
        }

    def reclaim_expired(self) -> list[dict[str, Any]]:
        """Report expired leases; takeover happens through conditional acquire.

        Deletion is deliberately NOT done here: an expired-but-present row
        keeps fencing stale tokens until the next owner's conditional upsert
        replaces it, which matches how expired op claims are superseded by
        generation bumps rather than erased.
        """
        rows = self.db.execute(
            "SELECT * FROM writer_leases WHERE lease_expires_at <= ?", (now_iso(),)
        ).fetchall()
        return [self.inspect(row["lease_key"]) for row in rows]

    # -- post-call fenced commit --------------------------------------------

    def with_writer_lease(
        self,
        lease_key: str,
        instance_id: str,
        *,
        work: Callable[[str], Any],
        commit: Callable[[Any, Any], Any],
        replay: Callable[[Any, Any], Any] | None = None,
        ttl_seconds: int = DEFAULT_WRITER_LEASE_SECONDS,
        release_after: bool = True,
    ) -> dict[str, Any]:
        """acquire -> run provider work -> FENCED revalidate -> commit-or-replay.

        The commit callable runs inside ONE ``BEGIN IMMEDIATE`` that first
        re-checks the lease row against our token and an unexpired deadline
        (fence revalidation). If a provider call outlived the lease, the
        commit is refused and the outcome becomes an idempotent replay
        lookup instead — the durable-op pattern dedupes any retry by
        idempotency key, so no duplicate catalog row can appear.
        """
        token = self.acquire(lease_key, instance_id, ttl_seconds=ttl_seconds)
        if token is None:
            raise ValueError(f"writer lease unavailable: {lease_key}")
        try:
            result = work(token)
            db = self.db
            lost = False
            with self.catalog.lock:
                db.execute("BEGIN IMMEDIATE")
                try:
                    live = db.execute(
                        "SELECT 1 FROM writer_leases WHERE lease_key=? AND claim_token=? "
                        "AND lease_expires_at > ?",
                        (lease_key, token, now_iso()),
                    ).fetchone()
                    if not live:
                        db.rollback()
                        lost = True
                    else:
                        out = commit(db, result)
                        db.commit()
                        return {
                            "committed": True,
                            "fenced": False,
                            "replayed": False,
                            "lease_key": lease_key,
                            "claim_token": token,
                            "result": out,
                        }
                except BaseException:
                    db.rollback()
                    raise
            if lost:
                existing = replay(db, result) if replay is not None else None
                return {
                    "committed": False,
                    "fenced": True,
                    "replayed": existing is not None,
                    "lease_key": lease_key,
                    "claim_token": token,
                    "result": existing,
                }
        finally:
            if release_after:
                try:
                    self.release(lease_key, token)
                except Exception:
                    pass
        raise AssertionError("unreachable")


# ---------------------------------------------------------------------------
# Storage profiles (schema v3 ``storage_profiles`` table)
# ---------------------------------------------------------------------------


class StorageProfiles:
    """Immutable storage-profile revisions.

    A profile is NEVER updated in place: changing configuration inserts a
    new row with ``revision+1`` under the same ``profile_id``; every
    historical revision stays queryable through :meth:`revisions`.
    """

    ALLOWED_KINDS = frozenset({"s3"})

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    # -- validation -----------------------------------------------------------

    def _validate(self, kind: str, config: dict[str, Any]) -> None:
        if kind not in self.ALLOWED_KINDS:
            raise ValueError(f"unsupported storage-profile kind: {kind}")
        if not isinstance(config, dict) or not str(config.get("bucket") or "").strip():
            raise ValueError("storage profile config requires a non-empty 'bucket'")
        for key in config:
            if not isinstance(key, str):
                raise ValueError("storage profile config keys must be strings")

    # -- revision writes --------------------------------------------------------

    def create(self, kind: str = "s3", config: dict[str, Any] | None = None) -> dict[str, Any]:
        config = config or {}
        self._validate(kind, config)
        profile_id = new_id("spr")
        stamp = now_iso()
        db = self.catalog.db
        with self.catalog.lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    """
                    INSERT INTO storage_profiles(profile_id, revision, kind, config_json,
                      capabilities_json, created_at)
                    VALUES (?, 1, ?, ?, '{}', ?)
                    """,
                    (
                        profile_id,
                        kind,
                        json.dumps(config, separators=(",", ":"), sort_keys=True),
                        stamp,
                    ),
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return self.get(profile_id)

    def update(self, profile_id: str, config: dict[str, Any]) -> dict[str, Any]:
        """Insert the NEXT immutable revision; old revisions stay queryable."""
        config = config or {}
        db = self.catalog.db
        with self.catalog.lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute(
                    "SELECT kind FROM storage_profiles WHERE profile_id=? "
                    "ORDER BY revision DESC LIMIT 1",
                    (profile_id,),
                ).fetchone()
                if not row:
                    db.rollback()
                    raise ValueError(f"Unknown storage profile: {profile_id}")
                kind = row["kind"]
                self._validate(kind, config)
                next_rev = db.execute(
                    "SELECT COALESCE(MAX(revision), 0) + 1 FROM storage_profiles WHERE profile_id=?",
                    (profile_id,),
                ).fetchone()[0]
                db.execute(
                    """
                    INSERT INTO storage_profiles(profile_id, revision, kind, config_json,
                      capabilities_json, created_at)
                    VALUES (?, ?, ?, ?, '{}', ?)
                    """,
                    (
                        profile_id,
                        int(next_rev),
                        kind,
                        json.dumps(config, separators=(",", ":"), sort_keys=True),
                        now_iso(),
                    ),
                )
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return self.get(profile_id)

    # -- reads --------------------------------------------------------------------

    @staticmethod
    def _row_dict(row) -> dict[str, Any]:
        return {
            "profile_id": row["profile_id"],
            "revision": int(row["revision"]),
            "kind": row["kind"],
            "config": json.loads(row["config_json"]),
            "capabilities": json.loads(row["capabilities_json"]),
            "created_at": row["created_at"],
        }

    def get(self, profile_id: str) -> dict[str, Any]:
        """Latest revision of a profile."""
        row = self.catalog.db.execute(
            "SELECT * FROM storage_profiles WHERE profile_id=? ORDER BY revision DESC LIMIT 1",
            (profile_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"Unknown storage profile: {profile_id}")
        return self._row_dict(row)

    def revisions(self, profile_id: str) -> list[dict[str, Any]]:
        rows = self.catalog.db.execute(
            "SELECT * FROM storage_profiles WHERE profile_id=? ORDER BY revision ASC",
            (profile_id,),
        ).fetchall()
        return [self._row_dict(row) for row in rows]

    # -- capability probing ----------------------------------------------------------

    def probe(self, profile_id: str, *, provider: S3Provider | None = None) -> dict[str, Any]:
        """Probe endpoint capabilities and store them on the LATEST revision.

        Without an injected provider this builds :class:`Boto3Provider`,
        which raises the targeted missing-extra ``ProviderError`` when boto3
        is not installed.
        """
        latest = self.get(profile_id)
        if provider is None:
            if latest["kind"] != "s3":
                raise ValueError(f"unsupported storage-profile kind: {latest['kind']}")
            provider = Boto3Provider(
                str(latest["config"]["bucket"]),
                region=latest["config"].get("region"),
                endpoint_url=latest["config"].get("endpoint_url"),
            )
        caps = capability_probe(provider, str(latest["config"].get("bucket", "")))
        db = self.catalog.db
        with self.catalog.lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                changed = db.execute(
                    """
                    UPDATE storage_profiles SET capabilities_json=?
                    WHERE profile_id=? AND revision=?
                    """,
                    (
                        json.dumps(caps, separators=(",", ":"), sort_keys=True),
                        profile_id,
                        int(latest["revision"]),
                    ),
                ).rowcount
                if not changed:
                    db.rollback()
                    raise ValueError(f"Unknown storage profile: {profile_id}")
                db.commit()
            except BaseException:
                db.rollback()
                raise
        latest["capabilities"] = caps
        return latest
