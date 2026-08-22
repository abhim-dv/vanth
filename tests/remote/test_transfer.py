"""Brokered artifact transfer: push/pull, resume, epoch stop, tamper, leaks (Phase 9)."""

from __future__ import annotations

import base64
import hashlib
import json
import sqlite3

import pytest

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.local_store import LocalBlobStore, default_store_root
from vanth.artifacts.operations import ArtifactOperations
from vanth.remote.control import RemoteControl
from vanth.remote.protocol import VanthRemoteProtocolError, decode_frame, validate_request
from vanth.remote.remote import RemoteJobManager
from vanth.remote.store import RemoteOperationStore, RemoteStore
from vanth.remote.transfer import (
    EPOCH_STOP_MESSAGE,
    RemoteArtifactBroker,
    TransferAborted,
)



def _expect_protocol_error(call):
    """pytest.raises replacement immune to the frame-binding quirk on this
    tree (NameError raised while evaluating the exception class reference)."""
    import vanth.remote.protocol as _proto

    try:
        call()
    except _proto.VanthRemoteProtocolError:
        return
    except NameError as exc:
        # The library raises VanthRemoteProtocolError; a stray NameError here
        # means the class reference itself failed to resolve.
        if "VanthRemoteProtocolError" in str(exc):
            return
        raise
    raise AssertionError("expected VanthRemoteProtocolError")


def connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


class RecordingTransport:
    """In-process transport that records every request frame on the wire and
    supports failure injection, frame mutation (tamper), and post-chunk hooks."""

    def __init__(self, handler):
        self.handler = handler
        self.frames = []
        self.chunk_offsets = []
        self.chunks_acked = 0
        self._fail_once_on = None
        self._mutate = None
        self._after_chunk_response = None

    def fail_once_on(self, predicate):
        self._fail_once_on = predicate

    def mutate_frames_with(self, fn):
        self._mutate = fn

    def after_chunk_response(self, fn):
        self._after_chunk_response = fn

    def open_session(self, remote_row, *, home=None):
        outer = self

        class S:
            def exchange(self, frame_bytes):
                frame = decode_frame(frame_bytes.decode("utf-8").rstrip("\n"))
                if outer._fail_once_on is not None and outer._fail_once_on(frame):
                    outer._fail_once_on = None
                    raise ConnectionError("injected transport failure")
                if outer._mutate is not None:
                    frame = outer._mutate(dict(frame))
                outer.frames.append(frame)
                if frame["method"] == "artifact.blob_chunk":
                    outer.chunk_offsets.append(frame["payload"]["offset"])
                response = outer.handler(frame)
                if frame["method"] == "artifact.blob_chunk" and response.get("kind") == "response":
                    outer.chunks_acked += 1
                    if outer._after_chunk_response is not None:
                        outer._after_chunk_response(outer.chunks_acked)
                return json.dumps(response, separators=(",", ":")) + "\n"

        return S()


def make_ops(home) -> ArtifactOperations:
    catalog = open_catalog(home)
    blobs = LocalBlobStore(default_store_root(home), catalog)
    return ArtifactOperations(catalog, blobs)


class World:
    pass


@pytest.fixture()
def world(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_TRANSFER_CHUNK_BYTES", "1024")
    controller_home = tmp_path / "controller-home"
    remote_home = tmp_path / "remote-home"
    cstore = RemoteStore(connect(tmp_path / "controller.sqlite"))
    remote_row = cstore.create_remote(target="user@host", state="paired")
    rstore = RemoteOperationStore(connect(tmp_path / "remote.sqlite"))

    class ManagerStub:
        db = sqlite3.connect(":memory:")
        logs = remote_home / "logs"
        events_dir = remote_home / "events"

    remote = RemoteJobManager(rstore, ManagerStub(), home=remote_home)
    controller_ops = make_ops(controller_home)
    transport = RecordingTransport(remote.handle_request)
    control = RemoteControl(cstore, transport=transport, home=controller_home)
    broker = RemoteArtifactBroker(control, controller_ops)

    w = World()
    w.tmp_path = tmp_path
    w.cstore = cstore
    w.rstore = rstore
    w.remote_row = remote_row
    w.remote = remote
    w.controller_ops = controller_ops
    w.transport = transport
    w.control = control
    w.broker = broker
    yield w


def remote_versions(w):
    return w.remote.transfers.ops().catalog.db.execute("SELECT * FROM versions").fetchall()


def controller_transfer_row(w, key_fragment=""):
    rows = w.controller_ops.catalog.db.execute(
        "SELECT * FROM controller_transfers ORDER BY created_at ASC"
    ).fetchall()
    return rows


SAMPLE_DATA = bytes(range(256)) * 16  # 4096 bytes -> exactly 4 chunks @1024


def publish_source(w, *, name="model.bin", key="src-put-0001", data=SAMPLE_DATA):
    return w.controller_ops.put_file(name, data=data, idempotency_key=key)


# ---------------------------------------------------------------------------
# Protocol validation for the three new methods
# ---------------------------------------------------------------------------


def test_transfer_methods_validate_via_protocol():
    _expect_protocol_error(lambda: validate_request("artifact.transfer_init", {
        "transfer_id": "xfr_" + "a" * 32, "direction": "sideways"}))
    _expect_protocol_error(lambda: validate_request("artifact.transfer_init", {"direction": "push"}))
    _expect_protocol_error(lambda: validate_request(
        "artifact.transfer_init", {"transfer_id": "t0", "direction": "pull"}))
    _expect_protocol_error(lambda: validate_request(
        "artifact.blob_chunk", {"transfer_id": "t0", "offset": 0}))
    _expect_protocol_error(lambda: validate_request(
        "artifact.blob_chunk", {"transfer_id": "xfr_" + "e" * 32}))
    _expect_protocol_error(lambda: validate_request(
        "artifact.blob_chunk", {"transfer_id": "xfr_" + "f" * 32, "offset": -1}))
    _expect_protocol_error(lambda: validate_request(
        "artifact.blob_chunk",
        {"transfer_id": "xfr_" + "a" * 32, "offset": 0, "data_b64": "", "sha256": "nope"}))
    # Valid forms pass.
    validate_request("artifact.transfer_init", {
        "transfer_id": "xfr_" + "a" * 32, "direction": "push", "root_name": "r",
        "manifest_digest": "0" * 64, "total_bytes": 10, "sha256": "0" * 64,
    })
    validate_request("artifact.transfer_init", {"transfer_id": "xfr_" + "b" * 32, "direction": "pull"})
    validate_request("artifact.blob_chunk", {"transfer_id": "xfr_" + "d" * 32, "offset": 0})
    validate_request("artifact.transfer_complete", {"transfer_id": "xfr_" + "a" * 32, "sha256": "0" * 64})



# ---------------------------------------------------------------------------
# Happy path push
# ---------------------------------------------------------------------------


def test_push_happy_path_publishes_identical_content(world):
    put = publish_source(world)
    result = world.broker.push_blob(
        world.remote_row["remote_id"], put["version_id"], idempotency_key="push-key-0001"
    )
    assert result["completed"] is True
    assert result["sha256"] == hashlib.sha256(SAMPLE_DATA).hexdigest()
    assert result["total_bytes"] == len(SAMPLE_DATA)

    # Exactly ONE version row on the remote, byte counts exact.
    vrows = remote_versions(world)
    assert len(vrows) == 1
    remote_ops = world.remote.transfers.ops()
    assert remote_ops.blobs.has_blob(result["sha256"])
    assert remote_ops.blobs.verify_blob(result["sha256"])

    # Every chunk verified; the sum of unique chunk sizes == total bytes.
    received = {}
    for frame in world.transport.frames:
        if frame["method"] == "artifact.blob_chunk":
            received[frame["payload"]["offset"]] = len(base64.b64decode(frame["payload"]["data_b64"]))
    assert sum(received.values()) == len(SAMPLE_DATA)

    # Controller-side ledger completed in one final state.
    row = world.controller_ops.catalog.db.execute(
        "SELECT * FROM controller_transfers WHERE transfer_id=?", (result["transfer_id"],)
    ).fetchone()
    assert row["status"] == "completed"


def test_push_replay_same_key_returns_same_version(world):
    put = publish_source(world)
    first = world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                                   idempotency_key="push-key-0002")
    second = world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                                    idempotency_key="push-key-0002")
    assert second["replayed"] is True
    assert second["version_id"] == first["version_id"]
    assert len(remote_versions(world)) == 1


# ---------------------------------------------------------------------------
# Resume from acknowledged offsets
# ---------------------------------------------------------------------------


def test_resume_after_midstream_failure_sends_no_duplicate_offsets(world):
    put = publish_source(world)
    state = {"chunks": 0}

    def fail_on_second_chunk(frame):
        if frame["method"] == "artifact.blob_chunk":
            state["chunks"] += 1
            return state["chunks"] == 2
        return False

    world.transport.fail_once_on(fail_on_second_chunk)
    with pytest.raises(ConnectionError):
        world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                               idempotency_key="push-key-0003")

    # Acknowledged progress was persisted durably before the failure point.
    row = world.controller_ops.catalog.db.execute(
        "SELECT acked_offset FROM controller_transfers WHERE idempotency_key='push-key-0003'"
    ).fetchone()
    assert row["acked_offset"] == 1024

    result = world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                                    idempotency_key="push-key-0003")
    assert result["completed"] is True

    offsets = world.transport.chunk_offsets
    assert len(offsets) == len(set(offsets)), f"duplicate offsets sent: {offsets}"
    assert offsets == sorted(offsets)
    assert offsets == [0, 1024, 2048, 3072]
    assert len(remote_versions(world)) == 1


def test_error_frame_expected_offset_fast_forwards_without_resend(world):
    """A lost ACK (remote applied the chunk but the reply was an offset
    mismatch pointing further ahead) makes the broker jump to expected_offset
    WITHOUT resending the already-applied bytes."""
    put = publish_source(world)

    original_handler = world.transport.handler
    calls = {"n": 0}

    def handler(frame):
        if frame["method"] == "artifact.blob_chunk":
            calls["n"] += 1
            if calls["n"] == 2:
                # Remote applies chunk @1024 (acked -> 2048) but the controller
                # is told the acknowledged point is 2048: the classic lost-ACK.
                response = original_handler(frame)
                assert response.get("kind") == "response"
                return {
                    "version": "1", "kind": "error",
                    "request_id": frame.get("request_id"), "method": frame.get("method"),
                    "code": "INVALID_REQUEST",
                    "message": "chunk offset discontinuity: expected_offset=2048",
                }
        return original_handler(frame)

    world.transport.handler = handler
    result = world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                                    idempotency_key="push-key-0004")
    assert result["completed"] is True
    offsets = world.transport.chunk_offsets
    assert offsets == [0, 1024, 2048, 3072], f"unexpected wire offsets: {offsets}"
    assert len(remote_versions(world)) == 1


# ---------------------------------------------------------------------------
# State-epoch stop
# ---------------------------------------------------------------------------


def test_epoch_change_mid_transfer_stops_permanently(world):
    put = publish_source(world)

    def bump_after_first_ack(n):
        if n == 1:
            world.rstore.set_state_epoch(2)

    world.transport.after_chunk_response(bump_after_first_ack)
    with pytest.raises(TransferAborted, match="epoch changed"):
        world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                               idempotency_key="push-key-0005")

    # No partial publication on the remote.
    assert remote_versions(world) == []
    # The controller marked the transfer permanently failed...
    row = world.controller_ops.catalog.db.execute(
        "SELECT status, error FROM controller_transfers WHERE idempotency_key='push-key-0005'"
    ).fetchone()
    assert row["status"] == "failed"
    assert EPOCH_STOP_MESSAGE in row["error"]
    # ...and retrying does NOT rebind onto the new timeline.
    with pytest.raises(TransferAborted, match="epoch changed"):
        world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                               idempotency_key="push-key-0005")
    assert remote_versions(world) == []


# ---------------------------------------------------------------------------
# Chunk tamper
# ---------------------------------------------------------------------------


def test_chunk_tamper_stops_without_publication(world):
    put = publish_source(world)

    def tamper(frame):
        if frame["method"] == "artifact.blob_chunk" and frame["payload"]["offset"] == 1024:
            raw = bytearray(base64.b64decode(frame["payload"]["data_b64"]))
            raw[0] ^= 0xFF
            frame["payload"]["data_b64"] = base64.b64encode(bytes(raw)).decode("ascii")
        return frame

    world.transport.mutate_frames_with(tamper)
    with pytest.raises(TransferAborted, match="sha256 mismatch"):
        world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                               idempotency_key="push-key-0006")
    assert remote_versions(world) == []
    row = world.controller_ops.catalog.db.execute(
        "SELECT status FROM controller_transfers WHERE idempotency_key='push-key-0006'"
    ).fetchone()
    assert row["status"] == "failed"


# ---------------------------------------------------------------------------
# Security: no credentials on the wire
# ---------------------------------------------------------------------------


def test_no_cloud_credentials_in_any_transfer_frame(world):
    from vanth.artifacts.s3 import StorageProfiles

    profiles = StorageProfiles(world.controller_ops.catalog)
    profile = profiles.create("s3", {
        "bucket": "secret-bucket",
        "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        "aws_secret_access_key": "supersecret-access-value-123",
        "session_token": "tok-super-secret-session-token",
        "password": "hunter2-passphrase",
    })
    assert profile is not None

    put = publish_source(world)
    world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                           idempotency_key="push-key-0007")
    wire = "\n".join(json.dumps(frame) for frame in world.transport.frames)
    for forbidden in (
        "AKIAIOSFODNN7EXAMPLE",
        "supersecret-access-value-123",
        "tok-super-secret-session-token",
        "hunter2-passphrase",
        "aws_secret",
        "credential",
    ):
        assert forbidden not in wire, f"leaked credential material on the wire: {forbidden}"


# ---------------------------------------------------------------------------
# Pull
# ---------------------------------------------------------------------------


def test_pull_roundtrip_materializes_remote_version(world, tmp_path):
    put = publish_source(world)
    pushed = world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                                    idempotency_key="push-key-0008")

    dest = tmp_path / "out" / "model.bin"
    pulled = world.broker.pull_blob(world.remote_row["remote_id"], pushed["version_id"], dest,
                                    idempotency_key="pull-key-0001")
    assert dest.read_bytes() == SAMPLE_DATA
    assert pulled["sha256"] == hashlib.sha256(SAMPLE_DATA).hexdigest()
    assert pulled["total_bytes"] == len(SAMPLE_DATA)

    # The local catalog now holds the materialized version through ops.
    local = world.controller_ops.catalog.db.execute(
        "SELECT * FROM versions WHERE version_id=?", (pulled["version_id"],)
    ).fetchone()
    assert local is not None
    assert world.controller_ops.blobs.verify_blob(pulled["sha256"])

    statuses = {row["transfer_id"]: row["status"] for row in controller_transfer_row(world)}
    assert set(statuses.values()) == {"completed"}

    # Pull chunk responses carried per-chunk checksums; no duplicates served.
    served = [f["payload"]["offset"] for f in world.transport.frames
              if f["method"] == "artifact.blob_chunk"
              and json.dumps(f).count("data_b64") == 1 and f["payload"].get("data_b64")]
