"""Brokered artifact transfer: push/pull, resume, epoch stop, tamper, leaks (Phase 9)."""

from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
from pathlib import Path

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
    def _vre():
        # Function-local binding sidesteps the frame-binding quirk on this
        # tree (review P2-5: prefer direct pytest.raises over a swallowing
        # helper).
        from vanth.remote.protocol import VanthRemoteProtocolError
        return VanthRemoteProtocolError

    with pytest.raises(_vre()):
        validate_request("artifact.transfer_init", {
            "transfer_id": "xfr_" + "a" * 32, "direction": "sideways"})
    with pytest.raises(_vre()):
        validate_request("artifact.transfer_init", {"direction": "push"})
    with pytest.raises(_vre()):
        validate_request(
            "artifact.transfer_init", {"transfer_id": "t0", "direction": "pull"})
    with pytest.raises(_vre()):
        validate_request(
            "artifact.blob_chunk", {"transfer_id": "t0", "offset": 0})
    with pytest.raises(_vre()):
        validate_request(
            "artifact.blob_chunk", {"transfer_id": "xfr_" + "e" * 32})
    with pytest.raises(_vre()):
        validate_request(
            "artifact.blob_chunk", {"transfer_id": "xfr_" + "f" * 32, "offset": -1})
    with pytest.raises(_vre()):
        validate_request(
            "artifact.blob_chunk",
            {"transfer_id": "xfr_" + "a" * 32, "offset": 0, "data_b64": "", "sha256": "nope"})
    # Valid forms pass.
    validate_request("artifact.transfer_init", {
        "transfer_id": "xfr_" + "a" * 32, "direction": "push", "root_name": "r",
        "manifest_digest": "0" * 64, "total_bytes": 10, "sha256": "0" * 64,
    })
    validate_request("artifact.transfer_init", {"transfer_id": "xfr_" + "b" * 32, "direction": "pull"})
    validate_request("artifact.blob_chunk", {"transfer_id": "xfr_" + "d" * 32, "offset": 0})
    validate_request("artifact.transfer_complete", {
        "transfer_id": "xfr_" + "a" * 32,
        "root_name": "r", "manifest_digest": "0" * 64, "total_bytes": 0,
        "sha256": "0" * 64,
    })



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


def test_remote_init_resets_ack_when_push_staging_prefix_is_missing(world):
    transfer_id = "xfr_" + "c" * 32
    payload = {
        "transfer_id": transfer_id, "direction": "push", "root_name": "r",
        "manifest_digest": "0" * 64, "total_bytes": 3, "sha256": hashlib.sha256(b"abc").hexdigest(),
    }
    first = world.remote.transfers.init(payload)
    world.remote.transfers.db.execute(
        "UPDATE remote_transfers SET acked_offset=2 WHERE transfer_id=?", (transfer_id,)
    )
    world.remote.transfers.db.commit()
    row = world.remote.transfers.db.execute(
        "SELECT staging_path FROM remote_transfers WHERE transfer_id=?", (transfer_id,)
    ).fetchone()
    Path(row["staging_path"]).unlink(missing_ok=True)
    resumed = world.remote.transfers.init(payload)
    assert resumed["acked_offset"] == 0
    assert Path(row["staging_path"]).is_file()


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


def test_no_cloud_credentials_in_any_transfer_frame(world, monkeypatch):
    """Review P1-14/P2-9 combined: transfer frames carry only content ids and
    bytes. Storage-profile configs reject credential fields outright, and
    ambient secret material from the environment never reaches the wire."""
    from vanth.artifacts.s3 import StorageProfiles

    # P2-9: credential-shaped config keys are rejected at creation.
    profiles = StorageProfiles(world.controller_ops.catalog)
    with pytest.raises(ValueError, match="credential fields"):
        profiles.create("s3", {
            "bucket": "secret-bucket",
            "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
        })
    # A legitimate profile carries no secret material.
    profile = profiles.create("s3", {"bucket": "secret-bucket", "prefix": "runs"})
    assert profile is not None

    # Ambient credentials exist in the environment; none may leak onto the wire.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAENVLEAKEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-super-secret-value-456")

    put = publish_source(world)
    world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                           idempotency_key="push-key-0007")
    wire = "\n".join(json.dumps(frame) for frame in world.transport.frames)
    for forbidden in (
        "AKIAIOSFODNN7EXAMPLE",
        "supersecret-access-value-123",
        "tok-super-secret-session-token",
        "hunter2-passphrase",
        "AKIAENVLEAKEXAMPLE",
        "env-super-secret-value-456",
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


def test_same_idempotency_key_across_contexts_gets_separate_rows(world, tmp_path):
    """Review rc14 P1-10b: the controller ledger must not globally unique-key
    idempotency; the same caller key against a DIFFERENT destination creates
    its own transfer instead of taking over the other context's row."""
    put = publish_source(world)
    rid = world.remote_row["remote_id"]
    pushed = world.broker.push_blob(rid, put["version_id"], idempotency_key="push-key-0009")

    dest_a = tmp_path / "out-a" / "model.bin"
    dest_b = tmp_path / "out-b" / "model.bin"
    a = world.broker.pull_blob(rid, pushed["version_id"], dest_a, idempotency_key="shared-key-01")
    b = world.broker.pull_blob(rid, pushed["version_id"], dest_b, idempotency_key="shared-key-01")

    assert dest_a.read_bytes() == SAMPLE_DATA
    assert dest_b.read_bytes() == SAMPLE_DATA
    assert a["transfer_id"] != b["transfer_id"]
    rows = controller_transfer_row(world)
    keys = [r["idempotency_key"] for r in rows]
    assert keys.count("shared-key-01") == 2


def test_push_rejects_responses_that_do_not_bind_to_request(world):
    """Sol review P2: response frames must echo request_id + method; a
    mismatched frame is a lost/corrupted exchange and must abort loudly."""
    put = publish_source(world)
    rid = world.remote_row["remote_id"]
    original_handler = world.transport.handler

    def unbound_handler(frame):
        resp = original_handler(frame)
        if resp.get("kind") == "response":
            # Valid frame shape, but bound to a DIFFERENT exchange.
            resp["request_id"] = "req_" + "0" * 32
        return resp

    world.transport.handler = unbound_handler
    with pytest.raises(ConnectionError, match="does not bind"):
        world.broker.push_blob(rid, put["version_id"], idempotency_key="push-key-0099")


def test_init_result_must_name_the_transfer(world):
    """Sol review P2: transfer_init answers must carry the transfer_id."""
    put = publish_source(world)
    rid = world.remote_row["remote_id"]
    original_handler = world.transport.handler

    def anonymous_init(frame):
        resp = original_handler(frame)
        if frame.get("method") == "artifact.transfer_init" and resp.get("kind") == "response":
            resp["result"].pop("transfer_id", None)
        return resp

    world.transport.handler = anonymous_init
    with pytest.raises(Exception, match="bind"):
        world.broker.push_blob(rid, put["version_id"], idempotency_key="push-key-0100")


def test_pull_resume_downloads_only_missing_bytes(world, tmp_path):
    """rc17 F5: pull resume starts from the controller's durable offset, not
    the remote's (always-zero) serve offset, and an inconsistent staging file
    resets BOTH to zero instead of extending a zero-filled prefix."""
    put = publish_source(world)
    pushed = world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                                    idempotency_key="push-key-0011")
    rid = world.remote_row["remote_id"]
    dest = tmp_path / "out" / "resume.bin"

    state = {"chunks": 0}

    def fail_on_second_chunk(frame):
        if frame["method"] == "artifact.blob_chunk":
            state["chunks"] += 1
            return state["chunks"] == 2
        return False

    world.transport.fail_once_on(fail_on_second_chunk)
    with pytest.raises(ConnectionError):
        world.broker.pull_blob(rid, pushed["version_id"], dest, idempotency_key="pull-key-0011")

    row = world.controller_ops.catalog.db.execute(
        "SELECT acked_offset FROM controller_transfers WHERE idempotency_key='pull-key-0011'"
    ).fetchone()
    assert row["acked_offset"] == 1024
    staging_dir = tmp_path / "controller-home" / "remote-pull-staging"
    parts = list(staging_dir.glob("*.part"))
    assert len(parts) == 1 and parts[0].stat().st_size >= 1024

    before = len([f for f in world.transport.frames if f["method"] == "artifact.blob_chunk"])
    result = world.broker.pull_blob(rid, pushed["version_id"], dest, idempotency_key="pull-key-0011")
    assert result["completed"] is True
    new_offsets = [f["payload"]["offset"] for f in world.transport.frames
                   if f["method"] == "artifact.blob_chunk"][before:]
    assert new_offsets and min(new_offsets) == 1024, (
        f"resume must start at the ledger offset 1024, got {new_offsets}"
    )
    assert dest.read_bytes() == SAMPLE_DATA


def test_pull_inconsistent_staging_resets_to_zero(world, tmp_path):
    put = publish_source(world)
    pushed = world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                                    idempotency_key="push-key-0012")
    rid = world.remote_row["remote_id"]
    dest = tmp_path / "out" / "reset.bin"

    state = {"chunks": 0}

    def fail_on_second_chunk(frame):
        if frame["method"] == "artifact.blob_chunk":
            state["chunks"] += 1
            return state["chunks"] == 2
        return False

    world.transport.fail_once_on(fail_on_second_chunk)
    with pytest.raises(ConnectionError):
        world.broker.pull_blob(rid, pushed["version_id"], dest, idempotency_key="pull-key-0012")
    row = world.controller_ops.catalog.db.execute(
        "SELECT acked_offset FROM controller_transfers WHERE idempotency_key='pull-key-0012'"
    ).fetchone()
    assert row["acked_offset"] == 1024
    # Corrupt the staging prefix: truncate BELOW the ledger offset.
    part = next((tmp_path / "controller-home" / "remote-pull-staging").glob("*.part"))
    with open(part, "r+b") as fh:
        fh.truncate(10)

    result = world.broker.pull_blob(rid, pushed["version_id"], dest, idempotency_key="pull-key-0012")
    assert result["completed"] is True
    offsets = [f["payload"]["offset"] for f in world.transport.frames
               if f["method"] == "artifact.blob_chunk"]
    assert min(offsets) == 0, "inconsistent staging must restart from zero"
    assert dest.read_bytes() == SAMPLE_DATA


def test_pull_staging_never_touches_destination_parent(world, tmp_path):
    """rc17 F4: pull staging lives in vanth-owned storage, not beside dest."""
    put = publish_source(world)
    pushed = world.broker.push_blob(world.remote_row["remote_id"], put["version_id"],
                                    idempotency_key="push-key-0013")
    dest = tmp_path / "elsewhere" / "model.bin"
    result = world.broker.pull_blob(world.remote_row["remote_id"], pushed["version_id"], dest,
                                    idempotency_key="pull-key-0013")
    assert result["completed"] is True
    assert list(dest.parent.glob(".*pulling*")) == []
    assert (tmp_path / "controller-home" / "remote-pull-staging").is_dir()


def test_completion_response_missing_fields_rejected(world, tmp_path):
    """rc17 P2: completion answers with MISSING epoch/size fields are a
    mismatch — never silently defaulted."""
    put = publish_source(world)
    rid = world.remote_row["remote_id"]
    original_handler = world.transport.handler

    def incomplete_complete(frame):
        resp = original_handler(frame)
        if frame.get("method") == "artifact.transfer_complete" and resp.get("kind") == "response":
            resp["result"].pop("state_epoch", None)
        return resp

    world.transport.handler = incomplete_complete
    with pytest.raises(Exception, match="missing"):
        world.broker.push_blob(rid, put["version_id"], idempotency_key="push-key-0014")


def test_unbound_error_frames_rejected(world, tmp_path):
    """rc17 P2: error frames must also bind to their request."""
    put = publish_source(world)
    rid = world.remote_row["remote_id"]
    original_handler = world.transport.handler

    def foreign_error(frame):
        if frame.get("method") == "artifact.blob_chunk":
            return {"version": "1", "kind": "error", "code": "INVALID_REQUEST",
                    "message": "boom", "request_id": "req_" + "1" * 32,
                    "method": "artifact.blob_chunk"}
        return original_handler(frame)

    world.transport.handler = foreign_error
    with pytest.raises(ConnectionError, match="does not bind"):
        world.broker.push_blob(rid, put["version_id"], idempotency_key="push-key-0015")
