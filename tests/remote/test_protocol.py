"""Protocol codec, canonical JSON, digest, and validation tests (Phase 0)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from vanth.remote.protocol import (
    ERROR_REGISTRY,
    FRAME_KINDS,
    VanthRemoteProtocolError,
    canonical_json,
    decode_frame,
    encode_frame,
    request_digest,
    validate_frame,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
VECTORS_PATH = REPO_ROOT / "docs" / "spec" / "request-digest-vectors-v1.json"


def test_encode_decode_round_trip_every_kind():
    start_payload = {"command": "echo hi"}
    frames = [
        {"version": "1", "kind": "hello", "protocol": "vanth.remote", "sent_at": "2026-08-20T12:00:00Z"},
        {
            "version": "1", "kind": "request", "request_id": "req_" + "0" * 32,
            "idempotency_key": "key-1234-abcd", "method": "job.start",
            "payload": start_payload,
            "digest": request_digest("job.start", start_payload, "key-1234-abcd"),
            "sent_at": "2026-08-20T12:00:00Z",
        },
        {
            "version": "1", "kind": "response", "request_id": "req_" + "0" * 32,
            "method": "job.start", "result": {"job_id": "job_x"}, "sent_at": "2026-08-20T12:00:00Z",
        },
        {
            "version": "1", "kind": "error", "request_id": "req_" + "0" * 32,
            "method": "job.start", "code": "INVALID_REQUEST", "message": "nope",
            "sent_at": "2026-08-20T12:00:00Z",
        },
        {
            "version": "1", "kind": "snapshot", "state_epoch": 1, "cursor": {"seq": 0},
            "jobs": [{"job_id": "job_x"}], "sent_at": "2026-08-20T12:00:00Z",
        },
        {
            "version": "1", "kind": "log_range", "remote_job_id": "job_x", "stream": "stdout",
            "offset": 0, "size": 3, "content": "YWJj", "sent_at": "2026-08-20T12:00:00Z",
        },
    ]
    for frame in frames:
        line = encode_frame(frame).decode("utf-8").rstrip("\n")
        assert decode_frame(line) == frame


def test_decode_rejects_after_digest_validation():
    frame = {
        "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
        "method": "job.start", "payload": {"command": "echo hi"},
        "digest": "0" * 64, "sent_at": "2026-08-20T12:00:00Z",
    }
    line = encode_frame(frame).decode("utf-8").rstrip("\n")
    with pytest.raises(VanthRemoteProtocolError) as exc:
        decode_frame(line)
    assert exc.value.code == "PROTOCOL_REPLAY_MISMATCH"


def test_malformed_json():
    with pytest.raises(VanthRemoteProtocolError) as exc:
        decode_frame("{not json")
    assert exc.value.code == "PROTOCOL_MALFORMED"


def test_non_object_frame():
    with pytest.raises(VanthRemoteProtocolError) as exc:
        decode_frame('["hello", "world"]')
    assert exc.value.code == "PROTOCOL_MALFORMED"


def test_oversized_frame():
    with pytest.raises(VanthRemoteProtocolError) as exc:
        decode_frame('{"kind": "hello", "pad": "' + "x" * 100 + '"}', max_bytes=50)
    assert exc.value.code == "PROTOCOL_OVERSIZED"
    with pytest.raises(VanthRemoteProtocolError) as exc:
        encode_frame({"pad": "x" * 100}, max_bytes=50)
    assert exc.value.code == "PROTOCOL_OVERSIZED"


def test_unknown_kind():
    with pytest.raises(VanthRemoteProtocolError) as exc:
        decode_frame('{"version": "1", "kind": "bogus"}')
    assert exc.value.code == "PROTOCOL_UNKNOWN_KIND"


def test_duplicate_keys():
    with pytest.raises(VanthRemoteProtocolError) as exc:
        decode_frame('{"kind": "hello", "kind": "hello"}')
    assert exc.value.code == "PROTOCOL_DUPLICATE_KEY"
    line = (
        '{"version":"1","kind":"request","idempotency_key":"key-1234-abcd",'
        '"method":"job.start","payload":{"command":"x","command":"y"},'
        '"digest":"0000000000000000000000000000000000000000000000000000000000000000",'
        '"sent_at":"2026-08-20T12:00:00Z"}'
    )
    with pytest.raises(VanthRemoteProtocolError) as exc:
        decode_frame(line)
    assert exc.value.code == "PROTOCOL_DUPLICATE_KEY"


def test_unknown_field_frame():
    frame = {"version": "1", "kind": "hello", "protocol": "vanth.remote", "wat": 1}
    with pytest.raises(VanthRemoteProtocolError) as exc:
        validate_frame(frame)
    assert exc.value.code == "PROTOCOL_UNKNOWN_FIELD"


def test_unknown_field_payload():
    payload = {"command": "echo hi", "bogus": 1}
    frame = {
        "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
        "method": "job.start", "payload": payload,
        "digest": request_digest("job.start", payload, "key-1234-abcd"),
        "sent_at": "2026-08-20T12:00:00Z",
    }
    with pytest.raises(VanthRemoteProtocolError) as exc:
        validate_frame(frame)
    assert exc.value.code == "PROTOCOL_UNKNOWN_FIELD"


def test_unsupported_method():
    payload = {"command": "echo hi"}
    frame = {
        "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
        "method": "job.frobnicate", "payload": payload,
        "digest": request_digest("job.frobnicate", payload, "key-1234-abcd"),
        "sent_at": "2026-08-20T12:00:00Z",
    }
    with pytest.raises(VanthRemoteProtocolError) as exc:
        validate_frame(frame)
    assert exc.value.code == "UNSUPPORTED_FEATURE"


def test_invalid_idempotency_key_rejected():
    for bad_key in ["short", "has space in it", "has/slash", "x" * 129]:
        payload = {"command": "echo hi"}
        frame = {
            "version": "1", "kind": "request", "idempotency_key": bad_key,
            "method": "job.start", "payload": payload,
            "digest": request_digest("job.start", payload, bad_key),
            "sent_at": "2026-08-20T12:00:00Z",
        }
        with pytest.raises(VanthRemoteProtocolError) as exc:
            validate_frame(frame)
        assert exc.value.code == "INVALID_REQUEST"


def test_invalid_start_payloads():
    cases = [
        {},  # missing command
        {"command": ""},  # empty command
        {"command": "x", "timeout_seconds": 0},
        {"command": "x", "timeout_seconds": True},
        {"command": "x", "env": {"A": 1}},
        {"command": "x", "tags": [1]},
        {"command": "x", "wake_targets": [1]},
        {"command": "x", "interactive": "yes"},
        {"command": "x", "trigger": 5},
    ]
    for payload in cases:
        frame = {
            "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
            "method": "job.start", "payload": payload,
            "digest": request_digest("job.start", payload, "key-1234-abcd"),
            "sent_at": "2026-08-20T12:00:00Z",
        }
        with pytest.raises(VanthRemoteProtocolError) as exc:
            validate_frame(frame)
        assert exc.value.code == "INVALID_REQUEST"


def test_invalid_stop_and_rerun_payloads():
    bad_stops = [
        {},
        {"job_id": "job_x", "signal": "SIGKILL"},
        {"job_id": "job_x", "kill_after_seconds": 0},
    ]
    for payload in bad_stops:
        frame = {
            "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
            "method": "job.stop", "payload": payload,
            "digest": request_digest("job.stop", payload, "key-1234-abcd"),
            "sent_at": "2026-08-20T12:00:00Z",
        }
        with pytest.raises(VanthRemoteProtocolError) as exc:
            validate_frame(frame)
        assert exc.value.code == "INVALID_REQUEST"

    unknown_field_payload = {"job_id": "job_x", "extra": 1}
    unknown_frame = {
        "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
        "method": "job.stop", "payload": unknown_field_payload,
        "digest": request_digest("job.stop", unknown_field_payload, "key-1234-abcd"),
        "sent_at": "2026-08-20T12:00:00Z",
    }
    with pytest.raises(VanthRemoteProtocolError) as exc:
        validate_frame(unknown_frame)
    assert exc.value.code == "PROTOCOL_UNKNOWN_FIELD"

    bad_rerun = {"job_id": "job_x", "bogus": 1}
    frame = {
        "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
        "method": "job.rerun", "payload": bad_rerun,
        "digest": request_digest("job.rerun", bad_rerun, "key-1234-abcd"),
        "sent_at": "2026-08-20T12:00:00Z",
    }
    with pytest.raises(VanthRemoteProtocolError) as exc:
        validate_frame(frame)
    assert exc.value.code == "PROTOCOL_UNKNOWN_FIELD"


def test_valid_frames_pass_validation():
    payload = {
        "command": "echo hi",
        "env": {"A": "1"},
        "timeout_seconds": 5,
        "notify_on": ["checkpoint"],
        "wake_targets": [{"type": "local_command", "command": ["echo", "hi"], "events": ["checkpoint"]}],
        "tags": ["a"],
        "notes": "n",
        "interactive": False,
    }
    frame = {
        "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
        "method": "job.start", "payload": payload,
        "digest": request_digest("job.start", payload, "key-1234-abcd"),
        "sent_at": "2026-08-20T12:00:00Z",
    }
    assert validate_frame(frame) == frame


def test_valid_stop_default_signal():
    payload = {"job_id": "job_x"}
    frame = {
        "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
        "method": "job.stop", "payload": payload,
        "digest": request_digest("job.stop", payload, "key-1234-abcd"),
        "sent_at": "2026-08-20T12:00:00Z",
    }
    assert validate_frame(frame) == frame


def test_job_status_is_a_valid_method():
    payload = {"job_id": "job_x"}
    frame = {
        "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
        "method": "job.status", "payload": payload,
        "digest": request_digest("job.status", payload, "key-1234-abcd"),
        "sent_at": "2026-08-20T12:00:00Z",
    }
    assert validate_frame(frame) == frame


def test_job_status_requires_job_id():
    frame = {
        "version": "1", "kind": "request", "idempotency_key": "key-1234-abcd",
        "method": "job.status", "payload": {},
        "digest": request_digest("job.status", {}, "key-1234-abcd"),
        "sent_at": "2026-08-20T12:00:00Z",
    }
    with pytest.raises(VanthRemoteProtocolError) as exc:
        validate_frame(frame)
    assert exc.value.code == "INVALID_REQUEST"


def test_canonical_json_golden_vectors():
    data = json.loads(VECTORS_PATH.read_text(encoding="utf-8"))
    for vector in data["vectors"]:
        canonical = canonical_json(
            {"method": vector["method"], "payload": vector["payload"], "idempotency_key": vector["idempotency_key"]}
        )
        assert canonical == vector["canonical"], vector["id"]
        assert request_digest(vector["method"], vector["payload"], vector["idempotency_key"]) == vector["digest_sha256"]


def test_canonical_json_number_formatting():
    assert canonical_json({"v": 1.0}) == '{"v":1}'
    assert canonical_json({"v": 0.5}) == '{"v":0.5}'
    assert canonical_json({"v": 1e3}) == '{"v":1000}'
    assert canonical_json({"v": 1e16}) == '{"v":10000000000000000}'
    assert canonical_json({"v": 1e21}) == '{"v":1E+21}'
    assert canonical_json({"v": 1.5e-8}) == '{"v":1.5E-8}'
    assert canonical_json({"v": 1e-6}) == '{"v":0.000001}'
    assert canonical_json({"v": -1.0}) == '{"v":-1}'


def test_canonical_json_string_escaping():
    assert canonical_json({"s": 'a\n"b\\c\u0001d'}) == r'{"s":"a\u000a\"b\\c\u0001d"}'
    assert canonical_json({"s": "\t"}) == r'{"s":"\u0009"}'
    assert canonical_json({"s": "x\u007f\u0080"}) == '{"s":"x\u007f\u0080"}'


def test_canonical_json_rejects_non_finite():
    for value in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(VanthRemoteProtocolError) as exc:
            canonical_json({"v": value})
        assert exc.value.code == "PROTOCOL_MALFORMED"


def test_canonical_json_key_sorting_utf16():
    assert canonical_json({"z": 1, "a": 2}) == '{"a":2,"z":1}'
    assert canonical_json({"\ud834\udd1e": 1, "z": 2}) == '{"z":2,"\ud834\udd1e":1}'


def test_error_registry_shape():
    for code, (status, message) in ERROR_REGISTRY.items():
        assert isinstance(status, int)
        assert isinstance(message, str)


def test_all_registry_codes_reachable():
    for code in ERROR_REGISTRY:
        assert VanthRemoteProtocolError(code).code == code
