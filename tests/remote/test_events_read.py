"""Bounded remote job.events reads."""

from __future__ import annotations

import pytest

from vanth.remote.protocol import (
    EVENTS_MAX_JOBS,
    EVENTS_MAX_LIMIT,
    VanthRemoteProtocolError,
    validate_request,
)

from test_feed import make_world  # noqa: E402


def test_events_request_validation(tmp_path):
    validate_request("job.events", {"cursors": {"job_x": None}})
    validate_request("job.events", {"cursors": {"job_x": 0}, "limit": EVENTS_MAX_LIMIT})
    bad = [
        {},
        {"cursors": {}},
        {"cursors": {str(i): 0 for i in range(EVENTS_MAX_JOBS + 1)}},
        {"cursors": {"job_x": True}},
        {"cursors": {"job_x": -1}},
        {"cursors": {"job_x": "1"}},
        {"cursors": {"job_x": 0}, "limit": EVENTS_MAX_LIMIT + 1},
    ]
    for payload in bad:
        with pytest.raises(VanthRemoteProtocolError):
            validate_request("job.events", payload)


def test_events_handler_paginates_and_initializes(tmp_path):
    _, _, remote, _, _, jobs_db = make_world(tmp_path)
    rows = [
        ("e1", "job_x", 1, "checkpoint", "info", "one", "{}", "runner", "t1"),
        ("e2", "job_x", 2, "progress", "info", "two", "{}", "runner", "t2"),
        ("e3", "job_x", 3, "metric", "info", "three", "{}", "runner", "t3"),
    ]
    jobs_db.executemany("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?)", rows)
    jobs_db.commit()
    first = remote.handle_events_request({"payload": {"cursors": {"job_x": 0}, "limit": 2}})["result"]
    page = first["jobs"]["job_x"]
    assert [event["seq"] for event in page["events"]] == [1, 2]
    assert page["next_seq"] == 2 and page["high_water_seq"] == 3 and page["has_more"]
    initialized = remote.handle_events_request({"payload": {"cursors": {"job_x": None}}})["result"]
    assert initialized["jobs"]["job_x"] == {
        "events": [], "next_seq": 3, "high_water_seq": 3, "has_more": False,
    }


def test_events_controller_round_trip(tmp_path):
    _, _, remote, control, row, jobs_db = make_world(tmp_path)
    jobs_db.execute(
        "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?)",
        ("e1", "job_x", 1, "checkpoint", "info", "one", "{}", "runner", "t1"),
    )
    jobs_db.commit()
    result = control.events(row["remote_id"], {"job_x": 0}, limit=10)
    assert result["kind"] == "events"
    assert result["jobs"]["job_x"]["events"][0]["seq"] == 1


def test_events_controller_chunks_over_max_jobs(tmp_path):
    """More bindings than the per-request job cap must be chunked, not rejected
    (an unchunked call would fail validation and disable all wakes on the host)."""
    _, _, remote, control, row, jobs_db = make_world(tmp_path)
    cursors = {f"job_{i}": None for i in range(EVENTS_MAX_JOBS + 1)}
    result = control.events(row["remote_id"], cursors)
    assert len(result["jobs"]) == EVENTS_MAX_JOBS + 1


def test_events_preserves_remote_error_code(tmp_path, monkeypatch):
    """The store persists the error as the string "<code>: <message>"; the code
    must survive so the daemon's UNSUPPORTED_FEATURE capability gate can fire."""
    _, _, _, control, row, _ = make_world(tmp_path)

    def fake_submit(remote_id, method, payload, *, idempotency_key, **kwargs):
        return {"status": "failed", "error": "UNSUPPORTED_FEATURE: job.events not supported", "response": None}

    monkeypatch.setattr(control, "submit", fake_submit)
    with pytest.raises(VanthRemoteProtocolError) as exc:
        control.events(row["remote_id"], {"job_x": 0})
    assert exc.value.code == "UNSUPPORTED_FEATURE"


def test_schema_method_enum_covers_every_valid_method():
    """The packed machine schema must list every implemented request method, or it
    rejects a valid v1 request."""
    import json
    from pathlib import Path

    from vanth.remote.protocol import VALID_REQUEST_METHODS

    schema_path = Path(__file__).resolve().parents[2] / "docs" / "spec" / "remote-protocol-v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert set(VALID_REQUEST_METHODS) <= set(schema["properties"]["method"]["enum"])
