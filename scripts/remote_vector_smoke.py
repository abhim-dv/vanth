#!/usr/bin/env python
"""Wheel smoke check for the Phase 0 remote protocol artifacts.

Loads ``remote-protocol-v1.schema.json`` and ``request-digest-vectors-v1.json``
from the installed ``vanth`` package data (falling back to the repo
``docs/spec`` directory) and prints PASS/FAIL using only the standard library.

``jsonschema`` is intentionally not used; the schema is structurally checked
(loadable JSON, draft keyword, every frame kind + request method present) and
the canonical/digest vectors are checked against the package's own
``canonical_json`` / ``request_digest`` implementations.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

EXPECTED_KINDS = ["hello", "request", "response", "error", "snapshot", "log_range"]
EXPECTED_METHODS = ["job.start", "job.stop", "job.rerun", "job.status", "job.snapshot", "job.log_range", "job.feed",
                    "artifact.transfer_init", "artifact.blob_chunk", "artifact.transfer_complete"]
EXPECTED_CODES = [
    "PROTOCOL_MALFORMED",
    "PROTOCOL_OVERSIZED",
    "PROTOCOL_UNKNOWN_KIND",
    "PROTOCOL_DUPLICATE_KEY",
    "PROTOCOL_UNKNOWN_FIELD",
    "PROTOCOL_REPLAY_MISMATCH",
    "UNSUPPORTED_FEATURE",
    "AUTH_FAILED",
    "INVALID_REQUEST",
]

ROOT = Path(__file__).resolve().parent.parent
SPEC_DIR = ROOT / "docs" / "spec"


def load_spec(name: str) -> dict:
    from importlib.resources import files

    try:
        data = files("vanth").joinpath("remote", "spec", name).read_bytes()
        return json.loads(data)
    except (ModuleNotFoundError, FileNotFoundError, KeyError):
        path = SPEC_DIR / name
        return json.loads(path.read_text(encoding="utf-8"))


def run() -> int:
    from vanth.remote.protocol import VanthRemoteProtocolError, canonical_json, request_digest

    failures: list[str] = []

    schema = load_spec("remote-protocol-v1.schema.json")
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        failures.append("schema is not draft 2020-12")
    kind_enum = schema.get("properties", {}).get("kind", {}).get("enum", [])
    missing_kinds = [k for k in EXPECTED_KINDS if k not in kind_enum]
    if missing_kinds:
        failures.append(f"schema missing frame kinds: {missing_kinds}")
    method_enum = schema.get("properties", {}).get("method", {}).get("enum", [])
    missing_methods = [m for m in EXPECTED_METHODS if m not in method_enum]
    if missing_methods:
        failures.append(f"schema missing request methods: {missing_methods}")
    payload_defs = list(schema.get("$defs", {}).keys())
    missing_defs = [
        name for name in ("payload_job.start", "payload_job.stop", "payload_job.rerun")
        if name not in payload_defs
    ]
    if missing_defs:
        failures.append(f"schema missing payload defs: {missing_defs}")

    vectors = load_spec("request-digest-vectors-v1.json")
    if not isinstance(vectors.get("vectors"), list) or not vectors["vectors"]:
        failures.append("vectors file has no vectors")
    for vector in vectors.get("vectors", []):
        try:
            canonical = canonical_json(
                {
                    "method": vector["method"],
                    "payload": vector["payload"],
                    "idempotency_key": vector["idempotency_key"],
                }
            )
        except VanthRemoteProtocolError as exc:
            failures.append(f"vector {vector.get('id')}: canonicalization failed: {exc}")
            continue
        digest = request_digest(
            vector["method"], vector["payload"], vector["idempotency_key"]
        )
        if canonical != vector["canonical"]:
            failures.append(
                f"vector {vector.get('id')}: canonical mismatch:\n  got  {canonical}\n  want {vector['canonical']}"
            )
        if digest != vector["digest_sha256"]:
            failures.append(
                f"vector {vector.get('id')}: digest mismatch:\n  got  {digest}\n  want {vector['digest_sha256']}"
            )

    for rejected in vectors.get("rejected_cases", []):
        if "payload" not in rejected:
            continue
        try:
            canonical_json(rejected["input"])
            failures.append(f"rejected case {rejected.get('id')}: expected PROTOCOL_DUPLICATE_KEY, got success")
        except VanthRemoteProtocolError as exc:
            if exc.code != "PROTOCOL_DUPLICATE_KEY":
                failures.append(
                    f"rejected case {rejected.get('id')}: got {exc.code}, want PROTOCOL_DUPLICATE_KEY"
                )

    if failures:
        print("FAIL")
        for failure in failures:
            print("  -", failure)
        return 1
    print(
        f"PASS: schema({len(EXPECTED_KINDS)} kinds, {len(EXPECTED_METHODS)} methods) "
        f"vectors({len(vectors.get('vectors', []))})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(run())
