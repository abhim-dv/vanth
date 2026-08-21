# Vanth remote protocol v1

**Status:** Phase 0 (contract and test harness)
**Transport:** newline-delimited JSON frames over the SSH forced-command
helper's stdin/stdout.
**Protocol version string:** `"1"` (must appear in every frame's `version`).

This document is the normative human-readable contract for the v1 remote
protocol. The machine-checkable companions are:

- `remote-protocol-v1.schema.json` — JSON Schema (draft 2020-12) covering every
  frame kind and the request methods.
- `remote-errors-v1.json` — the stable error registry (code → status, message).
- `remote-ddl-v1.sql` — the controller and remote-side sqlite DDL.
- `request-digest-vectors-v1.json` — golden canonicalization + digest vectors.

## 1. Framing

Each frame is exactly one JSON value serialized with **compact JSON** (no
whitespace) and terminated by a single `\n` (LF). A reader reads the transport
line by line and treats each line as one frame.

- A line longer than the maximum frame size (default **8 MiB**, including the
  trailing newline) is rejected **before** JSON parsing with
  `PROTOCOL_OVERSIZED`.
- After parsing, a frame whose compact serialization exceeds the maximum is
  also rejected with `PROTOCOL_OVERSIZED`.
- Malformed JSON, or a JSON value that is not an object, is rejected with
  `PROTOCOL_MALFORMED`.
- Any object with **duplicate keys** is rejected with `PROTOCOL_DUPLICATE_KEY`.
  This applies to the frame itself, its `payload`, and every nested object in
  a `snapshot`/`log_range` (any object decoded by the reference implementation).
- An unknown `kind` value is rejected with `PROTOCOL_UNKNOWN_KIND`.
- An unknown field anywhere in a frame is rejected with
  `PROTOCOL_UNKNOWN_FIELD` (Phase 0 requires strict unknown-field rejection).

All timestamps are ISO-8601 UTC with a `Z` suffix (e.g.
`2026-08-20T12:00:00.000000Z`), matching Vanth's `now_iso()`.

## 2. Frame kinds

Every frame carries `version` (`"1"`) and `sent_at` (ISO-8601). Per-kind
required fields are marked **bold**; all other listed fields are optional.

### 2.1 `hello`

Sent by the helper once at the start of a connection.

| Field | Type | Notes |
| --- | --- | --- |
| **version** | string | `"1"` |
| **kind** | string | `"hello"` |
| **protocol** | string | `"vanth.remote"` |
| agent | string | helper agent identifier |
| remote_id | string | persisted remote instance id |
| state_epoch | integer | current remote state epoch |
| sent_at | string | ISO-8601 UTC |

### 2.2 `request`

A mutation or action request from the controller to the remote.

| Field | Type | Notes |
| --- | --- | --- |
| **version** | string | `"1"` |
| **kind** | string | `"request"` |
| request_id | string | controller request id (`req_…`) |
| **idempotency_key** | string | `[A-Za-z0-9_-]{8,128}`, caller-supplied |
| **method** | string | one of `job.start`, `job.stop`, `job.rerun`, `job.status` |
| **payload** | object | method payload (see §4) |
| **digest** | string | SHA-256 of the canonical request triple (see §3) |
| sent_at | string | ISO-8601 UTC |

Unknown `method` values are rejected with `UNSUPPORTED_FEATURE`.

### 2.3 `response`

Success response to a request.

| Field | Type | Notes |
| --- | --- | --- |
| **version** | string | `"1"` |
| **kind** | string | `"response"` |
| request_id | string | echoes the request's `request_id` |
| **method** | string | echoes the request's method |
| result | object | method result |
| sent_at | string | ISO-8601 UTC |

### 2.4 `error`

Failure response to a request, or a connection-level failure.

| Field | Type | Notes |
| --- | --- | --- |
| **version** | string | `"1"` |
| **kind** | string | `"error"` |
| request_id | string | echoes the request's `request_id` when present |
| method | string | echoes the request's method when present |
| **code** | string | a code from the error registry (`remote-errors-v1.json`) |
| **message** | string | human-readable detail |
| sent_at | string | ISO-8601 UTC |

### 2.5 `snapshot`

Consistent state snapshot for recovery (Phase 3). Phase 0 defines the shape
only.

| Field | Type | Notes |
| --- | --- | --- |
| **version** | string | `"1"` |
| **kind** | string | `"snapshot"` |
| **state_epoch** | integer | remote state epoch this snapshot is fixed to |
| **cursor** | object | feed/pagination cursor (Phase 3) |
| **jobs** | array | job records |
| events | array | event records |
| has_more | boolean | more pages remain |
| sent_at | string | ISO-8601 UTC |

### 2.6 `log_range`

Byte-range stdout/stderr read (Phase 3). Phase 0 defines the shape only.

| Field | Type | Notes |
| --- | --- | --- |
| **version** | string | `"1"` |
| **kind** | string | `"log_range"` |
| remote_job_id | string | remote job id |
| **stream** | string | `"stdout"` or `"stderr"` |
| **offset** | integer | byte offset |
| size | integer | total stream size |
| **content** | string | base64-encoded bytes |
| truncated | boolean | response exceeded the requested range |
| sent_at | string | ISO-8601 UTC |

## 3. Canonicalization and request digests

Every `request` carries a `digest` computed over the canonical serialization of
the triple `{method, payload, idempotency_key}`:

```
digest = sha256hex( canonical_json( {"method": method, "payload": payload,
                                     "idempotency_key": idempotency_key} ) )
```

The receiver re-computes the digest. A mismatch is rejected with
`PROTOCOL_REPLAY_MISMATCH`. The digest also feeds replay protection in the
durable stores (§7).

`canonical_json` implements **RFC 8785** (JSON Canonicalization Scheme):

- Keys of every object are sorted by UTF-16 code-unit order
  (`codePointAt(0)` ordering).
- Numbers use shortest round-trip ES6 formatting: `1.0` → `1`,
  `0.5` → `0.5`, `1e3` → `1000`, `1e16` → `10000000000000000`,
  `1e21` → `1E+21`, `1.5e-8` → `1.5E-8`. `NaN` and `Infinity` are rejected.
  The exponent marker is a single **uppercase** `E` followed by an explicit
  sign (this matches RFC 8785's worked examples; ECMAScript's own
  `Number.prototype.toString` lowercases it — the two forms are
  interchangeable for parsing, and this spec pins the uppercase form).
- Strings use minimal escaping: only `"`, `\`, and control characters
  `< 0x20` are escaped; control characters are always emitted as lowercase
  `\uXXXX` (four hex digits, no `\n`/`\t` shorthand). Non-ASCII printable
  characters are emitted verbatim (UTF-8 bytes).
- No whitespace is emitted.
- Duplicate keys anywhere in the serialized structure are rejected.

Golden vectors for canonical output and digests live in
`request-digest-vectors-v1.json`; every implementation must reproduce them
byte-for-byte.

## 4. Request methods

Every `request` frame carries a caller-supplied `idempotency_key` at the frame
level (required; `[A-Za-z0-9_-]{8,128}`) — it is **not** part of the payload.
The request digest covers the triple `{method, payload, idempotency_key}`.
Field semantics reuse Vanth's local `JobManager.start` / `stop` / `rerun` /
`status` (`src/vanth/server.py`).

### 4.1 `job.start`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| command | string | yes | non-empty command to run |
| cwd | string | no | working directory |
| name | string | no | job name |
| env | object | no | string → string environment overrides |
| timeout_seconds | integer | no | `>= 1` |
| notify_on | array | no | array of strings |
| wake_targets | array | no | array of objects (Vanth wake target schema) |
| origin_thread_id | string | no | originating thread id |
| tags | array | no | array of strings |
| notes | string | no | free-form notes |
| interactive | boolean | no | interactive run flag |
| trigger | object | no | `{job_id, status}` trigger (Vanth semantics) |

No field other than those above is allowed.

### 4.2 `job.stop`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| job_id | string | yes | job to stop |
| signal | string | no | `"terminate"` (default) or `"kill"` |
| kill_after_seconds | integer | no | default `10`, `>= 1` |

### 4.3 `job.rerun`

`job_id` is required. All other fields are optional overrides of the original
run spec: `command`, `env`, `timeout_seconds` (`>= 1`), `name`, `tags`,
`notes`, `cwd`, `interactive`.

### 4.4 `job.status`

A read of a remote job's current status.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| job_id | string | yes | remote job id to query |

No field other than `job_id` is allowed.

### 4.5 `job.snapshot`

A paginated snapshot of the remote's jobs and events, fixed to one remote
state epoch and a high-water event point. Reads travel in a standard
`response` frame's `result`; the result carries `"kind": "snapshot"` plus:

| Field | Type | Notes |
| --- | --- | --- |
| kind | string | `"snapshot"` |
| state_epoch | integer | the remote's state epoch for this page |
| cursor | object | `{offset, high_water}` — pass back for the next page |
| jobs | array | minimal job rows (job_id, status, name, command, created_at, updated_at, exit_code) |
| events | array | bounded event tail for this page's jobs above `high_water` |
| has_more | boolean | true when another page follows |

Request payload: `cursor` (optional object) selects the page. A complete
snapshot application repairs missed updates and remote deletions without
merging epochs; old-epoch shadows are retained only for audit, and suppressed
(forgotten) shadows are never resurrected.

### 4.6 `job.log_range`

An exact byte-range read of a remote job's stdout/stderr log. The response
result carries `"kind": "log_range"` plus:

| Field | Type | Notes |
| --- | --- | --- |
| kind | string | `"log_range"` |
| remote_job_id | string | job whose log to read |
| stream | string | `"stdout"` (default) or `"stderr"` |
| offset | integer | byte offset to start from (`>= 0`) |
| size | integer | full log file size in bytes |
| content | string | base64 of exactly the requested bytes; arbitrary bytes round-trip |
| truncated | boolean | true when the window was clipped to the file size |

Request payload: `remote_job_id` (required), `stream`, `offset`, `size`
(`size` capped at half the maximum frame size).

### 4.7 `job.feed`

One bounded batch of the remote's durable change-feed outbox (Phase 4). The
remote records every controller-relevant change — job upserts and job
tombstones — as append-only rows pinned to `(state_epoch, feed_epoch, seq)`.
The response result carries `"kind": "feed"` plus:

| Field | Type | Notes |
| --- | --- | --- |
| kind | string | `"feed"` |
| state_epoch | integer | remote's current timeline version |
| feed_epoch | integer | bumped on every restore (alongside `state_epoch`) |
| cursor | object | `{state_epoch, feed_epoch, seq}` — pass back for the next batch |
| changes | array | `{seq, kind, job_id, payload, created_at}` rows strictly after the cursor's seq within the current epochs |
| has_more | boolean | true when more rows follow this batch |
| oldest_seq | integer | oldest retained seq (gap detection) |
| high_water_seq | integer | newest seq overall (cursor reset after recovery) |

Request payload: `cursor` (optional object), `limit` (default 100, max 500),
`wait_ms` (bounded long-poll, default 0, max 10000) — when no rows are
available the remote polls up to that duration before returning an empty
batch. Controllers advance their stored cursor only after transactionally
applying a whole batch. A cursor whose epochs disagree with the response
(remote database restore bumps both epochs), or that predates retained
history (`seq + 1 < oldest_seq` once compaction lands), is gapped: the
controller recovers through a Phase 3 full snapshot and resets its feed
cursor to `high_water_seq`.

## 5. Error codes

The full registry is `remote-errors-v1.json`. Codes used by this document:

| Code | Status | Meaning |
| --- | --- | --- |
| `PROTOCOL_MALFORMED` | 400 | frame is not valid JSON or violates the shape contract |
| `PROTOCOL_OVERSIZED` | 413 | frame exceeds the maximum allowed size |
| `PROTOCOL_UNKNOWN_KIND` | 400 | frame has an unknown `kind` |
| `PROTOCOL_DUPLICATE_KEY` | 400 | object contains a duplicate key |
| `PROTOCOL_UNKNOWN_FIELD` | 400 | object contains an unknown field |
| `PROTOCOL_REPLAY_MISMATCH` | 409 | idempotency key reused with a different request |
| `UNSUPPORTED_FEATURE` | 501 | requested feature is not supported |
| `AUTH_FAILED` | 401 | authentication failed |
| `INVALID_REQUEST` | 422 | request payload is invalid |

## 6. State machines

### 6.1 Pairing (`remotes.state`)

```
unpaired -> pairing -> paired
             \-> error
```

### 6.2 Remote request (`remote_requests.status`)

```
creating -> submitting -> accepted -> completed
                          |-> failed
                          |-> lost
```

### 6.3 Remote operation (`remote_operations.status`)

```
accepted -> queued -> launched -> running -> completed
                                  |-> failed
```

### 6.4 Queued launch

```
queued -> launched
```

Invalid transitions are rejected with `ValueError` (and surface as
`INVALID_REQUEST` at the protocol boundary).

## 7. Idempotency and crash safety

- **Controller.** A request is recorded by `(remote_id, idempotency_key)` in
  `remote_requests` with its digest, inside one transaction. Replaying the same
  key with the same digest returns the original request (and never starts a
  second job); replaying the same key with a **different** digest is rejected
  with `PROTOCOL_REPLAY_MISMATCH`.
- **Remote.** An operation is recorded by `idempotency_key` in
  `remote_operations` with its digest, inside one transaction. A lost response
  replays the accepted operation; a queued-but-not-launched job stays `queued`
  and can be launched later.
- **Tombstones.** Both sides record replay tombstones
  (`remote_replay_tombstones`) for keyed mutations so replay identity survives
  ordinary cleanup.

See `remote-ddl-v1.sql` for the DDL and `tests/remote/` for the five executable
crash cases.
