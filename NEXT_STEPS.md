# Vanth Next Steps

The production-candidate execution plan is in `V1_IMPLEMENTATION_PLAN.md`.

The staged native Go port, Python/uv compatibility, release gates, and ntcharts
terminal monitor implementation handoff are in `GO_PORT_IMPLEMENTATION_PLAN.md`.

This handoff starts from commit `f50e907 Add durable runner agent view and delivery retries`.

Current status: v1 production-candidate work is in progress. Vanth now has ordered SQLite migrations with WAL/busy-timeout policy and backups, authenticated loopback HTTP, single-daemon locking, bounded rotating diagnostics, delivery leases and attempt history, delivery idempotency IDs, runner heartbeats, process groups, bounded stream logs, manual cleanup, and graceful signal shutdown.

## Validation Baseline

These pass in the current working tree:

```cmd
uv run pytest -q
```

```cmd
uv run python -m compileall -q src tests examples
```

```cmd
uv build
```

The current suite reports 72 passing tests, with one Linux-only process-tree
test skipped on Windows. Manual daemon smokes also passed for:

- HTTP job start -> checkpoint wait -> local wake delivery -> `job_view` -> `job_doctor`
- HTTP job start -> checkpoint -> fake Codex app-server wake delivery, verifying `initialize -> thread/resume -> turn/start`

The automated suite also covers OpenCode wake delivery through a fake CLI,
verifying `run --session <id> --format json <prompt>`. A live model/session smoke
still depends on the user's OpenCode provider configuration.

Synthetic hardening coverage includes malformed and oversized HTTP/event input,
runner startup failure and disappearance, recovery/completion races, stop after
restart, automatic retry across restart, duplicate retry dispatch, a burst of
20 concurrent jobs emitting 2,000 progress events without loss, and a
cross-process event race asserting unique sequence numbers with no loss.

The heavy release-gate matrix is automated in `scripts/chaos_matrix.py`:

```cmd
uv run python scripts/chaos_matrix.py
```

It runs 50 concurrent jobs emitting 500 events per stream (25,000 durable rows
with exact counts and unique seq), a slow-adapter non-blocking check, repeated
daemon kill/restart cycles with delivery lease recovery, runner kills at
execution and terminal-persistence phases, a malformed-input battery against a
live daemon, and log-cap/cleanup idempotence. It passed on this machine.

While running the matrix, two cross-process event-path bugs were found and
fixed in `src/vanth/server.py` and `src/vanth/runner.py`:

- `AGENT_EVENT` sequence numbers are now allocated inside a `BEGIN IMMEDIATE`
  transaction, so concurrent runner/daemon processes cannot allocate the same
  per-job `seq`.
- the per-event SQLite write lock is no longer held during the JSONL mirror
  append, transient `database is locked` is retried (events, workload PID
  publication, heartbeats, and terminal transitions), and a reader thread
  survives a single failed persist instead of dying and losing the stream.
- the SQLite `busy_timeout` is configurable via `VANTH_BUSY_TIMEOUT_MS`
  (default 30000) so a heavy multi-process burst does not exhaust the old
  5000 ms default.

## Remaining release work

The v1 release gates are complete on Windows (chaos matrix, wheel smoke, deploy
template, adapter version recording). Linux CI, a clean-machine service install,
and the opt-in live Codex/OpenCode wakes still require their native environments
or user configuration. The changelog and a `1.0.0` version bump are staged but
not published. Publishing, tagging, and pushing still require explicit approval.

## Go port status

Phase 1 (read-only terminal monitor) is implemented and committed (`9bf3e7f`).
The module is at the repository root (`go.mod`, `go 1.25.0`), the Bubble Tea v2
/ Lip Gloss v2 / ntcharts v2 compile spike passed, and `cmd/vanth` supports
`--version [--json]` and `monitor`. The monitor is a W&B-LEET-style dashboard:
a summary header (status counts, needs-attention triage, job/event totals,
mode/refresh badges), a jobs sidebar (3-line blocks with progress bars), a
key-metrics panel, per-plot bordered tracking charts, and an event-table lower
pane by default. `internal/config` (home resolution) and `internal/state`
(schema-v5 typed queries) are implemented and tested. The design-blocker
cross-language conformance tests pass in both directions:

- Go opens and reads the Python-created schema-v5 fixture
  (`testdata/state/jobs.sqlite`, checked in).
- Go writes rows that Python's `sqlite3` verifies
  (`scripts/verify_go_write.py`).

The CI workflow runs `gofmt`, `go vet`, `go test`, `go build`, fixture
regeneration + semantic check, and the Python wheel smoke on Windows and Linux.

## Dogfood validation on this machine (done)

The Python v1 is wired into opencode as a local MCP server
(`~/.config/opencode/opencode.json`, `mcp.vanth`). Verified end to end:

- opencode connects to `vanth` MCP and discovers all 14 tools
  (`job_start`, `job_wait`, `job_tail`, `job_view`, `job_doctor`, ...).
- A full agent-style loop succeeded: start job -> wait progress -> wait
  checkpoint -> status -> tail -> view -> doctor -> delivery dispatch.
- The durable home (`%USERPROFILE%\.vanth`) persisted the job and delivery
  across a daemon restart (job `completed`, delivery `delivered`).
- Real wake smokes passed: Codex app-server `initialize -> thread/resume ->
  turn/start` against a live thread returned a real `inProgress`/completed
  turn; OpenCode `opencode run --session <id>` resumed a live session and the
  daemon delivery prompt embedded `delivery_id`.

Remaining for a defensible local testing candidate: run the daemon as a
start-at-login service on `%USERPROFILE%\.vanth` (deploy/vanthd.cmd) and do a
longer real-agent soak (a job that runs for minutes and wakes the agent). The
Go `monitor` reads the same home read-only and renders the dashboard.

## Deferred / out of v1 scope

1. Tighten progress semantics.
   - Current progress is generic event data with normalized `percent` when `current` and `total` are present.
   - Define recommended fields: `current`, `total`, `percent`, `unit`, `phase`, `message`.
   - Add examples for multi-phase tasks where each phase has its own progress.

2. Improve log/event parsing.
   - Current event format is line-based: `AGENT_EVENT {json}`.
   - Add a documented decorator/helper pattern for Python jobs beyond `agent_event(...)`.
   - Consider helpers for shell commands and Node scripts.
   - Keep log parsing as a fallback, not the primary protocol.

3. Remote access, TLS, multi-user policy, quotas, interactive stdin, terminal UI,
   distributed workers, and custom service-manager code remain outside v1.

## Important Files

- `src/vanth/server.py`: main `JobManager`, MCP tools, event parsing, deliveries, recovery, view/doctor.
- `src/vanth/daemon.py`: localhost HTTP daemon.
- `src/vanth/runner.py`: detached worker process that owns the real command.
- `src/vanth/codex_bridge.py`: Codex app-server JSON-RPC wake delivery.
- `src/vanth/opencode_bridge.py`: OpenCode CLI session wake delivery.
- `src/vanth/agent_events.py`: Python helper for emitting structured events.
- `tests/test_vanth.py`: core manager behavior.
- `tests/test_mcp_stdio.py`: MCP stdio integration against the daemon.

## Known Caveats

- `job_send` and interactive stdin are intentionally not implemented. Jobs run with stdin closed.
- Codex wake was tested with a fake app-server and previously with a local Codex binary; real wake behavior still depends on Codex app-server support remaining compatible.
- OpenCode wake was tested with a fake CLI. Live delivery depends on the configured provider, and `opencode run` may need a larger `timeout_seconds` for long turns.
- MCP itself does not wake dormant threads. Vanth resumes Codex through app-server and OpenCode through `opencode run --session`.
- If the daemon is killed during an external wake command, lease recovery makes the delivery retryable, but a crash after the external side effect and before the success write remains an at-least-once ambiguity.
- The untracked `job-monitoring-terminal-charts-scope.md` existed before this handoff and was not touched.
