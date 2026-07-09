# Vanth Next Steps

This handoff starts from commit `f50e907 Add durable runner agent view and delivery retries`.

Current v0 status: usable prototype. Vanth can start detached background jobs, parse `AGENT_EVENT` records from stdout/stderr, track progress, wait on durable SQLite events, tail logs with offsets, show an agent-oriented job view, dispatch wake deliveries through local commands or Codex app-server, retry failed deliveries, and recover across daemon/MCP restarts.

## Validation Baseline

These passed at handoff:

```cmd
uv run pytest -q
```

```cmd
uv run python -m compileall -q src tests examples
```

```cmd
uv build
```

Manual daemon smokes also passed for:

- HTTP job start -> checkpoint wait -> local wake delivery -> `job_view` -> `job_doctor`
- HTTP job start -> checkpoint -> fake Codex app-server wake delivery, verifying `initialize -> thread/resume -> turn/start`

## Highest Priority

1. Add OpenCode wake integration.
   - Codex has `codex_thread` delivery via `src/vanth/codex_bridge.py`.
   - Add a parallel target type such as `opencode_thread` once the local OpenCode automation/wake mechanism is confirmed.
   - Keep the target payload shape close to `codex_thread`: `thread_id`, optional command override, timeout, and `auto_dispatch`.

2. Make the daemon production-shaped.
   - Add a simple install/run story for a long-lived daemon on Windows and Unix.
   - Decide whether v0 daemon should stay HTTP-only on localhost or also support a Unix socket/named pipe later.
   - Add graceful shutdown handling so `JobManager.close()` is called by the daemon.
   - Add daemon logs; right now many internal runner/daemon failures are intentionally quiet.

3. Improve delivery reliability.
   - Add tests for automatic retry with `max_attempts` and `retry_delay_seconds`, not just manual `job_retry_delivery`.
   - Add a delivery attempt listing endpoint/tool if agents need to inspect why a notification failed.
   - Consider a lightweight dispatcher loop in the daemon for due retries instead of only timers plus startup dispatch.
   - Decide whether synchronous delivery dispatch inside runner event emission is acceptable long term. It is simple and durable for v0, but a slow wake adapter can slow event processing.

4. Harden detached runner behavior.
   - Runner stderr is currently discarded by the parent process when launching `python -m vanth.runner`; add a runner diagnostic log file if debugging becomes painful.
   - Add tests for stopping a job after daemon restart.
   - Add tests for timeout after manager restart.
   - Add explicit cleanup of spec files for completed jobs if state growth matters.

5. Build agent-facing ergonomics.
   - Add a small CLI wrapper around HTTP endpoints for manual testing: start, list, view, tail, retry, doctor.
   - Add richer `job_view` filters: `status`, `tag`, `needs_attention`, maybe `updated_since`.
   - Add stable event cursor helpers so agents can subscribe by last event id without repeatedly building their own loop.

## Medium Priority

1. Tighten progress semantics.
   - Current progress is generic event data with normalized `percent` when `current` and `total` are present.
   - Define recommended fields: `current`, `total`, `percent`, `unit`, `phase`, `message`.
   - Add examples for multi-phase tasks where each phase has its own progress.

2. Improve log/event parsing.
   - Current event format is line-based: `AGENT_EVENT {json}`.
   - Add a documented decorator/helper pattern for Python jobs beyond `agent_event(...)`.
   - Consider helpers for shell commands and Node scripts.
   - Keep log parsing as a fallback, not the primary protocol.

3. Add security boundaries.
   - The daemon is localhost HTTP and can run arbitrary commands.
   - Add a short threat model before exposing anything beyond local development.
   - Consider an auth token for daemon requests, especially if host/port are configurable.

4. Document state layout and migration policy.
   - Current state is under `VANTH_HOME` with SQLite, logs, events, and specs.
   - Schema migration is simple `ALTER TABLE`; document that this is acceptable for v0 and define what would trigger a real migration layer.

5. Add packaging/install notes.
   - Define how another project should depend on Vanth.
   - Document MCP config for Codex and Claude-style clients.
   - Add examples for setting `VANTH_HOME`, `VANTH_DAEMON_URL`, `VANTH_CODEX_BIN`.

## Lower Priority

1. Terminal/dashboard view.
   - A human dashboard would be useful, but agents can already use `job_view`, `job_status`, `job_tail`, and `job_events`.
   - If built, keep it thin over the existing HTTP API.

2. Metrics and cleanup.
   - Add optional retention for old logs/events/deliveries.
   - Add basic counters for started/completed/failed/orphaned jobs and delivery success rates.

3. Cross-platform process-group polish.
   - Windows currently uses `CREATE_NEW_PROCESS_GROUP`, `CREATE_BREAKAWAY_FROM_JOB`, and `taskkill /T`.
   - Unix uses normal process signals. Consider process groups with `start_new_session=True` if Unix child cleanup needs to be stronger.

## Important Files

- `src/vanth/server.py`: main `JobManager`, MCP tools, event parsing, deliveries, recovery, view/doctor.
- `src/vanth/daemon.py`: localhost HTTP daemon.
- `src/vanth/runner.py`: detached worker process that owns the real command.
- `src/vanth/codex_bridge.py`: Codex app-server JSON-RPC wake delivery.
- `src/vanth/agent_events.py`: Python helper for emitting structured events.
- `tests/test_vanth.py`: core manager behavior.
- `tests/test_mcp_stdio.py`: MCP stdio integration against the daemon.

## Known Caveats

- `job_send` and interactive stdin are intentionally not implemented. Jobs run with stdin closed.
- Codex wake was tested with a fake app-server and previously with a local Codex binary; real wake behavior still depends on Codex app-server support remaining compatible.
- MCP itself does not wake dormant threads. Vanth wakes Codex by sending an automation-style message through Codex app-server.
- The untracked `job-monitoring-terminal-charts-scope.md` existed before this handoff and was not touched.
