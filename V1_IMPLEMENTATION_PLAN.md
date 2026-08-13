# Vanth v1 Production Candidate Implementation Plan

## Mission

Turn the current v0 prototype into a production-worthy v1 for one trusted user
on one machine. Vanth v1 remains a localhost background-job daemon and MCP
interface. It is not a remote execution service, multi-user scheduler, or
interactive terminal.

The implementation is complete only when the release gates at the end of this
document pass on Windows and Unix.

## Scope Boundary

V1 supports:

- one daemon per `VANTH_HOME`;
- localhost HTTP used by the MCP process and local clients;
- detached non-interactive commands with stdin closed;
- durable SQLite jobs, events, wake targets, deliveries, and attempts;
- local command, Codex thread, and OpenCode session wake delivery;
- daemon and MCP restart while jobs continue;
- bounded local state, actionable diagnostics, and tested upgrades.

V1 does not include:

- remote network access, TLS, multi-user tenancy, RBAC, or quotas;
- Unix sockets or Windows named pipes;
- interactive stdin or `job_send`;
- a terminal/dashboard UI;
- distributed workers;
- a new web framework, ORM, migration dependency, or logging dependency.

Use the standard library and existing dependencies unless a concrete failing
test proves they are insufficient.

## Current Baseline

The repository HEAD is `47f6286 Add Vanth next steps handoff`, with important
uncommitted work on top. Do not reset, replace, or reimplement that work.

The dirty working tree currently contains:

- OpenCode wake delivery in `src/vanth/opencode_bridge.py`;
- delivery serialization and automatic due-retry dispatch;
- malformed HTTP and event input hardening;
- runner startup/disappearance handling;
- recovery race and stop-after-restart fixes;
- new hardening tests under `tests/`;
- README and `NEXT_STEPS.md` updates.

The untracked `job-monitoring-terminal-charts-scope.md` predates this work and
must remain untouched.

Last verified baseline:

```cmd
uv run pytest -q
```

Expected: `52 passed`.

```cmd
uv run python -m compileall -q src tests examples
```

```cmd
uv build
```

A synthetic burst of 20 concurrent jobs emitting 2,000 progress events retained
all 2,040 progress and lifecycle events. Preserve this behavior.

## Architecture to Preserve

The MCP server in `src/vanth/server.py` uses `VanthClient` to reach the local
HTTP daemon. The daemon owns the durable `JobManager`. Starting a job writes its
database row and spec, then launches `python -m vanth.runner`. The runner owns
the real command, captures stdout/stderr, parses `AGENT_EVENT` lines, writes
durable events, and records terminal state. A daemon dispatcher claims due
delivery rows and calls the local command, Codex, or OpenCode adapter.

Keep these ownership rules:

- the runner owns command execution, timeout, and stream draining;
- the daemon owns maintenance, delivery dispatch, API requests, and recovery;
- SQLite is the source of truth across process restarts;
- MCP and HTTP clients never need to remain alive for jobs to continue.

## Required Invariants

Every implementation phase must preserve these invariants:

1. A job has one durable terminal status: `completed`, `failed`, `timeout`,
   `cancelled`, or `orphaned`.
2. Terminal transitions use conditional SQL and cannot overwrite a terminal
   state recorded by another process.
3. A job is not terminal until both output streams have reached EOF and all
   structured events have been persisted.
4. A malformed request, event, target, spec, or adapter response cannot kill a
   daemon request thread, runner reader thread, dispatcher, or maintenance loop.
5. Delivery dispatch is never concurrent for the same `delivery_id`.
6. No failed or expired delivery is silently stranded.
7. Shutdown stops accepting work, drains bounded in-flight operations, closes
   SQLite, and leaves active detached jobs recoverable.
8. State growth is bounded or has an explicit, documented retention control.

## Delivery Guarantee

V1 provides durable at-least-once wake delivery with no concurrent duplicate
dispatch. Exactly-once delivery cannot be guaranteed when an external command
accepts a wake and the daemon dies before recording success.

Use `delivery_id` as the idempotency key:

- include it in every adapter payload;
- include it in default Codex/OpenCode wake text;
- require local command adapters to deduplicate it if duplicate side effects are
  unacceptable;
- document the crash-after-side-effect ambiguity;
- never claim exactly-once behavior unless an adapter later exposes an atomic
  idempotency API.

## Implementation Order

Complete phases in order. Each phase must leave the full suite green and should
be reviewable independently.

### Phase 0: Stabilize the Baseline

1. Review the current dirty diff and preserve all user-owned changes.
2. Run the three baseline commands above.
3. Confirm no test depends on a real Codex/OpenCode account by default.
4. Record the new baseline test count in `NEXT_STEPS.md` if it changes.
5. Do not commit, stage, or discard changes unless the user explicitly requests
   that git operation.

Acceptance:

- all current tests pass repeatedly;
- no temporary stress files or state directories are added to the repository;
- the pre-existing scope document remains untouched.

### Phase 1: Versioned Schema and Delivery Leases

This is the first production blocker because a daemon crash can currently leave
a delivery permanently `dispatching`.

#### Schema migration

Replace ad hoc column checks in `JobManager._migrate_columns()` with ordered,
transactional migrations driven by `PRAGMA user_version`. Keep migrations in a
small module such as `src/vanth/migrations.py`; do not add a migration framework.

Before the first migration of an existing database, use SQLite's backup API to
create one timestamped backup in `VANTH_HOME/backups/`. Do not copy only the main
database file while WAL mode is active.

Add delivery columns:

```sql
claim_token TEXT
```

```sql
claimed_at TEXT
```

```sql
lease_expires_at TEXT
```

Add attempt timestamps if the existing `created_at` cannot clearly represent
both start and completion:

```sql
started_at TEXT
```

```sql
ended_at TEXT
```

Set SQLite connection policy in one shared initializer:

- WAL mode;
- `PRAGMA busy_timeout` with a documented default;
- foreign keys enabled for newly created schemas;
- an explicit schema version checked by `job_doctor`.

#### Atomic claim algorithm

Claim one due delivery inside `BEGIN IMMEDIATE`:

1. Select a `pending` or `retrying` row whose `next_attempt_at` is due, or a
   `dispatching` row whose lease expired.
2. Generate a random `claim_token`.
3. Update that row to `dispatching` with `claimed_at` and
   `lease_expires_at`, conditional on its prior state/lease.
4. Commit before invoking the external adapter.
5. Complete or reschedule the row only with `WHERE claim_token=?`.

The lease duration must exceed the configured adapter timeout with a safety
margin. A slow adapter must not be reclaimed while its original process can
still succeed.

Remove any remaining timer-owned retry behavior. The daemon maintenance loop is
the sole owner of due delivery discovery. Track spawned delivery threads so
shutdown can wait for them up to a configured bound.

#### Attempt history and API

Create an attempt row when dispatch starts, then update it when the adapter
returns. Record:

- attempt number;
- claim token;
- target type;
- start/end timestamps;
- final status;
- bounded error text;
- whether the attempt was reclaimed after an expired lease.

Add:

- `JobManager.delivery_attempts(delivery_id, limit)`;
- `GET /deliveries/{delivery_id}/attempts`;
- MCP tool `job_delivery_attempts`.

Unknown delivery IDs must return the same structured error behavior as other
unknown resources.

#### Phase 1 tests

Add deterministic tests for:

- daemon death before adapter invocation;
- daemon death while the adapter is running;
- daemon death after the fake adapter records the side effect but before Vanth
  records success;
- reclaim only after lease expiry;
- stale claim tokens unable to complete a reclaimed delivery;
- concurrent dispatchers invoking one adapter once;
- retries continuing across multiple manager restarts;
- attempt listing through manager, HTTP, and MCP;
- migration from a copy of the current v0 schema with data preserved.

Acceptance:

- no delivery remains `dispatching` after its lease expires and a daemon is
  available;
- crash ambiguity is surfaced in attempts and documentation;
- no concurrent duplicate dispatch occurs in a 100-iteration race test.

### Phase 2: Daemon Lifecycle, Authentication, and Diagnostics

#### Local security boundary

Vanth runs arbitrary commands with the daemon user's permissions. Require an
authentication token even on localhost.

Implement with standard-library primitives:

- create a random token with `secrets.token_urlsafe()`;
- store it under `VANTH_HOME` with owner-only permissions where supported;
- have `VanthClient` create/read the token before auto-starting the daemon;
- send `Authorization: Bearer <token>` on every request;
- compare tokens with `hmac.compare_digest()`;
- return JSON `401` without revealing the expected token;
- never write the token to logs, doctor output, exceptions, or MCP responses.

Bind only to `127.0.0.1`, `::1`, or `localhost`. V1 must refuse a non-loopback
`VANTH_DAEMON_HOST`; remote mode belongs to a later threat model with TLS.

Keep the existing request body cap and structured 4xx/5xx behavior. Add bounded
response/error sizes and consistent validation for negative or excessive
`limit`, `offset`, timeout, and byte-count values.

#### Single daemon ownership

Hold an OS-backed lock file for the lifetime of the daemon. Use `msvcrt` on
Windows and `fcntl` on Unix. The OS lock, not lock-file existence, determines
ownership so a crash does not require stale-file deletion.

A second daemon for the same `VANTH_HOME` must exit quickly with a clear log
message. Runners must not take this lock because they intentionally share the
database.

#### Graceful shutdown

Update `src/vanth/daemon.py` so SIGINT and SIGTERM:

1. stop accepting new requests;
2. allow current request threads to finish within a bound;
3. stop and join the maintenance dispatcher;
4. wait for tracked delivery workers within their adapter timeout bound;
5. call `JobManager.close()` once;
6. close the HTTP server and release the daemon lock.

Do not call `ThreadingHTTPServer.shutdown()` from its own `serve_forever()`
thread. Use a signal-set event plus a helper thread, or another arrangement that
honors the standard-library shutdown contract.

Make `JobManager.close()` idempotent. New calls after close should fail clearly;
active waits should finish or receive an explicit shutdown result rather than a
raw `sqlite3.ProgrammingError`.

#### Logs

Use `logging` and `logging.handlers.RotatingFileHandler`:

- daemon log under `VANTH_HOME/logs/`;
- runner diagnostic log, preferably per job for direct correlation;
- timestamp, level, process ID, component, job/delivery ID where applicable;
- bounded file size and backup count;
- no environment values, auth token, full prompts, or arbitrary command payloads
  at normal log levels.

Replace intentionally discarded runner stderr with the diagnostic log. Preserve
job stdout/stderr as separate user-facing files.

#### Health and doctor

Keep `/health` cheap. Add readiness checks to `job_doctor` or `/ready` for:

- schema version;
- `PRAGMA quick_check`;
- daemon lock ownership;
- maintenance thread alive;
- stale delivery lease count;
- state path and available disk space;
- Codex/OpenCode binary resolution using `shutil.which()` or explicit paths;
- token file existence and safe permissions without exposing its contents.

#### Phase 2 tests

Add tests for unauthorized/malformed authorization, token redaction, loopback
binding enforcement, concurrent auto-start, second-daemon rejection, SIGINT and
SIGTERM shutdown, active request during shutdown, repeated close, log rotation,
and a forced internal error appearing in diagnostics without secrets.

Acceptance:

- every HTTP route requires the token;
- a second daemon cannot mutate the same home;
- normal and signaled shutdown leave SQLite clean and active jobs recoverable;
- daemon and runner failures are diagnosable from bounded logs.

### Phase 3: Process Lifecycle and Bounded State

#### Atomic job state transitions

Centralize terminal updates in a small helper that performs a conditional SQL
transition. Do not introduce a state-machine class. The helper should return
whether it changed the row, and callers emit a terminal event only when the
transition succeeded.

Use it for completion, failure, timeout, cancellation, orphan recovery, runner
startup failure, and runner disappearance.

#### Runner liveness after daemon restart

The current in-memory watcher only works when the manager launched the runner.
Add a durable runner heartbeat:

- `jobs.runner_heartbeat_at` column;
- runner updates it periodically while it owns a running job;
- daemon maintenance loop scans running jobs;
- if heartbeat is stale and `worker_pid` is dead, terminate the stored workload
  process tree and conditionally mark the job `orphaned`;
- heartbeat write failures are logged and retried without killing the workload.

Do not orphan solely because one heartbeat is late. Use a documented threshold
well above the heartbeat interval.

#### Process trees

On Unix, launch both the detached runner and workload in new sessions/process
groups and terminate the group on stop, timeout, or orphan cleanup. Preserve the
existing Windows process-group flags and forced `taskkill /T /F` fallback.

Test grandchildren, not only the direct command process.

#### Timeout and shutdown recovery

Add explicit tests showing:

- timeout still fires after the daemon and MCP processes restart;
- stop after restart kills runner, child, and grandchild;
- runner death after daemon restart is detected by heartbeat reconciliation;
- daemon shutdown during `job_wait` produces a controlled client outcome;
- a completion/recovery race cannot produce conflicting terminal state.

#### Bounded state

Prevent active jobs from exhausting disk:

- configurable per-stream log byte cap;
- continue draining the child pipe after the cap so the child never blocks;
- emit one `log_truncated` warning event per stream;
- retain a tail rather than repeatedly rewriting the whole file if practical.

Clean a spec after the runner has successfully read it and started the workload.
Keep failed startup diagnostics before deleting a bad spec.

Add a manual cleanup operation with dry-run support for completed jobs older than
a configured age. It may delete logs, JSONL event mirrors, specs, delivery
attempts, deliveries, wake targets, events, then jobs in dependency order.
Never delete running jobs. Automatic retention can remain off by default for v1
if the manual operation and disk caps are documented.

Acceptance:

- daemon restarts do not disable later orphan detection;
- stop/timeout removes full process trees on Windows and Unix;
- intentionally noisy jobs cannot grow one stream beyond its configured cap;
- cleanup is transactional for database rows and safe to retry for files.

### Phase 4: Packaging, Service Installation, and Documentation

#### Install validation

Test the built wheel in a clean uv-managed environment. The installed entry
points must run without the source tree on `PYTHONPATH`.

```cmd
uv build
```

```cmd
uv run --isolated --with .\dist\vanth-0.1.0-py3-none-any.whl python -c "import vanth; print(vanth.__file__)"
```

Add an automated installed-entry-point smoke that starts `vanthd` on an
ephemeral port, checks authenticated readiness, and terminates it cleanly. Keep
any documented smoke invocation one-line and reproducible.

#### Long-running daemon

Provide supported templates and docs for:

- a Windows per-user scheduled task or service wrapper;
- a systemd user service on Unix;
- foreground diagnostic execution;
- start, stop, restart, status, log location, upgrade, rollback, and uninstall.

Choose one supported approach per operating system. Do not build a custom service
manager.

#### Configuration reference

Document every supported setting and default, including:

- `VANTH_HOME`;
- `VANTH_DAEMON_URL`, host, and port;
- request, event-data, event-line, and log limits;
- SQLite busy timeout;
- delivery poll interval and lease margin;
- runner heartbeat interval/stale threshold;
- Codex/OpenCode binary paths;
- retention controls;
- log level and rotation controls.

Document the state layout, permissions, schema upgrade/backup behavior, delivery
guarantee, command-execution threat model, and recovery runbook.

#### Agent integration docs

Provide complete MCP configuration examples for Codex and one Claude-style MCP
client. Explain:

- how `origin_thread_id` and wake target IDs are supplied;
- when to use `job_wait`, `job_view`, events, and stable cursors;
- how to inspect and retry deliveries;
- OpenCode timeout behavior;
- that interactive stdin remains unsupported.

#### CI

Add CI for supported Python versions on Windows and Linux:

```cmd
uv run pytest -q
```

```cmd
uv run python -m compileall -q src tests examples
```

```cmd
uv build
```

Also install and import the built wheel. Keep real Codex/OpenCode smokes opt-in so
forks and ordinary CI do not require accounts or local model providers.

Acceptance:

- a clean machine can install, configure, run, diagnose, upgrade, and remove
  Vanth using only documented steps;
- the wheel works independently of the checkout;
- CI covers Windows and Unix process behavior.

### Phase 5: Release Validation and v1 Cut

Run a dedicated release suite in addition to unit/integration tests.

#### Synthetic workloads

1. Fifty concurrent short jobs, each emitting at least 500 events across stdout
   and stderr; assert exact event counts and unique sequence numbers.
2. Slow delivery adapter while a job emits rapid progress; assert stream parsing
   and terminal state are not delayed by adapter runtime.
3. Kill/restart the daemon repeatedly while jobs run and deliveries retry.
4. Kill runners before workload spawn, during execution, and during terminal
   persistence; assert terminal or recoverable state and no leaked process tree.
5. Send malformed JSON, invalid UTF-8, recursive JSON, oversized event lines,
   invalid target configs, short bodies, huge query integers, and broken client
   connections; assert structured errors and daemon health.
6. Fill log caps and run cleanup twice; assert bounded state and idempotence.

Run race/chaos cases enough times to expose timing failures, but keep a smaller
deterministic version in ordinary CI.

#### Real adapter smokes

With explicit opt-in environment configuration:

- wake a real idle Codex thread and verify one new turn contains `delivery_id`;
- resume a real OpenCode session and verify one new turn contains `delivery_id`;
- force adapter timeout/failure and verify attempt details, retry, and recovery;
- record CLI/app-server versions in test output without pinning user installations.

Never run real wake smokes by default.

#### Release artifacts

- bump the project version to `1.0.0` only after every release gate passes;
- update README status from prototype to supported v1 scope;
- add a changelog entry with migration, security token, state backup, and delivery
  semantics;
- build sdist and wheel and test both;
- do not publish, tag, push, or create a release without explicit user approval.

## Release Gates

V1 is ready only when all are true:

- **Correctness:** full tests and deterministic race tests pass; no conflicting
  terminal transitions or lost structured events.
- **Delivery durability:** expired leases recover; no concurrent duplicates;
  crash ambiguity is recorded and documented.
- **Lifecycle:** daemon, runner, child, and grandchild death/restart cases behave
  consistently on Windows and Unix.
- **Security:** token required and redacted; non-loopback bind rejected; state
  permissions and arbitrary-command threat model documented.
- **Operations:** graceful shutdown, single-daemon lock, bounded logs, health,
  readiness, diagnostics, retention, backup, and migration work.
- **Packaging:** clean wheel install and entry-point smokes pass outside the
  checkout.
- **Compatibility:** fake Codex/OpenCode contract tests pass; opt-in real smokes
  have been completed once for the release candidate.
- **Documentation:** install, service, configuration, upgrade, rollback,
  troubleshooting, and limitations are complete.

## Recommended File Map

Prefer the fewest files that keep responsibilities clear:

- `src/vanth/server.py`: job/event/delivery domain behavior and MCP tools;
- `src/vanth/daemon.py`: HTTP routing, auth enforcement, lifecycle, and lock;
- `src/vanth/client.py`: token-aware HTTP client and daemon auto-start;
- `src/vanth/runner.py`: workload ownership, heartbeat, process groups, streams;
- `src/vanth/migrations.py`: ordered SQLite migrations only;
- `src/vanth/codex_bridge.py`: Codex adapter contract;
- `src/vanth/opencode_bridge.py`: OpenCode adapter contract;
- `tests/test_delivery_hardening.py`: lease/retry/crash races;
- `tests/test_daemon_hardening.py`: auth, request boundary, lock, shutdown;
- `tests/test_runner_hardening.py`: spawn, heartbeat, timeout, process trees;
- `tests/test_server_hardening.py`: events, recovery, state transitions;
- `tests/test_migrations.py`: v0 fixtures, backup, forward migration;
- `tests/test_install.py`: clean built-artifact smoke if practical.

Do not split modules merely to match this list. Add `migrations.py` because
schema evolution is a distinct responsibility; keep smaller helpers in their
current module until they become independently testable behavior.

## Agent Working Rules

- Use `uv` for environments, dependencies, tests, and builds.
- Use `seekfs --under F:\git\vanth` for indexed filename discovery and `rg` for
  content/symbol search.
- Preserve the dirty working tree and unrelated files.
- Use `apply_patch` for edits.
- Add no dependency until a failing release-gate test requires it.
- Reproduce a bug before fixing it and leave one deterministic regression test.
- Run focused tests after each change and the full baseline before handoff.
- Keep all shell commands provided to the user on one line and cmd-compatible.
- Do not stage, commit, push, publish, tag, or create a release without explicit
  user authorization.

## Final Handoff Contents

The implementing agent's final handoff must include:

- release gates completed and still open;
- migrations applied and backup location;
- exact test/build results by platform;
- synthetic workload counts and chaos iterations;
- real adapter smoke status and versions;
- residual failure semantics, especially crash-after-side-effect delivery;
- configuration and operational documentation links;
- a clean separation between Vanth changes and pre-existing user files.
