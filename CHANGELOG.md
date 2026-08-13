# Changelog

All notable changes to Vanth are documented here.

## Unreleased

### Agent-facing features (schema v7)

- `job_status` / `job_view` now return the job's `command`, `cwd`, `env`,
  `timeout_seconds`, `notes`, a `run` overview (author, hostname, OS, Python
  version, CPU/GPU, git repo/branch/commit), and `runtime_seconds` — so an
  agent can answer "what is this job?" without reading logs.
- `job_list` accepts `name` (substring) and `tags` filters.
- `job_events` accepts `reverse=true` to return the newest events first, with
  backward paging via `since_event_id`.
- `job_rerun(job_id)` relaunches a job with its original command, cwd, env,
  timeout, name, tags, notes, origin thread, and wake targets.
- The daemon writes `daemon.json` discovery metadata atomically on start
  (url, pid, started_at, schema) and removes it on graceful shutdown; the MCP
  client discovers the daemon URL from it.
- `vanth.agent_logger` routes loguru records into structured `AGENT_EVENT`
  log events (timestamped, level-aware, with context) persisted in the event
  table.
- SQLite schema bumped to v7 (adds `notes`, `run_json` columns); existing homes
  migrate with a backup. Go fixture/conformance updated to v7.

## 1.0.0 - unreleased

First supported v1 release. Vanth is a localhost background-job daemon and MCP
interface for agents: start detached jobs, receive `AGENT_EVENT` structured
events, wait on durable SQLite state, and wake Codex or OpenCode sessions when
a job needs attention.

### Migration and state

- Ordered SQLite migrations driven by `PRAGMA user_version`; schema version 5.
- A timestamped backup of an existing database is created under
  `VANTH_HOME/backups/` before any migration runs.
- SQLite uses WAL mode with a configurable `busy_timeout`
  (`VANTH_BUSY_TIMEOUT_MS`, default 30000) and foreign keys enabled.
- Event sequence numbers are allocated inside a `BEGIN IMMEDIATE` transaction,
  so concurrent runner/daemon processes can never allocate the same per-job
  `seq`.
- The per-event SQLite write lock is no longer held during the JSONL mirror
  append; transient `database is locked` is retried for events, workload PID
  publication, heartbeats, and terminal transitions; a reader thread survives a
  single failed persist instead of dying and losing the stream.

### Security

- The daemon requires `Authorization: Bearer <token>` on every data route.
  The token is generated per home, stored owner-only, and never logged.
- The daemon binds only to loopback addresses; a non-loopback
  `VANTH_DAEMON_HOST` is rejected.
- One OS-backed daemon lock per `VANTH_HOME`; a second daemon exits quickly.

### Delivery

- Durable at-least-once wake delivery with leases, claim tokens, and attempt
  history (`job_delivery_attempts`), automatic due retries, and crash ambiguity
  surfaced as reclaimed attempts.
- `delivery_id` is the idempotency key in every adapter payload.
- Codex app-server (`initialize -> thread/resume -> turn/start`) and OpenCode
  CLI (`opencode run --session <id> --format json`) adapters. The OpenCode
  default command is resolved through `shutil.which()` so npm `.CMD` shims work
  on Windows.

### Operations

- Graceful signal shutdown that drains in-flight work and leaves detached jobs
  recoverable.
- Bounded rotating daemon and per-job runner diagnostic logs; bounded per-stream
  log caps with `log_truncated` events; structured event cap per job.
- `job_doctor` readiness and `job_cleanup` with dry-run and tombstones.
- Windows Task Scheduler action (`deploy/vanthd.cmd`) and Unix systemd user
  service (`deploy/vanthd.service`) templates.

### Compatibility and packaging

- `mcp` is pinned to `>=1.0,<2.0` because mcp 2.0 removed
  `mcp.server.fastmcp`.
- CI workflow (`github/workflows/ci.yml`) runs the suite, compile check, wheel
  build, and an isolated wheel import smoke on Windows and Linux for Python
  3.11 and 3.12.
- Release-gate matrix: `scripts/chaos_matrix.py` (50-job x 500-event burst with
  exact counts, slow-adapter non-blocking, daemon kill/restart recovery, runner
  kills, malformed-input battery, log caps and cleanup idempotence) and
  `scripts/real_adapter_smoke.py` (opt-in real Codex/OpenCode wakes; records
  installed versions).

### Limitations

- `job_send` and interactive stdin are not implemented; jobs run with stdin
  closed.
- A daemon crash after an external wake adapter accepts the side effect but
  before Vanth records success remains inherently at-least-once ambiguity.
- Live Codex/OpenCode wake smokes are opt-in and were not run during release
  validation; fake-adapter contract tests cover the protocol.
