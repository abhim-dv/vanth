# Changelog

All notable changes to Vanth are documented here.

## Unreleased / next (1.6.x)

### Review fixes (remote-artifacts-implementation-review.md)

- **P0-1 Pairing hardened**: strict target validation (rejects control chars /
  config injection), `Host *` dedicated per-remote config always passed via
  `-F` so directives can never be skipped by targeting the raw hostname,
  host-key pinning before any auth (`--host-fingerprint` verification or
  explicit `--accept-host-key` TOFU consent), real authorized-keys install
  script (atomic, idempotent, refuses unrestricted duplicates of our key),
  canonical hello sentinel requiring a validated `vanth.remote` response, and
  compensation that revokes ONLY the marker line on failure/remove.
- **P0-2 Cross-thread SQLite fixed**: shared remote-store connections opened
  with `check_same_thread=False` and every store operation serialized via
  RLock (JobManager db_lock pattern) — pairing + subsequent job requests on
  different handler threads no longer crash.
- **P0-3 job.stop / job.rerun dispatch correctly**: stop targets the existing
  job via the manager (no phantom queued job); rerun resolves the original
  immutable run spec and queues exactly one rerun whose replay returns the
  SAME new job id; both carry durable results for lost-response replay.
- **P0-4 Snapshot pagination repaired**: remote pages use a stable keyset
  cursor (job_id ordered) instead of mutable OFFSET; controller applies pages
  WITHOUT deletion reconciliation and reconciles only after the FINAL page
  over the accumulated identity set; every sync starts from a fresh cursor.
  >50-job snapshots and second syncs no longer suppress valid shadows.
- **P0-5 Helper framing**: daemon protocol frames forwarded UNCHANGED after
  request_id/method binding — no more double-wrapped responses hiding flat
  result fields (state_epoch, acked_offset) from the controller/transfer path.
- **P1 fixes**: responses bound to their request_id/method; artifact ops can
  no longer steal a live claim (only pending/failed/expired-running may be
  claimed); remote log reads enforce opaque-ID grammar + containment +
  no-symlink; `_run_request` re-drive seam preserved for retry.
- **P2/P3**: placeholder `submitting` shadows only created for mutations and
  retired when the real shadow lands; remote wait surfaces hard errors
  immediately instead of burning an hour of timeout; collection append
  returns the persisted timestamp; version bumped to 1.6.0rc12.

### Remote execution (in progress)

- **Phase 0-3 of the remote execution plan are implemented** (see
  `remote-execution-managed-artifacts-plan.md`): protocol contract with RFC
  8785 canonicalization and golden digest vectors; secure SSH pairing
  (`vanth remote pair/list/doctor/remove`, forced-command helper, Ed25519
  identities, ambient-config neutralization); durable remote
  start/status/stop/rerun with idempotency keys, state-epoch fencing, and a
  crash-safe remote dispatcher; paginated snapshot recovery with deletion
  repair and epoch supersession; exact byte-range remote log reads; and read
  API projection across local jobs and current remote shadows (Go monitor
  included). Spec: `docs/spec/remote-protocol-v1.md`.

### Robustness

- **NEW - MCP process watchdog**: the `vanth` MCP stdio server now self-terminates
  when its launching client (codex/opencode) dies or closes stdin, and reaps
  itself when idle for `VANTH_WATCH_IDLE` seconds (default 1800, `0` disables).
  Previously a force-killed session or a client that accumulates cached workers
  left `vanth.exe` processes running forever (observed: a new process every few
  minutes, none ever reaped). A blocking tool call (`job_wait`, `job_tail
  --follow`) is never killed mid-flight. Tuning: `VANTH_WATCH_INTERVAL`,
  `VANTH_WATCH_GRACE`, `VANTH_WATCH_PARENT_PID`.
- **`vanth doctor` reports orphaned MCP servers**: `orphaned_mcp_servers` lists
  `vanth` processes whose launching client is gone, and
  `vanth doctor --reap-orphans` terminates them. Doctor now warns when orphans
  are found.
- **Schema constants reconciled**: Go `internal/state/state.go` and the Go
  conformance fixture generator now report schema v9 (matching
  `migrations.py`), including the `trigger_json` column.
- **Legacy `artifact_read` HTTP retrieval is gated**: http(s) artifact reads are
  disabled by default; opt in with `VANTH_ALLOW_HTTP_ARTIFACT_READ=1`. This path
  is legacy and is never used by managed artifacts.

## 1.5.0 - 2026-08-20

### Agent + user QoL

- **MCP tool `job_wait` gains `metric_ge`**: wait until a named metric (e.g.
  `loss`, `progress.percent`) reaches a numeric threshold, returning
  `{"result": "metric", ...}` instead of blocking for an event. Combined with
  the existing multi-event `filters`, one call can wait on "loss < 0.5 OR job
  completes".
- **NEW - job DAG via `trigger`**: `job_start(..., trigger={"job_id": A,
  "status": "completed"})` creates the job `queued`; its runner starts
  automatically once A reaches that status. If A ends in a different terminal
  status, the queued job is `cancelled`. Lightweight — one column, no graph
  engine. `vanth stop` cancels a queued job before it fires.
- **MCP tool `job_tail` gains `grep`**: server-side line filtering on stdout /
  stderr, so agents can pull only the matching lines without shipping the whole
  log. CLI: `vanth logs <id> --grep <pattern>`.
- **NEW - MCP tool `job_diff` + CLI `vanth diff`**: compare the run specs
  (command, env, cwd, timeout, tags, wake targets) of two jobs — e.g. a job vs
  its rerun — returning per-field base/other changes or `identical: true`.

### Docs + CI

- Schema v9: `jobs.trigger_json` column (migrated automatically from v8).
- Chaos matrix gains a `ux` scenario covering metric waits, DAG trigger
  (cancel + success), tail grep, and job diff through the live daemon HTTP
  layer. All 9 scenarios pass.

## 1.4.1 - 2026-08-20

### Agent + user QoL

- **MCP tool `job_rerun` now accepts override params**: `command`, `env`,
  `timeout_seconds`, `name`, `tags`, `notes`, `cwd`, and `interactive` — omitted
  parameters reuse the original job's values.
- **NEW - MCP tool `job_status_batch`**: fetch many jobs' status in one call
  (`job_ids`, `limit`) instead of N `job_status` round trips.
- **MCP tool `job_wait` gains `return_progress`**: optionally include the job's
  latest progress block in the wait response.
- **MCP tool `job_tail` gains `follow` / `timeout_seconds`**: block for new
  output until the job ends or the timeout elapses.
- **MCP tool `daemon_wake` gains a shorthand**: pass a full target dict as
  `target`, or use `type` (required, one of `local_command` / `codex_thread` /
  `opencode_thread`) plus `events` / extra config kwargs (events default to
  `["completed", "failed"]`). `add_wake_target` now validates wake targets
  against `validate_wake_targets`, rejecting unsupported types.

### Docs + CI

- **Docs**: documented that `codex_thread` wakes require a thread that has
  already had at least one turn (a persisted rollout); resuming a zero-turn
  thread fails with `no rollout found for thread id`.
- **CI**: the flaky Windows python job now runs on `pull_request` only (not
  `push`), so pushes stop triggering the intermittent
  `test_stop_after_restart_kills_runner_and_workload` pid-teardown flake.

## 1.4.0 - 2026-08-19

### Agent + user QoL

- **NEW - richer human CLI**: `vanth list` (alias `ps`), `vanth logs`
  (alias `tail`), `vanth stop`, `vanth artifacts`, `vanth prune`, and
  `vanth --version` join `status` / `doctor` / `restart` / `setup` as
  first-class operations. `list` filters by `--status`/`--limit`/`--all` and
  prints JSON; `logs` selects `--stream stdout|stderr|all` with `--offset` /
  `--max-bytes`; `prune` is a manual retention pass that is dry-run by default
  (`--older-than N`, `--yes` to apply).
- **NEW - `vanth autostart enable|disable|status`**: installs a
  start-at-login mechanism per platform (Windows Task Scheduler / macOS
  launchd / Linux systemd user unit) so the daemon survives reboots, and
  reports activation state via `vanth status`.
- **NEW - MCP tool `job_metric_ingest`**: write scalar metric points
  into a job's series with idempotency-key support, complementing the existing
  read-only `job_metrics_query`.
- **NEW - MCP tool `job_artifact_read`**: read a stored artifact's
  contents/metadata back out of a job (the read side of `job_artifact_add`).
- **NEW - MCP tool `daemon_wake`**: request the daemon's attention from
  inside a job context (e.g. to surface an agent-facing wake without a wake
  target event).
- **NEW - MCP tool `job_cleanup_preview`**: a dedicated dry-run
  retention preview that reports exactly what would be removed, separate from
  the destructive `job_cleanup`.

## 1.3.1 - 2026-08-18

- Fixed a Linux-only daemon crash on shutdown: `_stop_httpd` is registered as a
  Unix signal handler, which passes `(signum, frame)`; it previously took no
  arguments and raised `TypeError`, leaking a traceback into daemon stderr and
  failing signal-driven shutdown. It now accepts and ignores the handler args
  (the authenticated `/shutdown` route still calls it with none).

## 1.3.0 - 2026-08-18

### Cross-platform wheels + release automation

- The wheel build now supports injecting a prebuilt (possibly cross-compiled)
  Go monitor via `VANTH_MONITOR_BIN` + `VANTH_MONITOR_TAG`, and cross-compiling
  in place via `VANTH_MONITOR_GOOS` / `VANTH_MONITOR_GOARCH`. Local `uv build`
  behavior is unchanged.
- New `.github/workflows/release.yml` publishes Linux x86_64/arm64, macOS
  x86_64/arm64, and Windows x86_64 wheels to PyPI and a GitHub Release on any
  `v*` tag push (also runnable via `workflow_dispatch`). `uv tool install vanth`
  now works on Linux and macOS, not just Windows.
- CI gains a `resilience` job that runs the chaos matrix on Linux.

### Interactive stdin + `job_send`

- `job_start(..., interactive=True)` opens the job's stdin. The runner forwards
  length-prefixed records from a per-job channel (`<home>/stdin/<job_id>.in`)
  to the child's stdin; a zero-length record closes stdin (EOF).
- New MCP tool `job_send(job_id, input, eof=False)` (and HTTP
  `POST /jobs/{id}/send`) appends input to a running interactive job's channel.
  Non-blocking; rejects unknown/not-running/non-interactive jobs.
- `job_rerun` preserves the `interactive` flag; `job_cleanup` also removes the
  stdin channel files.

### Quotas + automatic retention

- `VANTH_MAX_RUNNING_JOBS` caps concurrent running jobs (default `0` =
  unlimited). `job_start` returns a clean 400 when the quota is reached.
- `VANTH_RETENTION_SECONDS` (default `0` = off), `VANTH_RETENTION_INTERVAL_SECONDS`
  (default 3600), and `VANTH_RETENTION_DRY_RUN` (default `1`) add automatic
  background retention of old terminal jobs, wired into the existing dispatcher
  loop. Safe-by-default: dry-run unless explicitly enabled.
- `vanth doctor` now reports `running_jobs`, `max_running_jobs`, and a
  `retention` config block.

## 1.2.1 - 2026-08-18

### Stale opencode session recovery

- **Probe-before-dispatch**: `opencode_thread` deliveries now cheaply check
  (`opencode session list --format json`) that the target session still exists
  before burning a model turn. A confirmed-missing session raises a
  classifiable `OpenCodeSessionNotFound` instead of failing with a raw
  "Session not found" after wasting a turn.
- **Skip retries on a dead session**: the delivery layer treats
  `OpenCodeSessionNotFound` as permanently non-retryable — it dead-letters
  immediately (attempts=1) instead of exhausting `max_attempts` on backoff for
  a session that can never succeed.
- The probe never blocks a valid dispatch: on any ambiguity (timeout, probe
  failure, non-zero exit, bad JSON) it proceeds normally. Opt out per-target
  with `skip_probe: true` or globally with `VANTH_OPENCODE_SKIP_PROBE=1`;
  `attach` targets skip the probe automatically.
- Previously-silent dead-lettered wakes from 1.2.0 (`opencode ... Session not
  found`, e.g. "Session not found") now fail fast with actionable errors.

## 1.2.0 - 2026-08-18

Reliability hardening of the delivery/job core and the wake adapters. Goal: a
job is never lost because of Vanth itself.

### Job/delivery core

- **Runner spawn is now crash-safe** (`JobManager.start`): if the detached
  runner process fails to launch (missing venv python, OSError), the job is
  transitioned to `failed` with an error event instead of being left as a
  phantom `running` row with no worker.
- **`notify_on` now actually works**: it becomes the default `events` list for
  any wake target that doesn't specify its own `events`. Previously it was
  stored but never consumed (an agent setting `notify_on` would never get
  woken).
- **`retry_delivery` can force-advance a `retrying` delivery** (reset its
  backoff immediately), not just a `failed` one.
- **Delivery dispatch backpressure**: concurrent adapter dispatches are capped
  at `VANTH_DELIVERY_MAX_CONCURRENT` (default 4); excess stays queued in
  SQLite and is picked up on the next poll.
- **`job_start` MCP tool is now sync** so FastMCP runs it in a threadpool
  instead of blocking the event loop on HTTP.
- **Dead-letter visibility in `doctor`**: `dead_letter_count` and a
  `dead_lettered` list (deliveries that exhausted `max_attempts`).

### Wake adapters (codex_thread / opencode_thread)

- Codex `initialize` handshake retries up to 3 times with backoff (bounded by
  the delivery timeout); `thread/resume` and `turn/start` are never retried
  (side-effect safety).
- Dead/broken-pipe codex processes fail with a clear error including the exit
  code and last stderr tail, instead of a raw `BrokenPipeError`.
- Child cleanup can't mask the original delivery error.
- Codex binary launch failures and opencode binary launch failures raise
  clear `CodexBridgeError`/`OpenCodeBridgeError` messages.
- Codex child processes are detached from the daemon's console group on
  Windows.



- Fixed `vanth --help` through the real entry point (`vanth = vanth.server:main`
  previously only routed status/doctor/restart/setup to the CLI, so `--help`
  fell through into the MCP stdio server and printed nothing). Bare `vanth`
  still runs the MCP server as expected.

## 1.1.2 - 2026-08-18

- `vanth --help` / `vanth -h` / `vanth help` / bare `vanth` now print a proper
  usage summary listing status / doctor / restart / setup (previously bare
  `vanth` dumped the module docstring to stderr and `--help` was an unknown
  command).

## 1.1.1 - 2026-08-18

- `vanth status` and `vanth doctor` now show MCP client registration state
  (`opencode=configured, codex=not configured, ...`) and point at `vanth setup`
  when something is missing.
- The MCP server prints a one-line stderr hint on startup when a known client
  isn't configured yet (suppress with `VANTH_NO_SETUP_HINT=1`).

## 1.1.0 - 2026-08-18

### MCP client setup

- New `vanth setup` command that connects the MCP server to the clients
  installed on the machine in one step. It detects opencode, Codex, and
  generic `mcpServers`-style clients (Claude Code / Cursor), backs up each
  config it touches (`*.vanth-setup-<ts>.bak`), and upserts the Vanth MCP
  entry without disturbing anything else in the file.
  - `vanth setup` — detect and configure everything found (prompts before
    changing).
  - `vanth setup --yes` — apply without prompting (scripts/CI).
  - `vanth setup opencode codex` — only specific clients.
  - `vanth setup --json` — machine-readable result.
  - `vanth setup --remove` — remove the Vanth MCP entries instead.
  - `vanth setup --help` — usage.
- MCP stdio tests now spawn `python -m vanth` instead of `uv run vanth`,
  so they don't trip over a running daemon locking the venv's entry-point
  scripts on Windows.

## 1.0.1 - 2026-08-18

- Packaging: add Apache-2.0 `LICENSE`, production PyPI metadata (classifiers,
  URLs, keywords), and point the quick-start install back at `uv tool install
  vanth` now that the package is published. No runtime changes.

## 1.0.0 - 2026-08-18

First supported v1 release. Vanth is a localhost background-job daemon and MCP
interface for agents: start detached jobs, receive `AGENT_EVENT` structured
events, wait on durable SQLite state, and wake Codex or OpenCode sessions when
a job needs attention.

### Operations CLI and daemon lifecycle

- New `vanth` subcommands for humans (not MCP): `vanth status`, `vanth doctor`,
  `vanth restart`. `status` reports daemon up/down, pid, schema, running jobs,
  delivery counts (supports `--json`); `doctor` prints a human-readable health
  report; `restart` gracefully stops the daemon and starts a fresh one (jobs
  survive — runners are detached).
- The daemon gains an authenticated `POST /shutdown` route so a client can
  request graceful shutdown over loopback HTTP (used by `vanth restart`).
- `VanthClient` now honors `VANTH_DAEMON_HOST`/`VANTH_DAEMON_PORT` when no URL
  or discovery metadata is present, so clients and `vanth restart` work on
  non-default ports.
- The terminal monitor ships as a bundled native Go binary via the
  `vanth-monitor` console script (platform-tagged wheels, no Go toolchain
  needed at runtime).

### Telemetry (schema v8)

- `metric_series` table: scalar fields of `metric`/`progress` AGENT_EVENTs are
  mirrored into queryable series (job, metric, x/y, stage, event id, seq,
  timestamp), matching the Go monitor's transform semantics (`_step` as x,
  `progress.current`/`total`/`percent` derived).
- `artifacts` table: jobs can carry named artifacts (checkpoints, CSVs,
  rendered outputs) with uri, kind, size, sha256, and meta.
- New MCP tools + HTTP routes:
  - `job_metrics_query(job_id, metric?, from_ms?, to_ms?, limit?)` — read scalar series.
  - `job_metric_compare(job_ids, metric, aggregation)` — compare a metric across runs
    (latest/mean/min/max/sum/count).
  - `job_run_summary(job_id)` — one-call "did it work?" (status, runtime, progress,
    latest metrics, artifacts).
  - `job_artifact_add(job_id, name, uri, ...)` and `job_artifacts(job_id)`.
  - `job_dashboard(job_ids?, limit?)` — downsampled chart-data view for any renderer.
- SQLite schema bumped to v8 (adds `metric_series`, `artifacts` tables);
  existing homes migrate with a backup. Go fixture/conformance updated to v8.

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

### Migration and state

- Ordered SQLite migrations driven by `PRAGMA user_version`.
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
- On daemon start the state directory's permissions are re-tightened to the
  owner only (Unix `0700`/`0600`; Windows `icacls` disables ACL inheritance and
  grants only owner, SYSTEM, and Administrators). This prevents a broad
  profile-level grant (e.g. a sandbox group with read access to the user
  profile) from exposing the bearer token or per-job env/spec data.
- The daemon binds only to loopback addresses; a non-loopback
  `VANTH_DAEMON_HOST` is rejected. On Windows, socket `SO_REUSEADDR` is
  disabled so a second daemon cannot silently become a phantom listener on the
  same port; a failed bind releases the home lock and exits cleanly.
- An upstream `pydantic-settings` warning (an unresolved `lifespan` forward
  reference in mcp's FastMCP) that printed on every console-script invocation
  of a fresh install is suppressed.
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
- Windows Startup/Task Scheduler action (`deploy/vanthd.cmd`) and Unix systemd
  user service (`deploy/vanthd.service`) templates.

### Compatibility and packaging

- `mcp` is pinned to `>=1.0,<2.0` because mcp 2.0 removed
  `mcp.server.fastmcp`.
- The wheel bundles a native Go monitor binary (platform-tagged,
  `py3-none-<platform>`), built by a hatchling build hook; no Go toolchain is
  needed to install or run it.
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
