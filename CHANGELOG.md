# Changelog

All notable changes to Vanth are documented here.

## Unreleased / next (1.6.x)

### Launch-claim concurrency and crash-consistency fixes (rc34)

Six P1 concurrency/crash defects found in a review of the rc33 launch-token
implementation. Full suite: 636 passed, 6 skipped across two clean runs.

- **A delayed parent no longer kills a runner that already promoted (P1).**
  If the runner changes `launching -> running` before the parent's
  `worker_pid` write, the write returns rowcount 0. The parent previously
  treated every 0 as claim loss and terminated a VALID long-running workload.
  It now distinguishes owned success (same claim_token with status running or
  terminal — leave the runner alone) from genuine claim loss (mismatched
  token — terminate).
- **Ordinary starts can no longer resurrect terminal jobs (P1).** The no-token
  parent write that unconditionally set `status='running'` is now guarded by
  the run's original `started_at`. A fast job that emitted `completed`/`failed`
  (or was cancelled/orphaned) while the parent was returning from `Popen` is
  never written back to `running`.
- **A stale runner can no longer consume a newer claim token (P1).** Runners
  previously read the shared mutable `specs/{job_id}.json`, so a delayed runner
  from an old claim could read the replacement token after stale recovery. Each
  claim now writes a CLAIM-SPECIFIC spec (`specs/{job_id}-{claim_token}.json`)
  and the runner is given its own spec filename in argv; an old process can
  never acquire a newer run's identity. Claim-specific specs are cleaned up by
  the runner on success/abort and by job cleanup.
- **Stale recovery can no longer orphan a live run (P1).** Recovery used to
  snapshot a stale `launching` row, release the lock, and then transition with
  a helper that also accepted `running` — a runner promoting between the two
  got orphaned. Recovery now performs an ATOMIC launching-only, token-guarded
  transition (`status='launching' AND claim_token=?`); it reconciles/kills
  workload processes only AFTER winning that transition. If the runner promoted
  first, the guard returns 0 and the live workload is untouched.
- **Heartbeat reconciliation is run-identity guarded (P1).** Reconciliation
  finalized a stale `running` snapshot with an unguarded terminal update, so a
  newer restart could take ownership between PID reconciliation and the
  transition and then be orphaned by the old pass. The final update now
  requires the same claim_token (or the stale run's worker_pid for legacy rows).
- **Restart intent survives a claimed-but-unspawned launch (P1).** The restart
  deadline was cleared atomically with the claim, but a crash/disk error during
  spec construction left the row `launching`, recovery changed it to
  `orphaned`, and restart policy (which only watches failed/completed rows)
  dropped the already-budgeted retry. The claim now records the pre-clear
  deadline under `pending_restart_after`; an abandoned claim is recovered as
  `failed` with the deadline restored so the budgeted relaunch still fires. The
  pending intent is cleared once the launch is confirmed live (runner promoted
  or parent recorded worker_pid).

### Launch claims, runner ownership, and wheel-executable fixes (rc33)

- **POSIX wheels now bundle an executable monitor (P1).** Artifact download and
  `shutil.copyfile` do not preserve executable bits, so every published RC32
  Linux/macOS wheel stored `vanth/monitor-bin/vanth-monitor` as mode 0644 and
  failed with `PermissionError`. The build hook now `chmod 0755`s the injected
  binary for every non-Windows target.
- **Launch claims are exclusive across processes (P1).** `prepare_launch` now
  claims a job with a single guarded `UPDATE ... WHERE status IN (...)` whose
  rowcount==1 is authoritative, instead of SELECT-then-verify. Two
  `JobManager` instances can no longer both believe they own the same claim,
  because a post-UPDATE status SELECT cannot identify the writer.
- **The runner atomically promotes its owned claim (P1).** The runner promotes
  `launching -> running` guarded by a durable `claim_token` recorded in the run
  spec, and every runner terminal transition is claim-token guarded. A fast job
  can no longer finish while the row is still `launching` and then have the
  parent's unguarded update resurrect it as `running`.
- **Stale-claim recovery reconciles process ownership (P1).** Recovery now
  skips `launching` rows whose runner is still alive, terminates any workload
  PID before freeing the row, and emits an `orphaned` event through the normal
  terminal path so waits, wake targets, and feeds are notified.
- **Restart deadline clears atomically with the launch claim (P1).** The
  `restart_after` deadline and the `launching` claim are one guarded UPDATE, so
  a crash between the old deadline-clear and `prepare_launch` can no longer
  leave a failed job with its attempt consumed and no pending deadline (which
  turned `max_retries=1` into immediate `gave_up`).
- **Prebuilt target tagging falls back to the target tag (P2).** With a
  prebuilt binary + target GOOS/GOARCH and no explicit `VANTH_MONITOR_TAG`, the
  wheel is tagged for the target (`platform_tag_for(goos, goarch)`) instead of
  the build host.
- **RC tags publish as GitHub prereleases (P2).** The release workflow passes
  `--prerelease` for `*-rc*` tags so RC builds are not presented as stable
  releases.
- Adds `jobs.claim_token` (schema v11). Full suite: 630 passed, 6 skipped.

### Wheel bundles the Windows monitor with the correct `.exe` name

- **Windows wheels were missing the TUI binary** — every wheel is assembled on
  a Linux CI host, and the build hook named the bundled monitor binary from the
  BUILD host (no `.exe`) for every target. On Windows the runtime looks for
  `vanth/monitor-bin/vanth-monitor.exe`, so a Windows install reported the
  "native binary is not present" error (exit 2). The hook now names the bundled
  binary from the TARGET GOOS (`VANTH_MONITOR_GOOS`): Windows wheels bundle
  `vanth-monitor.exe`, POSIX wheels bundle `vanth-monitor`. The release
  workflow passes `VANTH_MONITOR_GOOS`/`VANTH_MONITOR_GOARCH` through to the
  wheel builds.
- Added build-hook regression tests for target-OS naming (Windows `.exe` vs
  POSIX no-suffix).

### Review fixes (RC30 policy + wake delivery reliability)

- **Launch claims are exclusive and stale claims recover (P1)** — `launching`
  is no longer in the runnable-status set, so two serialized `prepare_launch`
  calls cannot both succeed and double-spawn the same job. A claim abandoned
  by a crash (row stuck `launching`) is recovered to `orphaned` by the
  dispatch loop after `VANTH_LAUNCH_CLAIM_TIMEOUT` (default 30s), so the job
  becomes relaunchable.
- **Restart failures advance the failure streak (P1)** — automatic restarts
  reuse the job row, and `_watch_on_failure` previously suppressed every later
  failure once `last_failure_event_id` was set (a probe produced three `failed`
  events but a streak of one). The watcher now compares the newest failed event
  (ordered by `seq`, the per-job monotonic sequence — event ids are random
  UUIDs and not time-ordered) with the stored id, so each execution — original
  plus every restart — increments the streak exactly once.
- **Delivery leases cover the adapter's effective timeout (P1)** — thread
  bridges (codex/opencode) wait up to 300s by default, but the delivery lease
  defaulted to 30s + 5s margin, letting the dispatcher reclaim and re-send a
  wake while its turn was still running. The lease is now computed from each
  adapter's effective timeout (300s for thread targets, 30s otherwise), so a
  codex wake is never reclaimed mid-turn.
- **Webhook redirects cannot exfiltrate credentials (P1)** — urllib's default
  redirect handler forwarded configured headers (e.g. `Authorization`) to a
  cross-origin destination. A custom no-redirect handler fails 3xx deliveries
  instead of leaking; configured header secrets are also stripped from the
  JSON payload body (headers are sent as headers only).
- **Queue drain preserves settled history (P2)** — `clear_deliveries` without
  an explicit `status` now only touches `pending`/`retrying` rows; delivered
  records are audit history and require an explicit `status` to drain. Draining
  an in-flight row finalizes its `delivery_attempts` entry instead of leaving
  it `dispatching`.
- **Daemon no longer infers thread identity (P2)** — `JobManager.start` no
  longer falls back to the persistent daemon's `CODEX_THREAD_ID`/
  `OPENCODE_SESSION_ID` (which would inherit the thread that spawned the
  daemon); callers pass `origin_thread_id` explicitly (the MCP wrapper
  resolves it). Caller-owned wake-target dicts are copied before inherited ids
  or events are injected — never mutated.
- **Restart regression test de-flaked (P2)** — the polling-budget test observed
  for exactly the configured backoff window (2s) and could race the relaunch;
  it now uses an 8s backoff with a 2s observation window.

### Review fixes (RC27 policy + delivery reliability)

- **Restart budget consumed by launch, not polling (P1)** — the restart policy
  previously incremented `restart_attempts` and rescheduled an in-memory timer
  on every dispatcher tick because `restart_after` was never persisted. A probe
  exhausted three retries before the first timer fired. Restart bookkeeping now
  lives entirely in `policy_state`: one `restart_after` deadline is claimed
  atomically by a single dispatcher tick, no timers are involved (a daemon
  restart can't lose or double-schedule a pending relaunch), and polling never
  consumes budget.
- **Failure streaks count executions, not ticks (P1)** — a single failed run
  previously advanced `failure_streak` once per watcher poll (a `sys.exit(1)`
  with `after_n=3` tripped `failure_threshold` after three watcher calls). The
  watcher now records the terminal event id and folds each failed run into the
  streak exactly once.
- **Atomic launch gating (P1)** — `prepare_launch` now atomically verifies
  status + `policy_disabled` under one transaction and claims the row
  (`launching`) so concurrent callers cannot double-spawn; `run_job` can no
  longer launch an already-running reaction job (the probe's two-live-PIDs
  case), and the public `job_rerun` refuses a policy-disabled job.
- **Thread identity resolved in the MCP process (P1)** — `job_start` resolves
  `origin_thread_id` from the calling MCP task's environment (CODEX_THREAD_ID /
  OPENCODE_SESSION_ID) before POSTing to the daemon, and copies caller-owned
  wake-target dicts before injecting the inherited id (no caller mutation).
- **Remote jobs support policy end-to-end (P1)** — `policy` is accepted by the
  remote protocol (`START_OPTIONAL_FIELDS` + JSON schema), validated remotely,
  persisted on the remote queued job row, and carried across remote rerun.
- **Retention throttled + transactional (P1)** — per-job pruning runs at most
  once per `VANTH_RETENTION_MIN_INTERVAL_SECONDS` (default 60s) instead of a
  DELETE+commit on every 0.2s tick; deletion is rollback-on-error
  (no partial commits) and deleting deliveries cascades to their
  `delivery_attempts` (no orphans).
- **Dead-man's flags rearm on restart (P2)** — `_watch_schedule` tracks the
  observed `started_at` and clears `stuck_emitted`/`missed_emitted_at_elapsed`
  when an automatic restart reuses the same job row, so subsequent runs emit
  their own `job_stuck`/`schedule_missed`.
- **Interactive typo no longer hangs (P2)** — `vanth statsu` (unknown arg) in a
  terminal now prints `unknown command` and exits 2 instead of entering the MCP
  stdio loop; the TTY guard keys on interactive stdin alone (redirected stdout
  no longer masks it).

### Webhook wake target (notification channel beyond agent threads)

- **New `webhook` wake target type** — POSTs the delivery payload (same shape
  every adapter receives: `event`, `prompt`, `delivery_id`, `target`) as JSON
  to any HTTP(S) URL. One generic channel covers ntfy, Gotify, Telegram bots,
  Slack/Discord webhooks, PagerDuty Events, etc. — the roadmap's most-requested
  integration set (Discord, Telegram, Gotify, MQTT, IFTTT) without per-service
  code.
- Target config: `url` (required, http/https), `headers` (string key/value map
  for auth tokens / service presets), `timeout_seconds`. 2xx
  (200/201/202/204) marks the delivery `delivered`; other statuses or
  transport errors mark it failed (retried per `max_attempts` /
  `retry_delay_seconds`, then dead-lettered).
- Works everywhere wake targets work: `job_start` wake_targets, `daemon_wake`
  shorthand (`type="webhook", url=...`), and the remote protocol (schema enum
  updated).

### Delivery queue management + wake delivery reliability

- **Bulk delivery-queue clearing** — `vanth deliveries clear` (daemon route
  `POST /deliveries/clear`, MCP tool `job_clear_deliveries`). Filter by
  `job_id`, `status`, `older_than_seconds`, or `stale_only` (only deliveries
  whose source job is terminal). `dry_run` defaults to true so agents can
  preview what a drain would remove before committing; `limit` caps batch
  size.
- **Wake "delivered" now means the model actually ran** — the codex bridge
  previously returned `delivered` the instant `turn/start` acknowledged the
  turn (`inProgress`), then tore down the app-server process, killing the
  in-flight turn before the model acted on the wake. The bridge now waits
  for the `turn/completed` notification (matching the started turn id) and
  only then reports success; a failed turn is surfaced as a delivery error.
- **Turn-completion notification ordering** — the bridge buffers
  `turn/completed` notifications that arrive before the `turn/start`
  response (notification ordering is not guaranteed) so the completion
  waiter never misses them.
- **Wake delivery timeouts raised** — bridge default delivery timeout
  raised 30s -> 300s (opencode + codex), and `_complete_delivery` now
  retries with exponential backoff (5s x 3^(n-1), capped at 300s) instead
  of giving up after the first busy-session failure.

### Restart policies + retention pruning (job policies, continued)

- **Restart policies** — `policy.restart: {max_retries, backoff_seconds,
  backoff_max_seconds}` relaunches a failed job automatically with linear
  backoff capped at the max. Each relaunch emits `restarted` with attempt
  counts; the attempt budget is persisted before launch (crash-safe) and a
  successful completion resets it. When the budget is exhausted emits
  `gave_up` (level=error, flows to wake targets) once.
- **Retention pruning** — `policy.retention: {events_seconds,
  metrics_seconds, deliveries_seconds}` prunes a job's non-terminal events,
  metric points, and settled deliveries older than the TTL every dispatch
  iteration. Terminal events are always kept (status history survives).
  Log-retention without the per-entry paywall.
- **Stale-watcher race fix**: a previous run's `_watch_runner` could mark a
  relaunched job `orphaned` mid-boot (restart vs. watcher race). The watcher
  now verifies the recorded worker_pid still belongs to its own runner
  process before declaring the job dead.
- **Retention transaction hygiene**: a zero-match DELETE leaves an implicit
  transaction open which blocked runner processes for the full busy_timeout
  (30s) — retention now always settles the transaction, even when nothing
  matched.
- Fixed `DeadRunner` test-fake compatibility in `_watch_runner`.

### Dead-man's switch + failure reactions (job policies)

New per-job `policy` block on `job_start` (persisted, carried across
`rerun`, exposed in `job_status`), watched by the daemon dispatch loop:

- **Dead-man's switch** — `policy.schedule: {expected_interval_seconds,
  grace_period_seconds}` emits `schedule_missed` when no new run starts
  within interval+grace, and `job_stuck` when a run outlasts interval+grace.
  The daemon is the monitor: no external pinging service needed. Emitted
  once per window, reset on a fresh start.
- **Failure reactions** — `policy.on_failure: {after_n, action}` fires once
  the consecutive-failure streak reaches N (streak persists across reruns
  of the logical job; a completed run resets it):
  - `alert` emits `failure_threshold`
  - `disable` additionally sets a flag that blocks future launches
    (`prepare_launch` refuses disabled jobs)
  - `run_job` launches a named reaction job (e.g. cleanup/failover)

All policy events are `warning`/`error` level, flow to wake targets
(codex/opencode threads) and the delivery queue like any other event, and
carry structured data (`failure_streak`, `action`, `disabled`,
`reaction_job_id`, elapsed/interval/grace seconds).

Also: jobs launched via the dispatcher (`prepare_launch`/`_launch_prepared`)
now clear `exit_code`/`ended_at` so re-runs of a previously terminal job
start clean.

### Field-report fixes (from 1.6.0-rc23 pre-release use)

- **Wake targets inherit the launching thread by default**: a
  `codex_thread`/`opencode_thread` wake target without an explicit
  `thread_id`/`session_id` now resolves to the caller's thread
  (`origin_thread_id`, falling back to `CODEX_THREAD_ID` then
  `OPENCODE_SESSION_ID` from the MCP client environment) instead of failing
  permanently at delivery time with "requires thread_id". Explicit ids in
  the target always win, so agents can still fan out to other threads.
- **Bare `vanth` in a terminal no longer "hangs"**: with a TTY on stdin and
  stdout and no subcommand, the MCP stdio server refuses to start, prints
  where to find the dashboard (`vanth-monitor`) and human subcommands, and
  exits 2. Piped invocations (real MCP clients) are unaffected.
- **`vanth-monitor` fails fast without the bundled binary**: source/sdist
  installs raise a clear reinstall-from-platform-wheel error instead of the
  misleading "`go` not on PATH; run `uv build`" — local Go builds and
  standalone-binary overrides were removed so shipped wheels are the single
  supported path.

### RC22 review fixes

- **P1 lost-transport stop re-drive**: the public `stop()` convergence path
  now also re-drives response-less `submitting` requests (the durable state
  after transport loss), not just retry-pending `accepted` rows — a second
  same-key public call reopens transport and completes instead of returning
  the stale row.
- **P2 UNC staging paths**: Windows final-path normalization converts
  `\\?\UNC\server\share\...` to the intended `\\server\share\...` form, so
  valid UNC staging locations pass containment instead of being falsely
  rejected; `_final_path()` failures now close the opened handle before
  propagating (no leaked lock on the staging file).

### RC21 review fixes

- **P1 public stop convergence**: `control.stop()` (and any same-key public
  call) now RE-DRIVES a retry-pending request — an `accepted` row without a
  response goes back through `run_request` instead of returning the stale
  row. Regression test exercises two PUBLIC `stop()` calls through fail →
  pending → completed.
- **P1 snapshot/feed race**: the publish phase of a snapshot sync captures
  durable feed progress before fetching and ABORTS (`raced concurrent feed
  progress`) when cursor or timeline moved during fetch — a stale snapshot
  can no longer revert a newer shadow whose event would then be skipped.
- **P1 schema reconciliation**: `OPERATION_RETRY_PENDING` added to the JSON
  Schema error enum and `remote-errors-v1.json`; `STATE_EPOCH_MISMATCH`
  reconciled into both as well.
- **P2 Windows containment fails closed**: final-path resolution resizes its
  buffer as required and raises on API failure; handle-identity queries that
  cannot be completed abort the open instead of proceeding unvalidated.
- **P2 portable publication**: source-type inspection failures propagate
  instead of silently taking the non-atomic rename fallback.
- Snapshot page fetches run outside the global controller DB lock (publish
  phase still validates and holds it).

### RC20 adversarial review fixes

- **P1 retryable stops end-to-end**: transient stop failures return the new
  `OPERATION_RETRY_PENDING` error code; the controller keeps its request
  PENDING (never failed, no replay tombstone), and `accepted -> submitting`
  is now a legal request transition so same-key retries re-drive the stop
  and observe the eventual remote success. Verified by a full
  fail→pending→recover→complete controller cycle test.
- **P1 portable directories**: the fallback inspects the SOURCE
  descriptor-relatively (`lstat(dir_fd=...)`) before deciding, so directory
  publication fails closed on every platform lacking atomic no-replace —
  including the dir_fd-supplied call path used in production.
- **P1 Windows staging**: `_BY_HANDLE_FILE_INFORMATION` uses the exact ABI
  (FILETIME as two DWORDs — a c_uint64 misaligned every later field), and
  the I/O handle is validated via `GetFinalPathNameByHandleW` against the
  intended staging path, catching ancestor-junction redirects that leaf
  identity comparison alone could not.
- **P2 cleanup metadata ordering**: `wrapper_path` + `cleanup_pending=1`
  are committed BEFORE the first remote mutation of a pairing, so a lost ACK
  after a successful wrapper write always leaves a recorded obligation.
- **P2 stale-feed result fields**: the stale-batch branch derives ALL
  top-level epoch fields from the ACCEPTED durable cursor.
- Snapshot sync fetches paginated pages OUTSIDE the global controller DB
  lock; only the apply/publish transaction holds it.

### RC19 adversarial review fixes

- **P1 cross-timeline feed batches**: the apply-path guard now rejects ANY
  divergence between the durable cursor and the request/response timeline
  before touching shadows — foreign-epoch responses are additionally caught
  upstream by gap-recovery (snapshot resync), and `upsert_shadow` itself
  refuses writes bound to an older epoch than the shadow already carries.
  `feed_sync` reports the cursor DURABLY ACCEPTED, not the response's.
- **P1 zero-progress pull chunks**: an empty served window aborts the
  transfer (`no progress`) instead of spinning on the same offset forever.
- **P1 staging TOCTOU**: staging opens are now descriptor-relative on POSIX
  (`openat` walk with O_NOFOLLOW per component + leaf regular-file fstat);
  Windows uses a reparse-safe probe handle plus a second I/O handle compared
  BY FILE IDENTITY, so a synchronized parent/leaf swap aborts instead of
  redirecting access. The check-then-open pattern is gone from both
  controller pull and remote push staging.
- **P1 pairing orphan risk**: cleanup metadata is persisted BEFORE any
  remote mutation (`remotes.cleanup_pending=1` + wrapper path), cleared only
  after provable installation; removal attempts revocation for pending rows
  even without a stored authorization line, and refuses to delete records
  with live remote state unless forced.
- **P2 schema**: transfer_init requires `version_id` only under the pull
  conditional — canonical push frames validate against the JSON Schema too.
- **P2 remote push corruption**: a whole-content hash mismatch at completion
  resets the remote ledger offset AND truncates its staging file, returning
  `expected_offset=0` so the controller's classifier retransmits from zero
  instead of wedging at EOF forever.
- **P2 portable publication**: directory trees no longer fall back to
  lstat+rename when atomic primitives are missing — they fail closed with
  ENOSYS instead of racing a clobber.
- Transient stop failures now surface as ERROR frames so controllers never
  mark a still-queued stop as completed.

### RC18 adversarial review fixes

- **P1 deadlock**: the separate epoch lock is gone — state-epoch rotation
  and transfer publication both serialize on the store's `db_lock`, so the
  restore `db_lock→epoch_lock` vs publication `epoch_lock→db_lock` inversion
  can no longer deadlock (single-lock ordering).
- **P1 stale feed batches**: `feed_sync` now validates durable progress
  BEFORE applying anything — a batch whose end seq is at or below the stored
  cursor on the same timeline is skipped wholesale (`stale_batch_skipped`),
  so a stale `running` can never overwrite fresher shadows again.
- **P1 verified-bytes binding**: push completion and pull assembly each read
  ONE buffer through the no-follow handle, hash THAT buffer, and publish it;
  a same-size swap between verify-open and publish-reopen can no longer
  publish unverified data.
- **P1 staging containment**: every staging access sweeps its full ancestor
  chain (symlink/junction/reparse ancestors abort), and Windows leaves are
  checked for `FILE_ATTRIBUTE_REPARSE_POINT` via a
  `FILE_FLAG_OPEN_REPARSE_POINT` handle before any open.
- **P1 pairing cleanup handles**: `_compensate` only deletes local
  credentials when BOTH remote cleanup steps succeeded; `remove_remote`
  refuses to delete a record whose local revocation material is missing
  unless `force=True`.
- **P1 darwin AT_FDCWD**: `renameatx_np` gets Darwin's `-2`, not Linux's
  `-100`; relative non-overwrite publication works on macOS.
- **P2 resume wedge**: same-length staging corruption now fails whole-buffer
  verification, resets ledger+staging to zero, and restarts once from zero
  within the same call.
- **P2 transfer bindings**: push acks must carry epoch/acked_offset and
  acknowledge EXACTLY the sent window; pull serve responses require all
  binding fields; pull init requires `version_id` (runtime + spec); pull
  completion without `version_id` is an INVALID_REQUEST instead of a
  KeyError.

### RC17 adversarial review fixes

- **P1 stop retry semantics**: a transient `stop_sync` failure leaves the
  stop intent NONTERMINAL (`retrying: true` in the response) so the
  dispatcher re-drives it after recovery; only permanent validation failures
  (unknown target) are terminal. The dispatcher's reconciliation gained the
  same unknown-job fast-fail and already-terminal shortcut.
- **P1 pairing cleanup**: the exact wrapper path is persisted on the remote
  row at pair time; compensation and removal always target THAT per-remote
  file (legacy shared-name cleanup is best-effort). When remote revocation
  fails, local credentials and the DB row are RETAINED — `force=True`
  deletes anyway.
- **P1 cursor regression**: feed-cursor updates on one timeline are now
  compare-and-set forward-only (`_advance_feed_cursor`); gap recovery adopts
  the boundary the snapshot actually wrote instead of overwriting it with
  values from the stale feed response.
- **P1 staging containment**: pull staging lives exclusively under
  `<home>/remote-pull-staging/<transfer_id>.part` (never beside an arbitrary
  destination), opened no-follow via fd; remote push staging opens are
  leaf-symlink-proof (`O_NOFOLLOW`) across chunk receive, serve, hashing,
  and init zeroing.
- **P1 pull resume**: resume uses the controller's durable ledger offset;
  a staging file missing or truncated below it resets BOTH to zero rather
  than extending a zero-filled prefix that could never verify.
- **P1 atomic epoch fence**: publication holds the store's new epoch-rotation
  lock across put_file, whose `publish_guard` re-checks the epoch INSIDE the
  catalog transaction right before commit — a concurrent timeline rotation
  cannot land between check and publish.
- **P1 POSIX publication**: macOS uses `renameatx_np(RENAME_EXCL)` for
  atomic no-replace renames; Linux degrades to the portable hardlink/checked
  path when renameat2 is unavailable; directory tree construction stays
  descriptor-relative via `/dev/fd` on macOS.
- **P2**: completion responses must carry state_epoch/sha256/total_bytes/
  version_id (+root/manifest identity echoes added to both directions);
  unbound error frames are rejected like unbound responses; the protocol
  spec's transfer_init/transfer_complete payload definitions now match the
  runtime validators.
### Sol review fixes (second re-review)

- **P0**: replayed mutations keep the epoch binding SQLite stored — the
  controller no longer overwrites `expected_state_epoch` in memory, so a
  retry can never silently rebind while the durable row and journal keep the
  original.
- **P1 materialization ordering**: the fail-closed parent sweep now runs
  BEFORE any `mkdir` — file materialization, directory materialization, and
  pull staging never create directories through a symlink/reparse ancestor
  that the sweep is about to reject.
- **P1 stop semantics**: a failed stop records the op as FAILED (replays its
  failure durably) instead of completed; successful terminal stops emit
  full terminal UPSERTS (name/command/status/exit_code) instead of
  tombstones, so controller shadows learn the final status.
- **P1 wrapper isolation**: every pairing installs its OWN
  `remote-wrapper-<remote_id>.sh`; multiple remotes no longer overwrite or
  delete each other's forced-command target; literal remote-home paths with
  spaces are shell-quoted inside the forced command.
- **P1 snapshot feed boundary**: snapshot pages carry the feed boundary
  (`MAX(remote_feed.seq)` + feed_epoch) captured at page 1; the controller
  fail-fasts on missing/drifting boundaries and advances its stored feed
  cursor to that boundary at finalize — stale feed events can no longer
  regress fresher snapshot state.
- **P1 macOS support restored**: atomic publication falls back to
  hardlink-based no-replace publish for files (checked rename otherwise)
  outside Linux's renameat2; directory staging uses plain paths with a
  dev/inode cross-check of the opened parent where `/proc/self/fd` does not
  exist.
- **P2 transfer binding**: response frames must echo request_id+method;
  init/chunk/completion results must name the transfer, stay in range, and
  agree on epoch/content identity before bytes are adopted.
- **P2 restore temp names**: prepared restore databases include a random
  suffix so concurrent restores in one process cannot collide.

### Re-review fixes (remote-artifacts-rc14-rereview.md)

- **P0-1 pairing**: host-key fallback writes and uses a real OpenSSH config
  and cannot authenticate unless the caller explicitly selected TOFU;
  fingerprinting preserves the real key type. The forced-command wrapper is
  a syntax-checked literal POSIX script, reads daemon URL/token only at exec
  time, honors an explicitly configured Vanth home and helper path, and is
  removed during compensation/removal. Sentinel hello is bound to the paired
  remote ID plus authenticated daemon instance ID and state epoch.
- **P0-2 snapshots**: the remote materializes one immutable job/event view and
  serves every page from it. The controller verifies snapshot ID, epoch and
  high-water, stages every page, then publishes/reconciles in one transaction;
  failed or expired syncs leave shadows, epoch and cursor unchanged.
- **P1-1 concurrency**: remote and controller multi-statement transactions run
  under their store locks; 30-thread remote-start and controller-submit stress
  tests pass deterministically.
- **P1-2 durable fencing**: mutations require both expected epoch and stable
  daemon instance ID. Both are persisted with requests and journal retries;
  replay requires the original binding and never rebinds it. Response
  request-ID/method matching is mandatory.
- **P1-3 caller keys preserved** through HTTP → payload → submit.
- **P1-4 stop intents recoverable + trigger validation**: accepted stops are
  reconciled by the dispatcher after crashes; malformed/unknown triggers
  cancel instead of launching; already-terminal stop is an idempotent no-op
  (fixes the reproducible full-suite red).
- **P1-5/P1-6**: journal connection is thread-safe; queued, terminal, stop and
  tombstone feed records commit with their state transitions; production DefaultConfig wires
  RemoteDBPath so monitor shadow merging works without manual config.
- **P1-7 put_dir fence encloses catalog commit** (GC can no longer delete
  blobs between publish and version commit).
- **P1-10 transfers**: pull staging is retained and re-hashed for resume;
  missing/truncated staging resets to zero. Completion requires and validates
  epoch, bytes, whole-content SHA, root, manifest and exact version ID. Push
  publication checks the epoch inside the catalog commit; same-digest versions
  cannot substitute across roots.
- **P1-10b controller ledger**: `controller_transfers` no longer
  global-unique-keys idempotency (transfer ids bind context); existing
  databases are migrated automatically; takeover verifies the ledger row
  against the requested remote/direction/content binding; pull-derived
  publication/materialization op keys are scoped by destination token so
  shared caller keys across destinations cannot collide.
- **P2-5**: transfer protocol tests use direct `pytest.raises` again (the
  NameError-swallowing helper is gone).
- P2: descriptor-bound/no-follow log reads; recovery-marked catalog restore;
  atomic POSIX no-replace artifact publication; structured remote-wait errors;
  IPv6 bracket targets; dir-version dedup verifies/repairs all blobs; leases
  renew during long artifact loops; collection append returns its persisted
  timestamp; StorageProfiles.update has durable idempotency and is exposed as
  a guarded route/tool.

### Earlier rc14 items (monitor wiring, sweeps, transfer binding)

- **P1-7 monitor wiring**: the Go monitor now consumes remote shadows —
  `Config.RemoteDBPath` makes `Refresh` merge current-timeline shadow
  projections into the job list (failures are non-fatal warnings), with an
  end-to-end refresh test proving `job_live` arrives flagged as a remote.
- **P1-11**: materialization rejects symlink/reparse ancestors on every
  platform. POSIX final publication additionally traverses parents with
  descriptor-relative `O_NOFOLLOW` opens and uses atomic no-replace rename;
  directory publication can no longer replace a raced-in empty directory.
- **P2-5 transfer completion binding**: push completion validates the
  published version against the registered identity on sha256, total_bytes,
  manifest digest, AND root name, and re-checks the epoch immediately before
  acknowledging — any drift stops the transfer instead of committing.
- **P2-7 publication intent ledger**: put_file/put_dir write an explicit
  `<op_id>.intent.json` (content shas + manifest digest) before the first
  blob replace, removed only after the catalog commit — a crash in that
  window leaves discoverable evidence of exactly what was being published.
- **P2-9 storage profiles**: config is whitelisted to
  bucket/prefix/region/endpoint_url; secret-shaped keys are rejected outright
  before the whitelist; output configs are redacted on read; custom
  `endpoint_url` requires an explicit `VANTH_S3_ENDPOINT_ALLOWLIST` (SSRF).
- **P2-10 multipart**: InMemoryProvider completion enforces contiguous
  1..N part numbers, rejects duplicates, verifies every supplied ETag
  against stored parts, and assembles by part number (not list order);
  Boto3Provider detects S3's HTTP-200-with-embedded-error completion form.
- **P2-11**: caller-supplied idempotency keys exposed across the alias/
  delete/restore/pin/unpin/gc MCP tool surface (daemon routes already
  accepted them).
- **P2-15 capabilities-as-observations**: probe results are recorded in a
  separate `capability_observations` table with provenance/time; the
  immutable revision row is never rewritten (`get()` attaches the newest
  observation).
- **CLI**: `vanth remote pair` gains `--host-fingerprint <SHA256>` and
  `--accept-host-key`, matching the P0-1 host-key pinning contract.

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
- **P1-9 GC/publication fence**: blob publication (put_file/put_dir) and GC's
  unlink phase hold the same O_EXCL root fence, and GC re-verifies
  reachability inside the fence — a publisher can no longer commit a version
  whose blob GC just deleted.
- **P1-10 Restore crash windows closed**: backups are validated into a temp
  database (integrity_check + schema ceiling) BEFORE touching the live
  catalog; the recovery lockout is applied to the live catalog BEFORE content
  moves (any crash leaves it locked, never writable with a stale identity);
  `complete_restore` rewrites the blob owner marker first and only then
  unlocks mutations.
- **P1-12/P1-13**: storage-profile create/update/probe are gated behind
  `recovery_required`; S3-backed managed-artifact storage is explicitly
  marked UNSUPPORTED this release (provider/lease machinery only) until a
  full provider-side publication round trip ships.
- **P2 fixes**: dedup verifies the referenced blob before returning an
  existing version (corrupt content is republished instead of returned);
  Windows reserved-name validation covers basenames before extension after
  trailing dot/space trimming (`CON.txt`, `LPT1.log`); alias CAS refuses
  cross-root movement as a separate explicit error (`ALIAS_CROSS_ROOT_MOVE`);
  long artifact operations renew their claim lease between work units.
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
