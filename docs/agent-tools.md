# Vanth agent tool surface

This is the reference contract for the MCP tools an agent sees when connected
to Vanth. Every tool talks to the local daemon over HTTP; the daemon is
started on demand if it isn't already running. All responses are JSON objects.

Conventions:

- `job_id` values look like `job_<hex>`; `event_id` like `evt_<hex>`;
  `delivery_id` like `del_<hex>`; `artifact_id` like `art_<hex>`.
- Errors are returned as `{"result": "error", "error": "<message>"}` (HTTP 4xx
  on the wire) rather than raised.
- Timestamps are ISO-8601 UTC strings with a `Z` suffix (e.g.
  `2026-08-18T12:00:00Z`).
- `limit` is validated to 1–1000 (up to 10000 for metrics, 50000 for
  dashboard, 1000 for artifacts).

The current tool set is `job_start`, `job_rerun`, `job_status`, `job_send`,
`job_list`, `job_view`, `job_events`, `job_deliveries`, `job_mark_delivery`,
`job_retry_delivery`, `job_delivery_attempts`, `job_tail`, `job_wait`,
`job_stop`, `job_doctor`, `job_cleanup`, `job_metrics_query`,
`job_metric_compare`, `job_run_summary`, `job_artifact_add`, `job_artifacts`,
`job_dashboard`.

The following are part of the planned v1.4 surface (in progress, not yet
released): `job_metric_ingest`, `job_artifact_read`, `daemon_wake`,
`job_cleanup_preview`.

---

## `job_start`

Launch a command as a detached job. The job keeps running even if the MCP
client or daemon restarts.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `command` | `string` | required | Shell command to run detached |
| `cwd` | `string?` | `None` | Working directory |
| `name` | `string?` | `None` | Human-readable label |
| `env` | `map<string,string>?` | `{}` | Extra environment |
| `timeout_seconds` | `int?` | `None` | >= 1; `None` = no timeout (enforced even across daemon restarts) |
| `notify_on` | `string[]?` | `None` | Event types that become default wake-target `events` |
| `wake_targets` | `object[]?` | `None` | See README "Wake targets"; `{type, events, ...config}` |
| `origin_thread_id` | `string?` | `None` | The agent thread that launched it (defaults to `CODEX_THREAD_ID`) |
| `tags` | `string[]?` | `None` | Arbitrary labels, filterable in `job_list` |
| `notes` | `string?` | `None` | Free-form annotation shown in the monitor |
| `interactive` | `bool` | `false` | Open stdin for `job_send` |

**Response**

```json
{
  "job_id": "job_abc123",
  "status": "running",
  "worker_pid": 4242,
  "stdout_path": "C:/Users/you/.vanth/logs/job_abc123.stdout.log",
  "stderr_path": "C:/Users/you/.vanth/logs/job_abc123.stderr.log",
  "events_path": "C:/Users/you/.vanth/events/job_abc123.jsonl",
  "message": "Job started"
}
```

On runner-launch failure: `status: "failed"`, `exit_code: 1`, `message` with
the cause. On quota exhaustion (`VANTH_MAX_RUNNING_JOBS`):
`{"result": "error", "error": "concurrent job quota reached (N running jobs)"}`.

---

## `job_rerun`

Re-launch a job with its **original** command, cwd, env, timeout, name, tags,
notes, origin thread, wake targets, and interactive flag.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job to re-launch |

**Response**

Same shape as `job_start` (new `job_id`). Errors if `job_id` is unknown.

---

## `job_status`

One job's full status. The fastest way for an agent to answer "what is this
job doing?"

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job to inspect |

**Response**

```json
{
  "job_id": "job_abc123",
  "status": "running",
  "command": "python train.py",
  "cwd": "F:/git/project",
  "timeout_seconds": 3600,
  "pid": 4141,
  "worker_pid": 4242,
  "name": "training run",
  "origin_thread_id": "019f...",
  "wake_thread_id": null,
  "tags": ["training", "gpu"],
  "env": {"CUDA_VISIBLE_DEVICES": "0"},
  "notes": null,
  "run": {"author": "you", "hostname": "..."},
  "runtime_seconds": 12.3,
  "created_at": "2026-08-18T12:00:00Z",
  "updated_at": "2026-08-18T12:00:12Z",
  "exit_code": null,
  "last_event": { "event_id": "evt_...", "job_id": "job_abc123", "seq": 4,
                  "type": "progress", "level": "info", "message": "...",
                  "data": {}, "source": "stdout", "created_at": "..." },
  "progress": {"current": 10, "total": 100, "percent": 10.0,
               "stage": "train", "updated_at": "..."}
}
```

`progress` is the latest `progress` event's data plus `updated_at`, or `null`.

---

## `job_send`

Feed stdin to a running interactive job. Start the job with
`interactive=True` first.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Running interactive job |
| `input` | `string` | required | Bytes to append to the job's stdin |
| `eof` | `bool` | `false` | Close the job's stdin (`eof=True` with empty `input` allowed) |

**Response**

```json
{ "job_id": "job_abc123", "sent": 3, "eof": false }
```

Errors for unknown / not-running / non-interactive jobs.

---

## `job_list`

Recent jobs, ordered by most-recently-updated.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `status` | `string[]?` | `None` | Filter by one or more statuses |
| `limit` | `int` | `50` | 1–1000 |
| `thread_id` | `string?` | `None` | Matches `origin_thread_id` or `wake_thread_id` |
| `name` | `string?` | `None` | Substring match on job name |
| `tags` | `string[]?` | `None` | Must contain all listed tags |

**Response**

```json
{ "jobs": [ { "job_id": "job_abc123", "name": null, "status": "running",
              "updated_at": "...", "origin_thread_id": "...",
              "wake_thread_id": null, "tags": [] } ] }
```

---

## `job_view`

Agent-facing summaries sorted by attention priority (failed/timeout/orphaned
first, then pending/failed deliveries, then jobs with attention events, then
the rest).

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `thread_id` | `string?` | `None` | Filter to one thread's jobs |
| `limit` | `int` | `50` | 1–1000 |

**Response**

```json
{ "jobs": [ { "job_id": "job_abc123", "status": "failed", "...status fields...",
              "delivery_counts": {"failed": 1}, "priority": 175 } ] }
```

Each entry is a `job_status` object plus `delivery_counts` and `priority`.

---

## `job_events`

Structured events for a job.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job whose events to read |
| `since_event_id` | `string?` | `None` | Forward paging cursor: events after this one |
| `types` | `string[]?` | `None` | Filter by event type (`progress`, `checkpoint`, `metric`, ...) |
| `limit` | `int` | `20` | 1–1000 |
| `reverse` | `bool` | `false` | Newest events first; combine with `since_event_id` to page backward |

**Response**

```json
{ "events": [ { "event_id": "evt_...", "job_id": "job_abc123", "seq": 4,
                "type": "progress", "level": "info", "message": "10/100 epochs",
                "data": {"current": 10, "total": 100}, "source": "stdout",
                "created_at": "2026-08-18T12:00:00Z" } ] }
```

---

## `job_deliveries`

Wake deliveries for a job (or across all jobs), filterable by status.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string?` | `None` | Filter to one job |
| `status` | `string?` | `None` | `pending`, `dispatching`, `retrying`, `delivered`, `failed` |
| `limit` | `int` | `20` | 1–1000 |

**Response**

```json
{ "deliveries": [ { "delivery_id": "del_...", "event_id": "evt_...",
                    "target_id": "target_...", "job_id": "job_abc123",
                    "target_type": "codex_thread", "status": "delivered",
                    "attempts": 1, "payload": {"target": {}},
                    "created_at": "...", "next_attempt_at": null,
                    "delivered_at": "...", "last_error": null,
                    "claim_token": null, "claimed_at": null,
                    "lease_expires_at": null } ] }
```

---

## `job_mark_delivery`

Manually set a delivery's status (e.g. after resolving an adapter problem).

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `delivery_id` | `string` | required | Delivery to mark |
| `status` | `string` | required | `pending`, `retrying`, `delivered`, `failed` |
| `error` | `string?` | `None` | Optional reason (recorded on the attempt) |

**Response**

The full `job_deliveries` delivery object for the updated delivery
(`attempts` incremented).

---

## `job_retry_delivery`

Requeue a delivery for dispatch immediately — including one currently
`retrying` on backoff (resets `next_attempt_at`).

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `delivery_id` | `string` | required | Delivery to requeue |

**Response**

The full delivery object with `status: "retrying"`, `next_attempt_at: null`,
`last_error: null`.

---

## `job_delivery_attempts`

Attempt/lease history for one delivery.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `delivery_id` | `string` | required | Delivery whose attempts to read |
| `limit` | `int` | `20` | 1–1000 |

**Response**

```json
{ "attempts": [ { "attempt_id": "att_...", "delivery_id": "del_...",
                  "attempt": 1, "claim_token": "...", "target_type": "codex_thread",
                  "started_at": "...", "ended_at": "...", "status": "delivered",
                  "error": null, "reclaimed": false, "created_at": "..." } ] }
```

`reclaimed: true` means the lease expired and the delivery was re-claimed after
a daemon crash ambiguity.

---

## `job_tail`

Bounded stdout/stderr log tail with byte offsets.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job whose log to read |
| `stream` | `string` | `stdout` | `stdout` or `stderr` |
| `max_bytes` | `int` | `8192` | Max bytes to read |
| `offset` | `int?` | `None` | Byte offset to start from; `None` = last `max_bytes` bytes |

**Response**

```json
{ "job_id": "job_abc123", "stream": "stdout", "offset": 0,
  "next_offset": 1234, "size": 4096, "truncated": false, "content": "..." }
```

`truncated` is true when the requested window was clipped to the log size or
the byte cap. Use `next_offset` to page forward.

---

## `job_wait`

The heart of agent usage: block until the first event matching any filter is
persisted, then return it with the current status. The daemon wakes the wait
immediately — do not poll.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job to wait on |
| `filters` | `string[]` | required | Event types to wait for (e.g. `["checkpoint","failed","completed"]`) |
| `since_event_id` | `string?` | `None` | Only events newer than this one |
| `timeout_seconds` | `int` | `3600` | 0–86400 |

**Response**

```json
{ "result": "event", "job_id": "job_abc123", "status": "running",
  "event": { "event_id": "evt_...", "type": "checkpoint", "..." } }
```

Timeout: `{"result": "timeout", "job_id": ..., "status": ..., "message": "No matching event before timeout"}`.
Daemon shutdown: `{"result": "shutdown", "job_id": ..., "message": "Vanth is shutting down"}`.

---

## `job_stop`

Stop a running job by terminating its process tree.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Running job to stop |
| `signal` | `string` | `terminate` | `terminate` (graceful) or `kill` |
| `kill_after_seconds` | `int` | `10` | 0–86400; escalate to force-kill after this |

**Response**

```json
{ "job_id": "job_abc123", "status": "cancelled", "message": "Job stopped" }
```

The job becomes `cancelled` only after the workload tree actually terminated;
otherwise the stop is retryable (and a `RuntimeError` "Failed to stop workload
process tree" is returned).

---

## `job_doctor`

Daemon health report.

**Parameters**

None.

**Response**

```json
{
  "ok": true,
  "home": "C:/Users/you/.vanth",
  "db_path": "...", "logs_dir": "...", "events_dir": "...",
  "tables": ["jobs", "events", "..."],
  "delivery_counts": {"pending": 0, "delivered": 3},
  "codex": {"command": "codex", "available": true},
  "opencode": {"command": "opencode", "available": true},
  "schema_version": 8,
  "quick_check": "ok",
  "maintenance_alive": true,
  "stale_delivery_leases": 0,
  "dead_letter_count": 0,
  "dead_lettered": [],
  "running_jobs": 1, "max_running_jobs": 0,
  "retention": {"seconds": 0, "interval_seconds": 3600, "dry_run": true},
  "disk_free_bytes": 123456789,
  "token_path": "C:/Users/you/.vanth/token",
  "warnings": []
}
```

`ok` is false when there are warnings (e.g. missing tables, Codex/OpenCode
unavailable) or `quick_check != ok`. `dead_lettered` lists up to 20 deliveries
that exhausted `max_attempts`, each with `delivery_id`, `job_id`, `attempts`,
`last_error`. The token is never revealed.

---

## `job_cleanup`

Dry-run or real removal of old terminal jobs. Running jobs are never selected.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `older_than_seconds` | `int` | required | Non-negative cutoff age |
| `dry_run` | `bool` | `true` | Preview only (fully read-only); `false` deletes |

**Response**

```json
{ "dry_run": true, "older_than_seconds": 86400,
  "jobs": ["job_old1", "job_old2"], "count": 2 }
```

With `dry_run: false`, removes logs, event mirrors, specs, deliveries,
attempts, wake targets, events, stdin channels, and the job row (tombstoned,
idempotent, safe to repeat).

---

## `job_metrics_query`

Read stored scalar metric series for a job (loss, accuracy, `progress.percent`,
...). The read side of the terminal monitor's data.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job whose series to read |
| `metric` | `string?` | `None` | One series name; omit for all metrics |
| `from_ms` | `int?` | `None` | Epoch-ms lower bound |
| `to_ms` | `int?` | `None` | Epoch-ms upper bound |
| `limit` | `int` | `1000` | Up to 10000 |

**Response**

```json
{ "job_id": "job_abc123",
  "series": { "loss": [ { "x": 0, "y": 0.5, "stage": "train",
                          "event_id": "evt_...", "seq": 1, "at": "..." } ] },
  "metrics": ["loss"] }
```

---

## `job_metric_compare`

Compare one metric across jobs — the "which run won?" primitive.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_ids` | `string[]` | required | Non-empty, at most 50 |
| `metric` | `string` | required | Metric name |
| `aggregation` | `string` | `latest` | `latest`, `mean`, `min`, `max`, `sum`, `count` |
| `from_ms` | `int?` | `None` | Epoch-ms lower bound |
| `to_ms` | `int?` | `None` | Epoch-ms upper bound |

**Response**

```json
{ "metric": "val_loss", "aggregation": "min",
  "jobs": { "job_a": { "value": 0.41, "points": 5,
                       "first": { "x": 0, "y": 0.8, "..." },
                       "last": { "x": 4, "y": 0.41, "..." } } } }
```

`value` is `null` when a job has no points for the metric.

---

## `job_run_summary`

One-call "did it work?" — status, runtime, progress, metric overview, artifacts.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job to summarize |

**Response**

```json
{
  "job_id": "job_abc123", "status": "completed", "name": "training run",
  "runtime_seconds": 123.4, "exit_code": 0,
  "progress": {"current": 100, "total": 100, "percent": 100.0},
  "notes": null,
  "metrics": [ { "metric": "loss", "latest": 0.1, "first": 0.9,
                 "min": 0.1, "max": 0.9, "count": 10, "stage": "train" } ],
  "latest_metrics": {"loss": 0.1},
  "artifacts": [ { "artifact_id": "art_...", "name": "best.pt", "uri": "file:///...", "..." } ]
}
```

---

## `job_artifact_add`

Attach an artifact (checkpoint, CSV, rendered output) to a job.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job to attach to |
| `name` | `string` | required | Artifact name (non-empty) |
| `uri` | `string` | required | Where the artifact lives (non-empty) |
| `kind` | `string?` | `None` | e.g. `checkpoint`, `csv`, `output` |
| `size_bytes` | `int?` | `None` | Optional size |
| `sha256` | `string?` | `None` | Optional content hash |
| `meta` | `object?` | `{}` | Free-form JSON |

**Response**

```json
{ "artifact_id": "art_...", "job_id": "job_abc123", "name": "best.pt",
  "uri": "file:///...", "kind": "checkpoint", "size_bytes": 42,
  "sha256": "...", "meta": {"epoch": 5}, "created_at": "..." }
```

---

## `job_artifacts`

List artifacts attached to a job.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job whose artifacts to list |
| `limit` | `int` | `50` | Up to 1000 |

**Response**

```json
{ "artifacts": [ { "artifact_id": "art_...", "job_id": "job_abc123",
                   "name": "best.pt", "uri": "file:///...", "kind": "checkpoint",
                   "size_bytes": 42, "sha256": "...", "meta": {},
                   "created_at": "..." } ] }
```

---

## `job_dashboard`

Chart-data view for any renderer: every stored metric series (downsampled per
job) plus the job list — the same data the Go terminal monitor charts.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_ids` | `string[]?` | `None` | Jobs to chart; omit for all (max 50) |
| `limit` | `int` | `5000` | Max points per series, up to 50000 |

**Response**

```json
{ "jobs": [ {"job_id": "job_abc123", "name": null, "status": "running", "..."} ],
  "series": { "job_abc123": { "loss": [ {"x": 0, "y": 0.5, "..."} ] } },
  "series_count": 3 }
```

---

## Planned (v1.4, in progress — not yet released)

These tools are being added in parallel and are documented here as part of the
planned surface.

### `job_metric_ingest`

Write scalar metric points into a job's metric series programmatically
(complementing the read-only `job_metrics_query`).

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job whose series to extend |
| `metrics` | `object[]` | required | One or more points: `{name, value, ts_ms?, labels?}` |
| `idempotency_key` | `string?` | `None` | Replay protection — a repeated key is a no-op |

**Response**

```json
{ "job_id": "job_abc123", "ingested": 2, "idempotency_key": "..." }
```

### `job_artifact_read`

Read a stored artifact's metadata and (when it is a local file) contents back
out of a job — the read side of `job_artifact_add`.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job owning the artifact |
| `artifact_id` | `string` | required | Artifact to read |
| `max_bytes` | `int?` | `None` | Cap on returned content |

**Response**

```json
{ "artifact_id": "art_...", "job_id": "job_abc123", "name": "best.pt",
  "uri": "file:///...", "kind": "checkpoint", "size_bytes": 42,
  "sha256": "...", "meta": {}, "content": "...", "truncated": false }
```

### `daemon_wake`

Request the daemon's attention from inside a job context — surfaces an
agent-facing wake without requiring a matching wake-target event.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string?` | `None` | Job to wake on |
| `message` | `string` | required | Human/agent-readable reason |
| `data` | `object?` | `{}` | Free-form context |

**Response**

```json
{ "result": "queued", "wake_id": "wake_...", "job_id": "job_abc123" }
```

### `job_cleanup_preview`

A dedicated dry-run retention preview: reports exactly what would be removed
without deleting anything. Separate from the destructive `job_cleanup`.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `older_than_seconds` | `int` | required | Cutoff age |

**Response**

```json
{ "dry_run": true, "older_than_seconds": 86400,
  "jobs": [ {"job_id": "job_old1", "status": "completed", "updated_at": "...",
             "size_bytes": 1024} ],
  "total_size_bytes": 2048, "count": 2 }
```
