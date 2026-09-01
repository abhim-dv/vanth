# vanth

Event-driven background jobs for agents.

Vanth is a localhost background-job daemon with a Model Context Protocol (MCP)
interface. It runs detached, non-interactive shell commands; captures their
output durably; parses optional `AGENT_EVENT` structured events into progress
bars, metric series, and checkpoints; and can wake a Codex or OpenCode session
when a job needs attention. It is built for one trusted user on one machine.

- **Any command**: downloads, image/audio processing, ETL, ML training — if it
  runs in a shell, Vanth can run it detached and track it.
- **Durable**: jobs and events live in SQLite (`WAL`, busy-timeout) and survive
  daemon, MCP, and machine restarts.
- **Event-first**: agents `job_wait` for meaningful events instead of polling
  logs.
- **Wake-on-attention**: durable at-least-once deliveries resume a Codex thread
  or OpenCode session when a job needs a human or agent.
- **Terminal dashboard**: the native Go `monitor` renders a live
  W&B-LEET-style dashboard of jobs, metrics, and plots.

Out of scope for v1: remote network access, TLS, multi-user tenancy/RBAC,
a web UI, and distributed workers. Interactive stdin (`job_send`), concurrent
job quotas, and automatic retention are supported (see below).

**For agents:** start work with `job_start`, then `job_wait` for
`progress`/`checkpoint`/`completed` events instead of polling; make jobs emit
`AGENT_EVENT` lines (below) so progress, metrics, and checkpoints appear live
in the `vanth-monitor` dashboard; and let long jobs resume you via wake
targets instead of you checking in.

---

## Quick start

Install via `pip` or `uv` (Python 3.11+):

```cmd
pip install vanth              # or: uv tool install vanth
```

This installs the `vanth` MCP server, `vanthd` daemon, `vanth-monitor`, and
the ops CLI as standalone tools (the wheel bundles the native Go monitor, so
no Go toolchain is needed). Wheels are published for Windows x86_64, Linux
x86_64/arm64, and macOS x86_64/arm64.

From a source checkout (development):

```cmd
git clone https://github.com/abhim-dv/vanth.git && cd vanth
uv sync
```

The daemon **autostarts on demand**: the first MCP tool call or CLI command
starts it if it isn't already running, so there is no separate "start the
daemon" step.

1. **Register the MCP server** in opencode, Codex, and Claude-style clients:

   ```cmd
   vanth setup                 # detect + configure everything found (prompts)
   vanth setup --yes           # apply without prompting (scripts/CI)
   vanth setup opencode codex  # only specific clients
   ```

2. **Check health**:

   ```cmd
   vanth status                # is the daemon up? pid, schema, running jobs, deliveries
   vanth doctor                # full health report (same as job_doctor, human-readable)
   ```

3. **Pick up updates** — after upgrading, restart the daemon so it runs the new
   code. In-flight jobs survive (runners are detached):

   ```cmd
   vanth restart
   ```

### End-to-end: run a tracked job

Once the MCP client is connected, this is the whole loop:

```text
job_start(
  command="uv run python examples\\long_job.py",
  name="demo run",
  notify_on=["checkpoint", "failed", "completed"],
)
# -> job_<id>

job_wait(job_id="job_<id>", filters=["checkpoint"], timeout_seconds=120)
# -> returns the first checkpoint event + current status

job_wait(job_id="job_<id>", filters=["completed", "failed"], timeout_seconds=300)
# -> returns the terminal event + exit code
```

And in a third terminal, watch it live:

```cmd
uv run vanth-monitor
```

### Command-line entry points

| Command | Purpose |
|---|---|
| `uv run vanth` | MCP stdio server (bridge to the daemon); also the human CLI below |

The MCP stdio server self-terminates when its launching client dies or closes
stdin, and reaps itself after `VANTH_WATCH_IDLE` seconds of idle (default 1800;
`0` disables), so stale sessions never leave orphaned `vanth` processes. Blocking
tool calls are never reaped mid-flight. `vanth doctor --reap-orphans` cleans up
any orphans from older versions.
| `uv run vanthd` | The background HTTP daemon |
| `uv run vanth-monitor` | Live terminal dashboard (Go binary, bundled in the wheel) |
| `uv run vanth-codex-notify` | Delivery adapter: reads a wake payload on stdin, dispatches it to Codex |

### Human CLI

`vanth` doubles as a human-facing operations CLI. The daemon autostarts on
demand for any of these. Every flag-based command supports `--json` where noted
for scripts.

| Command | Purpose |
|---|---|
| `vanth --version` | Print the installed version |
| `vanth status` | Daemon up/down, pid, schema, running jobs, deliveries (`--json`) |
| `vanth doctor` | Full health report (same as `job_doctor`, human-readable; `--json`) |
| `vanth restart` | Gracefully stop + start the daemon (jobs survive) |
| `vanth setup [opencode] [codex] [claude] [--remove] [--yes]` | Register/unregister the MCP server in your clients' configs |
| `vanth list` (`ps` alias) | List jobs (`--status`, `--limit`, `--all`, `--json`) |
| `vanth logs <job_id>` (`tail` alias) | Show a job's output (`--stream stdout\|stderr\|all`, `--offset`, `--max-bytes`, `--json`) |
| `vanth stop <job_id>` | Stop a running job (`--signal`, `--kill-after`) |
| `vanth artifacts <job_id>` | List a job's artifacts (`--limit`, `--json`) |
| `vanth prune` | Manual retention cleanup; dry-run by default (`--older-than N`, `--yes`) |
| `vanth autostart enable\|disable\|status` | Daemon survives reboots (Windows Task Scheduler / macOS launchd / Linux systemd user unit) |

Examples:

```cmd
vanth status --json
vanth list --status running --limit 20 --json
vanth logs job_abc123 --stream stderr --max-bytes 65536
vanth stop job_abc123 --signal terminate --kill-after 10
vanth artifacts job_abc123 --json
vanth prune --older-than 604800 --yes     # actually delete (dry-run is the default)
vanth autostart enable                     # daemon survives reboots
```

`vanth restart` is the reliable way to pick up a code/version update: it sends
the daemon a graceful shutdown over loopback, waits for the old process to
fully release the home lock, then starts a fresh daemon. In-flight jobs are
owned by detached runners, so they continue across the restart.

`vanth autostart` installs a start-at-login mechanism per platform — a Windows
Task Scheduler task, a macOS launchd agent, or a Linux systemd user unit —
then `vanth status` reports whether it is active.

---

## How it works

```
MCP client / HTTP client
        |
        v
   vanthd (localhost HTTP daemon, bearer-token auth)
        |                 |                    |
        |                 |                    +---> wake adapters
        |                 |                          (local_command / codex_thread / opencode_thread / webhook)
        |                 |
        |                 +----> jobs.sqlite (durable source of truth)
        |
        +----> vanth.runner (detached worker process)
                    |
                    +----> your command (own process group)
                              |
                              +----> stdout/stderr -> logs/ + AGENT_EVENT parsing
```

Ownership rules:

- the **runner** owns the real command, its timeout, and stream draining;
- the **daemon** owns maintenance, delivery dispatch, API requests, and recovery;
- **SQLite is the source of truth** across process restarts;
- the **MCP and HTTP clients** never need to stay alive for jobs to continue.

A job is not considered terminal until both output streams have reached EOF and
all structured events have been persisted.

### Job lifecycle

A job moves through a small set of states. Terminal states are permanent.

| State | Meaning |
|---|---|
| `running` | Workload launched; runner is streaming output and heartbeating |
| `completed` | Command exited 0, streams drained, events persisted |
| `failed` | Command exited non-zero |
| `timeout` | Command exceeded `timeout_seconds`; runner terminated it |
| `cancelled` | `job_stop` was issued and the process tree actually terminated |
| `orphaned` | Runner died unexpectedly (crash); never silently dropped |

The runner enforces `timeout_seconds` even across daemon restarts. On recovery,
a `running` job whose runner is gone is marked `cancelled` (if a stop was
requested) or `orphaned` (if not) — never left as a zombie `running` row.

---

## Installing the MCP server

`vanth` is the MCP stdio server. It talks to the daemon, starting it
automatically on first use if it is not already running.

### One-shot setup

After installing the tool, connect it to the MCP clients on your machine in a
single step:

```cmd
uv tool install vanth
vanth setup
```

`vanth setup` detects your installed clients (opencode, Codex, and generic
`mcpServers`-style clients such as Claude Code / Cursor), shows what it found,
backs up each config before touching it (`.vanth-setup-<ts>.bak`), and upserts
the Vanth MCP entry — leaving every other setting and comment untouched.

```cmd
vanth setup                  # detect + configure everything found (prompts)
vanth setup --yes            # apply without prompting (scripts/CI)
vanth setup opencode codex   # only specific clients
vanth setup --json           # machine-readable result
vanth setup --remove         # remove the Vanth MCP entries instead
```

Configs it manages:

| Client | File | Section |
|---|---|---|
| opencode | `~/.config/opencode/opencode.json` | `mcp.vanth` |
| Codex | `~/.codex/config.toml` | `[mcp_servers.vanth]` |
| Claude Code / Cursor | `~/.claude.json` | `mcpServers.vanth` |

Manually, the same entries are:

### opencode

Add to `~/.config/opencode/opencode.json`:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "vanth": {
      "type": "local",
      "command": ["vanth"],
      "enabled": true,
      "timeout": 15000
    }
  }
}
```

From a source checkout, use `uv` directly instead of a bare `vanth`:

```json
{
  "mcp": {
    "vanth": {
      "type": "local",
      "command": ["uv", "run", "--directory", "/path/to/vanth", "vanth"],
      "enabled": true,
      "timeout": 15000
    }
  }
}
```

Verify the connection and tools:

```cmd
opencode mcp list
```

### Claude-style MCP clients (`mcpServers`)

Published wheel:

```json
{
  "mcpServers": {
    "vanth": { "command": "vanth", "env": { "VANTH_HOME": "C:/Users/you/.vanth" } }
  }
}
```

From a source checkout:

```json
{
  "mcpServers": {
    "vanth": {
      "command": "uv",
      "args": ["--directory", "/path/to/vanth", "run", "vanth"],
      "env": { "VANTH_HOME": "C:/Users/you/.vanth" }
    }
  }
}
```

### Configuring the daemon home

Both the MCP server and the daemon resolve the same state root from `VANTH_HOME`
(default `%USERPROFILE%\.vanth` on Windows, `~/.vanth` on Unix; `AGENT_BG_HOME`
is accepted as an alias). If both are set they must resolve to the same
directory.

---

## Instrumenting jobs with `agent_event`

Any Python script can emit structured events to stdout (or stderr) that Vanth
parses and the monitor charts. This is optional — plain scripts still run and
log — but it is what turns a job into a first-class tracked object.

```python
from vanth.agent_events import agent_event, progress

# A checkpoint: something meaningful happened.
agent_event("checkpoint", "epoch complete", epoch=10, val_loss=0.42)

# A progress update: drives the progress bar and progress.* plots.
progress(10, 100, unit="epoch", stage="train", message="10/100 epochs")

# Arbitrary scalar metrics: become their own line plots.
agent_event("metric", _step=10, loss=0.42, acc=0.88, mbps=12.4)
```

Notes:

- the helper prints `AGENT_EVENT {json}` with `flush=True` (flush matters);
- `progress(current, total, unit=..., stage=...)` computes `percent` for you;
- `metric` payloads: numeric fields become series; `_step` (if present and
  numeric) is the x-axis, otherwise the event sequence number is used; keys
  starting with `_` other than `_step` are ignored; booleans are not metrics;
  NaN/Infinity/null values are skipped and counted in the monitor's warning
  badge;
- any other field (e.g. `file`, `stage`, `phase`) is preserved and visible in
  the exact event table.

### Example: a tracked downloader

```python
# downloader.py
import os
from vanth.agent_events import agent_event, progress

files = ["a.bin", "b.bin", "c.bin"]
total = sum(os.path.getsize(f) for f in files)
done = 0

for f in files:
    agent_event("checkpoint", f"starting {f}", file=f)
    # ... download f ...
    done += os.path.getsize(f)
    progress(done, total, unit="bytes", stage="download",
             message=f"{done}/{total} bytes")
```

### Example: an image-processing batch

```python
from vanth.agent_events import agent_event, progress

images = list(find_images("input/"))
for i, img in enumerate(images, 1):
    out = process(img)                    # resize, denoise, ...
    agent_event("metric", _step=i, sharpness=out.sharpness, size_mb=out.size_mb)
    progress(i, len(images), unit="images", stage="process", message=img.name)
```

### Timestamped, leveled logging with `loguru`

Vanth ships a loguru wrapper that routes every record into a structured
`AGENT_EVENT` log line, so logs appear as timestamped, level-aware events in
the event table (with the level badge and exact timestamps) instead of bare
text:

```python
from vanth.agent_logger import logger, log_with_context

logger.info("training started", lr=8e-5, batch_size=8)     # event type "log", level info
logger.warning("low disk", free_gb=2.5)
log_with_context("error", "failed to load checkpoint", path="best.pt")
```

Each call emits `AGENT_EVENT {"type":"log","level":"info","message":"...","data":{...}}`
which the daemon persists as a durable event. `data` carries extra context. The
monitor shows these in the exact event table alongside `metric`/`progress`
events.

---

## Tool reference (MCP tools)

| Tool | Purpose |
|---|---|
| `job_start` | Launch a command as a detached job |
| `job_rerun` | Re-launch a job, optionally overriding `command`/`env`/`cwd`/`timeout_seconds`/`name`/`tags`/`notes`/`interactive` |
| `job_send` | Feed stdin to an interactive job (`interactive=True` first) |
| `job_wait` | Block until a matching event (or timeout) — the preferred way to await jobs (`return_progress` optional) |
| `job_status` | One job's status, command, env, progress, last event, linkage, tags |
| `job_status_batch` | Many jobs' status in one call (`job_ids`, `limit`) |
| `job_list` | Recent jobs, filterable by `status` / `thread_id` / `name` / `tags` |
| `job_view` | Agent-facing summaries sorted by attention priority |
| `job_events` | Structured events for a job (forward via `since_event_id`, or latest-first via `reverse`) |
| `job_tail` | Bounded stdout/stderr log tail with byte offsets (`follow`/`timeout_seconds`/`grep` optional) |
| `job_metrics_query` | Read stored scalar metric series (loss, acc, progress.percent, ...) |
| `job_metric_compare` | Compare one metric across jobs (latest/mean/min/max/sum/count) |
| `job_run_summary` | One-call "did it work?" — status, runtime, progress, metrics, artifacts |
| `job_diff` | Diff the run specs of two jobs (command/env/cwd/tags/wake targets) |
| `job_artifact_add` | Attach an artifact (checkpoint, CSV, output) to a job |
| `job_artifacts` | List artifacts attached to a job |
| `job_dashboard` | Downsampled chart-data view for any renderer |
| `job_deliveries` | Wake deliveries for a job, filterable by `status` |
| `job_mark_delivery` | Manually set a delivery's status |
| `job_retry_delivery` | Requeue a failed delivery for dispatch |
| `job_delivery_attempts` | Attempt/lease history for one delivery |
| `job_stop` | Stop a running job (terminate process tree) |
| `job_doctor` | Daemon health, schema, tables, binary availability |
| `job_cleanup` | Dry-run or real removal of old terminal jobs |
| `daemon_wake` | Schedule a self-resume wake target — full target dict or `events`/`type`/`...config` shorthand (Python API) |

For the full parameter contract and response shapes of every tool, see
[docs/agent-tools.md](docs/agent-tools.md).

### AGENT_EVENT protocol

Jobs can emit `AGENT_EVENT <json>` lines on stdout (or stderr) to create typed
events — `progress`, `metric`, `checkpoint`, `completed`, and more — that the
daemon persists durably, the dashboard charts, and wake deliveries carry to
agents. See `vanth/agent_events.py` for the Python helpers and
`vanth/agent_logger.py` for the loguru integration.

### job_start

```text
job_start(
  command="uv run python examples\\long_job.py",
  name="training run",
  cwd="F:\\git\\project",            # optional
  env={"CUDA_VISIBLE_DEVICES": "0"}, # optional
  timeout_seconds=3600,              # optional; None = no timeout
  interactive=True,                  # optional; open stdin for job_send
  notify_on=["progress","checkpoint","failed","completed"],
  origin_thread_id="019f...",        # the agent thread that launched it
  tags=["training","gpu"],           # optional
  wake_targets=[...],                # optional, see below
  trigger={"job_id": "job_A", "status": "completed"}  # optional DAG: start after job_A completes
)
```

Returns `job_id`, `status`, `worker_pid`, and the log/event paths. With
`trigger` set, the job is created `queued` and starts automatically when the
parent job reaches that status (or is `cancelled` if the parent ends
differently).

### job_send — feed stdin to an interactive job

```text
job_send(job_id="job_...", input="y", eof=False)
```

Appends `input` to a running job's stdin. Start the job with `interactive=True`
first. `eof=True` closes the job's stdin (the child sees EOF). Non-blocking:
returns immediately; the input is queued to the runner. Rejects jobs that are
not interactive, not running, or unknown. `job_rerun` preserves the
`interactive` flag.

### job_status — see what a job is running

```text
job_status(job_id="job_...")
```

Returns status, **command**, **cwd**, **env**, **timeout_seconds**, **notes**,
**run** (author, hostname, OS, Python version, CPU/GPU, git repo/branch/commit),
**runtime_seconds**, progress, last event, thread linkage, tags, and exit code.
This is the fastest way for an agent to answer "what is this job doing?" — and
mirrors the run-overview you'd see for a run in W&B.

Pass `notes="..."` to `job_start` to annotate a run ("what makes this run
special?"), which is preserved on `job_rerun` and shown in the monitor.

### job_rerun — relaunch a failed job

```text
job_rerun(job_id="job_...")
```

Re-launches the job with its **original command, cwd, env, timeout, name, tags,
origin thread, and wake targets** — a new `job_id` is returned. Use it to retry
a failed download, flaky processing batch, or transient failure without
reconstructing the request.

### job_list — filter by name or tag

```text
job_list(status=["running"], name="train", tags=["gpu"], limit=20)
```

Filters: `status` (list), `thread_id`, `name` (substring), `tags` (must contain
all listed tags).

### job_events — forward or latest-first

```text
job_events(job_id="job_...", since_event_id="evt_...", limit=20)      # events after the cursor
job_events(job_id="job_...", reverse=true, limit=20)                   # the 20 newest events, newest first
```

`reverse: true` returns the most recent events (newest first) — ideal for "what
happened recently?" — and can be combined with `since_event_id` to page
backward.

### job_wait — the heart of agent usage

```text
job_wait(job_id="job_...", filters=["checkpoint","failed","completed"], timeout_seconds=3600)
```

- waits for the **first event matching any filter**, returning it with the
  current status;
- pass `since_event_id` to wait only for events newer than one you already saw;
- on timeout returns `result: "timeout"`; on daemon shutdown returns
  `result: "shutdown"`.

### job_view — what to show the user

```text
job_view(thread_id="019f...", limit=20)
```

Returns compact summaries sorted by attention priority: running and failed jobs
first, then jobs with pending/failed deliveries, then everything else. Each
entry includes status, progress, the latest event, thread linkage, tags, and
delivery counts.

### job_stop — stop a running job

```text
job_stop(job_id="job_...", signal="terminate", kill_after_seconds=10)
```

Terminates the job's process tree. A graceful `signal` (default `terminate`) is
sent first; if the job has not exited within `kill_after_seconds`, it is killed.
The job becomes `cancelled` only after the workload tree actually terminated;
otherwise it stays `running` and the stop is retryable.

### job_mark_delivery / job_retry_delivery — manual delivery control

```text
job_mark_delivery(delivery_id="del_...", status="delivered", error="optional reason")
job_retry_delivery(delivery_id="del_...")   # requeue a failed delivery
```

`job_mark_delivery` sets a delivery's status by hand (e.g. after resolving an
adapter problem); `job_retry_delivery` requeues a failed one for the next
dispatch pass. `job_delivery_attempts` shows the claim/lease history.

### job_cleanup — remove old terminal jobs

```text
job_cleanup(older_than_seconds=86400, dry_run=true)   # preview
job_cleanup(older_than_seconds=86400, dry_run=false)  # delete
```

Removes terminal jobs older than the cutoff: logs, event mirrors, specs,
deliveries, attempts, wake targets, events, stdin channels, then the job row.
Running jobs are never selected. Dry-run is fully read-only. Cleanup is safe
to repeat.

**Automatic retention**: set `VANTH_RETENTION_SECONDS` on the daemon to purge
old terminal jobs in the background (polled every
`VANTH_RETENTION_INTERVAL_SECONDS`, default 3600; safe by default —
`VANTH_RETENTION_DRY_RUN=0` to actually delete).

**Concurrency quota**: set `VANTH_MAX_RUNNING_JOBS` (default `0` = unlimited)
to cap how many jobs may run at once; `job_start` returns a clear error when
the cap is reached.

### job_metrics_query — read stored scalar series

```text
job_metrics_query(job_id="job_...", metric="loss", from_ms=..., to_ms=..., limit=1000)
```

Returns the stored series for one job, grouped by metric name. `metric`
filters to a single series (e.g. `loss`, `acc`, `progress.percent`);
`from_ms`/`to_ms` filter by event timestamp (epoch milliseconds). Points are
ordered by event sequence. This is the read side of the terminal monitor's
data.

### job_metric_compare — compare a metric across runs

```text
job_metric_compare(job_ids=["job_a", "job_b"], metric="val_loss", aggregation="min")
```

Compares one metric across jobs (e.g. val_loss across seeds or configs).
`aggregation` is `latest`, `mean`, `min`, `max`, `sum`, or `count`; the result
includes the per-job value plus the first/last points. This is the W&B-style
"which run won?" primitive.

### job_run_summary — did it work?

```text
job_run_summary(job_id="job_...")
```

One call returns status, name, runtime, exit code, latest progress, notes,
per-metric overview (latest/first/min/max/count), and attached artifacts — the
fastest way for an agent to report on a finished job.

### job_artifact_add / job_artifacts — attach outputs

```text
job_artifact_add(job_id="job_...", name="best.pt", uri="file:///...", kind="checkpoint",
                 size_bytes=..., sha256="...", meta={"epoch": 5})
job_artifacts(job_id="job_...")
```

Attach artifacts (checkpoints, CSVs, rendered outputs) to a job so they are
listed in `job_run_summary` and retrievable later. `meta` is free-form JSON.

### job_dashboard — chart data for any renderer

```text
job_dashboard(job_ids=["job_..."], limit=5000)
```

Returns the job list plus every stored metric series, downsampled to `limit`
points per series — the same data the Go terminal monitor charts, exposed over
HTTP/MCP so any client (a future web/cloud dashboard) can render it.

---

## Wake targets (wake an agent when a job needs attention)

When a job emits a matching event, the daemon creates a durable delivery and
dispatches it through the adapter. Delivery is **at-least-once**; every payload
carries a `delivery_id` for deduplication.

### local_command

Runs an arbitrary command, passing the delivery payload as JSON on stdin:

```json
{
  "type": "local_command",
  "events": ["checkpoint", "failed", "completed"],
  "command": ["python", "deliver.py"]
}
```

Exit 0 marks the delivery `delivered`; any other exit marks it `failed`.

### codex_thread / codex_cli_thread

Resumes a Codex thread through the local app-server (for an unloaded CLI task):

```json
{
  "type": "codex_thread",
  "thread_id": "019f...",
  "events": ["checkpoint", "failed", "completed"],
  "codex_command": ["C:\\codex\\codex.exe"]
}
```

Protocol: `initialize -> thread/resume -> turn/start`.

The target thread must already have had at least one turn (a persisted
"rollout"). Resuming a brand-new, zero-turn thread fails with
`no rollout found for thread id <id>` — the intended wake target is an
existing/active conversation, not a never-started one.

### codex_desktop

Wakes a RUNNING Codex Desktop task through the native app-tools host pipe
(`codex_app/send_message_to_thread` on `CODEX_APP_TOOLS_PIPE_PATH`). This never
spawns a second app-server and never falls back to the CLI thread bridge:

```json
{
  "type": "codex_desktop",
  "thread_id": "019f...",
  "events": ["checkpoint", "failed", "completed"]
}
```

Desktop wake is delivered by a client-side relay: the Vanth MCP integration
registers the task id it can wake, long-polls the daemon for due deliveries,
submits the follow-up into the already-running Desktop task through the pipe,
and acknowledges only after admission succeeds. The private pipe stays inside
the Codex MCP process and is provisioned through a supported handoff — run
`vanth setup desktop` inside a Codex Desktop session with the app-tools
capability active (it writes a per-home `codex_desktop.json` capability file),
or launch Vanth with `VANTH_CODEX_DESKTOP_PIPE` /
`VANTH_CODEX_DESKTOP_THREAD` set. Without a pipe capability the delivery fails
closed with an actionable "Desktop integration unavailable" error and is never
routed to the CLI.

> **Experimental.** The private host-pipe contract is not documented by official
> Codex material. This integration is scoped to ONE provisioned task per Desktop
> lifetime: one per-home `codex_desktop.json` stores one pipe/thread tuple,
> provisioning a second Desktop task overwrites the first, and a Desktop restart
> invalidates the private pipe. A stale capability (older than 24h) is detected
> and fails closed with a diagnostic asking you to re-run `vanth setup desktop`
> inside an active Desktop session. Automatic or durable multi-task Desktop wake
> is NOT supported yet.

### opencode_thread

Resumes an OpenCode session:

```json
{
  "type": "opencode_thread",
  "thread_id": "ses_...",
  "events": ["checkpoint", "failed", "completed"],
  "cwd": "F:\\git\\project",
  "opencode_command": ["opencode"],     # override the binary
  "attach": "http://127.0.0.1:4096",    # submit via an opencode serve instance
  "timeout_seconds": 120
}
```

The default OpenCode turn timeout is 30 seconds; raise it for long turns.

Before dispatching to a plain (non-`attach`) session, Vanth runs a cheap
`opencode session list` probe to confirm the session still exists — a
confirmed-missing session fails fast (dead-lettered immediately, no retry
burn) with `opencode session not found: <id>`. The probe never blocks a valid
dispatch; on any ambiguity it proceeds. Opt out per-target with
`"skip_probe": true` or globally with `VANTH_OPENCODE_SKIP_PROBE=1`.

### webhook

POSTs the delivery payload as JSON to any HTTP(S) endpoint — a generic
channel that covers ntfy, Gotify, Telegram bots, Slack/Discord webhooks,
PagerDuty Events, and more:

```json
{
  "type": "webhook",
  "url": "https://hooks.slack.com/services/...",
  "events": ["failed", "completed"],
  "headers": { "Authorization": "Bearer <token>" },
  "timeout_seconds": 10
}
```

The payload is the same delivery payload every adapter receives (`event`,
`prompt`, `delivery_id`, `target`), POSTed with `Content-Type: application/json`.
2xx responses (200/201/202/204) mark the delivery `delivered`; anything else
is a failed delivery (retried per `max_attempts`/`retry_delay_seconds`, then
dead-lettered). `headers` lets you add auth tokens or presets for specific
services.

### Shared delivery options

```json
{
  "type": "codex_thread",
  "thread_id": "019f...",
  "events": ["checkpoint"],
  "auto_dispatch": false,      // leave the delivery pending for manual inspection
  "max_attempts": 3,           // default 1
  "retry_delay_seconds": 5,    // default 5
  "timeout_seconds": 30        // adapter timeout; also sizes the delivery lease
}
```

With `auto_dispatch: false`, deliveries stay `pending` until an agent either
dispatches them manually or changes the target.

A target that omits `events` (or `notify_on`) inherits the job's top-level
`notify_on` list. An explicit target `events` always wins:

```json
job_start(command="...", notify_on=["checkpoint","failed"],
          wake_targets=[{"type": "local_command", "command": ["deliver.py"]}])
```

Concurrent adapter dispatches are capped (default 4) so a burst of events
doesn't spawn unlimited adapter processes; excess deliveries stay queued and
are picked up on the next dispatch pass. Set `VANTH_DELIVERY_MAX_CONCURRENT`
to tune.

### Delivery operations

```text
job_deliveries(job_id="job_...")
job_delivery_attempts(delivery_id="del_...")
job_retry_delivery(delivery_id="del_...")     # requeue a failed OR retrying delivery
job_mark_delivery(delivery_id="del_...", status="delivered")
```

Attempt history records the claim token, start/end times, status, and whether
the attempt was reclaimed after an expired lease. If the daemon crashes after an
adapter accepts a wake but before Vanth records success, the delivery is
reclaimed and retried — surfaced as a `reclaimed` attempt rather than claimed as
exactly-once delivery.

`job_retry_delivery` requeues a delivery immediately for dispatch — including
one that is currently `retrying` on backoff (resets `next_attempt_at`). If a
delivery has exhausted `max_attempts`, it is dead-lettered: `vanth doctor`
reports `dead_letter_count` and the most recent `dead_lettered` deliveries
(each with `delivery_id`, `job_id`, `attempts`, `last_error`) so you can see
which wakes were never delivered and why.

---

## Running the daemon

Foreground (for development or diagnosis):

```cmd
uv run vanthd
```

Start-at-login options:

- **Windows**: the daemon is started from the user Startup folder
  (`startup_commands.bat`) alongside other startup commands; a Task Scheduler
  action template is also in `deploy/vanthd.cmd`.
- **Unix**: `deploy/vanthd.service` is a systemd user service.

Enable only one daemon per `VANTH_HOME`. A second daemon for the same home
exits immediately (OS-level lock). The daemon binds only to loopback
(`127.0.0.1` / `::1` / `localhost`); a non-loopback `VANTH_DAEMON_HOST` is
rejected.

### Security

- Every data route requires `Authorization: Bearer <token>`; the token is
  generated per home and never logged. `GET /health` is the only
  unauthenticated route (a cheap liveness probe for supervisors).
- On daemon start the state directory is re-tightened to the owner: Unix
  `chmod 0700`/`0600`; Windows disables ACL inheritance and grants only the
  owner, SYSTEM, and Administrators via `icacls`. This blocks other accounts
  (e.g. sandbox/CI users that inherit read from the user profile) from reading
  the token or per-job env/spec data.
- On Windows, socket `SO_REUSEADDR` is disabled so a second daemon cannot
  become a phantom listener on the same port; a failed bind releases the home
  lock and exits cleanly.

### The Go terminal monitor

The native Go dashboard reads the same home **read-only** and renders live
plots, progress bars, the exact event table, and log tails:

```cmd
uv run vanth-monitor
```

From a built wheel, `vanth-monitor` runs the bundled native binary (no Go
toolchain needed). From a source checkout, it builds the monitor on first use
and caches it under `~/.cache/vanth/` (requires `go` on PATH):

```cmd
go build -o bin\vanth.exe ./cmd\vanth
bin\vanth.exe monitor
```

Keys: `up/down` or `j/k` select jobs · `enter` pins a job's series · `e` event
table · `l` log tail · `+`/`-` zoom a chart · `[`/`]` pan · `t` back to live
tail · `?` help · `q` or `Ctrl+C` quit.

---

## Configuration reference

Environment variables (defaults live in `src/vanth/server.py`,
`src/vanth/daemon.py`, `src/vanth/migrations.py`):

| Variable | Default | Purpose |
|---|---|---|
| `VANTH_HOME` | `~/.vanth` | State root (alias: `AGENT_BG_HOME`) |
| `VANTH_DAEMON_URL` | `http://127.0.0.1:8765` | Where clients reach the daemon |
| `VANTH_DAEMON_HOST` | `127.0.0.1` | Bind address (loopback only) |
| `VANTH_DAEMON_PORT` | `8765` | Bind port |
| `VANTH_MAX_REQUEST_BYTES` | `1 MiB` | HTTP request body cap |
| `VANTH_MAX_RESPONSE_BYTES` | `4 MiB` | HTTP response cap |
| `VANTH_MAX_EVENT_BYTES` | `64 KiB` | Single event payload cap |
| `VANTH_MAX_EVENT_LINE_BYTES` | `1 MiB` | AGENT_EVENT line cap |
| `VANTH_MAX_LOG_BYTES` | `10 MiB` | Per-stream log cap (drain continues) |
| `VANTH_MAX_EVENTS_PER_JOB` | `100000` | Structured event cap per job |
| `VANTH_DELIVERY_POLL_INTERVAL` | `0.2s` | Maintenance loop cadence |
| `VANTH_DELIVERY_LEASE_MARGIN` | `5s` | Extra lease time beyond adapter timeout |
| `VANTH_RUNNER_HEARTBEAT_INTERVAL` | `1s` | Runner liveness heartbeat |
| `VANTH_RUNNER_HEARTBEAT_STALE_AFTER` | `10s` | Heartbeat staleness threshold |
| `VANTH_CODEX_BIN` | `codex` / `C:\codex\codex.exe` | Codex binary |
| `VANTH_OPENCODE_BIN` | `opencode` (via `shutil.which`) | OpenCode binary |
| `VANTH_LOG_LEVEL` | `INFO` | Daemon log level |
| `VANTH_LOG_MAX_BYTES` | `5 MiB` | Rotating daemon log size |
| `VANTH_LOG_BACKUP_COUNT` | `3` | Daemon log rotation count |
| `VANTH_BUSY_TIMEOUT_MS` | `30000` | SQLite write-lock wait |

Key knobs in one glance:

| Variable | Purpose |
|---|---|
| `VANTH_HOME` | State root (alias: `AGENT_BG_HOME`) |
| `VANTH_DAEMON_HOST` / `VANTH_DAEMON_PORT` | Where the daemon binds (loopback only; default `127.0.0.1:8765`) |
| `VANTH_MAX_RUNNING_JOBS` | Concurrency quota; `0` = unlimited |
| `VANTH_RETENTION_SECONDS` / `VANTH_RETENTION_INTERVAL` / `VANTH_RETENTION_DRY_RUN` | Automatic background retention of old terminal jobs (dry-run by default) |
| `VANTH_NO_SETUP_HINT` | Suppress the stderr "MCP server not configured" hint |
| `VANTH_OPENCODE_SKIP_PROBE` | Skip the `opencode session list` probe before dispatch |
| `VANTH_DELIVERY_MAX_CONCURRENT` | Cap on concurrent adapter dispatches (default 4) |
| `VANTH_MAX_REQUEST_BYTES` | HTTP request body cap (default 1 MiB) |

---

## Operations

### State layout

```
~/.vanth/
  jobs.sqlite      durable jobs (incl. env, notes, run-overview) / events / deliveries / targets / attempts / tombstones
  token            bearer token (owner-only permissions)
  daemon.lock      single-daemon OS lock
  daemon.json      discovery metadata (url, pid, started_at, schema) — written atomically, removed on graceful shutdown
  logs/            daemon.log + per-job runner/stdout/stderr logs
  events/          per-job JSONL event mirrors (monitor fallback source)
  specs/           per-job launch specs (removed once the runner starts)
  backups/         pre-migration SQLite backups
```

### Health, readiness, and diagnosis

```text
job_doctor()
```

Reports the state directory, database tables, delivery counts by status, schema
version, `PRAGMA quick_check`, stale delivery leases, free disk, token path, and
whether the Codex/OpenCode binaries resolve. It never reveals the token.

The HTTP daemon also exposes:

- `GET /health` — cheap, unauthenticated liveness probe for supervisors;
- `GET /ready` — authenticated readiness (doctor report; 503 when not ok).

### Upgrades and backups

Schema changes are ordered SQLite migrations. Before the first migration of an
existing database, a timestamped backup is written under `backups/` via
SQLite's backup API (never a raw file copy while WAL is active). To upgrade
manually, copy the latest `backups/*.sqlite` first. A future database schema is
rejected without touching the files.

---

## HTTP API (equivalent of the MCP tools)

Authenticated with `Authorization: Bearer <token>`.

| Method | Path | Purpose |
|---|---|---|
| GET | `/jobs` | List jobs (`status`, `limit`, `thread_id`, `name`, `tags`) |
| POST | `/jobs` | Start a job |
| POST | `/jobs/{id}/rerun` | Rerun a job with its original configuration |
| GET | `/jobs/{id}/status` | Job status (includes command/env/cwd) |
| GET | `/jobs/{id}/events` | Events (`since_event_id`, `types`, `limit`, `reverse`) |
| GET | `/jobs/{id}/metrics` | Metric series (`metric`, `from_ms`, `to_ms`, `limit`) |
| GET | `/jobs/{id}/summary` | Run summary (status, runtime, metrics, artifacts) |
| GET | `/jobs/{id}/artifacts` | Artifacts (`limit`) |
| POST | `/jobs/{id}/artifacts` | Add an artifact |
| GET | `/metrics/compare` | Compare metric across jobs (`job_ids`, `metric`, `aggregation`) |
| GET | `/dashboard` | Chart data (`job_ids`, `limit`) |
| GET | `/jobs/{id}/tail` | Log tail (`stream`, `max_bytes`, `offset`) |
| POST | `/jobs/{id}/wait` | Wait for an event |
| POST | `/jobs/{id}/stop` | Stop a job |
| GET | `/view` | Agent view (`thread_id`, `limit`) |
| GET | `/deliveries` | Deliveries (`job_id`, `status`, `limit`) |
| GET | `/deliveries/{id}/attempts` | Attempt history |
| POST | `/deliveries/{id}/mark` | Mark a delivery |
| POST | `/deliveries/{id}/retry` | Retry a delivery |
| POST | `/cleanup` | Cleanup (`older_than_seconds`, `dry_run`) |
| GET | `/doctor` | Health report |
| GET | `/health` | Unauthenticated liveness |

---

## Agent usage tips

1. **Wait, don't poll.** Use `job_wait(job_id, filters=[...], timeout_seconds=...)`
   instead of looping `job_status`. The daemon wakes the wait immediately when a
   matching event is persisted.
2. **Pass `since_event_id`** to the next `job_wait` after handling an event, so
   you never re-process an old one.
3. **Tag and thread your jobs.** Set `origin_thread_id` (the agent thread that
   launched the job) and `tags`; use `job_view(thread_id=...)` to summarize.
4. **Prefer `job_view` over `job_status`** when presenting a situation to a
   user — it is already sorted by attention priority.
5. **Make jobs self-describing.** Emit `AGENT_EVENT progress` / `checkpoint` /
   `metric` lines (see [above](#instrumenting-jobs-with-agent_event)). Jobs that
   are silent still work, but tracked jobs are far easier to reason about.
6. **Use wake targets for long jobs.** If a training run or long download needs
   a decision at a checkpoint, add a `codex_thread` or `opencode_thread` target
   with `events: ["checkpoint", "failed", "completed"]` so the agent is resumed
   instead of polling.
7. **Inspect delivery failures.** `job_delivery_attempts` shows the lease/claim
   history; `job_retry_delivery` requeues a failed one after fixing the cause.
8. **Set a sane `timeout_seconds`** on `job_start` so a hung command becomes a
   `timeout` (terminal) state instead of running forever; the runner enforces it
   even across daemon restarts.
9. **Clean up old state** with `job_cleanup(older_than_seconds=..., dry_run=false)`
   so the SQLite store and log files stay bounded.
10. **Rerun failed jobs, don't rebuild them.** `job_rerun(job_id=...)` relaunches
    with the original command, env, cwd, and wake targets — ideal for retrying a
    transiently failed download or batch.
11. **Ask "what is this job?" with `job_status`.** It now returns the command,
    cwd, env, and timeout, so you can explain a job to a user without reading
    logs.
12. **Filter lists by name/tag.** `job_list(name="train", tags=["gpu"])` narrows a
    growing job list without paging through everything.
13. **Use `reverse=true` for "what happened recently."** `job_events(job_id, reverse=true, limit=20)`
    returns the newest events first, and you can page further back with
    `since_event_id` set to the oldest id you've seen.
14. **A job survives the daemon.** The runner is detached; jobs continue across
    daemon/MCP restarts. If a runner is gone at recovery, the job is marked
    `orphaned` (never silently dropped).

---

## Examples

```text
uv run python examples\long_job.py    # emits progress + checkpoints
```

`examples/long_job.py` is a small reference job that uses `vanth.agent_events`.
Start it through `job_start` and watch it in `vanth monitor`.

---

## Troubleshooting

- **`Unauthorized` (401)**: the bearer token in `~/.vanth/token` is what the
  daemon expects. Confirm `VANTH_HOME` is the same for the daemon and client.
- **Second daemon won't start**: `another vanthd already owns this VANTH_HOME`.
  One daemon per home by design.
- **Job stuck `running` then `orphaned`**: the runner process died. Check
  `logs/<job_id>.runner.log` and the heartbeat thresholds.
- **No charts in the monitor**: the job isn't emitting `AGENT_EVENT` `metric` or
  `progress` lines — add them (optional).
- **OpenCode wake timing out**: increase `timeout_seconds` on the wake target
  beyond the expected turn length.
- **OpenCode wake failed with `Session not found`**: the wake target's
  `session_id` is stale or was removed. Vanth now probes the session before
  dispatching (`opencode session list`) and fails fast with
  `opencode session not found: <id>` instead of retrying a dead session.
  Refresh the wake target's `session_id` (or start a new session) and
  `job_retry_delivery` to re-dispatch. Per-target opt-out: `skip_probe: true`;
  global opt-out: `VANTH_OPENCODE_SKIP_PROBE=1`.
- **Codex wake failed with `no rollout found for thread id`**: the target
  thread has never had a turn. Start a first turn in that thread (or target an
  existing, active conversation) before waking it.
- **Monitor shows nothing / empty state**: confirm `VANTH_HOME` points at the
  daemon's home, and that `jobs.sqlite` exists there.

---

## Development

```cmd
uv run pytest -q                 # Python suite (112 passed, 1 Linux-only skip)
uv run python -m compileall -q src tests examples
uv build                         # sdist + wheel; wheel bundles the Go monitor
go vet ./... && go test ./...    # Go: config, state, monitor
```

The wheel build runs a hatchling build hook (`build-hooks/bundle_monitor.py`)
that compiles the Go monitor for the host platform and bundles it under
`vanth/monitor-bin/` so `vanth-monitor` needs no Go toolchain at runtime. `go`
must be on PATH when building the wheel; it is not needed to install or run it.
Wheels are platform-tagged (`py3-none-<platform>`) because they contain the
native binary.

Release-gate automation lives in `scripts/`:

- `scripts/chaos_matrix.py` — heavy synthetic workloads and kill/restart matrix;
- `scripts/real_adapter_smoke.py` — opt-in live Codex/OpenCode wake smokes
  (set `VANTH_SMOKE_CODEX_THREAD` / `VANTH_SMOKE_OPENCODE_SESSION`);
- `scripts/generate_go_fixture.py` — regenerates the deterministic schema-v5
  conformance fixture in `testdata/`;
- `scripts/demo_jobs.py` — starts demo jobs (training run, quick task, failing
  task) for the monitor.

## Limitations (v1)

- Interactive stdin and `job_send` are not implemented; jobs run with stdin
  closed (use non-interactive flags on commands).
- Delivery is at-least-once; a crash after an adapter accepts a wake but before
  Vanth records success is a documented, surfaced ambiguity.
- Remote access, TLS, multi-user policy, quotas, distributed workers, and a
  custom service manager are out of scope.
