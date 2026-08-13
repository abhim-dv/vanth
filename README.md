# vanth

Event-driven background jobs for agents.

Vanth is a supported v1 localhost background-job daemon and MCP interface. It
manages one state root per user, keeps durable SQLite state across restarts,
and wakes Codex or OpenCode sessions when a job needs attention. Remote access,
multi-user tenancy, and interactive stdin are out of scope.

```cmd
uv sync
```

```cmd
uv run vanth
```

`vanth` is the MCP interface. It talks to the authenticated local daemon, starting it when needed:

```cmd
uv run vanthd
```

MCP stdio config:

```json
{
  "mcpServers": {
    "vanth": {
      "command": "uv",
      "args": ["--directory", "F:/git/vanth", "run", "vanth"],
      "env": {
        "VANTH_HOME": "F:/git/vanth/.vanth"
      }
    }
  }
}
```

Jobs can emit events on stdout or stderr:

```python
from vanth.agent_events import agent_event

agent_event("progress", "10/100", current=10, total=100, unit="epoch")
agent_event("checkpoint", "epoch complete", epoch=10, val_loss=0.42)
```

Agents should wait for meaningful events instead of polling logs:

```text
job_wait(job_id, filters=["progress", "checkpoint", "failed", "completed"])
```

Typical tool flow:

```text
job_start(
  command="uv run python examples\\long_job.py",
  notify_on=["progress", "checkpoint", "failed", "completed"],
  origin_thread_id="019f...",
  tags=["training", "gpu"]
)
job_wait(job_id="...", filters=["progress", "checkpoint", "failed", "completed"], timeout_seconds=3600)
job_status(job_id="...")
job_tail(job_id="...", stream="stdout", max_bytes=8192, offset=0)
job_view(thread_id="019f...")
```

Codex wake target flow:

```text
job_start(
  command="uv run python examples\\long_job.py",
  wake_targets=[
    {
      "type": "codex_thread",
      "thread_id": "019f...",
      "events": ["checkpoint", "failed", "completed"]
    }
  ]
)
```

For `codex_thread` targets, the daemon calls Codex's local app-server over stdio:

```text
initialize -> thread/resume -> turn/start
```

Use `auto_dispatch: false` if an agent should inspect pending deliveries manually. Use `codex_command` to point at a non-default Codex binary.

```json
{
  "type": "codex_thread",
  "thread_id": "019f...",
  "events": ["checkpoint"],
  "codex_command": ["C:\\codex\\codex.exe"]
}
```

OpenCode wake targets use the same shape and resume the requested session with
`opencode run --session`:

```json
{
  "type": "opencode_thread",
  "thread_id": "ses_...",
  "events": ["checkpoint", "failed", "completed"],
  "cwd": "F:\\git\\project"
}
```

Use `opencode_command` to override the binary and `attach` to submit through an
existing `opencode serve` instance. `auto_dispatch: false` leaves deliveries
pending for manual inspection, just like `codex_thread`. Increase
`timeout_seconds` when an OpenCode turn may take longer than the 30-second default.

Example job:

```cmd
uv run python examples\long_job.py
```

Jobs run with stdin closed (`DEVNULL`). Interactive input and `job_send` are intentionally not supported.

Daemon-side wake dispatch:

```text
event inserted -> delivery inserted -> local_command adapter runs immediately
```

Example target:

```json
{
  "type": "local_command",
  "events": ["checkpoint", "failed", "completed"],
  "command": ["python", "deliver.py"]
}
```

The command receives the delivery payload as JSON on stdin. Successful exit marks the delivery `delivered`; failure marks it `failed`.

Delivery failures can be retried:

```text
job_retry_delivery(delivery_id="del_...")
```

Agent-facing status:

```text
job_view(thread_id="019f...")
job_doctor()
```

`job_view` returns compact job summaries sorted by attention priority, including progress, the latest event, thread linkage, tags, and delivery counts. `job_doctor` reports the daemon state directory, database tables, delivery counts, and Codex command availability.

Jobs are launched through a detached runner process. If the MCP server or HTTP daemon restarts while a job is running, the job can continue and the new manager instance will keep waiting on durable SQLite events. If the runner PID is gone during recovery, the job is marked `orphaned`.

## v1 operations

State lives under `VANTH_HOME` (default `%USERPROFILE%\.vanth` on Windows or
`~/.vanth` on Unix): `jobs.sqlite`, `token`, `daemon.lock`, `logs/`, `events/`,
`specs/`, and `backups/`. The daemon binds only to loopback. Every data or
mutation request uses `Authorization: Bearer <token>`; `job_doctor` reports the
token path and health details but never the token itself. The cheap `/health`
probe is intentionally unauthenticated for process supervisors.

Schema changes use ordered SQLite migrations and make a backup through SQLite's
backup API before changing an existing database. SQLite uses WAL mode with a
5-second busy timeout by default. A second daemon for the same `VANTH_HOME`
exits without taking ownership of the state.

Wake delivery is durable at-least-once delivery. Each payload contains a
`delivery_id`, which adapters should deduplicate when duplicate side effects are
unacceptable. Claims have leases and attempt history; inspect them with
`job_delivery_attempts(delivery_id=...)` and retry failed rows with
`job_retry_delivery`. A daemon crash after an adapter accepts a wake but before
Vanth records success remains inherently ambiguous; it is surfaced as a
reclaimed or failed attempt rather than claimed as exactly-once delivery.

Useful configuration includes `VANTH_HOME` (with legacy `AGENT_BG_HOME` accepted
as the same alias), `VANTH_DAEMON_URL`,
`VANTH_DAEMON_HOST`, `VANTH_DAEMON_PORT`, `VANTH_MAX_REQUEST_BYTES`,
`VANTH_MAX_RESPONSE_BYTES`, `VANTH_MAX_EVENT_BYTES`,
`VANTH_MAX_EVENT_LINE_BYTES`, `VANTH_MAX_LOG_BYTES`, `VANTH_DELIVERY_POLL_INTERVAL`,
`VANTH_DELIVERY_LEASE_MARGIN`, `VANTH_RUNNER_HEARTBEAT_INTERVAL`,
`VANTH_RUNNER_HEARTBEAT_STALE_AFTER`, `VANTH_CODEX_BIN`,
`VANTH_OPENCODE_BIN`, `VANTH_LOG_LEVEL`, `VANTH_LOG_MAX_BYTES`, and
`VANTH_LOG_BACKUP_COUNT`, `VANTH_MAX_EVENTS_PER_JOB`, and
`VANTH_BUSY_TIMEOUT_MS` (default 30000; the SQLite write-lock wait). Defaults
are defined in `src/vanth/server.py`, `src/vanth/daemon.py`, and
`src/vanth/migrations.py`.

If both home variables are set, they must resolve to the same directory;
different values are rejected so the daemon lock cannot be bypassed.

Use `job_cleanup(older_than_seconds=..., dry_run=true)` to preview removal of
old terminal jobs and `dry_run=false` to delete their database rows and files.
Running jobs are never selected. Cleanup is safe to repeat.

Interactive stdin and `job_send` remain unsupported. For a foreground
diagnostic daemon, run `uv run vanthd`. The repository includes
`deploy/vanthd.cmd` for a Windows Task Scheduler action and
`deploy/vanthd.service` for a Unix systemd user service; adjust the project path
and `uv` location for the machine, then enable only one daemon per
`VANTH_HOME`. Keep upgrades reversible by copying the documented `backups/`
database backup before replacing the wheel.
