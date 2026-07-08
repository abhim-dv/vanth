# Event-Driven Background Jobs for Agents

## Summary

Build a small MCP server that lets coding agents start long-running background jobs and wait for meaningful events instead of polling logs or process status.

The target use case is broad: ML experiments, evals, fuzzers, crawlers, build/test watchers, dev servers, data pipelines, deploy scripts, and any long-running command where an agent should only re-engage at useful checkpoints.

The base implementation should be a fork of [`mcp-background-job`](https://github.com/dylan-gluck/mcp-background-job). It already has the minimum useful core: MCP server, async shell execution, job IDs, process status, stdout/stderr capture, stdin interaction, termination, cleanup, Python, and `uv`.

The new product layer is event-driven coordination:

- Jobs emit structured events.
- The server stores events durably.
- Agents block on `job_wait(...)` until selected events occur.
- Clients that support MCP notifications also receive push updates.
- Agents do not burn context or tokens by polling.

## Problem

Current terminal agents handle long-running work poorly in two common ways:

1. They block on a command until timeout.
2. They start a background process, then repeatedly poll logs or status.

Both are bad for multi-hour work. Polling wastes model turns, burns context, increases cost, and still misses the semantic point: the agent usually does not care that another 50 log lines appeared. It cares when a checkpoint, failure, target metric, approval point, or completion happened.

For example:

- ML run reaches epoch 10 and validation loss improved.
- Fuzzer finds a crash.
- Crawler finishes a shard.
- Dev server reports readiness.
- Test watcher sees failures drop to zero.
- Evaluation crosses a quality threshold.
- Deploy script reaches a manual approval step.

The agent should sleep until one of those events happens, then decide what to do next.

## Product Positioning

This is not a full workflow engine. It is a small, local, agent-friendly process supervisor with durable event notifications.

It sits between raw shell execution and heavyweight orchestrators like Temporal.

## Existing Products And What To Reuse

### Claude Code Agent View

Relevant docs:

- [`Agent View`](https://code.claude.com/docs/en/agent-view)
- [`Hooks Guide`](https://code.claude.com/docs/en/hooks-guide)
- [`Hooks Reference`](https://code.claude.com/docs/en/hooks)

Claude Code appears to use two separate layers:

1. A per-user supervisor process owns background sessions separately from terminal UI.
2. Lifecycle transitions fire notifications and hooks such as `agent_needs_input` and `agent_completed`.

Useful design idea:

```text
worker/process changes state
  -> supervisor updates durable state
  -> event dispatcher emits notification
  -> UI/hook/agent can react
```

Do not clone Claude Code. Its core is not a normal open-source base for this. The public repo exists, but licensing and available source make it the wrong base.

### mcp-background-job

Repo: [`dylan-gluck/mcp-background-job`](https://github.com/dylan-gluck/mcp-background-job)

Best base to fork.

Why:

- Already an MCP server.
- Already Python and `uv`.
- Already starts and manages background shell commands.
- Already has stdout/stderr capture and process lifecycle tools.
- Small enough to modify.

Missing:

- Durable event store.
- Structured checkpoint parsing.
- Blocking wait primitive.
- Push notifications.
- Rich acceptance tests around no-polling behavior.

### Background Process MCP

Listing: [`Background Process MCP`](https://mcpservers.org/servers/github-com-waylaidwanderer-background-process-mcp)

Closest existing product shape for generic process management across agents.

Useful ideas:

- Generic MCP process management.
- Separate service.
- Human TUI.
- Works across Claude, Codex, Cursor, Gemini, Goose, LM Studio, etc.

Potential drawback:

- If implementation is larger or TypeScript-heavy, it may be less convenient than `mcp-background-job` for this repo's preferred `uv` workflow.

### pi-background-tasks

Package: [`pi-background-tasks`](https://pi.dev/packages/pi-background-tasks?name=safety)

Do not port wholesale.

Useful ideas to copy:

- Named tracked shell jobs.
- Durable output files.
- Bounded log reads.
- Kill/timeout safety.
- Completion notifications that can wake the agent.
- Task manager UI concept.

Why not port directly:

- Pi-specific extension hooks and UI/footer integrations.
- Product coupling that does not transfer cleanly to MCP clients.

### pilotty

Repo: [`msmps/pilotty`](https://github.com/msmps/pilotty)

Useful only when the process must run in an interactive PTY/TUI.

Not the main base for this product because most long-running jobs do not need terminal emulation. PTY support can be a later optional backend.

### MCP Tasks

Spec: [`MCP Tasks`](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/tasks)

Important but not enough by itself.

MCP defines task status notifications such as `notifications/tasks/status`, and progress notifications. However, task notifications are optional and clients must not rely on receiving them.

Therefore v1 must provide a blocking `job_wait` tool. Push notifications are additive, not the only mechanism.

### Trigger.dev, Temporal, Agentspan

Relevant links:

- [`Trigger.dev`](https://trigger.dev/)
- [`Temporal AI`](https://temporal.io/solutions/ai)
- [`Agentspan`](https://agentspan.ai/)

These are better when the product needs durable workflows, retries, DAGs, approvals, distributed workers, and long-lived business processes.

They are too heavy for v1 if the goal is "agents can supervise long-running shell jobs without polling."

## Goals

- Start arbitrary long-running commands.
- Capture stdout and stderr durably.
- Parse structured checkpoint events from logs or sidecar files.
- Store job metadata and events durably.
- Let agents wait on selected events with one blocking tool call.
- Notify clients through MCP notifications where available.
- Provide bounded log and event reads.
- Support job stop/kill.
- Survive completed job lookup across server restarts.
- Keep implementation small and easy to fork.

## Non-Goals

- No web dashboard in v1.
- No distributed cluster.
- No queue scaling.
- No DAG engine.
- No scheduler or cron in v1.
- No Docker orchestration in v1.
- No PTY/TUI support in v1.
- No experiment-tracking UI.
- No custom agent runtime.
- No promise that every MCP client can be woken by push notification.

## Core Concept

The product exposes a generic MCP background job server.

An agent starts a job:

```text
job_start(command="uv run python train.py", notify_on=["checkpoint", "failed", "completed"])
```

The job emits structured events:

```text
AGENT_EVENT {"type":"checkpoint","message":"epoch 10 complete","data":{"epoch":10,"loss":0.42}}
```

The agent waits:

```text
job_wait(job_id="abc123", filters=["checkpoint", "failed", "completed"], timeout_seconds=21600)
```

The server blocks the MCP tool call until a matching event exists. No model polling happens.

When a matching event occurs, the server returns a compact payload:

```json
{
  "job_id": "abc123",
  "event": {
    "event_id": "evt_00042",
    "type": "checkpoint",
    "message": "epoch 10 complete",
    "data": {"epoch": 10, "loss": 0.42},
    "created_at": "2026-07-08T18:12:00Z"
  },
  "status": "running",
  "next_actions": ["job_tail", "job_stop", "job_wait"]
}
```

The agent decides whether to continue waiting, inspect logs, stop the job, or launch another job.

## Progress Tracking

Progress is a normal structured event, not a separate subsystem.

Jobs emit `progress` events when useful work advances:

```text
AGENT_EVENT {"type":"progress","message":"epoch 10/100","data":{"current":10,"total":100,"unit":"epoch","percent":10,"stage":"training"}}
```

Progress data should use these conventional fields when they apply:

```json
{
  "current": 10,
  "total": 100,
  "unit": "epoch",
  "percent": 10,
  "stage": "training"
}
```

Fields:

- `current`: completed amount.
- `total`: expected final amount, if known.
- `unit`: what is being counted, such as `epoch`, `file`, `shard`, `test`, `byte`, or `step`.
- `percent`: optional numeric percentage from `0` to `100`.
- `stage`: optional phase name, such as `download`, `train`, `eval`, `deploy`, or `crawl`.

Agents can wait for progress the same way they wait for checkpoints:

```text
job_wait(job_id="abc123", filters=["progress", "failed", "completed"], timeout_seconds=3600)
```

The server should store every progress event, but `job_status` should also expose the latest progress snapshot so agents can inspect current state without reading the full event history.

## Architecture

```text
MCP client / agent
  |
  | job_start, job_wait, job_status, job_stop
  v
MCP Background Job Server
  |
  +-- Job Manager
  |     +-- starts processes
  |     +-- tracks PID/status
  |     +-- kills/stops processes
  |
  +-- Output Readers
  |     +-- stdout -> logs/<job_id>.stdout.log
  |     +-- stderr -> logs/<job_id>.stderr.log
  |     +-- line parser for AGENT_EVENT {...}
  |
  +-- Event Store
  |     +-- events/<job_id>.jsonl
  |     +-- jobs.sqlite
  |
  +-- Wait Registry
  |     +-- per-job asyncio.Condition or queue
  |     +-- wakes job_wait calls on matching events
  |
  +-- Notification Dispatcher
        +-- MCP notifications/tasks/status
        +-- MCP notifications/progress
        +-- optional webhook later
```

## Storage Layout

Default root:

```text
~/.agent-background-jobs/
  jobs.sqlite
  logs/
    <job_id>.stdout.log
    <job_id>.stderr.log
  events/
    <job_id>.jsonl
  tmp/
    <job_id>/
```

Allow override:

```text
AGENT_BG_HOME=/path/to/state
```

Use SQLite for job metadata and event index. Keep logs and event JSONL as append-only files for easy inspection and recovery.

## Data Model

### Job

```json
{
  "job_id": "job_01h...",
  "name": "train baseline",
  "command": "uv run python train.py",
  "cwd": "/repo",
  "env": {"EXAMPLE": "value"},
  "status": "running",
  "pid": 12345,
  "created_at": "2026-07-08T18:00:00Z",
  "updated_at": "2026-07-08T18:10:00Z",
  "started_at": "2026-07-08T18:00:01Z",
  "ended_at": null,
  "exit_code": null,
  "timeout_seconds": 21600,
  "notify_on": ["checkpoint", "failed", "completed"],
  "stdout_path": ".../logs/job_01h.stdout.log",
  "stderr_path": ".../logs/job_01h.stderr.log",
  "events_path": ".../events/job_01h.jsonl",
  "event_file": null
}
```

Statuses:

```text
queued
running
input_required
completed
failed
cancelled
timeout
orphaned
unknown
```

For v1, `queued` may not be used if commands start immediately.

### Event

```json
{
  "event_id": "evt_01h...",
  "job_id": "job_01h...",
  "type": "checkpoint",
  "level": "info",
  "message": "epoch 10 complete",
  "data": {"epoch": 10, "loss": 0.42},
  "source": "stdout",
  "created_at": "2026-07-08T18:12:00Z",
  "seq": 42
}
```

For progress events, `data` should follow the shared progress shape where possible:

```json
{
  "current": 10,
  "total": 100,
  "unit": "epoch",
  "percent": 10,
  "stage": "training"
}
```

Required fields:

- `event_id`
- `job_id`
- `type`
- `created_at`
- `seq`

Optional fields:

- `level`
- `message`
- `data`
- `source`

Reserved event types:

```text
started
ready
checkpoint
progress
metric
input_required
completed
failed
cancelled
timeout
log_match
artifact
```

Custom event types are allowed.

## Event Input Formats

### Stdout/Stderr Inline Events

The simplest cross-language format:

```text
AGENT_EVENT {"type":"checkpoint","message":"epoch complete","data":{"epoch":10}}
```

Only parse lines that start with `AGENT_EVENT `.

Everything after the prefix must be JSON.

Malformed JSON should create a warning event or be ignored. It must not crash the server.

### Sidecar JSONL Event File

Some tools should not mix coordination events into logs. Let callers pass an event file:

```json
{
  "event_file": "/repo/runs/001/events.jsonl"
}
```

Each line:

```json
{"type":"checkpoint","message":"shard done","data":{"shard":3}}
```

The server watches or tails this file while the job runs.

Use simple file polling inside the server if cross-platform file watching is annoying. This is not model polling and is acceptable.

### Lifecycle Events

The server creates these automatically:

- `started`
- `completed`
- `failed`
- `cancelled`
- `timeout`
- `orphaned`

## MCP Tools

### job_start

Start a command in the background.

Input:

```json
{
  "command": "uv run python train.py",
  "cwd": "/repo",
  "name": "baseline run",
  "env": {"WANDB_MODE": "offline"},
  "timeout_seconds": 21600,
  "notify_on": ["checkpoint", "failed", "completed"],
  "event_file": "/repo/runs/baseline/events.jsonl"
}
```

Output:

```json
{
  "job_id": "job_01h...",
  "status": "running",
  "pid": 12345,
  "stdout_path": "...",
  "stderr_path": "...",
  "events_path": "...",
  "message": "Job started"
}
```

Notes:

- Use the platform shell by default.
- Later add `argv` mode for shell-free execution.
- Redact sensitive env values from stored metadata unless explicitly allowed.

### job_status

Get one job's current state.

Input:

```json
{"job_id": "job_01h..."}
```

Output:

```json
{
  "job_id": "job_01h...",
  "status": "running",
  "pid": 12345,
  "created_at": "...",
  "updated_at": "...",
  "last_event": {
    "type": "checkpoint",
    "message": "epoch 10 complete"
  },
  "progress": {
    "current": 10,
    "total": 100,
    "unit": "epoch",
    "percent": 10,
    "stage": "training",
    "updated_at": "..."
  }
}
```

### job_list

List jobs.

Input:

```json
{
  "status": ["running", "failed"],
  "limit": 50
}
```

Output:

```json
{
  "jobs": [
    {
      "job_id": "job_01h...",
      "name": "baseline run",
      "status": "running",
      "updated_at": "..."
    }
  ]
}
```

### job_tail

Return bounded log output.

Input:

```json
{
  "job_id": "job_01h...",
  "stream": "stdout",
  "max_bytes": 8192
}
```

Output:

```json
{
  "job_id": "job_01h...",
  "stream": "stdout",
  "truncated": true,
  "content": "..."
}
```

### job_events

Read stored events.

Input:

```json
{
  "job_id": "job_01h...",
  "since_event_id": "evt_01h...",
  "types": ["checkpoint", "failed"],
  "limit": 20
}
```

Output:

```json
{
  "events": []
}
```

### job_wait

Block until a matching event occurs.

Input:

```json
{
  "job_id": "job_01h...",
  "filters": ["checkpoint", "failed", "completed"],
  "since_event_id": null,
  "timeout_seconds": 21600
}
```

Output on event:

```json
{
  "result": "event",
  "job_id": "job_01h...",
  "status": "running",
  "event": {
    "event_id": "evt_01h...",
    "type": "checkpoint",
    "message": "epoch 10 complete",
    "data": {"epoch": 10}
  }
}
```

Output on timeout:

```json
{
  "result": "timeout",
  "job_id": "job_01h...",
  "status": "running",
  "message": "No matching event before timeout"
}
```

Required behavior:

- If a matching event already exists after `since_event_id`, return immediately.
- Otherwise block server-side.
- Unblock all matching waiters when an event arrives.
- Return compact event data, not full logs.

### job_stop

Stop a job.

Input:

```json
{
  "job_id": "job_01h...",
  "signal": "terminate",
  "kill_after_seconds": 10
}
```

Output:

```json
{
  "job_id": "job_01h...",
  "status": "cancelled",
  "message": "Job stopped"
}
```

Behavior:

- Try graceful termination.
- Kill after timeout.
- Record a `cancelled` event.
- Wake waiters.

### job_send

Optional v1 if base already supports stdin.

Input:

```json
{
  "job_id": "job_01h...",
  "input": "y\n"
}
```

### job_clear

Clear finished jobs and optionally logs.

Input:

```json
{
  "job_id": "job_01h...",
  "delete_logs": false
}
```

## Notification Behavior

There are two notification paths.

### Required Path: Blocking Wait

`job_wait` is required because it works in every MCP client that supports long-running tool calls.

This is the reliable no-polling primitive.

### Optional Path: MCP Notifications

Where available, send:

- `notifications/tasks/status` for status changes.
- `notifications/progress` for checkpoint/progress events.
- `notifications/message` for warnings, such as malformed event lines.

Do not depend on MCP notifications for correctness. The MCP spec says task status notifications are optional and requestors must not rely on them.

## Agent Usage Patterns

### ML Experiment

```text
job_start:
  command: uv run python train.py --config configs/baseline.yaml
  notify_on: [checkpoint, metric, failed, completed]

agent then:
  job_wait(job_id, [checkpoint, metric, failed, completed], timeout=21600)
```

Training script emits:

```python
print("AGENT_EVENT " + json.dumps({
    "type": "metric",
    "message": "validation complete",
    "data": {"epoch": 10, "val_loss": 0.42}
}), flush=True)
```

### Dev Server

```text
job_start:
  command: npm run dev
  notify_on: [ready, failed]
```

Event can be regex-derived later:

```text
log line contains "Local: http://localhost:"
  -> ready event
```

Regex-derived events are optional v1. Inline `AGENT_EVENT` is simpler.

### Fuzzer

```text
job_start:
  command: cargo fuzz run parser
  notify_on: [artifact, failed, completed]
```

Fuzzer wrapper emits:

```text
AGENT_EVENT {"type":"artifact","message":"crash found","data":{"path":"crashes/id-001"}}
```

### Test Watcher

```text
job_start:
  command: npm test -- --watch
  notify_on: [checkpoint, failed]
```

Wrapper emits event when failure count changes.

## Implementation Plan

### Phase 1: Durable State

1. Fork `mcp-background-job`.
2. Add state root configuration:
   - `AGENT_BG_HOME`
   - default `~/.agent-background-jobs`
3. Add SQLite schema for jobs and events.
4. Keep full stdout/stderr logs as files.
5. Update existing job start/status/list tools to persist metadata.

SQLite schema sketch:

```sql
CREATE TABLE jobs (
  job_id TEXT PRIMARY KEY,
  name TEXT,
  command TEXT NOT NULL,
  cwd TEXT,
  status TEXT NOT NULL,
  pid INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  started_at TEXT,
  ended_at TEXT,
  exit_code INTEGER,
  timeout_seconds INTEGER,
  notify_on TEXT,
  stdout_path TEXT,
  stderr_path TEXT,
  events_path TEXT,
  event_file TEXT
);

CREATE TABLE events (
  event_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  type TEXT NOT NULL,
  level TEXT,
  message TEXT,
  data_json TEXT,
  source TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(job_id) REFERENCES jobs(job_id)
);

CREATE INDEX idx_events_job_seq ON events(job_id, seq);
CREATE INDEX idx_events_job_type_seq ON events(job_id, type, seq);
```

### Phase 2: Event Parsing

1. Wrap stdout/stderr readers.
2. Append raw output to log files.
3. For each complete line, detect `AGENT_EVENT ` prefix.
4. Parse JSON payload.
5. Normalize into event row and append JSONL.
6. Ignore malformed lines safely.
7. Generate lifecycle events.

Event normalization:

```python
event = {
    "event_id": new_event_id(),
    "job_id": job_id,
    "seq": next_seq(job_id),
    "type": payload["type"],
    "level": payload.get("level", "info"),
    "message": payload.get("message"),
    "data": payload.get("data", {}),
    "source": "stdout",
    "created_at": now_iso(),
}
```

### Phase 3: job_wait

1. Add an in-memory wait registry keyed by `job_id`.
2. Use `asyncio.Condition` or per-job `asyncio.Queue`.
3. `job_wait` first checks stored events after `since_event_id`.
4. If none exist, it waits on the condition.
5. On event insert, notify all waiters for that job.
6. Waiters re-check the store and return the first matching event.
7. Timeout returns cleanly.

Use store re-checking instead of passing event objects directly. This avoids missed wakeups and handles races.

### Phase 4: MCP Notifications

1. On job status transitions, emit `notifications/tasks/status` where SDK supports it.
2. On checkpoint/progress events, emit `notifications/progress` if a progress token exists or use `notifications/message` as fallback.
3. Keep notifications best-effort.
4. Tests should not depend on client wake behavior.

### Phase 5: Sidecar Event File

1. Accept `event_file` in `job_start`.
2. Tail the file while job is running.
3. Parse each new JSONL line as an event.
4. Record source as `event_file`.
5. Stop tailer when job ends.

Use simple interval polling in the server for v1. This is internal process monitoring, not agent/model polling.

### Phase 6: Documentation And Examples

Add:

- README quickstart.
- MCP client config examples.
- Python event helper.
- Shell event helper.
- ML run example.
- Long-running generic command example.
- "How to avoid polling" example using `job_wait`.

## Acceptance Criteria

### Core

- Agent can start a long-running job and receive a job ID immediately.
- Job stdout/stderr are persisted to files.
- Job metadata is persisted.
- Job lifecycle events are persisted.
- Agent can call `job_wait` and receive no response until a selected event occurs.
- `job_wait` returns immediately if a matching stored event already exists.
- `job_wait` does not busy-poll in the agent/model loop.
- `job_wait` supports timeouts.
- Multiple waiters on the same job are all woken by a matching event.
- Multiple jobs do not cross-notify.
- Killing a job records `cancelled` and wakes waiters.
- Nonzero exit records `failed` and wakes waiters.
- Zero exit records `completed` and wakes waiters.
- Progress events are persisted and exposed as the latest progress snapshot in `job_status`.
- Malformed `AGENT_EVENT` lines do not crash the server.

### Persistence

- Completed jobs are visible after MCP server restart.
- Logs remain available after restart.
- Events remain available after restart.
- Running jobs after server restart are either re-adopted or marked `orphaned`/`unknown`.

For v1, process adoption is optional. Marking running jobs as `orphaned` on restart is acceptable if documented.

### Safety

- Log reads are bounded by `max_bytes`.
- Tool responses never include unbounded stdout/stderr.
- `job_stop` has kill-after behavior.
- Job count and log retention limits are configurable.
- Environment values are not dumped in full by default.

### Compatibility

- Server runs with `uvx`.
- Development uses `uv sync`.
- Tests run with `uv run pytest`.
- Works on Windows, macOS, and Linux where possible.

## Tests

### Unit Tests

- Parse valid inline event.
- Ignore malformed inline event.
- Reject event without `type`.
- Normalize event with default `level`.
- Match event by type filter.
- Match terminal event filters.
- Normalize progress event data.
- Return latest progress in job status.
- Persist job row.
- Reload job row.
- Persist event row.
- Reload events after `since_event_id`.
- Bounded tail returns last N bytes.
- Bounded tail marks `truncated`.
- Generate `completed` lifecycle event from exit code `0`.
- Generate `failed` lifecycle event from nonzero exit.
- Generate `cancelled` event on stop.
- `job_wait` returns stored prior event.
- `job_wait` times out.

### Integration Tests

Use tiny Python commands run through the actual job manager.

#### Checkpoint wait

Command:

```cmd
python -c "import time,json; time.sleep(1); print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'done'}), flush=True); time.sleep(1)"
```

Expected:

- `job_wait(filters=["checkpoint"])` returns checkpoint event.
- Job remains running until process exits.

#### Completion wait

Command:

```cmd
python -c "print('hello')"
```

Expected:

- `job_wait(filters=["completed"])` returns completed event.
- Status is `completed`.

#### Failure wait

Command:

```cmd
python -c "import sys; sys.exit(3)"
```

Expected:

- `job_wait(filters=["failed"])` returns failed event.
- Exit code is `3`.

#### Cancel wait

Command:

```cmd
python -c "import time; time.sleep(60)"
```

Expected:

- `job_stop` changes status to `cancelled`.
- Existing `job_wait(filters=["cancelled"])` returns.

#### Sidecar event

Command writes JSONL event to a known file after sleep.

Expected:

- `job_wait(filters=["checkpoint"])` returns sidecar event.
- Event source is `event_file`.

#### Multiple waiters

Start two concurrent `job_wait` calls for the same event.

Expected:

- Both return the same matching event or equivalent event data.

#### Multiple jobs

Start two jobs. Emit checkpoint from job A.

Expected:

- Waiter for job A returns.
- Waiter for job B remains waiting.

#### Restart persistence

Start and complete a job. Restart server.

Expected:

- `job_list(status=["completed"])` includes job.
- `job_events(job_id)` returns completion event.
- `job_tail(job_id)` returns logs.

## Example Helper Libraries

### Python Helper

Optional helper module:

```python
import json


def agent_event(event_type: str, message: str | None = None, **data):
    payload = {"type": event_type, "data": data}
    if message:
        payload["message"] = message
    print("AGENT_EVENT " + json.dumps(payload), flush=True)
```

Usage:

```python
agent_event("checkpoint", "epoch complete", epoch=10, val_loss=0.42)
```

### Shell Helper

```sh
agent_event() {
  type="$1"
  message="$2"
  printf 'AGENT_EVENT {"type":"%s","message":"%s"}\n' "$type" "$message"
}
```

## Example Agent Prompt

```text
Start the training run in the background. Wait only for checkpoint, failed, or completed events. When a checkpoint arrives, inspect the metric payload and decide whether to continue waiting, stop the job, or launch a new experiment.
```

Expected tool flow:

```text
job_start(...)
job_wait(job_id, ["checkpoint", "failed", "completed"], 21600)
```

No loop of:

```text
job_status
sleep
job_status
tail logs
sleep
...
```

## Edge Cases

### Client tool timeout

Some MCP clients may impose maximum tool call durations. If a client cannot keep `job_wait` open for hours, the agent should call `job_wait` with the largest practical timeout and rely on MCP notifications or a later manual wait call.

This is a client limitation, not a server bug.

### Server restart while job running

V1 can mark previously running jobs as `orphaned` if the server restarts and cannot safely reattach.

Later improvement:

- Store process group ID.
- Re-detect live PIDs.
- Reattach log readers where possible.

### Process writes partial lines

Event parser should only parse complete newline-terminated lines.

### Huge event payload

Cap event payload size. If event JSON exceeds limit, store truncated data and message.

Default cap: 64 KB per event.

### Log growth

Add retention config:

```text
max_log_bytes_per_job
max_completed_jobs
max_job_age_days
```

Do not implement aggressive deletion until v1 behavior is stable.

### Secrets

Logs may contain secrets. This product cannot fully prevent that.

Mitigations:

- Keep state local by default.
- Do not send full logs in tool responses.
- Redact env metadata.
- Document risk.

## Configuration

Environment variables:

```text
AGENT_BG_HOME
AGENT_BG_MAX_LOG_BYTES_RESPONSE
AGENT_BG_MAX_EVENT_BYTES
AGENT_BG_DEFAULT_WAIT_TIMEOUT
AGENT_BG_PROCESS_KILL_AFTER_SECONDS
```

Suggested defaults:

```text
AGENT_BG_MAX_LOG_BYTES_RESPONSE=8192
AGENT_BG_MAX_EVENT_BYTES=65536
AGENT_BG_DEFAULT_WAIT_TIMEOUT=3600
AGENT_BG_PROCESS_KILL_AFTER_SECONDS=10
```

## Development Commands

Use `uv` only.

```cmd
uv sync
```

```cmd
uv run pytest
```

```cmd
uv run python -m mcp_background_job
```

Example manual smoke test:

```cmd
uv run python -m mcp_background_job
```

Exact MCP client config depends on the host client.

## Migration From Base

Start by preserving existing `mcp-background-job` public tools where practical.

Add new tools rather than breaking old names:

- Keep old execute/status/tail/terminate equivalents.
- Add `job_*` aliases if names differ.
- Mark old names as compatibility aliases in docs.

This keeps the fork usable immediately.

## Future Work

Only add after v1 works.

- PTY backend, possibly borrowing ideas from `pilotty`.
- Regex-derived events configured at `job_start`.
- Webhook notifications.
- Desktop notifications.
- Small TUI.
- Process adoption after restart.
- Job groups.
- Parent/child jobs.
- Resource metrics: CPU, memory, GPU.
- Artifact indexing.
- Retention cleanup command.
- Temporal/Trigger adapter for users who outgrow local jobs.

## Recommended First PRs

1. Add durable job/event storage.
2. Add inline `AGENT_EVENT` parser.
3. Add lifecycle events.
4. Add `job_wait`.
5. Add integration tests for checkpoint/completed/failed/cancelled.
6. Add README examples.

Stop there for v1. That is the useful product.
