# vanth

Event-driven background jobs for agents.

```cmd
uv sync
```

```cmd
uv run vanth
```

`vanth` is the MCP interface. It talks to the local daemon, starting it when needed:

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

Example job:

```cmd
uv run python examples\long_job.py
```

V0 jobs run with stdin closed (`DEVNULL`). Interactive input and `job_send` are intentionally not supported yet.

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
