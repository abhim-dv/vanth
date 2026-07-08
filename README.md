# vanth

Event-driven background jobs for agents.

```cmd
uv sync
```

```cmd
uv run vanth
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

Example job:

```cmd
uv run python examples\long_job.py
```
