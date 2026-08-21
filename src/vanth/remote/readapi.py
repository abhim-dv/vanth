"""Read-API projection across local jobs and current remote shadows (Phase 3).

The local ``JobManager`` never reads remote tables (plan: "Remote rows are
unreachable from local PID, heartbeat, stdin, timeout, stop, recovery, trigger,
quota, and cleanup code"). These thin projection helpers are the ONLY place
that merges the two views, and they are used exclusively by the daemon's
``/remotes/<id>/...`` routes — never by the local runner/process paths.
"""

from __future__ import annotations

from typing import Any


def projected_jobs(manager: Any, store: Any, remote_id: str, *, limit: int = 50) -> dict[str, Any]:
    """Merge the remote's current shadows with its locally-known jobs.

    On a controller this surfaces shadow rows; on a remote daemon it surfaces
    the same shape from the local jobs table so both sides speak one schema.
    """
    store.get_remote(remote_id)  # raises for unknown remote
    shadows = store.current_shadows(remote_id)
    jobs = []
    for shadow in shadows[:limit]:
        payload = shadow.get("payload") or {}
        jobs.append(
            {
                "job_id": shadow["remote_job_id"],
                "remote_id": remote_id,
                "shadow": True,
                "status": shadow["status"],
                "name": payload.get("name"),
                "command": payload.get("command"),
                "created_at": shadow["created_at"],
                "updated_at": shadow["updated_at"],
                "exit_code": payload.get("exit_code"),
            }
        )
    return {"jobs": jobs, "remote_id": remote_id}


def projected_status(manager: Any, store: Any, remote_id: str, remote_job_id: str) -> dict[str, Any]:
    """Status of one remote job from the controller's shadow (no SSH round trip)."""
    shadow = store.get_shadow(remote_id, remote_job_id)
    payload = shadow.get("payload") or {}
    return {
        "job_id": remote_job_id,
        "remote_id": remote_id,
        "shadow": True,
        "status": shadow["status"],
        "name": payload.get("name"),
        "command": payload.get("command"),
        "exit_code": payload.get("exit_code"),
        "updated_at": shadow["updated_at"],
    }


def projected_dashboard(manager: Any, store: Any, remote_id: str, *, limit: int = 5000) -> dict[str, Any]:
    """Chart-data view over the remote's shadows (metrics live in the payload)."""
    store.get_remote(remote_id)
    series: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for shadow in store.current_shadows(remote_id):
        payload = shadow.get("payload") or {}
        metrics = payload.get("metrics") or {}
        if not isinstance(metrics, dict):
            continue
        entries = {}
        for metric, points in metrics.items():
            if isinstance(points, list):
                entries[metric] = points
        if entries:
            series[shadow["remote_job_id"]] = entries
    return {"job_ids": list(series), "series": series, "remote_id": remote_id}
