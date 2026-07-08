import json


def agent_event(event_type: str, message: str | None = None, **data: object) -> None:
    payload: dict[str, object] = {"type": event_type, "data": data}
    if message is not None:
        payload["message"] = message
    print("AGENT_EVENT " + json.dumps(payload, separators=(",", ":")), flush=True)


def progress(
    current: float,
    total: float | None = None,
    unit: str | None = None,
    stage: str | None = None,
    message: str | None = None,
) -> None:
    data: dict[str, object] = {"current": current}
    if total is not None:
        data["total"] = total
        data["percent"] = round((current / total) * 100, 2) if total else 0
    if unit is not None:
        data["unit"] = unit
    if stage is not None:
        data["stage"] = stage
    agent_event("progress", message, **data)
