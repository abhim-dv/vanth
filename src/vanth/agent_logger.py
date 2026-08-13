"""Loguru-based structured logging for Vanth jobs.

`from vanth.agent_logger import logger` then `logger.info(...)`. Every record is
routed to stdout as an `AGENT_EVENT` line with a `log` type, so the daemon
persists it as a timestamped, level-aware structured event (visible in the exact
event table) instead of a bare text line.

Unlike a plain `print`, loguru gives you levels, timestamps, exception
capture, and the option to also mirror records to a file or stderr while keeping
the Vanth event stream clean.
"""

from __future__ import annotations

import sys
from typing import Any

from loguru import logger as _logger


def _vanth_sink(message: Any) -> None:
    record = message.record
    payload = {
        "type": "log",
        "level": record["level"].name.lower(),
        "message": record["message"],
    }
    extra = _clean_extra(dict(record.get("extra") or {}))
    if extra:
        payload["data"] = extra
    exception = record.get("exception")
    if exception is not None and exception.type is not None:
        payload["message"] = f"{payload['message']} :: {exception.type.__name__}: {exception.value}"
    print("AGENT_EVENT " + _json(payload), flush=True)


def _json(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=str)


# A module-level logger whose default sink emits AGENT_EVENT log lines. The
# original stderr default is removed so records do not double-print.
logger = _logger.bind(__name__="vanth")
logger.remove()  # drop loguru's default stderr sink
logger.add(_vanth_sink, format="{message}", level="TRACE")


def _clean_extra(extra: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in extra.items() if key != "__name__"}


def log_with_context(level: str, message: str, **context: Any) -> None:
    """Emit a log event with extra context carried in the event `data`."""
    method = getattr(logger, level.lower(), logger.info)
    method(message, **context)


__all__ = ["logger", "log_with_context"]
