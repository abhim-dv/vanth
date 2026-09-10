"""Minimal, dependency-free cron/interval time computation for Vanth.

A schedule fires either on a 5-field cron expression or on a fixed interval.
Cron matching is performed by scanning UTC minutes and comparing the *local*
wall-clock fields of each minute in the schedule's timezone. That makes DST
handling automatic: a local time that does not exist (spring forward) is simply
never produced, and an ambiguous local time (fall back) matches once per UTC
minute that maps to it.

No third-party dependency (``zoneinfo`` is stdlib). Kept small and pure so it is
cheap to test; the dispatcher in :mod:`vanth.server` owns all persistence.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Names handled without the IANA database (always available, even where the OS
# ships no tzdata and the ``tzdata`` package is not installed).
_UTC_ALIASES = {"UTC", "Etc/UTC", "GMT", "Z"}

# Inclusive bounds for each field.
_FIELD_RANGES = {
    "minute": (0, 59),
    "hour": (0, 23),
    "dom": (1, 31),
    "month": (1, 12),
    "dow": (0, 6),  # 0 = Sunday, matching cron
}
_FIELD_ORDER = ("minute", "hour", "dom", "month", "dow")

_MACROS = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}


def _zone(timezone_name: str) -> tzinfo:
    if timezone_name in _UTC_ALIASES:
        return timezone.utc
    try:
        return ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError, TypeError) as exc:
        raise ValueError(f"unknown timezone: {timezone_name!r}") from exc


def _parse_field(text: str, name: str) -> set[int]:
    low, high = _FIELD_RANGES[name]
    if name == "dow":
        # Cron also accepts 7 as Sunday; normalized to 0 after parsing.
        high = 7
    values: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ValueError(f"empty {name} field")
        step = 1
        if "/" in part:
            part, _, step_text = part.partition("/")
            try:
                step = int(step_text)
            except ValueError as exc:
                raise ValueError(f"invalid step in {name} field: {text!r}") from exc
            if step < 1:
                raise ValueError(f"step must be >= 1 in {name} field: {text!r}")
        if part in {"*", ""}:
            start, end = low, high
        elif "-" in part:
            start_text, _, end_text = part.partition("-")
            try:
                start, end = int(start_text), int(end_text)
            except ValueError as exc:
                raise ValueError(f"invalid range in {name} field: {text!r}") from exc
        else:
            try:
                start = end = int(part)
            except ValueError as exc:
                raise ValueError(f"invalid value in {name} field: {text!r}") from exc
        if start > end:
            raise ValueError(f"range start > end in {name} field: {part!r}")
        if start < low or end > high:
            raise ValueError(f"{name} value out of range [{low},{high}]: {part!r}")
        values.update(range(start, end + 1, step))
    if name == "dow" and 7 in values:
        values.discard(7)
        values.add(0)
    return values


def validate_cron(expr: str) -> str:
    """Validate a 5-field cron expression (or a supported ``@macro``)."""
    if not isinstance(expr, str) or not expr.strip():
        raise ValueError("cron must be a non-empty string")
    text = expr.strip().lower() if expr.strip().startswith("@") else expr.strip()
    if text.startswith("@"):
        if text not in _MACROS:
            raise ValueError(f"unknown cron macro: {expr!r}")
        text = _MACROS[text]
    fields = text.split()
    if len(fields) != 5:
        raise ValueError("cron must have exactly 5 fields: minute hour day-of-month month day-of-week")
    for field, name in zip(fields, _FIELD_ORDER):
        _parse_field(field, name)
    return expr.strip()


def parse_cron(expr: str) -> tuple[set[int], set[int], set[int], set[int], set[int]]:
    """Parse a validated cron expression into five value sets."""
    validate_cron(expr)
    text = expr.strip().lower() if expr.strip().startswith("@") else expr.strip()
    if text.startswith("@"):
        text = _MACROS[text]
    fields = text.split()
    return tuple(_parse_field(field, name) for field, name in zip(fields, _FIELD_ORDER))  # type: ignore[return-value]


def _cron_dow(local: datetime) -> int:
    # Python weekday(): Mon=0..Sun=6; cron: Sun=0..Sat=6.
    return (local.weekday() + 1) % 7


def _matches(fields: tuple[set[int], set[int], set[int], set[int], set[int]], local: datetime) -> bool:
    minute, hour, dom, month, dow = fields
    if local.minute not in minute or local.hour not in hour or local.month not in month:
        return False
    dom_restricted = dom != set(range(1, 32))
    dow_restricted = dow != set(range(0, 7))
    dom_match = local.day in dom
    dow_match = _cron_dow(local) in dow
    if dom_restricted and dow_restricted:
        # POSIX: when both day fields are restricted, either may match.
        return dom_match or dow_match
    if dom_restricted:
        return dom_match
    if dow_restricted:
        return dow_match
    return True


def next_cron_fire(
    expr: str,
    *,
    timezone_name: str = "UTC",
    after: datetime | None = None,
    max_years: int = 5,
) -> datetime:
    """Return the first UTC fire time strictly after ``after``."""
    if after is None:
        after = datetime.now(timezone.utc)
    if after.tzinfo is None:
        after = after.replace(tzinfo=timezone.utc)
    tz = _zone(timezone_name)
    fields = parse_cron(expr)
    current = after.astimezone(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
    limit = current + timedelta(days=366 * max_years)
    while current <= limit:
        if _matches(fields, current.astimezone(tz)):
            return current
        current += timedelta(minutes=1)
    raise ValueError(f"cron {expr!r} has no fire time within {max_years} years")


def next_cron_fires(
    expr: str,
    *,
    timezone_name: str = "UTC",
    after: datetime | None = None,
    count: int = 5,
) -> list[datetime]:
    """Return the next ``count`` UTC fire times (preview)."""
    fires: list[datetime] = []
    cursor = after
    for _ in range(max(1, count)):
        cursor = next_cron_fire(expr, timezone_name=timezone_name, after=cursor)
        fires.append(cursor)
    return fires


def validate_timezone(name: str) -> str:
    """Validate and normalize an IANA timezone name."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("timezone must be a non-empty string")
    _zone(name.strip())
    return name.strip()


def validate_schedule_spec(*, cron: str | None, interval_seconds: int | None) -> None:
    """Require exactly one of ``cron`` / ``interval_seconds`` and validate it."""
    if (cron is None) == (interval_seconds is None):
        raise ValueError("exactly one of cron or interval_seconds is required")
    if cron is not None:
        validate_cron(cron)
    if interval_seconds is not None:
        if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, int) or interval_seconds < 1:
            raise ValueError("interval_seconds must be an integer >= 1")


def compute_next_fire(
    *,
    cron: str | None = None,
    interval_seconds: int | None = None,
    timezone_name: str = "UTC",
    after: datetime | None = None,
) -> datetime:
    """Next UTC fire time for an interval or cron schedule."""
    if after is None:
        after = datetime.now(timezone.utc)
    if after.tzinfo is None:
        after = after.replace(tzinfo=timezone.utc)
    if interval_seconds is not None:
        return after.astimezone(timezone.utc) + timedelta(seconds=int(interval_seconds))
    if cron is None:
        raise ValueError("cron is required when interval_seconds is not set")
    return next_cron_fire(cron, timezone_name=timezone_name, after=after)
