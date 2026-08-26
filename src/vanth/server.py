from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import secrets
import shutil
import struct
import subprocess
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from logging.handlers import RotatingFileHandler

# mcp's FastMCP has a Settings model with a `lifespan` field whose annotation
# contains an unresolved forward reference; pydantic-settings >=2.15 warns about
# it on import and on every console-script invocation (including the ops CLI,
# which never touches MCP). The warning is upstream noise — suppress it so a
# fresh install doesn't print a scary traceback-shaped message.
try:
    from pydantic_settings.exceptions import IncompleteFieldDefinitionWarning as _IncompleteFieldWarning

    warnings.filterwarnings("ignore", category=_IncompleteFieldWarning)
except Exception:
    pass

from mcp.server.fastmcp import FastMCP

from .client import VanthClient
from .codex_bridge import send_delivery_to_codex
from .migrations import LATEST_SCHEMA_VERSION, configure_connection, migrate
from .opencode_bridge import OpenCodeSessionNotFound, send_delivery_to_opencode
from .paths import canonical_home
from .runtime_info import capture_run_metadata, serialize_run_metadata

EVENT_PREFIX = "AGENT_EVENT "
DEFAULT_MAX_EVENT_BYTES = 65536
DEFAULT_MAX_EVENT_LINE_BYTES = 1024 * 1024
DEFAULT_MAX_LOG_BYTES = 10 * 1024 * 1024
DEFAULT_MAX_ERROR_BYTES = 4096
TERMINAL_STATUSES = {"completed", "failed", "timeout", "cancelled", "orphaned"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ms_to_iso(ms: int) -> str:
    """Convert epoch milliseconds to the RFC3339 text Vanth stores."""
    try:
        ms = int(ms)
    except (TypeError, ValueError):
        raise ValueError("timestamp must be epoch milliseconds (int)") from None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _downsample(points: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Uniformly reduce a series to at most ``limit`` points.

    When the series is larger than ``limit``, points are sampled by index to
    keep an even spread across the whole series (same idea as the Go
    monitor's downsample). The first and last points are always retained.
    """
    n = len(points)
    if n <= limit or limit <= 0:
        return points
    if limit == 1:
        return [points[0]]
    indices = sorted(set(int(round(i * (n - 1) / (limit - 1))) for i in range(limit)))
    return [points[i] for i in indices]


def _parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _runtime_seconds(started_at: str | None, ended_at: str | None) -> float | None:
    start = _parse_iso(started_at) if started_at else None
    if start is None:
        return None
    end = _parse_iso(ended_at) if ended_at else datetime.now(timezone.utc)
    seconds = (end - start).total_seconds()
    return max(0.0, seconds)


def parse_agent_event_line(line: str) -> dict[str, Any] | None:
    if not line.startswith(EVENT_PREFIX):
        return None
    try:
        payload = json.loads(line[len(EVENT_PREFIX) :])
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("type"), str):
        return None
    if payload.get("message") is not None and not isinstance(payload["message"], str):
        return None
    if payload.get("level") is not None and not isinstance(payload["level"], str):
        return None
    return payload


def normalize_event_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if not isinstance(data, dict):
        data = {}
    if payload["type"] == "progress":
        current = data.get("current")
        total = data.get("total")
        if "percent" not in data and isinstance(current, (int, float)) and isinstance(total, (int, float)):
            data["percent"] = round((current / total) * 100, 2) if total else 0
        if isinstance(data.get("percent"), (int, float)):
            data["percent"] = max(0, min(100, data["percent"]))
    return {
        "type": payload["type"],
        "level": payload.get("level", "info"),
        "message": payload.get("message"),
        "data": data,
    }


ATTENTION_EVENTS = {"needs_input", "permission_required", "blocked"}
WAKE_TARGET_TYPES = {"local_command", "codex_thread", "opencode_thread"}


def validate_wake_targets(targets: list[dict[str, Any]] | None) -> None:
    if targets is None:
        return
    if not isinstance(targets, list):
        raise ValueError("wake_targets must be a list")
    for target in targets:
        if not isinstance(target, dict):
            raise ValueError("each wake target must be an object")
        target_type = target.get("type")
        if target_type not in WAKE_TARGET_TYPES:
            raise ValueError(f"unsupported wake target type: {target_type!r}")
        events = target.get("events", target.get("notify_on", []))
        if not isinstance(events, list) or not all(isinstance(event, str) for event in events):
            raise ValueError("wake target events must be a list of strings")
        command = target.get("command")
        if command is not None and not (
            (isinstance(command, str) and command) or (isinstance(command, list) and command)
        ):
            raise ValueError("wake target command must be a non-empty string or argv list")
        if target_type == "local_command" and command is None:
            raise ValueError("local_command target requires command")
        if target_type != "local_command" and command is None:
            thread_id = target.get("thread_id") or target.get("threadId") or target.get("session_id") or target.get("sessionId")
            if not isinstance(thread_id, str) or not thread_id:
                raise ValueError(f"{target_type} target requires thread_id")
        for key, minimum in (("timeout_seconds", 1), ("max_attempts", 1), ("retry_delay_seconds", 0)):
            if key in target and (not isinstance(target[key], int) or isinstance(target[key], bool) or target[key] < minimum):
                raise ValueError(f"wake target {key} must be an integer >= {minimum}")


def validate_limit(value: int, name: str, maximum: int = 1000) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > maximum:
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


class JobManager:
    def __init__(self, home: str | Path | None = None, *, recover: bool = True) -> None:
        self.home = canonical_home(home)
        self.max_event_bytes = int(
            os.environ.get("VANTH_MAX_EVENT_BYTES")
            or os.environ.get("AGENT_BG_MAX_EVENT_BYTES")
            or DEFAULT_MAX_EVENT_BYTES
        )
        self.max_event_line_bytes = int(os.environ.get("VANTH_MAX_EVENT_LINE_BYTES", DEFAULT_MAX_EVENT_LINE_BYTES))
        self.max_log_bytes = int(os.environ.get("VANTH_MAX_LOG_BYTES", DEFAULT_MAX_LOG_BYTES))
        self.delivery_lease_margin = int(os.environ.get("VANTH_DELIVERY_LEASE_MARGIN", "5"))
        self.heartbeat_interval = float(os.environ.get("VANTH_RUNNER_HEARTBEAT_INTERVAL", "1"))
        self.heartbeat_stale_after = float(os.environ.get("VANTH_RUNNER_HEARTBEAT_STALE_AFTER", "10"))
        self.recovery_kill_timeout = max(0, int(os.environ.get("VANTH_RECOVERY_KILL_TIMEOUT", "10")))
        self.logs = self.home / "logs"
        self.events_dir = self.home / "events"
        self.logs.mkdir(parents=True, exist_ok=True)
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.backup_path = None
        self.db = sqlite3.connect(self.home / "jobs.sqlite", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        configure_connection(self.db)
        self.db_lock = threading.RLock()
        self._closed = False
        self._close_lock = threading.Lock()
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.reader_threads: dict[str, list[threading.Thread]] = {}
        self.conditions: dict[str, threading.Condition] = {}
        self._log_truncated: set[tuple[str, str]] = set()
        self._metric_ingest_keys: set[str] = set()
        self._delivery_threads: set[threading.Thread] = set()
        self._delivery_threads_lock = threading.Lock()
        self.max_delivery_concurrency = max(1, int(os.environ.get("VANTH_DELIVERY_MAX_CONCURRENT", "4")))
        self.max_running_jobs = max(0, int(os.environ.get("VANTH_MAX_RUNNING_JOBS", "0")))
        self.max_retention_seconds = max(0, int(os.environ.get("VANTH_RETENTION_SECONDS", "0")))
        self.retention_interval_seconds = max(0, int(os.environ.get("VANTH_RETENTION_INTERVAL_SECONDS", "3600")))
        self.retention_dry_run = os.environ.get("VANTH_RETENTION_DRY_RUN", "1") != "0"
        self._last_retention_run: float | None = None
        self.shutdown_requested = threading.Event()
        self.max_events_per_job = max(1, int(os.environ.get("VANTH_MAX_EVENTS_PER_JOB", "100000")))
        self._events_truncated: set[str] = set()
        self.logger = logging.getLogger(f"vanth.manager.{id(self)}")
        self.logger.setLevel(os.environ.get("VANTH_LOG_LEVEL", "INFO").upper())
        self.logger.propagate = False
        if not self.logger.handlers:
            handler = RotatingFileHandler(
                self.logs / "daemon.log",
                maxBytes=int(os.environ.get("VANTH_LOG_MAX_BYTES", str(5 * 1024 * 1024))),
                backupCount=int(os.environ.get("VANTH_LOG_BACKUP_COUNT", "3")),
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s pid=%(process)d component=manager %(message)s"))
            self.logger.addHandler(handler)
        self.dispatch_enabled = recover
        self.dispatcher_stop = threading.Event()
        self.dispatcher_thread: threading.Thread | None = None
        self.backup_path = migrate(self.db, self.home)
        if recover:
            self._recover_jobs()
            self._reconcile_running_jobs()
        if recover:
            self._dispatch_due_deliveries()
            self.dispatcher_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
            self.dispatcher_thread.start()

    def _pid_alive(self, pid: int | None) -> bool:
        if not pid:
            return False
        try:
            if sys.platform == "win32":
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    timeout=2,
                )
                return str(pid) in result.stdout
            os.kill(pid, 0)
            return True
        except Exception:
            return False

    def _recover_jobs(self) -> None:
        rows = self.db.execute("SELECT job_id, worker_pid, stop_requested_at FROM jobs WHERE status='running'").fetchall()
        for row in rows:
            if self._pid_alive(row["worker_pid"]):
                continue
            workload = self._row("SELECT pid FROM jobs WHERE job_id=?", (row["job_id"],))
            if workload and workload["pid"]:
                if not self._terminate_pid(int(workload["pid"]), force=True, deadline=time.monotonic() + self.recovery_kill_timeout):
                    self.logger.error("recovery could not terminate workload job_id=%s pid=%s", row["job_id"], workload["pid"])
                    continue
            terminal = "cancelled" if row["stop_requested_at"] else "orphaned"
            if self._transition_terminal(row["job_id"], terminal):
                self._emit(row["job_id"], terminal, message="Job runner was not alive during recovery")

    def _reconcile_running_jobs(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.heartbeat_stale_after)
        cutoff_text = cutoff.isoformat().replace("+00:00", "Z")
        rows = self.db.execute(
            "SELECT job_id, worker_pid, pid, stop_requested_at FROM jobs WHERE status='running' AND (runner_heartbeat_at IS NULL OR runner_heartbeat_at < ?)",
            (cutoff_text,),
        ).fetchall()
        for row in rows:
            if self._pid_alive(row["worker_pid"]):
                continue
            if row["pid"]:
                if not self._terminate_pid(int(row["pid"]), force=True, deadline=time.monotonic() + self.recovery_kill_timeout):
                    self.logger.error("heartbeat reconciliation could not terminate workload job_id=%s pid=%s", row["job_id"], row["pid"])
                    continue
            terminal = "cancelled" if row["stop_requested_at"] else "orphaned"
            if self._transition_terminal(row["job_id"], terminal):
                self._emit(row["job_id"], terminal, message="Runner heartbeat is stale and the runner is not alive")

    def begin_shutdown(self) -> None:
        self.shutdown_requested.set()
        for condition in self.conditions.values():
            with condition:
                condition.notify_all()

    def _transition_terminal(self, job_id: str, status: str, exit_code: int | None = None) -> bool:
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"invalid terminal status: {status}")
        self._ensure_open()
        stamp = now_iso()

        def transition() -> bool:
            with self.db_lock:
                changed = self.db.execute(
                    "UPDATE jobs SET status=?, exit_code=?, ended_at=?, updated_at=?, stop_requested_at=NULL WHERE job_id=? AND status='running'",
                    (status, exit_code, stamp, stamp, job_id),
                ).rowcount
                self.db.commit()
            return bool(changed)

        return self._retry_locked(transition)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("JobManager is closed")

    def _retry_locked(self, fn, *args, attempts: int = 5):
        """Run fn, retrying transient SQLite write-lock contention.

        The per-process db_lock serializes threads inside one process, but
        runners and the daemon are separate processes sharing one database.
        A short retry loop keeps a transient ``database is locked`` from
        killing a runner thread or abandoning a critical write.
        """
        for attempt in range(attempts):
            try:
                return fn(*args)
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == attempts - 1:
                    raise
                time.sleep(0.02 * (attempt + 1))
        raise RuntimeError("unreachable")  # pragma: no cover

    def _dispatch_loop(self) -> None:
        while not self.dispatcher_stop.wait(float(os.environ.get("VANTH_DELIVERY_POLL_INTERVAL", "0.2"))):
            try:
                self._dispatch_due_deliveries()
                self._reconcile_running_jobs()
                self._fire_triggered_jobs()
                self._maybe_auto_cleanup()
            except Exception:
                self.logger.exception("maintenance iteration failed")

    def _fire_triggered_jobs(self) -> None:
        """Launch queued jobs whose trigger parent has reached the status.

        Lightweight DAG: a job started with ``trigger={"job_id": A, "status":
        S}`` stays ``queued`` until A reaches S, then its runner is launched.
        """
        try:
            with self.db_lock:
                rows = self.db.execute(
                    "SELECT job_id, trigger_json FROM jobs WHERE status='queued'"
                ).fetchall()
                parents: dict[str, str] = {}
                triggers: dict[str, tuple[str, str]] = {}
                for row in rows:
                    trigger = json.loads(row["trigger_json"] or "null")
                    if isinstance(trigger, dict) and trigger.get("job_id") and trigger.get("status"):
                        triggers[row["job_id"]] = (trigger["job_id"], trigger["status"])
                        parents.setdefault(trigger["job_id"], None)
                if not triggers:
                    return
                for parent in parents:
                    status_row = self.db.execute(
                        "SELECT status FROM jobs WHERE job_id=?", (parent,)
                    ).fetchone()
                    if status_row:
                        parents[parent] = status_row["status"]
            to_launch = [
                job_id for job_id, (parent, status) in triggers.items()
                if parents.get(parent) == status
            ]
            to_cancel = [
                (job_id, parent, status)
                for job_id, (parent, status) in triggers.items()
                if parents.get(parent) in TERMINAL_STATUSES and parents.get(parent) != status
            ]
            for job_id in to_launch:
                launch = self.prepare_launch(job_id)
                if launch is None:
                    continue
                self._launch_prepared(launch)
            for job_id, parent, status in to_cancel:
                with self.db_lock:
                    changed = self.db.execute(
                        "UPDATE jobs SET status='cancelled', ended_at=?, updated_at=? WHERE job_id=? AND status='queued'",
                        (now_iso(), now_iso(), job_id),
                    ).rowcount
                    self.db.commit()
                if changed:
                    self._emit(
                        job_id,
                        "cancelled",
                        message=f"Trigger parent {parent} reached a different terminal status than {status}",
                        data={"trigger": {"job_id": parent, "status": status}, "parent_status": parents.get(parent)},
                    )
        except Exception:
            self.logger.exception("triggered-job fire failed")

    def _running_count(self) -> int:
        with self.db_lock:
            return self.db.execute("SELECT COUNT(*) FROM jobs WHERE status='running'").fetchone()[0]

    def _maybe_auto_cleanup(self) -> dict[str, Any] | None:
        if self.max_retention_seconds <= 0:
            return
        if self._last_retention_run is not None and time.monotonic() - self._last_retention_run < self.retention_interval_seconds:
            return
        self._last_retention_run = time.monotonic()
        try:
            return self.cleanup(older_than_seconds=self.max_retention_seconds, dry_run=self.retention_dry_run)
        except Exception:
            self.logger.exception("automatic retention cleanup failed")
            return None

    def _dispatch_due_deliveries(self) -> None:
        self._ensure_open()
        with self.db_lock:
            rows = self.db.execute(
                """
                SELECT * FROM deliveries
                WHERE (status IN ('pending', 'retrying') AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                   OR (status='dispatching' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
                """,
                (now_iso(), now_iso()),
            ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                payload = {}
            if payload.get("target", {}).get("auto_dispatch") is False:
                continue
            with self._delivery_threads_lock:
                if len(self._delivery_threads) >= self.max_delivery_concurrency:
                    continue
                thread = threading.Thread(target=self._dispatch_delivery, args=(self._delivery_dict(row),), daemon=True)
                self._delivery_threads.add(thread)
            thread.start()

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self.begin_shutdown()
            self._closed = True
            self.dispatcher_stop.set()
            if self.dispatcher_thread and self.dispatcher_thread is not threading.current_thread():
                self.dispatcher_thread.join(timeout=2)
            deadline = time.monotonic() + float(os.environ.get("VANTH_SHUTDOWN_TIMEOUT", "10"))
            with self._delivery_threads_lock:
                workers = list(self._delivery_threads)
            for thread in workers:
                remaining = max(0, deadline - time.monotonic())
                if remaining:
                    thread.join(timeout=remaining)
            with self.db_lock:
                self.db.close()
            for handler in self.logger.handlers[:]:
                handler.close()
                self.logger.removeHandler(handler)

    @property
    def specs_dir(self) -> Path:
        path = self.home / "specs"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _condition(self, job_id: str) -> threading.Condition:
        self.conditions.setdefault(job_id, threading.Condition())
        return self.conditions[job_id]

    def _row(self, sql: str, args: tuple[Any, ...]) -> sqlite3.Row | None:
        self._ensure_open()
        with self.db_lock:
            return self.db.execute(sql, args).fetchone()

    def _event_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_id": row["event_id"],
            "job_id": row["job_id"],
            "seq": row["seq"],
            "type": row["type"],
            "level": row["level"],
            "message": row["message"],
            "data": json.loads(row["data_json"] or "{}"),
            "source": row["source"],
            "created_at": row["created_at"],
        }

    def _emit(
        self,
        job_id: str,
        event_type: str,
        *,
        message: str | None = None,
        data: dict[str, Any] | None = None,
        level: str = "info",
        source: str = "server",
    ) -> dict[str, Any]:
        self._ensure_open()
        payload = normalize_event_payload({"type": event_type, "message": message, "data": data or {}, "level": level})
        data_json = json.dumps(payload["data"], separators=(",", ":"))
        if len(data_json.encode()) > self.max_event_bytes:
            payload["data"] = {"truncated": True, "max_bytes": self.max_event_bytes}
            payload["message"] = payload["message"] or "Event payload exceeded max bytes"
            payload["level"] = "warning"
            data_json = json.dumps(payload["data"], separators=(",", ":"))
        with self.db_lock:
            for attempt in range(4):
                try:
                    event = self._emit_transactional(
                        job_id, payload, data_json, event_type, level, source, message
                    )
                    break
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower() or attempt == 3:
                        raise
                    self.logger.warning("event write contended, retrying job_id=%s attempt=%s", job_id, attempt + 1)
                    time.sleep(0.05 * (attempt + 1))
            else:  # pragma: no cover - loop always breaks
                raise RuntimeError("event write failed")
        if event is not None and event.get("persisted") is not False:
            self._append_event_mirror(event, job_id)
        with self._condition(job_id):
            self._condition(job_id).notify_all()
        return event

    def _emit_transactional(
        self,
        job_id: str,
        payload: dict[str, Any],
        data_json: str,
        event_type: str,
        level: str,
        source: str,
        message: str | None,
    ) -> dict[str, Any]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if event_type not in TERMINAL_STATUSES:
                count = self.db.execute("SELECT COUNT(*) FROM events WHERE job_id=?", (job_id,)).fetchone()[0]
                if count >= self.max_events_per_job:
                    self.db.rollback()
                    if job_id not in self._events_truncated:
                        self._events_truncated.add(job_id)
                        self.logger.warning("structured event cap reached job_id=%s max_events=%s", job_id, self.max_events_per_job)
                    return {
                        "event_id": None,
                        "job_id": job_id,
                        "seq": count + 1,
                        "type": event_type,
                        "level": "warning",
                        "message": "Structured event cap reached",
                        "data": {"max_events": self.max_events_per_job, "truncated": True},
                        "source": source,
                        "created_at": now_iso(),
                        "persisted": False,
                    }
            row = self.db.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM events WHERE job_id=?", (job_id,)).fetchone()
            seq = int(row["seq"])
            created_at = now_iso()
            event_id = "evt_" + uuid.uuid4().hex[:16]
            self.db.execute(
                """
                INSERT INTO events(event_id, job_id, seq, type, level, message, data_json, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    job_id,
                    seq,
                    payload["type"],
                    payload["level"],
                    payload["message"],
                    data_json,
                    source,
                    created_at,
                ),
            )
            event = {
                "event_id": event_id,
                "job_id": job_id,
                "seq": seq,
                "type": payload["type"],
                "level": payload["level"],
                "message": payload["message"],
                "data": payload["data"],
                "source": source,
                "created_at": created_at,
            }
            self._enqueue_deliveries_uncommitted(event)
            self._persist_metric_series_uncommitted(event)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return event

    def _append_event_mirror(self, event: dict[str, Any], job_id: str) -> None:
        try:
            row = self._row("SELECT events_path FROM jobs WHERE job_id=?", (job_id,))
            if not row:
                return
            with Path(row["events_path"]).open("a", encoding="utf-8") as f:
                f.write(json.dumps(event, separators=(",", ":")) + "\n")
        except OSError:
            self.logger.exception("event mirror write failed job_id=%s event_id=%s", job_id, event.get("event_id"))
        except (sqlite3.Error, RuntimeError):
            self.logger.exception("event mirror path lookup failed job_id=%s", job_id)

    def _enqueue_deliveries(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        deliveries = self._enqueue_deliveries_uncommitted(event)
        self.db.commit()
        return deliveries

    def _enqueue_deliveries_uncommitted(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        deliveries = []
        targets = self.db.execute("SELECT * FROM wake_targets WHERE job_id=?", (event["job_id"],)).fetchall()
        for target in targets:
            events = json.loads(target["events_json"] or "[]")
            if events and event["type"] not in events:
                continue
            delivery_id = "del_" + uuid.uuid4().hex[:16]
            payload = self._delivery_payload(event, target, delivery_id)
            self.db.execute(
                """
                INSERT OR IGNORE INTO deliveries(
                  delivery_id, event_id, target_id, job_id, target_type, status, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    delivery_id,
                    event["event_id"],
                    target["target_id"],
                    event["job_id"],
                    target["type"],
                    json.dumps(payload, separators=(",", ":")),
                    now_iso(),
                ),
            )
            row = self._row("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,))
            if row:
                deliveries.append(self._delivery_dict(row))
        return deliveries

    def _is_finite_number(self, value: Any) -> bool:
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return value == value and value not in (float("inf"), float("-inf"))
        return False

    def _metric_x(self, data: dict[str, Any], seq: int) -> float:
        step = data.get("_step")
        if self._is_finite_number(step):
            return float(step)
        return float(seq)

    def _persist_metric_series_uncommitted(self, event: dict[str, Any]) -> None:
        """Mirror scalar fields of metric/progress events into metric_series.

        Mirrors the Go monitor's transform: numeric `metric` payload fields
        become series named after the field; `progress` events produce
        ``progress.current`` / ``progress.total`` / ``progress.percent``.
        ``_step`` (when finite numeric) is the x value, otherwise the event
        sequence. Keys prefixed with ``_`` and the ``stage``/``phase`` keys are
        skipped as series names.
        """
        if event.get("persisted") is False or not event.get("event_id"):
            return
        event_type = event["type"]
        data = event["data"]
        if event_type not in ("metric", "progress"):
            return
        stage = data.get("stage") or data.get("phase")
        if not isinstance(stage, str):
            stage = None
        rows: list[tuple[str, str, float, float]] = []
        if event_type == "metric":
            for key, value in data.items():
                if key.startswith("_") or key in ("stage", "phase"):
                    continue
                if not self._is_finite_number(value):
                    continue
                rows.append((key, float(value), float(self._metric_x(data, event["seq"])), stage))
        else:
            current = data.get("current")
            total = data.get("total")
            percent = data.get("percent")
            if self._is_finite_number(current):
                rows.append(("progress.current", float(current), float(self._metric_x(data, event["seq"])), stage))
            if self._is_finite_number(total):
                rows.append(("progress.total", float(total), float(self._metric_x(data, event["seq"])), stage))
            if self._is_finite_number(percent):
                rows.append(("progress.percent", float(percent), float(self._metric_x(data, event["seq"])), stage))
        x = float(self._metric_x(data, event["seq"]))
        for metric, y, seq_value, series_stage in rows:
            series_id = "ser_" + uuid.uuid4().hex[:16]
            self.db.execute(
                """
                INSERT INTO metric_series(series_id, job_id, metric, x, y, stage, event_id, seq, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    series_id,
                    event["job_id"],
                    metric,
                    x,
                    y,
                    series_stage,
                    event["event_id"],
                    event["seq"],
                    event["created_at"],
                ),
            )
        if rows:
            self.logger.debug("persisted %d metric points job_id=%s event_id=%s", len(rows), event["job_id"], event["event_id"])

    def _claim_delivery(self, delivery_id: str) -> dict[str, Any] | None:
        with self.db_lock:
            self.db.execute("BEGIN IMMEDIATE")
            now = now_iso()
            row = self.db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
            if not row:
                self.db.rollback()
                return None
            target = json.loads(row["payload_json"] or "{}").get("target", {})
            timeout = int(target.get("timeout_seconds", 30))
            lease_seconds = timeout + max(1, self.delivery_lease_margin)
            due = (
                row["status"] in {"pending", "retrying"}
                and (row["next_attempt_at"] is None or row["next_attempt_at"] <= now)
            ) or (row["status"] == "dispatching" and row["lease_expires_at"] and row["lease_expires_at"] <= now)
            if not due:
                self.db.rollback()
                return None
            token = secrets.token_urlsafe(24)
            lease_expires = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat().replace("+00:00", "Z")
            attempt = int(row["attempts"]) + 1
            reclaimed = int(row["status"] == "dispatching")
            changed = self.db.execute(
                """
                UPDATE deliveries SET status='dispatching', attempts=?, claim_token=?, claimed_at=?, lease_expires_at=?
                WHERE delivery_id=? AND (status IN ('pending','retrying') OR (status='dispatching' AND lease_expires_at<=?))
                """,
                (attempt, token, now, lease_expires, delivery_id, now),
            ).rowcount
            if not changed:
                self.db.rollback()
                return None
            if reclaimed:
                self.db.execute(
                    "UPDATE delivery_attempts SET status='reclaimed', ended_at=?, error=? WHERE delivery_id=? AND ended_at IS NULL",
                    (now, "delivery lease expired", delivery_id),
                )
            self.db.execute(
                """
                INSERT INTO delivery_attempts(attempt_id, delivery_id, attempt, claim_token, target_type,
                  started_at, status, reclaimed, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'dispatching', ?, ?)
                """,
                (
                    "att_" + uuid.uuid4().hex[:16], delivery_id, attempt, token,
                    row["target_type"], now, reclaimed, now,
                ),
            )
            self.db.commit()
            claimed = self.db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
        return self._delivery_dict(claimed)

    def _dispatch_delivery(self, delivery: dict[str, Any]) -> None:
        try:
            target = delivery["payload"].get("target", {})
            command = target.get("command")
            target_type = target.get("type")
            if not command and target_type in {"codex_thread", "opencode_thread"} and target.get("auto_dispatch") is False:
                return
            if not command and target_type not in {"codex_thread", "opencode_thread"}:
                return
            delivery = self._claim_delivery(delivery["delivery_id"])
            if not delivery:
                return
            payload = delivery["payload"]
            if not command and target_type == "codex_thread":
                try:
                    send_delivery_to_codex(payload)
                    self._complete_delivery(delivery, "delivered")
                except Exception as exc:
                    self._complete_delivery(delivery, "failed", str(exc))
                return
            if not command and target_type == "opencode_thread":
                try:
                    send_delivery_to_opencode(payload)
                    self._complete_delivery(delivery, "delivered")
                except OpenCodeSessionNotFound as exc:
                    effective_delivery = {
                        **delivery,
                        "payload": {
                            **delivery["payload"],
                            "target": {
                                **delivery["payload"].get("target", {}),
                                "max_attempts": 1,
                            },
                        },
                    }
                    self._complete_delivery(effective_delivery, "failed", str(exc))
                except Exception as exc:
                    self._complete_delivery(delivery, "failed", str(exc))
                return
            try:
                proc = subprocess.run(
                    command,
                    input=json.dumps(payload),
                    text=True,
                    shell=isinstance(command, str),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    timeout=int(target.get("timeout_seconds", 30)),
                )
                if proc.returncode == 0:
                    self._complete_delivery(delivery, "delivered")
                else:
                    self._complete_delivery(delivery, "failed", (proc.stderr or "").strip())
            except Exception as exc:
                self._complete_delivery(delivery, "failed", str(exc))
        finally:
            with self._delivery_threads_lock:
                self._delivery_threads.discard(threading.current_thread())

    def _complete_delivery(self, delivery: dict[str, Any], status: str, error: str | None = None) -> dict[str, Any]:
        if status not in {"delivered", "failed"}:
            raise ValueError("delivery completion status must be delivered or failed")
        target = delivery["payload"].get("target", {})
        attempt = int(delivery["attempts"])
        final_status = status
        next_attempt_at = None
        if status == "failed" and attempt < int(target.get("max_attempts", 1)):
            delay = int(target.get("retry_delay_seconds", 5))
            final_status = "retrying"
            next_attempt_at = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat().replace("+00:00", "Z")
        delivered_at = now_iso() if final_status == "delivered" else None
        error = (error or "")[:DEFAULT_MAX_ERROR_BYTES] or None
        with self.db_lock:
            stamp = now_iso()
            changed = self.db.execute(
                """
                UPDATE deliveries SET status=?, delivered_at=COALESCE(?, delivered_at), last_error=?, next_attempt_at=?,
                  claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL
                WHERE delivery_id=? AND status='dispatching' AND claim_token=?
                """,
                (final_status, delivered_at, error, next_attempt_at, delivery["delivery_id"], delivery["claim_token"]),
            ).rowcount
            if changed:
                self.db.execute(
                    """
                    UPDATE delivery_attempts SET status=?, error=?, ended_at=?
                    WHERE delivery_id=? AND claim_token=? AND ended_at IS NULL
                    """,
                    (final_status, error, stamp, delivery["delivery_id"], delivery["claim_token"]),
                )
            self.db.commit()
            row = self.db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery["delivery_id"],)).fetchone()
        return self._delivery_dict(row)

    def _delivery_payload(self, event: dict[str, Any], target: sqlite3.Row, delivery_id: str) -> dict[str, Any]:
        config = json.loads(target["config_json"] or "{}")
        prompt = config.get("prompt")
        if not prompt:
            prompt = (
                "vanth event\n"
                f"delivery_id: {delivery_id}\n"
                f"job_id: {event['job_id']}\n"
                f"event: {event['type']}\n"
                f"message: {event.get('message') or ''}\n"
                f"data: {json.dumps(event.get('data') or {}, separators=(',', ':'))}\n\n"
                "Continue from this event. Use vanth job_status/job_events/job_tail for details instead of polling."
            )
        return {
            "target": {
                "type": target["type"],
                **config,
            },
            "event": event,
            "prompt": prompt,
            "delivery_id": delivery_id,
        }

    async def start(
        self,
        command: str,
        cwd: str | None = None,
        name: str | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
        notify_on: list[str] | None = None,
        wake_targets: list[dict[str, Any]] | None = None,
        origin_thread_id: str | None = None,
        tags: list[str] | None = None,
        notes: str | None = None,
        interactive: bool = False,
        trigger: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        self._ensure_open()
        validate_wake_targets(wake_targets)
        if notify_on:
            for target in wake_targets or []:
                if "events" not in target and "notify_on" not in target:
                    target["events"] = notify_on
        if not isinstance(command, str) or not command.strip():
            raise ValueError("command must be a non-empty string")
        if timeout_seconds is not None and (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int) or timeout_seconds < 1):
            raise ValueError("timeout_seconds must be an integer >= 1")
        trigger = self._validate_trigger(trigger)
        queued = trigger is not None
        if self.max_running_jobs and not queued and self._running_count() >= self.max_running_jobs:
            raise ValueError(f"concurrent job quota reached ({self.max_running_jobs} running jobs)")
        job_id = "job_" + uuid.uuid4().hex[:12]
        stdout_path = self.logs / f"{job_id}.stdout.log"
        stderr_path = self.logs / f"{job_id}.stderr.log"
        events_path = self.events_dir / f"{job_id}.jsonl"
        created_at = now_iso()
        wake_thread_id = next(
            (
                target.get("thread_id") or target.get("threadId")
                for target in (wake_targets or [])
                if target.get("type") in {"codex_thread", "opencode_thread"}
            ),
            None,
        )
        origin_thread_id = origin_thread_id or os.environ.get("CODEX_THREAD_ID")
        spec_path = self._write_spec(
            job_id,
            {
                "command": command,
                "cwd": cwd,
                "env": env or {},
                "timeout_seconds": timeout_seconds,
                "max_log_bytes": self.max_log_bytes,
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "interactive": interactive,
            },
        )
        run_info = capture_run_metadata(cwd=cwd, notes=notes)
        run_payload = {**run_info, "interactive": interactive}
        with self.db_lock:
            self.db.execute(
                """
                INSERT INTO jobs(job_id, name, command, cwd, status, created_at, updated_at, started_at, runner_heartbeat_at,
                  timeout_seconds, notify_on, origin_thread_id, wake_thread_id, tags_json, env_json, notes, run_json,
                  stdout_path, stderr_path, events_path, trigger_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    name,
                    command,
                    cwd,
                    "queued" if queued else "running",
                    created_at,
                    created_at,
                    None if queued else created_at,
                    None if queued else created_at,
                    timeout_seconds,
                    json.dumps(notify_on or []),
                    origin_thread_id,
                    wake_thread_id,
                    json.dumps(tags or [], separators=(",", ":")),
                    json.dumps(env or {}, separators=(",", ":")),
                    notes,
                    serialize_run_metadata(run_payload),
                    str(stdout_path),
                    str(stderr_path),
                    str(events_path),
                    json.dumps(trigger, separators=(",", ":")) if trigger else None,
                ),
            )
            self.db.commit()
            self._insert_wake_targets(job_id, wake_targets or [], created_at)
            self.db.commit()
        if queued:
            return {
                "job_id": job_id,
                "status": "queued",
                "trigger": trigger,
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "events_path": str(events_path),
                "message": f"Job queued; will start when {trigger['job_id']} reaches {trigger['status']}",
            }
        return self._launch(job_id, stdout_path, stderr_path, events_path, spec_path)

    def _validate_trigger(self, trigger: dict[str, str] | None) -> dict[str, str] | None:
        if trigger is None:
            return None
        if not isinstance(trigger, dict):
            raise ValueError("trigger must be an object with job_id and status")
        job_id = trigger.get("job_id")
        status = trigger.get("status")
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("trigger.job_id must be a non-empty string")
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"trigger.status must be one of {sorted(TERMINAL_STATUSES)}")
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown trigger job_id: {job_id}")
        return {"job_id": job_id, "status": status}

    def _write_spec(self, job_id: str, spec: dict[str, Any]) -> Path:
        """Write a job's run spec JSON and return its path.

        Shared by ``start`` and the remote dispatcher so both local and remote
        launches reuse one serialization path.
        """
        path = self.specs_dir / f"{job_id}.json"
        path.write_text(
            json.dumps(spec, separators=(",", ":")),
            encoding="utf-8",
        )
        return path

    def _launch(
        self,
        job_id: str,
        stdout_path: Path,
        stderr_path: Path,
        events_path: Path,
        spec_path: Path,
    ) -> dict[str, Any]:
        creationflags = 0
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0)
        runner_log = (self.logs / f"{job_id}.runner.log").open("ab")
        try:
            try:
                proc = subprocess.Popen(
                    [sys.executable, "-m", "vanth.runner", str(self.home), job_id],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=runner_log,
                    creationflags=creationflags,
                    start_new_session=sys.platform != "win32",
                )
            except (SystemExit, KeyboardInterrupt):
                raise
            except Exception as exc:
                self.processes.pop(job_id, None)
                self._transition_terminal(job_id, "failed", 1)
                self._emit(
                    job_id,
                    "failed",
                    message=f"Job runner failed to start: {exc}",
                    data={"error": str(exc)},
                    level="error",
                    source="server",
                )
                Path(spec_path).unlink(missing_ok=True)
                return {
                    "job_id": job_id,
                    "status": "failed",
                    "exit_code": 1,
                    "message": f"Job runner failed to start: {exc}",
                    "worker_pid": None,
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                    "events_path": str(events_path),
                }
        finally:
            runner_log.close()
        self.processes[job_id] = proc
        threading.Thread(target=self._watch_runner, args=(job_id, proc), daemon=True).start()
        with self.db_lock:
            self.db.execute(
                "UPDATE jobs SET status='running', started_at=?, runner_heartbeat_at=?, worker_pid=?, updated_at=? WHERE job_id=?",
                (now_iso(), now_iso(), proc.pid, now_iso(), job_id),
            )
            self.db.commit()
        return {
            "job_id": job_id,
            "status": "running",
            "worker_pid": proc.pid,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "events_path": str(events_path),
            "message": "Job started",
        }

    def prepare_launch(self, job_id: str) -> dict[str, Any]:
        """Prepare a job already inserted in a runnable state for launch.

        Writes the spec JSON (command/cwd/env/timeout/etc. from the row) and
        returns a launch dict. This is the shared explicit operation used by the
        local dispatcher and the remote dispatcher so a remote launch never
        calls ``JobManager.start`` directly. Returns ``None`` when the job is
        unknown or not launchable.
        """
        self._ensure_open()
        row = self._row(
            "SELECT job_id, command, cwd, env_json, timeout_seconds, run_json FROM jobs WHERE job_id=?",
            (job_id,),
        )
        if not row:
            return None
        interactive = json.loads(row["run_json"] or "{}").get("interactive") is True
        spec = {
            "command": row["command"],
            "cwd": row["cwd"],
            "env": json.loads(row["env_json"] or "{}"),
            "timeout_seconds": row["timeout_seconds"],
            "max_log_bytes": self.max_log_bytes,
            "stdout_path": str(self.logs / f"{job_id}.stdout.log"),
            "stderr_path": str(self.logs / f"{job_id}.stderr.log"),
            "interactive": interactive,
        }
        spec_path = self._write_spec(job_id, spec)
        return {
            "job_id": job_id,
            "stdout_path": Path(spec["stdout_path"]),
            "stderr_path": Path(spec["stderr_path"]),
            "events_path": self.events_dir / f"{job_id}.jsonl",
            "spec_path": spec_path,
        }

    def _launch_prepared(self, launch: dict[str, Any]) -> dict[str, Any]:
        """Launch a job prepared by :meth:`prepare_launch` (wraps ``_launch``).

        Shared by ``_fire_triggered_jobs`` and the remote dispatcher. Keeps the
        existing ``_launch`` behavior byte-for-byte for local jobs.
        """
        return self._launch(
            launch["job_id"],
            launch["stdout_path"],
            launch["stderr_path"],
            launch["events_path"],
            launch["spec_path"],
        )

    async def rerun(self, job_id: str, **overrides: Any) -> dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(
            None, lambda: self.rerun_sync(job_id, **overrides)
        )

    def rerun_sync(
        self,
        job_id: str,
        *,
        command: str | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
        name: str | None = None,
        tags: list[str] | None = None,
        notes: str | None = None,
        cwd: str | None = None,
        interactive: bool | None = None,
    ) -> dict[str, Any]:
        self._ensure_open()
        row = self._row(
            "SELECT job_id, command, cwd, env_json, timeout_seconds, notify_on, origin_thread_id, wake_thread_id, "
            "tags_json, name, notes, run_json FROM jobs WHERE job_id=?",
            (job_id,),
        )
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        targets = []
        for target in self.db.execute("SELECT * FROM wake_targets WHERE job_id=?", (job_id,)).fetchall():
            config = json.loads(target["config_json"] or "{}")
            targets.append({
                "type": target["type"],
                "events": json.loads(target["events_json"] or "[]"),
                **config,
            })
        stored_tags = json.loads(row["tags_json"] or "[]")
        stored_notify_on = json.loads(row["notify_on"] or "[]")
        stored_interactive = json.loads(row["run_json"] or "{}").get("interactive") is True
        stored_env = json.loads(row["env_json"] or "{}")
        merged_env = stored_env
        if env is not None:
            if not isinstance(env, dict):
                raise ValueError("env must be an object of string values")
            merged_env = {**stored_env, **env}
        return asyncio.run(self.start(
            command=command if command is not None else row["command"],
            cwd=cwd if cwd is not None else row["cwd"],
            name=name if name is not None else row["name"],
            env=merged_env,
            timeout_seconds=timeout_seconds if timeout_seconds is not None else row["timeout_seconds"],
            notify_on=stored_notify_on or None,
            wake_targets=targets or None,
            origin_thread_id=row["origin_thread_id"],
            tags=stored_tags if tags is None else tags,
            notes=notes if notes is not None else row["notes"],
            interactive=stored_interactive if not isinstance(interactive, bool) else interactive,
        ))

    def _watch_runner(self, job_id: str, proc: subprocess.Popen[bytes]) -> None:
        proc.wait()
        try:
            row = self._row("SELECT status, pid, stop_requested_at FROM jobs WHERE job_id=?", (job_id,))
            if not row or row["status"] != "running":
                return
            if row["pid"] and not self._terminate_pid(int(row["pid"]), force=True, deadline=time.monotonic() + self.recovery_kill_timeout):
                self.logger.error("runner watcher could not terminate workload job_id=%s pid=%s", job_id, row["pid"])
                return
            terminal = "cancelled" if row["stop_requested_at"] else "orphaned"
            if self._transition_terminal(job_id, terminal):
                self._emit(job_id, terminal, message="Job runner exited before recording a terminal status")
        except (sqlite3.Error, RuntimeError):
            return

    def _insert_wake_targets(self, job_id: str, targets: list[dict[str, Any]], created_at: str) -> list[str]:
        inserted = []
        for target in targets:
            target_type = target.get("type")
            events = target.get("events") or target.get("notify_on") or []
            if not isinstance(target_type, str):
                continue
            config = {key: value for key, value in target.items() if key not in {"type", "events", "notify_on"}}
            target_id = "target_" + uuid.uuid4().hex[:12]
            self.db.execute(
                """
                INSERT INTO wake_targets(target_id, job_id, type, events_json, config_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    target_id,
                    job_id,
                    target_type,
                    json.dumps(events, separators=(",", ":")),
                    json.dumps(config, separators=(",", ":")),
                    created_at,
                ),
            )
            inserted.append(target_id)
        return inserted

    def _read_stream(
        self,
        job_id: str,
        stream,
        path: Path,
        source: str,
    ) -> None:
        if stream is None:
            return
        max_bytes = self.max_log_bytes
        written = path.stat().st_size if path.exists() else 0
        with path.open("ab") as f:
            while line := stream.readline(self.max_event_line_bytes + 1):
                if written < max_bytes:
                    chunk = line[: max_bytes - written]
                    f.write(chunk)
                    f.flush()
                    written += len(chunk)
                if written >= max_bytes and (job_id, source) not in self._log_truncated:
                    self._log_truncated.add((job_id, source))
                    self._emit_safely(
                        job_id,
                        "log_truncated",
                        message=f"{source} log reached its configured byte cap",
                        data={"stream": source, "max_bytes": max_bytes},
                        level="warning",
                        source="server",
                    )
                if len(line) > self.max_event_line_bytes:
                    while line and not line.endswith(b"\n"):
                        line = stream.readline(self.max_event_line_bytes + 1)
                    self._emit_safely(
                        job_id,
                        "event_rejected",
                        message="AGENT_EVENT line exceeded the configured byte limit",
                        data={"max_bytes": self.max_event_line_bytes},
                        level="warning",
                        source=source,
                    )
                    continue
                try:
                    payload = parse_agent_event_line(line.decode(errors="replace").rstrip("\r\n"))
                except Exception:
                    payload = None
                if payload:
                    event = normalize_event_payload(payload)
                    self._emit_safely(
                        job_id,
                        event["type"],
                        message=event["message"],
                        data=event["data"],
                        level=event["level"],
                        source=source,
                    )

    def _emit_safely(
        self,
        job_id: str,
        event_type: str,
        *,
        message: str | None = None,
        data: dict[str, Any] | None = None,
        level: str = "info",
        source: str = "server",
    ) -> None:
        try:
            self._emit(job_id, event_type, message=message, data=data, level=level, source=source)
        except (sqlite3.Error, RuntimeError):
            self.logger.exception("structured event persisted failed job_id=%s type=%s", job_id, event_type)

    def _watch(self, job_id: str, proc: subprocess.Popen[bytes], timeout_seconds: int | None) -> None:
        try:
            exit_code = proc.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            self._kill_process(proc, force=True)
            exit_code = proc.wait()
            self._readers_done(job_id)
            self._finish(job_id, "timeout", exit_code)
            return
        status = self._row("SELECT status FROM jobs WHERE job_id=?", (job_id,))["status"]
        if status == "cancelled":
            return
        self._readers_done(job_id)
        self._finish(job_id, "completed" if exit_code == 0 else "failed", exit_code)

    def _readers_done(self, job_id: str) -> None:
        for thread in self.reader_threads.pop(job_id, []):
            thread.join()

    def _kill_process(self, proc: subprocess.Popen[bytes], force: bool) -> None:
        self._kill_pid(proc.pid, force)

    def _kill_pid(self, pid: int, force: bool) -> None:
        if sys.platform == "win32":
            args = ["taskkill", "/PID", str(pid), "/T"]
            if force:
                args.append("/F")
            subprocess.run(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        signal_number = 9 if force else 15
        try:
            os.killpg(pid, signal_number)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal_number)
            except ProcessLookupError:
                return

    def _terminate_pid(self, pid: int, force: bool, deadline: float) -> bool:
        if not pid or not self._pid_alive(pid):
            return True
        self._kill_pid(pid, force=force)
        while self._pid_alive(pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        if self._pid_alive(pid):
            self._kill_pid(pid, force=True)
            force_deadline = min(deadline + 1, time.monotonic() + 1)
            while self._pid_alive(pid) and time.monotonic() < force_deadline:
                time.sleep(0.05)
        return not self._pid_alive(pid)

    def _finish(self, job_id: str, status: str, exit_code: int | None = None) -> None:
        row = self._row("SELECT stop_requested_at FROM jobs WHERE job_id=?", (job_id,))
        if row and row["stop_requested_at"]:
            status = "cancelled"
        if self._transition_terminal(job_id, status, exit_code):
            data = {"exit_code": exit_code} if exit_code is not None else {}
            self._emit(job_id, status, data=data)
        self.processes.pop(job_id, None)

    def _event_query(self, job_id: str, types: list[str] | None, since_event_id: str | None, limit: int,
                     reverse: bool = False) -> list[dict[str, Any]]:
        since_seq = None
        if since_event_id:
            row = self._row("SELECT seq FROM events WHERE job_id=? AND event_id=?", (job_id, since_event_id))
            since_seq = int(row["seq"]) if row else None
        args: list[Any] = [job_id]
        if reverse:
            if since_seq is not None:
                where = "job_id=? AND seq<?"
                args.append(since_seq)
            else:
                where = "job_id=?"
            order = "seq DESC"
        else:
            where = "job_id=? AND seq>?"
            args.append(since_seq if since_seq is not None else 0)
            order = "seq"
        if types:
            where += " AND type IN (%s)" % ",".join("?" for _ in types)
            args.extend(types)
        with self.db_lock:
            rows = self.db.execute(
                f"SELECT * FROM events WHERE {where} ORDER BY {order} LIMIT ?",
                (*args, limit),
            ).fetchall()
        return [self._event_dict(row) for row in rows]

    async def wait(
        self,
        job_id: str,
        filters: list[str],
        since_event_id: str | None = None,
        timeout_seconds: int = 3600,
        return_progress: bool = False,
        metric_ge: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(
            None,
            self.wait_sync,
            job_id,
            filters,
            since_event_id,
            timeout_seconds,
            return_progress,
            metric_ge,
        )

    def wait_sync(
        self,
        job_id: str,
        filters: list[str],
        since_event_id: str | None = None,
        timeout_seconds: int = 3600,
        return_progress: bool = False,
        metric_ge: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        return self._wait(job_id, filters, since_event_id, timeout_seconds, return_progress, metric_ge)

    def _wait(
        self,
        job_id: str,
        filters: list[str],
        since_event_id: str | None = None,
        timeout_seconds: int = 3600,
        return_progress: bool = False,
        metric_ge: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds < 0 or timeout_seconds > 86400:
            raise ValueError("timeout_seconds must be between 0 and 86400")
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        if metric_ge is not None:
            if not isinstance(metric_ge, dict) or not metric_ge:
                raise ValueError("metric_ge must be a non-empty object of metric names to numeric thresholds")
            for metric, threshold in metric_ge.items():
                if not isinstance(metric, str) or not metric:
                    raise ValueError("metric_ge keys must be non-empty metric names")
                if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
                    raise ValueError(f"metric_ge threshold for {metric!r} must be a number")
        deadline = time.monotonic() + timeout_seconds
        while True:
            if self.shutdown_requested.is_set():
                return {"result": "shutdown", "job_id": job_id, "message": "Vanth is shutting down"}
            try:
                events = self._event_query(job_id, filters, since_event_id, 1)
            except RuntimeError:
                return {"result": "shutdown", "job_id": job_id, "message": "Vanth is shutting down"}
            if events:
                return {"result": "event", "job_id": job_id, "status": self.status(job_id)["status"], "event": events[0]}
            if metric_ge:
                try:
                    for metric, threshold in metric_ge.items():
                        value = self._latest_metric_value(job_id, metric)
                        if value is not None and value >= threshold:
                            return {
                                "result": "metric",
                                "job_id": job_id,
                                "status": self.status(job_id)["status"],
                                "metric": metric,
                                "threshold": threshold,
                                "value": value,
                                "event": self._latest_metric_event(job_id, metric),
                            }
                except RuntimeError:
                    pass
            if return_progress and "progress" not in filters:
                try:
                    progress = self._event_query(job_id, ["progress"], since_event_id, 1)
                except RuntimeError:
                    progress = []
                if progress:
                    event = progress[0]
                    return {
                        "result": "progress",
                        "job_id": job_id,
                        "event": event,
                        "status": self.status(job_id)["status"],
                        "progress": event.get("data"),
                    }
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {"result": "timeout", "job_id": job_id, "status": self.status(job_id)["status"], "message": "No matching event before timeout"}
            with self._condition(job_id):
                if not self._condition(job_id).wait(timeout=min(0.1, remaining)):
                    if remaining <= 0:
                        return {"result": "timeout", "job_id": job_id, "status": self.status(job_id)["status"], "message": "No matching event before timeout"}
                    continue
                else:
                    continue

    def status(self, job_id: str) -> dict[str, Any]:
        row = self._row("SELECT * FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        with self.db_lock:
            last = self.db.execute("SELECT * FROM events WHERE job_id=? ORDER BY seq DESC LIMIT 1", (job_id,)).fetchone()
            progress = self.db.execute(
                "SELECT * FROM events WHERE job_id=? AND type='progress' ORDER BY seq DESC LIMIT 1",
                (job_id,),
            ).fetchone()
        result = {
            "job_id": job_id,
            "status": row["status"],
            "command": row["command"],
            "cwd": row["cwd"],
            "timeout_seconds": row["timeout_seconds"],
            "pid": row["pid"],
            "worker_pid": row["worker_pid"],
            "name": row["name"],
            "origin_thread_id": row["origin_thread_id"],
            "wake_thread_id": row["wake_thread_id"],
            "tags": json.loads(row["tags_json"] or "[]"),
            "env": json.loads(row["env_json"] or "{}"),
            "notes": row["notes"],
            "run": json.loads(row["run_json"] or "{}"),
            "trigger": json.loads(row["trigger_json"] or "null"),
            "runtime_seconds": _runtime_seconds(row["started_at"], row["ended_at"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "exit_code": row["exit_code"],
            "last_event": self._event_dict(last) if last else None,
        }
        result["progress"] = ({**json.loads(progress["data_json"] or "{}"), "updated_at": progress["created_at"]} if progress else None)
        return result

    def status_batch(self, job_ids: list[str], limit: int = 500) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(limit, "limit", 1000)
        if not isinstance(job_ids, list) or not job_ids:
            raise ValueError("job_ids must be a non-empty list of job ids")
        if any(isinstance(job_id, bool) or not isinstance(job_id, str) for job_id in job_ids):
            raise ValueError("job_ids must be a list of strings")
        if len(job_ids) > limit:
            raise ValueError(f"job_ids must contain at most {limit} ids")
        jobs = []
        unknown = []
        for job_id in job_ids:
            try:
                jobs.append(self.status(job_id))
            except ValueError:
                unknown.append(job_id)
                jobs.append({"job_id": job_id, "status": "unknown", "error": "Unknown job_id"})
        return {"jobs": jobs, "count": len(jobs), "unknown": unknown}

    def list(self, status: list[str] | None = None, limit: int = 50, thread_id: str | None = None,
             name: str | None = None, tags: list[str] | None = None) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(limit, "limit")
        args: list[Any] = []
        filters = []
        if status:
            filters.append("status IN (%s)" % ",".join("?" for _ in status))
            args.extend(status)
        if thread_id:
            filters.append("(origin_thread_id=? OR wake_thread_id=?)")
            args.extend([thread_id, thread_id])
        if name:
            filters.append("name LIKE ?")
            args.append(f"%{name}%")
        for tag in tags or []:
            filters.append("tags_json LIKE ?")
            args.append(f'%"{tag}"%')
        where = "WHERE " + " AND ".join(filters) if filters else ""
        with self.db_lock:
            rows = self.db.execute(
                f"""
                SELECT job_id, name, status, updated_at, origin_thread_id, wake_thread_id, tags_json
                FROM jobs {where} ORDER BY updated_at DESC LIMIT ?
                """,
                (*args, limit),
            ).fetchall()
        jobs = []
        for row in rows:
            item = dict(row)
            item["tags"] = json.loads(item.pop("tags_json") or "[]")
            jobs.append(item)
        return {"jobs": jobs}

    def events(self, job_id: str, since_event_id: str | None = None, types: list[str] | None = None, limit: int = 20,
               reverse: bool = False) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(limit, "limit")
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        return {"events": self._event_query(job_id, types, since_event_id, limit, reverse=reverse)}

    def deliveries(self, job_id: str | None = None, status: str | None = None, limit: int = 20) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(limit, "limit")
        where = []
        args: list[Any] = []
        if job_id:
            where.append("job_id=?")
            args.append(job_id)
        if status:
            where.append("status=?")
            args.append(status)
        sql_where = "WHERE " + " AND ".join(where) if where else ""
        with self.db_lock:
            rows = self.db.execute(
                f"SELECT * FROM deliveries {sql_where} ORDER BY created_at DESC LIMIT ?",
                (*args, limit),
            ).fetchall()
        return {"deliveries": [self._delivery_dict(row) for row in rows]}

    def metrics_query(self, job_id: str, metric: str | None = None, from_ms: int | None = None,
                      to_ms: int | None = None, limit: int = 1000) -> dict[str, Any]:
        """Return stored scalar series for one job.

        Series come from ``metric``/``progress`` AGENT_EVENT payloads mirrored
        into ``metric_series``. ``metric`` filters to one name (e.g.
        ``loss`` or ``progress.percent``); without it all metrics for the job
        are returned grouped by name. ``from_ms``/``to_ms`` filter by event
        timestamp (milliseconds since epoch). Points are ordered by seq.
        """
        self._ensure_open()
        validate_limit(limit, "limit", 10000)
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        where = ["job_id=?"]
        args: list[Any] = [job_id]
        if metric:
            where.append("metric=?")
            args.append(metric)
        if from_ms is not None:
            where.append("created_at >= ?")
            args.append(_ms_to_iso(from_ms))
        if to_ms is not None:
            where.append("created_at <= ?")
            args.append(_ms_to_iso(to_ms))
        with self.db_lock:
            rows = self.db.execute(
                f"""
                SELECT metric, x, y, stage, event_id, seq, created_at
                FROM metric_series WHERE {' AND '.join(where)}
                ORDER BY metric, seq ASC LIMIT ?
                """,
                (*args, limit),
            ).fetchall()
        series: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            point = {
                "x": row["x"],
                "y": row["y"],
                "stage": row["stage"],
                "event_id": row["event_id"],
                "seq": row["seq"],
                "at": row["created_at"],
            }
            series.setdefault(row["metric"], []).append(point)
        return {"job_id": job_id, "series": series, "metrics": list(series.keys())}

    def metric_compare(self, job_ids: list[str], metric: str, aggregation: str = "latest",
                       from_ms: int | None = None, to_ms: int | None = None) -> dict[str, Any]:
        """Compare one metric across jobs.

        ``aggregation`` is one of ``latest`` (last point), ``mean``, ``min``,
        ``max``, ``sum``, or ``count``. Returns per-job summary values plus the
        raw series points so agents can reason about the comparison.
        """
        self._ensure_open()
        valid_aggs = {"latest", "mean", "min", "max", "sum", "count"}
        if aggregation not in valid_aggs:
            raise ValueError(f"aggregation must be one of {sorted(valid_aggs)}")
        if not isinstance(job_ids, list) or not job_ids or len(job_ids) > 50:
            raise ValueError("job_ids must be a non-empty list of at most 50 ids")
        if not isinstance(metric, str) or not metric:
            raise ValueError("metric must be a non-empty string")
        for job_id in job_ids:
            if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
                raise ValueError(f"Unknown job_id: {job_id}")
        result: dict[str, Any] = {"metric": metric, "aggregation": aggregation, "jobs": {}}
        for job_id in job_ids:
            points = self.metrics_query(job_id, metric, from_ms, to_ms, limit=10000)["series"].get(metric, [])
            values = [p["y"] for p in points]
            summary: Any = None
            if values:
                if aggregation == "latest":
                    summary = values[-1]
                elif aggregation == "mean":
                    summary = sum(values) / len(values)
                elif aggregation == "min":
                    summary = min(values)
                elif aggregation == "max":
                    summary = max(values)
                elif aggregation == "sum":
                    summary = sum(values)
                elif aggregation == "count":
                    summary = len(values)
            result["jobs"][job_id] = {
                "value": summary,
                "points": len(points),
                "first": points[0] if points else None,
                "last": points[-1] if points else None,
            }
        return result

    def _latest_metric_value(self, job_id: str, metric: str) -> float | None:
        """Return the latest stored value for one job+metric, or None."""
        with self.db_lock:
            row = self.db.execute(
                "SELECT y FROM metric_series WHERE job_id=? AND metric=? ORDER BY seq DESC LIMIT 1",
                (job_id, metric),
            ).fetchone()
        return float(row["y"]) if row else None

    def _latest_metric_event(self, job_id: str, metric: str) -> dict[str, Any] | None:
        """Return the event that produced the latest point for one job+metric."""
        with self.db_lock:
            row = self.db.execute(
                """
                SELECT e.* FROM events e
                JOIN metric_series m ON m.event_id = e.event_id
                WHERE e.job_id=? AND m.metric=?
                ORDER BY m.seq DESC LIMIT 1
                """,
                (job_id, metric),
            ).fetchone()
        return self._event_dict(row) if row else None

    def metric_ingest(self, job_id: str, metrics: list[dict[str, Any]], idempotency_key: str | None = None) -> dict[str, Any]:
        """Record scalar metric points for a job programmatically.

        Each point is ``{"name": str, "value": float, "ts_ms": int|None,
        "labels": dict|None}``. Points are mirrored into the same
        ``metric_series`` pipeline used by ``metric`` AGENT_EVENT payloads, so
        ``metrics_query``/``metric_compare`` see them immediately. A repeated
        ``idempotency_key`` is ignored.
        """
        self._ensure_open()
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        if not isinstance(metrics, list) or not metrics:
            raise ValueError("metrics must be a non-empty list")
        if len(metrics) > 1000:
            raise ValueError("metrics must contain at most 1000 points")
        if idempotency_key is not None and not isinstance(idempotency_key, str):
            raise ValueError("idempotency_key must be a string")
        points: list[tuple[str, float, int | None, dict[str, Any]]] = []
        for point in metrics:
            if not isinstance(point, dict):
                raise ValueError("each metric point must be an object")
            name = point.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError("each metric point must have a non-empty string name")
            value = point.get("value")
            if not self._is_finite_number(value):
                raise ValueError(f"metric {name!r} value must be a finite number")
            ts_ms = point.get("ts_ms")
            if ts_ms is not None and (isinstance(ts_ms, bool) or not isinstance(ts_ms, int)):
                raise ValueError("ts_ms must be an integer epoch milliseconds")
            labels = point.get("labels")
            if labels is not None:
                if not isinstance(labels, dict) or not all(
                    isinstance(key, str) and isinstance(label_value, (str, int, float))
                    for key, label_value in labels.items()
                ):
                    raise ValueError(f"metric {name!r} labels must be a dict of scalar values")
            points.append((name, float(value), ts_ms, labels or {}))
        if idempotency_key is not None:
            if idempotency_key in self._metric_ingest_keys:
                return {
                    "result": "ok",
                    "job_id": job_id,
                    "ingested": 0,
                    "event_id": None,
                    "deduplicated": True,
                }
            self._metric_ingest_keys.add(idempotency_key)
        event_id: str | None = None
        for name, value, ts_ms, labels in points:
            data: dict[str, Any] = {name: value}
            if ts_ms is not None:
                data["_step"] = ts_ms
            data.update(labels)
            event = self._emit(job_id, "metric", data=data, message=f"metric ingest {name}", source="server")
            event_id = event.get("event_id")
            if not event_id:
                if idempotency_key is not None:
                    self._metric_ingest_keys.discard(idempotency_key)
                raise RuntimeError("metric ingest event was not persisted")
        return {"result": "ok", "job_id": job_id, "ingested": len(points), "event_id": event_id}

    def run_summary(self, job_id: str) -> dict[str, Any]:
        """One-call summary of a job: status, runtime, progress, top metrics.

        Computes the latest value of every stored metric series plus the last
        progress event, so an agent can answer "did it work?" in a single call.
        """
        status = self.status(job_id)
        series = self.metrics_query(job_id, limit=10000)["series"]
        latest_metrics = {metric: points[-1]["y"] for metric, points in series.items() if points}
        # Build per-metric latest + a compact metric overview.
        overview = []
        for metric, points in sorted(series.items()):
            if not points:
                continue
            overview.append(
                {
                    "metric": metric,
                    "latest": points[-1]["y"],
                    "first": points[0]["y"],
                    "min": min(p["y"] for p in points),
                    "max": max(p["y"] for p in points),
                    "count": len(points),
                    "stage": points[-1].get("stage"),
                }
            )
        artifacts = self.artifacts(job_id)["artifacts"]
        return {
            "job_id": job_id,
            "status": status["status"],
            "name": status.get("name"),
            "runtime_seconds": status.get("runtime_seconds"),
            "exit_code": status.get("exit_code"),
            "progress": status.get("progress"),
            "notes": status.get("notes"),
            "metrics": overview,
            "latest_metrics": latest_metrics,
            "artifacts": artifacts,
        }

    def diff_spec(self, base_job_id: str, other_job_id: str) -> dict[str, Any]:
        """Diff the run specs (command/env/cwd/timeout/etc) of two jobs."""
        self._ensure_open()
        base = self._row("SELECT * FROM jobs WHERE job_id=?", (base_job_id,))
        other = self._row("SELECT * FROM jobs WHERE job_id=?", (other_job_id,))
        if not base:
            raise ValueError(f"Unknown job_id: {base_job_id}")
        if not other:
            raise ValueError(f"Unknown job_id: {other_job_id}")

        fields = ["command", "cwd", "timeout_seconds", "name", "notes", "interactive"]
        changes: list[dict[str, Any]] = []
        for field in fields:
            if field == "interactive":
                base_value = json.loads(base["run_json"] or "{}").get("interactive") is True
                other_value = json.loads(other["run_json"] or "{}").get("interactive") is True
            else:
                base_value = base[field]
                other_value = other[field]
            if base_value != other_value:
                changes.append({
                    "field": field,
                    "base": base_value,
                    "other": other_value,
                })

        base_env = json.loads(base["env_json"] or "{}")
        other_env = json.loads(other["env_json"] or "{}")
        env_changes: list[dict[str, Any]] = []
        for key in sorted(set(base_env) | set(other_env)):
            if base_env.get(key) != other_env.get(key):
                env_changes.append({"key": key, "base": base_env.get(key), "other": other_env.get(key)})
        if env_changes:
            changes.append({"field": "env", "changes": env_changes})

        base_tags = json.loads(base["tags_json"] or "[]")
        other_tags = json.loads(other["tags_json"] or "[]")
        if sorted(base_tags) != sorted(other_tags):
            changes.append({"field": "tags", "base": base_tags, "other": other_tags})

        base_targets = self._wake_targets_for_job(base_job_id)
        other_targets = self._wake_targets_for_job(other_job_id)
        if base_targets != other_targets:
            changes.append({"field": "wake_targets", "base": base_targets, "other": other_targets})

        return {
            "base_job_id": base_job_id,
            "other_job_id": other_job_id,
            "identical": not changes,
            "changes": changes,
        }

    def _wake_targets_for_job(self, job_id: str) -> list[dict[str, Any]]:
        with self.db_lock:
            rows = self.db.execute(
                "SELECT type, events_json, config_json FROM wake_targets WHERE job_id=? ORDER BY created_at ASC",
                (job_id,),
            ).fetchall()
        return [
            {
                "type": row["type"],
                "events": json.loads(row["events_json"] or "[]"),
                **json.loads(row["config_json"] or "{}"),
            }
            for row in rows
        ]


    def artifacts(self, job_id: str, limit: int = 50) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(limit, "limit", 1000)
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        with self.db_lock:
            rows = self.db.execute(
                "SELECT * FROM artifacts WHERE job_id=? ORDER BY created_at ASC LIMIT ?",
                (job_id, limit),
            ).fetchall()
        return {
            "artifacts": [
                {
                    "artifact_id": row["artifact_id"],
                    "job_id": row["job_id"],
                    "name": row["name"],
                    "uri": row["uri"],
                    "kind": row["kind"],
                    "size_bytes": row["size_bytes"],
                    "sha256": row["sha256"],
                    "meta": json.loads(row["meta_json"] or "{}"),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
        }

    def artifact_add(self, job_id: str, name: str, uri: str, kind: str | None = None,
                     size_bytes: int | None = None, sha256: str | None = None,
                     meta: dict[str, Any] | None = None) -> dict[str, Any]:
        self._ensure_open()
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty string")
        if not isinstance(uri, str) or not uri:
            raise ValueError("uri must be a non-empty string")
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        artifact_id = "art_" + uuid.uuid4().hex[:16]
        created_at = now_iso()
        with self.db_lock:
            self.db.execute(
                """
                INSERT INTO artifacts(artifact_id, job_id, name, uri, kind, size_bytes, sha256, meta_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (artifact_id, job_id, name, uri, kind, size_bytes, sha256,
                 json.dumps(meta or {}, separators=(",", ":")), created_at),
            )
            self.db.commit()
        return {"artifact_id": artifact_id, "job_id": job_id, "name": name, "uri": uri,
                "kind": kind, "size_bytes": size_bytes, "sha256": sha256,
                "meta": meta or {}, "created_at": created_at}

    def artifact_read(self, artifact_id: str, max_bytes: int = 262144) -> dict[str, Any]:
        """Read the content of an artifact (file://, local path, or http(s)://).

        Returns base64-encoded content so binary and JSON artifacts round-trip
        cleanly through the MCP JSON transport. ``truncated`` is set when the
        artifact exceeds ``max_bytes``.

        HTTP(S) retrieval is disabled by default and must be opted into via
        ``VANTH_ALLOW_HTTP_ARTIFACT_READ=1``. This path is legacy and is never
        used by managed artifacts (see ``artifacts/``).
        """
        self._ensure_open()
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 256 or max_bytes > 1024 * 1024:
            raise ValueError("max_bytes must be an integer between 256 and 1048576")
        row = self._row(
            "SELECT artifact_id, job_id, name, uri, kind, size_bytes, sha256 FROM artifacts WHERE artifact_id=?",
            (artifact_id,),
        )
        if not row:
            raise ValueError(f"artifact content unavailable: unknown artifact_id: {artifact_id}")
        uri = row["uri"]
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme in {"http", "https"}:
            if os.environ.get("VANTH_ALLOW_HTTP_ARTIFACT_READ") != "1":
                raise ValueError(
                    "artifact content unavailable: http(s) retrieval is disabled; "
                    "set VANTH_ALLOW_HTTP_ARTIFACT_READ=1 to enable legacy retrieval"
                )
            try:
                with urllib.request.urlopen(uri, timeout=5) as response:
                    content = response.read(max_bytes + 1)
            except Exception as exc:
                raise ValueError(f"artifact content unavailable: {exc}") from None
        elif parsed.scheme in {"", "file"} or (len(parsed.scheme) == 1 and parsed.scheme.isalpha()):
            path = Path(uri)
            if parsed.scheme == "file":
                path = Path(urllib.request.url2pathname(parsed.path))
            if not path.is_absolute():
                path = Path(self.home) / path
            if not path.exists() or not path.is_file():
                raise ValueError(f"artifact content unavailable: {uri}")
            with path.open("rb") as handle:
                content = handle.read(max_bytes + 1)
        else:
            raise ValueError(f"artifact content unavailable: unsupported scheme {parsed.scheme!r}")
        truncated = len(content) > max_bytes
        content = content[:max_bytes]
        return {
            "artifact_id": row["artifact_id"],
            "name": row["name"],
            "kind": row["kind"],
            "uri": uri,
            "size_bytes": row["size_bytes"],
            "content_base64": base64.b64encode(content).decode("ascii"),
            "truncated": truncated,
            "bytes_read": len(content),
        }

    def dashboard(self, job_ids: list[str] | None = None, limit: int = 5000) -> dict[str, Any]:
        """Chart-data view for one or more jobs, mirroring the Go monitor.

        Returns every stored metric series (downsampled to ``limit`` points
        per series) plus the job list, so any client can render charts the way
        the terminal monitor does.
        """
        self._ensure_open()
        validate_limit(limit, "limit", 50000)
        jobs = self.list(limit=100)["jobs"]
        if job_ids is None:
            job_ids = [j["job_id"] for j in jobs]
        elif not isinstance(job_ids, list) or not job_ids or len(job_ids) > 50:
            raise ValueError("job_ids must be a non-empty list of at most 50 ids")
        for job_id in job_ids:
            if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
                raise ValueError(f"Unknown job_id: {job_id}")
        series: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for job_id in job_ids:
            q = self.metrics_query(job_id, limit=limit)["series"]
            series[job_id] = {metric: _downsample(points, limit) for metric, points in q.items()}
        return {"jobs": jobs, "series": series, "series_count": sum(len(v) for v in series.values())}

    def _delivery_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "delivery_id": row["delivery_id"],
            "event_id": row["event_id"],
            "target_id": row["target_id"],
            "job_id": row["job_id"],
            "target_type": row["target_type"],
            "status": row["status"],
            "attempts": row["attempts"],
            "payload": json.loads(row["payload_json"] or "{}"),
            "created_at": row["created_at"],
            "next_attempt_at": row["next_attempt_at"],
            "delivered_at": row["delivered_at"],
            "last_error": row["last_error"],
            "claim_token": row["claim_token"],
            "claimed_at": row["claimed_at"],
            "lease_expires_at": row["lease_expires_at"],
        }

    def delivery_attempts(self, delivery_id: str, limit: int = 20) -> dict[str, Any]:
        validate_limit(limit, "limit")
        if not self._row("SELECT delivery_id FROM deliveries WHERE delivery_id=?", (delivery_id,)):
            raise ValueError(f"Unknown delivery_id: {delivery_id}")
        with self.db_lock:
            rows = self.db.execute(
                "SELECT * FROM delivery_attempts WHERE delivery_id=? ORDER BY attempt DESC LIMIT ?",
                (delivery_id, limit),
            ).fetchall()
        return {
            "attempts": [
                {
                    "attempt_id": row["attempt_id"],
                    "delivery_id": row["delivery_id"],
                    "attempt": row["attempt"],
                    "claim_token": row["claim_token"],
                    "target_type": row["target_type"],
                    "started_at": row["started_at"],
                    "ended_at": row["ended_at"],
                    "status": row["status"],
                    "error": row["error"],
                    "reclaimed": bool(row["reclaimed"]),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
        }

    def mark_delivery(self, delivery_id: str, status: str, error: str | None = None) -> dict[str, Any]:
        if status not in {"pending", "retrying", "delivered", "failed"}:
            raise ValueError("invalid delivery status")
        error = (error or "")[:DEFAULT_MAX_ERROR_BYTES] or None
        delivered_at = now_iso() if status == "delivered" else None
        with self.db_lock:
            current = self._row("SELECT attempts FROM deliveries WHERE delivery_id=?", (delivery_id,))
            if not current:
                raise ValueError(f"Unknown delivery_id: {delivery_id}")
            attempt = int(current["attempts"]) + 1
            self.db.execute(
                """
                UPDATE deliveries
                SET status=?, attempts=?, delivered_at=COALESCE(?, delivered_at), last_error=?, next_attempt_at=NULL
                WHERE delivery_id=?
                """,
                (status, attempt, delivered_at, error, delivery_id),
            )
            self.db.execute(
                """
                INSERT INTO delivery_attempts(attempt_id, delivery_id, attempt, target_type, started_at, ended_at, status, error, created_at)
                SELECT ?, delivery_id, ?, target_type, ?, ?, ?, ?, ? FROM deliveries WHERE delivery_id=?
                """,
                ("att_" + uuid.uuid4().hex[:16], attempt, now_iso(), now_iso(), status, error, now_iso(), delivery_id),
            )
            self.db.commit()
            row = self._row("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,))
        return self._delivery_dict(row)

    def retry_delivery(self, delivery_id: str) -> dict[str, Any]:
        row = self._row("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,))
        if not row:
            raise ValueError(f"Unknown delivery_id: {delivery_id}")
        with self.db_lock:
            changed = self.db.execute(
                """
                UPDATE deliveries SET status='retrying', next_attempt_at=NULL, last_error=NULL
                WHERE delivery_id=? AND status IN ('failed','retrying')
                """,
                (delivery_id,),
            ).rowcount
            self.db.commit()
            row = self._row("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,))
        delivery = self._delivery_dict(row)
        return delivery

    def tail(self, job_id: str, stream: str = "stdout", max_bytes: int = 8192, offset: int | None = None,
             follow: bool = False, timeout_seconds: float = 5.0, grep: str | None = None) -> dict[str, Any]:
        validate_limit(max_bytes, "max_bytes", max(self.max_log_bytes, 8192))
        if offset is not None and (isinstance(offset, bool) or not isinstance(offset, int) or offset < 0):
            raise ValueError("offset must be a non-negative integer")
        if stream not in {"stdout", "stderr"}:
            raise ValueError("stream must be stdout or stderr")
        if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or timeout_seconds < 0 or timeout_seconds > 86400:
            raise ValueError("timeout_seconds must be between 0 and 86400")
        if grep is not None and (not isinstance(grep, str) or not grep):
            raise ValueError("grep must be a non-empty string")
        row = self._row(f"SELECT {stream}_path AS path FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        path = Path(row["path"])
        size = path.stat().st_size if path.exists() else 0
        if not path.exists():
            result = {
                "job_id": job_id,
                "stream": stream,
                "offset": 0,
                "next_offset": 0,
                "size": 0,
                "truncated": False,
                "content": "",
            }
            return self._tail_follow_result(job_id, stream, result, follow, max_bytes, timeout_seconds, path)
        start = max(0, offset or 0)
        truncated = False
        with path.open("rb") as f:
            if offset is None and size > max_bytes:
                f.seek(-max_bytes, os.SEEK_END)
                start = f.tell()
                truncated = True
            else:
                if size - start > max_bytes:
                    start = max(0, size - max_bytes)
                    truncated = True
                f.seek(min(start, size))
            content = f.read(max_bytes).decode(errors="replace")
            cursor = f.tell()
        if grep:
            content = "".join(line for line in content.splitlines(keepends=True) if grep in line)
        result = {
            "job_id": job_id,
            "stream": stream,
            "offset": start,
            "next_offset": cursor,
            "size": size,
            "truncated": truncated,
            "content": content,
        }
        if not follow:
            return result
        return self._tail_follow_result(job_id, stream, result, follow, max_bytes, timeout_seconds, path)

    def _tail_follow_result(self, job_id: str, stream: str, result: dict[str, Any], follow: bool,
                            max_bytes: int, timeout_seconds: float, path: Path) -> dict[str, Any]:
        if not follow:
            return result
        deadline = time.monotonic() + timeout_seconds
        cursor = int(result["next_offset"])
        new_chunks: list[str] = []
        limit = 4 * max_bytes
        seen_bytes = 0
        while True:
            if self.shutdown_requested.is_set():
                break
            got = False
            if path.exists():
                size = path.stat().st_size
                if size > cursor:
                    with path.open("rb") as f:
                        f.seek(cursor)
                        raw = f.read(max(0, min(limit - seen_bytes, size - cursor)))
                    if raw:
                        got = True
                        chunk = raw.decode(errors="replace")
                        new_chunks.append(chunk)
                        seen_bytes += len(chunk)
                        cursor += len(raw)
                        if seen_bytes >= limit:
                            break
            current_status = self.status(job_id)["status"]
            if not got and current_status in TERMINAL_STATUSES:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.1, remaining))
        return {
            **result,
            "followed": True,
            "new_bytes": seen_bytes,
            "next_offset": cursor,
            "content": result["content"] + "".join(new_chunks),
            "size": path.stat().st_size if path.exists() else result["size"],
        }

    def agent_view(self, thread_id: str | None = None, limit: int = 50) -> dict[str, Any]:
        jobs = []
        for row in self.list(limit=limit, thread_id=thread_id)["jobs"]:
            status = self.status(row["job_id"])
            deliveries = self.deliveries(row["job_id"], limit=100)["deliveries"]
            counts: dict[str, int] = {}
            for delivery in deliveries:
                counts[delivery["status"]] = counts.get(delivery["status"], 0) + 1
            priority = 0
            if status["status"] in {"failed", "timeout", "orphaned"}:
                priority += 100
            if counts.get("failed") or counts.get("pending") or counts.get("retrying"):
                priority += 50
            last_event = status.get("last_event") or {}
            if last_event.get("type") in ATTENTION_EVENTS:
                priority += 25
            jobs.append({**status, "delivery_counts": counts, "priority": priority})
        jobs.sort(key=lambda item: (item["priority"], item["updated_at"]), reverse=True)
        return {"jobs": jobs}

    def reap_orphans(self) -> dict[str, Any]:
        """Terminate MCP stdio server processes whose launching client is gone."""
        orphans = _orphaned_mcp_servers()
        reaped = []
        failed = []
        import signal as _signal

        for entry in orphans:
            pid = entry["pid"]
            try:
                if sys.platform == "win32":
                    subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                else:
                    os.kill(pid, _signal.SIGKILL)
                reaped.append(pid)
            except Exception as exc:
                failed.append({"pid": pid, "error": str(exc)})
        return {"reaped": reaped, "failed": failed, "orphan_count": len(orphans)}

    def doctor(self) -> dict[str, Any]:
        self._ensure_open()
        tables = {
            row["name"]
            for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        required = {"jobs", "events", "wake_targets", "deliveries", "delivery_attempts", "cleanup_tombstones"}
        delivery_counts = {
            row["status"]: row["count"]
            for row in self.db.execute("SELECT status, COUNT(*) AS count FROM deliveries GROUP BY status").fetchall()
        }
        warnings = []
        missing = sorted(required - tables)
        if missing:
            warnings.append({"type": "missing_tables", "tables": missing})
        codex_bin = os.environ.get("VANTH_CODEX_BIN") or (r"C:\codex\codex.exe" if sys.platform == "win32" else "codex")
        codex_available = bool(Path(codex_bin).exists() if ("\\" in codex_bin or "/" in codex_bin) else shutil.which(codex_bin))
        if not codex_available:
            warnings.append({"type": "codex_unavailable", "command": codex_bin})
        opencode_bin = os.environ.get("VANTH_OPENCODE_BIN", "opencode")
        opencode_available = bool(shutil.which(opencode_bin) or Path(opencode_bin).exists())
        if not opencode_available:
            warnings.append({"type": "opencode_unavailable", "command": opencode_bin})
        orphaned_mcp = _orphaned_mcp_servers()
        if orphaned_mcp:
            warnings.append(
                {
                    "type": "orphaned_mcp_servers",
                    "count": len(orphaned_mcp),
                    "pids": [entry["pid"] for entry in orphaned_mcp],
                    "detail": "MCP stdio servers whose launching client is gone; "
                    "reap with `vanth doctor --reap-orphans`",
                }
            )
        quick_check = self.db.execute("PRAGMA quick_check").fetchone()[0]
        stale_leases = self.db.execute(
            "SELECT COUNT(*) FROM deliveries WHERE status='dispatching' AND lease_expires_at IS NOT NULL AND lease_expires_at<=?",
            (now_iso(),),
        ).fetchone()[0]
        dead_lettered = []
        for row in self.db.execute(
            "SELECT delivery_id, job_id, attempts, last_error, payload_json FROM deliveries WHERE status='failed' ORDER BY created_at DESC LIMIT 20"
        ).fetchall():
            target = json.loads(row["payload_json"] or "{}").get("target", {})
            max_attempts = int(target.get("max_attempts", 1))
            if int(row["attempts"]) < max_attempts:
                continue
            dead_lettered.append(
                {
                    "delivery_id": row["delivery_id"],
                    "job_id": row["job_id"],
                    "attempts": int(row["attempts"]),
                    "last_error": row["last_error"],
                }
            )
        disk = shutil.disk_usage(self.home)
        running_jobs = self._running_count()
        # Optional agent adapters being absent is informational, not a health
        # problem: the daemon (and its jobs) are fully functional without them.
        soft_warning_types = {"codex_unavailable", "opencode_unavailable"}
        hard_warnings = [w for w in warnings if w.get("type") not in soft_warning_types]
        return {
            "ok": not hard_warnings and quick_check == "ok",
            "ok_warnings": [w.get("type") for w in warnings],
            "home": str(self.home),
            "db_path": str(self.home / "jobs.sqlite"),
            "logs_dir": str(self.logs),
            "events_dir": str(self.events_dir),
            "tables": sorted(tables),
            "delivery_counts": delivery_counts,
            "codex": {"command": codex_bin, "available": codex_available},
            "opencode": {"command": opencode_bin, "available": opencode_available},
            "schema_version": int(self.db.execute("PRAGMA user_version").fetchone()[0]),
            "quick_check": quick_check,
            "maintenance_alive": bool(self.dispatcher_thread and self.dispatcher_thread.is_alive()),
            "stale_delivery_leases": stale_leases,
            "dead_letter_count": len(dead_lettered),
            "dead_lettered": dead_lettered,
            "running_jobs": running_jobs,
            "max_running_jobs": self.max_running_jobs,
            "retention": {
                "seconds": self.max_retention_seconds,
                "interval_seconds": self.retention_interval_seconds,
                "dry_run": self.retention_dry_run,
            },
            "disk_free_bytes": disk.free,
            "token_path": str(self.home / "token"),
            "warnings": warnings,
            "orphaned_mcp_servers": orphaned_mcp,
        }

    def _cleanup_rows(self, older_than_seconds: int) -> list[sqlite3.Row]:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat().replace("+00:00", "Z")
        with self.db_lock:
            return self.db.execute(
                "SELECT * FROM jobs WHERE status IN ('completed','failed','timeout','cancelled','orphaned') AND updated_at<=?",
                (cutoff,),
            ).fetchall()

    def cleanup(self, older_than_seconds: int, dry_run: bool = True) -> dict[str, Any]:
        self._ensure_open()
        if isinstance(older_than_seconds, bool) or not isinstance(older_than_seconds, int) or older_than_seconds < 0:
            raise ValueError("older_than_seconds must be a non-negative integer")
        rows = self._cleanup_rows(older_than_seconds)
        job_ids = [row["job_id"] for row in rows]
        if not dry_run and job_ids:
            with self.db_lock:
                placeholders = ",".join("?" for _ in job_ids)
                self.db.execute("BEGIN IMMEDIATE")
                for row in rows:
                    job_id = row["job_id"]
                    artifacts = [
                        row["stdout_path"],
                        row["stderr_path"],
                        row["events_path"],
                        str(self.logs / f"{job_id}.runner.log"),
                        str(self.home / "specs" / f"{job_id}.json"),
                        str(self.home / "stdin" / f"{job_id}.in"),
                    ]
                    self.db.execute(
                        "INSERT OR IGNORE INTO cleanup_tombstones(tombstone_id, job_id, artifacts_json, created_at) VALUES (?, ?, ?, ?)",
                        ("clean_" + uuid.uuid4().hex[:16], job_id, json.dumps(artifacts, separators=(",", ":")), now_iso()),
                    )
                self.db.execute(f"DELETE FROM delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM deliveries WHERE job_id IN ({placeholders}))", job_ids)
                self.db.execute(f"DELETE FROM deliveries WHERE job_id IN ({placeholders})", job_ids)
                self.db.execute(f"DELETE FROM wake_targets WHERE job_id IN ({placeholders})", job_ids)
                self.db.execute(f"DELETE FROM events WHERE job_id IN ({placeholders})", job_ids)
                self.db.execute(f"DELETE FROM jobs WHERE job_id IN ({placeholders}) AND status!='running'", job_ids)
                self.db.commit()
        if not dry_run:
            self._drain_cleanup_tombstones()
        return {"dry_run": dry_run, "older_than_seconds": older_than_seconds, "jobs": job_ids, "count": len(job_ids)}

    def cleanup_preview(self, older_than_seconds: int) -> dict[str, Any]:
        """Dry-run preview of the jobs a cleanup would remove, without deleting."""
        self._ensure_open()
        if isinstance(older_than_seconds, bool) or not isinstance(older_than_seconds, int) or older_than_seconds < 0:
            raise ValueError("older_than_seconds must be a non-negative integer")
        jobs = [
            {
                "job_id": row["job_id"],
                "name": row["name"],
                "status": row["status"],
                "updated_at": row["updated_at"],
                "stdout_path": row["stdout_path"],
                "stderr_path": row["stderr_path"],
                "events_path": row["events_path"],
            }
            for row in self._cleanup_rows(older_than_seconds)
        ]
        return {"dry_run": True, "older_than_seconds": older_than_seconds, "jobs": jobs, "count": len(jobs)}

    def _drain_cleanup_tombstones(self) -> None:
        with self.db_lock:
            tombstones = self.db.execute("SELECT tombstone_id, artifacts_json FROM cleanup_tombstones").fetchall()
        for tombstone in tombstones:
            failed = False
            for raw_path in json.loads(tombstone["artifacts_json"] or "[]"):
                for attempt in range(3):
                    try:
                        Path(raw_path).unlink()
                        break
                    except FileNotFoundError:
                        break
                    except OSError:
                        if attempt == 2:
                            failed = True
                        else:
                            time.sleep(0.05)
            if not failed:
                with self.db_lock:
                    self.db.execute("DELETE FROM cleanup_tombstones WHERE tombstone_id=?", (tombstone["tombstone_id"],))
                    self.db.commit()

    async def stop(self, job_id: str, signal: str = "terminate", kill_after_seconds: int = 10) -> dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(None, self.stop_sync, job_id, signal, kill_after_seconds)

    def stop_sync(self, job_id: str, signal: str = "terminate", kill_after_seconds: int = 10) -> dict[str, Any]:
        return self._stop(job_id, signal, kill_after_seconds)

    def _stop(self, job_id: str, signal: str = "terminate", kill_after_seconds: int = 10) -> dict[str, Any]:
        if isinstance(kill_after_seconds, bool) or not isinstance(kill_after_seconds, int) or kill_after_seconds < 0 or kill_after_seconds > 86400:
            raise ValueError("kill_after_seconds must be between 0 and 86400")
        proc = self.processes.get(job_id)
        row = self._row("SELECT status, worker_pid, pid, stop_requested_at, trigger_json FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            raise ValueError(f"Job is not running in this server: {job_id}")
        if row and row["status"] == "queued":
            with self.db_lock:
                changed = self.db.execute(
                    "UPDATE jobs SET status='cancelled', ended_at=?, updated_at=? WHERE job_id=? AND status='queued'",
                    (now_iso(), now_iso(), job_id),
                ).rowcount
                self.db.commit()
            if changed:
                self._emit(job_id, "cancelled", message="Queued job cancelled before its trigger fired")
            return {"job_id": job_id, "status": self.status(job_id)["status"], "message": "Queued job cancelled"}
        if row and row["status"] != "running":
            # Already-terminal stop is an idempotent no-op (review rc14 P1-11):
            # repeated/cleanup stops must not raise when the job finished
            # between a caller's observation and its stop request.
            return {
                "job_id": job_id,
                "status": row["status"],
                "message": f"Job already {row['status']}; stop is a no-op",
            }
        deadline = time.monotonic() + kill_after_seconds
        stop_token = now_iso()
        with self.db_lock:
            requested = self.db.execute(
                "UPDATE jobs SET stop_requested_at=? WHERE job_id=? AND status='running'",
                (stop_token, job_id),
            ).rowcount
            self.db.commit()
        if not requested:
            return {"job_id": job_id, "status": self.status(job_id)["status"], "message": "Job was already terminal"}
        row = self._row("SELECT status, worker_pid, pid, stop_requested_at FROM jobs WHERE job_id=?", (job_id,))
        if not row or row["status"] != "running":
            return {"job_id": job_id, "status": self.status(job_id)["status"], "message": "Job was already terminal"}
        workload_pid = int(row["pid"]) if row["pid"] else None
        if workload_pid and not self._terminate_pid(workload_pid, signal == "kill", deadline):
            with self.db_lock:
                self.db.execute(
                    "UPDATE jobs SET stop_requested_at=NULL WHERE job_id=? AND status='running' AND stop_requested_at=?",
                    (job_id, stop_token),
                )
                self.db.commit()
            raise RuntimeError(f"Failed to stop workload process tree: {workload_pid}")
        changed = self._transition_terminal(job_id, "cancelled")
        if not changed:
            return {"job_id": job_id, "status": self.status(job_id)["status"], "message": "Job was already terminal"}
        self._emit(job_id, "cancelled")
        failures = []
        runner_pid = int(row["worker_pid"]) if row["worker_pid"] else None
        if runner_pid and not self._terminate_pid(runner_pid, signal == "kill", deadline):
            failures.append(runner_pid)
        if proc and proc.pid != runner_pid:
            if not self._terminate_pid(proc.pid, signal == "kill", deadline):
                failures.append(proc.pid)
            try:
                proc.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                failures.append(proc.pid)
        self._readers_done(job_id)
        self.processes.pop(job_id, None)
        if failures:
            raise RuntimeError(f"Failed to stop process tree(s): {failures}")
        return {"job_id": job_id, "status": "cancelled", "message": "Job stopped"}

    def add_wake_target(self, job_id: str, target: dict[str, Any]) -> dict[str, Any]:
        """Schedule a self-resume wake target against a job after the fact."""
        self._ensure_open()
        if not isinstance(target, dict):
            raise ValueError("target must be an object")
        validate_wake_targets([target])
        target_type = target.get("type")
        events = target.get("events")
        if events is None:
            events = target.get("notify_on") or ["completed", "failed"]
        if not events:
            raise ValueError("target events must not be empty")
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        config = {key: value for key, value in target.items() if key not in {"type", "events", "notify_on"}}
        with self.db_lock:
            inserted = self._insert_wake_targets(job_id, [{"type": target_type, "events": events, **config}], now_iso())
            self.db.commit()
        return {"result": "ok", "job_id": job_id, "target_id": inserted[0], "target_type": target_type, "events": events}

    async def send(self, job_id: str, input: str, eof: bool = False) -> dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(None, self.send_sync, job_id, input, eof)

    def send_sync(self, job_id: str, input: str, eof: bool = False) -> dict[str, Any]:
        """Append a stdin record to a running interactive job's channel."""
        self._ensure_open()
        if not isinstance(input, str):
            raise ValueError("input must be a string")
        if not eof and not input:
            raise ValueError("input must be a non-empty string")
        row = self._row(
            "SELECT status, run_json FROM jobs WHERE job_id=?",
            (job_id,),
        )
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        run = json.loads(row["run_json"] or "{}")
        if run.get("interactive") is not True:
            raise ValueError("job is not interactive (started without interactive=True)")
        if row["status"] != "running":
            raise ValueError(f"job is not running: {row['status']}")
        channel_dir = self.home / "stdin"
        channel_dir.mkdir(parents=True, exist_ok=True)
        data = input.encode()
        with (channel_dir / f"{job_id}.in").open("ab") as f:
            if data:
                f.write(struct.pack("<Q", len(data)) + data)
            if eof:
                f.write(struct.pack("<Q", 0))
        return {"job_id": job_id, "sent": len(data), "eof": bool(eof)}


client: VanthClient | None = None
mcp = FastMCP("vanth")


def get_client() -> VanthClient:
    global client
    if client is None:
        client = VanthClient()
        client.ensure()
    return client


def tool_error(message: str) -> dict[str, Any]:
    return {"result": "error", "error": message}


@mcp.tool()
def job_start(
    command: str,
    cwd: str | None = None,
    name: str | None = None,
    env: dict[str, str] | None = None,
    timeout_seconds: int | None = None,
    notify_on: list[str] | None = None,
    wake_targets: list[dict[str, Any]] | None = None,
    origin_thread_id: str | None = None,
    tags: list[str] | None = None,
    notes: str | None = None,
    interactive: bool = False,
    trigger: dict[str, str] | None = None,
    remote_id: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Start a background job.

    ``trigger`` optionally gates launch on another job: pass
    ``{"job_id": "A", "status": "completed"}`` to start this job only once job
    A reaches ``completed``. The new job is created ``queued`` and its runner
    starts automatically when the trigger fires (or it is ``cancelled`` if A
    ends in a different terminal status).

    Pass ``remote_id`` to run the job on a paired remote host instead of the
    local daemon. Remote mutations require ``idempotency_key`` (8..128 chars in
    ``[A-Za-z0-9_-]``); when omitted the daemon mints one.
    """
    return get_client().post(
        "/jobs",
        {
            "command": command,
            "cwd": cwd,
            "name": name,
            "env": env,
            "timeout_seconds": timeout_seconds,
            "notify_on": notify_on,
            "wake_targets": wake_targets,
            "origin_thread_id": origin_thread_id,
            "tags": tags,
            "notes": notes,
            "interactive": interactive,
            "trigger": trigger,
            "remote_id": remote_id,
            "idempotency_key": idempotency_key,
        },
    )


@mcp.tool()
def job_rerun(job_id: str, command: str | None = None, env: dict[str, str] | None = None,
              timeout_seconds: int | None = None, name: str | None = None, tags: list[str] | None = None,
              notes: str | None = None, cwd: str | None = None, interactive: bool | None = None,
              remote_id: str | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
    payload = {key: value for key, value in {
        "command": command,
        "env": env,
        "timeout_seconds": timeout_seconds,
        "name": name,
        "tags": tags,
        "notes": notes,
        "cwd": cwd,
        "interactive": interactive,
        "remote_id": remote_id,
        "idempotency_key": idempotency_key,
    }.items() if value is not None}
    return get_client().post(f"/jobs/{job_id}/rerun", payload)


@mcp.tool()
def job_status_batch(job_ids: list[str], limit: int = 500) -> dict[str, Any]:
    return get_client().get("/status/batch", {"job_ids": ",".join(job_ids), "limit": limit})


@mcp.tool()
def job_status(job_id: str, remote_id: str | None = None) -> dict[str, Any]:
    return get_client().get(f"/jobs/{job_id}/status", {"remote_id": remote_id})


@mcp.tool()
def job_send(job_id: str, input: str, eof: bool = False) -> dict[str, Any]:
    return get_client().post(f"/jobs/{job_id}/send", {"input": input, "eof": eof})


@mcp.tool()
def job_list(status: list[str] | None = None, limit: int = 50, thread_id: str | None = None,
             name: str | None = None, tags: list[str] | None = None) -> dict[str, Any]:
    return get_client().get("/jobs", {"status": status, "limit": limit, "thread_id": thread_id, "name": name, "tags": tags})


@mcp.tool()
def job_view(thread_id: str | None = None, limit: int = 50) -> dict[str, Any]:
    return get_client().get("/view", {"thread_id": thread_id, "limit": limit})


@mcp.tool()
def job_events(job_id: str, since_event_id: str | None = None, types: list[str] | None = None, limit: int = 20,
               reverse: bool = False) -> dict[str, Any]:
    return get_client().get(f"/jobs/{job_id}/events", {"since_event_id": since_event_id, "types": types, "limit": limit, "reverse": reverse})


@mcp.tool()
def job_deliveries(job_id: str | None = None, status: str | None = None, limit: int = 20) -> dict[str, Any]:
    return get_client().get("/deliveries", {"job_id": job_id, "status": status, "limit": limit})


@mcp.tool()
def job_mark_delivery(delivery_id: str, status: str, error: str | None = None) -> dict[str, Any]:
    return get_client().post(f"/deliveries/{delivery_id}/mark", {"status": status, "error": error})


@mcp.tool()
def job_retry_delivery(delivery_id: str) -> dict[str, Any]:
    return get_client().post(f"/deliveries/{delivery_id}/retry")


@mcp.tool()
def job_delivery_attempts(delivery_id: str, limit: int = 20) -> dict[str, Any]:
    return get_client().get(f"/deliveries/{delivery_id}/attempts", {"limit": limit})


@mcp.tool()
def job_tail(job_id: str, stream: str = "stdout", max_bytes: int = 8192, offset: int | None = None,
             follow: bool = False, timeout_seconds: float = 5.0, grep: str | None = None) -> dict[str, Any]:
    return get_client().get(f"/jobs/{job_id}/tail", {"stream": stream, "max_bytes": max_bytes, "offset": offset,
                                                      "follow": follow, "timeout_seconds": timeout_seconds, "grep": grep})


@mcp.tool()
def job_wait(
    job_id: str,
    filters: list[str],
    since_event_id: str | None = None,
    timeout_seconds: int = 3600,
    return_progress: bool = False,
    metric_ge: dict[str, float] | None = None,
    remote_id: str | None = None,
) -> dict[str, Any]:
    """Wait until a matching event fires or a metric crosses a threshold.

    ``filters`` is a list of event types (e.g. ``["completed", "failed"]``);
    the first matching event (after ``since_event_id``) is returned as
    ``{"result": "event", ...}``. ``metric_ge`` maps a metric name (e.g.
    ``loss``, ``progress.percent``) to a numeric threshold; when the latest
    stored value reaches it, the wait returns ``{"result": "metric", ...}``
    with the threshold and current value. ``return_progress`` streams progress
    events instead of blocking. Returns ``{"result": "timeout"}`` when
    ``timeout_seconds`` elapses.

    With ``remote_id`` the wait polls ``RemoteControl.status`` every 0.2s until
    the remote reports a terminal status or the timeout elapses (a real
    cross-machine event push arrives in Phase 4); ``since_event_id``,
    ``return_progress`` and ``metric_ge`` are ignored in that mode.
    """
    return get_client().post(
        f"/jobs/{job_id}/wait",
        {"filters": filters, "since_event_id": since_event_id, "timeout_seconds": timeout_seconds,
         "return_progress": return_progress, "metric_ge": metric_ge, "remote_id": remote_id},
    )


@mcp.tool()
def job_stop(job_id: str, signal: str = "terminate", kill_after_seconds: int = 10,
             remote_id: str | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
    return get_client().post(f"/jobs/{job_id}/stop", {"signal": signal, "kill_after_seconds": kill_after_seconds,
                                                       "remote_id": remote_id, "idempotency_key": idempotency_key})


@mcp.tool()
def job_doctor() -> dict[str, Any]:
    return get_client().get("/doctor")


@mcp.tool()
def job_cleanup(older_than_seconds: int, dry_run: bool = True) -> dict[str, Any]:
    return get_client().post("/cleanup", {"older_than_seconds": older_than_seconds, "dry_run": dry_run})


@mcp.tool()
def job_metrics_query(job_id: str, metric: str | None = None, from_ms: int | None = None,
                      to_ms: int | None = None, limit: int = 1000) -> dict[str, Any]:
    """Return stored scalar metric series for a job (loss, accuracy, progress.percent, ...)."""
    return get_client().get(f"/jobs/{job_id}/metrics", {"metric": metric, "from_ms": from_ms, "to_ms": to_ms, "limit": limit})


@mcp.tool()
def job_metric_compare(job_ids: list[str], metric: str, aggregation: str = "latest",
                       from_ms: int | None = None, to_ms: int | None = None) -> dict[str, Any]:
    """Compare one metric across jobs (e.g. val_loss across training runs)."""
    return get_client().get("/metrics/compare", {"job_ids": job_ids, "metric": metric, "aggregation": aggregation,
                                                 "from_ms": from_ms, "to_ms": to_ms})


@mcp.tool()
def job_run_summary(job_id: str) -> dict[str, Any]:
    """One-call summary of a job: status, runtime, progress, latest metrics, artifacts."""
    return get_client().get(f"/jobs/{job_id}/summary")


@mcp.tool()
def job_diff(base_job_id: str, other_job_id: str) -> dict[str, Any]:
    """Diff the run specs of two jobs (command, env, cwd, timeout, tags, wake targets).

    Useful for comparing a job to its rerun, or two pipeline stages, to see
    exactly what changed. Returns a list of per-field changes with base/other
    values and `identical: true` when nothing differs.
    """
    return get_client().get(f"/jobs/{base_job_id}/diff", {"other": other_job_id})


@mcp.tool()
def job_artifact_add(job_id: str, name: str, uri: str, kind: str | None = None,
                     size_bytes: int | None = None, sha256: str | None = None,
                     meta: dict[str, Any] | None = None) -> dict[str, Any]:
    """Attach an artifact (checkpoint, CSV, rendered output) to a job."""
    return get_client().post(f"/jobs/{job_id}/artifacts", {"name": name, "uri": uri, "kind": kind,
                                                           "size_bytes": size_bytes, "sha256": sha256, "meta": meta})


@mcp.tool()
def job_artifacts(job_id: str, limit: int = 50) -> dict[str, Any]:
    """List artifacts attached to a job."""
    return get_client().get(f"/jobs/{job_id}/artifacts", {"limit": limit})


@mcp.tool()
def job_dashboard(job_ids: list[str] | None = None, limit: int = 5000) -> dict[str, Any]:
    """Chart-data view (downsampled series per job + job list) for any chart renderer."""
    return get_client().get("/dashboard", {"job_ids": job_ids, "limit": limit})


@mcp.tool()
def job_metric_ingest(job_id: str, metrics: list[dict[str, Any]], idempotency_key: str | None = None) -> dict[str, Any]:
    """Record scalar metric points for a job (loss, accuracy, ...) programmatically."""
    return get_client().post(f"/jobs/{job_id}/metrics", {"metrics": metrics, "idempotency_key": idempotency_key})


@mcp.tool()
def job_artifact_read(artifact_id: str, max_bytes: int = 262144) -> dict[str, Any]:
    """Fetch the content of an artifact (base64-encoded) for direct consumption."""
    return get_client().get(f"/artifacts/{artifact_id}/content", {"max_bytes": max_bytes})


@mcp.tool()
def artifact_put(path: str, name: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Publish a local file into the managed artifact store as an immutable version.

    ``name`` selects the file root; identical content re-published to the same
    root deduplicates onto the existing version.
    """
    return get_client().post("/artifacts/put", {"path": path, "name": name, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_put_dir(source_path: str, name: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Publish a local directory tree into the managed artifact store as an immutable v1 version.

    Capture refuses symlinks, reparse points, special files, and source
    mutation; identical trees re-published to the same root deduplicate.
    """
    return get_client().post("/artifacts/put-dir",
                             {"source_path": source_path, "name": name, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_resolve(name: str, alias: str | None = None, version_id: str | None = None) -> dict[str, Any]:
    """Resolve a root (latest), alias pin, or explicit version to one immutable version."""
    return get_client().get("/artifacts/resolve", {"name": name, "alias": alias, "version_id": version_id})


@mcp.tool()
def artifact_info(version_id: str) -> dict[str, Any]:
    """Manifest plus blob existence and verification flag for one artifact version."""
    return get_client().get(f"/artifacts/info/{version_id}")


@mcp.tool()
def artifact_materialize(version_id: str, dest_path: str, overwrite: bool = False) -> dict[str, Any]:
    """Write an artifact version's content to dest_path atomically (existing destinations fail unless overwrite)."""
    return get_client().post("/artifacts/materialize",
                             {"version_id": version_id, "dest_path": dest_path, "overwrite": overwrite})


@mcp.tool()
def artifact_verify(version_id: str) -> dict[str, Any]:
    """Re-hash the stored content of an artifact version and report ok/expected/actual."""
    return get_client().post("/artifacts/verify", {"version_id": version_id})


@mcp.tool()
def artifact_collection_create(name: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Create a named artifact collection for monotonic immutable version lists."""
    return get_client().post("/artifacts/collections", {"name": name, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_collection_append(collection: str, version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Append an immutable version to a collection with a monotonic ordinal (duplicate append is a no-op)."""
    return get_client().post("/artifacts/collections/append",
                             {"collection": collection, "version_id": version_id, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_collection_get(name: str) -> dict[str, Any]:
    """Get a collection's ordered versions (by monotonic ordinal)."""
    return get_client().get(f"/artifacts/collections/{name}")


@mcp.tool()
def artifact_alias_set(alias_name: str, root_id: str, new_version_id: str,
                       expected_version_id: str | None = None, updated_by: str | None = None,
                       idempotency_key: str | None = None) -> dict[str, Any]:
    """Compare-and-swap an alias pin: moves only if it currently points at expected_version_id.

    Pass expected_version_id=None to create a new alias; any mismatch fails
    with ALIAS_CAS_MISMATCH and never silently moves the alias.
    """
    return get_client().post("/artifacts/alias-set",
                             {"alias_name": alias_name, "root_id": root_id, "new_version_id": new_version_id,
                              "expected_version_id": expected_version_id, "updated_by": updated_by,
                              "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_link_lineage(producer_kind: str, producer_id: str, consumer_kind: str, consumer_id: str,
                          version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Link a producer/consumer identity ('job'|'remote_job'|'version'|'alias') to one immutable version."""
    return get_client().post("/artifacts/lineage",
                             {"producer_kind": producer_kind, "producer_id": producer_id,
                              "consumer_kind": consumer_kind, "consumer_id": consumer_id,
                              "version_id": version_id, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_lineage_for(version_id: str) -> dict[str, Any]:
    """List all lineage links recorded against one immutable version."""
    return get_client().get(f"/artifacts/lineage/{version_id}")


@mcp.tool()
def artifact_delete_request(version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Logically delete an artifact version (content stays until GC reclaims it); rejects aliased versions."""
    return get_client().post("/artifacts/delete-request",
                             {"version_id": version_id, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_restore(version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Clear a pending delete request on an artifact version."""
    return get_client().post("/artifacts/restore-version",
                             {"version_id": version_id, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_pin(version_id: str, hold_reason: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Pin/hold an artifact version so GC can never reclaim it."""
    return get_client().post("/artifacts/pin", {"version_id": version_id, "hold_reason": hold_reason,
                                                "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_unpin(version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Remove a pin/hold from an artifact version."""
    return get_client().post("/artifacts/unpin", {"version_id": version_id,
                                                  "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_gc(dry_run: bool = True, idempotency_key: str | None = None) -> dict[str, Any]:
    """Fenced garbage collection of unreachable versions/blobs; dry_run=True only reports candidates."""
    return get_client().post("/artifacts/gc", {"dry_run": dry_run, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_backup() -> dict[str, Any]:
    """Take a manual sqlite backup of the artifacts catalog."""
    return get_client().post("/artifacts/backup", {})


@mcp.tool()
def artifact_begin_restore(backup_path: str) -> dict[str, Any]:
    """Restore the artifacts catalog from a backup copy; rotates instance identity and locks mutations until complete-restore."""
    return get_client().post("/artifacts/begin-restore", {"backup_path": backup_path})


@mcp.tool()
def artifact_complete_restore() -> dict[str, Any]:
    """Clear the recovery_required marker after a restore so mutations are allowed again."""
    return get_client().post("/artifacts/complete-restore", {})


@mcp.tool()
def artifact_storage_profile_create(kind: str = "s3", config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Register a storage profile (immutable revisions; creates revision 1)."""
    return get_client().post("/artifacts/storage-profiles", {"kind": kind, "config": config})


@mcp.tool()
def artifact_storage_profile_get(profile_id: str) -> dict[str, Any]:
    """Get the latest revision of a storage profile (config + capabilities)."""
    return get_client().get(f"/artifacts/storage-profiles/{profile_id}")


@mcp.tool()
def artifact_storage_profile_probe(profile_id: str) -> dict[str, Any]:
    """Probe a storage profile's endpoint capabilities and store them on the latest revision."""
    return get_client().post(f"/artifacts/storage-profiles/{profile_id}/probe", {})


@mcp.tool()
def artifact_storage_profile_update(profile_id: str, config: dict[str, Any],
                                    idempotency_key: str | None = None) -> dict[str, Any]:
    """Insert the NEXT immutable revision of a storage profile; old revisions stay queryable."""
    return get_client().post(f"/artifacts/storage-profiles/{profile_id}/update",
                             {"config": config, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_push_remote(remote_id: str, version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Publish a local managed artifact version to a paired remote (chunked, resumable; no credentials cross the wire)."""
    return get_client().post("/artifacts/push-remote",
                             {"remote_id": remote_id, "version_id": version_id,
                              "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_pull_remote(remote_id: str, version_id: str, dest_path: str,
                         idempotency_key: str | None = None) -> dict[str, Any]:
    """Materialize a remote artifact version onto this machine via the controller broker (chunked, resumable)."""
    return get_client().post("/artifacts/pull-remote",
                             {"remote_id": remote_id, "version_id": version_id,
                              "dest_path": dest_path, "idempotency_key": idempotency_key})


def _build_wake_target(
    target: dict[str, Any] | None,
    events: list[str] | None,
    target_type: str | None,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Resolve the daemon_wake shorthand into a full wake-target dict.

    When ``target`` is given it is returned unchanged (backward compatible).
    Otherwise a target is built from ``type`` / ``events`` / ``config``.
    ``type`` is required and must be a supported wake target type
    (local_command, codex_thread, opencode_thread); events default to
    ["completed", "failed"].
    """
    if target is not None:
        if not isinstance(target, dict):
            raise ValueError("target must be a dict")
        return target
    if target_type is None or not isinstance(target_type, str) or not target_type:
        raise ValueError("type is required when target is not provided")
    if target_type not in WAKE_TARGET_TYPES:
        raise ValueError(f"unsupported wake target type: {target_type!r}")
    if events is not None:
        if not isinstance(events, list) or not events or not all(isinstance(event, str) for event in events):
            raise ValueError("events must be a non-empty list of strings")
    return {"type": target_type, "events": events or ["completed", "failed"], **config}


@mcp.tool()
def daemon_wake(
    job_id: str,
    target: dict[str, Any] | None = None,
    events: list[str] | None = None,
    type: str | None = None,
    **config: Any,
) -> dict[str, Any]:
    """Schedule a self-resume wake target against a running/known job.

    Pass a full target dict ({"type", "events", ...config}) as ``target``, or
    use the shorthand: ``type`` (required, one of local_command / codex_thread
    / opencode_thread) plus optional ``events`` and extra config kwargs.
    Events default to ["completed", "failed"].
    """
    resolved = _build_wake_target(target, events, type, config)
    return get_client().post(f"/jobs/{job_id}/wake", {"target": resolved})


@mcp.tool()
def job_cleanup_preview(older_than_seconds: int) -> dict[str, Any]:
    """Dry-run preview of what job_cleanup would remove, without deleting anything."""
    return get_client().get("/cleanup/preview", {"older_than_seconds": older_than_seconds})


def _orphaned_mcp_servers() -> list[dict[str, Any]]:
    """Find MCP stdio server processes whose launching client is gone.

    Scans for ``vanth``/python processes running the Vanth MCP entrypoint and
    checks whether their parent process is still alive. A ``vanth`` process
    launched by a dead client is an orphan that the new watchdog is designed to
    prevent, but which older versions (and force-killed sessions) may still
    have left behind. Returns a list of ``{pid, started, parent_pid}`` entries.
    """
    from .process_watch import process_alive

    import subprocess as _sp

    candidates = []
    try:
        if sys.platform == "win32":
            result = _sp.run(
                ["wmic", "process", "get", "name,processid,parentprocessid,creationdate", "/format:csv"],
                stdout=_sp.PIPE, stderr=_sp.DEVNULL, text=True, timeout=10,
            )
            for line in result.stdout.splitlines()[1:]:
                if not line.strip():
                    continue
                parts = line.split(",")
                if len(parts) < 5:
                    continue
                _, name, pid, ppid, created = parts[:5]
                name = (name or "").strip()
                pid_s = (pid or "").strip()
                if not name or not pid_s.isdigit():
                    continue
                if "vanth" not in name.lower() and "python" not in name.lower():
                    continue
                candidates.append(
                    {
                        "pid": int(pid_s),
                        "name": name,
                        "started": created.strip(),
                        "ppid": int(ppid.strip()) if (ppid or "").strip().isdigit() else None,
                    }
                )
        else:
            result = _sp.run(["ps", "-eo", "pid=,ppid=,etime=,comm="],
                             stdout=_sp.PIPE, stderr=_sp.DEVNULL, text=True, timeout=10)
            for line in result.stdout.splitlines():
                parts = line.split(None, 3)
                if len(parts) < 4:
                    continue
                pid, ppid, etime, comm = parts
                if "vanth" not in comm.lower() and "python" not in comm.lower():
                    continue
                candidates.append(
                    {
                        "pid": int(pid),
                        "name": comm,
                        "started": etime,
                        "ppid": int(ppid) if ppid.isdigit() else None,
                    }
                )
    except Exception:
        return []
    orphans = []
    for entry in candidates:
        if entry["ppid"] in (0, 1, None):
            continue
        if process_alive(entry["ppid"]):
            continue
        orphans.append(
            {
                "pid": entry["pid"],
                "started": entry["started"],
                "parent_pid": entry["ppid"],
            }
        )
    return orphans


def _hint_setup() -> None:
    """Print a stderr hint on MCP startup when a known client still lacks the
    Vanth MCP entry. stdout is the JSON-RPC protocol, so hints go to stderr
    (harmless to the transport). Suppress with VANTH_NO_SETUP_HINT=1."""
    if os.environ.get("VANTH_NO_SETUP_HINT") in {"1", "true", "yes"}:
        return
    try:
        from .setup import _is_configured, client_config_paths

        found = client_config_paths()
        missing = []
        for client, paths in found.items():
            if not any(_is_configured(client, path) for path in paths):
                missing.append(client)
        if missing:
            print(
                "vanth: MCP server not configured in " + ", ".join(missing)
                + " — run `vanth setup` to register (or set VANTH_NO_SETUP_HINT=1)",
                file=sys.stderr,
            )
    except Exception:
        pass


def main(argv: list[str] | None = None) -> None:
    from .cli import main as cli_main

    args = list(sys.argv[1:] if argv is None else argv)
    # Human-facing subcommands are dispatched to the CLI; anything else
    # (including no args) runs the MCP stdio server, which is what MCP
    # clients expect from `vanth` (bare).
    if args and args[0] in {
        "status", "doctor", "restart", "setup", "--help", "-h", "help",
        "list", "ps", "logs", "tail", "stop", "artifacts", "prune",
        "autostart", "--version", "version", "remote",
    }:
        raise SystemExit(cli_main(args))
    # Interactive misuse guard (user report): bare `vanth` typed in a real
    # terminal would otherwise start the MCP stdio server and appear to
    # "hang" reading JSON-RPC from the keyboard. Real MCP clients always
    # run us with pipes, never a TTY on stdin.
    if not args and sys.stdin.isatty() and sys.stdout.isatty():
        print(
            "vanth: refusing to start the MCP stdio server in an interactive "
            "terminal.\n"
            "  - Terminal dashboard:            vanth-monitor\n"
            "  - Human subcommands:             vanth doctor | status | setup\n"
            "  - MCP clients launch `vanth` with pipes automatically.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    _hint_setup()
    _run_mcp_server()


def _run_mcp_server() -> None:
    """Run the MCP stdio server under a parent-liveness + idle watchdog.

    Agent clients (codex/opencode) launch ``vanth`` as a stdio MCP server. If a
    session dies without closing stdin, or the client accumulates cached
    workers, those processes would otherwise linger forever holding
    ``vanth.exe``. The watchdog (see ``vanth.process_watch``) self-terminates
    the process when the parent dies or the process is idle, while never
    killing a blocking tool call (``job_wait``/``job_tail --follow``) mid-flight.
    """
    from .process_watch import start_watchdog

    thread, tracker = start_watchdog()

    # Bump the in-flight counter around every tool call so a long-running
    # blocking call (job_wait with a filter) is never idle-reaped mid-call.
    original_call_tool = mcp.call_tool

    async def guarded_call_tool(name: str, arguments: dict[str, object]) -> object:
        with tracker:
            return await original_call_tool(name, arguments)

    mcp.call_tool = guarded_call_tool  # type: ignore[method-assign]

    try:
        mcp.run()
    finally:
        if thread is not None and thread.is_alive():
            # The stdio loop ended (client closed stdin or sent exit): no need
            # for the watchdog anymore; it would only fire a redundant exit.
            pass
