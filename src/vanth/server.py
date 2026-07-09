from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from .client import VanthClient

EVENT_PREFIX = "AGENT_EVENT "
DEFAULT_MAX_EVENT_BYTES = 65536


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_agent_event_line(line: str) -> dict[str, Any] | None:
    if not line.startswith(EVENT_PREFIX):
        return None
    try:
        payload = json.loads(line[len(EVENT_PREFIX) :])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) and isinstance(payload.get("type"), str) else None


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


class JobManager:
    def __init__(self, home: str | Path | None = None) -> None:
        self.home = Path(home or os.environ.get("VANTH_HOME") or os.environ.get("AGENT_BG_HOME") or Path.home() / ".vanth")
        self.max_event_bytes = int(
            os.environ.get("VANTH_MAX_EVENT_BYTES")
            or os.environ.get("AGENT_BG_MAX_EVENT_BYTES")
            or DEFAULT_MAX_EVENT_BYTES
        )
        self.logs = self.home / "logs"
        self.events_dir = self.home / "events"
        self.logs.mkdir(parents=True, exist_ok=True)
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.home / "jobs.sqlite", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
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
              stdout_path TEXT NOT NULL,
              stderr_path TEXT NOT NULL,
              events_path TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
              event_id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL,
              seq INTEGER NOT NULL,
              type TEXT NOT NULL,
              level TEXT,
              message TEXT,
              data_json TEXT,
              source TEXT,
              created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_job_seq ON events(job_id, seq);
            CREATE INDEX IF NOT EXISTS idx_events_job_type_seq ON events(job_id, type, seq);
            CREATE TABLE IF NOT EXISTS wake_targets (
              target_id TEXT PRIMARY KEY,
              job_id TEXT NOT NULL,
              type TEXT NOT NULL,
              events_json TEXT NOT NULL,
              config_json TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deliveries (
              delivery_id TEXT PRIMARY KEY,
              event_id TEXT NOT NULL,
              target_id TEXT NOT NULL,
              job_id TEXT NOT NULL,
              target_type TEXT NOT NULL,
              status TEXT NOT NULL,
              attempts INTEGER NOT NULL DEFAULT 0,
              payload_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              delivered_at TEXT,
              last_error TEXT,
              UNIQUE(event_id, target_id)
            );
            CREATE INDEX IF NOT EXISTS idx_wake_targets_job ON wake_targets(job_id);
            CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status, created_at);
            """
        )
        self.db.execute(
            "UPDATE jobs SET status='orphaned', updated_at=? WHERE status='running'",
            (now_iso(),),
        )
        self.db.commit()
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.reader_threads: dict[str, list[threading.Thread]] = {}
        self.conditions: dict[str, threading.Condition] = {}
        self.db_lock = threading.RLock()

    def close(self) -> None:
        self.db.close()

    def _condition(self, job_id: str) -> threading.Condition:
        self.conditions.setdefault(job_id, threading.Condition())
        return self.conditions[job_id]

    def _row(self, sql: str, args: tuple[Any, ...]) -> sqlite3.Row | None:
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
        payload = normalize_event_payload({"type": event_type, "message": message, "data": data or {}, "level": level})
        data_json = json.dumps(payload["data"], separators=(",", ":"))
        if len(data_json.encode()) > self.max_event_bytes:
            payload["data"] = {"truncated": True, "max_bytes": self.max_event_bytes}
            payload["message"] = payload["message"] or "Event payload exceeded max bytes"
            payload["level"] = "warning"
            data_json = json.dumps(payload["data"], separators=(",", ":"))
        with self.db_lock:
            row = self._row("SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM events WHERE job_id=?", (job_id,))
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
            self.db.commit()
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
            events_path = self._row("SELECT events_path FROM jobs WHERE job_id=?", (job_id,))["events_path"]
            with Path(events_path).open("a", encoding="utf-8") as f:
                f.write(json.dumps(event, separators=(",", ":")) + "\n")
            deliveries = self._enqueue_deliveries(event)
        with self._condition(job_id):
            self._condition(job_id).notify_all()
        for delivery in deliveries:
            threading.Thread(target=self._dispatch_delivery, args=(delivery,), daemon=True).start()
        return event

    def _enqueue_deliveries(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        deliveries = []
        targets = self.db.execute("SELECT * FROM wake_targets WHERE job_id=?", (event["job_id"],)).fetchall()
        for target in targets:
            events = json.loads(target["events_json"] or "[]")
            if events and event["type"] not in events:
                continue
            payload = self._delivery_payload(event, target)
            delivery_id = "del_" + uuid.uuid4().hex[:16]
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
        self.db.commit()
        return deliveries

    def _dispatch_delivery(self, delivery: dict[str, Any]) -> None:
        payload = delivery["payload"]
        target = payload.get("target", {})
        command = target.get("command")
        if not command:
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
                self.mark_delivery(delivery["delivery_id"], "delivered")
            else:
                self.mark_delivery(delivery["delivery_id"], "failed", (proc.stderr or "").strip())
        except Exception as exc:
            self.mark_delivery(delivery["delivery_id"], "failed", str(exc))

    def _delivery_payload(self, event: dict[str, Any], target: sqlite3.Row) -> dict[str, Any]:
        config = json.loads(target["config_json"] or "{}")
        prompt = config.get("prompt")
        if not prompt:
            prompt = (
                "vanth event\n"
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
    ) -> dict[str, Any]:
        job_id = "job_" + uuid.uuid4().hex[:12]
        stdout_path = self.logs / f"{job_id}.stdout.log"
        stderr_path = self.logs / f"{job_id}.stderr.log"
        events_path = self.events_dir / f"{job_id}.jsonl"
        created_at = now_iso()
        proc_env = os.environ.copy()
        if env:
            proc_env.update(env)
        creationflags = 0
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0)
        proc = subprocess.Popen(
            command,
            cwd=cwd,
            env=proc_env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            creationflags=creationflags,
        )
        self.processes[job_id] = proc
        with self.db_lock:
            self.db.execute(
                """
                INSERT INTO jobs(job_id, name, command, cwd, status, pid, created_at, updated_at, started_at,
                  timeout_seconds, notify_on, stdout_path, stderr_path, events_path)
                VALUES (?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    name,
                    command,
                    cwd,
                    proc.pid,
                    created_at,
                    created_at,
                    created_at,
                    timeout_seconds,
                    json.dumps(notify_on or []),
                    str(stdout_path),
                    str(stderr_path),
                    str(events_path),
                ),
            )
            self.db.commit()
            self._insert_wake_targets(job_id, wake_targets or [], created_at)
            self.db.commit()
        self._emit(job_id, "started")
        self.reader_threads[job_id] = [
            threading.Thread(target=self._read_stream, args=(job_id, proc.stdout, stdout_path, "stdout"), daemon=True),
            threading.Thread(target=self._read_stream, args=(job_id, proc.stderr, stderr_path, "stderr"), daemon=True),
        ]
        for thread in self.reader_threads[job_id]:
            thread.start()
        threading.Thread(target=self._watch, args=(job_id, proc, timeout_seconds), daemon=True).start()
        return {
            "job_id": job_id,
            "status": "running",
            "pid": proc.pid,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "events_path": str(events_path),
            "message": "Job started",
        }

    def _insert_wake_targets(self, job_id: str, targets: list[dict[str, Any]], created_at: str) -> None:
        for target in targets:
            target_type = target.get("type")
            events = target.get("events") or target.get("notify_on") or []
            if not isinstance(target_type, str):
                continue
            config = {key: value for key, value in target.items() if key not in {"type", "events", "notify_on"}}
            self.db.execute(
                """
                INSERT INTO wake_targets(target_id, job_id, type, events_json, config_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "target_" + uuid.uuid4().hex[:12],
                    job_id,
                    target_type,
                    json.dumps(events, separators=(",", ":")),
                    json.dumps(config, separators=(",", ":")),
                    created_at,
                ),
            )

    def _read_stream(
        self,
        job_id: str,
        stream,
        path: Path,
        source: str,
    ) -> None:
        if stream is None:
            return
        with path.open("ab") as f:
            while line := stream.readline():
                f.write(line)
                f.flush()
                payload = parse_agent_event_line(line.decode(errors="replace").rstrip("\r\n"))
                if payload:
                    event = normalize_event_payload(payload)
                    self._emit(
                        job_id,
                        event["type"],
                        message=event["message"],
                        data=event["data"],
                        level=event["level"],
                        source=source,
                    )

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
            thread.join(timeout=2)

    def _kill_process(self, proc: subprocess.Popen[bytes], force: bool) -> None:
        if sys.platform == "win32" and proc.pid:
            args = ["taskkill", "/PID", str(proc.pid), "/T"]
            if force:
                args.append("/F")
            subprocess.run(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        proc.kill() if force else proc.terminate()

    def _finish(self, job_id: str, status: str, exit_code: int | None = None) -> None:
        with self.db_lock:
            self.db.execute(
                "UPDATE jobs SET status=?, exit_code=?, ended_at=?, updated_at=? WHERE job_id=?",
                (status, exit_code, now_iso(), now_iso(), job_id),
            )
            self.db.commit()
        data = {"exit_code": exit_code} if exit_code is not None else {}
        self._emit(job_id, status, data=data)
        self.processes.pop(job_id, None)

    def _event_query(self, job_id: str, types: list[str] | None, since_event_id: str | None, limit: int) -> list[dict[str, Any]]:
        since_seq = 0
        if since_event_id:
            row = self._row("SELECT seq FROM events WHERE job_id=? AND event_id=?", (job_id, since_event_id))
            since_seq = int(row["seq"]) if row else 0
        args: list[Any] = [job_id, since_seq]
        where = "job_id=? AND seq>?"
        if types:
            where += " AND type IN (%s)" % ",".join("?" for _ in types)
            args.extend(types)
        with self.db_lock:
            rows = self.db.execute(
                f"SELECT * FROM events WHERE {where} ORDER BY seq LIMIT ?",
                (*args, limit),
            ).fetchall()
        return [self._event_dict(row) for row in rows]

    async def wait(
        self,
        job_id: str,
        filters: list[str],
        since_event_id: str | None = None,
        timeout_seconds: int = 3600,
    ) -> dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(
            None,
            self.wait_sync,
            job_id,
            filters,
            since_event_id,
            timeout_seconds,
        )

    def wait_sync(
        self,
        job_id: str,
        filters: list[str],
        since_event_id: str | None = None,
        timeout_seconds: int = 3600,
    ) -> dict[str, Any]:
        return self._wait(job_id, filters, since_event_id, timeout_seconds)

    def _wait(
        self,
        job_id: str,
        filters: list[str],
        since_event_id: str | None = None,
        timeout_seconds: int = 3600,
    ) -> dict[str, Any]:
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        deadline = time.monotonic() + timeout_seconds
        while True:
            events = self._event_query(job_id, filters, since_event_id, 1)
            if events:
                return {"result": "event", "job_id": job_id, "status": self.status(job_id)["status"], "event": events[0]}
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {"result": "timeout", "job_id": job_id, "status": self.status(job_id)["status"], "message": "No matching event before timeout"}
            with self._condition(job_id):
                if not self._condition(job_id).wait(timeout=remaining):
                    return {"result": "timeout", "job_id": job_id, "status": self.status(job_id)["status"], "message": "No matching event before timeout"}

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
            "pid": row["pid"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "exit_code": row["exit_code"],
            "last_event": self._event_dict(last) if last else None,
        }
        result["progress"] = ({**json.loads(progress["data_json"] or "{}"), "updated_at": progress["created_at"]} if progress else None)
        return result

    def list(self, status: list[str] | None = None, limit: int = 50) -> dict[str, Any]:
        args: list[Any] = []
        where = ""
        if status:
            where = "WHERE status IN (%s)" % ",".join("?" for _ in status)
            args.extend(status)
        with self.db_lock:
            rows = self.db.execute(
                f"SELECT job_id, name, status, updated_at FROM jobs {where} ORDER BY updated_at DESC LIMIT ?",
                (*args, limit),
            ).fetchall()
        return {"jobs": [dict(row) for row in rows]}

    def events(self, job_id: str, since_event_id: str | None = None, types: list[str] | None = None, limit: int = 20) -> dict[str, Any]:
        return {"events": self._event_query(job_id, types, since_event_id, limit)}

    def deliveries(self, job_id: str | None = None, status: str | None = None, limit: int = 20) -> dict[str, Any]:
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
            "delivered_at": row["delivered_at"],
            "last_error": row["last_error"],
        }

    def mark_delivery(self, delivery_id: str, status: str, error: str | None = None) -> dict[str, Any]:
        delivered_at = now_iso() if status == "delivered" else None
        with self.db_lock:
            self.db.execute(
                """
                UPDATE deliveries
                SET status=?, attempts=attempts+1, delivered_at=COALESCE(?, delivered_at), last_error=?
                WHERE delivery_id=?
                """,
                (status, delivered_at, error, delivery_id),
            )
            self.db.commit()
            row = self._row("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,))
        if not row:
            raise ValueError(f"Unknown delivery_id: {delivery_id}")
        return self._delivery_dict(row)

    def tail(self, job_id: str, stream: str = "stdout", max_bytes: int = 8192) -> dict[str, Any]:
        if stream not in {"stdout", "stderr"}:
            raise ValueError("stream must be stdout or stderr")
        row = self._row(f"SELECT {stream}_path AS path FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        path = Path(row["path"])
        size = path.stat().st_size if path.exists() else 0
        with path.open("rb") as f:
            if size > max_bytes:
                f.seek(-max_bytes, os.SEEK_END)
            content = f.read().decode(errors="replace")
        return {"job_id": job_id, "stream": stream, "truncated": size > max_bytes, "content": content}

    async def stop(self, job_id: str, signal: str = "terminate", kill_after_seconds: int = 10) -> dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(None, self.stop_sync, job_id, signal, kill_after_seconds)

    def stop_sync(self, job_id: str, signal: str = "terminate", kill_after_seconds: int = 10) -> dict[str, Any]:
        return self._stop(job_id, signal, kill_after_seconds)

    def _stop(self, job_id: str, signal: str = "terminate", kill_after_seconds: int = 10) -> dict[str, Any]:
        proc = self.processes.get(job_id)
        if not proc:
            raise ValueError(f"Job is not running in this server: {job_id}")
        with self.db_lock:
            self.db.execute(
                "UPDATE jobs SET status='cancelled', ended_at=?, updated_at=? WHERE job_id=?",
                (now_iso(), now_iso(), job_id),
            )
            self.db.commit()
        self._emit(job_id, "cancelled")
        self._kill_process(proc, force=signal == "kill")
        try:
            proc.wait(timeout=kill_after_seconds)
        except subprocess.TimeoutExpired:
            self._kill_process(proc, force=True)
            proc.wait()
        self._readers_done(job_id)
        self.processes.pop(job_id, None)
        return {"job_id": job_id, "status": "cancelled", "message": "Job stopped"}


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
async def job_start(
    command: str,
    cwd: str | None = None,
    name: str | None = None,
    env: dict[str, str] | None = None,
    timeout_seconds: int | None = None,
    notify_on: list[str] | None = None,
    wake_targets: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
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
        },
    )


@mcp.tool()
def job_status(job_id: str) -> dict[str, Any]:
    return get_client().get(f"/jobs/{job_id}/status")


@mcp.tool()
def job_list(status: list[str] | None = None, limit: int = 50) -> dict[str, Any]:
    return get_client().get("/jobs", {"status": status, "limit": limit})


@mcp.tool()
def job_events(job_id: str, since_event_id: str | None = None, types: list[str] | None = None, limit: int = 20) -> dict[str, Any]:
    return get_client().get(f"/jobs/{job_id}/events", {"since_event_id": since_event_id, "types": types, "limit": limit})


@mcp.tool()
def job_deliveries(job_id: str | None = None, status: str | None = None, limit: int = 20) -> dict[str, Any]:
    return get_client().get("/deliveries", {"job_id": job_id, "status": status, "limit": limit})


@mcp.tool()
def job_mark_delivery(delivery_id: str, status: str, error: str | None = None) -> dict[str, Any]:
    return get_client().post(f"/deliveries/{delivery_id}/mark", {"status": status, "error": error})


@mcp.tool()
def job_tail(job_id: str, stream: str = "stdout", max_bytes: int = 8192) -> dict[str, Any]:
    return get_client().get(f"/jobs/{job_id}/tail", {"stream": stream, "max_bytes": max_bytes})


@mcp.tool()
def job_wait(
    job_id: str,
    filters: list[str],
    since_event_id: str | None = None,
    timeout_seconds: int = 3600,
) -> dict[str, Any]:
    return get_client().post(
        f"/jobs/{job_id}/wait",
        {"filters": filters, "since_event_id": since_event_id, "timeout_seconds": timeout_seconds},
    )


@mcp.tool()
def job_stop(job_id: str, signal: str = "terminate", kill_after_seconds: int = 10) -> dict[str, Any]:
    return get_client().post(f"/jobs/{job_id}/stop", {"signal": signal, "kill_after_seconds": kill_after_seconds})


def main() -> None:
    mcp.run()
