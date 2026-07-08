from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

EVENT_PREFIX = "AGENT_EVENT "
TERMINAL = {"completed", "failed", "cancelled", "timeout"}


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
        self.home = Path(home or os.environ.get("AGENT_BG_HOME") or Path.home() / ".vanth")
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
            """
        )
        self.db.execute(
            "UPDATE jobs SET status='orphaned', updated_at=? WHERE status='running'",
            (now_iso(),),
        )
        self.db.commit()
        self.processes: dict[str, asyncio.subprocess.Process] = {}
        self.reader_tasks: dict[str, list[asyncio.Task[None]]] = {}
        self.conditions: dict[str, asyncio.Condition] = {}
        self.lock = asyncio.Lock()

    def close(self) -> None:
        self.db.close()

    def _condition(self, job_id: str) -> asyncio.Condition:
        self.conditions.setdefault(job_id, asyncio.Condition())
        return self.conditions[job_id]

    def _row(self, sql: str, args: tuple[Any, ...]) -> sqlite3.Row | None:
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

    async def _emit(
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
        async with self.lock:
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
                    json.dumps(payload["data"], separators=(",", ":")),
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
        async with self._condition(job_id):
            self._condition(job_id).notify_all()
        return event

    async def start(
        self,
        command: str,
        cwd: str | None = None,
        name: str | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
        notify_on: list[str] | None = None,
    ) -> dict[str, Any]:
        job_id = "job_" + uuid.uuid4().hex[:12]
        stdout_path = self.logs / f"{job_id}.stdout.log"
        stderr_path = self.logs / f"{job_id}.stderr.log"
        events_path = self.events_dir / f"{job_id}.jsonl"
        created_at = now_iso()
        proc_env = os.environ.copy()
        if env:
            proc_env.update(env)
        proc = await asyncio.create_subprocess_shell(
            command,
            cwd=cwd,
            env=proc_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.processes[job_id] = proc
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
        await self._emit(job_id, "started")
        self.reader_tasks[job_id] = [
            asyncio.create_task(self._read_stream(job_id, proc.stdout, stdout_path, "stdout")),
            asyncio.create_task(self._read_stream(job_id, proc.stderr, stderr_path, "stderr")),
        ]
        asyncio.create_task(self._watch(job_id, proc, timeout_seconds))
        return {
            "job_id": job_id,
            "status": "running",
            "pid": proc.pid,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "events_path": str(events_path),
            "message": "Job started",
        }

    async def _read_stream(
        self,
        job_id: str,
        stream: asyncio.StreamReader | None,
        path: Path,
        source: str,
    ) -> None:
        if stream is None:
            return
        with path.open("ab") as f:
            while line := await stream.readline():
                f.write(line)
                f.flush()
                payload = parse_agent_event_line(line.decode(errors="replace").rstrip("\r\n"))
                if payload:
                    event = normalize_event_payload(payload)
                    await self._emit(
                        job_id,
                        event["type"],
                        message=event["message"],
                        data=event["data"],
                        level=event["level"],
                        source=source,
                    )

    async def _watch(self, job_id: str, proc: asyncio.subprocess.Process, timeout_seconds: int | None) -> None:
        try:
            exit_code = await asyncio.wait_for(proc.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            proc.kill()
            exit_code = await proc.wait()
            await self._readers_done(job_id)
            await asyncio.sleep(0)
            await self._finish(job_id, "timeout", exit_code)
            return
        status = self._row("SELECT status FROM jobs WHERE job_id=?", (job_id,))["status"]
        if status == "cancelled":
            return
        await self._readers_done(job_id)
        await asyncio.sleep(0)
        await self._finish(job_id, "completed" if exit_code == 0 else "failed", exit_code)

    async def _readers_done(self, job_id: str) -> None:
        tasks = self.reader_tasks.pop(job_id, [])
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _finish(self, job_id: str, status: str, exit_code: int | None = None) -> None:
        self.db.execute(
            "UPDATE jobs SET status=?, exit_code=?, ended_at=?, updated_at=? WHERE job_id=?",
            (status, exit_code, now_iso(), now_iso(), job_id),
        )
        self.db.commit()
        data = {"exit_code": exit_code} if exit_code is not None else {}
        await self._emit(job_id, status, data=data)
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
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            events = self._event_query(job_id, filters, since_event_id, 1)
            if events:
                return {"result": "event", "job_id": job_id, "status": self.status(job_id)["status"], "event": events[0]}
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return {"result": "timeout", "job_id": job_id, "status": self.status(job_id)["status"], "message": "No matching event before timeout"}
            async with self._condition(job_id):
                try:
                    await asyncio.wait_for(self._condition(job_id).wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    return {"result": "timeout", "job_id": job_id, "status": self.status(job_id)["status"], "message": "No matching event before timeout"}

    def status(self, job_id: str) -> dict[str, Any]:
        row = self._row("SELECT * FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
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
        rows = self.db.execute(
            f"SELECT job_id, name, status, updated_at FROM jobs {where} ORDER BY updated_at DESC LIMIT ?",
            (*args, limit),
        ).fetchall()
        return {"jobs": [dict(row) for row in rows]}

    def events(self, job_id: str, since_event_id: str | None = None, types: list[str] | None = None, limit: int = 20) -> dict[str, Any]:
        return {"events": self._event_query(job_id, types, since_event_id, limit)}

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
        proc = self.processes.get(job_id)
        if not proc:
            raise ValueError(f"Job is not running in this server: {job_id}")
        self.db.execute(
            "UPDATE jobs SET status='cancelled', ended_at=?, updated_at=? WHERE job_id=?",
            (now_iso(), now_iso(), job_id),
        )
        self.db.commit()
        await self._emit(job_id, "cancelled")
        proc.kill() if signal == "kill" else proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=kill_after_seconds)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
        await self._readers_done(job_id)
        await asyncio.sleep(0)
        self.processes.pop(job_id, None)
        return {"job_id": job_id, "status": "cancelled", "message": "Job stopped"}


manager: JobManager | None = None
mcp = FastMCP("vanth")


def get_manager() -> JobManager:
    global manager
    if manager is None:
        manager = JobManager()
    return manager


@mcp.tool()
async def job_start(
    command: str,
    cwd: str | None = None,
    name: str | None = None,
    env: dict[str, str] | None = None,
    timeout_seconds: int | None = None,
    notify_on: list[str] | None = None,
) -> dict[str, Any]:
    return await get_manager().start(command, cwd, name, env, timeout_seconds, notify_on)


@mcp.tool()
def job_status(job_id: str) -> dict[str, Any]:
    return get_manager().status(job_id)


@mcp.tool()
def job_list(status: list[str] | None = None, limit: int = 50) -> dict[str, Any]:
    return get_manager().list(status, limit)


@mcp.tool()
def job_events(job_id: str, since_event_id: str | None = None, types: list[str] | None = None, limit: int = 20) -> dict[str, Any]:
    return get_manager().events(job_id, since_event_id, types, limit)


@mcp.tool()
def job_tail(job_id: str, stream: str = "stdout", max_bytes: int = 8192) -> dict[str, Any]:
    return get_manager().tail(job_id, stream, max_bytes)


@mcp.tool()
async def job_wait(
    job_id: str,
    filters: list[str],
    since_event_id: str | None = None,
    timeout_seconds: int = 3600,
) -> dict[str, Any]:
    return await get_manager().wait(job_id, filters, since_event_id, timeout_seconds)


@mcp.tool()
async def job_stop(job_id: str, signal: str = "terminate", kill_after_seconds: int = 10) -> dict[str, Any]:
    return await get_manager().stop(job_id, signal, kill_after_seconds)


def main() -> None:
    mcp.run()
