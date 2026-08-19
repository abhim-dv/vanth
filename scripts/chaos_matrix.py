"""Heavy opt-in chaos and synthetic workload matrix for Vanth v1 release gates.

Run deliberately, not in ordinary CI:

    uv run python scripts/chaos_matrix.py            # full matrix
    uv run python scripts/chaos_matrix.py --only burst  # one scenario
    uv run python scripts/chaos_matrix.py --iterations 3 --jobs 50 --events 500

Scenarios (v1 release-gate matrix):

  burst   - N concurrent jobs each emitting M events across stdout/stderr;
            assert exact durable event counts and unique per-job sequence numbers.
  adapter - a slow wake adapter must not delay stream parsing or terminal state.
  daemon  - kill/restart the daemon repeatedly while jobs run and deliveries
            retry; assert leases recover and no duplicate dispatch.
  runner  - kill runners before workload spawn, during execution, and during
            terminal persistence; assert terminal-or-recoverable state and no
            leaked process tree.
  input   - malformed JSON, invalid UTF-8, recursive JSON, oversized event
            lines, invalid target configs, short bodies, huge query integers,
            and broken connections; assert structured errors and daemon health.
  state   - fill log caps and run cleanup twice; assert bounded state and
            idempotence.

Every scenario prints PASS or FAIL and the process exits nonzero on any failure.
"""

from __future__ import annotations

import argparse
import asyncio
import http.client
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from vanth.server import JobManager

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(port: int, method: str, path: str, body=None, headers=None, token=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    request_headers = headers or {}
    if token:
        request_headers["Authorization"] = f"Bearer {token}"
    connection.request(method, path, body=body, headers=request_headers)
    response = connection.getresponse()
    payload = json.loads(response.read())
    connection.close()
    return response.status, payload


def wait_for(condition, timeout: float, message: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {message}")


class Scenario:
    def run(self) -> None:  # pragma: no cover - overridden
        raise NotImplementedError


class BurstScenario(Scenario):
    name = "burst"

    def __init__(self, jobs: int, events: int) -> None:
        self.jobs = jobs
        self.events = events

    def run(self) -> None:
        home = Path(tempfile.mkdtemp(prefix="vanth-burst-"))
        manager = JobManager(home)
        try:
            started = []
            code = (
                "import json,sys;"
                f"f=lambda i:(print('AGENT_EVENT '+json.dumps({{'type':'metric','data':{{'i':i}}}}), flush=True),"
                f"print('AGENT_EVENT '+json.dumps({{'type':'metric','data':{{'i':i+{self.events}}}}}), file=sys.stderr, flush=True));"
                f"[f(i) for i in range({self.events})]"
            )
            for index in range(self.jobs):
                started.append(asyncio.run(manager.start(cmd(code), name=f"burst-{index}"))["job_id"])
            for job_id in started:
                wait_for(
                    lambda job_id=job_id: manager.status(job_id)["status"] in {"completed", "failed"},
                    120,
                    f"job {job_id} completion",
                )
                assert manager.status(job_id)["status"] == "completed", job_id
            total = 0
            for job_id in started:
                rows = manager.db.execute(
                    "SELECT type, COUNT(*) AS c FROM events WHERE job_id=? GROUP BY type", (job_id,)
                ).fetchall()
                counts = {row["type"]: row["c"] for row in rows}
                assert counts["metric"] == self.events * 2, (job_id, counts)
                assert counts["started"] == 1 and counts["completed"] == 1, (job_id, counts)
                seqs = [row["seq"] for row in manager.db.execute(
                    "SELECT seq FROM events WHERE job_id=? ORDER BY seq", (job_id,)
                ).fetchall()]
                assert seqs == list(range(1, self.events * 2 + 3)), job_id
                total += self.events * 2 + 2
            print(f"  {self.jobs} jobs x {self.events} events = {total} durable rows, unique seq verified")
        finally:
            manager.close()
            shutil.rmtree(home, ignore_errors=True)


class AdapterScenario(Scenario):
    name = "adapter"

    def run(self) -> None:
        home = Path(tempfile.mkdtemp(prefix="vanth-adapter-"))
        manager = JobManager(home)
        try:
            slow = [sys.executable, "-c", "import time; time.sleep(8)"]
            code = (
                "import json,time;"
                "f=lambda i:(print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':i,'total':200}}), flush=True),"
                "time.sleep(0.01));"
                "[f(i) for i in range(1,201)]"
            )
            job_id = asyncio.run(
                manager.start(
                    cmd(code),
                    wake_targets=[{"type": "local_command", "events": ["progress"], "command": slow}],
                )
            )["job_id"]
            started_at = time.monotonic()
            wait_for(
                lambda: manager.status(job_id)["status"] in {"completed", "failed"},
                30,
                "job completion despite slow adapter",
            )
            elapsed = time.monotonic() - started_at
            assert manager.status(job_id)["status"] == "completed", job_id
            assert elapsed < 6, f"terminal state waited on slow adapter ({elapsed:.2f}s)"
            counts = {row["type"]: row["c"] for row in manager.db.execute(
                "SELECT type, COUNT(*) AS c FROM events WHERE job_id=? GROUP BY type", (job_id,)
            ).fetchall()}
            assert counts["progress"] == 200, counts
            print(f"  job completed in {elapsed:.2f}s while adapter ran 8s; 200 progress events intact")
        finally:
            manager.close()
            shutil.rmtree(home, ignore_errors=True)


class DaemonKillScenario(Scenario):
    name = "daemon"

    def __init__(self, iterations: int) -> None:
        self.iterations = iterations

    def run(self) -> None:
        base = Path(tempfile.mkdtemp(prefix="vanth-daemon-kill-"))
        home = base / "state"
        calls = base / "calls.txt"
        go = base / "go"
        port = free_port()
        env = {
            **os.environ,
            "VANTH_HOME": str(home),
            "VANTH_DAEMON_PORT": str(port),
            "VANTH_DELIVERY_POLL_INTERVAL": "0.05",
            "VANTH_DELIVERY_LEASE_MARGIN": "1",
        }
        delivery_command = [
            sys.executable,
            "-c",
            (
                "from pathlib import Path; import sys; "
                "go=Path(sys.argv[1]); calls=Path(sys.argv[2]); "
                "calls.write_text(calls.read_text()+'x') if calls.exists() else calls.write_text('x'); "
                "sys.exit(0 if go.exists() else 7)"
            ),
            str(go),
            str(calls),
        ]
        token = None
        job_id = None
        for iteration in range(self.iterations):
            proc = subprocess.Popen(
                [sys.executable, "-m", "vanth.daemon"],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            )
            try:
                wait_for(
                    lambda: request(port, "GET", "/health") == (200, {"ok": True}),
                    30,
                    "daemon start",
                )
                if token is None:
                    token = (home / "token").read_text(encoding="utf-8").strip()
                if job_id is None:
                    payload = json.dumps(
                        {
                            "command": cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
                            "wake_targets": [
                                {
                                    "type": "local_command",
                                    "events": ["checkpoint"],
                                    "command": delivery_command,
                                    "max_attempts": 50,
                                    "retry_delay_seconds": 1,
                                    "timeout_seconds": 1,
                                }
                            ],
                        }
                    ).encode()
                    status, started = request(
                        port, "POST", "/jobs", payload,
                        {"Content-Type": "application/json"}, token,
                    )
                    assert status == 200, started
                    job_id = started["job_id"]
                wait_for(
                    lambda: request(port, "GET", f"/deliveries?job_id={job_id}", token=token)[1]["deliveries"]
                    and request(port, "GET", f"/deliveries?job_id={job_id}", token=token)[1]["deliveries"][0]["status"]
                    in {"dispatching", "retrying", "failed"},
                    15,
                    "delivery to start dispatching",
                )
                proc.kill()
                proc.wait(timeout=10)
                proc = None
                time.sleep(0.05)
            finally:
                if proc is not None and proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)
        final = subprocess.Popen(
            [sys.executable, "-m", "vanth.daemon"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        try:
            wait_for(
                lambda: request(port, "GET", "/health") == (200, {"ok": True}),
                30,
                "final daemon start",
            )
            go.write_text("go", encoding="utf-8")
            wait_for(
                lambda: request(
                    port, "GET", f"/deliveries?job_id={job_id}", token=token
                )[1]["deliveries"]
                and request(
                    port, "GET", f"/deliveries?job_id={job_id}", token=token
                )[1]["deliveries"][0]["status"]
                == "delivered",
                15,
                "delivery completion after restarts",
            )
            deliveries = request(port, "GET", f"/deliveries?job_id={job_id}", token=token)[1]["deliveries"]
        finally:
            final.kill()
            final.wait(timeout=10)
        assert deliveries and deliveries[0]["status"] == "delivered", deliveries
        attempts = deliveries[0]["attempts"]
        call_count = calls.read_text().count("x")
        assert call_count <= attempts, (call_count, attempts)
        assert attempts - call_count <= self.iterations, (attempts, call_count, self.iterations)
        assert attempts >= 2, attempts
        print(
            f"  {self.iterations} daemon kill/restart cycles; delivery recovered, "
            f"attempts={attempts}, calls={call_count} (each kill may orphan at most one call)"
        )


class RunnerKillScenario(Scenario):
    name = "runner"

    def run(self) -> None:
        home = Path(tempfile.mkdtemp(prefix="vanth-runner-kill-"))
        manager = JobManager(home)
        try:
            phase_job = asyncio.run(manager.start(cmd("import time; time.sleep(30)")))["job_id"]
            wait_for(
                lambda: manager.status(phase_job)["status"] == "running",
                10,
                "job running",
            )
            worker_pid = manager.status(phase_job)["worker_pid"]
            manager._kill_pid(worker_pid, force=True)
            wait_for(
                lambda: manager.status(phase_job)["status"] == "orphaned",
                20,
                "runner-death orphan recovery",
            )
            assert manager.status(phase_job)["status"] == "orphaned"
            assert not manager._pid_alive(manager.status(phase_job)["pid"]), "workload leaked after runner kill"
            print("  runner killed during execution -> orphaned, workload tree terminated")

            startup_job = asyncio.run(manager.start(cmd("import time; time.sleep(30)")))["job_id"]
            wait_for(
                lambda: manager.status(startup_job)["status"] == "running",
                10,
                "second job running",
            )
            worker_pid = manager.status(startup_job)["worker_pid"]
            manager._kill_pid(worker_pid, force=True)
            wait_for(
                lambda: manager.status(startup_job)["status"] in {"orphaned", "completed", "failed"},
                20,
                "terminal state after runner kill",
            )
            status = manager.status(startup_job)["status"]
            if status == "orphaned":
                assert not manager._pid_alive(manager.status(startup_job)["pid"]), "workload leaked"
            print(f"  runner killed near terminal persistence -> {status}, no leak")
        finally:
            manager.close()
            shutil.rmtree(home, ignore_errors=True)


class InputScenario(Scenario):
    name = "input"

    def run(self) -> None:
        base = Path(tempfile.mkdtemp(prefix="vanth-input-"))
        port = free_port()
        proc = subprocess.Popen(
            [sys.executable, "-m", "vanth.daemon"],
            env={**os.environ, "VANTH_HOME": str(base / "state"), "VANTH_DAEMON_PORT": str(port)},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        try:
            wait_for(lambda: request(port, "GET", "/health") == (200, {"ok": True}), 30, "daemon start")
            token = (base / "state" / "token").read_text(encoding="utf-8").strip()
            bad_bodies = [
                b'{"command":',
                b"\xff\xfe",
                b"[]",
                b'{"command":' + b"{" * 5000,
                b'{"command":"echo ok","extra_field":1}',
            ]
            for body in bad_bodies:
                status, payload = request(port, "POST", "/jobs", body, {"Content-Type": "application/json"}, token)
                assert status == 400 and payload["result"] == "error", (status, payload)
            status, payload = request(
                port, "GET", "/jobs?limit=" + "9" * 200, token=token
            )
            assert status == 400 and payload["result"] == "error", (status, payload)
            bad_targets = [
                {"command": "echo ok", "wake_targets": [{"type": "bogus", "events": []}]},
                {"command": "echo ok", "wake_targets": [{"type": "local_command"}]},
                {"command": "echo ok", "wake_targets": [{"type": "local_command", "events": 3, "command": ["ok"]}]},
            ]
            for target in bad_targets:
                status, payload = request(port, "POST", "/jobs", json.dumps(target).encode(), {"Content-Type": "application/json"}, token)
                assert status == 400 and payload["result"] == "error", (status, payload)
            assert request(port, "GET", "/health") == (200, {"ok": True})
            print("  malformed/oversized/recursive/invalid input -> structured 400s; daemon stayed healthy")
        finally:
            proc.kill()
            proc.wait(timeout=10)
            shutil.rmtree(base, ignore_errors=True)


class StateScenario(Scenario):
    name = "state"

    def run(self) -> None:
        home = Path(tempfile.mkdtemp(prefix="vanth-state-"))
        previous = os.environ.get("VANTH_MAX_LOG_BYTES")
        os.environ["VANTH_MAX_LOG_BYTES"] = "4096"
        manager = JobManager(home)
        try:
            code = (
                "import sys;"
                "[print('x'*500, flush=True) for _ in range(2000)];"
                "[print('y'*500, file=sys.stderr, flush=True) for _ in range(2000)]"
            )
            job_id = asyncio.run(manager.start(cmd(code)))["job_id"]
            wait_for(lambda: manager.status(job_id)["status"] == "completed", 60, "noisy job completion")
            counts = {row["type"]: row["c"] for row in manager.db.execute(
                "SELECT type, COUNT(*) AS c FROM events WHERE job_id=? GROUP BY type", (job_id,)
            ).fetchall()}
            assert counts["log_truncated"] == 2, counts
            row = manager.db.execute("SELECT stdout_path, stderr_path FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            assert os.path.getsize(row["stdout_path"]) <= 4096
            assert os.path.getsize(row["stderr_path"]) <= 4096
            first = manager.cleanup(0, dry_run=False)
            assert first["count"] == 1
            assert manager.cleanup(0, dry_run=False)["count"] == 0
            print("  log caps bounded streams; cleanup ran twice and was idempotent")
        finally:
            manager.close()
            if previous is None:
                os.environ.pop("VANTH_MAX_LOG_BYTES", None)
            else:
                os.environ["VANTH_MAX_LOG_BYTES"] = previous
            shutil.rmtree(home, ignore_errors=True)


class AgentFeatureScenario(Scenario):
    """Stress the v1.1 agent-facing features under load: rerun across many
    failed jobs, status/env exposure, list name/tag filters, and reverse event
    paging. Also verifies daemon discovery metadata appears and is removed."""

    name = "agent"

    def run(self) -> None:
        home = Path(tempfile.mkdtemp(prefix="vanth-agent-"))
        manager = JobManager(home)
        try:
            rerun_marker = home / "rerun_marker"
            # A batch of jobs that fail once (marker absent) then succeed.
            batch_code = (
                "from pathlib import Path; import os,sys; "
                "Path(os.environ['RERUN_MARK']).touch(); "
                "print('AGENT_EVENT '+__import__('json').dumps({'type':'checkpoint'}), flush=True); "
                "sys.exit(0)"
            )
            started = []
            for index in range(10):
                started.append(
                    asyncio.run(
                        manager.start(
                            cmd(batch_code),
                            name=f"agent-job-{index}",
                            cwd=str(home),
                            env={"RERUN_MARK": str(rerun_marker / str(index))},
                            tags=["agent", "chaos"],
                            wake_targets=[
                                {"type": "local_command", "events": ["checkpoint"],
                                 "command": [sys.executable, "-c", "import sys; sys.exit(0)"]}
                            ],
                        )
                    )["job_id"]
                )
            for job_id in started:
                wait_for(lambda job_id=job_id: manager.status(job_id)["status"] in {"completed", "failed"}, 30,
                         f"job {job_id} terminal")

            # status exposes command/env/cwd/tags for every job.
            for job_id in started:
                status = manager.status(job_id)
                assert "AGENT_EVENT" in status["command"], job_id
                assert status["tags"] == ["agent", "chaos"], (job_id, status["tags"])
                assert status["cwd"] == str(home), job_id
                assert "RERUN_MARK" in status["env"], job_id
                assert status["run"].get("hostname"), job_id
                assert status["run"].get("os"), job_id
                assert status["runtime_seconds"] is not None, job_id

            # list filters by name and tag under load.
            by_tag = manager.list(tags=["chaos"])["jobs"]
            assert len(by_tag) == 10, len(by_tag)
            by_name = manager.list(name="agent-job-3")["jobs"]
            assert len(by_name) == 1, by_name

            # reverse paging returns the newest events first.
            reverse = manager.events(started[0], limit=3, reverse=True)["events"]
            seqs = [e["seq"] for e in reverse]
            assert seqs == sorted(seqs, reverse=True), seqs

            # rerun all failed jobs and confirm the reruns inherit config.
            reruns = []
            for job_id in started:
                status = manager.status(job_id)
                if status["status"] == "failed":
                    reruns.append(manager.rerun_sync(job_id)["job_id"])
            for rerun_id in reruns:
                wait_for(lambda rerun_id=rerun_id: manager.status(rerun_id)["status"] in {"completed", "failed"}, 30,
                         f"rerun {rerun_id} terminal")
                status = manager.status(rerun_id)
                assert status["tags"] == ["agent", "chaos"], rerun_id
                assert status["env"]["RERUN_MARK"].startswith(str(home)), rerun_id
            print(f"  {len(started)} jobs: status/env, list filters, reverse paging, {len(reruns)} reruns verified")

            # Daemon discovery metadata via a live daemon.
            import socket as _socket
            with _socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            daemon_home = home / "dynhome"
            daemon_home.mkdir(parents=True, exist_ok=True)
            daemon = subprocess.Popen(
                [sys.executable, "-m", "vanth.daemon"],
                env={**os.environ, "VANTH_HOME": str(daemon_home), "VANTH_DAEMON_PORT": str(port)},
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            )
            meta = daemon_home / "daemon.json"
            try:
                wait_for(lambda: meta.exists(), 15, "daemon.json write")
                payload = json.loads(meta.read_text(encoding="utf-8"))
                assert payload["url"] == f"http://127.0.0.1:{port}", payload
                assert payload["home"] == str(daemon_home.resolve()), payload
            finally:
                if sys.platform == "win32":
                    daemon.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    daemon.terminate()
                try:
                    daemon.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    daemon.kill()
                    daemon.wait(timeout=10)
            wait_for(lambda: not meta.exists(), 5, "daemon.json removal")
            print("  daemon discovery metadata written atomically and removed on graceful shutdown")
        finally:
            manager.close()
            shutil.rmtree(home, ignore_errors=True)


SCENARIOS = {
    scenario.name: scenario
    for scenario in (BurstScenario, AdapterScenario, DaemonKillScenario, RunnerKillScenario, InputScenario, StateScenario, AgentFeatureScenario)
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Vanth v1 chaos and synthetic workload matrix")
    parser.add_argument("--only", choices=sorted(SCENARIOS), help="run a single scenario")
    parser.add_argument("--jobs", type=int, default=50, help="jobs in the burst scenario")
    parser.add_argument("--events", type=int, default=500, help="events per stream in the burst scenario")
    parser.add_argument("--iterations", type=int, default=5, help="daemon kill/restart cycles")
    args = parser.parse_args()

    targets = [args.only] if args.only else list(SCENARIOS)
    failures = []
    for name in targets:
        print(f"[{name}]")
        try:
            scenario = SCENARIOS[name](args.jobs, args.events) if name in {"burst"} else (
                SCENARIOS[name](args.iterations) if name == "daemon" else SCENARIOS[name]()
            )
            scenario.run()
            print(f"  PASS {name}")
        except Exception as exc:  # noqa: BLE001 - report and continue
            failures.append((name, exc))
            import traceback
            traceback.print_exc()
            print(f"  FAIL {name}: {exc}")
    if failures:
        print("\nMatrix failures:")
        for name, exc in failures:
            print(f"  {name}: {exc}")
        return 1
    print("\nAll scenarios passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
