"""Start the Vanth daemon and a few tracking jobs for a monitor demo.

Usage: uv run python scripts/demo_jobs.py
Creates/uses VANTH_HOME (default .vanth-demo under the repo), starts the
daemon if not running, and starts:
  - job_track: a 60s job emitting progress + loss/acc metrics every 0.5s
  - job_done:  a short job that completes with a checkpoint
  - job_fail:  a job that fails immediately
Prints the home and daemon URL.
"""
import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HOME = Path(os.environ.get("VANTH_HOME", str(REPO / ".vanth-demo"))).resolve()
PORT = 8765


def cmd(code: str) -> str:
    return subprocess.list2cmdline([sys.executable, "-c", code])


def request(method, path, body=None, token=None):
    conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=10)
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        headers["Content-Type"] = "application/json"
    conn.request(method, path, body=body, headers=headers)
    resp = conn.getresponse()
    payload = json.loads(resp.read())
    conn.close()
    return resp.status, payload


def main():
    HOME.mkdir(parents=True, exist_ok=True)
    token_path = HOME / "token"

    def home_ready():
        try:
            status, payload = request("GET", "/doctor", token=token_path.read_text(encoding="utf-8").strip() if token_path.exists() else None)
            return (
                status == 200
                and payload.get("result") != "error"
                and Path(str(payload.get("home"))).resolve() == HOME
            )
        except OSError:
            return False

    if not home_ready():
        subprocess.Popen(
            [sys.executable, "-m", "vanth.daemon"],
            env={**os.environ, "VANTH_HOME": str(HOME), "VANTH_DAEMON_PORT": str(PORT)},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and not home_ready():
            time.sleep(0.2)
        if not home_ready():
            raise RuntimeError(f"daemon did not become ready for {HOME}")

    token = token_path.read_text(encoding="utf-8").strip()
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    track_code = (
        "import json,time;"
        "steps=120;"
        "f=lambda i:(print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':i,'total':steps,'unit':'step','stage':'train'}}), flush=True),"
        "print('AGENT_EVENT '+json.dumps({'type':'metric','data':{'_step':i,'loss':round(0.9*0.99**i,5),'acc':round(1-0.98**i,5)}}), flush=True),"
        "time.sleep(0.5));"
        "[f(i) for i in range(1, steps+1)]"
    )
    done_code = (
        "import json,time;"
        "print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':5,'total':5,'percent':100}}), flush=True);"
        "time.sleep(0.5);"
        "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'task finished ok'}), flush=True)"
    )
    fail_code = "import sys; print('boom'); sys.exit(3)"

    for name, code, tags in [
        ("training run", track_code, ["gpu", "train"]),
        ("quick task", done_code, ["demo"]),
        ("failing task", fail_code, ["demo"]),
    ]:
        payload = json.dumps({"command": cmd(code), "name": name, "tags": tags}).encode()
        status, started = request("POST", "/jobs", payload, token=token)
        print(f"{name}: {started.get('job_id')} status={status}")

    print("VANTH_HOME=" + str(HOME))
    print("daemon_url=http://127.0.0.1:" + str(PORT))


if __name__ == "__main__":
    main()
