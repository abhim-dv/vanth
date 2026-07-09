from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


DEFAULT_DAEMON_URL = "http://127.0.0.1:8765"


class VanthClient:
    def __init__(self, url: str | None = None) -> None:
        self.url = (url or os.environ.get("VANTH_DAEMON_URL") or DEFAULT_DAEMON_URL).rstrip("/")

    def ensure(self) -> None:
        try:
            self.get("/health")
            return
        except Exception:
            pass
        subprocess.Popen(
            [sys.executable, "-m", "vanth.daemon"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=os.environ.copy(),
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if sys.platform == "win32" else 0,
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                self.get("/health")
                return
            except Exception:
                time.sleep(0.1)
        raise RuntimeError("vanthd did not start")

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = self.url + path
        if params:
            clean = {key: value for key, value in params.items() if value is not None}
            if clean:
                url += "?" + urllib.parse.urlencode(clean, doseq=True)
        try:
            with urllib.request.urlopen(url, timeout=None) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            return json.loads(exc.read().decode())

    def post(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = json.dumps(payload or {}).encode()
        request = urllib.request.Request(
            self.url + path,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=None) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            return json.loads(exc.read().decode())
