from __future__ import annotations

import json
import secrets
import os
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .paths import canonical_home


def _default_daemon_url() -> str:
    host = os.environ.get("VANTH_DAEMON_HOST", "127.0.0.1")
    try:
        port = int(os.environ.get("VANTH_DAEMON_PORT", "8765"))
    except ValueError:
        port = 8765
    return f"http://{host}:{port}"


def auth_token_path(home: str | os.PathLike[str] | None = None) -> str:
    return os.fspath(canonical_home(home) / "token")


def ensure_auth_token(home: str | os.PathLike[str] | None = None) -> str:
    path = auth_token_path(home)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "x", encoding="utf-8") as handle:
            handle.write(secrets.token_urlsafe(32))
    except FileExistsError:
        pass
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
    with open(path, encoding="utf-8") as handle:
        token = handle.read().strip()
    if not token:
        raise RuntimeError("Vanth authentication token is empty")
    return token


class VanthClient:
    def __init__(self, url: str | None = None, home: str | os.PathLike[str] | None = None) -> None:
        self.home = canonical_home(home)
        self.url = self._resolve_url(url).rstrip("/")
        self.token = ensure_auth_token(self.home)

    def _resolve_url(self, url: str | None) -> str:
        if url:
            return url
        env_url = os.environ.get("VANTH_DAEMON_URL")
        if env_url:
            return env_url
        discovered = self._discover_url()
        if discovered:
            return discovered
        host = os.environ.get("VANTH_DAEMON_HOST", "127.0.0.1")
        try:
            port = int(os.environ.get("VANTH_DAEMON_PORT", "8765"))
        except ValueError:
            port = 8765
        return f"http://{host}:{port}"

    def _discover_url(self) -> str | None:
        try:
            payload = json.loads((self.home / "daemon.json").read_text(encoding="utf-8"))
            return payload.get("url")
        except (OSError, ValueError):
            return None

    def _ready(self) -> bool:
        payload = self.get("/doctor")
        return (
            isinstance(payload, dict)
            and payload.get("result") != "error"
            and payload.get("schema_version") is not None
            and Path(str(payload.get("home", ""))).expanduser().resolve() == self.home
        )

    def ensure(self) -> None:
        try:
            if self._ready():
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
                if self._ready():
                    return
            except Exception:
                pass
            time.sleep(0.1)
        raise RuntimeError("vanthd did not start")

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = self.url + path
        if params:
            clean = {key: value for key, value in params.items() if value is not None}
            if clean:
                url += "?" + urllib.parse.urlencode(clean, doseq=True)
        try:
            request = urllib.request.Request(url, headers={"Authorization": f"Bearer {self.token}"})
            with urllib.request.urlopen(request, timeout=None) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            return json.loads(exc.read().decode())

    def post(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = json.dumps(payload or {}).encode()
        request = urllib.request.Request(
            self.url + path,
            data=data,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=None) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            return json.loads(exc.read().decode())
