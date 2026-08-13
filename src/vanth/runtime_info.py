"""Best-effort run-overview metadata capture, mirroring W&B's overview tab.

Gathered at job start so an agent can answer "what is this job?" — author,
host/OS/Python, git state, and system hardware. Every lookup is optional and
must never raise: a missing field is simply omitted. Machine-wide values (GPU,
CPU) are captured once and cached.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
from typing import Any

_gpu_cache: list[dict[str, str]] | None | bool = False
_cpu_cache: int | None = None
_git_cache: dict[str, tuple[str | None, str | None, str | None] | None] = {}


def _git(cwd: str | None, *args: str) -> str | None:
    if not cwd:
        return None
    try:
        result = subprocess.run(
            ["git", "-C", cwd, *args],
            capture_output=True, text=True, timeout=2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _git_state(cwd: str | None) -> dict[str, str] | None:
    """Return git repo/branch/commit for a directory, cached per cwd."""
    if not cwd:
        return None
    cached = _git_cache.get(cwd)
    if cwd in _git_cache:
        if cached is None:
            return None
        return dict(cached)
    repo = _git(cwd, "remote", "get-url", "origin")
    branch = _git(cwd, "rev-parse", "--abbrev-ref", "HEAD")
    commit = _git(cwd, "rev-parse", "HEAD")
    state: dict[str, str] = {}
    if branch:
        state["branch"] = branch
    if commit:
        state["commit"] = commit
    if repo:
        state["repository"] = repo
    _git_cache[cwd] = state or None
    return state or None


def _gpu_info() -> list[dict[str, str]] | None:
    global _gpu_cache
    if _gpu_cache is not False:
        return _gpu_cache
    smi = shutil.which("nvidia-smi")
    if smi:
        try:
            result = subprocess.run(
                [smi, "--query-gpu=name,driver_version", "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=3,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass
        else:
            if result.returncode == 0:
                gpus = []
                for line in result.stdout.splitlines():
                    parts = [p.strip() for p in line.split(",")]
                    if parts and parts[0]:
                        gpus.append({"name": parts[0], "driver": parts[1] if len(parts) > 1 else ""})
                _gpu_cache = gpus or None
                return _gpu_cache
    try:
        import torch  # type: ignore[import-not-found]
    except ImportError:
        _gpu_cache = None
        return None
    try:
        count = torch.cuda.device_count()
    except Exception:
        _gpu_cache = None
        return None
    if count <= 0:
        _gpu_cache = None
        return None
    gpus = []
    for index in range(count):
        try:
            gpus.append({"name": torch.cuda.get_device_name(index)})
        except Exception:
            gpus.append({"name": f"cuda:{index}"})
    _gpu_cache = gpus or None
    return _gpu_cache


def capture_run_metadata(cwd: str | None = None, notes: str | None = None) -> dict[str, Any]:
    """Capture the run-overview fields W&B shows in its overview tab."""
    global _cpu_cache
    if _cpu_cache is None:
        _cpu_cache = os.cpu_count() or None
    git_state = _git_state(cwd)
    info: dict[str, Any] = {
        "author": os.environ.get("USERNAME") or os.environ.get("USER") or None,
        "hostname": platform.node() or None,
        "os": platform.system() or None,
        "os_release": platform.release() or None,
        "machine": platform.machine() or None,
        "python_version": platform.python_version() or None,
        "python_executable": sys.executable or None,
        "cpu_count": _cpu_cache,
        "gpus": _gpu_info(),
        "cwd": cwd,
    }
    if git_state:
        info["git"] = git_state
    if notes:
        info["notes"] = notes
    return info


def serialize_run_metadata(info: dict[str, Any]) -> str:
    return json.dumps(info, separators=(",", ":"), ensure_ascii=False)
