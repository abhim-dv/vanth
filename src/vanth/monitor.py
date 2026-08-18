"""Console entry point for the native Go terminal monitor.

The monitor is a read-only Go binary that renders Vanth's durable SQLite/JSONL
state as a Bubble Tea TUI. The Python package bundles the host-platform binary
inside the wheel (``vanth/monitor-bin/vanth-monitor``) and this module locates
and re-executes it, so ``uv run vanth-monitor`` works without a separate Go
toolchain.

When no bundled binary is present (e.g. a source checkout before a build), the
wrapper falls back to building the monitor from the repository's Go module
into a user cache directory. That keeps development workflows working while
releases ship the real artifact.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import vanth

_BIN_DIR_NAME = "monitor-bin"
_EXE_SUFFIX = ".exe" if os.name == "nt" else ""


def _binary_name() -> str:
    return "vanth-monitor" + _EXE_SUFFIX


def bundled_binary() -> Path | None:
    """Return the host-platform monitor binary bundled in the wheel, if any."""
    candidates = [
        Path(vanth.__file__).parent / _BIN_DIR_NAME / _binary_name(),
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _dev_cache_binary() -> Path:
    """Return a cached build of the monitor for source checkouts.

    The build is cached under the user cache dir so repeated invocations do
    not recompile. It is rebuilt only if the cache entry is missing.
    """
    override = os.environ.get("VANTH_CACHE_DIR")
    cache_root = Path(override) if override else Path.home() / ".cache" / "vanth"
    binary = cache_root / "monitor" / _binary_name()
    if binary.is_file():
        return binary

    repo = _repo_root()
    go = shutil.which("go")
    if go is None:
        raise RuntimeError(
            "no bundled vanth-monitor binary found, and 'go' is not on PATH; "
            "run `uv build` to produce a wheel with the native monitor"
        )
    build_dir = repo / "cmd" / "vanth"
    if not (build_dir / "main.go").is_file():
        raise RuntimeError(f"expected Go monitor source at {build_dir}")
    binary.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [go, "build", "-o", str(binary), "."],
        cwd=str(build_dir),
        check=True,
    )
    return binary


def find_monitor_binary() -> Path:
    """Return the path to an executable monitor binary for this platform."""
    bundled = bundled_binary()
    if bundled is not None:
        return bundled
    return _dev_cache_binary()


def main() -> int:
    binary = find_monitor_binary()
    try:
        return subprocess.call([str(binary), *sys.argv[1:]])
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
