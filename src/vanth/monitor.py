"""Console entry point for the native Go terminal monitor.

The monitor is a read-only Go binary that renders Vanth's durable SQLite/JSONL
state as a Bubble Tea TUI. Platform wheels bundle the host-platform binary
inside the wheel (``vanth/monitor-bin/vanth-monitor``); this module locates and
re-executes it, so ``vanth-monitor`` works without a Go toolchain.

If the bundled binary is missing (source checkout or sdist install), the
wrapper errors out and recommends reinstalling from a platform wheel — a
standalone-binary override or local Go build is deliberately NOT attempted.
"""

from __future__ import annotations

import os
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


def find_monitor_binary() -> Path:
    """Return the bundled monitor binary, or raise with a fix-it message."""
    bundled = bundled_binary()
    if bundled is None:
        raise RuntimeError(
            "the vanth-monitor native binary is not present in this install.\n"
            "This happens when vanth was installed from source or an sdist "
            "instead of a platform wheel. Fix:\n"
            "  uv tool install --force <platform wheel URL or 'vanth' from "
            "PyPI>\n"
            f"(looked in: {Path(vanth.__file__).parent / _BIN_DIR_NAME})"
        )
    return bundled


def main() -> int:
    try:
        binary = find_monitor_binary()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    args = sys.argv[1:]
    # The native binary uses subcommands; default to the monitor when invoked
    # via the dedicated console script (vanth-monitor).
    if not args or args[0] not in {"monitor", "daemon", "mcp", "doctor", "cleanup", "--version"}:
        args = ["monitor", *args]
    try:
        return subprocess.call([str(binary), *args])
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
