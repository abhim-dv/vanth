"""Platform-correct quoting for shell command strings used in tests.

The runner executes a workload's command through the platform shell
(``subprocess.Popen(command, shell=True)``), so the command string a test builds
must use the *host* shell's quoting rules:

- Windows: ``subprocess.list2cmdline`` (the ``cmd.exe`` rules).
- POSIX: ``shlex.join``. ``list2cmdline`` implements Windows rules only; on
  POSIX it leaves ``python -c "print('x')"`` unquoted, and the shell rejects the
  parentheses (``bash: syntax error near unexpected token '('``).

Keeping this in one place lets the Python test matrix run on Linux/macOS as
well as Windows. ``tests`` is on ``sys.path`` via pytest's ``pythonpath``.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys


def join(argv: object) -> str:
    """Quote ``argv`` for the current platform's shell."""
    parts = [str(part) for part in argv]  # type: ignore[union-attr]
    if os.name == "nt":
        return subprocess.list2cmdline(parts)
    return shlex.join(parts)


def cmd(code: str) -> str:
    """Build a ``python -c <code>`` command string for the current platform."""
    return join([sys.executable, "-c", code])
