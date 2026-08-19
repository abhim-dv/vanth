"""Autostart registration for the Vanth daemon.

Lets the daemon survive reboots by registering it as a background service
per platform:

- Windows: a Task Scheduler task (``VanthDaemon``) that runs at logon and
  startup, defined via a generated XML task file that sets ``VANTH_HOME``.
- macOS: a launchd LaunchAgent plist
  (``~/Library/LaunchAgents/com.vanth.daemon.plist``) with RunAtLoad,
  KeepAlive, and the ``VANTH_HOME`` environment variable.
- Linux: a systemd user unit
  (``~/.config/systemd/user/vanth-daemon.service``) wanted by
  ``default.target``.

Nothing here runs ``schtasks``/``launchctl``/``systemctl`` at import time.
Every side-effecting function accepts injectable ``_run``/``_write``/
``_remove`` callables so the registration logic is unit-testable and safe
to dry-run.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Sequence

HOME_ENV = "VANTH_HOME"
TASK_NAME = "VanthDaemon"
LAUNCHAGENT_NAME = "com.vanth.daemon"
LINUX_UNIT_NAME = "vanth-daemon.service"

_Run = Callable[[Sequence[str]], "subprocess.CompletedProcess[str]"]
_Write = Callable[[Path, str], None]
_Remove = Callable[[Path], None]
_Exists = Callable[[Path], bool]


def platform() -> str:
    """Return the current platform key: "windows", "macos", or "linux"."""
    if sys.platform == "win32":
        return "windows"
    if sys.platform == "darwin":
        return "macos"
    return "linux"


def _command_line(home: Path) -> list[str]:
    """The exact command that launches the daemon for this home.

    Prefers the ``vanthd`` console script on PATH; falls back to
    ``sys.executable -m vanth.daemon``. ``VANTH_HOME`` is always passed in
    the environment so the autostart uses this home regardless of the
    registering shell's env; ``VANTH_DAEMON_PORT``/``VANTH_DAEMON_HOST`` are
    deliberately left unset so the daemon picks them up from its own
    environment at start.
    """
    vanthd = shutil.which("vanthd")
    if vanthd:
        return [vanthd]
    return [sys.executable, "-m", "vanth.daemon"]


def _windows_xml(home: Path) -> str:
    """A Task Scheduler task definition that runs the daemon at logon+startup."""
    exe, *args = _command_line(home)
    args_xml = ""
    if args:
        args_xml = "\n".join(
            f'<Argument>{_escape_xml(a)}</Argument>' for a in args
        )
    return f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Vanth background-job daemon</Description>
  </RegistrationInfo>
  <Triggers>
    <BootTrigger>
      <Enabled>true</Enabled>
    </BootTrigger>
    <LogonTrigger>
      <Enabled>true</Enabled>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{_escape_xml(exe)}</Command>
      {args_xml}
    </Exec>
  </Actions>
  <EnvironmentVariables>
    <Variable name="VANTH_HOME"><Value>{_escape_xml(str(home))}</Value></Variable>
  </EnvironmentVariables>
</Task>
"""


def _escape_xml(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _macos_plist(home: Path) -> str:
    """A launchd LaunchAgent plist that keeps the daemon alive."""
    exe, *args = _command_line(home)
    argv = [exe, *args]
    lines = "".join(f"      <string>{_escape_plist(a)}</string>\n" for a in argv)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LAUNCHAGENT_NAME}</string>
  <key>ProgramArguments</key>
  <array>
{lines}  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>EnvironmentVariables</key>
  <dict>
    <key>{HOME_ENV}</key>
    <string>{_escape_plist(str(home))}</string>
  </dict>
  <key>StandardOutPath</key>
  <string>{_escape_plist(str(home / "daemon.log"))}</string>
  <key>StandardErrorPath</key>
  <string>{_escape_plist(str(home / "daemon.log"))}</string>
</dict>
</plist>
"""


def _escape_plist(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _linux_unit(home: Path) -> str:
    """A systemd user unit that keeps the daemon alive after login."""
    exe, *args = _command_line(home)
    exec_line = " ".join(_shell_quote(a) for a in [exe, *args])
    return f"""[Unit]
Description=Vanth background-job daemon
After=default.target

[Service]
Type=simple
ExecStart={exec_line}
Restart=on-failure
RestartSec=5
Environment={HOME_ENV}={_shell_quote(str(home))}

[Install]
WantedBy=default.target
"""


def _shell_quote(text: str) -> str:
    if text and all(c.isalnum() or c in "/._-+" for c in text):
        return text
    return "'" + text.replace("'", "'\\''") + "'"


def _write_file(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _remove_file(path: Path) -> None:
    if path.exists():
        path.unlink()


def _path_exists(path: Path) -> bool:
    return path.exists()


def _run_cmd(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command), capture_output=True, text=True, shell=False
    )


def _targets(home: Path) -> dict[str, Any]:
    platform_key = platform()
    if platform_key == "windows":
        return {
            "kind": "task",
            "target": TASK_NAME,
            "file": home / "vanthd-task.xml",
            "enable_cmd": ["schtasks", "/Create", "/XML", str(home / "vanthd-task.xml"), "/TN", TASK_NAME, "/F"],
            "disable_cmd": ["schtasks", "/Delete", "/F", "/TN", TASK_NAME],
            "query_cmd": ["schtasks", "/Query", "/TN", TASK_NAME],
        }
    if platform_key == "macos":
        target = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHAGENT_NAME}.plist"
        return {
            "kind": "launchagent",
            "target": str(target),
            "file": target,
            "enable_cmd": ["launchctl", "load", str(target)],
            "disable_cmd": ["launchctl", "unload", str(target)],
            "query_cmd": [],
        }
    target = Path.home() / ".config" / "systemd" / "user" / LINUX_UNIT_NAME
    return {
        "kind": "systemd-user",
        "target": str(target),
        "file": home / LINUX_UNIT_NAME,
        "enable_cmd": ["systemctl", "--user", "enable", "--now", str(home / LINUX_UNIT_NAME)],
        "disable_cmd": ["systemctl", "--user", "disable", "--now", LINUX_UNIT_NAME],
        "query_cmd": ["systemctl", "--user", "is-enabled", LINUX_UNIT_NAME],
    }


def _render(home: Path, platform_key: str) -> str:
    if platform_key == "windows":
        return _windows_xml(home)
    if platform_key == "macos":
        return _macos_plist(home)
    return _linux_unit(home)


def _ensure_home(home: Path) -> Path:
    home = Path(home)
    if not home.exists():
        home.mkdir(parents=True, exist_ok=True)
    if not home.is_dir() or not os.access(home, os.W_OK):
        raise OSError(f"vanth home is not writable: {home}")
    return home


def plan(home: Path) -> dict[str, Any]:
    """Describe what autostart would do, without changing anything."""
    home = Path(home)
    platform_key = platform()
    targets = _targets(home)
    return {
        "platform": platform_key,
        "target": targets["target"],
        "command": _command_line(home),
        "home": str(home),
        "enabled": detect(home).get("enabled", False),
    }


def detect(
    home: Path,
    *,
    _run: _Run = _run_cmd,
    _exists: _Exists = _path_exists,
) -> dict[str, Any]:
    """Detect the current registration state without modifying anything."""
    home = Path(home)
    platform_key = platform()
    targets = _targets(home)
    result: dict[str, Any] = {"platform": platform_key, "target": targets["target"], "enabled": False}
    try:
        if platform_key == "windows":
            proc = _run(targets["query_cmd"])
            result["enabled"] = proc.returncode == 0
            if proc.returncode != 0:
                stderr = proc.stderr.strip()
                if not stderr or "cannot find the file specified" in stderr.lower() or "does not exist" in stderr.lower():
                    result["error"] = "task not registered"
                else:
                    result["error"] = stderr
        elif platform_key == "macos":
            result["enabled"] = _exists(Path(targets["target"]))
        else:
            result["enabled"] = _exists(Path(targets["target"]))
            if not result["enabled"]:
                result["error"] = "unit file not found"
            else:
                try:
                    proc = _run(targets["query_cmd"])
                    result["enabled"] = proc.returncode == 0
                    if proc.returncode != 0:
                        result["error"] = proc.stderr.strip() or "unit not enabled"
                except Exception as exc:  # noqa: BLE001 - tool missing or broken
                    result["error"] = f"systemctl unavailable: {exc}"
    except Exception as exc:  # noqa: BLE001 - tool missing or broken
        result["error"] = str(exc)
    return result


def enable(
    home: Path,
    *,
    dry_run: bool = False,
    _run: _Run = _run_cmd,
    _write: _Write = _write_file,
) -> dict[str, Any]:
    """Register the daemon to start automatically, or describe doing so."""
    home = _ensure_home(home)
    platform_key = platform()
    targets = _targets(home)
    content = _render(home, platform_key)
    if dry_run:
        return {
            "dry_run": True,
            "would_install": f"write {targets['target']!r} and run {' '.join(targets['enable_cmd'])}",
            "target": targets["target"],
            "platform": platform_key,
        }
    _write(Path(targets["file"]), content)
    proc = _run(targets["enable_cmd"])
    if proc.returncode != 0:
        raise RuntimeError(
            f"autostart enable failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    return {"enabled": True, "target": targets["target"], "platform": platform_key}


def disable(
    home: Path,
    *,
    dry_run: bool = False,
    _run: _Run = _run_cmd,
    _remove: _Remove = _remove_file,
) -> dict[str, Any]:
    """Remove the autostart registration, or describe doing so."""
    home = Path(home)
    platform_key = platform()
    targets = _targets(home)
    if dry_run:
        return {
            "dry_run": True,
            "would_uninstall": f"run {' '.join(targets['disable_cmd'])} and remove {targets['target']!r}",
            "target": targets["target"],
            "platform": platform_key,
        }
    proc = _run(targets["disable_cmd"])
    if proc.returncode != 0:
        raise RuntimeError(
            f"autostart disable failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    _remove(Path(targets["target"]))
    return {"enabled": False, "target": targets["target"], "platform": platform_key}
