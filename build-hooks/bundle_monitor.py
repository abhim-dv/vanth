"""Hatchling build hook that bundles the native Go monitor into the wheel.

Runs ``go build`` for the host platform and injects the resulting binary into
the wheel under ``vanth/monitor-bin/`` so the ``vanth-monitor`` console script
can locate it without requiring a Go toolchain at runtime.

The hook runs only for the ``wheel`` target (not ``sdist``). ``go`` must be on
PATH during the build; it is not required at install or run time.
"""

from __future__ import annotations

import os
import subprocess

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

try:
    from packaging.tags import platform_tags
except ImportError:  # pragma: no cover - packaging is a hatchling dependency
    platform_tags = None


def get_build_hook():
    return BundleMonitorBuildHook


def wheel_platform_tag() -> str:
    """Return the PEP 425 platform tag for the host, e.g. ``win_amd64``."""
    if platform_tags is not None:
        tags = list(platform_tags())
        if tags:
            return tags[0]
    # Fallback: packaging is unavailable; derive a coarse tag.
    machine = os.environ.get("PROCESSOR_ARCHITECTURE") or os.uname().machine
    if os.name == "nt":
        return {"AMD64": "win_amd64", "ARM64": "win_arm64"}.get(machine, machine.lower())
    sysname = os.uname().sysname.lower()
    return f"{sysname}_{machine.lower()}"


class BundleMonitorBuildHook(BuildHookInterface):
    PLUGIN_NAME = "bundle-monitor"

    def initialize(self, version: str, build_data: dict[str, dict]) -> None:
        if self.target_name != "wheel":
            return

        root = self.root
        exe = ".exe" if os.name == "nt" else ""
        binary = os.path.join(root, "dist", f"vanth-monitor{exe}")

        os.makedirs(os.path.dirname(binary), exist_ok=True)

        ldflags = f"-X vanth/internal/config.Version={self.metadata.version}"
        cmd = [
            "go",
            "build",
            "-trimpath",
            "-ldflags",
            ldflags,
            "-o",
            binary,
            "./cmd/vanth",
        ]
        subprocess.run(cmd, cwd=root, check=True)

        rel = os.path.join("vanth", "monitor-bin", os.path.basename(binary))
        build_data["force_include"] = {binary: rel}
        build_data["tag"] = f"py3-none-{wheel_platform_tag()}"
        self.app.display_success(f"Bundled monitor binary -> {rel}")
