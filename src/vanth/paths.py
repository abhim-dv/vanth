from __future__ import annotations

import os
from pathlib import Path


def canonical_home(home: str | os.PathLike[str] | None = None) -> Path:
    """Resolve the one state root shared by daemon, client, runner, and manager."""
    if home is not None:
        return Path(home).expanduser().resolve()
    vant_home = os.environ.get("VANTH_HOME")
    agent_home = os.environ.get("AGENT_BG_HOME")
    if vant_home and agent_home:
        vant_path = Path(vant_home).expanduser().resolve()
        agent_path = Path(agent_home).expanduser().resolve()
        if vant_path != agent_path:
            raise ValueError("VANTH_HOME and AGENT_BG_HOME refer to different state directories")
        return vant_path
    return Path(vant_home or agent_home or Path.home() / ".vanth").expanduser().resolve()
