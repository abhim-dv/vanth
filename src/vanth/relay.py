"""Client-side outbound relay for Desktop wake (review rc36 P0).

The persistent daemon never discovers client processes or stores rotating Codex
pipe names. Instead, the Vanth MCP/client integration owns an outbound relay:

1. The relay opens a durable local subscription to the daemon (registering the
   client/session/task identities it can wake) and long-polls for due
   ``codex_desktop`` deliveries.
2. The daemon publishes a generic delivery envelope (delivery_id, destination
   identity, prompt/event payload).
3. The relay acknowledges ONLY after the client-native prompt admission
   succeeds.
4. A disconnect leaves the delivery pending; a reconnect re-registers and
   resumes from the last acknowledged delivery id.

Only the final adapter differs between clients: OpenCode uses its documented
attached-server/session prompt API; Codex Desktop uses the host-provided
``codex_app/send_message_to_thread`` capability through the inherited
``CODEX_APP_TOOLS_PIPE_PATH``. That private pipe stays inside the Codex MCP
process — it is never sent to the daemon or written to the job database.

The relay runs as an internal thread using a separate localhost connection to
the daemon (never MCP stdio), stops with the MCP process, and uses bounded
reconnect backoff. Registration is authenticated by the daemon's loopback
bearer token (the same token the MCP client already uses).
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

from .client import VanthClient


class RelayError(RuntimeError):
    pass


def _destinations() -> list[dict[str, Any]]:
    """The client identities this MCP process can wake.

    Codex Desktop: the calling thread is derived from ``CODEX_THREAD_ID`` in
    this process (the MCP process is spawned fresh per task/session), and the
    pipe path must be inherited. Only register if the pipe is actually present —
    fail closed when the Desktop capability is absent.
    """
    codex_thread = os.environ.get("CODEX_THREAD_ID")
    if not codex_thread:
        return []
    from .codex_pipe import app_tools_pipe_path

    if not app_tools_pipe_path():
        return []
    return [{"client_type": "codex_desktop", "thread_id": codex_thread}]


class DesktopRelay:
    """Long-poll the daemon for codex_desktop deliveries and deliver them."""

    def __init__(
        self,
        client: VanthClient,
        client_id: str,
        *,
        destinations: list[dict[str, Any]] | None = None,
        poll_interval: float = 0.5,
        max_reconnect_delay: float = 15.0,
    ) -> None:
        self.client = client
        self.client_id = client_id
        self.destinations = destinations if destinations is not None else _destinations()
        self.poll_interval = poll_interval
        self.max_reconnect_delay = max_reconnect_delay
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._backoff = 0.5

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="vanth-desktop-relay", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _register(self) -> bool:
        try:
            result = self.client.post(
                "/relay/register",
                {
                    "client_id": self.client_id,
                    "client_type": "codex_desktop",
                    "destinations": self.destinations,
                },
            )
            return bool(result and result.get("result") == "ok")
        except Exception:
            return False

    def _unregister(self) -> None:
        try:
            self.client.post("/relay/unregister", {"client_id": self.client_id})
        except Exception:
            pass

    def _poll(self) -> list[dict[str, Any]]:
        # Long-poll up to 15s per request so a reconnect doesn't spin.
        params = {"client_id": self.client_id, "timeout_seconds": "15"}
        result = self.client.get("/relay/poll", params)
        return result.get("deliveries") if isinstance(result, dict) else []

    def _deliver(self, delivery: dict[str, Any]) -> None:
        from .codex_bridge import send_delivery_to_codex_desktop

        try:
            send_delivery_to_codex_desktop(delivery["payload"])
            self.client.post(
                "/relay/ack",
                {"client_id": self.client_id, "delivery_id": delivery["delivery_id"], "status": "delivered"},
            )
        except Exception as exc:
            self.client.post(
                "/relay/ack",
                {
                    "client_id": self.client_id,
                    "delivery_id": delivery["delivery_id"],
                    "status": "failed",
                    "error": str(exc),
                },
            )

    def _run(self) -> None:
        while not self._stop.is_set():
            if not self._register():
                # Registration failed (daemon not up, or no destinations). Wait
                # with bounded backoff and retry.
                time.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, self.max_reconnect_delay)
                continue
            self._backoff = 0.5
            try:
                deliveries = self._poll()
            except Exception:
                # Daemon unreachable: reconnect with backoff.
                time.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, self.max_reconnect_delay)
                continue
            for delivery in deliveries:
                if self._stop.is_set():
                    break
                self._deliver(delivery)


def start_desktop_relay() -> DesktopRelay | None:
    """Start the Desktop wake relay if this process can wake a Desktop task.

    Returns None when there is nothing to wake (no CODEX_THREAD_ID and no
    inherited CODEX_APP_TOOLS_PIPE_PATH), so no relay thread is spawned.
    """
    codex_thread = os.environ.get("CODEX_THREAD_ID")
    from .codex_pipe import app_tools_pipe_path

    if not codex_thread or not app_tools_pipe_path():
        return None
    from .client import VanthClient

    try:
        client = VanthClient()
        client.ensure()
    except Exception:
        return None
    relay = DesktopRelay(client, f"mcp-{os.getpid()}-{codex_thread}")
    relay.start()
    return relay
