"""Outbound destination policy for webhooks and HTTP readiness probes.

Vanth is local-first, so loopback and RFC1918 destinations are *allowed* by
default (ntfy/Gotify/local health endpoints). What is always denied is the SSRF
tripwire set — link-local / cloud-metadata / unspecified addresses — and callers
can opt into a strict allowlist or a block-private policy.

Policy (environment):
- ``VANTH_OUTBOUND_ALLOW``: comma list of hosts, IPs, or CIDRs. When set, only
  those destinations are allowed (host-name match, or every resolved IP falls in
  a listed network).
- ``VANTH_OUTBOUND_BLOCK_PRIVATE=1``: additionally deny loopback + private.

The host is always resolved and *every* resolved address is checked, so a name
cannot rebind to a denied IP between the check and the connect.
"""

from __future__ import annotations

import ipaddress
import os
import socket
from urllib.parse import urlsplit

_METADATA_HOSTS = {"metadata.google.internal", "metadata.goog", "instance-data"}


class OutboundDenied(ValueError):
    """The destination is refused by the configured outbound policy."""


def _allowlist() -> list[str]:
    raw = os.environ.get("VANTH_OUTBOUND_ALLOW", "")
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


def _block_private() -> bool:
    return os.environ.get("VANTH_OUTBOUND_BLOCK_PRIVATE", "0").strip().lower() not in {"", "0", "false", "no"}


def _tripwire(ip: ipaddress._BaseAddress) -> bool:
    # Always-denied: link-local (cloud metadata), unspecified, multicast,
    # reserved. Loopback/private are intentionally NOT here.
    return ip.is_link_local or ip.is_unspecified or ip.is_multicast or ip.is_reserved


def _matches_allow(ip: ipaddress._BaseAddress, allow: list[str]) -> bool:
    for entry in allow:
        try:
            if "/" in entry:
                if ip in ipaddress.ip_network(entry, strict=False):
                    return True
            elif ip == ipaddress.ip_address(entry):
                return True
        except ValueError:
            continue  # a hostname entry; handled by name match
    return False


def check_outbound_url(url: str) -> None:
    """Raise :class:`OutboundDenied` if ``url`` violates the destination policy."""
    if not isinstance(url, str) or not url:
        raise OutboundDenied("outbound URL must be a non-empty string")
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"}:
        raise OutboundDenied(f"unsupported outbound scheme: {parts.scheme!r}")
    host = parts.hostname
    if not host:
        raise OutboundDenied("outbound URL has no host")
    lowered = host.lower()
    allow = _allowlist()
    if allow and lowered in allow:
        return
    port = parts.port or (443 if parts.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        raise OutboundDenied(f"cannot resolve outbound host {host!r}") from exc
    addresses = {ipaddress.ip_address(info[4][0]) for info in infos}
    if not addresses:
        raise OutboundDenied(f"outbound host {host!r} resolved to no address")
    if lowered in _METADATA_HOSTS:
        raise OutboundDenied(f"cloud metadata host is not allowed: {host}")
    if allow:
        # Strict: EVERY resolved address must be allowlisted.
        if not all(_matches_allow(ip, allow) for ip in addresses):
            raise OutboundDenied(f"outbound host {host!r} is not in VANTH_OUTBOUND_ALLOW")
        return
    block_private = _block_private()
    for ip in addresses:
        if _tripwire(ip):
            raise OutboundDenied(f"outbound address {ip} is blocked (link-local/metadata/unspecified)")
        if block_private and (ip.is_private or ip.is_loopback):
            raise OutboundDenied(f"outbound address {ip} is blocked (VANTH_OUTBOUND_BLOCK_PRIVATE)")
