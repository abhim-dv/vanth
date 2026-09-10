"""Outbound destination policy (SSRF allowlist / block-private)."""

from __future__ import annotations

import socket

import pytest

from vanth.outbound import OutboundDenied, check_outbound_url


def _fake_getaddrinfo(ip: str):
    def _resolver(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port))]

    return _resolver


def test_default_allows_loopback_and_private(monkeypatch):
    monkeypatch.delenv("VANTH_OUTBOUND_ALLOW", raising=False)
    monkeypatch.delenv("VANTH_OUTBOUND_BLOCK_PRIVATE", raising=False)
    # Numeric hosts resolve locally, no DNS needed.
    check_outbound_url("http://127.0.0.1:8765/health")
    check_outbound_url("http://192.168.1.10/")
    check_outbound_url("https://10.0.0.5/hook")


def test_default_denies_tripwires(monkeypatch):
    monkeypatch.delenv("VANTH_OUTBOUND_ALLOW", raising=False)
    monkeypatch.delenv("VANTH_OUTBOUND_BLOCK_PRIVATE", raising=False)
    for url in ("http://169.254.169.254/latest/meta-data/", "http://0.0.0.0/", "ftp://example.com/"):
        with pytest.raises(OutboundDenied):
            check_outbound_url(url)


def test_metadata_hostname_denied(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("93.184.216.34"))
    with pytest.raises(OutboundDenied):
        check_outbound_url("http://metadata.google.internal/computeMetadata/v1/")


def test_dns_rebinding_to_denied_ip_is_caught(monkeypatch):
    # A public-looking name that resolves to a link-local metadata address.
    monkeypatch.delenv("VANTH_OUTBOUND_ALLOW", raising=False)
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("169.254.169.254"))
    with pytest.raises(OutboundDenied):
        check_outbound_url("http://evil.example.com/")


def test_allowlist_mode_is_strict(monkeypatch):
    monkeypatch.setenv("VANTH_OUTBOUND_ALLOW", "127.0.0.1,10.0.0.0/8,ntfy.local")
    check_outbound_url("http://127.0.0.1:8765/")
    check_outbound_url("http://10.5.6.7/")
    check_outbound_url("http://ntfy.local/")  # name match, no resolution needed
    # A resolved address outside the allowlist is refused even in allowlist mode.
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("93.184.216.34"))
    with pytest.raises(OutboundDenied):
        check_outbound_url("http://public.example.com/")


def test_block_private_mode(monkeypatch):
    monkeypatch.delenv("VANTH_OUTBOUND_ALLOW", raising=False)
    monkeypatch.setenv("VANTH_OUTBOUND_BLOCK_PRIVATE", "1")
    with pytest.raises(OutboundDenied):
        check_outbound_url("http://127.0.0.1/")
    with pytest.raises(OutboundDenied):
        check_outbound_url("http://192.168.0.1/")
    # A public numeric address is still allowed.
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("93.184.216.34"))
    check_outbound_url("https://example.com/")


def test_webhook_and_probe_reject_blocked_destinations(tmp_path, monkeypatch):
    from vanth.server import JobManager

    monkeypatch.delenv("VANTH_OUTBOUND_ALLOW", raising=False)
    monkeypatch.delenv("VANTH_OUTBOUND_BLOCK_PRIVATE", raising=False)
    manager = JobManager(tmp_path, recover=False)
    try:
        with pytest.raises(ValueError, match="blocked by policy"):
            manager._validate_trigger({"probe": {"type": "http", "url": "http://169.254.169.254/"}})
        with pytest.raises(ValueError):
            from vanth.server import validate_wake_targets

            validate_wake_targets([{"type": "webhook", "url": "http://169.254.169.254/x", "events": ["completed"]}])
    finally:
        manager.close()
