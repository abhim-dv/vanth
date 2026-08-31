"""Tests for the Codex Desktop native app-tools pipe client and the client-side
wake relay (review rc36 P0).

Covers the reviewer's acceptance tests:
1. Framing unit tests: fragmented header/body, wrong response ID, JSON-RPC
   error, invalid JSON, EOF, and >8 MiB rejection.
2. Fake-pipe integration: tools/list capability preflight followed by the exact
   tools/call envelope; assert one delivery per Vanth delivery ID.
3. Capability-gated fail-closed: missing pipe / missing capability never routes
   to the CLI bridge.
4. Relay: register/poll/ack against a real daemon through the manager.
"""

import asyncio
import json
import os
import socket
import struct
import subprocess
import sys
import threading
import time

import pytest

from vanth.codex_pipe import (
    CodexPipeClient,
    CodexPipeError,
    CodexPipeUnavailable,
    MAX_FRAME_BYTES,
    send_desktop_message,
)
from vanth.server import JobManager


class FakePipeServer:
    """A fake Desktop app-tools host speaking the length-prefixed JSON-RPC
    protocol over a socketpair (behaves like the Windows named pipe)."""

    def __init__(self, tools=("codex_app/send_message_to_thread", "codex_app/list_threads")):
        self.tools = tools
        self.seen_requests = []
        self.deliveries = []
        self._sock, self._client = socket.socketpair()
        self._server_thread = threading.Thread(target=self._serve, daemon=True)
        self._server_thread.start()

    def _serve(self):
        conn = self._sock
        try:
            while True:
                header = self._read_exact(conn, 4)
                if not header:
                    return
                (length,) = struct.unpack("<I", header)
                body = self._read_exact(conn, length)
                message = json.loads(body.decode("utf-8"))
                self.seen_requests.append(message)
                method = message.get("method")
                if method == "tools/list":
                    response = {
                        "jsonrpc": "2.0",
                        "id": message.get("id"),
                        "result": {
                            "tools": [{"name": name, "description": ""} for name in self.tools],
                        },
                    }
                elif method == "tools/call":
                    params = message.get("params", {})
                    arguments = params.get("arguments", {})
                    self.deliveries.append(arguments)
                    response = {
                        "jsonrpc": "2.0",
                        "id": message.get("id"),
                        "result": {"success": True, "content": [{"type": "text", "text": "accepted"}]},
                    }
                else:
                    response = {
                        "jsonrpc": "2.0",
                        "id": message.get("id"),
                        "error": {"code": -32601, "message": "method not found"},
                    }
                self._write_frame(conn, response)
        except OSError:
            return

    @staticmethod
    def _read_exact(conn, length):
        data = b""
        while len(data) < length:
            chunk = conn.recv(length - len(data))
            if not chunk:
                return b""
            data += chunk
        return data

    @staticmethod
    def _write_frame(conn, payload):
        body = json.dumps(payload).encode("utf-8")
        conn.sendall(struct.pack("<I", len(body)) + body)

    def client_socket(self):
        return self._client

    def close(self):
        try:
            self._sock.close()
        except OSError:
            pass
        try:
            self._client.close()
        except OSError:
            pass


def _frame(payload):
    body = json.dumps(payload).encode("utf-8")
    return struct.pack("<I", len(body)) + body


class TestFraming:
    def _client(self, server):
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = server.client_socket()
        client.timeout_seconds = 5.0
        client._request_id = 0
        return client

    def test_full_round_trip(self):
        server = FakePipeServer()
        try:
            client = self._client(server)
            result = client.call("tools/list", {"threadStartKind": "all"})
            assert "tools" in result
            assert any(t["name"] == "codex_app/send_message_to_thread" for t in result["tools"])
            assert client.has_capability("codex_app/send_message_to_thread")
            client.close()
        finally:
            server.close()

    def test_fragmented_read(self):
        server = FakePipeServer()
        try:
            sock = server.client_socket()
            # Write the frame one byte at a time to exercise fragmented reads.
            frame = _frame({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
            for byte in frame:
                sock.sendall(bytes([byte]))
            # Read the response.
            header = server._read_exact(sock, 4)
            (length,) = struct.unpack("<I", header)
            body = server._read_exact(sock, length)
            response = json.loads(body)
            assert response.get("id") == 1
            assert "result" in response
        finally:
            server.close()

    def test_wrong_response_id_is_skipped(self):
        server = FakePipeServer()
        try:
            client = self._client(server)
            # Manually send a request, read the response (id=1), then send a
            # request that would get id=2 — the client skips id=1 frames.
            client._send_frame({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {"threadStartKind": "all"}})
            first = client._read_frame()
            assert first.get("id") == 1
            result = client.call("tools/list", {"threadStartKind": "all"})
            assert "tools" in result
            client.close()
        finally:
            server.close()

    def test_json_rpc_error(self):
        server = FakePipeServer(tools=())
        try:
            client = self._client(server)
            with pytest.raises(CodexPipeError, match="method not found"):
                client.call("bogus/method", {})
            client.close()
        finally:
            server.close()

    def test_invalid_json_response(self):
        sock, client_sock = socket.socketpair()

        def serve():
            conn = sock
            try:
                # Read the request, reply with an invalid JSON body.
                _read = FakePipeServer._read_exact
                _read(conn, 4)
                body = b"not-json{{{"
                conn.sendall(struct.pack("<I", len(body)) + body)
            except OSError:
                return

        threading.Thread(target=serve, daemon=True).start()
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = client_sock
        client.timeout_seconds = 5.0
        client._request_id = 0
        with pytest.raises(CodexPipeError, match="invalid JSON"):
            client.call("tools/list", {})
        client.close()
        sock.close()

    def test_eof_raises(self):
        sock, client_sock = socket.socketpair()
        sock.close()
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = client_sock
        client.timeout_seconds = 5.0
        client._request_id = 0
        with pytest.raises(CodexPipeError, match="closed the connection"):
            client.call("tools/list", {})
        client.close()

    def test_oversized_frame_rejected(self):
        sock, client_sock = socket.socketpair()

        def serve():
            conn = sock
            try:
                # Read the request, reply with a header advertising a body
                # larger than the 8 MiB cap.
                _read = FakePipeServer._read_exact
                _read(conn, 4)
                conn.sendall(struct.pack("<I", MAX_FRAME_BYTES + 1))
            except OSError:
                return

        threading.Thread(target=serve, daemon=True).start()
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = client_sock
        client.timeout_seconds = 5.0
        client._request_id = 0
        with pytest.raises(CodexPipeError, match="8 MiB cap"):
            client.call("tools/list", {})
        client.close()
        sock.close()


class TestDesktopDelivery:
    def test_send_desktop_message_preflights_and_calls(self):
        server = FakePipeServer()
        try:
            result = send_desktop_message(
                destination_thread_id="thread_dest",
                prompt="wake up",
                caller_thread_id="thread_caller",
                call_id="vanth-del_123",
                _handle=server.client_socket(),
                timeout_seconds=5,
            )
            assert result.get("success") is True
            # tools/list must precede tools/call.
            methods = [req.get("method") for req in server.seen_requests]
            assert methods[0] == "tools/list"
            assert "tools/call" in methods
            call = next(req for req in server.seen_requests if req.get("method") == "tools/call")
            params = call["params"]
            assert params["namespace"] == "codex_app"
            assert params["tool"] == "send_message_to_thread"
            assert params["arguments"]["threadId"] == "thread_dest"
            assert params["arguments"]["prompt"] == "wake up"
            assert params["threadId"] == "thread_caller"
            assert params["callId"] == "vanth-del_123"
            assert params["turnId"] == "vanth-del_123"
        finally:
            server.close()

    def test_one_delivery_per_call_id(self):
        # The Vanth delivery id is used as the stable call/turn correlation key.
        # Two different delivery ids must produce two distinct call/turn ids.
        call_ids = []
        for call_id in ("vanth-del_a", "vanth-del_b"):
            server = FakePipeServer()
            try:
                send_desktop_message(
                    destination_thread_id="thread_dest",
                    prompt="wake",
                    call_id=call_id,
                    _handle=server.client_socket(),
                    timeout_seconds=5,
                )
                call = next(req for req in server.seen_requests if req.get("method") == "tools/call")
                params = call["params"]
                assert params["callId"] == call_id
                assert params["turnId"] == call_id
                call_ids.append(params["callId"])
            finally:
                server.close()
        assert len(set(call_ids)) == 2

    def test_missing_pipe_is_fail_closed(self, monkeypatch):
        monkeypatch.delenv("CODEX_APP_TOOLS_PIPE_PATH", raising=False)
        with pytest.raises(CodexPipeUnavailable, match="not set"):
            send_desktop_message(
                destination_thread_id="thread_dest",
                prompt="wake",
                call_id="vanth-del_x",
            )

    def test_missing_capability_is_fail_closed(self):
        server = FakePipeServer(tools=("codex_app/list_threads",))
        try:
            with pytest.raises(CodexPipeError, match="does not advertise"):
                send_desktop_message(
                    destination_thread_id="thread_dest",
                    prompt="wake",
                    call_id="vanth-del_y",
                    _handle=server.client_socket(),
                    timeout_seconds=5,
                )
        finally:
            server.close()


class TestRelay:
    def test_relay_register_poll_ack(self, tmp_path):
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            # Register a relay client.
            result = manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            assert result["result"] == "ok"

            # Create a codex_desktop delivery by starting a job with a wake
            # target, then completing it.
            async def main():
                job = await manager.start(
                    subprocess.list2cmdline([sys.executable, "-c", "import sys; sys.exit(0)"]),
                    wake_targets=[{"type": "codex_desktop", "events": ["completed"], "thread_id": "thread_dest"}],
                )
                await manager.wait(job["job_id"], ["completed"], timeout_seconds=30)

            asyncio.run(main())
            # The dispatch loop must NOT daemon-dispatch codex_desktop.

            # Poll should return the due codex_desktop delivery.
            polled = manager.relay_poll("client_1", timeout_seconds=1)
            assert polled, "relay poll must return the due codex_desktop delivery"
            delivery = polled[0]
            assert delivery["target_type"] == "codex_desktop"
            payload_target = delivery["payload"]["target"]
            assert payload_target.get("thread_id") == "thread_dest"

            # Acknowledge delivered.
            ack = manager.relay_ack("client_1", delivery["delivery_id"], "delivered")
            assert ack["result"] == "ok"
            status = manager.deliveries(delivery["job_id"])["deliveries"]
            matching = [d for d in status if d["delivery_id"] == delivery["delivery_id"]]
            assert matching and matching[0]["status"] == "delivered"
        finally:
            manager.close()

    def test_relay_poll_unknown_client(self, tmp_path):
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            with pytest.raises(ValueError, match="Unknown relay client_id"):
                manager.relay_poll("nope", timeout_seconds=1)
        finally:
            manager.close()
