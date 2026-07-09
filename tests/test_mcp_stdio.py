import asyncio
import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


def content(result):
    if result.structuredContent is not None:
        return result.structuredContent
    return json.loads(result.content[0].text)


def test_mcp_stdio_start_wait_tail(tmp_path):
    async def main():
        env = {**os.environ, "VANTH_HOME": str(tmp_path)}
        server = StdioServerParameters(
            command="uv",
            args=["run", "vanth"],
            cwd=str(Path(__file__).parents[1]),
            env=env,
        )
        async with stdio_client(server) as (read, write):
            async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=10)) as session:
                await session.initialize()
                tools = {tool.name for tool in (await session.list_tools()).tools}
                assert {"job_start", "job_wait", "job_tail"} <= tools

                start = content(
                    await session.call_tool(
                        "job_start",
                        {
                            "command": subprocess.list2cmdline(
                                [sys.executable, str(Path(__file__).parents[1] / "examples" / "long_job.py")]
                            )
                        },
                    )
                )
                progress = content(
                    await session.call_tool(
                        "job_wait",
                        {"job_id": start["job_id"], "filters": ["progress"], "timeout_seconds": 5},
                        read_timeout_seconds=timedelta(seconds=10),
                    )
                )
                status = content(await session.call_tool("job_status", {"job_id": start["job_id"]}))
                waited = content(
                    await session.call_tool(
                        "job_wait",
                        {
                            "job_id": start["job_id"],
                            "filters": ["checkpoint"],
                            "since_event_id": progress["event"]["event_id"],
                            "timeout_seconds": 5,
                        },
                        read_timeout_seconds=timedelta(seconds=10),
                    )
                )
                tail = content(await session.call_tool("job_tail", {"job_id": start["job_id"]}))

                assert progress["event"]["type"] == "progress"
                assert progress["event"]["data"]["current"] == 1
                assert progress["status"] == "running"
                assert status["progress"]["current"] >= 1
                assert waited["event"]["message"] == "demo checkpoint"
                assert "AGENT_EVENT" in tail["content"]

    asyncio.run(main())


def test_mcp_stdio_errors_and_event_cap(tmp_path):
    async def main():
        env = {**os.environ, "VANTH_HOME": str(tmp_path), "VANTH_MAX_EVENT_BYTES": "20"}
        server = StdioServerParameters(
            command="uv",
            args=["run", "vanth"],
            cwd=str(Path(__file__).parents[1]),
            env=env,
        )
        async with stdio_client(server) as (read, write):
            async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=10)) as session:
                await session.initialize()
                missing = content(await session.call_tool("job_status", {"job_id": "job_missing"}))
                assert missing["result"] == "error"

                command = subprocess.list2cmdline(
                    [
                        sys.executable,
                        "-c",
                        "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','data':{'big':'x'*100}}), flush=True)",
                    ]
                )
                start = content(await session.call_tool("job_start", {"command": command}))
                event = content(
                    await session.call_tool(
                        "job_wait",
                        {"job_id": start["job_id"], "filters": ["checkpoint"], "timeout_seconds": 5},
                        read_timeout_seconds=timedelta(seconds=10),
                    )
                )
                assert event["event"]["data"] == {"truncated": True, "max_bytes": 20}

    asyncio.run(main())
