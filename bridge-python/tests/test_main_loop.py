import asyncio
import json
import os

from conduit_bridge.__main__ import MemoryMcpProxy, ParentRpc, handle_message


def test_new_session_dispatches_to_runner(monkeypatch):
    async def fake_run(workspace, prompt, model, emit, memory=None, memory_socket=None):
        await emit({"kind": "agent_message_delta", "delta": "hi"})

    from conduit_bridge import claude_runner

    monkeypatch.setattr(claude_runner, "run_turn", fake_run)
    out_lines = []

    async def write(line):
        out_lines.append(line)

    asyncio.run(
        handle_message(
            '{"jsonrpc":"2.0","id":1,"method":"newSession",'
            '"params":{"prompt":"hi","workspace":"/tmp"}}',
            write,
        )
    )

    responses = [json.loads(line) for line in out_lines]
    assert any(response.get("id") == 1 for response in responses)
    assert any(response.get("method") == "event" for response in responses)


def test_new_session_passes_memory_socket_to_runner(monkeypatch):
    seen = {}

    async def fake_run(workspace, prompt, model, emit, memory=None, memory_socket=None):
        seen["memory"] = memory
        seen["memory_socket"] = memory_socket
        seen["socket_exists"] = os.path.exists(memory_socket)
        await emit({"kind": "session_ended", "reason": "completed"})

    async def fake_memory_call(method, params):
        return {"entry": {"key": params["key"], "value": "from-memory"}}

    from conduit_bridge import claude_runner

    monkeypatch.setattr(claude_runner, "run_turn", fake_run)
    out_lines = []

    async def write(line):
        out_lines.append(line)

    asyncio.run(
        handle_message(
            '{"jsonrpc":"2.0","id":1,"method":"newSession",'
            '"params":{"prompt":"hi","workspace":"/tmp",'
            '"memory":{"scope":"issue:I1","tags":["agent:claude-code"],'
            '"tools":["memory_get"]}}}',
            write,
            memory_call=fake_memory_call,
        )
    )

    assert seen["memory"]["scope"] == "issue:I1"
    assert seen["memory_socket"]
    assert seen["socket_exists"]


def test_memory_proxy_forwards_socket_calls_to_parent_rpc():
    async def scenario():
        async def fake_memory_call(method, params):
            return {"entry": {"key": params["key"], "value": method}}

        async with MemoryMcpProxy(fake_memory_call) as proxy:
            reader, writer = await asyncio.open_unix_connection(proxy.socket_path)
            writer.write(b'{"method":"memory_get","params":{"key":"k"}}\n')
            await writer.drain()
            response = json.loads((await reader.readline()).decode("utf-8"))
            writer.close()
            await writer.wait_closed()
            return response

    response = asyncio.run(scenario())
    assert response["result"]["entry"]["value"] == "memory_get"


def test_memory_proxy_rejects_oversized_socket_requests(monkeypatch):
    from conduit_bridge import __main__ as main_loop

    monkeypatch.setattr(main_loop, "MAX_MEMORY_REQUEST_BYTES", 8)

    async def scenario():
        async def fake_memory_call(method, params):
            raise AssertionError("memory call should not run")

        async with MemoryMcpProxy(fake_memory_call) as proxy:
            reader, writer = await asyncio.open_unix_connection(proxy.socket_path)
            writer.write(b"a" * 9)
            await writer.drain()
            response = json.loads((await reader.readline()).decode("utf-8"))
            writer.close()
            await writer.wait_closed()
            return response

    response = asyncio.run(scenario())
    assert "exceeds byte limit" in response["error"]


def test_parent_rpc_routes_memory_response_to_pending_request():
    async def scenario():
        out_lines = []

        async def write(line):
            out_lines.append(line)

        rpc = ParentRpc(write)
        request_task = asyncio.create_task(
            rpc.request("memory_get", {"key": "agent-note"})
        )
        await asyncio.sleep(0)
        outbound = json.loads(out_lines[0])
        await rpc.dispatch(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": outbound["id"],
                    "result": {"entry": {"key": "agent-note", "value": "v"}},
                }
            )
        )
        return await request_task

    result = asyncio.run(scenario())
    assert result["entry"]["value"] == "v"
