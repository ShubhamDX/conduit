import json

from conduit_bridge import memory_mcp


def test_tools_list_returns_memory_tools():
    response = memory_mcp.handle_payload(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        "/tmp/no-socket",
    )

    tools = response["result"]["tools"]
    assert [tool["name"] for tool in tools] == [
        "memory_search",
        "memory_get",
        "memory_upsert",
    ]


def test_tools_call_wraps_memory_result(monkeypatch):
    def fake_call_memory(socket_path, method, params):
        assert socket_path == "/tmp/memory.sock"
        assert method == "memory_get"
        assert params == {"key": "k"}
        return {"result": {"entry": {"key": "k", "value": "v"}}}

    monkeypatch.setattr(memory_mcp, "_call_memory", fake_call_memory)

    response = memory_mcp.handle_payload(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "memory_get", "arguments": {"key": "k"}},
        },
        "/tmp/memory.sock",
    )

    text = response["result"]["content"][0]["text"]
    assert json.loads(text)["entry"]["value"] == "v"
    assert response["result"]["isError"] is False
