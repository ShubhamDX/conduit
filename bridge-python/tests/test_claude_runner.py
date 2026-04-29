import asyncio

from conduit_bridge import claude_runner


class FakeMessageStream:
    def __init__(self, items):
        self._items = items

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)


class FakeClient:
    last_options = None
    last_prompt = None

    def __init__(self, *args, **kwargs):
        FakeClient.last_options = kwargs.get("options")
        self._stream = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def query(self, prompt):
        FakeClient.last_prompt = prompt
        self._stream = FakeMessageStream(
            [
                {"type": "assistant", "text": "hello "},
                {"type": "assistant", "text": "world"},
                {"type": "result", "usage": {"input_tokens": 10, "output_tokens": 2}},
            ]
        )

    def receive_response(self):
        return self._stream


def test_run_turn_emits_token_deltas_and_turn_completed(monkeypatch):
    monkeypatch.setattr(claude_runner, "ClaudeSDKClient", FakeClient)
    events = []

    async def emit(event):
        events.append(event)

    asyncio.run(
        claude_runner.run_turn(
            workspace="/tmp",
            prompt="hi",
            model="claude-sonnet-4-6",
            emit=emit,
        )
    )

    kinds = [event["kind"] for event in events]
    assert kinds.count("agent_message_delta") == 2
    assert "turn_completed" in kinds
    assert kinds[-1] == "session_ended"


def test_run_turn_exposes_memory_as_claude_mcp_tools(monkeypatch):
    class FakeOptions:
        def __init__(self, cwd, model=None, mcp_servers=None, allowed_tools=None):
            self.cwd = cwd
            self.model = model
            self.mcp_servers = mcp_servers
            self.allowed_tools = allowed_tools

    monkeypatch.setattr(claude_runner, "ClaudeAgentOptions", FakeOptions)
    monkeypatch.setattr(claude_runner, "ClaudeSDKClient", FakeClient)
    events = []

    async def emit(event):
        events.append(event)

    asyncio.run(
        claude_runner.run_turn(
            workspace="/tmp",
            prompt="hi",
            model="claude-sonnet-4-6",
            emit=emit,
            memory={
                "scope": "issue:I1",
                "tags": ["agent:claude-code"],
                "tools": ["memory_search", "memory_get"],
            },
            memory_socket="/tmp/conduit-memory.sock",
        )
    )

    options = FakeClient.last_options
    assert options.mcp_servers["conduit_memory"]["args"][-1] == "/tmp/conduit-memory.sock"
    assert "mcp__conduit_memory__memory_get" in options.allowed_tools
    assert "mcp__conduit_memory__memory_search" in FakeClient.last_prompt
