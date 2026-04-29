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
    def __init__(self, *args, **kwargs):
        self._stream = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def query(self, prompt):
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
