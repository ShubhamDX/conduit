import asyncio
import json

from conduit_bridge.__main__ import handle_message


def test_new_session_dispatches_to_runner(monkeypatch):
    async def fake_run(workspace, prompt, model, emit):
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
