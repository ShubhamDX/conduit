from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

try:
    from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient
except ImportError:
    ClaudeAgentOptions = None
    ClaudeSDKClient = None


async def run_turn(
    workspace: str,
    prompt: str,
    model: str | None,
    emit: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    if ClaudeSDKClient is None:
        raise RuntimeError("claude-agent-sdk is not installed")

    options = _options(workspace, model)
    async with ClaudeSDKClient(options=options) as client:
        await client.query(prompt)
        async for message in client.receive_response():
            await _emit_message(message, emit)

    await emit({"kind": "session_ended", "reason": "completed"})


def _options(workspace: str, model: str | None) -> Any:
    if ClaudeAgentOptions is None:
        return None
    if model:
        return ClaudeAgentOptions(cwd=workspace, model=model)
    return ClaudeAgentOptions(cwd=workspace)


async def _emit_message(
    message: Any,
    emit: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    message_type = _message_get(message, "type")

    if message_type == "assistant":
        await emit(
            {
                "kind": "agent_message_delta",
                "delta": _message_get(message, "text", ""),
            }
        )
    elif message_type == "tool_use":
        await emit(
            {
                "kind": "tool_call_started",
                "call_id": _message_get(message, "id", ""),
                "name": _message_get(message, "name", ""),
                "args": _message_get(message, "input", {}),
            }
        )
    elif message_type == "tool_result":
        await emit(
            {
                "kind": "tool_call_completed",
                "call_id": _message_get(message, "tool_use_id", ""),
                "ok": not _message_get(message, "is_error", False),
                "output": str(_message_get(message, "content", "")),
            }
        )
    elif message_type == "result":
        usage = _message_get(message, "usage", {}) or {}
        await emit(
            {
                "kind": "turn_completed",
                "tokens_in": int(usage.get("input_tokens", 0)),
                "tokens_out": int(usage.get("output_tokens", 0)),
            }
        )


def _message_get(message: Any, key: str, default: Any = None) -> Any:
    if isinstance(message, dict):
        return message.get(key, default)
    return getattr(message, key, default)
