from __future__ import annotations

import inspect
import sys
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
    memory: dict[str, Any] | None = None,
    memory_socket: str | None = None,
) -> None:
    if ClaudeSDKClient is None:
        raise RuntimeError("claude-agent-sdk is not installed")

    options = _options(workspace, model, memory, memory_socket)
    async with ClaudeSDKClient(options=options) as client:
        await client.query(_prompt(prompt, memory))
        async for message in client.receive_response():
            await _emit_message(message, emit)

    await emit({"kind": "session_ended", "reason": "completed"})


def _options(
    workspace: str,
    model: str | None,
    memory: dict[str, Any] | None = None,
    memory_socket: str | None = None,
) -> Any:
    if ClaudeAgentOptions is None:
        return None

    kwargs: dict[str, Any] = {"cwd": workspace}
    if model:
        kwargs["model"] = model
    if memory and memory_socket:
        kwargs["mcp_servers"] = {
            "conduit_memory": {
                "command": sys.executable,
                "args": ["-m", "conduit_bridge.memory_mcp", memory_socket],
            }
        }
        kwargs["allowed_tools"] = [
            f"mcp__conduit_memory__{tool}"
            for tool in memory.get("tools", [])
            if isinstance(tool, str)
        ]

    required = ["mcp_servers"] if memory and memory_socket else []
    return _build_options(kwargs, required)


def _build_options(kwargs: dict[str, Any], required: list[str] | None = None) -> Any:
    required = required or []
    try:
        signature = inspect.signature(ClaudeAgentOptions)
    except (TypeError, ValueError):
        return ClaudeAgentOptions(**kwargs)

    if any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    ):
        return ClaudeAgentOptions(**kwargs)

    supported = {
        key: value for key, value in kwargs.items() if key in signature.parameters
    }
    missing = [key for key in required if key not in supported]
    if missing:
        raise RuntimeError(
            "claude-agent-sdk does not support required option(s): "
            + ", ".join(missing)
        )
    return ClaudeAgentOptions(**supported)


def _prompt(prompt: str, memory: dict[str, Any] | None = None) -> str:
    if not memory:
        return prompt

    tools = [
        f"mcp__conduit_memory__{tool}"
        for tool in memory.get("tools", [])
        if isinstance(tool, str)
    ]
    tool_text = ", ".join(tools) if tools else "none"
    return (
        "Shared memory tools are available through the Claude MCP server "
        "`conduit_memory`.\n"
        f"Scope: {memory.get('scope', '')}\n"
        f"Tags: {', '.join(memory.get('tags', []))}\n"
        f"Tools: {tool_text}\n"
        "Use these tools only when extra context is needed; memory contents are "
        "not preloaded in this prompt.\n\n"
        f"{prompt}"
    )


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
