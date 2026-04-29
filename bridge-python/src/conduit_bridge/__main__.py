from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable

from conduit_bridge import claude_runner
from conduit_bridge.protocol import (
    decode_request,
    encode_error,
    encode_event,
    encode_response,
)


async def handle_message(
    line: str,
    write: Callable[[str], Awaitable[None]],
) -> None:
    try:
        request = decode_request(line)
    except Exception as error:
        await write(encode_error(0, -32700, f"parse error: {error}"))
        return

    if request.method != "newSession":
        await write(encode_error(request.id, -32601, f"unknown method: {request.method}"))
        return

    await write(encode_response(request.id, {"session_id": f"claude-{request.id}"}))

    async def emit(event: dict) -> None:
        await write(encode_event(event))

    try:
        await claude_runner.run_turn(
            workspace=request.params.get("workspace", "."),
            prompt=request.params.get("prompt", ""),
            model=request.params.get("model"),
            emit=emit,
        )
    except Exception as error:
        await emit({"kind": "error", "code": "runner_error", "message": str(error)})
        await emit({"kind": "session_ended", "reason": "failed"})


async def amain() -> None:
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    lock = asyncio.Lock()

    async def write(line: str) -> None:
        async with lock:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()

    while True:
        raw = await reader.readline()
        if not raw:
            break
        line = raw.decode("utf-8").rstrip()
        if line:
            asyncio.create_task(handle_message(line, write))


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
