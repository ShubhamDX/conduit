from __future__ import annotations

import asyncio
import json
import sys
import tempfile
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

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
    memory_call: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]] | None = None,
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
        memory = request.params.get("memory")
        if memory and memory_call is not None:
            async with MemoryMcpProxy(memory_call) as proxy:
                await claude_runner.run_turn(
                    workspace=request.params.get("workspace", "."),
                    prompt=request.params.get("prompt", ""),
                    model=request.params.get("model"),
                    emit=emit,
                    memory=memory,
                    memory_socket=proxy.socket_path,
                )
        else:
            await claude_runner.run_turn(
                workspace=request.params.get("workspace", "."),
                prompt=request.params.get("prompt", ""),
                model=request.params.get("model"),
                emit=emit,
                memory=memory,
                memory_socket=None,
            )
    except Exception as error:
        await emit({"kind": "error", "code": "runner_error", "message": str(error)})
        await emit({"kind": "session_ended", "reason": "failed"})


class ParentRpc:
    def __init__(
        self,
        write: Callable[[str], Awaitable[None]],
    ) -> None:
        self._write = write
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._next_id = 100_000

    async def request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        request_id = self._next_id
        self._next_id += 1
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[request_id] = future
        await self._write(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params,
                }
            )
        )
        payload = await future
        if "error" in payload:
            error = payload["error"]
            raise RuntimeError(error.get("message", "memory tool call failed"))
        return payload.get("result", {})

    async def dispatch(self, line: str) -> None:
        try:
            payload = json.loads(line)
        except Exception:
            await handle_message(line, self._write, memory_call=self.request)
            return

        if "id" in payload and "method" not in payload:
            future = self._pending.pop(int(payload["id"]), None)
            if future is not None and not future.done():
                future.set_result(payload)
                return

        await handle_message(line, self._write, memory_call=self.request)


class MemoryMcpProxy:
    def __init__(
        self,
        memory_call: Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> None:
        self._memory_call = memory_call
        self._server: asyncio.AbstractServer | None = None
        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None
        self.socket_path = ""

    async def __aenter__(self) -> "MemoryMcpProxy":
        self._tmpdir = tempfile.TemporaryDirectory(prefix="conduit-memory-", dir="/tmp")
        self.socket_path = str(Path(self._tmpdir.name) / "memory.sock")
        self._server = await asyncio.start_unix_server(
            self._handle_client,
            path=self.socket_path,
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._tmpdir is not None:
            self._tmpdir.cleanup()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            raw = await reader.readline()
            if not raw:
                return
            payload = json.loads(raw.decode("utf-8"))
            result = await self._memory_call(
                payload.get("method", ""),
                payload.get("params", {}),
            )
            writer.write(json.dumps({"result": result}).encode("utf-8") + b"\n")
            await writer.drain()
        except Exception as error:
            writer.write(json.dumps({"error": str(error)}).encode("utf-8") + b"\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


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

    parent = ParentRpc(write)
    tasks: set[asyncio.Task[None]] = set()
    while True:
        raw = await reader.readline()
        if not raw:
            break
        line = raw.decode("utf-8").rstrip()
        if line:
            task = asyncio.create_task(parent.dispatch(line))
            tasks.add(task)
            task.add_done_callback(tasks.discard)


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
