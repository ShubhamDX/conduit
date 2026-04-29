from __future__ import annotations

import json
import socket
import sys
from typing import Any

MAX_MEMORY_RESPONSE_BYTES = 1024 * 1024
MEMORY_RESPONSE_TIMEOUT_SECS = 5


TOOLS = [
    {
        "name": "memory_search",
        "description": "Search scoped Conduit shared memory by optional tags.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tags": {"type": "array", "items": {"type": "string"}},
                "limit": {"type": "integer", "minimum": 1, "maximum": 20},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "memory_get",
        "description": "Fetch one scoped Conduit shared-memory entry by key.",
        "inputSchema": {
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"],
            "additionalProperties": False,
        },
    },
    {
        "name": "memory_upsert",
        "description": "Write a scoped Conduit shared-memory entry.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key": {"type": "string"},
                "value": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["key", "value"],
            "additionalProperties": False,
        },
    },
]


def handle_payload(payload: dict[str, Any], socket_path: str) -> dict[str, Any] | None:
    method = payload.get("method")
    request_id = payload.get("id")

    if method == "notifications/initialized":
        return None
    if method == "initialize":
        return _response(
            request_id,
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "conduit-memory", "version": "0.1.0"},
            },
        )
    if method == "tools/list":
        return _response(request_id, {"tools": TOOLS})
    if method == "tools/call":
        params = payload.get("params", {})
        result = _call_memory(
            socket_path,
            params.get("name", ""),
            params.get("arguments", {}),
        )
        if "error" in result:
            return _response(
                request_id,
                {"content": [{"type": "text", "text": result["error"]}], "isError": True},
            )
        return _response(
            request_id,
            {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result.get("result", {}), sort_keys=True),
                    }
                ],
                "isError": False,
            },
        )

    return _error(request_id, -32601, f"unknown method: {method}")


def _call_memory(socket_path: str, method: str, params: dict[str, Any]) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(MEMORY_RESPONSE_TIMEOUT_SECS)
        client.connect(socket_path)
        client.sendall(
            json.dumps({"method": method, "params": params}).encode("utf-8") + b"\n"
        )
        chunks: list[bytes] = []
        total = 0
        while True:
            remaining = MAX_MEMORY_RESPONSE_BYTES - total
            if remaining <= 0:
                raise RuntimeError("memory response exceeds byte limit")
            chunk = client.recv(min(4096, remaining + 1))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > MAX_MEMORY_RESPONSE_BYTES:
                raise RuntimeError("memory response exceeds byte limit")
            if b"\n" in chunk:
                break
    raw = b"".join(chunks).split(b"\n", 1)[0]
    return json.loads(raw.decode("utf-8"))


def _response(request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": code, "message": message},
    }


def main() -> None:
    socket_path = sys.argv[1]
    for line in sys.stdin:
        if not line.strip():
            continue
        response = handle_payload(json.loads(line), socket_path)
        if response is not None:
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
