from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass
class Request:
    id: int
    method: str
    params: dict[str, Any]


def decode_request(line: str) -> Request:
    payload = json.loads(line)
    return Request(
        id=int(payload["id"]),
        method=payload["method"],
        params=payload.get("params", {}),
    )


def encode_response(id: int, result: Any) -> str:
    return json.dumps({"jsonrpc": "2.0", "id": id, "result": result})


def encode_error(id: int, code: int, message: str) -> str:
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message},
        }
    )


def encode_event(params: dict[str, Any]) -> str:
    return json.dumps({"jsonrpc": "2.0", "method": "event", "params": params})
