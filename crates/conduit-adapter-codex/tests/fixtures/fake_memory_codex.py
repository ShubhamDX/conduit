#!/usr/bin/env python3
import json
import sys


new_session = json.loads(sys.stdin.readline())
print(
    json.dumps(
        {
            "jsonrpc": "2.0",
            "id": new_session["id"],
            "result": {"session_id": "s1"},
        }
    ),
    flush=True,
)

print(
    json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 99,
            "method": "memory_get",
            "params": {"key": "k"},
        }
    ),
    flush=True,
)

memory_response = json.loads(sys.stdin.readline())
value = memory_response["result"]["entry"]["value"]
print(
    json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "event",
            "params": {"kind": "agent_message_delta", "delta": value},
        }
    ),
    flush=True,
)
