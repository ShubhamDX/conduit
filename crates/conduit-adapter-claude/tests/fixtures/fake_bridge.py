import json
import sys

line = sys.stdin.readline()
request = json.loads(line)
sys.stdout.write(
    json.dumps(
        {
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {"session_id": "claude-1"},
        }
    )
    + "\n"
)
sys.stdout.write(
    json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "event",
            "params": {"kind": "agent_message_delta", "delta": "hello"},
        }
    )
    + "\n"
)
sys.stdout.flush()
