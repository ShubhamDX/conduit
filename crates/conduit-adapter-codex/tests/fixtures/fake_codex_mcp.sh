#!/usr/bin/env bash
set -euo pipefail

args="$*"
case "$args" in
  *"mcp_servers.conduit_memory.command="*"mcp_servers.conduit_memory.args="*".sock"* ) ;;
  *)
    echo "missing conduit memory MCP config in args: $args" >&2
    exit 64
    ;;
esac

while IFS= read -r line; do
  id=$(printf '%s' "$line" | python3 -c "import sys,json; data=json.loads(sys.stdin.read()); print(data['id'])")
  memory_scope=$(printf '%s' "$line" | python3 -c "import sys,json; data=json.loads(sys.stdin.read()); print(data['params']['memory']['scope'])")
  test "$memory_scope" = "issue:I1"
  printf '{"jsonrpc":"2.0","id":%s,"result":{"session_id":"s1"}}\n' "$id"
  printf '{"jsonrpc":"2.0","method":"event","params":{"kind":"agent_message_delta","delta":"mcp-ok"}}\n'
  break
done
