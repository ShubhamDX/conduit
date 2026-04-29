#!/usr/bin/env bash
set -euo pipefail

while IFS= read -r line; do
  id=$(printf '%s' "$line" | python3 -c "import sys,json; data=json.loads(sys.stdin.read()); print(data['id'])")
  printf '{"jsonrpc":"2.0","id":%s,"result":{"session_id":"s1"}}\n' "$id"
  printf '{"jsonrpc":"2.0","method":"event","params":{"kind":"agent_message_delta","delta":"%s"}}\n' "${CONDUIT_TEST_ENV:-missing}"
  break
done
