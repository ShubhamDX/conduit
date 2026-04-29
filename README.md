# Conduit

> Multi-agent orchestrator fork of [openai/symphony](https://github.com/openai/symphony) — drives **OpenAI Codex** and **Anthropic Claude Code** behind one uniform OS-level sandbox.

**Status:** Pre-alpha. Plan drafted; implementation underway.

## Why fork

Upstream Symphony orchestrates Codex via a hardcoded `codex app-server` JSON-RPC client. Great if Codex is your only agent. Not great if you want to mix agents, compare them on the same issue, or add a new backend without patching the core.

Conduit pulls four levers:

1. **`AgentAdapter` trait** — `start_session → event stream → stop_session`. Any backend that can talk JSON-RPC over stdio becomes an adapter.
2. **Canonical `AgentEvent` enum** — every adapter maps its backend events into the same schema. The orchestrator sees one protocol.
3. **Sandbox above the adapter** — macOS `sandbox-exec` / Linux `bwrap`+landlock + HTTP CONNECT egress allowlist proxy + rlimits + secret redaction. Every child process runs in the same jail regardless of backend.
4. **Orchestrator-owned shared memory** — agents receive a scoped memory reference and request context on demand via `memory_search`, `memory_get`, and `memory_upsert` JSON-RPC calls; redacted run summaries are written back for later agents.

Linear issue labels (`agent:codex`, `agent:claude-code`) route to the right adapter; workflow defaults cover the rest.

## Quickstart

```bash
git clone https://github.com/<you>/conduit.git && cd conduit
cargo build --workspace --release
cd bridge-python && python -m pip install -e . && cd ..
./target/release/conduit-cli doctor              # checks sandbox-exec/bwrap/codex/python3 on PATH
./target/release/conduit-cli validate --workflow examples/workflow.yaml
./target/release/conduit-cli run --workflow examples/workflow.yaml --issue I-123
```

## Security model

- Writes denied outside the declared workspace (Seatbelt/landlock).
- Egress denied unless host matches `security.egress_allowlist`.
- CPU/memory/fd caps via `setrlimit` before `exec`.
- Regex redaction on every event before persistence/posting.
- Approval gate on destructive tool calls (`on_write` default).

See [SPEC-EXTENSIONS.md](./SPEC-EXTENSIONS.md) for the full divergence from upstream.

## Plan

Implementation plan lives at `docs/plans/2026-04-29-conduit-fork.md`. Phases 0-9, TDD throughout.

## License

Apache-2.0. Same as upstream Symphony.
