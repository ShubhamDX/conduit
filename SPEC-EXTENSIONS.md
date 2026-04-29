# conduit — SPEC extensions over upstream

Upstream: https://github.com/openai/symphony (SPEC.md v0.1)

This fork adds:

1. **AgentAdapter trait** — §3 component diagram gets a new layer between orchestrator and agent runner. The upstream §5.3.6 `codex:` config block becomes one of several `agents:` entries.
2. **Multi-adapter routing** — Linear issue label `agent:<name>` selects adapter; unlabeled issues use `workflow.yaml::default_agent`.
3. **Uniform sandbox** — §4 security moves up from adapter-internal to orchestrator-enforced. All agents run inside the same OS sandbox profile regardless of backend.
4. **Claude Code adapter** — new adapter backed by a Python bridge using `claude_agent_sdk`.
5. **Shared memory** — orchestrator-owned memory store exposes a scoped capability reference to agents and writes back redacted run summaries. Adapters do not receive raw persistence access.

Upstream compatibility: a workflow file with only a `codex:` block still works (maps to `agents: [{ name: codex, kind: codex }]`).

## Shared memory

Shared memory is mediated by the orchestrator, not the adapters. Before a run, the orchestrator passes a scoped memory reference with tags and supported tool names, but it does not inject full memory contents into the prompt. Agents request context on demand by issuing child-to-parent JSON-RPC calls for `memory_search`, `memory_get`, and `memory_upsert`; the stdio client routes those calls to an orchestrator-scoped `MemoryToolProvider`. After a run, the orchestrator redacts the transcript summary and upserts it under the issue id with the issue labels as tags.

The default persisted store is SQLite via `memory.kind: sqlite`. Reads are limited to the current scope or entries sharing the capability tags, and writes are stored under the current scope after redaction. This keeps the security boundary simple: agents can benefit from shared context, but they do not own raw database access and cannot bypass redaction.

For Claude Code, the Python bridge starts a run-scoped local MCP server named `conduit_memory`. Its MCP tools forward through a private Unix socket to the bridge, then through the child-to-parent JSON-RPC memory calls handled by the Rust stdio client. The raw SQLite file is never mounted or handed to the agent as a capability.

For Codex, the adapter injects a run-scoped `mcp_servers.conduit_memory` config override into the `codex app-server` launch. That MCP server is the hidden `conduit memory-mcp` subcommand, which connects to a short-lived Unix socket owned by the adapter and forwards tool calls to the same orchestrator-scoped `MemoryToolProvider`.

## Required CI gates

- `cargo test --workspace`
- `cargo test -p conduit-security --test sandbox_deny_write`
- `cargo test -p conduit-adapter-codex --test client_roundtrip`
- `cargo test -p conduit-adapter-claude`
- `cd bridge-python && pytest`
