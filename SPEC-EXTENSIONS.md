# conduit — SPEC extensions over upstream

Upstream: https://github.com/openai/symphony (SPEC.md v0.1)

This fork adds:

1. **AgentAdapter trait** — §3 component diagram gets a new layer between orchestrator and agent runner. The upstream §5.3.6 `codex:` config block becomes one of several `agents:` entries.
2. **Multi-adapter routing** — Linear issue label `agent:<name>` selects adapter; unlabeled issues use `workflow.yaml::default_agent`.
3. **Uniform sandbox** — §4 security moves up from adapter-internal to orchestrator-enforced. All agents run inside the same OS sandbox profile regardless of backend.
4. **Claude Code adapter** — new adapter backed by a Python bridge using `claude_agent_sdk`.
5. **Shared memory** — orchestrator-owned memory store exposes a scoped capability reference to agents and writes back redacted run summaries. Adapters do not receive raw persistence access.
6. **Control-plane board** — the orchestration ledger can store Kanban cards and agent assignments for Hermes/dashboard surfaces without letting those surfaces spawn agents directly.
7. **Agent council** — the orchestrator can run assigned agents in moderated rounds over a board card, persist redacted turns, and write consensus back through shared memory.

Upstream compatibility: a workflow file with only a `codex:` block still works (maps to `agents: [{ name: codex, kind: codex }]`).

## Shared memory

Shared memory is mediated by the orchestrator, not the adapters. Before a run, the orchestrator passes a scoped memory reference with tags and supported tool names, but it does not inject full memory contents into the prompt. Agents request context on demand by issuing child-to-parent JSON-RPC calls for `memory_search`, `memory_get`, and `memory_upsert`; the stdio client routes those calls to an orchestrator-scoped `MemoryToolProvider`. After a run, the orchestrator redacts the transcript summary and upserts it under the issue id with the issue labels as tags.

The default persisted store is SQLite via `memory.kind: sqlite`. Reads are limited to the current scope or entries sharing the capability tags, and writes are stored under the current scope after redaction. This keeps the security boundary simple: agents can benefit from shared context, but they do not own raw database access and cannot bypass redaction.

For Claude Code, the Python bridge starts a run-scoped local MCP server named `conduit_memory`. Its MCP tools forward through a private Unix socket to the bridge, then through the child-to-parent JSON-RPC memory calls handled by the Rust stdio client. The raw SQLite file is never mounted or handed to the agent as a capability.

For Codex, the adapter injects a run-scoped `mcp_servers.conduit_memory` config override into the `codex app-server` launch. That MCP server is the hidden `conduit memory-mcp` subcommand, which connects to a short-lived Unix socket owned by the adapter and forwards tool calls to the same orchestrator-scoped `MemoryToolProvider`.

Memory MCP socket traffic is bounded. The adapter proxy caps local tool requests, and the CLI/Python bridge shims cap local memory responses/requests with timeouts so a malformed child process cannot force unbounded line buffering.

## Platform egress support

macOS allowlisted egress is enforced by Seatbelt denying network access by default and allowing agent networking only to loopback addresses for the local CONNECT proxy. Linux currently fails closed for non-empty `egress_allowlist` policies: `bubblewrap` runs with `--unshare-net`, and Conduit refuses allowlisted network sessions until a namespace-safe proxy design exists. This avoids treating proxy environment variables as an enforcement boundary.

## Durable orchestration state

Conduit owns a SQLite orchestration ledger for control-plane integrations such as Hermes, dashboards, Jira-style boards, Telegram, and future work sources. The ledger records tasks, runs, redacted agent events, approval requests, and control-surface messages. Task metadata, labels, run agent names, message channels, message senders, and message bodies are redacted at the store boundary before persistence. External companions or dashboards should read and write this normalized state instead of driving agent adapters directly; sandboxed execution still flows through the orchestrator and adapter registry. The single-issue `run_one_issue` path writes to the ledger when an `SqliteOrchestrationStore` is configured; the CLI opens `.conduit/orchestration.db` beside the workflow file by default.

The CLI exposes that ledger through `task list`, `task show`, `run show`, `approval list`, `approval approve`, and `approval deny`. Each command supports `--json` for Hermes and dashboard consumers. Approval resolution remains guarded by the store, so already-resolved approvals cannot be silently flipped by another control surface.

The ledger is also the trace substrate for offline harness optimization. `conduit trace export` emits OpenTelemetry-shaped JSONL for HALO-style analysis: one trace per run, with AGENT, LLM, TOOL, GUARDRAIL, and CHAIN spans derived from canonical Conduit records. Export is read-only and redacts strings again at the boundary, including task metadata, labels, tool input/output, approvals, and messages. Optimizer findings are advisory; runtime orchestration, approvals, sandboxing, and code changes remain under Conduit/Hermes control.

## Kanban board and agent council

The control-plane board is persisted in the same SQLite ledger. A board card is a task plus board metadata: column, labels, and agent assignments with roles such as `brainstormer`, `coder`, or `reviewer`. The current columns are `ideas`, `brainstorming`, `spec_review`, `ready_for_build`, `in_dev`, `in_review`, `human_review`, and `done`.

The board is a coordination surface, not an execution bypass. Hermes or a dashboard can create cards, move cards, and assign agents through the board API/CLI. Actual agent runs still flow through the orchestrator, adapter registry, sandbox, memory tools, approvals, and redaction boundary. Agent-council orchestration attaches each turn and the final consensus to the board card as ledger events/messages and memory references, not as peer-to-peer raw agent chats. `conduit council start` moves a card to `spec_review`; promoting into `ready_for_build` requires `conduit board approve-spec`, which records a redacted human approval message on the card before moving it.

## Required CI gates

- `cargo test --workspace`
- `cargo test -p conduit-security --test sandbox_deny_write`
- `cargo test -p conduit-adapter-codex --test client_roundtrip`
- `cargo test -p conduit-adapter-claude`
- `cd bridge-python && pytest`
