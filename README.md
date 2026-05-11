# Conduit

> Multi-agent orchestrator fork of [openai/symphony](https://github.com/openai/symphony) — drives **OpenAI Codex** and **Anthropic Claude Code** behind one uniform OS-level sandbox.

**Status:** Pre-alpha. Plan drafted; implementation underway.

## Why fork

Upstream Symphony orchestrates Codex via a hardcoded `codex app-server` JSON-RPC client. Great if Codex is your only agent. Not great if you want to mix agents, compare them on the same issue, or add a new backend without patching the core.

Conduit pulls eight levers:

1. **`AgentAdapter` trait** — `start_session → event stream → stop_session`. Any backend that can talk JSON-RPC over stdio becomes an adapter.
2. **Canonical `AgentEvent` enum** — every adapter maps its backend events into the same schema. The orchestrator sees one protocol.
3. **Sandbox above the adapter** — macOS `sandbox-exec` / Linux `bwrap`+landlock + HTTP CONNECT egress allowlist proxy + rlimits + secret redaction. Every child process runs in the same jail regardless of backend.
4. **Orchestrator-owned shared memory** — agents receive a scoped memory reference and request context on demand via `memory_search`, `memory_get`, and `memory_upsert`; Codex and Claude get these as run-scoped MCP tools, and redacted run summaries are written back for later agents.
5. **Durable control-plane ledger** — live runs write tasks, runs, redacted events, approvals, and tracker messages to SQLite for dashboards and future Hermes-style orchestration.
6. **HALO-ready trace export** — the ledger can be exported as OpenTelemetry-shaped JSONL for offline harness optimization with [HALO](https://github.com/context-labs/HALO).
7. **Kanban control board** — product ideas and implementation work can be tracked as cards with columns and role-based agent assignments.
8. **Agent council orchestration** — assigned agents can run moderated brainstorming rounds with redacted turns and consensus stored in memory.

Linear issue labels (`agent:codex`, `agent:claude-code`) route to the right adapter; workflow defaults cover the rest.

## Quickstart

```bash
git clone https://github.com/<you>/conduit.git && cd conduit
cargo build --workspace --release
cd bridge-python && python -m pip install -e . && cd ..
./target/release/conduit-cli doctor              # checks deps and Linux userns support
./target/release/conduit-cli validate --workflow examples/workflow.yaml
./target/release/conduit-cli run --workflow examples/workflow.yaml --issue I-123
./target/release/conduit-cli task list --workflow examples/workflow.yaml --json
./target/release/conduit-cli board list --workflow examples/workflow.yaml --json
./target/release/conduit-cli council start --workflow examples/workflow.yaml --card product-launch --json
./target/release/conduit-cli trace export --workflow examples/workflow.yaml --out traces.jsonl
```

## HALO trace optimization

Conduit does not run HALO in the orchestration hot path. Instead, it exports redacted ledger data to HALO-compatible JSONL so an operator can analyze completed runs, review findings, and turn accepted harness changes into normal PRs.

```bash
cargo run -p conduit-cli -- trace export \
  --workflow examples/workflow.yaml \
  --out traces.jsonl \
  --project-id conduit-local \
  --service-name conduit

source bridge-python/.venv/bin/activate
python -m pip install halo-engine
halo traces.jsonl -p "Diagnose systemic Conduit agent failures and suggest harness fixes"
```

See [docs/halo.md](./docs/halo.md) for the export contract and safety boundaries.

## Control plane

Hermes, dashboards, and chat surfaces can inspect Conduit's normalized ledger through read-focused CLI commands:

```bash
conduit-cli task list --workflow examples/workflow.yaml --json
conduit-cli task show I-123 --workflow examples/workflow.yaml --json
conduit-cli run show run-... --workflow examples/workflow.yaml --json
conduit-cli approval list --workflow examples/workflow.yaml --status pending --json
conduit-cli approval approve approval-... --workflow examples/workflow.yaml --json
conduit-cli approval deny approval-... --workflow examples/workflow.yaml --json
conduit-cli board create --workflow examples/workflow.yaml --id product-launch --title "Product launch" --body "Brainstorm launch plan" --json
conduit-cli board assign product-launch --workflow examples/workflow.yaml --agent codex --role coder --model gpt-5.5 --json
conduit-cli board move product-launch --workflow examples/workflow.yaml --column brainstorming --json
conduit-cli council start --workflow examples/workflow.yaml --card product-launch --json
conduit-cli board approve-spec product-launch --workflow examples/workflow.yaml --reviewer shubham --note "Approved for implementation" --json
```

See [docs/control-plane.md](./docs/control-plane.md) for the current contract.

## Security model

- Writes denied outside the declared workspace (Seatbelt/landlock).
- Egress denied unless host matches `security.egress_allowlist`.
- CPU/memory/fd caps via `setrlimit` before `exec`.
- Regex redaction on every event before persistence/posting.
- Bounded local Memory MCP socket I/O prevents unbounded request/response buffering.
- Approval gate on destructive tool calls (`on_write` default).

See [SPEC-EXTENSIONS.md](./SPEC-EXTENSIONS.md) for the full divergence from upstream.

## Plan

Implementation plan lives at `docs/plans/2026-04-29-conduit-fork.md`. Phases 0-9, TDD throughout.

## License

Apache-2.0. Same as upstream Symphony.
