# conduit — SPEC extensions over upstream

Upstream: https://github.com/openai/symphony (SPEC.md v0.1)

This fork adds:

1. **AgentAdapter trait** — §3 component diagram gets a new layer between orchestrator and agent runner. The upstream §5.3.6 `codex:` config block becomes one of several `agents:` entries.
2. **Multi-adapter routing** — Linear issue label `agent:<name>` selects adapter; unlabeled issues use `workflow.yaml::default_agent`.
3. **Uniform sandbox** — §4 security moves up from adapter-internal to orchestrator-enforced. All agents run inside the same OS sandbox profile regardless of backend.
4. **Claude Code adapter** — new adapter backed by a Python bridge using `claude_agent_sdk`.
5. **Shared memory** — orchestrator-owned memory store injects relevant context into agent prompts and writes back redacted run summaries. Adapters do not receive direct persistence access.

Upstream compatibility: a workflow file with only a `codex:` block still works (maps to `agents: [{ name: codex, kind: codex }]`).

## Shared memory

Shared memory is mediated by the orchestrator, not the adapters. Before a run, the orchestrator queries memory entries matching the issue labels and prepends them as a `Shared memory:` prompt block. After a run, the orchestrator redacts the transcript summary and upserts it under the issue id with the issue labels as tags.

This keeps the security boundary simple: agents can benefit from shared context, but they do not own memory persistence and cannot bypass redaction.

## Required CI gates

- `cargo test --workspace`
- `cargo test -p conduit-security --test sandbox_deny_write`
- `cargo test -p conduit-adapter-codex --test client_roundtrip`
- `cargo test -p conduit-adapter-claude`
- `cd bridge-python && pytest`
