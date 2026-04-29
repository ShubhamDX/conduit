# conduit — SPEC extensions over upstream

Upstream: https://github.com/openai/symphony (SPEC.md v0.1)

This fork adds:

1. **AgentAdapter trait** — §3 component diagram gets a new layer between orchestrator and agent runner. The upstream §5.3.6 `codex:` config block becomes one of several `agents:` entries.
2. **Multi-adapter routing** — Linear issue label `agent:<name>` selects adapter; unlabeled issues use `workflow.yaml::default_agent`.
3. **Uniform sandbox** — §4 security moves up from adapter-internal to orchestrator-enforced. All agents run inside the same OS sandbox profile regardless of backend.
4. **Claude Code adapter** — new adapter backed by a Python bridge using `claude_agent_sdk`.

Upstream compatibility: a workflow file with only a `codex:` block still works (maps to `agents: [{ name: codex, kind: codex }]`).
