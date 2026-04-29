# Conduit — Project Instructions

## What this is

**Conduit** is a fork of [openai/symphony](https://github.com/openai/symphony) that adds a pluggable `AgentAdapter` abstraction so the orchestrator can drive both **OpenAI Codex** and **Anthropic Claude Code** (with more adapters pluggable later), with a single OS-level sandbox enforced uniformly across every agent.

Upstream Symphony is hardcoded to drive Codex via JSON-RPC over stdio. Conduit decouples the agent runner into a trait, normalizes events into a canonical enum, and elevates sandbox + egress + approval policy above the adapter layer so every agent gets the same Seatbelt/bwrap jail, the same domain-allowlist egress proxy, the same rlimits, the same secret redaction.

Open source. Apache-2.0.

## Where to start

1. Read `docs/plans/2026-04-29-conduit-fork.md` — TDD implementation plan, phase-by-phase, bite-sized tasks with code + commands + expected output. This is the source of truth for what to build.
2. Read `SPEC-EXTENSIONS.md` once it exists (created in Phase 0 Task 0.2) — records the divergence from upstream SPEC.
3. Upstream SPEC lives at `https://github.com/openai/symphony/blob/main/SPEC.md` — reference it when adding features that touch orchestrator/state-machine behavior.

## Execution mode

Plan uses the `superpowers:writing-plans` format. Execute it with:

- **Recommended:** `superpowers:subagent-driven-development` — fresh subagent per task, review between, fast iteration.
- **Alternative:** `superpowers:executing-plans` — batch execution with checkpoints in-session.

TDD throughout. Every task is (failing test → minimal impl → passing test → commit). Do not skip the "write failing test first" step.

## Architectural invariants

Break these and the security story falls apart:

1. **Adapters never touch OS sandbox primitives directly.** They call `conduit_security::wrap::wrap_command_args` to prefix their child command. Security evolves in one crate, not scattered.
2. **Canonical events only.** Adapters map their backend events into `conduit_core::event::AgentEvent`. The orchestrator sees one schema, never backend-specific payloads.
3. **Egress goes through the local CONNECT proxy.** Child processes inherit `HTTPS_PROXY=http://127.0.0.1:$PORT`; the proxy enforces domain allowlist. Never allow an adapter to bypass.
4. **Redaction runs before transcripts leave memory.** Every `TokenDelta` / tool-call output passes through `conduit_security::redact::redact` before persistence or posting to the tracker.
5. **Workspace is the only writable path.** Sandbox profiles deny all writes except the declared workspace subtree. Logs go to the workspace too.

## Tech stack

- Rust 1.75+ workspace, 8 crates under `crates/`
- Python 3.11 bridge under `bridge-python/` (wraps `claude-agent-sdk`)
- macOS: `sandbox-exec` (Seatbelt SBPL)
- Linux: `bubblewrap` + `landlock-rs`
- Local HTTP CONNECT proxy for egress allowlist (no TLS interception; allowlist by SNI/Host only)
- `tokio` async runtime, `serde` for serialization, `clap` for CLI

## Coding conventions

- No co-authored-by lines in git commits.
- Commit messages follow conventional-commits: `feat(crate): …`, `test(crate): …`, `docs: …`, `chore: …`.
- Keep crate-boundary imports honest — `conduit-orchestrator` never imports adapter crates directly (goes through `AdapterRegistry`).
- Don't add `unsafe` without a comment explaining why.
- Tests that prove a security property (sandbox denies write, egress denies host, secrets redacted) are **required CI gates**, not optional.

## What NOT to do

- Don't add feature flags for "disable sandbox in dev" — there's no dev escape hatch. Use a narrower policy instead.
- Don't log raw transcripts. Always redact first.
- Don't import `openssl` — use `rustls` everywhere.
- Don't rename upstream-visible concepts (`AgentEvent`, `StartRequest`) without bumping `SPEC-EXTENSIONS.md` major version.

## Current state

Fresh fork, nothing implemented yet. Plan is fully drafted; execute it.
