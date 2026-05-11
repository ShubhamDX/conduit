# Control-Plane CLI

Conduit's SQLite orchestration ledger is the boundary between secure agent execution and external control surfaces. Hermes, dashboards, Jira-style boards, and chat integrations should read or update this ledger instead of driving agent adapters directly. Store writes redact task metadata, labels, run agent names, message channels, senders, and message bodies before persistence.

The CLI resolves `.conduit/orchestration.db` beside a workflow when `--workflow` is provided. Use `--state` to point directly at a ledger file.

## Tasks

```bash
conduit-cli task list --workflow examples/workflow.yaml --json
conduit-cli task show I-123 --workflow examples/workflow.yaml --json
```

`task list` returns task records. `task show` returns a task snapshot with runs, events, approvals, and messages.

## Runs

```bash
conduit-cli run show run-... --workflow examples/workflow.yaml --json
```

`run show` returns the run, its parent task, run events, approval requests, and control-plane messages scoped to that run.

## Approvals

```bash
conduit-cli approval list --workflow examples/workflow.yaml --status pending --json
conduit-cli approval approve approval-... --workflow examples/workflow.yaml --json
conduit-cli approval deny approval-... --workflow examples/workflow.yaml --json
```

Approval resolution is idempotency-guarded by the store: only pending approvals can transition to `approved` or `denied`.

## JSON Mode

Pass `--json` for machine-readable output. Without `--json`, commands emit compact tab-separated summaries for local inspection.

The control-plane commands do not expose the raw memory database or agent adapter sessions. They operate only on Conduit's normalized, redacted orchestration ledger.

Memory MCP traffic stays local and bounded. The Codex adapter proxy, CLI MCP shim, and Claude bridge proxy cap local socket request/response size and apply timeouts so malformed children cannot force unbounded buffering.

## Board

```bash
conduit-cli board create \
  --workflow examples/workflow.yaml \
  --id product-launch \
  --title "Product launch" \
  --body "Brainstorm positioning, feature scope, and build plan" \
  --label product:new \
  --json

conduit-cli board assign product-launch \
  --workflow examples/workflow.yaml \
  --agent codex \
  --role coder \
  --model gpt-5.5 \
  --json

conduit-cli board move product-launch \
  --workflow examples/workflow.yaml \
  --column brainstorming \
  --json

conduit-cli board list --workflow examples/workflow.yaml --json
conduit-cli board show product-launch --workflow examples/workflow.yaml --json
```

Board columns are `ideas`, `brainstorming`, `spec_review`, `ready_for_build`, `in_dev`, `in_review`, `human_review`, and `done`.

The board is a coordination surface only. It stores card metadata and assignments, but it does not spawn agents directly or bypass sandbox, egress, approval, memory, or redaction policy.

Cards cannot be moved directly into `ready_for_build`. After reviewing the council output, use the guarded spec approval command so the ledger records who approved the handoff:

```bash
conduit-cli board approve-spec product-launch \
  --workflow examples/workflow.yaml \
  --reviewer shubham \
  --note "Scope and guardrails approved for implementation" \
  --json
```

## Council

```bash
conduit-cli council start \
  --workflow examples/workflow.yaml \
  --card product-launch \
  --max-rounds 1 \
  --json
```

`council start` reads a board card's assignments and runs one moderated adapter session per assigned agent for each round. Each turn is persisted as redacted ledger events and `council` messages linked to the card. The final consensus is written to shared memory as `council:<card>:consensus`, then the card moves to `spec_review`.

The council does not move cards to `ready_for_build`; that transition is reserved for `board approve-spec` after human review.
