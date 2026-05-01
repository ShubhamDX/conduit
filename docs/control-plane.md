# Control-Plane CLI

Conduit's SQLite orchestration ledger is the boundary between secure agent execution and external control surfaces. Hermes, dashboards, Jira-style boards, and chat integrations should read or update this ledger instead of driving agent adapters directly.

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
