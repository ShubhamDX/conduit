# HALO Trace Export

Conduit can export its durable orchestration ledger as OpenTelemetry-shaped JSONL for offline analysis with [HALO](https://github.com/context-labs/HALO). This is intentionally a read-only bridge: HALO receives redacted traces, produces findings, and accepted changes still land through normal code review.

## Export

The CLI reads `.conduit/orchestration.db` from the workflow directory by default:

```bash
cargo run -p conduit-cli -- trace export \
  --workflow examples/workflow.yaml \
  --out traces.jsonl \
  --project-id conduit-local \
  --service-name conduit
```

You can point directly at a ledger file or export a single task:

```bash
cargo run -p conduit-cli -- trace export \
  --state examples/.conduit/orchestration.db \
  --task I-123 \
  --out traces-I-123.jsonl
```

When `--out` is omitted, JSONL is written to stdout.

## Analyze With HALO

Install HALO in the existing Python venv so it stays out of Conduit's Rust dependency graph:

```bash
source bridge-python/.venv/bin/activate
python -m pip install halo-engine
halo traces.jsonl -p "Diagnose systemic Conduit agent failures and suggest harness fixes"
```

The `halo-engine` package is optional and new, so keep it out of production runtime images until it is separately vetted. Conduit's exporter is useful without the package: the JSONL can also be inspected with `jq`, archived as run evidence, or fed to another trace-analysis tool.

## Trace Shape

Each run becomes one trace:

- `AGENT` root span: task metadata, labels, run id, run status, and agent name.
- `LLM` turn spans: accumulated assistant token deltas and turn token counts when available.
- `TOOL` spans: canonical tool call name, input JSON, output text, call id, and success status.
- `GUARDRAIL` spans: approval requests, risk, reason, and resolution status.
- `CHAIN` spans: outbound or inbound control-plane messages and explicit error events.

All exported strings are passed through Conduit's redactor, including task metadata and labels. The exporter never hands HALO the raw SQLite file as an agent capability.

## Safety Boundary

HALO should be treated as an optimizer, not an orchestrator. Hermes or Conduit owns scheduling, approvals, sandboxing, dashboards, Telegram/Jira control surfaces, and agent dispatch. HALO findings can suggest prompt, tool, retry, or harness changes, but those changes should be applied by Codex/Claude/Hermes as reviewable commits or PRs.
