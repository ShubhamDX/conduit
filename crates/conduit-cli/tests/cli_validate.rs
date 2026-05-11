use std::process::Command;

use conduit_core::event::{AgentEvent, Risk};
use conduit_orchestrator::state::{
    MessageDirection, NewMessage, NewTask, RunStatus, SqliteOrchestrationStore,
};

#[test]
fn validate_good_workflow_exits_zero() {
    let fixture_dir = format!("{}/tests/fixtures", env!("CARGO_MANIFEST_DIR"));
    let path = format!("{fixture_dir}/workflow_good.yaml");
    std::fs::create_dir_all(&fixture_dir).unwrap();
    std::fs::write(
        &path,
        r#"
workspace: "./repo"
assignee: "bot"
default_agent: "codex"
security:
  egress_allowlist: []
  workspace_writable: true
  redact_secrets: true
agents:
  - name: codex
    kind: codex
    program: codex
"#,
    )
    .unwrap();

    let binary = env!("CARGO_BIN_EXE_conduit-cli");
    let output = Command::new(binary)
        .args(["validate", "--workflow", &path])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn validate_bad_workflow_exits_nonzero() {
    let fixture_dir = format!("{}/tests/fixtures", env!("CARGO_MANIFEST_DIR"));
    let path = format!("{fixture_dir}/workflow_bad.yaml");
    std::fs::create_dir_all(&fixture_dir).unwrap();
    std::fs::write(&path, "[not").unwrap();

    let binary = env!("CARGO_BIN_EXE_conduit-cli");
    let output = Command::new(binary)
        .args(["validate", "--workflow", &path])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[test]
fn trace_export_outputs_halo_jsonl_from_state() {
    let path = unique_db_path("trace-export");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let store = SqliteOrchestrationStore::open(&path).unwrap();
        let task = store
            .create_task(NewTask {
                id: "task-cli".into(),
                source: "tracker".into(),
                title: "Trace export".into(),
                body: "Make HALO useful".into(),
                labels: vec!["agent:codex".into()],
            })
            .await
            .unwrap();
        let run = store.start_run(&task.id, "codex").await.unwrap();
        store
            .record_event(
                &run.id,
                AgentEvent::TokenDelta {
                    text: "trace body".into(),
                },
            )
            .await
            .unwrap();
        store
            .record_event(
                &run.id,
                AgentEvent::TurnCompleted {
                    tokens_in: 3,
                    tokens_out: 2,
                },
            )
            .await
            .unwrap();
        store
            .finish_run(&run.id, RunStatus::Succeeded)
            .await
            .unwrap();
    });

    let binary = env!("CARGO_BIN_EXE_conduit-cli");
    let output = Command::new(binary)
        .args([
            "trace",
            "export",
            "--state",
            path.to_str().unwrap(),
            "--project-id",
            "conduit-cli-test",
            "--service-name",
            "conduit-cli-test",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines = stdout.lines().collect::<Vec<_>>();
    assert!(lines.len() >= 2, "stdout: {stdout}");
    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["name"], "conduit.run.codex");
    assert_eq!(
        first["attributes"]["inference.project_id"],
        "conduit-cli-test"
    );
    assert!(stdout.contains("\"name\":\"conduit.llm.turn\""));

    let _ = std::fs::remove_file(path);
}

#[test]
fn control_plane_commands_expose_ledger_json() {
    let path = unique_db_path("control-plane");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (run_id, approval_id, denied_approval_id) = runtime.block_on(async {
        let store = SqliteOrchestrationStore::open(&path).unwrap();
        let task = store
            .create_task(NewTask {
                id: "task-control".into(),
                source: "jira".into(),
                title: "Control plane".into(),
                body: "Expose dashboard state with sk-proj-abc123XYZ456def789GHJ012".into(),
                labels: vec!["agent:codex".into(), "project:hermes".into()],
            })
            .await
            .unwrap();
        let run = store.start_run(&task.id, "codex").await.unwrap();
        store
            .record_event(
                &run.id,
                AgentEvent::TokenDelta {
                    text: "working".into(),
                },
            )
            .await
            .unwrap();
        let approval = store
            .request_approval(&run.id, "write generated files", Risk::Medium)
            .await
            .unwrap();
        let denied_approval = store
            .request_approval(&run.id, "open external browser", Risk::High)
            .await
            .unwrap();
        store
            .record_message(NewMessage {
                task_id: Some(task.id.clone()),
                run_id: Some(run.id.clone()),
                channel: "telegram".into(),
                sender: "hermes".into(),
                direction: MessageDirection::Inbound,
                body: "status?".into(),
            })
            .await
            .unwrap();
        (run.id, approval.id, denied_approval.id)
    });

    let binary = env!("CARGO_BIN_EXE_conduit-cli");
    let state = path.to_str().unwrap();

    let tasks = run_json(binary, &["task", "list", "--state", state, "--json"]);
    assert_eq!(tasks[0]["id"], "task-control");
    assert_eq!(tasks[0]["status"], "running");

    let task = run_json(
        binary,
        &["task", "show", "task-control", "--state", state, "--json"],
    );
    assert_eq!(task["task"]["title"], "Control plane");
    assert_eq!(
        task["task"]["body"],
        "Expose dashboard state with sk-proj-[REDACTED]"
    );
    assert_eq!(task["runs"][0]["id"], run_id);
    assert_eq!(task["approvals"].as_array().unwrap().len(), 2);
    assert_eq!(task["messages"][0]["channel"], "telegram");

    let run = run_json(
        binary,
        &["run", "show", &run_id, "--state", state, "--json"],
    );
    assert_eq!(run["task"]["id"], "task-control");
    assert_eq!(run["run"]["agent"], "codex");
    assert_eq!(run["events"][0]["event_type"], "token_delta");

    let approvals = run_json(binary, &["approval", "list", "--state", state, "--json"]);
    assert_eq!(approvals.as_array().unwrap().len(), 2);

    let pending = run_json(
        binary,
        &[
            "approval", "list", "--state", state, "--status", "pending", "--json",
        ],
    );
    assert_eq!(pending.as_array().unwrap().len(), 2);

    let approved = run_json(
        binary,
        &[
            "approval",
            "approve",
            &approval_id,
            "--state",
            state,
            "--json",
        ],
    );
    assert_eq!(approved["status"], "approved");
    assert!(approved["resolved_at_ms"].is_number());

    let denied = run_json(
        binary,
        &[
            "approval",
            "deny",
            &denied_approval_id,
            "--state",
            state,
            "--json",
        ],
    );
    assert_eq!(denied["status"], "denied");

    let pending = run_json(
        binary,
        &[
            "approval", "list", "--state", state, "--status", "pending", "--json",
        ],
    );
    assert_eq!(pending.as_array().unwrap().len(), 0);

    let _ = std::fs::remove_file(path);
}

#[test]
fn board_commands_manage_cards_columns_and_assignments() {
    let path = unique_db_path("board");
    let binary = env!("CARGO_BIN_EXE_conduit-cli");
    let state = path.to_str().unwrap();

    let created = run_json(
        binary,
        &[
            "board",
            "create",
            "--state",
            state,
            "--id",
            "product-launch",
            "--title",
            "New product launch",
            "--body",
            "Brainstorm with sk-proj-abc123XYZ456def789GHJ012",
            "--label",
            "product:new",
            "--label",
            "council",
            "--json",
        ],
    );
    assert_eq!(created["task"]["id"], "product-launch");
    assert_eq!(created["column"], "ideas");
    assert_eq!(
        created["task"]["body"],
        "Brainstorm with sk-proj-[REDACTED]"
    );

    let assigned = run_json(
        binary,
        &[
            "board",
            "assign",
            "product-launch",
            "--state",
            state,
            "--agent",
            "codex",
            "--role",
            "coder",
            "--model",
            "gpt-5.5",
            "--json",
        ],
    );
    assert_eq!(assigned["assignments"][0]["agent"], "codex");
    assert_eq!(assigned["assignments"][0]["role"], "coder");
    assert_eq!(assigned["assignments"][0]["model"], "gpt-5.5");

    let assigned = run_json(
        binary,
        &[
            "board",
            "assign",
            "product-launch",
            "--state",
            state,
            "--agent",
            "claude-code",
            "--role",
            "brainstormer",
            "--model",
            "opus-4.7",
            "--json",
        ],
    );
    assert_eq!(assigned["assignments"].as_array().unwrap().len(), 2);

    let moved = run_json(
        binary,
        &[
            "board",
            "move",
            "product-launch",
            "--state",
            state,
            "--column",
            "brainstorming",
            "--json",
        ],
    );
    assert_eq!(moved["column"], "brainstorming");

    let cards = run_json(binary, &["board", "list", "--state", state, "--json"]);
    assert_eq!(cards[0]["task"]["id"], "product-launch");
    assert_eq!(cards[0]["assignments"].as_array().unwrap().len(), 2);

    let shown = run_json(
        binary,
        &[
            "board",
            "show",
            "product-launch",
            "--state",
            state,
            "--json",
        ],
    );
    assert_eq!(shown["column"], "brainstorming");
    let serialized = serde_json::to_string(&shown).unwrap();
    assert!(!serialized.contains("abc123"));

    let spec_review = run_json(
        binary,
        &[
            "board",
            "move",
            "product-launch",
            "--state",
            state,
            "--column",
            "spec_review",
            "--json",
        ],
    );
    assert_eq!(spec_review["column"], "spec_review");

    let direct_ready = Command::new(binary)
        .args([
            "board",
            "move",
            "product-launch",
            "--state",
            state,
            "--column",
            "ready_for_build",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!direct_ready.status.success());
    let stderr = String::from_utf8_lossy(&direct_ready.stderr);
    assert!(stderr.contains("board approve-spec"), "stderr: {stderr}");

    let approved = run_json(
        binary,
        &[
            "board",
            "approve-spec",
            "product-launch",
            "--state",
            state,
            "--reviewer",
            "shubham",
            "--note",
            "Good to build with sk-proj-abc123XYZ456def789GHJ012",
            "--json",
        ],
    );
    assert_eq!(approved["column"], "ready_for_build");

    let task = run_json(
        binary,
        &["task", "show", "product-launch", "--state", state, "--json"],
    );
    assert_eq!(task["messages"][0]["channel"], "board");
    assert_eq!(task["messages"][0]["sender"], "shubham");
    assert_eq!(task["messages"][0]["direction"], "inbound");
    assert!(task["messages"][0]["body"]
        .as_str()
        .unwrap()
        .contains("Spec approved"));
    assert!(!serde_json::to_string(&task).unwrap().contains("abc123"));

    let _ = std::fs::remove_file(path);
}

#[test]
fn council_start_rejects_without_workflow_and_requires_existing_card() {
    let path = unique_db_path("council");
    let binary = env!("CARGO_BIN_EXE_conduit-cli");
    let state = path.to_str().unwrap();

    let output = Command::new(binary)
        .args(["council", "start", "--state", state, "--card", "missing"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--workflow required for council start"),
        "stderr: {stderr}"
    );

    let _ = std::fs::remove_file(path);
}

#[test]
fn build_start_rejects_without_workflow() {
    let path = unique_db_path("build-start");
    let binary = env!("CARGO_BIN_EXE_conduit-cli");
    let state = path.to_str().unwrap();

    let output = Command::new(binary)
        .args(["build", "start", "--state", state, "--card", "missing"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--workflow required for build start"),
        "stderr: {stderr}"
    );

    let _ = std::fs::remove_file(path);
}

fn run_json(binary: &str, args: &[&str]) -> serde_json::Value {
    let output = Command::new(binary).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "args: {args:?}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn unique_db_path(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "conduit-cli-{label}-{}-{nanos}.db",
        std::process::id()
    ))
}
