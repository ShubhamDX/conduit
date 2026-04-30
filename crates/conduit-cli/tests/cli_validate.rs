use std::process::Command;

use conduit_core::event::AgentEvent;
use conduit_orchestrator::state::{NewTask, RunStatus, SqliteOrchestrationStore};

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
