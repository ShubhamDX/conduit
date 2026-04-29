use std::process::Command;

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
