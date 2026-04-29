#[cfg(target_os = "macos")]
#[test]
fn write_outside_workspace_is_denied() {
    let workspace = std::env::temp_dir().join("conduit-test-ws");
    std::fs::create_dir_all(&workspace).unwrap();
    let fixture = format!(
        "{}/tests/fixtures/evil_write.sh",
        env!("CARGO_MANIFEST_DIR")
    );
    let policy = conduit_core::adapter::SecurityPolicy {
        workspace_writable: true,
        ..Default::default()
    };
    let wrapped =
        conduit_security::wrap::wrap_command_args(&workspace, &policy, "bash", &[fixture]);
    let (program, args) = wrapped.split_first().unwrap();
    let status = std::process::Command::new(program)
        .args(args)
        .status()
        .unwrap();

    assert!(!status.success(), "write was not blocked by sandbox");
}
