#[cfg(target_os = "macos")]
#[test]
fn non_loopback_network_is_denied() {
    let workspace = TempWorkspace::new("conduit-test-net-ws");
    let policy = conduit_core::adapter::SecurityPolicy {
        workspace_writable: true,
        ..Default::default()
    };
    let code = r#"
import socket
socket.create_connection(("1.1.1.1", 80), timeout=1)
"#;
    let wrapped = conduit_security::wrap::wrap_command(
        workspace.path(),
        &policy,
        "python3",
        &["-c".into(), code.into()],
    )
    .unwrap();
    let (program, args) = wrapped.program_and_args().unwrap();
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !stderr.contains("sandbox-exec:"),
        "sandbox profile failed to load: {stderr}"
    );
    assert!(
        !output.status.success(),
        "non-loopback network escaped sandbox"
    );
}

#[cfg(target_os = "macos")]
struct TempWorkspace(std::path::PathBuf);

#[cfg(target_os = "macos")]
impl TempWorkspace {
    fn new(prefix: &str) -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

#[cfg(target_os = "macos")]
impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
