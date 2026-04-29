use conduit_core::adapter::SecurityPolicy;
use std::path::Path;

#[cfg(target_os = "macos")]
pub fn wrap_command_args(
    workspace: &Path,
    policy: &SecurityPolicy,
    program: &str,
    program_args: &[String],
) -> Vec<String> {
    let profile =
        crate::sandbox_macos::write_profile_to_tempfile(workspace, policy.workspace_writable)
            .expect("write sandbox profile");
    let mut out = vec![
        "sandbox-exec".to_string(),
        "-f".to_string(),
        profile.display().to_string(),
        program.to_string(),
    ];
    out.extend(program_args.iter().cloned());
    out
}

#[cfg(target_os = "linux")]
pub fn wrap_command_args(
    workspace: &Path,
    policy: &SecurityPolicy,
    program: &str,
    program_args: &[String],
) -> Vec<String> {
    let mut out = vec!["bwrap".to_string()];
    out.extend(crate::sandbox_linux::build_bwrap_args(
        workspace,
        policy.workspace_writable,
        policy.egress_allowlist.is_empty(),
    ));
    out.push("--".to_string());
    out.push(program.to_string());
    out.extend(program_args.iter().cloned());
    out
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn wrap_command_args(
    _workspace: &Path,
    _policy: &SecurityPolicy,
    program: &str,
    program_args: &[String],
) -> Vec<String> {
    let mut out = vec![program.to_string()];
    out.extend(program_args.iter().cloned());
    out
}

#[cfg(test)]
mod tests {
    use super::wrap_command_args;
    use conduit_core::adapter::SecurityPolicy;
    use std::path::Path;

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_prefixes_with_sandbox_exec_and_f_flag() {
        let args = wrap_command_args(
            Path::new("/tmp/w"),
            &SecurityPolicy::default(),
            "codex",
            &["app-server".into()],
        );

        assert_eq!(args[0], "sandbox-exec");
        assert_eq!(args[1], "-f");
        assert_eq!(args[3], "codex");
        assert_eq!(args[4], "app-server");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_prefixes_with_bwrap() {
        let args = wrap_command_args(
            Path::new("/tmp/w"),
            &SecurityPolicy::default(),
            "codex",
            &["app-server".into()],
        );

        assert_eq!(args[0], "bwrap");
        assert!(args.iter().any(|arg| arg == "codex"));
    }
}
