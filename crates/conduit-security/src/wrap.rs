use conduit_core::adapter::SecurityPolicy;
use std::path::{Path, PathBuf};

pub struct WrappedCommand {
    argv: Vec<String>,
    cleanup_paths: Vec<PathBuf>,
}

impl WrappedCommand {
    pub fn program_and_args(&self) -> Option<(&str, &[String])> {
        let (program, args) = self.argv.split_first()?;
        Some((program.as_str(), args))
    }

    pub fn argv(&self) -> &[String] {
        &self.argv
    }
}

impl Drop for WrappedCommand {
    fn drop(&mut self) {
        for path in &self.cleanup_paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(target_os = "macos")]
pub fn wrap_command(
    workspace: &Path,
    policy: &SecurityPolicy,
    program: &str,
    program_args: &[String],
) -> std::io::Result<WrappedCommand> {
    let profile =
        crate::sandbox_macos::write_profile_to_tempfile(workspace, policy.workspace_writable)?;
    let mut out = vec![
        "sandbox-exec".to_string(),
        "-f".to_string(),
        profile.display().to_string(),
        program.to_string(),
    ];
    out.extend(program_args.iter().cloned());
    Ok(WrappedCommand {
        argv: out,
        cleanup_paths: vec![profile],
    })
}

#[cfg(target_os = "linux")]
pub fn wrap_command(
    workspace: &Path,
    policy: &SecurityPolicy,
    program: &str,
    program_args: &[String],
) -> std::io::Result<WrappedCommand> {
    let mut out = vec!["bwrap".to_string()];
    out.extend(crate::sandbox_linux::build_bwrap_args(
        workspace,
        policy.workspace_writable,
        true,
    ));
    out.push("--".to_string());
    out.push(program.to_string());
    out.extend(program_args.iter().cloned());
    Ok(WrappedCommand {
        argv: out,
        cleanup_paths: Vec::new(),
    })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn wrap_command(
    _workspace: &Path,
    _policy: &SecurityPolicy,
    program: &str,
    program_args: &[String],
) -> std::io::Result<WrappedCommand> {
    let mut out = vec![program.to_string()];
    out.extend(program_args.iter().cloned());
    Ok(WrappedCommand {
        argv: out,
        cleanup_paths: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::wrap_command;
    use conduit_core::adapter::SecurityPolicy;
    use std::path::Path;

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_prefixes_with_sandbox_exec_and_f_flag() {
        let wrapped = wrap_command(
            Path::new("/tmp/w"),
            &SecurityPolicy::default(),
            "codex",
            &["app-server".into()],
        )
        .unwrap();
        let args = wrapped.argv();

        assert_eq!(args[0], "sandbox-exec");
        assert_eq!(args[1], "-f");
        assert_eq!(args[3], "codex");
        assert_eq!(args[4], "app-server");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_profile_is_deleted_when_wrapper_drops() {
        let profile_path = {
            let wrapped = wrap_command(
                Path::new("/tmp/w"),
                &SecurityPolicy::default(),
                "codex",
                &["app-server".into()],
            )
            .unwrap();
            let path = std::path::PathBuf::from(&wrapped.argv()[2]);
            assert!(path.exists());
            path
        };

        assert!(!profile_path.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_prefixes_with_bwrap() {
        let wrapped = wrap_command(
            Path::new("/tmp/w"),
            &SecurityPolicy::default(),
            "codex",
            &["app-server".into()],
        )
        .unwrap();
        let args = wrapped.argv();

        assert_eq!(args[0], "bwrap");
        assert!(args.iter().any(|arg| arg == "codex"));
    }
}
