use std::path::Path;
use std::process::Command;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserNamespaceCheck {
    pub ok: bool,
    pub message: String,
}

pub fn build_bwrap_args(workspace: &Path, writable: bool, disable_network: bool) -> Vec<String> {
    let workspace = workspace.display().to_string();
    let mut args = vec![
        "--ro-bind".into(),
        "/".into(),
        "/".into(),
        "--proc".into(),
        "/proc".into(),
        "--dev".into(),
        "/dev".into(),
        "--tmpfs".into(),
        "/tmp".into(),
        "--unshare-user".into(),
        "--unshare-pid".into(),
        "--unshare-ipc".into(),
        "--unshare-uts".into(),
        "--die-with-parent".into(),
    ];

    if disable_network {
        args.push("--unshare-net".into());
    }

    if writable {
        args.push("--bind".into());
        args.push(workspace.clone());
        args.push(workspace);
    }

    args
}

pub fn probe_user_namespace() -> UserNamespaceCheck {
    if let Ok(value) = std::fs::read_to_string("/proc/sys/kernel/unprivileged_userns_clone") {
        if let Some(message) = userns_disabled_by_sysctl(Some(&value)) {
            return UserNamespaceCheck { ok: false, message };
        }
    }

    match Command::new("bwrap")
        .args([
            "--unshare-user",
            "--uid",
            "0",
            "--gid",
            "0",
            "--ro-bind",
            "/",
            "/",
            "/bin/sh",
            "-c",
            "true",
        ])
        .output()
    {
        Ok(output) => classify_userns_probe(
            output.status.success(),
            &String::from_utf8_lossy(&output.stderr),
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => UserNamespaceCheck {
            ok: false,
            message: "bwrap not found on PATH; install bubblewrap for Linux sandboxing".into(),
        },
        Err(error) => UserNamespaceCheck {
            ok: false,
            message: format!("failed to run bwrap --unshare-user probe: {error}"),
        },
    }
}

fn userns_disabled_by_sysctl(value: Option<&str>) -> Option<String> {
    match value.map(str::trim) {
        Some("0") => Some(
            "kernel.unprivileged_userns_clone=0; bwrap --unshare-user will fail unless user namespaces are enabled or a distro-supported bubblewrap setup is installed"
                .into(),
        ),
        _ => None,
    }
}

fn classify_userns_probe(ok: bool, stderr: &str) -> UserNamespaceCheck {
    if ok {
        return UserNamespaceCheck {
            ok: true,
            message: "ok: bwrap --unshare-user probe succeeded".into(),
        };
    }

    let stderr = stderr.trim();
    let detail = if stderr.is_empty() {
        "no stderr".to_string()
    } else {
        stderr.to_string()
    };
    UserNamespaceCheck {
        ok: false,
        message: format!("bwrap --unshare-user probe failed: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{build_bwrap_args, classify_userns_probe, userns_disabled_by_sysctl};
    use std::path::Path;

    #[test]
    fn args_include_ro_bind_root() {
        let args = build_bwrap_args(Path::new("/home/u/work"), true, false);
        assert!(args.windows(2).any(|window| window == ["--ro-bind", "/"]));
    }

    #[test]
    fn args_include_rw_bind_workspace() {
        let args = build_bwrap_args(Path::new("/home/u/work"), true, false);
        assert!(args
            .windows(3)
            .any(|window| window[0] == "--bind" && window[1] == "/home/u/work"));
    }

    #[test]
    fn no_rw_when_workspace_not_writable() {
        let args = build_bwrap_args(Path::new("/home/u/work"), false, false);
        assert!(!args
            .windows(3)
            .any(|window| window[0] == "--bind" && window[1] == "/home/u/work"));
    }

    #[test]
    fn can_unshare_network_namespace() {
        let args = build_bwrap_args(Path::new("/home/u/work"), true, true);
        assert!(args.iter().any(|arg| arg == "--unshare-net"));
    }

    #[test]
    fn sysctl_zero_reports_user_namespaces_disabled() {
        let check = userns_disabled_by_sysctl(Some("0\n"));
        assert!(check
            .unwrap()
            .contains("kernel.unprivileged_userns_clone=0"));
    }

    #[test]
    fn bwrap_probe_failure_mentions_unshare_user() {
        let check = classify_userns_probe(false, "No permissions to create new namespace");
        assert!(!check.ok);
        assert!(check.message.contains("bwrap --unshare-user"));
        assert!(check.message.contains("No permissions"));
    }
}
