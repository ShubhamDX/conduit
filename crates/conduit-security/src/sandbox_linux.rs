use std::path::Path;

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

#[cfg(test)]
mod tests {
    use super::build_bwrap_args;
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
    fn can_disable_network_namespace() {
        let args = build_bwrap_args(Path::new("/home/u/work"), true, true);
        assert!(args.iter().any(|arg| arg == "--unshare-net"));
    }
}
