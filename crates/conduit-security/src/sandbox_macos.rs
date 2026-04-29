use std::path::{Path, PathBuf};

pub fn build_profile(workspace: &Path, writable: bool) -> String {
    let workspace = escape_sbpl_string(&workspace.display().to_string());
    let writable_block = if writable {
        format!("(allow file-write* (subpath \"{workspace}\"))\n")
    } else {
        String::new()
    };

    format!(
        r#"(version 1)
(deny default)
(allow process-fork)
(allow process-exec)
(allow file-read*)
(deny file-write*)
{writable_block}(allow file-write* (literal "/dev/null"))
(allow file-write* (literal "/dev/stdout"))
(allow file-write* (literal "/dev/stderr"))
(allow sysctl-read)
(allow mach-lookup)
(allow iokit-open)
(allow network* (remote ip "localhost:*"))
(allow network* (local ip "*:*"))
"#
    )
}

pub fn write_profile_to_tempfile(workspace: &Path, writable: bool) -> std::io::Result<PathBuf> {
    let profile = build_profile(workspace, writable);
    let path = std::env::temp_dir().join(format!("conduit-sbpl-{}.sb", std::process::id()));
    std::fs::write(&path, profile)?;
    Ok(path)
}

fn escape_sbpl_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::build_profile;
    use std::path::Path;

    #[test]
    fn profile_allows_workspace_write() {
        let profile = build_profile(Path::new("/tmp/work"), true);
        assert!(profile.contains("(allow file-write* (subpath \"/tmp/work\"))"));
        assert!(profile.contains("(deny file-write*)"));
    }

    #[test]
    fn profile_denies_all_write_when_not_writable() {
        let profile = build_profile(Path::new("/tmp/work"), false);
        assert!(!profile.contains("(allow file-write* (subpath \"/tmp/work\"))"));
        assert!(profile.contains("(deny file-write*)"));
    }

    #[test]
    fn profile_allows_loopback_network() {
        let profile = build_profile(Path::new("/tmp/work"), true);
        assert!(profile.contains("(allow network*"));
        assert!(profile.contains("localhost"));
    }
}
