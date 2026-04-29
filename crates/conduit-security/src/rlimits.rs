use conduit_core::adapter::SecurityPolicy;

#[cfg(unix)]
use rlimit::{setrlimit, Resource};

#[cfg(unix)]
pub fn limits_to_closure(
    policy: &SecurityPolicy,
) -> Option<Box<dyn Fn() -> std::io::Result<()> + Send + Sync>> {
    let cpu = policy.max_cpu_secs;
    let memory = policy.max_memory_bytes;
    let files = policy.max_open_files;

    if cpu.is_none() && memory.is_none() && files.is_none() {
        return None;
    }

    Some(Box::new(move || {
        if let Some(seconds) = cpu {
            set_limit(Resource::CPU, seconds)?;
        }

        if let Some(bytes) = memory {
            set_limit(Resource::AS, bytes)?;
        }

        if let Some(count) = files {
            set_limit(Resource::NOFILE, count)?;
        }

        Ok(())
    }))
}

#[cfg(unix)]
fn set_limit(resource: Resource, soft_and_hard: u64) -> std::io::Result<()> {
    setrlimit(resource, soft_and_hard, soft_and_hard)
        .map_err(|err| std::io::Error::other(err.to_string()))
}

#[cfg(not(unix))]
pub fn limits_to_closure(
    _policy: &SecurityPolicy,
) -> Option<Box<dyn Fn() -> std::io::Result<()> + Send + Sync>> {
    None
}

#[cfg(all(test, unix))]
mod tests {
    use super::limits_to_closure;
    use conduit_core::adapter::SecurityPolicy;

    #[test]
    fn builds_closure_when_limits_present() {
        let policy = SecurityPolicy {
            max_cpu_secs: Some(60),
            max_memory_bytes: Some(1 << 30),
            ..Default::default()
        };

        let callback = limits_to_closure(&policy);
        assert!(callback.is_some());
    }

    #[test]
    fn no_closure_when_none() {
        let policy = SecurityPolicy::default();
        let callback = limits_to_closure(&policy);
        assert!(callback.is_none());
    }
}
