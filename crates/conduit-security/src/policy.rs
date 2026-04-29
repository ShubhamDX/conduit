use conduit_core::adapter::SecurityPolicy;

pub fn merged_policy(base: &SecurityPolicy, over: Option<&SecurityPolicy>) -> SecurityPolicy {
    let Some(override_policy) = over else {
        return base.clone();
    };

    let mut merged = base.clone();
    merged
        .egress_allowlist
        .extend(override_policy.egress_allowlist.iter().cloned());
    merged.egress_allowlist.sort();
    merged.egress_allowlist.dedup();

    if override_policy.max_cpu_secs.is_some() {
        merged.max_cpu_secs = override_policy.max_cpu_secs;
    }

    if override_policy.max_memory_bytes.is_some() {
        merged.max_memory_bytes = override_policy.max_memory_bytes;
    }

    if override_policy.max_open_files.is_some() {
        merged.max_open_files = override_policy.max_open_files;
    }

    merged.workspace_writable = base.workspace_writable || override_policy.workspace_writable;
    merged.redact_secrets = base.redact_secrets || override_policy.redact_secrets;
    merged
}
