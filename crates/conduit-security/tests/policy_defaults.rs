use conduit_core::adapter::SecurityPolicy;
use conduit_security::policy::merged_policy;

#[test]
fn workflow_default_applied() {
    let workflow_default = SecurityPolicy {
        egress_allowlist: vec!["api.openai.com".into()],
        redact_secrets: true,
        ..SecurityPolicy::default()
    };
    let issue_override = SecurityPolicy {
        egress_allowlist: vec!["api.github.com".into()],
        ..SecurityPolicy::default()
    };

    let merged = merged_policy(&workflow_default, Some(&issue_override));
    assert!(merged
        .egress_allowlist
        .contains(&"api.openai.com".to_string()));
    assert!(merged
        .egress_allowlist
        .contains(&"api.github.com".to_string()));
    assert!(merged.redact_secrets);
}
