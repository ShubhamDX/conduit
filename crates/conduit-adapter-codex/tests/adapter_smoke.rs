use conduit_adapter_codex::adapter::{CodexAdapter, CodexConfig};
use conduit_core::adapter::{
    AgentAdapter, ApprovalMode, MemoryCapability, MemoryToolError, MemoryToolProvider,
    SecurityPolicy, StartRequest,
};
use std::sync::Arc;

struct FakeMemoryTools;

#[async_trait::async_trait]
impl MemoryToolProvider for FakeMemoryTools {
    async fn call(
        &self,
        _name: &str,
        _args: serde_json::Value,
    ) -> Result<serde_json::Value, MemoryToolError> {
        Ok(serde_json::json!({ "ok": true }))
    }
}

#[tokio::test]
async fn start_session_returns_handle_with_events() {
    let fixture = format!(
        "{}/tests/fixtures/fake_codex.sh",
        env!("CARGO_MANIFEST_DIR")
    );
    let config = CodexConfig {
        program: "bash".into(),
        program_args: vec![fixture],
        model: Some("gpt-5".into()),
        memory_mcp: None,
    };
    let adapter = CodexAdapter::new(config);
    let request = StartRequest {
        workspace: std::env::current_dir().unwrap(),
        prompt: "hi".into(),
        model: None,
        approval_mode: ApprovalMode::Never,
        security_policy: SecurityPolicy::default(),
        memory: None,
        memory_tools: None,
        env: Default::default(),
    };

    let mut handle = adapter.start_session(request).await.unwrap();
    let first = handle.events.recv().await.unwrap();
    assert!(matches!(
        first,
        conduit_core::event::AgentEvent::TokenDelta { .. }
    ));
}

#[tokio::test]
async fn start_session_injects_memory_mcp_config_for_codex() {
    let fixture = format!(
        "{}/tests/fixtures/fake_codex_mcp.sh",
        env!("CARGO_MANIFEST_DIR")
    );
    let workspace = test_workspace("codex-mcp-config");
    let config = CodexConfig {
        program: "bash".into(),
        program_args: vec![fixture],
        model: Some("gpt-5".into()),
        memory_mcp: Some(conduit_adapter_codex::adapter::MemoryMcpConfig {
            program: "/bin/conduit".into(),
            args: vec!["memory-mcp".into(), "--socket".into()],
        }),
    };
    let adapter = CodexAdapter::new(config);
    let request = StartRequest {
        workspace,
        prompt: "hi".into(),
        model: None,
        approval_mode: ApprovalMode::Never,
        security_policy: SecurityPolicy::default(),
        memory: Some(MemoryCapability {
            scope: "issue:I1".into(),
            tags: vec!["agent:codex".into()],
            tools: vec!["memory_get".into()],
        }),
        memory_tools: Some(Arc::new(FakeMemoryTools)),
        env: Default::default(),
    };

    let mut handle = adapter.start_session(request).await.unwrap();
    let first = handle.events.recv().await.unwrap();
    assert!(matches!(
        first,
        conduit_core::event::AgentEvent::TokenDelta { .. }
    ));
}

fn test_workspace(label: &str) -> std::path::PathBuf {
    let path = std::path::PathBuf::from("/tmp").join(format!(
        "conduit-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}
