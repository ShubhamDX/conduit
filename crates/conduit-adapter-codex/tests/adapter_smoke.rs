use conduit_adapter_codex::adapter::{CodexAdapter, CodexConfig};
use conduit_core::adapter::{AgentAdapter, ApprovalMode, SecurityPolicy, StartRequest};

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
