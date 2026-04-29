use conduit_adapter_claude::adapter::{ClaudeCodeAdapter, ClaudeConfig};
use conduit_core::adapter::{AgentAdapter, ApprovalMode, SecurityPolicy, StartRequest};

#[tokio::test]
async fn emits_first_token_delta() {
    let fixture = format!(
        "{}/tests/fixtures/fake_bridge.py",
        env!("CARGO_MANIFEST_DIR")
    );
    let config = ClaudeConfig {
        python: "python3".into(),
        bridge_args: vec!["-B".into(), fixture],
        model: None,
    };
    let adapter = ClaudeCodeAdapter::new(config);
    let request = StartRequest {
        workspace: std::env::current_dir().unwrap(),
        prompt: "hi".into(),
        model: None,
        approval_mode: ApprovalMode::Never,
        security_policy: SecurityPolicy::default(),
        env: Default::default(),
    };

    let mut handle = adapter.start_session(request).await.unwrap();
    let event = handle.events.recv().await.unwrap();
    assert!(matches!(
        event,
        conduit_core::event::AgentEvent::TokenDelta { .. }
    ));
}
