use async_trait::async_trait;
use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{AgentAdapter, SecurityPolicy, SessionHandle, StartRequest};
use conduit_core::error::AdapterError;
use conduit_core::event::{AgentEvent, EndReason};
use conduit_orchestrator::{run_one_issue, OrchestratorConfig};
use conduit_tracker::{fake::FakeTracker, Issue};

struct EchoAgent;

#[async_trait]
impl AgentAdapter for EchoAgent {
    fn name(&self) -> &str {
        "codex"
    }

    async fn start_session(&self, request: StartRequest) -> Result<SessionHandle, AdapterError> {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let prompt = request.prompt;
        tokio::spawn(async move {
            let _ = tx
                .send(AgentEvent::TokenDelta {
                    text: format!("echo: {prompt}"),
                })
                .await;
            let _ = tx
                .send(AgentEvent::TurnCompleted {
                    tokens_in: 1,
                    tokens_out: 2,
                })
                .await;
            let _ = tx
                .send(AgentEvent::SessionEnded {
                    reason: EndReason::Completed,
                })
                .await;
        });

        Ok(SessionHandle {
            session_id: "x".into(),
            events: rx,
        })
    }

    async fn stop_session(&self, _session_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }
}

#[tokio::test]
async fn runs_one_issue_and_posts_summary() {
    let tracker = FakeTracker::with(vec![Issue {
        id: "I1".into(),
        title: "t".into(),
        body: "do the thing".into(),
        labels: vec!["agent:codex".into()],
        assignee: Some("bot".into()),
        state: "todo".into(),
    }]);
    let mut registry = AdapterRegistry::new();
    registry.insert(Box::new(EchoAgent));
    registry.set_default("codex");
    let config = OrchestratorConfig {
        workspace: ".".into(),
        assignee: "bot".into(),
        default_policy: SecurityPolicy::default(),
    };

    run_one_issue(&tracker, &registry, &config, "I1")
        .await
        .unwrap();

    let comments = tracker.comments().await;
    assert_eq!(comments.len(), 1);
    assert!(comments[0].1.contains("do the thing"));
    assert_eq!(
        tracker.state_updates().await,
        vec![
            ("I1".to_string(), "in_progress".to_string()),
            ("I1".to_string(), "done".to_string())
        ]
    );
}
