use async_trait::async_trait;
use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{AgentAdapter, SecurityPolicy, SessionHandle, StartRequest};
use conduit_core::error::AdapterError;
use conduit_core::event::{AgentEvent, EndReason, Risk};
use conduit_orchestrator::state::{
    MessageDirection, RunStatus, SqliteOrchestrationStore, TaskStatus,
};
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

struct LedgerAgent;

#[async_trait]
impl AgentAdapter for LedgerAgent {
    fn name(&self) -> &str {
        "codex"
    }

    async fn start_session(&self, _request: StartRequest) -> Result<SessionHandle, AdapterError> {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let _ = tx
                .send(AgentEvent::TokenDelta {
                    text: "done sk-proj-abc123XYZ456def789GHJ012".into(),
                })
                .await;
            let _ = tx
                .send(AgentEvent::ApprovalRequested {
                    call_id: "approve-1".into(),
                    reason: "needs write".into(),
                    risk: Risk::High,
                })
                .await;
            let _ = tx
                .send(AgentEvent::SessionEnded {
                    reason: EndReason::Completed,
                })
                .await;
        });

        Ok(SessionHandle {
            session_id: "ledger-session".into(),
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
        shared_memory: None,
        orchestration_store: None,
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

#[tokio::test]
async fn run_one_issue_records_live_execution_in_state_ledger() {
    let tracker = FakeTracker::with(vec![Issue {
        id: "I-ledger".into(),
        title: "ledger task".into(),
        body: "record this run".into(),
        labels: vec!["agent:codex".into(), "project:state".into()],
        assignee: Some("bot".into()),
        state: "todo".into(),
    }]);
    let mut registry = AdapterRegistry::new();
    registry.insert(Box::new(LedgerAgent));
    registry.set_default("codex");
    let store = std::sync::Arc::new(SqliteOrchestrationStore::open_in_memory().unwrap());
    let config = OrchestratorConfig {
        workspace: ".".into(),
        assignee: "bot".into(),
        default_policy: SecurityPolicy::default(),
        shared_memory: None,
        orchestration_store: Some(store.clone()),
    };

    run_one_issue(&tracker, &registry, &config, "I-ledger")
        .await
        .unwrap();

    let snapshot = store
        .task_snapshot("I-ledger")
        .await
        .unwrap()
        .expect("task should be recorded");
    assert_eq!(snapshot.task.source, "tracker");
    assert_eq!(snapshot.task.title, "ledger task");
    assert_eq!(snapshot.task.status, TaskStatus::Done);
    assert_eq!(snapshot.runs.len(), 1);
    assert_eq!(snapshot.runs[0].agent, "codex");
    assert_eq!(snapshot.runs[0].status, RunStatus::Succeeded);
    assert!(snapshot.runs[0].completed_at_ms.is_some());
    assert_eq!(
        snapshot
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["token_delta", "approval_requested", "session_ended"]
    );
    assert_eq!(snapshot.approvals.len(), 1);
    assert_eq!(snapshot.approvals[0].status, "pending");
    assert_eq!(snapshot.approvals[0].risk, Risk::High);
    assert_eq!(snapshot.messages.len(), 1);
    assert_eq!(snapshot.messages[0].channel, "tracker");
    assert_eq!(snapshot.messages[0].sender, "orchestrator");
    assert_eq!(snapshot.messages[0].direction, MessageDirection::Outbound);
    assert!(!snapshot.messages[0].body.contains("abc123"));
    assert!(snapshot.messages[0].body.contains("sk-proj-[REDACTED]"));
}
