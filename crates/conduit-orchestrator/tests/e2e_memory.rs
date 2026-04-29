use async_trait::async_trait;
use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{AgentAdapter, SecurityPolicy, SessionHandle, StartRequest};
use conduit_core::error::AdapterError;
use conduit_core::event::{AgentEvent, EndReason};
use conduit_memory::memory::InMemoryStore;
use conduit_memory::{MemoryEntry, MemoryStore};
use conduit_orchestrator::{run_one_issue, OrchestratorConfig};
use conduit_tracker::{fake::FakeTracker, Issue};
use std::sync::Arc;
use tokio::sync::Mutex;

struct PromptCaptureAgent {
    seen_prompt: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl AgentAdapter for PromptCaptureAgent {
    fn name(&self) -> &str {
        "codex"
    }

    async fn start_session(&self, request: StartRequest) -> Result<SessionHandle, AdapterError> {
        *self.seen_prompt.lock().await = Some(request.prompt);
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let _ = tx
                .send(AgentEvent::TokenDelta {
                    text: "done".into(),
                })
                .await;
            let _ = tx
                .send(AgentEvent::SessionEnded {
                    reason: EndReason::Completed,
                })
                .await;
        });

        Ok(SessionHandle {
            session_id: "memory".into(),
            events: rx,
        })
    }

    async fn stop_session(&self, _session_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }
}

struct SecretAgent;

#[async_trait]
impl AgentAdapter for SecretAgent {
    fn name(&self) -> &str {
        "codex"
    }

    async fn start_session(&self, _request: StartRequest) -> Result<SessionHandle, AdapterError> {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let _ = tx
                .send(AgentEvent::TokenDelta {
                    text: "shared sk-proj-abc123XYZ456def789GHJ012".into(),
                })
                .await;
            let _ = tx
                .send(AgentEvent::SessionEnded {
                    reason: EndReason::Completed,
                })
                .await;
        });

        Ok(SessionHandle {
            session_id: "secret-memory".into(),
            events: rx,
        })
    }

    async fn stop_session(&self, _session_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }
}

#[tokio::test]
async fn injects_matching_shared_memory_into_prompt() {
    let memory = Arc::new(InMemoryStore::new());
    memory
        .upsert(MemoryEntry {
            key: "previous-issue".into(),
            value: "prior decision: use the registry route".into(),
            tags: vec!["agent:codex".into()],
            source: "issue:previous-issue".into(),
        })
        .await
        .unwrap();
    let seen_prompt = Arc::new(Mutex::new(None));
    let tracker = tracker();
    let mut registry = AdapterRegistry::new();
    registry.insert(Box::new(PromptCaptureAgent {
        seen_prompt: Arc::clone(&seen_prompt),
    }));
    registry.set_default("codex");
    let shared_memory: Arc<dyn MemoryStore> = memory;
    let config = OrchestratorConfig {
        workspace: ".".into(),
        assignee: "bot".into(),
        default_policy: SecurityPolicy::default(),
        shared_memory: Some(shared_memory),
    };

    run_one_issue(&tracker, &registry, &config, "I1")
        .await
        .unwrap();

    let prompt = seen_prompt.lock().await.clone().unwrap();
    assert!(prompt.contains("Shared memory:"));
    assert!(prompt.contains("prior decision: use the registry route"));
    assert!(prompt.contains("Current issue:"));
}

#[tokio::test]
async fn writes_redacted_summary_to_shared_memory() {
    let memory = Arc::new(InMemoryStore::new());
    let tracker = tracker();
    let mut registry = AdapterRegistry::new();
    registry.insert(Box::new(SecretAgent));
    registry.set_default("codex");
    let shared_memory: Arc<dyn MemoryStore> = memory.clone();
    let config = OrchestratorConfig {
        workspace: ".".into(),
        assignee: "bot".into(),
        default_policy: SecurityPolicy::default(),
        shared_memory: Some(shared_memory),
    };

    run_one_issue(&tracker, &registry, &config, "I1")
        .await
        .unwrap();

    let entries = memory.entries().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, "I1");
    assert!(!entries[0].value.contains("abc123"));
    assert!(entries[0].value.contains("sk-proj-[REDACTED]"));
}

fn tracker() -> FakeTracker {
    FakeTracker::with(vec![Issue {
        id: "I1".into(),
        title: "t".into(),
        body: "do the thing".into(),
        labels: vec!["agent:codex".into()],
        assignee: Some("bot".into()),
        state: "todo".into(),
    }])
}
