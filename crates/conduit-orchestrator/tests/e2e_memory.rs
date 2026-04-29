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
    seen_memory_scope: Arc<Mutex<Option<String>>>,
    seen_memory_value: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl AgentAdapter for PromptCaptureAgent {
    fn name(&self) -> &str {
        "codex"
    }

    async fn start_session(&self, request: StartRequest) -> Result<SessionHandle, AdapterError> {
        *self.seen_memory_scope.lock().await = request.memory.map(|memory| memory.scope);
        if let Some(memory_tools) = request.memory_tools {
            let result = memory_tools
                .call("memory_search", serde_json::json!({ "limit": 5 }))
                .await
                .map_err(|error| AdapterError::Protocol(error.to_string()))?;
            *self.seen_memory_value.lock().await =
                result["entries"][0]["value"].as_str().map(str::to_string);
        }
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

struct MemoryWritingAgent;

#[async_trait]
impl AgentAdapter for MemoryWritingAgent {
    fn name(&self) -> &str {
        "codex"
    }

    async fn start_session(&self, request: StartRequest) -> Result<SessionHandle, AdapterError> {
        if let Some(memory_tools) = request.memory_tools {
            memory_tools
                .call(
                    "memory_upsert",
                    serde_json::json!({
                        "key": "agent-note",
                        "value": "remember sk-proj-abc123XYZ456def789GHJ012",
                        "tags": ["decision"]
                    }),
                )
                .await
                .map_err(|error| AdapterError::Protocol(error.to_string()))?;
        }

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let _ = tx
                .send(AgentEvent::SessionEnded {
                    reason: EndReason::Completed,
                })
                .await;
        });

        Ok(SessionHandle {
            session_id: "memory-writer".into(),
            events: rx,
        })
    }

    async fn stop_session(&self, _session_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }
}

#[tokio::test]
async fn shares_memory_reference_without_injecting_contents() {
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
    let seen_memory_scope = Arc::new(Mutex::new(None));
    let seen_memory_value = Arc::new(Mutex::new(None));
    let tracker = tracker();
    let mut registry = AdapterRegistry::new();
    registry.insert(Box::new(PromptCaptureAgent {
        seen_prompt: Arc::clone(&seen_prompt),
        seen_memory_scope: Arc::clone(&seen_memory_scope),
        seen_memory_value: Arc::clone(&seen_memory_value),
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
    assert!(prompt.contains("Shared memory is available by capability reference."));
    assert!(prompt.contains("Scope: issue:I1"));
    assert!(prompt.contains("Tags: agent:codex"));
    assert!(prompt.contains("memory_search"));
    assert!(!prompt.contains("prior decision: use the registry route"));
    assert!(prompt.contains("Current issue:"));
    assert_eq!(
        seen_memory_scope.lock().await.clone(),
        Some("issue:I1".to_string())
    );
    assert_eq!(
        seen_memory_value.lock().await.clone(),
        Some("prior decision: use the registry route".to_string())
    );
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

#[tokio::test]
async fn memory_tool_upsert_is_scoped_and_redacted() {
    let memory = Arc::new(InMemoryStore::new());
    let tracker = tracker();
    let mut registry = AdapterRegistry::new();
    registry.insert(Box::new(MemoryWritingAgent));
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
    let note = entries
        .iter()
        .find(|entry| entry.key == "agent-note")
        .expect("agent note should be written");
    assert_eq!(note.source, "issue:I1");
    assert!(note.tags.contains(&"agent:codex".to_string()));
    assert!(note.tags.contains(&"decision".to_string()));
    assert!(!note.value.contains("abc123"));
    assert!(note.value.contains("sk-proj-[REDACTED]"));
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
