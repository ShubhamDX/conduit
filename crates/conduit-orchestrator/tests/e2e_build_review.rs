use async_trait::async_trait;
use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{AgentAdapter, SecurityPolicy, SessionHandle, StartRequest};
use conduit_core::error::AdapterError;
use conduit_core::event::{AgentEvent, EndReason};
use conduit_memory::memory::InMemoryStore;
use conduit_memory::MemoryStore;
use conduit_orchestrator::build::{run_build_review, BuildReviewOptions};
use conduit_orchestrator::state::{
    BoardColumn, MessageDirection, NewBoardAssignment, NewBoardCard, RunStatus,
    SqliteOrchestrationStore,
};
use conduit_orchestrator::OrchestratorConfig;
use std::sync::Arc;
use tokio::sync::Mutex;

struct BuildAgent {
    name: &'static str,
    response: &'static str,
    prompts: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl AgentAdapter for BuildAgent {
    fn name(&self) -> &str {
        self.name
    }

    async fn start_session(&self, request: StartRequest) -> Result<SessionHandle, AdapterError> {
        self.prompts.lock().await.push(request.prompt);
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let text = format!("{} says {}", self.name, self.response);
        tokio::spawn(async move {
            let _ = tx.send(AgentEvent::TokenDelta { text }).await;
            let _ = tx
                .send(AgentEvent::SessionEnded {
                    reason: EndReason::Completed,
                })
                .await;
        });

        Ok(SessionHandle {
            session_id: format!("{}-session", self.name),
            events: rx,
        })
    }

    async fn stop_session(&self, _session_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }
}

#[tokio::test]
async fn build_review_runs_coder_then_reviewer_and_records_handoff() {
    let store = Arc::new(SqliteOrchestrationStore::open_in_memory().unwrap());
    store
        .create_board_card(NewBoardCard {
            id: "product-launch".into(),
            title: "New product launch".into(),
            body: "Build from approved spec with sk-proj-abc123XYZ456def789GHJ012".into(),
            labels: vec!["product:new".into(), "council".into()],
            column: BoardColumn::SpecReview,
        })
        .await
        .unwrap();
    store
        .approve_board_spec("product-launch", "shubham", Some("approved"))
        .await
        .unwrap();
    store
        .assign_board_card(
            "product-launch",
            NewBoardAssignment {
                agent: "codex".into(),
                role: "coder".into(),
                model: Some("gpt-5.5".into()),
            },
        )
        .await
        .unwrap();
    store
        .assign_board_card(
            "product-launch",
            NewBoardAssignment {
                agent: "claude-code".into(),
                role: "reviewer".into(),
                model: Some("opus-4.7".into()),
            },
        )
        .await
        .unwrap();

    let codex_prompts = Arc::new(Mutex::new(Vec::new()));
    let claude_prompts = Arc::new(Mutex::new(Vec::new()));
    let mut registry = AdapterRegistry::new();
    registry.insert(Box::new(BuildAgent {
        name: "codex",
        response: "implemented the scoped build with sk-proj-abc123XYZ456def789GHJ012",
        prompts: Arc::clone(&codex_prompts),
    }));
    registry.insert(Box::new(BuildAgent {
        name: "claude-code",
        response: "review passed; missing nothing material",
        prompts: Arc::clone(&claude_prompts),
    }));
    registry.set_default("codex");

    let memory = Arc::new(InMemoryStore::new());
    let shared_memory: Arc<dyn MemoryStore> = memory.clone();
    let config = OrchestratorConfig {
        workspace: ".".into(),
        assignee: "bot".into(),
        default_policy: SecurityPolicy::default(),
        shared_memory: Some(shared_memory),
        orchestration_store: Some(store.clone()),
    };

    let report = run_build_review(
        &registry,
        &config,
        "product-launch",
        BuildReviewOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(report.card_id, "product-launch");
    assert_eq!(report.column, BoardColumn::HumanReview);
    assert_eq!(report.build_turns.len(), 1);
    assert_eq!(report.review_turns.len(), 1);
    assert_eq!(report.build_turns[0].status, RunStatus::Succeeded);
    assert_eq!(report.review_turns[0].status, RunStatus::Succeeded);
    assert!(!report.summary.contains("abc123"));

    let card = store.board_card("product-launch").await.unwrap().unwrap();
    assert_eq!(card.column, BoardColumn::HumanReview);
    let snapshot = store
        .task_snapshot("product-launch")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.runs.len(), 2);
    assert!(snapshot
        .runs
        .iter()
        .all(|run| run.status == RunStatus::Succeeded));
    assert!(snapshot
        .messages
        .iter()
        .any(|message| message.channel == "build"
            && message.direction == MessageDirection::Inbound
            && message.sender == "codex"));
    assert!(snapshot
        .messages
        .iter()
        .any(|message| message.channel == "review"
            && message.direction == MessageDirection::Inbound
            && message.sender == "claude-code"));
    assert!(snapshot
        .messages
        .iter()
        .any(|message| message.channel == "build_review"
            && message.direction == MessageDirection::Outbound
            && message.sender == "orchestrator"));
    assert!(snapshot
        .messages
        .iter()
        .all(|message| !message.body.contains("abc123")));

    let entries = memory.entries().await;
    let handoff = entries
        .iter()
        .find(|entry| entry.key == "build:product-launch:handoff")
        .expect("build handoff should be written to memory");
    assert_eq!(handoff.source, "build:product-launch");
    assert!(handoff.tags.contains(&"build".to_string()));
    assert!(!handoff.value.contains("abc123"));

    let codex_prompt = codex_prompts.lock().await.join("\n");
    assert!(codex_prompt.contains("Build card: product-launch"));
    assert!(codex_prompt.contains("Your build role: coder"));
    assert!(codex_prompt.contains("Shared memory reference:"));
    assert!(!codex_prompt.contains("abc123"));
    let claude_prompt = claude_prompts.lock().await.join("\n");
    assert!(claude_prompt.contains("Review card: product-launch"));
    assert!(claude_prompt.contains("Implementation context:"));
    assert!(claude_prompt.contains("implemented the scoped build"));
}
