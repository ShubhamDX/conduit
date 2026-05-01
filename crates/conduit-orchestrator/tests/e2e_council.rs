use async_trait::async_trait;
use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{AgentAdapter, SecurityPolicy, SessionHandle, StartRequest};
use conduit_core::error::AdapterError;
use conduit_core::event::{AgentEvent, EndReason};
use conduit_memory::memory::InMemoryStore;
use conduit_memory::MemoryStore;
use conduit_orchestrator::council::{run_agent_council, CouncilOptions};
use conduit_orchestrator::state::{
    BoardColumn, MessageDirection, NewBoardAssignment, NewBoardCard, RunStatus,
    SqliteOrchestrationStore,
};
use conduit_orchestrator::OrchestratorConfig;
use std::sync::Arc;
use tokio::sync::Mutex;

struct CouncilAgent {
    name: &'static str,
    response: &'static str,
    prompts: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl AgentAdapter for CouncilAgent {
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
async fn agent_council_runs_assigned_agents_and_records_consensus() {
    let store = Arc::new(SqliteOrchestrationStore::open_in_memory().unwrap());
    store
        .create_board_card(NewBoardCard {
            id: "product-launch".into(),
            title: "New product launch".into(),
            body: "Discuss launch plan with sk-proj-abc123XYZ456def789GHJ012".into(),
            labels: vec!["product:new".into(), "council".into()],
            column: BoardColumn::Brainstorming,
        })
        .await
        .unwrap();
    store
        .assign_board_card(
            "product-launch",
            NewBoardAssignment {
                agent: "claude-code".into(),
                role: "brainstormer".into(),
                model: Some("opus-4.7".into()),
            },
        )
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

    let codex_prompts = Arc::new(Mutex::new(Vec::new()));
    let claude_prompts = Arc::new(Mutex::new(Vec::new()));
    let mut registry = AdapterRegistry::new();
    registry.insert(Box::new(CouncilAgent {
        name: "codex",
        response: "build the API first",
        prompts: Arc::clone(&codex_prompts),
    }));
    registry.insert(Box::new(CouncilAgent {
        name: "claude-code",
        response: "clarify user positioning",
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

    let report = run_agent_council(
        &registry,
        &config,
        "product-launch",
        CouncilOptions { max_rounds: 1 },
    )
    .await
    .unwrap();

    assert_eq!(report.card_id, "product-launch");
    assert_eq!(report.turns.len(), 2);
    assert_eq!(report.column, BoardColumn::SpecReview);
    assert!(report.summary.contains("claude-code"));
    assert!(report.summary.contains("codex"));
    assert!(!report.summary.contains("abc123"));

    let card = store.board_card("product-launch").await.unwrap().unwrap();
    assert_eq!(card.column, BoardColumn::SpecReview);
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
        .any(|message| message.channel == "council"
            && message.direction == MessageDirection::Outbound
            && message.sender == "orchestrator"));
    assert!(snapshot
        .messages
        .iter()
        .all(|message| !message.body.contains("abc123")));

    let entries = memory.entries().await;
    let consensus = entries
        .iter()
        .find(|entry| entry.key == "council:product-launch:consensus")
        .expect("consensus should be written to memory");
    assert_eq!(consensus.source, "council:product-launch");
    assert!(consensus.tags.contains(&"council".to_string()));
    assert!(consensus.tags.contains(&"product:new".to_string()));
    assert!(!consensus.value.contains("abc123"));

    let codex_prompt = codex_prompts.lock().await.join("\n");
    assert!(codex_prompt.contains("Agent council card: product-launch"));
    assert!(codex_prompt.contains("Your council role: coder"));
    assert!(codex_prompt.contains("Previous council context:"));
    assert!(!codex_prompt.contains("abc123"));
    let claude_prompt = claude_prompts.lock().await.join("\n");
    assert!(claude_prompt.contains("Your council role: brainstormer"));
}
