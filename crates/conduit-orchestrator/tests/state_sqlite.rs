use conduit_core::event::{AgentEvent, Risk};
use conduit_orchestrator::state::{
    ApprovalDecision, BoardColumn, MessageDirection, NewBoardAssignment, NewBoardCard, NewMessage,
    NewTask, RunStatus, SqliteOrchestrationStore, TaskStatus,
};

#[tokio::test]
async fn sqlite_state_persists_task_run_and_redacted_events() {
    let path = unique_db_path("state-persist");
    let store = SqliteOrchestrationStore::open(&path).unwrap();
    let task = store
        .create_task(NewTask {
            id: "task-1".into(),
            source: "telegram".into(),
            title: "Build Hermes".into(),
            body: "Wire a buddy orchestrator".into(),
            labels: vec!["agent:codex".into(), "project:hermes".into()],
        })
        .await
        .unwrap();
    let run = store.start_run(&task.id, "codex").await.unwrap();
    store
        .record_event(
            &run.id,
            AgentEvent::TokenDelta {
                text: "token sk-proj-abc123XYZ456def789GHJ012".into(),
            },
        )
        .await
        .unwrap();
    drop(store);

    let reopened = SqliteOrchestrationStore::open(&path).unwrap();
    let snapshot = reopened.task_snapshot("task-1").await.unwrap().unwrap();

    assert_eq!(snapshot.task.status, TaskStatus::Running);
    assert_eq!(snapshot.task.labels, vec!["agent:codex", "project:hermes"]);
    assert_eq!(snapshot.runs.len(), 1);
    assert_eq!(snapshot.runs[0].status, RunStatus::Running);
    assert_eq!(snapshot.runs[0].agent, "codex");
    assert_eq!(snapshot.events.len(), 1);
    assert_eq!(snapshot.events[0].event_type, "token_delta");
    let payload = serde_json::to_string(&snapshot.events[0].payload).unwrap();
    assert!(!payload.contains("abc123"));
    assert!(payload.contains("sk-proj-[REDACTED]"));

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_state_records_approvals_and_messages_for_control_surfaces() {
    let store = SqliteOrchestrationStore::open_in_memory().unwrap();
    let task = store
        .create_task(NewTask {
            id: "task-2".into(),
            source: "jira".into(),
            title: "Ship dashboard".into(),
            body: "Expose orchestration status".into(),
            labels: vec!["agent:claude-code".into()],
        })
        .await
        .unwrap();
    let run = store.start_run(&task.id, "claude-code").await.unwrap();

    let approval = store
        .request_approval(&run.id, "write outside workspace", Risk::High)
        .await
        .unwrap();
    store
        .record_message(NewMessage {
            task_id: Some(task.id.clone()),
            run_id: Some(run.id.clone()),
            channel: "telegram".into(),
            sender: "shubham".into(),
            direction: MessageDirection::Inbound,
            body: "please use sk-proj-abc123XYZ456def789GHJ012".into(),
        })
        .await
        .unwrap();

    let snapshot = store.task_snapshot("task-2").await.unwrap().unwrap();

    assert_eq!(snapshot.approvals.len(), 1);
    assert_eq!(snapshot.approvals[0].id, approval.id);
    assert_eq!(snapshot.approvals[0].status, "pending");
    assert_eq!(snapshot.approvals[0].risk, Risk::High);
    let resolved = store
        .resolve_approval(&approval.id, ApprovalDecision::Approved)
        .await
        .unwrap();
    assert_eq!(resolved.status, "approved");
    assert!(resolved.resolved_at_ms.is_some());
    let second_resolution = store
        .resolve_approval(&approval.id, ApprovalDecision::Denied)
        .await
        .unwrap_err();
    assert!(second_resolution
        .to_string()
        .contains("approval already resolved"));
    assert!(second_resolution.to_string().contains("requested denied"));

    let snapshot = store.task_snapshot("task-2").await.unwrap().unwrap();
    assert_eq!(snapshot.approvals[0].status, "approved");
    assert_eq!(snapshot.messages.len(), 1);
    assert_eq!(snapshot.messages[0].channel, "telegram");
    assert_eq!(snapshot.messages[0].direction, MessageDirection::Inbound);
    assert!(!snapshot.messages[0].body.contains("abc123"));
    assert!(snapshot.messages[0].body.contains("sk-proj-[REDACTED]"));
}

#[tokio::test]
async fn sqlite_state_orders_events_across_multiple_runs() {
    let store = SqliteOrchestrationStore::open_in_memory().unwrap();
    let task = store
        .create_task(NewTask {
            id: "task-3".into(),
            source: "dashboard".into(),
            title: "Compare agents".into(),
            body: "Run both adapters".into(),
            labels: vec!["agent:codex".into(), "agent:claude-code".into()],
        })
        .await
        .unwrap();
    let codex = store.start_run(&task.id, "codex").await.unwrap();
    let claude = store.start_run(&task.id, "claude-code").await.unwrap();

    store
        .record_event(&codex.id, AgentEvent::TokenDelta { text: "c1".into() })
        .await
        .unwrap();
    store
        .record_event(&codex.id, AgentEvent::TokenDelta { text: "c2".into() })
        .await
        .unwrap();
    store
        .record_event(&claude.id, AgentEvent::TokenDelta { text: "a1".into() })
        .await
        .unwrap();

    let snapshot = store.task_snapshot("task-3").await.unwrap().unwrap();
    assert_eq!(snapshot.runs.len(), 2);
    assert_eq!(snapshot.events.len(), 3);
    assert_eq!(
        snapshot
            .events
            .iter()
            .filter(|event| event.run_id == codex.id)
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        snapshot
            .events
            .iter()
            .filter(|event| event.run_id == claude.id)
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[tokio::test]
async fn sqlite_state_lists_tasks_runs_and_approvals_for_control_plane() {
    let store = SqliteOrchestrationStore::open_in_memory().unwrap();
    let task = store
        .create_task(NewTask {
            id: "task-control".into(),
            source: "jira".into(),
            title: "Control plane".into(),
            body: "Expose state".into(),
            labels: vec!["agent:codex".into()],
        })
        .await
        .unwrap();
    let run = store.start_run(&task.id, "codex").await.unwrap();
    store
        .record_event(&run.id, AgentEvent::TokenDelta { text: "hi".into() })
        .await
        .unwrap();
    let approval = store
        .request_approval(&run.id, "write files", conduit_core::event::Risk::Medium)
        .await
        .unwrap();

    let tasks = store.tasks().await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id, task.id);

    let run_snapshot = store.run_snapshot(&run.id).await.unwrap().unwrap();
    assert_eq!(run_snapshot.task.id, task.id);
    assert_eq!(run_snapshot.run.id, run.id);
    assert_eq!(run_snapshot.events.len(), 1);
    assert_eq!(run_snapshot.approvals.len(), 1);

    let approvals = store.approvals(Some("pending")).await.unwrap();
    assert_eq!(approvals.len(), 1);
    assert_eq!(approvals[0].id, approval.id);

    store
        .resolve_approval(&approval.id, ApprovalDecision::Approved)
        .await
        .unwrap();
    assert!(store.approvals(Some("pending")).await.unwrap().is_empty());
    assert_eq!(store.approvals(Some("approved")).await.unwrap().len(), 1);
}

#[tokio::test]
async fn sqlite_state_tracks_board_cards_and_agent_assignments() {
    let path = unique_db_path("board");
    let store = SqliteOrchestrationStore::open(&path).unwrap();
    let card = store
        .create_board_card(NewBoardCard {
            id: "product-launch".into(),
            title: "Launch strategy".into(),
            body: "Brainstorm product launch with sk-proj-abc123XYZ456def789GHJ012".into(),
            labels: vec!["product:new".into(), "council".into()],
            column: BoardColumn::Ideas,
        })
        .await
        .unwrap();

    assert_eq!(card.task.id, "product-launch");
    assert_eq!(card.column, BoardColumn::Ideas);
    assert!(card.assignments.is_empty());
    assert!(!card.task.body.contains("abc123"));
    assert!(card.task.body.contains("sk-proj-[REDACTED]"));

    let assigned = store
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
    assert_eq!(assigned.assignments.len(), 1);
    assert_eq!(assigned.assignments[0].agent, "codex");
    assert_eq!(assigned.assignments[0].role, "coder");
    assert_eq!(assigned.assignments[0].model.as_deref(), Some("gpt-5.5"));

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
    let moved = store
        .move_board_card("product-launch", BoardColumn::Brainstorming)
        .await
        .unwrap();
    assert_eq!(moved.column, BoardColumn::Brainstorming);
    assert_eq!(moved.assignments.len(), 2);
    drop(store);

    let reopened = SqliteOrchestrationStore::open(&path).unwrap();
    let cards = reopened.board_cards().await.unwrap();
    assert_eq!(cards.len(), 1);
    assert_eq!(cards[0].task.id, "product-launch");
    assert_eq!(cards[0].column, BoardColumn::Brainstorming);
    assert_eq!(cards[0].assignments.len(), 2);

    let card = reopened
        .board_card("product-launch")
        .await
        .unwrap()
        .unwrap();
    assert!(card
        .assignments
        .iter()
        .any(|assignment| assignment.agent == "claude-code" && assignment.role == "brainstormer"));

    let _ = std::fs::remove_file(path);
}

fn unique_db_path(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "conduit-orchestrator-{label}-{}-{nanos}.db",
        std::process::id()
    ))
}
