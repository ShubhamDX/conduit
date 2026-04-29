use conduit_core::event::{AgentEvent, Risk};
use conduit_orchestrator::state::{
    ApprovalDecision, MessageDirection, NewMessage, NewTask, RunStatus, SqliteOrchestrationStore,
    TaskStatus,
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
