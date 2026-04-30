use conduit_core::event::{AgentEvent, Risk};
use conduit_orchestrator::state::{
    MessageDirection, NewMessage, NewTask, RunStatus, SqliteOrchestrationStore,
};
use conduit_orchestrator::trace_export::{export_halo_spans, HaloExportOptions};

#[tokio::test]
async fn halo_export_maps_ledger_to_redacted_otlp_spans() {
    let store = SqliteOrchestrationStore::open_in_memory().unwrap();
    let task = store
        .create_task(NewTask {
            id: "task-halo".into(),
            source: "tracker".into(),
            title: "Optimize harness".into(),
            body: "Investigate sk-proj-abc123XYZ456def789GHJ012".into(),
            labels: vec![
                "agent:codex".into(),
                "token:sk-secret-with-dashes_1234567890".into(),
            ],
        })
        .await
        .unwrap();
    let run = store.start_run(&task.id, "codex").await.unwrap();

    store
        .record_event(
            &run.id,
            AgentEvent::ToolCallStarted {
                call_id: "call-1".into(),
                name: "memory_search".into(),
                args: serde_json::json!({ "query": "sk-proj-abc123XYZ456def789GHJ012" }),
            },
        )
        .await
        .unwrap();
    store
        .record_event(
            &run.id,
            AgentEvent::ToolCallCompleted {
                call_id: "call-1".into(),
                ok: true,
                output: "found sk-proj-abc123XYZ456def789GHJ012".into(),
            },
        )
        .await
        .unwrap();
    store
        .record_event(
            &run.id,
            AgentEvent::TokenDelta {
                text: "HALO should inspect repeated tool failures. ".into(),
            },
        )
        .await
        .unwrap();
    store
        .record_event(
            &run.id,
            AgentEvent::TurnCompleted {
                tokens_in: 11,
                tokens_out: 7,
            },
        )
        .await
        .unwrap();
    store
        .request_approval(
            &run.id,
            "approve write with sk-proj-abc123XYZ456def789GHJ012",
            Risk::High,
        )
        .await
        .unwrap();
    store
        .record_message(NewMessage {
            task_id: Some(task.id.clone()),
            run_id: Some(run.id.clone()),
            channel: "tracker".into(),
            sender: "orchestrator".into(),
            direction: MessageDirection::Outbound,
            body: "posted sk-proj-abc123XYZ456def789GHJ012".into(),
        })
        .await
        .unwrap();
    store
        .finish_run(&run.id, RunStatus::Succeeded)
        .await
        .unwrap();

    let snapshots = store.task_snapshots().await.unwrap();
    let spans = export_halo_spans(
        &snapshots,
        &HaloExportOptions {
            project_id: "conduit-test".into(),
            service_name: "conduit-test".into(),
            service_version: Some("0.1.0".into()),
            deployment_environment: Some("test".into()),
        },
    );
    let jsonl = spans
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join("\n");

    assert!(!jsonl.contains("abc123"));
    assert!(!jsonl.contains("sk-secret-with-dashes"));
    assert!(jsonl.contains("sk-proj-[REDACTED]"));
    assert!(jsonl.contains("\"inference.project_id\":\"conduit-test\""));

    let agent = spans
        .iter()
        .find(|span| span.name == "conduit.run.codex")
        .expect("agent root span");
    assert_eq!(agent.parent_span_id, "");
    assert_eq!(agent.kind, "SPAN_KIND_INTERNAL");
    assert_eq!(
        agent.attributes.get("openinference.span.kind").unwrap(),
        "AGENT"
    );
    assert!(agent.start_time.ends_with('Z'));

    let tool = spans
        .iter()
        .find(|span| span.name == "conduit.tool.memory_search")
        .expect("tool span");
    assert_eq!(tool.parent_span_id, agent.span_id);
    assert_eq!(tool.attributes.get("tool.name").unwrap(), "memory_search");
    assert_eq!(
        tool.attributes.get("openinference.span.kind").unwrap(),
        "TOOL"
    );

    let llm = spans
        .iter()
        .find(|span| span.name == "conduit.llm.turn")
        .expect("llm turn span");
    assert_eq!(llm.attributes.get("llm.token_count.prompt").unwrap(), 11);
    assert_eq!(llm.attributes.get("llm.token_count.completion").unwrap(), 7);

    let approval = spans
        .iter()
        .find(|span| span.name == "conduit.approval.high")
        .expect("approval span");
    assert_eq!(
        approval
            .attributes
            .get("inference.observation_kind")
            .unwrap(),
        "GUARDRAIL"
    );
}
