use crate::state::{
    ApprovalRecord, EventRecord, MessageDirection, MessageRecord, RunRecord, RunStatus, TaskRecord,
    TaskSnapshot,
};
use conduit_core::event::{AgentEvent, EndReason, Risk};
use conduit_security::redact::{redact, redact_json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HaloExportOptions {
    pub project_id: String,
    pub service_name: String,
    pub service_version: Option<String>,
    pub deployment_environment: Option<String>,
}

impl Default for HaloExportOptions {
    fn default() -> Self {
        Self {
            project_id: "conduit".into(),
            service_name: "conduit".into(),
            service_version: None,
            deployment_environment: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HaloTraceSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub trace_state: String,
    pub name: String,
    pub kind: String,
    pub start_time: String,
    pub end_time: String,
    pub status: HaloStatus,
    pub resource: HaloResource,
    pub scope: HaloScope,
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaloStatus {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HaloResource {
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaloScope {
    pub name: String,
    pub version: String,
}

pub fn export_halo_spans(
    snapshots: &[TaskSnapshot],
    options: &HaloExportOptions,
) -> Vec<HaloTraceSpan> {
    let mut spans = Vec::new();

    for snapshot in snapshots {
        for run in &snapshot.runs {
            export_run_spans(&mut spans, snapshot, run, options);
        }
    }

    spans
}

fn export_run_spans(
    spans: &mut Vec<HaloTraceSpan>,
    snapshot: &TaskSnapshot,
    run: &RunRecord,
    options: &HaloExportOptions,
) {
    let trace_id = trace_id_for(&run.id);
    let root_span_id = span_id_for(&[&run.id, "root"]);
    let run_events = events_for_run(snapshot, &run.id);
    let run_approvals = approvals_for_run(snapshot, &run.id);
    let run_messages = messages_for_run(snapshot, &run.id);
    let end_ms = run_end_ms(run, &run_events, &run_approvals, &run_messages);

    spans.push(agent_span(
        &trace_id,
        &root_span_id,
        &snapshot.task,
        run,
        options,
        end_ms,
    ));
    spans.extend(event_spans(
        &trace_id,
        &root_span_id,
        &snapshot.task,
        run,
        options,
        &run_events,
    ));
    spans.extend(
        run_approvals
            .into_iter()
            .map(|approval| approval_span(&trace_id, &root_span_id, run, approval, options)),
    );
    spans.extend(
        run_messages
            .into_iter()
            .map(|message| message_span(&trace_id, &root_span_id, run, message, options)),
    );
}

fn agent_span(
    trace_id: &str,
    root_span_id: &str,
    task: &TaskRecord,
    run: &RunRecord,
    options: &HaloExportOptions,
    end_ms: i64,
) -> HaloTraceSpan {
    let mut attributes = base_attributes(options, "AGENT", &run.agent);
    attributes.insert("agent.name".into(), json!(redact(&run.agent)));
    attributes.insert("input.value".into(), json!(redact(&task.body)));
    attributes.insert("conduit.task.id".into(), json!(redact(&task.id)));
    attributes.insert("conduit.task.source".into(), json!(redact(&task.source)));
    attributes.insert("conduit.task.title".into(), json!(redact(&task.title)));
    attributes.insert(
        "conduit.task.labels".into(),
        Value::Array(
            task.labels
                .iter()
                .map(|label| Value::String(redact(label)))
                .collect(),
        ),
    );
    attributes.insert("conduit.task.status".into(), json!(task_status(task)));
    attributes.insert("conduit.run.id".into(), json!(redact(&run.id)));
    attributes.insert("conduit.run.status".into(), json!(run_status(run)));

    span(
        trace_id,
        root_span_id,
        "",
        &format!("conduit.run.{}", sanitize_name(&run.agent)),
        "SPAN_KIND_INTERNAL",
        run.started_at_ms,
        end_ms,
        status_for_run(&run.status),
        options,
        attributes,
    )
}

fn event_spans(
    trace_id: &str,
    root_span_id: &str,
    task: &TaskRecord,
    run: &RunRecord,
    options: &HaloExportOptions,
    events: &[&EventRecord],
) -> Vec<HaloTraceSpan> {
    let mut spans = Vec::new();
    let mut pending_tools = BTreeMap::new();
    let mut pending_llm = None;

    for event in events {
        let parsed = serde_json::from_value::<AgentEvent>(event.payload.clone()).ok();
        match parsed {
            Some(AgentEvent::TokenDelta { text }) => {
                let turn = pending_llm.get_or_insert_with(|| PendingLlmTurn {
                    start_ms: event.created_at_ms,
                    first_sequence: event.sequence,
                    last_sequence: event.sequence,
                    text: String::new(),
                });
                turn.last_sequence = event.sequence;
                turn.text.push_str(&redact(&text));
            }
            Some(AgentEvent::TurnCompleted {
                tokens_in,
                tokens_out,
            }) => flush_llm_turn(
                &mut spans,
                &mut pending_llm,
                trace_id,
                root_span_id,
                task,
                run,
                options,
                event.created_at_ms,
                Some((tokens_in, tokens_out)),
                ok_status(),
            ),
            Some(AgentEvent::ToolCallStarted {
                call_id,
                name,
                args,
            }) => {
                pending_tools.insert(
                    call_id.clone(),
                    PendingToolCall {
                        call_id,
                        name,
                        args,
                        started_at_ms: event.created_at_ms,
                        sequence: event.sequence,
                    },
                );
            }
            Some(AgentEvent::ToolCallCompleted {
                call_id,
                ok,
                output,
            }) => {
                let pending = pending_tools.remove(&call_id);
                spans.push(tool_span(
                    trace_id,
                    root_span_id,
                    run,
                    options,
                    event,
                    pending,
                    &call_id,
                    ok,
                    &output,
                ));
            }
            Some(AgentEvent::Error { code, message }) => {
                flush_llm_turn(
                    &mut spans,
                    &mut pending_llm,
                    trace_id,
                    root_span_id,
                    task,
                    run,
                    options,
                    event.created_at_ms,
                    None,
                    error_status(&message),
                );
                spans.push(error_span(
                    trace_id,
                    root_span_id,
                    run,
                    options,
                    event,
                    &code,
                    &message,
                ));
            }
            Some(AgentEvent::SessionEnded { reason }) => {
                flush_llm_turn(
                    &mut spans,
                    &mut pending_llm,
                    trace_id,
                    root_span_id,
                    task,
                    run,
                    options,
                    event.created_at_ms,
                    None,
                    status_for_end_reason(&reason),
                );
            }
            Some(AgentEvent::ApprovalRequested { .. })
            | Some(AgentEvent::SessionStarted { .. }) => {}
            None => spans.push(raw_event_span(trace_id, root_span_id, run, options, event)),
        }
    }

    flush_llm_turn(
        &mut spans,
        &mut pending_llm,
        trace_id,
        root_span_id,
        task,
        run,
        options,
        run.completed_at_ms.unwrap_or(run.started_at_ms),
        None,
        ok_status(),
    );

    spans.extend(
        pending_tools
            .into_values()
            .map(|pending| pending_tool_span(trace_id, root_span_id, run, options, pending)),
    );
    spans
}

fn tool_span(
    trace_id: &str,
    root_span_id: &str,
    run: &RunRecord,
    options: &HaloExportOptions,
    completed: &EventRecord,
    pending: Option<PendingToolCall>,
    call_id: &str,
    ok: bool,
    output: &str,
) -> HaloTraceSpan {
    let name = pending
        .as_ref()
        .map(|pending| pending.name.as_str())
        .unwrap_or("unknown");
    let started_at_ms = pending
        .as_ref()
        .map(|pending| pending.started_at_ms)
        .unwrap_or(completed.created_at_ms);
    let started_sequence = pending
        .as_ref()
        .map(|pending| pending.sequence)
        .unwrap_or(completed.sequence);
    let args = pending
        .as_ref()
        .map(|pending| redact_json(pending.args.clone()))
        .unwrap_or(Value::Null);
    let mut attributes = base_attributes(options, "TOOL", &run.agent);
    attributes.insert("tool.name".into(), json!(redact(name)));
    attributes.insert("input.value".into(), json!(json_string(&args)));
    attributes.insert("output.value".into(), json!(redact(output)));
    attributes.insert("conduit.tool.call_id".into(), json!(redact(call_id)));
    attributes.insert("conduit.run.id".into(), json!(redact(&run.id)));
    attributes.insert(
        "conduit.event.sequence_start".into(),
        json!(started_sequence),
    );
    attributes.insert(
        "conduit.event.sequence_end".into(),
        json!(completed.sequence),
    );

    span(
        trace_id,
        &span_id_for(&[&run.id, "tool", call_id, &completed.sequence.to_string()]),
        root_span_id,
        &format!("conduit.tool.{}", sanitize_name(name)),
        "SPAN_KIND_INTERNAL",
        started_at_ms,
        completed.created_at_ms,
        if ok {
            ok_status()
        } else {
            error_status("tool call failed")
        },
        options,
        attributes,
    )
}

fn pending_tool_span(
    trace_id: &str,
    root_span_id: &str,
    run: &RunRecord,
    options: &HaloExportOptions,
    pending: PendingToolCall,
) -> HaloTraceSpan {
    let mut attributes = base_attributes(options, "TOOL", &run.agent);
    attributes.insert("tool.name".into(), json!(redact(&pending.name)));
    attributes.insert(
        "input.value".into(),
        json!(json_string(&redact_json(pending.args))),
    );
    attributes.insert(
        "conduit.tool.call_id".into(),
        json!(redact(&pending.call_id)),
    );
    attributes.insert("conduit.run.id".into(), json!(redact(&run.id)));
    attributes.insert(
        "conduit.event.sequence_start".into(),
        json!(pending.sequence),
    );

    span(
        trace_id,
        &span_id_for(&[&run.id, "tool_pending", &pending.call_id]),
        root_span_id,
        &format!("conduit.tool.{}", sanitize_name(&pending.name)),
        "SPAN_KIND_INTERNAL",
        pending.started_at_ms,
        pending.started_at_ms,
        unset_status(),
        options,
        attributes,
    )
}

#[allow(clippy::too_many_arguments)]
fn flush_llm_turn(
    spans: &mut Vec<HaloTraceSpan>,
    pending_llm: &mut Option<PendingLlmTurn>,
    trace_id: &str,
    root_span_id: &str,
    task: &TaskRecord,
    run: &RunRecord,
    options: &HaloExportOptions,
    end_ms: i64,
    tokens: Option<(u64, u64)>,
    status: HaloStatus,
) {
    let Some(turn) = pending_llm.take() else {
        return;
    };
    let mut attributes = base_attributes(options, "LLM", &run.agent);
    attributes.insert(
        "llm.output_messages".into(),
        json!(json_string(
            &json!([{ "role": "assistant", "content": redact(&turn.text) }])
        )),
    );
    attributes.insert("conduit.task.id".into(), json!(redact(&task.id)));
    attributes.insert("conduit.run.id".into(), json!(redact(&run.id)));
    attributes.insert(
        "conduit.event.sequence_start".into(),
        json!(turn.first_sequence),
    );
    attributes.insert(
        "conduit.event.sequence_end".into(),
        json!(turn.last_sequence),
    );
    if let Some((tokens_in, tokens_out)) = tokens {
        attributes.insert("llm.token_count.prompt".into(), json!(tokens_in));
        attributes.insert("llm.token_count.completion".into(), json!(tokens_out));
        attributes.insert("inference.llm.input_tokens".into(), json!(tokens_in));
        attributes.insert("inference.llm.output_tokens".into(), json!(tokens_out));
    }

    spans.push(span(
        trace_id,
        &span_id_for(&[&run.id, "llm", &turn.first_sequence.to_string()]),
        root_span_id,
        "conduit.llm.turn",
        "SPAN_KIND_CLIENT",
        turn.start_ms,
        end_ms,
        status,
        options,
        attributes,
    ));
}

fn approval_span(
    trace_id: &str,
    root_span_id: &str,
    run: &RunRecord,
    approval: &ApprovalRecord,
    options: &HaloExportOptions,
) -> HaloTraceSpan {
    let mut attributes = base_attributes(options, "GUARDRAIL", &run.agent);
    attributes.insert("conduit.approval.id".into(), json!(redact(&approval.id)));
    attributes.insert(
        "conduit.approval.status".into(),
        json!(redact(&approval.status)),
    );
    attributes.insert(
        "conduit.approval.risk".into(),
        json!(risk_str(&approval.risk)),
    );
    attributes.insert(
        "conduit.approval.reason".into(),
        json!(redact(&approval.reason)),
    );
    attributes.insert("conduit.run.id".into(), json!(redact(&run.id)));

    span(
        trace_id,
        &span_id_for(&[&run.id, "approval", &approval.id]),
        root_span_id,
        &format!("conduit.approval.{}", risk_str(&approval.risk)),
        "SPAN_KIND_INTERNAL",
        approval.created_at_ms,
        approval.resolved_at_ms.unwrap_or(approval.created_at_ms),
        status_for_approval(&approval.status),
        options,
        attributes,
    )
}

fn message_span(
    trace_id: &str,
    root_span_id: &str,
    run: &RunRecord,
    message: &MessageRecord,
    options: &HaloExportOptions,
) -> HaloTraceSpan {
    let direction = match message.direction {
        MessageDirection::Inbound => "inbound",
        MessageDirection::Outbound => "outbound",
    };
    let mut attributes = base_attributes(options, "CHAIN", &run.agent);
    attributes.insert("input.value".into(), json!(redact(&message.body)));
    attributes.insert(
        "conduit.message.channel".into(),
        json!(redact(&message.channel)),
    );
    attributes.insert(
        "conduit.message.sender".into(),
        json!(redact(&message.sender)),
    );
    attributes.insert("conduit.message.direction".into(), json!(direction));
    attributes.insert("conduit.run.id".into(), json!(redact(&run.id)));

    span(
        trace_id,
        &span_id_for(&[&run.id, "message", &message.id.to_string()]),
        root_span_id,
        &format!(
            "conduit.message.{}.{}",
            sanitize_name(&message.channel),
            direction
        ),
        "SPAN_KIND_INTERNAL",
        message.created_at_ms,
        message.created_at_ms,
        ok_status(),
        options,
        attributes,
    )
}

fn error_span(
    trace_id: &str,
    root_span_id: &str,
    run: &RunRecord,
    options: &HaloExportOptions,
    event: &EventRecord,
    code: &str,
    message: &str,
) -> HaloTraceSpan {
    let mut attributes = base_attributes(options, "CHAIN", &run.agent);
    attributes.insert("conduit.error.code".into(), json!(redact(code)));
    attributes.insert("conduit.error.message".into(), json!(redact(message)));
    attributes.insert("conduit.run.id".into(), json!(redact(&run.id)));
    attributes.insert("conduit.event.sequence".into(), json!(event.sequence));

    span(
        trace_id,
        &span_id_for(&[&run.id, "error", &event.sequence.to_string()]),
        root_span_id,
        &format!("conduit.error.{}", sanitize_name(code)),
        "SPAN_KIND_INTERNAL",
        event.created_at_ms,
        event.created_at_ms,
        error_status(message),
        options,
        attributes,
    )
}

fn raw_event_span(
    trace_id: &str,
    root_span_id: &str,
    run: &RunRecord,
    options: &HaloExportOptions,
    event: &EventRecord,
) -> HaloTraceSpan {
    let mut attributes = base_attributes(options, "SPAN", &run.agent);
    attributes.insert("conduit.run.id".into(), json!(redact(&run.id)));
    attributes.insert("conduit.event.sequence".into(), json!(event.sequence));
    attributes.insert(
        "conduit.event.type".into(),
        json!(redact(&event.event_type)),
    );
    attributes.insert(
        "conduit.event.payload".into(),
        redact_json(event.payload.clone()),
    );

    span(
        trace_id,
        &span_id_for(&[&run.id, "event", &event.sequence.to_string()]),
        root_span_id,
        &format!("conduit.event.{}", sanitize_name(&event.event_type)),
        "SPAN_KIND_INTERNAL",
        event.created_at_ms,
        event.created_at_ms,
        unset_status(),
        options,
        attributes,
    )
}

#[allow(clippy::too_many_arguments)]
fn span(
    trace_id: &str,
    span_id: &str,
    parent_span_id: &str,
    name: &str,
    kind: &str,
    start_ms: i64,
    end_ms: i64,
    status: HaloStatus,
    options: &HaloExportOptions,
    attributes: BTreeMap<String, Value>,
) -> HaloTraceSpan {
    HaloTraceSpan {
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        parent_span_id: parent_span_id.to_string(),
        trace_state: String::new(),
        name: name.to_string(),
        kind: kind.to_string(),
        start_time: format_otlp_time(start_ms),
        end_time: format_otlp_time(end_ms.max(start_ms)),
        status,
        resource: resource(options),
        scope: HaloScope {
            name: "conduit".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
        attributes,
    }
}

fn base_attributes(
    options: &HaloExportOptions,
    observation_kind: &str,
    agent_name: &str,
) -> BTreeMap<String, Value> {
    let mut attributes = BTreeMap::new();
    attributes.insert("openinference.span.kind".into(), json!(observation_kind));
    attributes.insert("inference.export.schema_version".into(), json!(1));
    attributes.insert(
        "inference.project_id".into(),
        json!(redact(&options.project_id)),
    );
    attributes.insert("inference.observation_kind".into(), json!(observation_kind));
    attributes.insert("inference.agent_name".into(), json!(redact(agent_name)));
    attributes
}

fn resource(options: &HaloExportOptions) -> HaloResource {
    let mut attributes = BTreeMap::new();
    attributes.insert("service.name".into(), json!(redact(&options.service_name)));
    if let Some(version) = &options.service_version {
        attributes.insert("service.version".into(), json!(redact(version)));
    }
    if let Some(environment) = &options.deployment_environment {
        attributes.insert("deployment.environment".into(), json!(redact(environment)));
    }
    HaloResource { attributes }
}

fn events_for_run<'a>(snapshot: &'a TaskSnapshot, run_id: &str) -> Vec<&'a EventRecord> {
    let mut events = snapshot
        .events
        .iter()
        .filter(|event| event.run_id == run_id)
        .collect::<Vec<_>>();
    events.sort_by_key(|event| event.sequence);
    events
}

fn approvals_for_run<'a>(snapshot: &'a TaskSnapshot, run_id: &str) -> Vec<&'a ApprovalRecord> {
    let mut approvals = snapshot
        .approvals
        .iter()
        .filter(|approval| approval.run_id == run_id)
        .collect::<Vec<_>>();
    approvals.sort_by_key(|approval| (approval.created_at_ms, approval.id.clone()));
    approvals
}

fn messages_for_run<'a>(snapshot: &'a TaskSnapshot, run_id: &str) -> Vec<&'a MessageRecord> {
    let mut messages = snapshot
        .messages
        .iter()
        .filter(|message| message.run_id.as_deref() == Some(run_id))
        .collect::<Vec<_>>();
    messages.sort_by_key(|message| (message.created_at_ms, message.id));
    messages
}

fn run_end_ms(
    run: &RunRecord,
    events: &[&EventRecord],
    approvals: &[&ApprovalRecord],
    messages: &[&MessageRecord],
) -> i64 {
    let event_max = events.iter().map(|event| event.created_at_ms).max();
    let approval_max = approvals
        .iter()
        .map(|approval| approval.resolved_at_ms.unwrap_or(approval.created_at_ms))
        .max();
    let message_max = messages.iter().map(|message| message.created_at_ms).max();
    [run.completed_at_ms, event_max, approval_max, message_max]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(run.started_at_ms)
        .max(run.started_at_ms)
}

fn status_for_run(status: &RunStatus) -> HaloStatus {
    match status {
        RunStatus::Running => unset_status(),
        RunStatus::Succeeded => ok_status(),
        RunStatus::Failed => error_status("run failed"),
        RunStatus::Cancelled => error_status("run cancelled"),
    }
}

fn status_for_end_reason(reason: &EndReason) -> HaloStatus {
    match reason {
        EndReason::Completed => ok_status(),
        EndReason::Failed => error_status("session failed"),
        EndReason::Cancelled => error_status("session cancelled"),
        EndReason::Timeout => error_status("session timed out"),
    }
}

fn status_for_approval(status: &str) -> HaloStatus {
    match status {
        "approved" => ok_status(),
        "denied" => error_status("approval denied"),
        _ => unset_status(),
    }
}

fn ok_status() -> HaloStatus {
    HaloStatus {
        code: "STATUS_CODE_OK".into(),
        message: String::new(),
    }
}

fn unset_status() -> HaloStatus {
    HaloStatus {
        code: "STATUS_CODE_UNSET".into(),
        message: String::new(),
    }
}

fn error_status(message: &str) -> HaloStatus {
    HaloStatus {
        code: "STATUS_CODE_ERROR".into(),
        message: redact(message),
    }
}

fn task_status(task: &TaskRecord) -> &'static str {
    match &task.status {
        crate::state::TaskStatus::Pending => "pending",
        crate::state::TaskStatus::Running => "running",
        crate::state::TaskStatus::Done => "done",
        crate::state::TaskStatus::Failed => "failed",
        crate::state::TaskStatus::Cancelled => "cancelled",
    }
}

fn run_status(run: &RunRecord) -> &'static str {
    match &run.status {
        RunStatus::Running => "running",
        RunStatus::Succeeded => "succeeded",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

fn risk_str(risk: &Risk) -> &'static str {
    match risk {
        Risk::Low => "low",
        Risk::Medium => "medium",
        Risk::High => "high",
    }
}

fn json_string(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".into())
}

fn trace_id_for(run_id: &str) -> String {
    format!(
        "{:016x}{:016x}",
        hash64(&["trace", "a", run_id]),
        hash64(&["trace", "b", run_id])
    )
}

fn span_id_for(parts: &[&str]) -> String {
    format!("{:016x}", hash64(parts))
}

fn hash64(parts: &[&str]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for part in parts {
        part.hash(&mut hasher);
    }
    hasher.finish()
}

fn sanitize_name(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".into()
    } else {
        out
    }
}

fn format_otlp_time(ms: i64) -> String {
    let seconds = ms.div_euclid(1_000);
    let millis = ms.rem_euclid(1_000);
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}000000Z")
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

struct PendingToolCall {
    call_id: String,
    name: String,
    args: Value,
    started_at_ms: i64,
    sequence: i64,
}

struct PendingLlmTurn {
    start_ms: i64,
    first_sequence: i64,
    last_sequence: i64,
    text: String,
}
