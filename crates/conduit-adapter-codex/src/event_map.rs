use conduit_core::event::{AgentEvent, EndReason, Risk};
use serde_json::Value;

pub fn map_codex_event(value: &Value) -> Option<AgentEvent> {
    let kind = value.get("kind")?.as_str()?;

    match kind {
        "agent_message_delta" => Some(AgentEvent::TokenDelta {
            text: value.get("delta")?.as_str()?.to_string(),
        }),
        "tool_call_started" => Some(AgentEvent::ToolCallStarted {
            call_id: value.get("call_id")?.as_str()?.to_string(),
            name: value.get("name")?.as_str()?.to_string(),
            args: value.get("args").cloned().unwrap_or(Value::Null),
        }),
        "tool_call_completed" => Some(AgentEvent::ToolCallCompleted {
            call_id: value.get("call_id")?.as_str()?.to_string(),
            ok: value
                .get("ok")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            output: value
                .get("output")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string(),
        }),
        "approval_requested" => Some(AgentEvent::ApprovalRequested {
            call_id: value.get("call_id")?.as_str()?.to_string(),
            reason: value
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string(),
            risk: map_risk(
                value
                    .get("risk")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("medium"),
            ),
        }),
        "turn_completed" => Some(AgentEvent::TurnCompleted {
            tokens_in: value
                .get("tokens_in")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
            tokens_out: value
                .get("tokens_out")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        }),
        "session_ended" => Some(AgentEvent::SessionEnded {
            reason: map_end_reason(
                value
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("completed"),
            ),
        }),
        _ => None,
    }
}

fn map_risk(value: &str) -> Risk {
    match value {
        "low" => Risk::Low,
        "high" => Risk::High,
        _ => Risk::Medium,
    }
}

fn map_end_reason(value: &str) -> EndReason {
    match value {
        "failed" => EndReason::Failed,
        "cancelled" => EndReason::Cancelled,
        "timeout" => EndReason::Timeout,
        _ => EndReason::Completed,
    }
}
