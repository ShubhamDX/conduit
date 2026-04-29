use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    SessionStarted {
        session_id: String,
        agent: String,
        model: String,
    },
    TokenDelta {
        text: String,
    },
    ToolCallStarted {
        call_id: String,
        name: String,
        args: serde_json::Value,
    },
    ToolCallCompleted {
        call_id: String,
        ok: bool,
        output: String,
    },
    ApprovalRequested {
        call_id: String,
        reason: String,
        risk: Risk,
    },
    TurnCompleted {
        tokens_in: u64,
        tokens_out: u64,
    },
    SessionEnded {
        reason: EndReason,
    },
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Risk {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    Completed,
    Failed,
    Cancelled,
    Timeout,
}
