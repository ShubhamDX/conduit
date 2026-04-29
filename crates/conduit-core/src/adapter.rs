use crate::error::AdapterError;
use crate::event::AgentEvent;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    Never,
    OnRequest,
    OnWrite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityPolicy {
    pub egress_allowlist: Vec<String>,
    pub max_cpu_secs: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub max_open_files: Option<u64>,
    pub workspace_writable: bool,
    pub redact_secrets: bool,
}

impl Default for SecurityPolicy {
    fn default() -> Self {
        Self {
            egress_allowlist: Vec::new(),
            max_cpu_secs: None,
            max_memory_bytes: None,
            max_open_files: None,
            workspace_writable: true,
            redact_secrets: true,
        }
    }
}

pub struct StartRequest {
    pub workspace: PathBuf,
    pub prompt: String,
    pub model: Option<String>,
    pub approval_mode: ApprovalMode,
    pub security_policy: SecurityPolicy,
    pub env: HashMap<String, String>,
}

pub struct SessionHandle {
    pub session_id: String,
    pub events: tokio::sync::mpsc::Receiver<AgentEvent>,
}

#[async_trait]
pub trait AgentAdapter: Send + Sync {
    fn name(&self) -> &str;

    async fn start_session(&self, req: StartRequest) -> Result<SessionHandle, AdapterError>;

    async fn stop_session(&self, session_id: &str) -> Result<(), AdapterError>;
}
