use crate::error::AdapterError;
use crate::event::AgentEvent;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

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
    /// Defaults to writable for compatibility with issue-driven coding workflows.
    /// Production workflows should set this explicitly so write access is a
    /// deliberate policy choice.
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
    pub memory: Option<MemoryCapability>,
    pub memory_tools: Option<Arc<dyn MemoryToolProvider>>,
    pub env: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryCapability {
    pub scope: String,
    pub tags: Vec<String>,
    pub tools: Vec<String>,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{code}: {message}")]
pub struct MemoryToolError {
    pub code: String,
    pub message: String,
}

impl MemoryToolError {
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_request".into(),
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: "unavailable".into(),
            message: message.into(),
        }
    }

    pub fn backend(message: impl Into<String>) -> Self {
        Self {
            code: "backend".into(),
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait MemoryToolProvider: Send + Sync {
    async fn call(
        &self,
        name: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, MemoryToolError>;
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
