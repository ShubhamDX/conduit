//! Conduit orchestration loop.

use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{ApprovalMode, MemoryCapability, SecurityPolicy, StartRequest};
use conduit_core::event::AgentEvent;
use conduit_memory::{MemoryEntry, MemoryError, MemoryStore};
use conduit_security::redact::redact;
use conduit_tracker::Issue;
use conduit_tracker::{Tracker, TrackerError};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

pub mod config;

const MEMORY_TOOLS: &[&str] = &["memory_search", "memory_get", "memory_upsert"];

#[derive(Debug, Error)]
pub enum OrchError {
    #[error("tracker: {0}")]
    Tracker(#[from] TrackerError),
    #[error("adapter routing: {0}")]
    Route(#[from] conduit_adapter_registry::RouteError),
    #[error("adapter: {0}")]
    Adapter(#[from] conduit_core::error::AdapterError),
    #[error("memory: {0}")]
    Memory(#[from] MemoryError),
    #[error("issue not found: {0}")]
    NotFound(String),
}

pub struct OrchestratorConfig {
    pub workspace: PathBuf,
    pub assignee: String,
    pub default_policy: SecurityPolicy,
    pub shared_memory: Option<Arc<dyn MemoryStore>>,
}

pub async fn run_one_issue(
    tracker: &(dyn Tracker + Send + Sync),
    registry: &AdapterRegistry,
    config: &OrchestratorConfig,
    issue_id: &str,
) -> Result<(), OrchError> {
    let issues = tracker.fetch_assigned(&config.assignee).await?;
    let issue = issues
        .into_iter()
        .find(|issue| issue.id == issue_id)
        .ok_or_else(|| OrchError::NotFound(issue_id.to_string()))?;

    let adapter = registry.route(&issue.labels)?;
    tracker.set_state(issue_id, "in_progress").await?;
    let memory_capability = memory_capability(config, issue_id, &issue.labels);

    let request = StartRequest {
        workspace: config.workspace.clone(),
        prompt: build_prompt(&issue, memory_capability.as_ref()),
        model: None,
        approval_mode: ApprovalMode::OnWrite,
        security_policy: config.default_policy.clone(),
        memory: memory_capability,
        env: HashMap::new(),
    };
    let mut handle = adapter.start_session(request).await?;
    let mut transcript = String::new();

    while let Some(event) = handle.events.recv().await {
        match event {
            AgentEvent::TokenDelta { text } => transcript.push_str(&text),
            AgentEvent::SessionEnded { .. } => break,
            AgentEvent::Error { message, .. } => {
                transcript.push_str(&format!("\n[error] {message}"));
            }
            _ => {}
        }
    }

    let summary = if config.default_policy.redact_secrets {
        redact(&transcript)
    } else {
        transcript
    };
    tracker.post_comment(issue_id, &summary).await?;
    write_memory(config, issue_id, &issue.labels, &summary).await?;
    tracker.set_state(issue_id, "done").await?;
    Ok(())
}

fn memory_capability(
    config: &OrchestratorConfig,
    issue_id: &str,
    tags: &[String],
) -> Option<MemoryCapability> {
    if config.shared_memory.is_none() {
        return None;
    }

    Some(MemoryCapability {
        scope: format!("issue:{issue_id}"),
        tags: tags.to_vec(),
        tools: MEMORY_TOOLS
            .iter()
            .map(|tool| (*tool).to_string())
            .collect(),
    })
}

async fn write_memory(
    config: &OrchestratorConfig,
    issue_id: &str,
    tags: &[String],
    summary: &str,
) -> Result<(), OrchError> {
    if let Some(memory) = &config.shared_memory {
        memory
            .upsert(MemoryEntry {
                key: issue_id.to_string(),
                value: summary.to_string(),
                tags: tags.to_vec(),
                source: format!("issue:{issue_id}"),
            })
            .await?;
    }

    Ok(())
}

fn build_prompt(issue: &Issue, memory: Option<&MemoryCapability>) -> String {
    let issue_prompt = format!("{}\n\n{}", issue.title, issue.body);

    match memory {
        Some(memory) => format!(
            "Shared memory is available by capability reference.\n\
             Scope: {}\n\
             Tags: {}\n\
             Tools: {}\n\
             Use memory tools only when extra context is needed; do not assume memory contents are already in this prompt.\n\n\
             Current issue:\n{}",
            memory.scope,
            memory.tags.join(", "),
            memory.tools.join(", "),
            issue_prompt
        ),
        None => issue_prompt,
    }
}
