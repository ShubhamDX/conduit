//! Conduit orchestration loop.

use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{ApprovalMode, SecurityPolicy, StartRequest};
use conduit_core::event::AgentEvent;
use conduit_memory::{MemoryEntry, MemoryError, MemoryQuery, MemorySnapshot, MemoryStore};
use conduit_security::redact::redact;
use conduit_tracker::Issue;
use conduit_tracker::{Tracker, TrackerError};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

pub mod config;

const DEFAULT_MEMORY_LIMIT: usize = 12;

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
    let memory_snapshot = load_memory(config, &issue.labels).await?;

    let request = StartRequest {
        workspace: config.workspace.clone(),
        prompt: build_prompt(&issue, &memory_snapshot),
        model: None,
        approval_mode: ApprovalMode::OnWrite,
        security_policy: config.default_policy.clone(),
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

async fn load_memory(
    config: &OrchestratorConfig,
    tags: &[String],
) -> Result<MemorySnapshot, OrchError> {
    match &config.shared_memory {
        Some(memory) => Ok(memory
            .load(MemoryQuery {
                tags: tags.to_vec(),
                limit: DEFAULT_MEMORY_LIMIT,
            })
            .await?),
        None => Ok(MemorySnapshot::default()),
    }
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

fn build_prompt(issue: &Issue, memory: &MemorySnapshot) -> String {
    let issue_prompt = format!("{}\n\n{}", issue.title, issue.body);
    if memory.entries.is_empty() {
        return issue_prompt;
    }

    let memory_block = memory
        .entries
        .iter()
        .map(|entry| format!("- [{}] {}", entry.key, redact(&entry.value)))
        .collect::<Vec<_>>()
        .join("\n");

    format!("Shared memory:\n{memory_block}\n\nCurrent issue:\n{issue_prompt}")
}
