//! Conduit orchestration loop.

use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{ApprovalMode, SecurityPolicy, StartRequest};
use conduit_core::event::AgentEvent;
use conduit_security::redact::redact;
use conduit_tracker::{Tracker, TrackerError};
use std::collections::HashMap;
use std::path::PathBuf;
use thiserror::Error;

pub mod config;

#[derive(Debug, Error)]
pub enum OrchError {
    #[error("tracker: {0}")]
    Tracker(#[from] TrackerError),
    #[error("adapter routing: {0}")]
    Route(#[from] conduit_adapter_registry::RouteError),
    #[error("adapter: {0}")]
    Adapter(#[from] conduit_core::error::AdapterError),
    #[error("issue not found: {0}")]
    NotFound(String),
}

pub struct OrchestratorConfig {
    pub workspace: PathBuf,
    pub assignee: String,
    pub default_policy: SecurityPolicy,
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

    let request = StartRequest {
        workspace: config.workspace.clone(),
        prompt: format!("{}\n\n{}", issue.title, issue.body),
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
    tracker.set_state(issue_id, "done").await?;
    Ok(())
}
