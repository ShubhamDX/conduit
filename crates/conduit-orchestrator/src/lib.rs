//! Conduit orchestration loop.

use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{
    ApprovalMode, MemoryCapability, MemoryToolError, MemoryToolProvider, SecurityPolicy,
    StartRequest,
};
use conduit_core::event::AgentEvent;
use conduit_memory::{MemoryEntry, MemoryError, MemoryQuery, MemoryStore};
use conduit_security::redact::redact;
use conduit_tracker::Issue;
use conduit_tracker::{Tracker, TrackerError};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

pub mod config;

const MEMORY_TOOLS: &[&str] = &["memory_search", "memory_get", "memory_upsert"];
const DEFAULT_MEMORY_LIMIT: usize = 8;
const MAX_MEMORY_LIMIT: usize = 20;

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
    let memory_tools = memory_tools(config, memory_capability.as_ref());

    let request = StartRequest {
        workspace: config.workspace.clone(),
        prompt: build_prompt(&issue, memory_capability.as_ref()),
        model: None,
        approval_mode: ApprovalMode::OnWrite,
        security_policy: config.default_policy.clone(),
        memory: memory_capability,
        memory_tools,
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

fn memory_tools(
    config: &OrchestratorConfig,
    capability: Option<&MemoryCapability>,
) -> Option<Arc<dyn MemoryToolProvider>> {
    let memory = Arc::clone(config.shared_memory.as_ref()?);
    let capability = capability?;
    Some(Arc::new(ScopedMemoryTools {
        memory,
        scope: capability.scope.clone(),
        tags: capability.tags.clone(),
    }))
}

struct ScopedMemoryTools {
    memory: Arc<dyn MemoryStore>,
    scope: String,
    tags: Vec<String>,
}

#[async_trait::async_trait]
impl MemoryToolProvider for ScopedMemoryTools {
    async fn call(
        &self,
        name: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, MemoryToolError> {
        match name {
            "memory_search" => self.search(args).await,
            "memory_get" => self.get(args).await,
            "memory_upsert" => self.upsert(args).await,
            _ => Err(MemoryToolError::unavailable(format!(
                "unknown memory tool: {name}"
            ))),
        }
    }
}

impl ScopedMemoryTools {
    async fn search(&self, args: serde_json::Value) -> Result<serde_json::Value, MemoryToolError> {
        let args: SearchArgs = parse_args(args)?;
        let limit = clamp_limit(args.limit);
        let snapshot = self
            .memory
            .load(MemoryQuery {
                tags: self.tags.clone(),
                limit: MAX_MEMORY_LIMIT,
            })
            .await
            .map_err(|error| MemoryToolError::backend(error.to_string()))?;
        let entries: Vec<_> = snapshot
            .entries
            .into_iter()
            .filter(|entry| self.can_read(entry))
            .filter(|entry| args.tags.is_empty() || tags_overlap(&entry.tags, &args.tags))
            .take(limit)
            .collect();

        Ok(serde_json::json!({ "entries": entries }))
    }

    async fn get(&self, args: serde_json::Value) -> Result<serde_json::Value, MemoryToolError> {
        let args: GetArgs = parse_args(args)?;
        let entry = self
            .memory
            .get(&args.key)
            .await
            .map_err(|error| MemoryToolError::backend(error.to_string()))?
            .filter(|entry| self.can_read(entry));

        Ok(serde_json::json!({ "entry": entry }))
    }

    async fn upsert(&self, args: serde_json::Value) -> Result<serde_json::Value, MemoryToolError> {
        let args: UpsertArgs = parse_args(args)?;
        self.memory
            .upsert(MemoryEntry {
                key: args.key,
                value: redact(&args.value),
                tags: merge_tags(&self.tags, &args.tags),
                source: self.scope.clone(),
            })
            .await
            .map_err(|error| MemoryToolError::backend(error.to_string()))?;

        Ok(serde_json::json!({ "ok": true }))
    }

    fn can_read(&self, entry: &MemoryEntry) -> bool {
        entry.source == self.scope || tags_overlap(&entry.tags, &self.tags)
    }
}

#[derive(Debug, Deserialize)]
struct SearchArgs {
    #[serde(default)]
    tags: Vec<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct GetArgs {
    key: String,
}

#[derive(Debug, Deserialize)]
struct UpsertArgs {
    key: String,
    value: String,
    #[serde(default)]
    tags: Vec<String>,
}

fn parse_args<T: DeserializeOwned>(args: serde_json::Value) -> Result<T, MemoryToolError> {
    serde_json::from_value(args)
        .map_err(|error| MemoryToolError::invalid_request(error.to_string()))
}

fn clamp_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(DEFAULT_MEMORY_LIMIT)
        .clamp(1, MAX_MEMORY_LIMIT)
}

fn merge_tags(base: &[String], extra: &[String]) -> Vec<String> {
    let mut merged = base.to_vec();
    for tag in extra {
        if !merged.iter().any(|existing| existing == tag) {
            merged.push(tag.clone());
        }
    }
    merged
}

fn tags_overlap(left: &[String], right: &[String]) -> bool {
    left.iter()
        .any(|left_tag| right.iter().any(|right_tag| right_tag == left_tag))
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
