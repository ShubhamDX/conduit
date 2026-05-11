//! Conduit orchestration loop.

use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{
    ApprovalMode, MemoryCapability, MemoryToolError, MemoryToolProvider, SecurityPolicy,
    StartRequest,
};
use conduit_core::event::{AgentEvent, EndReason};
use conduit_memory::{MemoryEntry, MemoryError, MemoryQuery, MemoryStore};
use conduit_security::redact::{redact, redact_event};
use conduit_tracker::Issue;
use conduit_tracker::{Tracker, TrackerError};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

pub mod build;
pub mod config;
pub mod council;
pub mod state;
pub mod trace_export;

use state::{
    MessageDirection, NewMessage, NewTask, RunRecord, RunStatus, SqliteOrchestrationStore,
    StateError,
};

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
    #[error("state: {0}")]
    State(#[from] StateError),
    #[error("config: {0}")]
    Config(String),
    #[error("issue not found: {0}")]
    NotFound(String),
}

pub struct OrchestratorConfig {
    pub workspace: PathBuf,
    pub assignee: String,
    pub default_policy: SecurityPolicy,
    pub shared_memory: Option<Arc<dyn MemoryStore>>,
    pub orchestration_store: Option<Arc<SqliteOrchestrationStore>>,
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
    let ledger_run = start_ledger_run(config, &issue, adapter.name()).await?;
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
    let mut handle = match adapter.start_session(request).await {
        Ok(handle) => handle,
        Err(error) => {
            if let Some(run) = &ledger_run {
                finish_ledger_run(config, &run.id, RunStatus::Failed).await?;
            }
            return Err(error.into());
        }
    };
    let mut transcript = String::new();
    let mut final_status = RunStatus::Succeeded;

    while let Some(event) = handle.events.recv().await {
        let event = if config.default_policy.redact_secrets {
            redact_event(event)
        } else {
            event
        };
        if let Some(run) = &ledger_run {
            record_ledger_event(config, &run.id, event.clone()).await?;
            if let AgentEvent::ApprovalRequested { reason, risk, .. } = &event {
                record_ledger_approval(config, &run.id, reason, risk.clone()).await?;
            }
        }
        match event {
            AgentEvent::TokenDelta { text } => transcript.push_str(&text),
            AgentEvent::SessionEnded { reason } => {
                final_status = run_status_for_end_reason(&reason);
                break;
            }
            AgentEvent::Error { message, .. } => {
                final_status = RunStatus::Failed;
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
    if let Some(run) = &ledger_run {
        record_ledger_message(config, issue_id, &run.id, &summary).await?;
        finish_ledger_run(config, &run.id, final_status).await?;
    }
    write_memory(config, issue_id, &issue.labels, &summary).await?;
    tracker.set_state(issue_id, "done").await?;
    Ok(())
}

async fn start_ledger_run(
    config: &OrchestratorConfig,
    issue: &Issue,
    agent: &str,
) -> Result<Option<RunRecord>, OrchError> {
    let Some(store) = &config.orchestration_store else {
        return Ok(None);
    };

    store
        .create_task(NewTask {
            id: issue.id.clone(),
            source: "tracker".into(),
            title: issue.title.clone(),
            body: issue.body.clone(),
            labels: issue.labels.clone(),
        })
        .await?;
    Ok(Some(store.start_run(&issue.id, agent).await?))
}

async fn record_ledger_event(
    config: &OrchestratorConfig,
    run_id: &str,
    event: AgentEvent,
) -> Result<(), OrchError> {
    if let Some(store) = &config.orchestration_store {
        store.record_event(run_id, event).await?;
    }
    Ok(())
}

async fn record_ledger_approval(
    config: &OrchestratorConfig,
    run_id: &str,
    reason: &str,
    risk: conduit_core::event::Risk,
) -> Result<(), OrchError> {
    if let Some(store) = &config.orchestration_store {
        store.request_approval(run_id, reason, risk).await?;
    }
    Ok(())
}

async fn record_ledger_message(
    config: &OrchestratorConfig,
    issue_id: &str,
    run_id: &str,
    body: &str,
) -> Result<(), OrchError> {
    if let Some(store) = &config.orchestration_store {
        store
            .record_message(NewMessage {
                task_id: Some(issue_id.to_string()),
                run_id: Some(run_id.to_string()),
                channel: "tracker".into(),
                sender: "orchestrator".into(),
                direction: MessageDirection::Outbound,
                body: body.to_string(),
            })
            .await?;
    }
    Ok(())
}

async fn finish_ledger_run(
    config: &OrchestratorConfig,
    run_id: &str,
    status: RunStatus,
) -> Result<(), OrchError> {
    if let Some(store) = &config.orchestration_store {
        store.finish_run(run_id, status).await?;
    }
    Ok(())
}

fn run_status_for_end_reason(reason: &EndReason) -> RunStatus {
    match reason {
        EndReason::Completed => RunStatus::Succeeded,
        EndReason::Cancelled => RunStatus::Cancelled,
        EndReason::Failed | EndReason::Timeout => RunStatus::Failed,
    }
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
        tags: dedup_tags(tags.iter().map(|tag| redact(tag)).collect()),
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
        let base_tags: Vec<_> = self.tags.iter().map(|tag| redact(tag)).collect();
        let extra_tags: Vec<_> = args.tags.iter().map(|tag| redact(tag)).collect();
        self.memory
            .upsert(MemoryEntry {
                key: redact(&args.key),
                value: redact(&args.value),
                tags: merge_tags(&base_tags, &extra_tags),
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
    let mut merged = dedup_tags(base.to_vec());
    for tag in extra {
        if !merged.iter().any(|existing| existing == tag) {
            merged.push(tag.clone());
        }
    }
    merged
}

fn dedup_tags(tags: Vec<String>) -> Vec<String> {
    let mut deduped = Vec::new();
    for tag in tags {
        if !deduped.iter().any(|existing| existing == &tag) {
            deduped.push(tag);
        }
    }
    deduped
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
        let redacted_tags = dedup_tags(tags.iter().map(|tag| redact(tag)).collect());
        memory
            .upsert(MemoryEntry {
                key: issue_id.to_string(),
                value: summary.to_string(),
                tags: redacted_tags,
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
