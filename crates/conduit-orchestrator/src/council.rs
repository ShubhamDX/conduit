//! Moderated multi-agent council runs for board cards.

use crate::state::{BoardCardRecord, BoardColumn, MessageDirection, NewMessage, RunStatus};
use crate::{
    finish_ledger_run, memory_capability, memory_tools, record_ledger_approval,
    record_ledger_event, run_status_for_end_reason, write_memory, OrchError, OrchestratorConfig,
};
use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{ApprovalMode, StartRequest};
use conduit_core::event::AgentEvent;
use conduit_security::redact::{redact, redact_event};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CouncilOptions {
    pub max_rounds: usize,
}

impl Default for CouncilOptions {
    fn default() -> Self {
        Self { max_rounds: 1 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CouncilTurn {
    pub agent: String,
    pub role: String,
    pub model: Option<String>,
    pub run_id: String,
    pub output: String,
    pub status: RunStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CouncilReport {
    pub card_id: String,
    pub column: BoardColumn,
    pub turns: Vec<CouncilTurn>,
    pub summary: String,
}

pub async fn run_agent_council(
    registry: &AdapterRegistry,
    config: &OrchestratorConfig,
    card_id: &str,
    options: CouncilOptions,
) -> Result<CouncilReport, OrchError> {
    let store = config
        .orchestration_store
        .as_ref()
        .ok_or_else(|| OrchError::Config("orchestration store required for council".into()))?;
    let card = store
        .board_card(card_id)
        .await?
        .ok_or_else(|| OrchError::NotFound(format!("board card not found: {card_id}")))?;
    if card.assignments.is_empty() {
        return Err(OrchError::Config(format!(
            "board card has no agent assignments: {card_id}"
        )));
    }

    let mut turns = Vec::new();
    let mut prior_context = String::new();
    let rounds = options.max_rounds.max(1);

    for round in 1..=rounds {
        for assignment in &card.assignments {
            let labels = assignment_labels(&card, &assignment.agent);
            let adapter = registry.route(&labels)?;
            let run = store.start_run(card_id, adapter.name()).await?;
            let memory_capability = memory_capability(config, card_id, &card.task.labels);
            let memory_tools = memory_tools(config, memory_capability.as_ref());
            let prompt = build_council_prompt(
                &card,
                &assignment.role,
                assignment.model.as_deref(),
                round,
                rounds,
                &prior_context,
                memory_capability.as_ref(),
            );
            let request = StartRequest {
                workspace: config.workspace.clone(),
                prompt,
                model: assignment.model.clone(),
                approval_mode: ApprovalMode::OnRequest,
                security_policy: config.default_policy.clone(),
                memory: memory_capability,
                memory_tools,
                env: HashMap::new(),
            };
            let mut handle = match adapter.start_session(request).await {
                Ok(handle) => handle,
                Err(error) => {
                    finish_ledger_run(config, &run.id, RunStatus::Failed).await?;
                    return Err(error.into());
                }
            };
            let mut output = String::new();
            let mut status = RunStatus::Succeeded;

            while let Some(event) = handle.events.recv().await {
                let event = if config.default_policy.redact_secrets {
                    redact_event(event)
                } else {
                    event
                };
                record_ledger_event(config, &run.id, event.clone()).await?;
                if let AgentEvent::ApprovalRequested { reason, risk, .. } = &event {
                    record_ledger_approval(config, &run.id, reason, risk.clone()).await?;
                }
                match event {
                    AgentEvent::TokenDelta { text } => output.push_str(&text),
                    AgentEvent::SessionEnded { reason } => {
                        status = run_status_for_end_reason(&reason);
                        break;
                    }
                    AgentEvent::Error { message, .. } => {
                        status = RunStatus::Failed;
                        output.push_str(&format!("\n[error] {message}"));
                    }
                    _ => {}
                }
            }

            let output = if config.default_policy.redact_secrets {
                redact(&output)
            } else {
                output
            };
            store
                .record_message(NewMessage {
                    task_id: Some(card_id.to_string()),
                    run_id: Some(run.id.clone()),
                    channel: "council".into(),
                    sender: assignment.agent.clone(),
                    direction: MessageDirection::Inbound,
                    body: output.clone(),
                })
                .await?;
            finish_ledger_run(config, &run.id, status.clone()).await?;
            prior_context.push_str(&format!(
                "\n[round {round}][{} as {}] {}\n",
                assignment.agent, assignment.role, output
            ));
            turns.push(CouncilTurn {
                agent: assignment.agent.clone(),
                role: assignment.role.clone(),
                model: assignment.model.clone(),
                run_id: run.id,
                output,
                status,
            });
        }
    }

    let summary = redact(&summarize_council(&card, &turns));
    record_council_summary(config, card_id, &summary).await?;
    write_memory(config, card_id, &card.task.labels, &summary).await?;
    if let Some(memory) = &config.shared_memory {
        memory
            .upsert(conduit_memory::MemoryEntry {
                key: format!("council:{card_id}:consensus"),
                value: summary.clone(),
                tags: council_memory_tags(&card),
                source: format!("council:{card_id}"),
            })
            .await?;
    }
    let card = store
        .move_board_card(card_id, BoardColumn::SpecReview)
        .await?;

    Ok(CouncilReport {
        card_id: card_id.to_string(),
        column: card.column,
        turns,
        summary,
    })
}

async fn record_council_summary(
    config: &OrchestratorConfig,
    card_id: &str,
    summary: &str,
) -> Result<(), OrchError> {
    if let Some(store) = &config.orchestration_store {
        store
            .record_message(NewMessage {
                task_id: Some(card_id.to_string()),
                run_id: None,
                channel: "council".into(),
                sender: "orchestrator".into(),
                direction: MessageDirection::Outbound,
                body: summary.to_string(),
            })
            .await?;
    }
    Ok(())
}

fn assignment_labels(card: &BoardCardRecord, agent: &str) -> Vec<String> {
    let mut labels = vec![format!("agent:{agent}")];
    labels.extend(card.task.labels.clone());
    labels
}

fn council_memory_tags(card: &BoardCardRecord) -> Vec<String> {
    let mut tags = vec!["council".to_string()];
    for tag in &card.task.labels {
        if !tags.iter().any(|existing| existing == tag) {
            tags.push(redact(tag));
        }
    }
    tags
}

fn build_council_prompt(
    card: &BoardCardRecord,
    role: &str,
    model: Option<&str>,
    round: usize,
    rounds: usize,
    prior_context: &str,
    memory: Option<&conduit_core::adapter::MemoryCapability>,
) -> String {
    let memory = match memory {
        Some(memory) => format!(
            "Shared memory reference: scope={}, tags={}, tools={}. Use memory tools only if extra context is needed.",
            memory.scope,
            memory.tags.join(", "),
            memory.tools.join(", ")
        ),
        None => "Shared memory reference: unavailable.".into(),
    };
    let prior_context = if prior_context.trim().is_empty() {
        "Previous council context: none yet.".to_string()
    } else {
        format!("Previous council context:\n{}", redact(prior_context))
    };
    format!(
        "Agent council card: {}\n\
         Title: {}\n\
         Body: {}\n\
         Labels: {}\n\
         Your council role: {}\n\
         Model hint: {}\n\
         Round: {round}/{rounds}\n\
         {memory}\n\
         {prior_context}\n\n\
         Respond with concise findings, risks, and next actions for this role. Do not assume consensus unless the prior context supports it.",
        redact(&card.task.id),
        redact(&card.task.title),
        redact(&card.task.body),
        card.task
            .labels
            .iter()
            .map(|label| redact(label))
            .collect::<Vec<_>>()
            .join(", "),
        redact(role),
        model.map(redact).unwrap_or_else(|| "default".into())
    )
}

fn summarize_council(card: &BoardCardRecord, turns: &[CouncilTurn]) -> String {
    let mut summary = format!(
        "Council consensus for {}: {}\n\n",
        card.task.id, card.task.title
    );
    for turn in turns {
        summary.push_str(&format!(
            "- {} as {} ({:?}): {}\n",
            turn.agent, turn.role, turn.status, turn.output
        ));
    }
    summary.push_str("\nNext gate: human review of the council output before ready_for_build.");
    summary
}
