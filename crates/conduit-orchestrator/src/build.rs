//! Build and review handoff for approved board cards.

use crate::state::{
    BoardAssignmentRecord, BoardCardRecord, BoardColumn, MessageDirection, NewMessage, RunStatus,
};
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BuildReviewOptions {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildReviewTurn {
    pub agent: String,
    pub role: String,
    pub model: Option<String>,
    pub run_id: String,
    pub output: String,
    pub status: RunStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildReviewReport {
    pub card_id: String,
    pub column: BoardColumn,
    pub build_turns: Vec<BuildReviewTurn>,
    pub review_turns: Vec<BuildReviewTurn>,
    pub summary: String,
}

pub async fn run_build_review(
    registry: &AdapterRegistry,
    config: &OrchestratorConfig,
    card_id: &str,
    _options: BuildReviewOptions,
) -> Result<BuildReviewReport, OrchError> {
    let store = config
        .orchestration_store
        .as_ref()
        .ok_or_else(|| OrchError::Config("orchestration store required for build".into()))?;
    let card = store
        .board_card(card_id)
        .await?
        .ok_or_else(|| OrchError::NotFound(format!("board card not found: {card_id}")))?;
    if card.column != BoardColumn::ReadyForBuild {
        return Err(OrchError::Config(format!(
            "board card {card_id} must be ready_for_build before build start; current column: {}",
            card.column.as_str()
        )));
    }

    let coders = assignments_for_role(&card, "coder");
    if coders.is_empty() {
        return Err(OrchError::Config(format!(
            "board card has no coder assignments: {card_id}"
        )));
    }
    let reviewers = assignments_for_role(&card, "reviewer");
    if reviewers.is_empty() {
        return Err(OrchError::Config(format!(
            "board card has no reviewer assignments: {card_id}"
        )));
    }

    store.move_board_card(card_id, BoardColumn::InDev).await?;
    let mut build_turns = Vec::new();
    let mut implementation_context = String::new();
    for assignment in coders {
        let memory_capability = memory_capability(config, card_id, &card.task.labels);
        let prompt = build_prompt(&card, assignment, None, memory_capability.as_ref());
        let turn = run_assignment_turn(
            registry,
            config,
            &card,
            assignment,
            prompt,
            "build",
            ApprovalMode::OnWrite,
        )
        .await?;
        implementation_context.push_str(&format!(
            "\n[build][{} as {}][{:?}] {}\n",
            turn.agent, turn.role, turn.status, turn.output
        ));
        build_turns.push(turn);
    }

    let mut review_turns = Vec::new();
    if build_turns
        .iter()
        .all(|turn| turn.status == RunStatus::Succeeded)
    {
        store
            .move_board_card(card_id, BoardColumn::InReview)
            .await?;
        for assignment in reviewers {
            let memory_capability = memory_capability(config, card_id, &card.task.labels);
            let prompt = review_prompt(
                &card,
                assignment,
                &implementation_context,
                memory_capability.as_ref(),
            );
            let turn = run_assignment_turn(
                registry,
                config,
                &card,
                assignment,
                prompt,
                "review",
                ApprovalMode::OnRequest,
            )
            .await?;
            review_turns.push(turn);
        }
    }

    let summary = redact(&summarize_build_review(&card, &build_turns, &review_turns));
    record_build_review_summary(config, card_id, &summary).await?;
    write_memory(config, card_id, &card.task.labels, &summary).await?;
    if let Some(memory) = &config.shared_memory {
        memory
            .upsert(conduit_memory::MemoryEntry {
                key: format!("build:{card_id}:handoff"),
                value: summary.clone(),
                tags: build_memory_tags(&card),
                source: format!("build:{card_id}"),
            })
            .await?;
    }
    let card = store
        .move_board_card(card_id, BoardColumn::HumanReview)
        .await?;

    Ok(BuildReviewReport {
        card_id: card_id.to_string(),
        column: card.column,
        build_turns,
        review_turns,
        summary,
    })
}

async fn run_assignment_turn(
    registry: &AdapterRegistry,
    config: &OrchestratorConfig,
    card: &BoardCardRecord,
    assignment: &BoardAssignmentRecord,
    prompt: String,
    channel: &str,
    approval_mode: ApprovalMode,
) -> Result<BuildReviewTurn, OrchError> {
    let store = config
        .orchestration_store
        .as_ref()
        .ok_or_else(|| OrchError::Config("orchestration store required for build".into()))?;
    let labels = assignment_labels(card, &assignment.agent);
    let adapter = registry.route(&labels)?;
    let run = store.start_run(&card.task.id, adapter.name()).await?;
    let memory_capability = memory_capability(config, &card.task.id, &card.task.labels);
    let memory_tools = memory_tools(config, memory_capability.as_ref());
    let request = StartRequest {
        workspace: config.workspace.clone(),
        prompt,
        model: assignment.model.clone(),
        approval_mode,
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
            task_id: Some(card.task.id.clone()),
            run_id: Some(run.id.clone()),
            channel: channel.to_string(),
            sender: assignment.agent.clone(),
            direction: MessageDirection::Inbound,
            body: output.clone(),
        })
        .await?;
    finish_ledger_run(config, &run.id, status.clone()).await?;

    Ok(BuildReviewTurn {
        agent: assignment.agent.clone(),
        role: assignment.role.clone(),
        model: assignment.model.clone(),
        run_id: run.id,
        output,
        status,
    })
}

async fn record_build_review_summary(
    config: &OrchestratorConfig,
    card_id: &str,
    summary: &str,
) -> Result<(), OrchError> {
    if let Some(store) = &config.orchestration_store {
        store
            .record_message(NewMessage {
                task_id: Some(card_id.to_string()),
                run_id: None,
                channel: "build_review".into(),
                sender: "orchestrator".into(),
                direction: MessageDirection::Outbound,
                body: summary.to_string(),
            })
            .await?;
    }
    Ok(())
}

fn assignments_for_role<'a>(
    card: &'a BoardCardRecord,
    role: &str,
) -> Vec<&'a BoardAssignmentRecord> {
    card.assignments
        .iter()
        .filter(|assignment| assignment.role.eq_ignore_ascii_case(role))
        .collect()
}

fn assignment_labels(card: &BoardCardRecord, agent: &str) -> Vec<String> {
    let mut labels = vec![format!("agent:{agent}")];
    labels.extend(card.task.labels.clone());
    labels
}

fn build_memory_tags(card: &BoardCardRecord) -> Vec<String> {
    let mut tags = vec!["build".to_string(), "review".to_string()];
    for tag in &card.task.labels {
        if !tags.iter().any(|existing| existing == tag) {
            tags.push(redact(tag));
        }
    }
    tags
}

fn memory_reference(memory: Option<&conduit_core::adapter::MemoryCapability>) -> String {
    match memory {
        Some(memory) => format!(
            "Shared memory reference: scope={}, tags={}, tools={}. Use memory tools only if extra context is needed.",
            memory.scope,
            memory.tags.join(", "),
            memory.tools.join(", ")
        ),
        None => "Shared memory reference: unavailable.".into(),
    }
}

fn build_prompt(
    card: &BoardCardRecord,
    assignment: &BoardAssignmentRecord,
    context: Option<&str>,
    memory: Option<&conduit_core::adapter::MemoryCapability>,
) -> String {
    let context = context
        .filter(|context| !context.trim().is_empty())
        .map(|context| format!("Prior context:\n{}", redact(context)))
        .unwrap_or_else(|| "Prior context: approved spec and council memory by reference.".into());
    format!(
        "Build card: {}\n\
         Title: {}\n\
         Body: {}\n\
         Labels: {}\n\
         Your build role: {}\n\
         Model hint: {}\n\
         {}\n\
         {context}\n\n\
         Implement the approved scope conservatively. Respect security guardrails, avoid unrelated refactors, and summarize changed files, tests, risks, and handoff notes.",
        redact(&card.task.id),
        redact(&card.task.title),
        redact(&card.task.body),
        card.task
            .labels
            .iter()
            .map(|label| redact(label))
            .collect::<Vec<_>>()
            .join(", "),
        redact(&assignment.role),
        assignment
            .model
            .as_deref()
            .map(redact)
            .unwrap_or_else(|| "default".into()),
        memory_reference(memory)
    )
}

fn review_prompt(
    card: &BoardCardRecord,
    assignment: &BoardAssignmentRecord,
    implementation_context: &str,
    memory: Option<&conduit_core::adapter::MemoryCapability>,
) -> String {
    format!(
        "Review card: {}\n\
         Title: {}\n\
         Body: {}\n\
         Labels: {}\n\
         Your review role: {}\n\
         Model hint: {}\n\
         {}\n\
         Implementation context:\n{}\n\n\
         Review for correctness, missing requirements, security regressions, tests, and deployment risk. Return concise findings first, then approval or required fixes.",
        redact(&card.task.id),
        redact(&card.task.title),
        redact(&card.task.body),
        card.task
            .labels
            .iter()
            .map(|label| redact(label))
            .collect::<Vec<_>>()
            .join(", "),
        redact(&assignment.role),
        assignment
            .model
            .as_deref()
            .map(redact)
            .unwrap_or_else(|| "default".into()),
        memory_reference(memory),
        redact(implementation_context)
    )
}

fn summarize_build_review(
    card: &BoardCardRecord,
    build_turns: &[BuildReviewTurn],
    review_turns: &[BuildReviewTurn],
) -> String {
    let mut summary = format!(
        "Build and review handoff for {}: {}\n\nBuild turns:\n",
        card.task.id, card.task.title
    );
    for turn in build_turns {
        summary.push_str(&format!(
            "- {} as {} ({:?}): {}\n",
            turn.agent, turn.role, turn.status, turn.output
        ));
    }
    summary.push_str("\nReview turns:\n");
    if review_turns.is_empty() {
        summary.push_str("- skipped because build did not fully succeed\n");
    } else {
        for turn in review_turns {
            summary.push_str(&format!(
                "- {} as {} ({:?}): {}\n",
                turn.agent, turn.role, turn.status, turn.output
            ));
        }
    }
    summary.push_str("\nNext gate: human review before done.");
    summary
}
