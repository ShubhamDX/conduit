use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use conduit_adapter_claude::adapter::{ClaudeCodeAdapter, ClaudeConfig};
use conduit_adapter_codex::adapter::{CodexAdapter, CodexConfig, MemoryMcpConfig};
use conduit_adapter_registry::AdapterRegistry;
use conduit_core::adapter::{AgentAdapter, SessionHandle, StartRequest};
use conduit_core::error::AdapterError;
use conduit_memory::sqlite::SqliteMemoryStore;
use conduit_memory::MemoryStore;
use conduit_orchestrator::config::{load_workflow, AgentSpec, Workflow};
use conduit_orchestrator::state::{
    ApprovalDecision, ApprovalRecord, RunSnapshot, SqliteOrchestrationStore, TaskRecord,
    TaskSnapshot,
};
use conduit_orchestrator::trace_export::{export_halo_spans, HaloExportOptions};
use conduit_orchestrator::{run_one_issue, OrchestratorConfig};
use conduit_security::redact::{redact, redact_json};
use serde::Serialize;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod memory_mcp;

#[derive(Parser)]
#[command(name = "conduit")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Validate {
        #[arg(long)]
        workflow: String,
    },
    Run {
        #[command(subcommand)]
        command: Option<RunCommand>,
        #[arg(long)]
        workflow: Option<String>,
        #[arg(long)]
        issue: Option<String>,
        #[arg(long)]
        tracker: Option<String>,
    },
    Doctor,
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    Approval {
        #[command(subcommand)]
        command: ApprovalCommand,
    },
    Trace {
        #[command(subcommand)]
        command: TraceCommand,
    },
    #[command(hide = true)]
    MemoryMcp {
        #[arg(long)]
        socket: PathBuf,
    },
}

#[derive(Subcommand)]
enum TaskCommand {
    List {
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long)]
        workflow: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Show {
        id: String,
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long)]
        workflow: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum RunCommand {
    Show {
        id: String,
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long)]
        workflow: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ApprovalCommand {
    List {
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long)]
        workflow: Option<String>,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Approve {
        id: String,
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long)]
        workflow: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Deny {
        id: String,
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long)]
        workflow: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum TraceCommand {
    Export {
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long)]
        workflow: Option<String>,
        #[arg(long)]
        task: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long, default_value = "conduit")]
        project_id: String,
        #[arg(long, default_value = "conduit")]
        service_name: String,
        #[arg(long)]
        service_version: Option<String>,
        #[arg(long)]
        deployment_environment: Option<String>,
    },
}

struct Renamed {
    inner: Box<dyn AgentAdapter>,
    name: String,
}

#[async_trait::async_trait]
impl AgentAdapter for Renamed {
    fn name(&self) -> &str {
        &self.name
    }

    async fn start_session(&self, request: StartRequest) -> Result<SessionHandle, AdapterError> {
        self.inner.start_session(request).await
    }

    async fn stop_session(&self, session_id: &str) -> Result<(), AdapterError> {
        self.inner.stop_session(session_id).await
    }
}

fn rename<A: AgentAdapter + 'static>(adapter: A, name: &str) -> Renamed {
    Renamed {
        inner: Box::new(adapter),
        name: name.to_string(),
    }
}

fn build_registry(workflow: &Workflow) -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();

    for agent in &workflow.agents {
        match agent {
            AgentSpec::Codex {
                name,
                program,
                program_args,
                model,
            } => {
                let adapter = CodexAdapter::new(CodexConfig {
                    program: program.clone(),
                    program_args: program_args.clone(),
                    model: model.clone(),
                    memory_mcp: default_memory_mcp_config(),
                });
                registry.insert(Box::new(rename(adapter, name)));
            }
            AgentSpec::Claude {
                name,
                python,
                bridge_args,
                model,
            } => {
                let adapter = ClaudeCodeAdapter::new(ClaudeConfig {
                    python: python.clone(),
                    bridge_args: bridge_args.clone(),
                    model: model.clone(),
                });
                registry.insert(Box::new(rename(adapter, name)));
            }
        }
    }

    registry.set_default(&workflow.default_agent);
    registry
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().try_init().ok();

    let cli = Cli::parse();
    match cli.command {
        Command::Validate { workflow } => {
            let yaml = std::fs::read_to_string(&workflow).context("read workflow")?;
            let workflow = load_workflow(&yaml).context("parse workflow")?;
            println!(
                "ok: workflow parses, {} agents configured",
                workflow.agents.len()
            );
            Ok(())
        }
        Command::Run {
            command: Some(command),
            ..
        } => handle_run_command(command).await,
        Command::Run {
            command: None,
            workflow,
            issue,
            tracker,
        } => {
            let workflow = workflow.context("--workflow required for run execution")?;
            let yaml = std::fs::read_to_string(&workflow).context("read workflow")?;
            let workflow_path = workflow;
            let workflow = load_workflow(&yaml).context("parse workflow")?;
            let registry = build_registry(&workflow);
            let shared_memory = build_memory_store(&workflow, &workflow_path)?;
            let orchestration_store = build_orchestration_store(&workflow_path)?;
            let config = OrchestratorConfig {
                workspace: workflow.workspace.clone(),
                assignee: workflow.assignee.clone(),
                default_policy: workflow.security.clone(),
                shared_memory,
                orchestration_store: Some(orchestration_store),
            };
            let issue_id = issue.context("--issue required in v0.1")?;
            let tracker_kind = tracker
                .context("no tracker configured; pass --tracker fake only for smoke tests")?;
            if tracker_kind != "fake" {
                anyhow::bail!("unsupported tracker kind: {tracker_kind}");
            }
            let tracker = conduit_tracker::fake::FakeTracker::with(Vec::new());
            run_one_issue(&tracker, &registry, &config, &issue_id).await?;
            Ok(())
        }
        Command::Doctor => {
            check_dep("codex");
            check_dep("python3");
            #[cfg(target_os = "macos")]
            check_dep("sandbox-exec");
            #[cfg(target_os = "linux")]
            {
                check_dep("bwrap");
                let check = conduit_security::sandbox_linux::probe_user_namespace();
                if check.ok {
                    println!("{}", check.message);
                } else {
                    println!("MISSING: {}", check.message);
                }
            }
            Ok(())
        }
        Command::Task { command } => handle_task_command(command).await,
        Command::Approval { command } => handle_approval_command(command).await,
        Command::Trace { command } => match command {
            TraceCommand::Export {
                state,
                workflow,
                task,
                out,
                project_id,
                service_name,
                service_version,
                deployment_environment,
            } => {
                export_trace_command(
                    state,
                    workflow.as_deref(),
                    task.as_deref(),
                    out,
                    HaloExportOptions {
                        project_id,
                        service_name,
                        service_version,
                        deployment_environment,
                    },
                )
                .await
            }
        },
        Command::MemoryMcp { socket } => memory_mcp::run(&socket).await,
    }
}

async fn handle_task_command(command: TaskCommand) -> Result<()> {
    match command {
        TaskCommand::List {
            state,
            workflow,
            json,
        } => {
            let store = open_existing_orchestration_store(state, workflow.as_deref())?;
            let tasks = store.tasks().await.context("read tasks")?;
            if json {
                write_json(&tasks)
            } else {
                print_task_list(&tasks);
                Ok(())
            }
        }
        TaskCommand::Show {
            id,
            state,
            workflow,
            json,
        } => {
            let store = open_existing_orchestration_store(state, workflow.as_deref())?;
            let snapshot = store
                .task_snapshot(&id)
                .await
                .context("read task snapshot")?
                .with_context(|| format!("task not found in orchestration store: {id}"))?;
            if json {
                write_json(&snapshot)
            } else {
                print_task_snapshot(&snapshot);
                Ok(())
            }
        }
    }
}

async fn handle_run_command(command: RunCommand) -> Result<()> {
    match command {
        RunCommand::Show {
            id,
            state,
            workflow,
            json,
        } => {
            let store = open_existing_orchestration_store(state, workflow.as_deref())?;
            let snapshot = store
                .run_snapshot(&id)
                .await
                .context("read run snapshot")?
                .with_context(|| format!("run not found in orchestration store: {id}"))?;
            if json {
                write_json(&snapshot)
            } else {
                print_run_snapshot(&snapshot);
                Ok(())
            }
        }
    }
}

async fn handle_approval_command(command: ApprovalCommand) -> Result<()> {
    match command {
        ApprovalCommand::List {
            state,
            workflow,
            status,
            json,
        } => {
            let store = open_existing_orchestration_store(state, workflow.as_deref())?;
            let approvals = store
                .approvals(status.as_deref())
                .await
                .context("read approvals")?;
            if json {
                write_json(&approvals)
            } else {
                print_approval_list(&approvals);
                Ok(())
            }
        }
        ApprovalCommand::Approve {
            id,
            state,
            workflow,
            json,
        } => {
            resolve_approval_command(
                id,
                state,
                workflow.as_deref(),
                ApprovalDecision::Approved,
                json,
            )
            .await
        }
        ApprovalCommand::Deny {
            id,
            state,
            workflow,
            json,
        } => {
            resolve_approval_command(
                id,
                state,
                workflow.as_deref(),
                ApprovalDecision::Denied,
                json,
            )
            .await
        }
    }
}

async fn resolve_approval_command(
    id: String,
    state: Option<PathBuf>,
    workflow: Option<&str>,
    decision: ApprovalDecision,
    json: bool,
) -> Result<()> {
    let store = open_existing_orchestration_store(state, workflow)?;
    let approval = store
        .resolve_approval(&id, decision)
        .await
        .context("resolve approval")?;
    if json {
        write_json(&approval)
    } else {
        print_approval(&approval);
        Ok(())
    }
}

async fn export_trace_command(
    state: Option<PathBuf>,
    workflow: Option<&str>,
    task: Option<&str>,
    out: Option<PathBuf>,
    options: HaloExportOptions,
) -> Result<()> {
    let state_path = resolve_orchestration_state_path(state, workflow);
    if !state_path.exists() {
        anyhow::bail!("orchestration store not found: {}", state_path.display());
    }
    let store = SqliteOrchestrationStore::open(&state_path).with_context(|| {
        format!(
            "open sqlite orchestration store at {}",
            state_path.display()
        )
    })?;
    let snapshots = match task {
        Some(task_id) => {
            let snapshot = store
                .task_snapshot(task_id)
                .await
                .context("read task snapshot")?
                .with_context(|| format!("task not found in orchestration store: {task_id}"))?;
            vec![snapshot]
        }
        None => store
            .task_snapshots()
            .await
            .context("read task snapshots")?,
    };
    let spans = export_halo_spans(&snapshots, &options);

    let mut writer: Box<dyn Write> = match out {
        Some(path) => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("create trace export directory {}", parent.display())
                })?;
            }
            Box::new(
                std::fs::File::create(&path)
                    .with_context(|| format!("create trace export {}", path.display()))?,
            )
        }
        None => Box::new(std::io::stdout()),
    };

    for span in spans {
        serde_json::to_writer(&mut writer, &span).context("encode halo trace span")?;
        writer.write_all(b"\n").context("write halo trace span")?;
    }
    writer.flush().context("flush halo trace export")?;
    Ok(())
}

fn open_existing_orchestration_store(
    state: Option<PathBuf>,
    workflow: Option<&str>,
) -> Result<SqliteOrchestrationStore> {
    let state_path = resolve_orchestration_state_path(state, workflow);
    if !state_path.exists() {
        anyhow::bail!("orchestration store not found: {}", state_path.display());
    }
    SqliteOrchestrationStore::open(&state_path).with_context(|| {
        format!(
            "open sqlite orchestration store at {}",
            state_path.display()
        )
    })
}

fn write_json<T: Serialize>(value: &T) -> Result<()> {
    let mut stdout = std::io::stdout();
    let value = serde_json::to_value(value).context("encode json output")?;
    let value = redact_json(value);
    serde_json::to_writer_pretty(&mut stdout, &value).context("write json output")?;
    stdout.write_all(b"\n").context("write json output")?;
    stdout.flush().context("flush json output")
}

fn print_task_list(tasks: &[TaskRecord]) {
    for task in tasks {
        println!(
            "{}\t{:?}\t{}\t{}",
            redact(&task.id),
            task.status,
            redact(&task.source),
            redact(&task.title)
        );
    }
}

fn print_task_snapshot(snapshot: &TaskSnapshot) {
    println!(
        "{}\t{:?}\t{}\t{}",
        redact(&snapshot.task.id),
        snapshot.task.status,
        redact(&snapshot.task.source),
        redact(&snapshot.task.title)
    );
    println!(
        "runs: {}\tevents: {}\tapprovals: {}\tmessages: {}",
        snapshot.runs.len(),
        snapshot.events.len(),
        snapshot.approvals.len(),
        snapshot.messages.len()
    );
}

fn print_run_snapshot(snapshot: &RunSnapshot) {
    println!(
        "{}\t{}\t{:?}\ttask:{}",
        redact(&snapshot.run.id),
        redact(&snapshot.run.agent),
        snapshot.run.status,
        redact(&snapshot.task.id)
    );
    println!(
        "events: {}\tapprovals: {}\tmessages: {}",
        snapshot.events.len(),
        snapshot.approvals.len(),
        snapshot.messages.len()
    );
}

fn print_approval_list(approvals: &[ApprovalRecord]) {
    for approval in approvals {
        print_approval(approval);
    }
}

fn print_approval(approval: &ApprovalRecord) {
    println!(
        "{}\t{}\t{:?}\trun:{}\t{}",
        redact(&approval.id),
        redact(&approval.status),
        approval.risk,
        redact(&approval.run_id),
        redact(&approval.reason)
    );
}

fn build_orchestration_store(workflow_path: &str) -> Result<Arc<SqliteOrchestrationStore>> {
    let path = resolve_relative_to_workflow(workflow_path, Path::new(".conduit/orchestration.db"));
    Ok(Arc::new(
        SqliteOrchestrationStore::open(path).context("open sqlite orchestration store")?,
    ))
}

fn default_memory_mcp_config() -> Option<MemoryMcpConfig> {
    let program = std::env::current_exe().ok()?;
    Some(MemoryMcpConfig {
        program: program.to_string_lossy().to_string(),
        args: vec!["memory-mcp".into(), "--socket".into()],
    })
}

fn build_memory_store(
    workflow: &Workflow,
    workflow_path: &str,
) -> Result<Option<Arc<dyn MemoryStore>>> {
    let Some(memory) = &workflow.memory else {
        return Ok(None);
    };

    match memory.kind.as_str() {
        "sqlite" => {
            let path = resolve_relative_to_workflow(workflow_path, &memory.path);
            Ok(Some(Arc::new(
                SqliteMemoryStore::open(path).context("open sqlite memory store")?,
            )))
        }
        other => anyhow::bail!("unsupported memory kind: {other}"),
    }
}

fn resolve_relative_to_workflow(workflow_path: &str, path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }

    Path::new(workflow_path)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(path)
}

fn resolve_orchestration_state_path(state: Option<PathBuf>, workflow: Option<&str>) -> PathBuf {
    match (state, workflow) {
        (Some(path), _) => path,
        (None, Some(workflow_path)) => {
            resolve_relative_to_workflow(workflow_path, Path::new(".conduit/orchestration.db"))
        }
        (None, None) => PathBuf::from(".conduit/orchestration.db"),
    }
}

fn check_dep(binary: &str) {
    match std::process::Command::new("which").arg(binary).output() {
        Ok(output) if output.status.success() => {
            println!(
                "ok: {binary} at {}",
                String::from_utf8_lossy(&output.stdout).trim()
            );
        }
        _ => println!("MISSING: {binary} not found on PATH"),
    }
}
