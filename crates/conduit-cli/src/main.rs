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
use conduit_orchestrator::{run_one_issue, OrchestratorConfig};
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
        #[arg(long)]
        workflow: String,
        #[arg(long)]
        issue: Option<String>,
    },
    Doctor,
    #[command(hide = true)]
    MemoryMcp {
        #[arg(long)]
        socket: PathBuf,
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
        Command::Run { workflow, issue } => {
            let yaml = std::fs::read_to_string(&workflow).context("read workflow")?;
            let workflow_path = workflow;
            let workflow = load_workflow(&yaml).context("parse workflow")?;
            let registry = build_registry(&workflow);
            let shared_memory = build_memory_store(&workflow, &workflow_path)?;
            let config = OrchestratorConfig {
                workspace: workflow.workspace.clone(),
                assignee: workflow.assignee.clone(),
                default_policy: workflow.security.clone(),
                shared_memory,
            };
            let issue_id = issue.context("--issue required in v0.1")?;
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
            check_dep("bwrap");
            Ok(())
        }
        Command::MemoryMcp { socket } => memory_mcp::run(&socket).await,
    }
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
