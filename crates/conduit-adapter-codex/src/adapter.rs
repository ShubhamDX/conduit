use crate::client::{StdioClient, StdioClientOptions};
use crate::memory_mcp::{start_memory_mcp_proxy, MemoryMcpProxy};
use async_trait::async_trait;
use conduit_core::adapter::{AgentAdapter, SessionHandle, StartRequest};
use conduit_core::error::AdapterError;
use conduit_core::event::AgentEvent;
use conduit_security::egress::ProxyHandle;
use conduit_security::wrap::WrappedCommand;
use std::path::Path;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CodexConfig {
    pub program: String,
    pub program_args: Vec<String>,
    pub model: Option<String>,
    pub memory_mcp: Option<MemoryMcpConfig>,
}

#[derive(Debug, Clone)]
pub struct MemoryMcpConfig {
    pub program: String,
    pub args: Vec<String>,
}

pub struct CodexAdapter {
    config: CodexConfig,
}

impl CodexAdapter {
    pub fn new(config: CodexConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl AgentAdapter for CodexAdapter {
    fn name(&self) -> &str {
        "codex"
    }

    async fn start_session(&self, req: StartRequest) -> Result<SessionHandle, AdapterError> {
        let memory = req.memory.clone();
        let memory_tools = req.memory_tools.clone();
        let policy = req.security_policy.clone();
        let (proxy_env, egress_proxy) =
            conduit_security::egress::start_proxy_for_policy(&policy).await?;
        let mut env = req.env.clone();
        env.extend(proxy_env);
        let mut program_args = self.config.program_args.clone();
        let memory_proxy = if memory.is_some() {
            match (memory_tools.clone(), &self.config.memory_mcp) {
                (Some(memory_tools), Some(memory_mcp)) => {
                    let proxy = start_memory_mcp_proxy(&req.workspace, memory_tools)?;
                    append_memory_mcp_config(&mut program_args, memory_mcp, proxy.socket_path());
                    Some(proxy)
                }
                _ => None,
            }
        } else {
            None
        };
        let wrapped = conduit_security::wrap::wrap_command(
            &req.workspace,
            &policy,
            &self.config.program,
            &program_args,
        )?;
        let (program, args) = wrapped
            .program_and_args()
            .ok_or_else(|| AdapterError::Config("empty wrapped argv".into()))?;
        let mut client = StdioClient::spawn_with_options(
            program,
            args,
            StdioClientOptions {
                memory_tools,
                env,
                rlimits: conduit_security::rlimits::limits_to_closure(&policy),
            },
        )
        .await?;
        let _ = client
            .request(
                "newSession",
                serde_json::json!({
                    "prompt": req.prompt,
                    "model": req.model.clone().or(self.config.model.clone()),
                    "workspace": req.workspace.display().to_string(),
                    "memory": memory,
                }),
            )
            .await?;

        Ok(SessionHandle {
            session_id: Uuid::new_v4().to_string(),
            events: hold_session_guards(
                client.take_events_rx(),
                SessionGuards {
                    _wrapped: wrapped,
                    _memory_proxy: memory_proxy,
                    _egress_proxy: egress_proxy,
                },
            ),
        })
    }

    async fn stop_session(&self, _session_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }
}

fn append_memory_mcp_config(
    args: &mut Vec<String>,
    memory_mcp: &MemoryMcpConfig,
    socket_path: &Path,
) {
    let mut server_args = memory_mcp.args.clone();
    server_args.push(socket_path.display().to_string());

    args.push("-c".into());
    args.push(format!(
        "mcp_servers.conduit_memory.command={}",
        toml_string(&memory_mcp.program)
    ));
    args.push("-c".into());
    args.push(format!(
        "mcp_servers.conduit_memory.args={}",
        toml_array(&server_args)
    ));
}

struct SessionGuards {
    _wrapped: WrappedCommand,
    _memory_proxy: Option<MemoryMcpProxy>,
    _egress_proxy: Option<ProxyHandle>,
}

impl SessionGuards {
    fn is_empty(&self) -> bool {
        !self._wrapped.needs_cleanup()
            && self._memory_proxy.is_none()
            && self._egress_proxy.is_none()
    }
}

fn hold_session_guards(
    mut events: mpsc::Receiver<AgentEvent>,
    guards: SessionGuards,
) -> mpsc::Receiver<AgentEvent> {
    if guards.is_empty() {
        return events;
    }

    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        let _guards = guards;
        while let Some(event) = events.recv().await {
            if tx.send(event).await.is_err() {
                break;
            }
        }
    });
    rx
}

fn toml_array(values: &[String]) -> String {
    let values = values
        .iter()
        .map(|value| toml_string(value))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

fn toml_string(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("\"{escaped}\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_mcp_config_is_encoded_as_codex_overrides() {
        let mut args = vec!["app-server".to_string()];
        append_memory_mcp_config(
            &mut args,
            &MemoryMcpConfig {
                program: "/bin/conduit".into(),
                args: vec!["memory-mcp".into(), "--socket".into()],
            },
            Path::new("/tmp/memory.sock"),
        );

        assert!(args.contains(&"-c".to_string()));
        assert!(args
            .iter()
            .any(|arg| arg == "mcp_servers.conduit_memory.command=\"/bin/conduit\""));
        assert!(args.iter().any(|arg| {
            arg == "mcp_servers.conduit_memory.args=[\"memory-mcp\", \"--socket\", \"/tmp/memory.sock\"]"
        }));
    }
}
