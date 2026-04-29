use async_trait::async_trait;
use conduit_adapter_codex::client::{StdioClient, StdioClientOptions};
use conduit_core::adapter::{AgentAdapter, SessionHandle, StartRequest};
use conduit_core::error::AdapterError;
use conduit_core::event::AgentEvent;
use conduit_security::egress::ProxyHandle;
use conduit_security::wrap::WrappedCommand;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ClaudeConfig {
    pub python: String,
    pub bridge_args: Vec<String>,
    pub model: Option<String>,
}

pub struct ClaudeCodeAdapter {
    config: ClaudeConfig,
}

impl ClaudeCodeAdapter {
    pub fn new(config: ClaudeConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl AgentAdapter for ClaudeCodeAdapter {
    fn name(&self) -> &str {
        "claude-code"
    }

    async fn start_session(&self, req: StartRequest) -> Result<SessionHandle, AdapterError> {
        let memory = req.memory.clone();
        let memory_tools = req.memory_tools.clone();
        let policy = req.security_policy.clone();
        let (proxy_env, egress_proxy) =
            conduit_security::egress::start_proxy_for_policy(&policy).await?;
        let mut env = req.env.clone();
        env.extend(proxy_env);
        let wrapped = conduit_security::wrap::wrap_command(
            &req.workspace,
            &policy,
            &self.config.python,
            &self.config.bridge_args,
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
                    _egress_proxy: egress_proxy,
                },
            ),
        })
    }

    async fn stop_session(&self, _session_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }
}

struct SessionGuards {
    _wrapped: WrappedCommand,
    _egress_proxy: Option<ProxyHandle>,
}

fn hold_session_guards(
    mut events: mpsc::Receiver<AgentEvent>,
    guards: SessionGuards,
) -> mpsc::Receiver<AgentEvent> {
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
