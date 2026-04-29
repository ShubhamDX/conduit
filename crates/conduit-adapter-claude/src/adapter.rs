use async_trait::async_trait;
use conduit_adapter_codex::client::StdioClient;
use conduit_core::adapter::{AgentAdapter, SessionHandle, StartRequest};
use conduit_core::error::AdapterError;
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
        let wrapped = conduit_security::wrap::wrap_command_args(
            &req.workspace,
            &req.security_policy,
            &self.config.python,
            &self.config.bridge_args,
        );
        let (program, args) = wrapped
            .split_first()
            .ok_or_else(|| AdapterError::Config("empty wrapped argv".into()))?;
        let mut client = StdioClient::spawn(program, args).await?;
        let _ = client
            .request(
                "newSession",
                serde_json::json!({
                    "prompt": req.prompt,
                    "model": req.model.clone().or(self.config.model.clone()),
                    "workspace": req.workspace.display().to_string(),
                }),
            )
            .await?;

        Ok(SessionHandle {
            session_id: Uuid::new_v4().to_string(),
            events: client.take_events_rx(),
        })
    }

    async fn stop_session(&self, _session_id: &str) -> Result<(), AdapterError> {
        Ok(())
    }
}
