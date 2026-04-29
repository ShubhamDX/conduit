use crate::client::StdioClient;
use async_trait::async_trait;
use conduit_core::adapter::{AgentAdapter, SessionHandle, StartRequest};
use conduit_core::error::AdapterError;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CodexConfig {
    pub program: String,
    pub program_args: Vec<String>,
    pub model: Option<String>,
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
        let wrapped = conduit_security::wrap::wrap_command_args(
            &req.workspace,
            &req.security_policy,
            &self.config.program,
            &self.config.program_args,
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
