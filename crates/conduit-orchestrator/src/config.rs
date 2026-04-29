use conduit_core::adapter::SecurityPolicy;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub struct Workflow {
    pub workspace: PathBuf,
    pub assignee: String,
    pub default_agent: String,
    pub security: SecurityPolicy,
    pub agents: Vec<AgentSpec>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum AgentSpec {
    Codex {
        name: String,
        program: String,
        #[serde(default)]
        program_args: Vec<String>,
        #[serde(default)]
        model: Option<String>,
    },
    Claude {
        name: String,
        python: String,
        #[serde(default)]
        bridge_args: Vec<String>,
        #[serde(default)]
        model: Option<String>,
    },
}

pub fn load_workflow(yaml: &str) -> Result<Workflow, serde_yaml::Error> {
    serde_yaml::from_str(yaml)
}

#[cfg(test)]
mod tests {
    use super::load_workflow;

    #[test]
    fn parses_multi_agent_workflow() {
        let yaml = r#"
workspace: "./repo"
assignee: "bot"
default_agent: "codex"
security:
  egress_allowlist: ["api.openai.com", "api.anthropic.com"]
  max_cpu_secs: 600
  redact_secrets: true
  workspace_writable: true
agents:
  - name: codex
    kind: codex
    program: "codex"
    program_args: ["app-server"]
    model: "gpt-5"
  - name: claude-code
    kind: claude
    python: "python3"
    bridge_args: ["-m", "conduit_bridge"]
    model: "claude-sonnet-4-6"
"#;
        let workflow = load_workflow(yaml).unwrap();
        assert_eq!(workflow.default_agent, "codex");
        assert_eq!(workflow.agents.len(), 2);
        assert_eq!(workflow.security.egress_allowlist.len(), 2);
    }
}
