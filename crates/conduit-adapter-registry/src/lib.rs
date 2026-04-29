//! Adapter registry and routing helpers.

use conduit_core::adapter::AgentAdapter;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

const LABEL_PREFIX: &str = "agent:";

#[derive(Debug, Error)]
pub enum RouteError {
    #[error("no default adapter configured")]
    NoDefault,
    #[error("label references unknown adapter: {0}")]
    UnknownAdapter(String),
}

pub struct AdapterRegistry {
    adapters: HashMap<String, Arc<dyn AgentAdapter>>,
    default_name: Option<String>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self {
            adapters: HashMap::new(),
            default_name: None,
        }
    }

    pub fn insert(&mut self, adapter: Box<dyn AgentAdapter>) {
        let name = adapter.name().to_string();
        self.adapters.insert(name, Arc::from(adapter));
    }

    pub fn set_default(&mut self, name: &str) {
        self.default_name = Some(name.to_string());
    }

    pub fn route(&self, labels: &[String]) -> Result<Arc<dyn AgentAdapter>, RouteError> {
        for label in labels {
            if let Some(name) = label.strip_prefix(LABEL_PREFIX) {
                return self
                    .adapters
                    .get(name)
                    .cloned()
                    .ok_or_else(|| RouteError::UnknownAdapter(name.to_string()));
            }
        }

        let default_name = self.default_name.as_ref().ok_or(RouteError::NoDefault)?;
        self.adapters
            .get(default_name)
            .cloned()
            .ok_or_else(|| RouteError::UnknownAdapter(default_name.clone()))
    }
}

impl Default for AdapterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use conduit_core::adapter::{AgentAdapter, SessionHandle, StartRequest};
    use conduit_core::error::AdapterError;

    struct Stub(&'static str);

    #[async_trait]
    impl AgentAdapter for Stub {
        fn name(&self) -> &str {
            self.0
        }

        async fn start_session(
            &self,
            _request: StartRequest,
        ) -> Result<SessionHandle, AdapterError> {
            unimplemented!()
        }

        async fn stop_session(&self, _session_id: &str) -> Result<(), AdapterError> {
            Ok(())
        }
    }

    #[test]
    fn route_by_label_prefers_specific() {
        let mut registry = AdapterRegistry::new();
        registry.insert(Box::new(Stub("codex")));
        registry.insert(Box::new(Stub("claude-code")));
        registry.set_default("codex");
        let labels = vec!["agent:claude-code".to_string(), "kind:bug".to_string()];

        let picked = registry.route(&labels).unwrap();
        assert_eq!(picked.name(), "claude-code");
    }

    #[test]
    fn route_falls_back_to_default_when_no_label() {
        let mut registry = AdapterRegistry::new();
        registry.insert(Box::new(Stub("codex")));
        registry.set_default("codex");
        let labels: Vec<String> = Vec::new();

        assert_eq!(registry.route(&labels).unwrap().name(), "codex");
    }

    #[test]
    fn route_err_when_label_unknown() {
        let mut registry = AdapterRegistry::new();
        registry.insert(Box::new(Stub("codex")));
        registry.set_default("codex");
        let labels = vec!["agent:gemini".to_string()];

        assert!(registry.route(&labels).is_err());
    }
}
