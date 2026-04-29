use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdapterError {
    #[error("agent session timed out")]
    Timeout,
    #[error("agent protocol error: {0}")]
    Protocol(String),
    #[error("sandbox refused to start: {0}")]
    Sandbox(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("agent exited abnormally: code={0:?}")]
    AgentExit(Option<i32>),
    #[error("bad config: {0}")]
    Config(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_variant() {
        let e = AdapterError::Timeout;
        assert_eq!(e.to_string(), "agent session timed out");
    }
}
