//! Tracker abstractions for issue control planes.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TrackerError {
    #[error("backend: {0}")]
    Backend(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Issue {
    pub id: String,
    pub title: String,
    pub body: String,
    pub labels: Vec<String>,
    pub assignee: Option<String>,
    pub state: String,
}

#[async_trait]
pub trait Tracker: Send + Sync {
    async fn fetch_assigned(&self, assignee: &str) -> Result<Vec<Issue>, TrackerError>;
    async fn post_comment(&self, issue_id: &str, body: &str) -> Result<(), TrackerError>;
    async fn set_state(&self, issue_id: &str, state: &str) -> Result<(), TrackerError>;
}

pub mod fake {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    pub struct FakeTracker {
        issues: Arc<Mutex<Vec<Issue>>>,
        comments: Arc<Mutex<Vec<(String, String)>>>,
        state_updates: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl FakeTracker {
        pub fn with(issues: Vec<Issue>) -> Self {
            Self {
                issues: Arc::new(Mutex::new(issues)),
                comments: Arc::new(Mutex::new(Vec::new())),
                state_updates: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub async fn comments(&self) -> Vec<(String, String)> {
            self.comments.lock().await.clone()
        }

        pub async fn state_updates(&self) -> Vec<(String, String)> {
            self.state_updates.lock().await.clone()
        }
    }

    #[async_trait]
    impl Tracker for FakeTracker {
        async fn fetch_assigned(&self, assignee: &str) -> Result<Vec<Issue>, TrackerError> {
            Ok(self
                .issues
                .lock()
                .await
                .iter()
                .filter(|issue| issue.assignee.as_deref() == Some(assignee))
                .cloned()
                .collect())
        }

        async fn post_comment(&self, issue_id: &str, body: &str) -> Result<(), TrackerError> {
            self.comments
                .lock()
                .await
                .push((issue_id.to_string(), body.to_string()));
            Ok(())
        }

        async fn set_state(&self, issue_id: &str, state: &str) -> Result<(), TrackerError> {
            self.state_updates
                .lock()
                .await
                .push((issue_id.to_string(), state.to_string()));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_returns_assigned_issues() {
        let tracker = fake::FakeTracker::with(vec![Issue {
            id: "A".into(),
            title: "t".into(),
            body: "b".into(),
            labels: vec!["agent:codex".into()],
            assignee: Some("bot".into()),
            state: "todo".into(),
        }]);

        let got = tracker.fetch_assigned("bot").await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, "A");
    }

    #[tokio::test]
    async fn fake_records_comments() {
        let tracker = fake::FakeTracker::with(Vec::new());
        tracker.post_comment("A", "done").await.unwrap();

        assert_eq!(
            tracker.comments().await,
            vec![("A".to_string(), "done".to_string())]
        );
    }
}
