//! Shared memory abstractions for orchestrator-mediated agent context.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("backend: {0}")]
    Backend(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub key: String,
    pub value: String,
    pub tags: Vec<String>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryQuery {
    pub tags: Vec<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySnapshot {
    pub entries: Vec<MemoryEntry>,
}

#[async_trait]
pub trait MemoryStore: Send + Sync {
    async fn load(&self, query: MemoryQuery) -> Result<MemorySnapshot, MemoryError>;
    async fn upsert(&self, entry: MemoryEntry) -> Result<(), MemoryError>;
}

pub mod memory {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone, Default)]
    pub struct InMemoryStore {
        entries: Arc<Mutex<Vec<MemoryEntry>>>,
    }

    impl InMemoryStore {
        pub fn new() -> Self {
            Self::default()
        }

        pub async fn entries(&self) -> Vec<MemoryEntry> {
            self.entries.lock().await.clone()
        }
    }

    #[async_trait]
    impl MemoryStore for InMemoryStore {
        async fn load(&self, query: MemoryQuery) -> Result<MemorySnapshot, MemoryError> {
            let mut entries: Vec<MemoryEntry> = self
                .entries
                .lock()
                .await
                .iter()
                .filter(|entry| matches_query(entry, &query.tags))
                .take(query.limit)
                .cloned()
                .collect();
            entries.sort_by(|left, right| left.key.cmp(&right.key));
            Ok(MemorySnapshot { entries })
        }

        async fn upsert(&self, entry: MemoryEntry) -> Result<(), MemoryError> {
            let mut entries = self.entries.lock().await;
            if let Some(existing) = entries
                .iter_mut()
                .find(|existing| existing.key == entry.key && existing.source == entry.source)
            {
                *existing = entry;
            } else {
                entries.push(entry);
            }
            Ok(())
        }
    }

    fn matches_query(entry: &MemoryEntry, tags: &[String]) -> bool {
        entry.tags.is_empty()
            || tags.is_empty()
            || entry
                .tags
                .iter()
                .any(|entry_tag| tags.iter().any(|query_tag| query_tag == entry_tag))
    }
}

#[cfg(test)]
mod tests {
    use super::memory::InMemoryStore;
    use super::*;

    #[tokio::test]
    async fn in_memory_store_returns_matching_entries() {
        let store = InMemoryStore::new();
        store
            .upsert(MemoryEntry {
                key: "a".into(),
                value: "codex context".into(),
                tags: vec!["agent:codex".into()],
                source: "issue:A".into(),
            })
            .await
            .unwrap();
        store
            .upsert(MemoryEntry {
                key: "b".into(),
                value: "claude context".into(),
                tags: vec!["agent:claude-code".into()],
                source: "issue:B".into(),
            })
            .await
            .unwrap();

        let snapshot = store
            .load(MemoryQuery {
                tags: vec!["agent:codex".into()],
                limit: 10,
            })
            .await
            .unwrap();

        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].value, "codex context");
    }

    #[tokio::test]
    async fn upsert_replaces_same_key_and_source() {
        let store = InMemoryStore::new();
        let first = MemoryEntry {
            key: "issue-1".into(),
            value: "old".into(),
            tags: Vec::new(),
            source: "issue:issue-1".into(),
        };
        let second = MemoryEntry {
            value: "new".into(),
            ..first.clone()
        };

        store.upsert(first).await.unwrap();
        store.upsert(second).await.unwrap();

        let entries = store.entries().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value, "new");
    }
}
