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
    async fn get(&self, key: &str) -> Result<Option<MemoryEntry>, MemoryError>;
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

        async fn get(&self, key: &str) -> Result<Option<MemoryEntry>, MemoryError> {
            Ok(self
                .entries
                .lock()
                .await
                .iter()
                .find(|entry| entry.key == key)
                .cloned())
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

pub mod sqlite {
    use super::*;
    use rusqlite::types::Value;
    use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone)]
    pub struct SqliteMemoryStore {
        connection: Arc<Mutex<Connection>>,
    }

    impl SqliteMemoryStore {
        pub fn open(path: impl AsRef<Path>) -> Result<Self, MemoryError> {
            let path = path.as_ref();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|error| MemoryError::Backend(error.to_string()))?;
            }
            let connection =
                Connection::open(path).map_err(|error| MemoryError::Backend(error.to_string()))?;
            initialize_schema(&connection)?;
            Ok(Self {
                connection: Arc::new(Mutex::new(connection)),
            })
        }

        pub fn open_in_memory() -> Result<Self, MemoryError> {
            let connection = Connection::open_in_memory()
                .map_err(|error| MemoryError::Backend(error.to_string()))?;
            initialize_schema(&connection)?;
            Ok(Self {
                connection: Arc::new(Mutex::new(connection)),
            })
        }
    }

    #[async_trait]
    impl MemoryStore for SqliteMemoryStore {
        async fn load(&self, query: MemoryQuery) -> Result<MemorySnapshot, MemoryError> {
            if query.limit == 0 {
                return Ok(MemorySnapshot::default());
            }

            let connection = self.connection.lock().await;
            let rows = if query.tags.is_empty() {
                select_recent_entries(&connection, query.limit)?
            } else {
                select_entries_by_tags(&connection, &query.tags, query.limit)?
            };
            let mut entries = Vec::with_capacity(rows.len());
            for (key, source, value) in rows {
                entries.push(MemoryEntry {
                    tags: select_tags(&connection, &key, &source)?,
                    key,
                    source,
                    value,
                });
            }

            Ok(MemorySnapshot { entries })
        }

        async fn get(&self, key: &str) -> Result<Option<MemoryEntry>, MemoryError> {
            let connection = self.connection.lock().await;
            let row = connection
                .query_row(
                    "SELECT key, source, value FROM memory_entries WHERE key = ?1 ORDER BY updated_at_ms DESC LIMIT 1",
                    params![key],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(|error| MemoryError::Backend(error.to_string()))?;

            match row {
                Some((key, source, value)) => Ok(Some(MemoryEntry {
                    tags: select_tags(&connection, &key, &source)?,
                    key,
                    source,
                    value,
                })),
                None => Ok(None),
            }
        }

        async fn upsert(&self, entry: MemoryEntry) -> Result<(), MemoryError> {
            let mut connection = self.connection.lock().await;
            let transaction = connection
                .transaction()
                .map_err(|error| MemoryError::Backend(error.to_string()))?;
            let now = unix_time_millis();

            transaction
                .execute(
                    r#"
                    INSERT INTO memory_entries (key, source, value, created_at_ms, updated_at_ms)
                    VALUES (?1, ?2, ?3, ?4, ?4)
                    ON CONFLICT(key, source) DO UPDATE SET
                        value = excluded.value,
                        updated_at_ms = excluded.updated_at_ms
                    "#,
                    params![entry.key, entry.source, entry.value, now],
                )
                .map_err(|error| MemoryError::Backend(error.to_string()))?;
            transaction
                .execute(
                    "DELETE FROM memory_tags WHERE key = ?1 AND source = ?2",
                    params![entry.key, entry.source],
                )
                .map_err(|error| MemoryError::Backend(error.to_string()))?;

            for tag in &entry.tags {
                transaction
                    .execute(
                        "INSERT OR IGNORE INTO memory_tags (key, source, tag) VALUES (?1, ?2, ?3)",
                        params![entry.key, entry.source, tag],
                    )
                    .map_err(|error| MemoryError::Backend(error.to_string()))?;
            }

            transaction
                .commit()
                .map_err(|error| MemoryError::Backend(error.to_string()))?;
            Ok(())
        }
    }

    fn initialize_schema(connection: &Connection) -> Result<(), MemoryError> {
        connection
            .execute_batch(
                r#"
                PRAGMA foreign_keys = ON;
                CREATE TABLE IF NOT EXISTS memory_entries (
                    key TEXT NOT NULL,
                    source TEXT NOT NULL,
                    value TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (key, source)
                );
                CREATE TABLE IF NOT EXISTS memory_tags (
                    key TEXT NOT NULL,
                    source TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    PRIMARY KEY (key, source, tag),
                    FOREIGN KEY (key, source)
                        REFERENCES memory_entries (key, source)
                        ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_memory_tags_tag ON memory_tags(tag);
                "#,
            )
            .map_err(|error| MemoryError::Backend(error.to_string()))
    }

    fn select_recent_entries(
        connection: &Connection,
        limit: usize,
    ) -> Result<Vec<(String, String, String)>, MemoryError> {
        let mut statement = connection
            .prepare(
                "SELECT key, source, value FROM memory_entries ORDER BY updated_at_ms DESC LIMIT ?1",
            )
            .map_err(|error| MemoryError::Backend(error.to_string()))?;
        collect_entry_rows(statement.query_map(params![limit as i64], entry_row))
    }

    fn select_entries_by_tags(
        connection: &Connection,
        tags: &[String],
        limit: usize,
    ) -> Result<Vec<(String, String, String)>, MemoryError> {
        let placeholders = std::iter::repeat("?")
            .take(tags.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
            SELECT DISTINCT e.key, e.source, e.value
            FROM memory_entries e
            JOIN memory_tags t ON t.key = e.key AND t.source = e.source
            WHERE t.tag IN ({placeholders})
            ORDER BY e.updated_at_ms DESC
            LIMIT ?
            "#
        );
        let mut params: Vec<Value> = tags.iter().cloned().map(Value::Text).collect();
        params.push(Value::Integer(limit as i64));
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| MemoryError::Backend(error.to_string()))?;
        collect_entry_rows(statement.query_map(params_from_iter(params), entry_row))
    }

    fn collect_entry_rows(
        rows: rusqlite::Result<
            rusqlite::MappedRows<
                '_,
                fn(&rusqlite::Row<'_>) -> rusqlite::Result<(String, String, String)>,
            >,
        >,
    ) -> Result<Vec<(String, String, String)>, MemoryError> {
        rows.map_err(|error| MemoryError::Backend(error.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|error| MemoryError::Backend(error.to_string()))
    }

    fn entry_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(String, String, String)> {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    }

    fn select_tags(
        connection: &Connection,
        key: &str,
        source: &str,
    ) -> Result<Vec<String>, MemoryError> {
        let mut statement = connection
            .prepare("SELECT tag FROM memory_tags WHERE key = ?1 AND source = ?2 ORDER BY tag")
            .map_err(|error| MemoryError::Backend(error.to_string()))?;
        let tags = statement
            .query_map(params![key, source], |row| row.get::<_, String>(0))
            .map_err(|error| MemoryError::Backend(error.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|error| MemoryError::Backend(error.to_string()))?;
        Ok(tags)
    }

    fn unix_time_millis() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
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

    #[tokio::test]
    async fn sqlite_store_persists_entries_across_reopen() {
        let path = unique_db_path("persist");
        let store = sqlite::SqliteMemoryStore::open(&path).unwrap();
        store
            .upsert(MemoryEntry {
                key: "issue-1".into(),
                value: "durable context".into(),
                tags: vec!["agent:codex".into()],
                source: "issue:issue-1".into(),
            })
            .await
            .unwrap();
        drop(store);

        let reopened = sqlite::SqliteMemoryStore::open(&path).unwrap();
        let entry = reopened.get("issue-1").await.unwrap().unwrap();
        assert_eq!(entry.value, "durable context");
        assert_eq!(entry.tags, vec!["agent:codex".to_string()]);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn sqlite_store_filters_by_tags() {
        let store = sqlite::SqliteMemoryStore::open_in_memory().unwrap();
        store
            .upsert(MemoryEntry {
                key: "codex".into(),
                value: "codex context".into(),
                tags: vec!["agent:codex".into()],
                source: "issue:codex".into(),
            })
            .await
            .unwrap();
        store
            .upsert(MemoryEntry {
                key: "claude".into(),
                value: "claude context".into(),
                tags: vec!["agent:claude-code".into()],
                source: "issue:claude".into(),
            })
            .await
            .unwrap();

        let snapshot = store
            .load(MemoryQuery {
                tags: vec!["agent:claude-code".into()],
                limit: 10,
            })
            .await
            .unwrap();

        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].key, "claude");
    }

    fn unique_db_path(label: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "conduit-memory-{label}-{}-{nanos}.db",
            std::process::id()
        ))
    }
}
