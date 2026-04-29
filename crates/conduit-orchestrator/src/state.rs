use conduit_core::event::{AgentEvent, Risk};
use conduit_security::redact::{redact, redact_event};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("backend: {0}")]
    Backend(String),
    #[error("task not found: {0}")]
    TaskNotFound(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Done,
    Failed,
    Cancelled,
}

impl TaskStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "running" => Self::Running,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => Self::Pending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => Self::Running,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageDirection {
    Inbound,
    Outbound,
}

impl MessageDirection {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "outbound" => Self::Outbound,
            _ => Self::Inbound,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewTask {
    pub id: String,
    pub source: String,
    pub title: String,
    pub body: String,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: String,
    pub source: String,
    pub title: String,
    pub body: String,
    pub labels: Vec<String>,
    pub status: TaskStatus,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: String,
    pub task_id: String,
    pub agent: String,
    pub status: RunStatus,
    pub started_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub id: i64,
    pub run_id: String,
    pub sequence: i64,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRecord {
    pub id: String,
    pub run_id: String,
    pub status: String,
    pub reason: String,
    pub risk: Risk,
    pub created_at_ms: i64,
    pub resolved_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewMessage {
    pub task_id: Option<String>,
    pub run_id: Option<String>,
    pub channel: String,
    pub sender: String,
    pub direction: MessageDirection,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRecord {
    pub id: i64,
    pub task_id: Option<String>,
    pub run_id: Option<String>,
    pub channel: String,
    pub sender: String,
    pub direction: MessageDirection,
    pub body: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskSnapshot {
    pub task: TaskRecord,
    pub runs: Vec<RunRecord>,
    pub events: Vec<EventRecord>,
    pub approvals: Vec<ApprovalRecord>,
    pub messages: Vec<MessageRecord>,
}

#[derive(Debug, Clone)]
pub struct SqliteOrchestrationStore {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteOrchestrationStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(to_backend)?;
        }
        let connection = Connection::open(path).map_err(to_backend)?;
        initialize_schema(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn open_in_memory() -> Result<Self, StateError> {
        let connection = Connection::open_in_memory().map_err(to_backend)?;
        initialize_schema(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub async fn create_task(&self, task: NewTask) -> Result<TaskRecord, StateError> {
        let connection = self.connection.lock().await;
        let now = unix_time_millis();
        let labels_json = serde_json::to_string(&task.labels).map_err(to_backend)?;
        connection
            .execute(
                r#"
                INSERT INTO orchestration_tasks (
                    id, source, title, body, labels_json, status, created_at_ms, updated_at_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
                ON CONFLICT(id) DO UPDATE SET
                    source = excluded.source,
                    title = excluded.title,
                    body = excluded.body,
                    labels_json = excluded.labels_json,
                    updated_at_ms = excluded.updated_at_ms
                "#,
                params![
                    task.id,
                    task.source,
                    task.title,
                    task.body,
                    labels_json,
                    TaskStatus::Pending.as_str(),
                    now
                ],
            )
            .map_err(to_backend)?;

        select_task(&connection, &task.id)?.ok_or_else(|| StateError::TaskNotFound(task.id.clone()))
    }

    pub async fn start_run(&self, task_id: &str, agent: &str) -> Result<RunRecord, StateError> {
        let mut connection = self.connection.lock().await;
        let transaction = connection.transaction().map_err(to_backend)?;
        ensure_task_exists(&transaction, task_id)?;
        let now = unix_time_millis();
        let run_id = format!("run-{}", Uuid::new_v4());
        transaction
            .execute(
                r#"
                INSERT INTO orchestration_runs (
                    id, task_id, agent, status, started_at_ms, completed_at_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5, NULL)
                "#,
                params![run_id, task_id, agent, RunStatus::Running.as_str(), now],
            )
            .map_err(to_backend)?;
        transaction
            .execute(
                "UPDATE orchestration_tasks SET status = ?1, updated_at_ms = ?2 WHERE id = ?3",
                params![TaskStatus::Running.as_str(), now, task_id],
            )
            .map_err(to_backend)?;
        transaction.commit().map_err(to_backend)?;

        select_run(&connection, &run_id)?.ok_or_else(|| StateError::Backend("run missing".into()))
    }

    pub async fn record_event(
        &self,
        run_id: &str,
        event: AgentEvent,
    ) -> Result<EventRecord, StateError> {
        let connection = self.connection.lock().await;
        ensure_run_exists(&connection, run_id)?;
        let event = redact_event(event);
        let payload = serde_json::to_value(&event).map_err(to_backend)?;
        let event_type = payload
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let payload_json = serde_json::to_string(&payload).map_err(to_backend)?;
        let sequence = next_event_sequence(&connection, run_id)?;
        let now = unix_time_millis();

        connection
            .execute(
                r#"
                INSERT INTO orchestration_events (
                    run_id, sequence, event_type, payload_json, created_at_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![run_id, sequence, event_type, payload_json, now],
            )
            .map_err(to_backend)?;
        let id = connection.last_insert_rowid();

        Ok(EventRecord {
            id,
            run_id: run_id.to_string(),
            sequence,
            event_type,
            payload,
            created_at_ms: now,
        })
    }

    pub async fn request_approval(
        &self,
        run_id: &str,
        reason: &str,
        risk: Risk,
    ) -> Result<ApprovalRecord, StateError> {
        let connection = self.connection.lock().await;
        ensure_run_exists(&connection, run_id)?;
        let now = unix_time_millis();
        let id = format!("approval-{}", Uuid::new_v4());
        let reason = redact(reason);
        let risk_json = serde_json::to_string(&risk).map_err(to_backend)?;
        connection
            .execute(
                r#"
                INSERT INTO orchestration_approvals (
                    id, run_id, status, reason, risk_json, created_at_ms, resolved_at_ms
                )
                VALUES (?1, ?2, 'pending', ?3, ?4, ?5, NULL)
                "#,
                params![id, run_id, reason, risk_json, now],
            )
            .map_err(to_backend)?;

        Ok(ApprovalRecord {
            id,
            run_id: run_id.to_string(),
            status: "pending".into(),
            reason,
            risk,
            created_at_ms: now,
            resolved_at_ms: None,
        })
    }

    pub async fn record_message(&self, message: NewMessage) -> Result<MessageRecord, StateError> {
        let connection = self.connection.lock().await;
        let now = unix_time_millis();
        let body = redact(&message.body);
        connection
            .execute(
                r#"
                INSERT INTO orchestration_messages (
                    task_id, run_id, channel, sender, direction, body, created_at_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    message.task_id,
                    message.run_id,
                    message.channel,
                    message.sender,
                    message.direction.as_str(),
                    body,
                    now
                ],
            )
            .map_err(to_backend)?;
        let id = connection.last_insert_rowid();

        Ok(MessageRecord {
            id,
            task_id: message.task_id,
            run_id: message.run_id,
            channel: message.channel,
            sender: message.sender,
            direction: message.direction,
            body,
            created_at_ms: now,
        })
    }

    pub async fn task_snapshot(&self, task_id: &str) -> Result<Option<TaskSnapshot>, StateError> {
        let connection = self.connection.lock().await;
        let Some(task) = select_task(&connection, task_id)? else {
            return Ok(None);
        };
        let runs = select_runs_for_task(&connection, task_id)?;
        let run_ids = runs.iter().map(|run| run.id.as_str()).collect::<Vec<_>>();
        let events = select_events_for_runs(&connection, &run_ids)?;
        let approvals = select_approvals_for_runs(&connection, &run_ids)?;
        let messages = select_messages_for_task(&connection, task_id)?;

        Ok(Some(TaskSnapshot {
            task,
            runs,
            events,
            approvals,
            messages,
        }))
    }
}

fn initialize_schema(connection: &Connection) -> Result<(), StateError> {
    connection
        .execute_batch(
            r#"
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS orchestration_tasks (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                labels_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS orchestration_runs (
                id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                agent TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at_ms INTEGER NOT NULL,
                completed_at_ms INTEGER,
                FOREIGN KEY (task_id)
                    REFERENCES orchestration_tasks(id)
                    ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_orchestration_runs_task
                ON orchestration_runs(task_id, started_at_ms);
            CREATE TABLE IF NOT EXISTS orchestration_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                UNIQUE(run_id, sequence),
                FOREIGN KEY (run_id)
                    REFERENCES orchestration_runs(id)
                    ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS orchestration_approvals (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                risk_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                resolved_at_ms INTEGER,
                FOREIGN KEY (run_id)
                    REFERENCES orchestration_runs(id)
                    ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS orchestration_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                run_id TEXT,
                channel TEXT NOT NULL,
                sender TEXT NOT NULL,
                direction TEXT NOT NULL,
                body TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                FOREIGN KEY (task_id)
                    REFERENCES orchestration_tasks(id)
                    ON DELETE CASCADE,
                FOREIGN KEY (run_id)
                    REFERENCES orchestration_runs(id)
                    ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_orchestration_messages_task
                ON orchestration_messages(task_id, created_at_ms);
            "#,
        )
        .map_err(to_backend)
}

fn select_task(connection: &Connection, task_id: &str) -> Result<Option<TaskRecord>, StateError> {
    connection
        .query_row(
            r#"
            SELECT id, source, title, body, labels_json, status, created_at_ms, updated_at_ms
            FROM orchestration_tasks
            WHERE id = ?1
            "#,
            params![task_id],
            task_row,
        )
        .optional()
        .map_err(to_backend)
}

fn select_run(connection: &Connection, run_id: &str) -> Result<Option<RunRecord>, StateError> {
    connection
        .query_row(
            r#"
            SELECT id, task_id, agent, status, started_at_ms, completed_at_ms
            FROM orchestration_runs
            WHERE id = ?1
            "#,
            params![run_id],
            run_row,
        )
        .optional()
        .map_err(to_backend)
}

fn select_runs_for_task(
    connection: &Connection,
    task_id: &str,
) -> Result<Vec<RunRecord>, StateError> {
    let mut statement = connection
        .prepare(
            r#"
            SELECT id, task_id, agent, status, started_at_ms, completed_at_ms
            FROM orchestration_runs
            WHERE task_id = ?1
            ORDER BY started_at_ms, id
            "#,
        )
        .map_err(to_backend)?;
    let rows = statement
        .query_map(params![task_id], run_row)
        .map_err(to_backend)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(to_backend)?;
    Ok(rows)
}

fn select_events_for_runs(
    connection: &Connection,
    run_ids: &[&str],
) -> Result<Vec<EventRecord>, StateError> {
    let mut events = Vec::new();
    for run_id in run_ids {
        let mut statement = connection
            .prepare(
                r#"
                SELECT id, run_id, sequence, event_type, payload_json, created_at_ms
                FROM orchestration_events
                WHERE run_id = ?1
                ORDER BY sequence
                "#,
            )
            .map_err(to_backend)?;
        let rows = statement
            .query_map(params![run_id], event_row)
            .map_err(to_backend)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(to_backend)?;
        events.extend(rows);
    }
    Ok(events)
}

fn select_approvals_for_runs(
    connection: &Connection,
    run_ids: &[&str],
) -> Result<Vec<ApprovalRecord>, StateError> {
    let mut approvals = Vec::new();
    for run_id in run_ids {
        let mut statement = connection
            .prepare(
                r#"
                SELECT id, run_id, status, reason, risk_json, created_at_ms, resolved_at_ms
                FROM orchestration_approvals
                WHERE run_id = ?1
                ORDER BY created_at_ms, id
                "#,
            )
            .map_err(to_backend)?;
        let rows = statement
            .query_map(params![run_id], approval_row)
            .map_err(to_backend)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(to_backend)?;
        approvals.extend(rows);
    }
    Ok(approvals)
}

fn select_messages_for_task(
    connection: &Connection,
    task_id: &str,
) -> Result<Vec<MessageRecord>, StateError> {
    let mut statement = connection
        .prepare(
            r#"
            SELECT id, task_id, run_id, channel, sender, direction, body, created_at_ms
            FROM orchestration_messages
            WHERE task_id = ?1
            ORDER BY created_at_ms, id
            "#,
        )
        .map_err(to_backend)?;
    let rows = statement
        .query_map(params![task_id], message_row)
        .map_err(to_backend)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(to_backend)?;
    Ok(rows)
}

fn task_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRecord> {
    let labels_json: String = row.get(4)?;
    let labels = serde_json::from_str(&labels_json).unwrap_or_default();
    let status: String = row.get(5)?;
    Ok(TaskRecord {
        id: row.get(0)?,
        source: row.get(1)?,
        title: row.get(2)?,
        body: row.get(3)?,
        labels,
        status: TaskStatus::parse(&status),
        created_at_ms: row.get(6)?,
        updated_at_ms: row.get(7)?,
    })
}

fn run_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRecord> {
    let status: String = row.get(3)?;
    Ok(RunRecord {
        id: row.get(0)?,
        task_id: row.get(1)?,
        agent: row.get(2)?,
        status: RunStatus::parse(&status),
        started_at_ms: row.get(4)?,
        completed_at_ms: row.get(5)?,
    })
}

fn event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventRecord> {
    let payload_json: String = row.get(4)?;
    let payload = serde_json::from_str(&payload_json).unwrap_or(serde_json::Value::Null);
    Ok(EventRecord {
        id: row.get(0)?,
        run_id: row.get(1)?,
        sequence: row.get(2)?,
        event_type: row.get(3)?,
        payload,
        created_at_ms: row.get(5)?,
    })
}

fn approval_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ApprovalRecord> {
    let risk_json: String = row.get(4)?;
    let risk = serde_json::from_str(&risk_json).unwrap_or(Risk::Medium);
    Ok(ApprovalRecord {
        id: row.get(0)?,
        run_id: row.get(1)?,
        status: row.get(2)?,
        reason: row.get(3)?,
        risk,
        created_at_ms: row.get(5)?,
        resolved_at_ms: row.get(6)?,
    })
}

fn message_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MessageRecord> {
    let direction: String = row.get(5)?;
    Ok(MessageRecord {
        id: row.get(0)?,
        task_id: row.get(1)?,
        run_id: row.get(2)?,
        channel: row.get(3)?,
        sender: row.get(4)?,
        direction: MessageDirection::parse(&direction),
        body: row.get(6)?,
        created_at_ms: row.get(7)?,
    })
}

fn ensure_task_exists(connection: &Connection, task_id: &str) -> Result<(), StateError> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM orchestration_tasks WHERE id = ?1",
            params![task_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(to_backend)?;
    exists
        .map(|_| ())
        .ok_or_else(|| StateError::TaskNotFound(task_id.to_string()))
}

fn ensure_run_exists(connection: &Connection, run_id: &str) -> Result<(), StateError> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM orchestration_runs WHERE id = ?1",
            params![run_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(to_backend)?;
    exists
        .map(|_| ())
        .ok_or_else(|| StateError::Backend(format!("run not found: {run_id}")))
}

fn next_event_sequence(connection: &Connection, run_id: &str) -> Result<i64, StateError> {
    connection
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM orchestration_events WHERE run_id = ?1",
            params![run_id],
            |row| row.get(0),
        )
        .map_err(to_backend)
}

fn unix_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn to_backend(error: impl std::fmt::Display) -> StateError {
    StateError::Backend(error.to_string())
}
