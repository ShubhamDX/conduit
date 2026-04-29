use crate::event_map::map_codex_event;
use crate::protocol::{RpcError, RpcNotification, RpcRequest, RpcResponse};
use conduit_core::error::AdapterError;
use conduit_core::event::AgentEvent;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, Mutex};

pub struct StdioClient {
    child: Child,
    stdin: Arc<Mutex<ChildStdin>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<RpcResponse>>>>,
    events_rx: mpsc::Receiver<AgentEvent>,
    next_id: Arc<AtomicU64>,
}

impl StdioClient {
    pub async fn spawn(program: &str, args: &[String]) -> Result<Self, AdapterError> {
        let mut command = Command::new(program);
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| AdapterError::Protocol("child stdin unavailable".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AdapterError::Protocol("child stdout unavailable".into()))?;
        let pending = Arc::new(Mutex::new(
            HashMap::<u64, oneshot::Sender<RpcResponse>>::new(),
        ));
        let (events_tx, events_rx) = mpsc::channel::<AgentEvent>(64);

        let pending_reader = Arc::clone(&pending);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Ok(response) = serde_json::from_str::<RpcResponse>(&line) {
                    if let Some(tx) = pending_reader.lock().await.remove(&response.id) {
                        let _ = tx.send(response);
                        continue;
                    }
                }

                if let Ok(notification) = serde_json::from_str::<RpcNotification>(&line) {
                    if notification.method == "event" {
                        if let Some(event) = map_codex_event(&notification.params) {
                            let _ = events_tx.send(event).await;
                        }
                    }
                }
            }

            let mut pending = pending_reader.lock().await;
            for (id, tx) in pending.drain() {
                let _ = tx.send(RpcResponse {
                    id,
                    result: None,
                    error: Some(RpcError {
                        code: -32000,
                        message: "stdio child exited before responding".into(),
                    }),
                });
            }
        });

        Ok(Self {
            child,
            stdin: Arc::new(Mutex::new(stdin)),
            pending,
            events_rx,
            next_id: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn request(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, AdapterError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        let request = RpcRequest {
            id,
            method: method.into(),
            params,
        };
        let mut line = serde_json::to_string(&request)
            .map_err(|err| AdapterError::Protocol(err.to_string()))?;
        line.push('\n');

        {
            let mut stdin = self.stdin.lock().await;
            stdin.write_all(line.as_bytes()).await?;
            stdin.flush().await?;
        }

        let response = rx
            .await
            .map_err(|_| AdapterError::Protocol("stdio response channel closed".into()))?;
        if let Some(error) = response.error {
            return Err(AdapterError::Protocol(format!(
                "{} ({})",
                error.message, error.code
            )));
        }

        Ok(response.result.unwrap_or(serde_json::Value::Null))
    }

    pub async fn next_event(&mut self) -> Option<AgentEvent> {
        self.events_rx.recv().await
    }

    pub fn take_events_rx(self) -> mpsc::Receiver<AgentEvent> {
        let Self {
            mut child,
            stdin,
            pending,
            events_rx,
            next_id,
        } = self;

        tokio::spawn(async move {
            let _stdin = stdin;
            let _pending = pending;
            let _next_id = next_id;
            let _ = child.wait().await;
        });

        events_rx
    }
}
