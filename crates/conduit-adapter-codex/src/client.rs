use crate::event_map::map_codex_event;
use crate::protocol::{
    decode_incoming, IncomingRpcMessage, IncomingRpcRequest, RpcError, RpcRequest, RpcResponse,
};
use conduit_core::adapter::{MemoryToolError, MemoryToolProvider};
use conduit_core::error::AdapterError;
use conduit_core::event::AgentEvent;
use conduit_security::redact::redact_event;
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

pub type PreExecHook = Box<dyn Fn() -> std::io::Result<()> + Send + Sync + 'static>;

pub struct StdioClientOptions {
    pub memory_tools: Option<Arc<dyn MemoryToolProvider>>,
    pub env: HashMap<String, String>,
    pub rlimits: Option<PreExecHook>,
    pub redact_events: bool,
}

impl Default for StdioClientOptions {
    fn default() -> Self {
        Self {
            memory_tools: None,
            env: HashMap::new(),
            rlimits: None,
            redact_events: true,
        }
    }
}

impl StdioClient {
    pub async fn spawn(program: &str, args: &[String]) -> Result<Self, AdapterError> {
        Self::spawn_with_options(program, args, StdioClientOptions::default()).await
    }

    pub async fn spawn_with_memory_tools(
        program: &str,
        args: &[String],
        memory_tools: Option<Arc<dyn MemoryToolProvider>>,
    ) -> Result<Self, AdapterError> {
        Self::spawn_with_options(
            program,
            args,
            StdioClientOptions {
                memory_tools,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn spawn_with_options(
        program: &str,
        args: &[String],
        options: StdioClientOptions,
    ) -> Result<Self, AdapterError> {
        let StdioClientOptions {
            memory_tools,
            env,
            rlimits,
            redact_events,
        } = options;
        let mut command = Command::new(program);
        command
            .args(args)
            .envs(env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        #[cfg(unix)]
        if let Some(rlimits) = rlimits {
            // SAFETY: pre_exec runs in the child after fork and before exec. The
            // configured hook only applies OS rlimits so the spawned agent starts
            // under the policy requested by the orchestrator.
            unsafe {
                command.pre_exec(move || rlimits());
            }
        }

        #[cfg(not(unix))]
        let _ = rlimits;

        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| AdapterError::Protocol("child stdin unavailable".into()))?;
        let stdin = Arc::new(Mutex::new(stdin));
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AdapterError::Protocol("child stdout unavailable".into()))?;
        let pending = Arc::new(Mutex::new(
            HashMap::<u64, oneshot::Sender<RpcResponse>>::new(),
        ));
        let (events_tx, events_rx) = mpsc::channel::<AgentEvent>(64);

        let pending_reader = Arc::clone(&pending);
        let stdin_reader = Arc::clone(&stdin);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Ok(message) = decode_incoming(&line) {
                    match message {
                        IncomingRpcMessage::Response(response) => {
                            if let Some(tx) = pending_reader.lock().await.remove(&response.id) {
                                let _ = tx.send(response);
                                continue;
                            }
                        }
                        IncomingRpcMessage::Notification(notification) => {
                            if notification.method == "event" {
                                if let Some(event) = map_codex_event(&notification.params) {
                                    let event = maybe_redact_event(event, redact_events);
                                    let _ = events_tx.send(event).await;
                                }
                            }
                            continue;
                        }
                        IncomingRpcMessage::Request(request) => {
                            handle_child_request(
                                request,
                                memory_tools.as_deref(),
                                &stdin_reader,
                                &events_tx,
                                redact_events,
                            )
                            .await;
                            continue;
                        }
                    }
                }

                if let Ok(response) = serde_json::from_str::<RpcResponse>(&line) {
                    if let Some(tx) = pending_reader.lock().await.remove(&response.id) {
                        let _ = tx.send(response);
                        continue;
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
            stdin,
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

async fn handle_child_request(
    request: IncomingRpcRequest,
    memory_tools: Option<&dyn MemoryToolProvider>,
    stdin: &Arc<Mutex<ChildStdin>>,
    events_tx: &mpsc::Sender<AgentEvent>,
    redact_events: bool,
) {
    let call_id = format!("jsonrpc:{}", request.id);
    let _ = events_tx
        .send(maybe_redact_event(
            AgentEvent::ToolCallStarted {
                call_id: call_id.clone(),
                name: request.method.clone(),
                args: request.params.clone(),
            },
            redact_events,
        ))
        .await;

    let result = match memory_tools {
        Some(memory_tools) if is_memory_tool(&request.method) => {
            memory_tools
                .call(&request.method, request.params.clone())
                .await
        }
        Some(_) => Err(MemoryToolError::unavailable(format!(
            "unknown method: {}",
            request.method
        ))),
        None => Err(MemoryToolError::unavailable(
            "memory tools are not available for this session",
        )),
    };

    match result {
        Ok(result) => {
            let output = serde_json::to_string(&result).unwrap_or_else(|_| String::new());
            let _ = write_json_line(
                stdin,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request.id,
                    "result": result,
                }),
            )
            .await;
            let _ = events_tx
                .send(maybe_redact_event(
                    AgentEvent::ToolCallCompleted {
                        call_id,
                        ok: true,
                        output,
                    },
                    redact_events,
                ))
                .await;
        }
        Err(error) => {
            let output = error.to_string();
            let _ = write_json_line(
                stdin,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request.id,
                    "error": {
                        "code": json_rpc_error_code(&error),
                        "message": error.message.clone(),
                    },
                }),
            )
            .await;
            let _ = events_tx
                .send(maybe_redact_event(
                    AgentEvent::ToolCallCompleted {
                        call_id,
                        ok: false,
                        output,
                    },
                    redact_events,
                ))
                .await;
        }
    }
}

fn maybe_redact_event(event: AgentEvent, redact_events: bool) -> AgentEvent {
    if redact_events {
        redact_event(event)
    } else {
        event
    }
}

async fn write_json_line(
    stdin: &Arc<Mutex<ChildStdin>>,
    value: serde_json::Value,
) -> Result<(), std::io::Error> {
    let mut line = serde_json::to_string(&value).map_err(std::io::Error::other)?;
    line.push('\n');
    let mut stdin = stdin.lock().await;
    stdin.write_all(line.as_bytes()).await?;
    stdin.flush().await
}

fn is_memory_tool(name: &str) -> bool {
    matches!(name, "memory_search" | "memory_get" | "memory_upsert")
}

fn json_rpc_error_code(error: &MemoryToolError) -> i64 {
    match error.code.as_str() {
        "invalid_request" => -32602,
        "unavailable" => -32601,
        _ => -32000,
    }
}
