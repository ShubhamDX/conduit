use conduit_core::adapter::{MemoryToolError, MemoryToolProvider};
use conduit_core::error::AdapterError;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use uuid::Uuid;

const MAX_MEMORY_REQUEST_BYTES: usize = 1024 * 1024;
const MEMORY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub struct MemoryMcpProxy {
    socket_path: PathBuf,
    task: JoinHandle<()>,
}

impl MemoryMcpProxy {
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl Drop for MemoryMcpProxy {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

pub fn start_memory_mcp_proxy(
    workspace: &Path,
    memory_tools: Arc<dyn MemoryToolProvider>,
) -> Result<MemoryMcpProxy, AdapterError> {
    let runtime_dir = workspace.join(".conduit").join("sockets");
    std::fs::create_dir_all(&runtime_dir)?;
    let id = Uuid::new_v4().simple().to_string();
    let socket_path = runtime_dir.join(format!("m-{}.sock", &id[..12]));
    let listener = UnixListener::bind(&socket_path)?;
    let task = tokio::spawn(serve(listener, memory_tools));

    Ok(MemoryMcpProxy { socket_path, task })
}

async fn serve(listener: UnixListener, memory_tools: Arc<dyn MemoryToolProvider>) {
    loop {
        let Ok((stream, _addr)) = listener.accept().await else {
            break;
        };
        let memory_tools = Arc::clone(&memory_tools);
        tokio::spawn(async move {
            let _ = handle_stream(stream, memory_tools).await;
        });
    }
}

async fn handle_stream(
    stream: UnixStream,
    memory_tools: Arc<dyn MemoryToolProvider>,
) -> Result<(), std::io::Error> {
    let (mut reader, mut writer) = stream.into_split();
    let Some(line) = read_bounded_line(&mut reader).await? else {
        return Ok(());
    };

    let response = match serde_json::from_str::<serde_json::Value>(&line) {
        Ok(payload) => {
            let method = payload
                .get("method")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let params = payload
                .get("params")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            match memory_tools.call(method, params).await {
                Ok(result) => serde_json::json!({ "result": result }),
                Err(error) => serde_json::json!({ "error": error.message }),
            }
        }
        Err(error) => serde_json::json!({
            "error": MemoryToolError::invalid_request(error.to_string()).message,
        }),
    };

    let mut line = serde_json::to_string(&response).map_err(std::io::Error::other)?;
    line.push('\n');
    writer.write_all(line.as_bytes()).await?;
    writer.flush().await
}

async fn read_bounded_line<R>(reader: &mut R) -> Result<Option<String>, std::io::Error>
where
    R: AsyncRead + Unpin,
{
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        if buffer.len() >= MAX_MEMORY_REQUEST_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "memory request exceeds byte limit",
            ));
        }
        let read_len = chunk.len().min(MAX_MEMORY_REQUEST_BYTES - buffer.len());
        let bytes_read = timeout(MEMORY_REQUEST_TIMEOUT, reader.read(&mut chunk[..read_len]))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "memory request timed out")
            })??;
        if bytes_read == 0 {
            return if buffer.is_empty() {
                Ok(None)
            } else {
                String::from_utf8(buffer)
                    .map(Some)
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
            };
        }
        buffer.extend_from_slice(&chunk[..bytes_read]);
        if let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
            buffer.truncate(position);
            return String::from_utf8(buffer)
                .map(Some)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncBufReadExt, BufReader};

    struct FakeMemoryTools;

    #[async_trait::async_trait]
    impl MemoryToolProvider for FakeMemoryTools {
        async fn call(
            &self,
            name: &str,
            args: serde_json::Value,
        ) -> Result<serde_json::Value, MemoryToolError> {
            assert_eq!(name, "memory_get");
            assert_eq!(args["key"], "k");
            Ok(serde_json::json!({"entry": {"key": "k", "value": "v"}}))
        }
    }

    #[tokio::test]
    async fn proxy_forwards_unix_socket_calls_to_provider() {
        let workspace = test_workspace("codex-memory-proxy");
        let proxy = start_memory_mcp_proxy(&workspace, Arc::new(FakeMemoryTools)).unwrap();
        let mut stream = UnixStream::connect(proxy.socket_path()).await.unwrap();
        stream
            .write_all(br#"{"method":"memory_get","params":{"key":"k"}}"#)
            .await
            .unwrap();
        stream.write_all(b"\n").await.unwrap();

        let mut lines = BufReader::new(stream).lines();
        let response: serde_json::Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(response["result"]["entry"]["value"], "v");
    }

    #[tokio::test]
    async fn proxy_rejects_oversized_memory_requests() {
        let (mut client, mut server) = duplex(MAX_MEMORY_REQUEST_BYTES + 1);
        let task = tokio::spawn(async move { read_bounded_line(&mut server).await });

        client
            .write_all(&vec![b'a'; MAX_MEMORY_REQUEST_BYTES + 1])
            .await
            .unwrap();

        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    fn test_workspace(label: &str) -> PathBuf {
        let path = PathBuf::from("/tmp").join(format!(
            "conduit-{label}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}
