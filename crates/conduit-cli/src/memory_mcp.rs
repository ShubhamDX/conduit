use anyhow::{Context, Result};
use serde_json::Value;
use std::path::Path;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const TOOLS: &[(&str, &str, &str)] = &[
    (
        "memory_search",
        "Search scoped Conduit shared memory by optional tags.",
        r#"{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"}},"limit":{"type":"integer","minimum":1,"maximum":20}},"additionalProperties":false}"#,
    ),
    (
        "memory_get",
        "Fetch one scoped Conduit shared-memory entry by key.",
        r#"{"type":"object","properties":{"key":{"type":"string"}},"required":["key"],"additionalProperties":false}"#,
    ),
    (
        "memory_upsert",
        "Write a scoped Conduit shared-memory entry.",
        r#"{"type":"object","properties":{"key":{"type":"string"},"value":{"type":"string"},"tags":{"type":"array","items":{"type":"string"}}},"required":["key","value"],"additionalProperties":false}"#,
    ),
];

pub async fn run(socket: &Path) -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = stdout;

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let payload: Value = serde_json::from_str(&line).context("parse mcp request")?;
        if let Some(response) = handle_payload(&payload, socket).await? {
            let mut encoded = serde_json::to_string(&response)?;
            encoded.push('\n');
            stdout.write_all(encoded.as_bytes()).await?;
            stdout.flush().await?;
        }
    }

    Ok(())
}

async fn handle_payload(payload: &Value, socket: &Path) -> Result<Option<Value>> {
    let method = payload
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let id = payload.get("id").cloned().unwrap_or(Value::Null);

    match method {
        "notifications/initialized" => Ok(None),
        "initialize" => Ok(Some(response(
            id,
            serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "conduit-memory", "version": "0.1.0" },
            }),
        ))),
        "tools/list" => Ok(Some(response(
            id,
            serde_json::json!({ "tools": tool_specs() }),
        ))),
        "tools/call" => {
            let params = payload
                .get("params")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let result = call_memory(socket, name, arguments).await?;
            if let Some(error) = result.get("error").and_then(Value::as_str) {
                return Ok(Some(response(
                    id,
                    serde_json::json!({
                        "content": [{ "type": "text", "text": error }],
                        "isError": true,
                    }),
                )));
            }
            Ok(Some(response(
                id,
                serde_json::json!({
                    "content": [{
                        "type": "text",
                        "text": serde_json::to_string(result.get("result").unwrap_or(&Value::Null))?,
                    }],
                    "isError": false,
                }),
            )))
        }
        _ => Ok(Some(error(id, -32601, format!("unknown method: {method}")))),
    }
}

fn tool_specs() -> Vec<Value> {
    TOOLS
        .iter()
        .map(|(name, description, schema)| {
            serde_json::json!({
                "name": name,
                "description": description,
                "inputSchema": serde_json::from_str::<Value>(schema).expect("valid tool schema"),
            })
        })
        .collect()
}

async fn call_memory(socket: &Path, method: &str, params: Value) -> Result<Value> {
    let mut stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect memory socket {}", socket.display()))?;
    let mut request = serde_json::to_string(&serde_json::json!({
        "method": method,
        "params": params,
    }))?;
    request.push('\n');
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut lines = BufReader::new(stream).lines();
    let line = lines
        .next_line()
        .await?
        .context("memory socket closed without response")?;
    serde_json::from_str(&line).context("parse memory socket response")
}

fn response(id: Value, result: Value) -> Value {
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn error(id: Value, code: i64, message: impl Into<String>) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message.into() },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tools_list_returns_memory_tools() {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
        });

        let response = handle_payload(&payload, Path::new("/tmp/no-socket"))
            .await
            .unwrap()
            .unwrap();
        let tools = response["result"]["tools"].as_array().unwrap();
        assert_eq!(tools[0]["name"], "memory_search");
        assert_eq!(tools[1]["name"], "memory_get");
        assert_eq!(tools[2]["name"], "memory_upsert");
    }
}
