use conduit_adapter_codex::client::StdioClient;
use conduit_core::adapter::{MemoryToolError, MemoryToolProvider};
use conduit_core::event::AgentEvent;
use std::sync::Arc;

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
        Ok(serde_json::json!({
            "entry": {
                "key": "k",
                "value": "from-memory",
                "tags": ["agent:codex"],
                "source": "issue:I1"
            }
        }))
    }
}

#[tokio::test]
async fn round_trip_request_and_event() {
    let fixture = format!(
        "{}/tests/fixtures/fake_codex.sh",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut client = StdioClient::spawn("bash", &[fixture]).await.unwrap();

    let response = client
        .request("newSession", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(response["session_id"], "s1");

    let event = client.next_event().await.unwrap();
    match event {
        AgentEvent::TokenDelta { text } => assert_eq!(text, "ok"),
        _ => panic!("wrong variant"),
    }
}

#[tokio::test]
async fn child_can_call_memory_tools_over_stdio() {
    let fixture = format!(
        "{}/tests/fixtures/fake_memory_codex.py",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut client = StdioClient::spawn_with_memory_tools(
        "python3",
        &[fixture],
        Some(Arc::new(FakeMemoryTools)),
    )
    .await
    .unwrap();

    let response = client
        .request("newSession", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(response["session_id"], "s1");

    assert!(matches!(
        client.next_event().await.unwrap(),
        AgentEvent::ToolCallStarted { name, .. } if name == "memory_get"
    ));
    assert!(matches!(
        client.next_event().await.unwrap(),
        AgentEvent::ToolCallCompleted { ok: true, .. }
    ));
    assert!(matches!(
        client.next_event().await.unwrap(),
        AgentEvent::TokenDelta { text } if text == "from-memory"
    ));
}
