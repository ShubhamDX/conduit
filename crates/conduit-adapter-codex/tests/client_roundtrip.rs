use conduit_adapter_codex::client::{StdioClient, StdioClientOptions};
use conduit_core::adapter::{MemoryToolError, MemoryToolProvider, SecurityPolicy};
use conduit_core::event::AgentEvent;
use std::collections::HashMap;
use std::sync::Arc;

struct FakeMemoryTools;
struct SecretMemoryTools;

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

#[async_trait::async_trait]
impl MemoryToolProvider for SecretMemoryTools {
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
                "value": "sk-proj-abc123XYZ456def789GHJ012",
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
async fn spawn_options_inject_environment() {
    let fixture = format!(
        "{}/tests/fixtures/fake_env_codex.sh",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut env = HashMap::new();
    env.insert("CONDUIT_TEST_ENV".to_string(), "from-env".to_string());
    let mut client = StdioClient::spawn_with_options(
        "bash",
        &[fixture],
        StdioClientOptions {
            env,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let response = client
        .request("newSession", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(response["session_id"], "s1");

    match client.next_event().await.unwrap() {
        AgentEvent::TokenDelta { text } => assert_eq!(text, "from-env"),
        _ => panic!("wrong variant"),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn spawn_options_apply_resource_limits() {
    let fixture = format!(
        "{}/tests/fixtures/fake_nofile_codex.sh",
        env!("CARGO_MANIFEST_DIR")
    );
    let policy = SecurityPolicy {
        max_open_files: Some(64),
        ..Default::default()
    };
    let mut client = StdioClient::spawn_with_options(
        "bash",
        &[fixture],
        StdioClientOptions {
            rlimits: conduit_security::rlimits::limits_to_closure(&policy),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let response = client
        .request("newSession", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(response["session_id"], "s1");

    match client.next_event().await.unwrap() {
        AgentEvent::TokenDelta { text } => assert_eq!(text, "64"),
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

#[tokio::test]
async fn memory_tool_events_preserve_raw_output_before_orchestrator_persistence() {
    let fixture = format!(
        "{}/tests/fixtures/fake_memory_codex.py",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut client = StdioClient::spawn_with_options(
        "python3",
        &[fixture],
        StdioClientOptions {
            memory_tools: Some(Arc::new(SecretMemoryTools)),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let response = client
        .request("newSession", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(response["session_id"], "s1");

    let _started = client.next_event().await.unwrap();
    match client.next_event().await.unwrap() {
        AgentEvent::ToolCallCompleted { output, .. } => {
            assert!(output.contains("sk-proj-abc123XYZ456def789GHJ012"));
        }
        _ => panic!("wrong variant"),
    }
    match client.next_event().await.unwrap() {
        AgentEvent::TokenDelta { text } => {
            assert!(text.contains("sk-proj-abc123XYZ456def789GHJ012"));
        }
        _ => panic!("wrong variant"),
    }
}
