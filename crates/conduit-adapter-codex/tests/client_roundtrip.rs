use conduit_adapter_codex::client::StdioClient;
use conduit_core::event::AgentEvent;

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
