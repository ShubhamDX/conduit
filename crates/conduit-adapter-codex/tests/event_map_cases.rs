use conduit_adapter_codex::event_map::map_codex_event;
use conduit_core::event::AgentEvent;

#[test]
fn maps_token_delta() {
    let value = serde_json::json!({"kind": "agent_message_delta", "delta": "hello "});
    let out = map_codex_event(&value).unwrap();

    match out {
        AgentEvent::TokenDelta { text } => assert_eq!(text, "hello "),
        _ => panic!("wrong variant"),
    }
}

#[test]
fn maps_tool_start() {
    let value = serde_json::json!({
        "kind": "tool_call_started",
        "call_id": "c42",
        "name": "apply_patch",
        "args": {"path": "a.rs"}
    });
    let out = map_codex_event(&value).unwrap();

    assert!(matches!(out, AgentEvent::ToolCallStarted { .. }));
}

#[test]
fn unknown_returns_none() {
    let value = serde_json::json!({"kind": "something_weird"});
    assert!(map_codex_event(&value).is_none());
}
