use conduit_core::event::{AgentEvent, Risk};

#[test]
fn serde_roundtrip_approval() {
    let ev = AgentEvent::ApprovalRequested {
        call_id: "c1".into(),
        reason: "writes outside workspace".into(),
        risk: Risk::High,
    };

    let json = serde_json::to_string(&ev).unwrap();
    assert!(json.contains("\"type\":\"approval_requested\""));
    assert!(json.contains("\"risk\":\"high\""));

    let back: AgentEvent = serde_json::from_str(&json).unwrap();
    match back {
        AgentEvent::ApprovalRequested { call_id, .. } => assert_eq!(call_id, "c1"),
        _ => panic!("wrong variant"),
    }
}
