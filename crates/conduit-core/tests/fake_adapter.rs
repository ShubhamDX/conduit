use async_trait::async_trait;
use conduit_core::adapter::{
    AgentAdapter, ApprovalMode, SecurityPolicy, SessionHandle, StartRequest,
};
use conduit_core::error::AdapterError;
use conduit_core::event::{AgentEvent, EndReason};

struct Fake;

#[async_trait]
impl AgentAdapter for Fake {
    fn name(&self) -> &str {
        "fake"
    }

    async fn start_session(&self, _req: StartRequest) -> Result<SessionHandle, AdapterError> {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(AgentEvent::SessionEnded {
            reason: EndReason::Completed,
        })
        .await
        .unwrap();

        Ok(SessionHandle {
            session_id: "s1".into(),
            events: rx,
        })
    }

    async fn stop_session(&self, _id: &str) -> Result<(), AdapterError> {
        Ok(())
    }
}

#[tokio::test]
async fn fake_emits_session_ended() {
    let a = Fake;
    let req = StartRequest {
        workspace: ".".into(),
        prompt: "hi".into(),
        model: None,
        approval_mode: ApprovalMode::Never,
        security_policy: SecurityPolicy::default(),
        memory: None,
        env: Default::default(),
    };

    let mut h = a.start_session(req).await.unwrap();
    let ev = h.events.recv().await.unwrap();
    assert!(matches!(ev, AgentEvent::SessionEnded { .. }));
    assert_eq!(a.name(), "fake");
}
