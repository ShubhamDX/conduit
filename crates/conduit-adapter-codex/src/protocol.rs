use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct RpcRequest {
    pub id: u64,
    pub method: String,
    pub params: serde_json::Value,
}

impl Serialize for RpcRequest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("RpcRequest", 4)?;
        state.serialize_field("jsonrpc", "2.0")?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("method", &self.method)?;
        state.serialize_field("params", &self.params)?;
        state.end()
    }
}

#[derive(Debug, Deserialize)]
pub struct RpcResponse {
    pub id: u64,
    #[serde(default)]
    pub result: Option<serde_json::Value>,
    #[serde(default)]
    pub error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct RpcNotification {
    pub method: String,
    pub params: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_request_serializes() {
        let request = RpcRequest {
            id: 1,
            method: "newSession".into(),
            params: serde_json::json!({"prompt": "hi"}),
        };

        let encoded = serde_json::to_string(&request).unwrap();
        assert!(encoded.contains("\"jsonrpc\":\"2.0\""));
        assert!(encoded.contains("\"method\":\"newSession\""));
    }

    #[test]
    fn event_notification_deserializes() {
        let encoded =
            r#"{"jsonrpc":"2.0","method":"event","params":{"type":"token_delta","text":"foo"}}"#;
        let notification: RpcNotification = serde_json::from_str(encoded).unwrap();
        assert_eq!(notification.method, "event");
    }
}
