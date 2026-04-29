use conduit_core::event::AgentEvent;
use regex::Regex;
use serde_json::{Map, Value};
use std::sync::OnceLock;

struct Pattern {
    regex: Regex,
    replace: &'static str,
}

fn patterns() -> &'static [Pattern] {
    static PATTERNS: OnceLock<Vec<Pattern>> = OnceLock::new();

    PATTERNS.get_or_init(|| {
        vec![
            Pattern {
                regex: Regex::new(r"sk-proj-[A-Za-z0-9_-]{20,}").unwrap(),
                replace: "sk-proj-[REDACTED]",
            },
            Pattern {
                regex: Regex::new(r"sk-ant-api\d+-[A-Za-z0-9_-]{20,}").unwrap(),
                replace: "sk-ant-[REDACTED]",
            },
            Pattern {
                regex: Regex::new(r"\bsk-[A-Za-z0-9_-]{20,}").unwrap(),
                replace: "sk-[REDACTED]",
            },
            Pattern {
                regex: Regex::new(r"AKIA[0-9A-Z]{16}").unwrap(),
                replace: "AKIA[REDACTED]",
            },
            Pattern {
                regex: Regex::new(r"ghp_[A-Za-z0-9]{20,}").unwrap(),
                replace: "ghp_[REDACTED]",
            },
            Pattern {
                regex: Regex::new(r"AIza[0-9A-Za-z_-]{35}").unwrap(),
                replace: "AIza[REDACTED]",
            },
            Pattern {
                regex: Regex::new(r"xoxb-[A-Za-z0-9-]{20,}").unwrap(),
                replace: "xoxb-[REDACTED]",
            },
            Pattern {
                regex: Regex::new(r"(?i)bearer[ \t]+[A-Za-z0-9._~+/=-]{20,}").unwrap(),
                replace: "Bearer [REDACTED]",
            },
            Pattern {
                regex: Regex::new(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}")
                    .unwrap(),
                replace: "jwt-[REDACTED]",
            },
            Pattern {
                regex: Regex::new(
                    r"(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
                )
                .unwrap(),
                replace: "-----BEGIN PRIVATE KEY-----[REDACTED]-----END PRIVATE KEY-----",
            },
        ]
    })
}

pub fn redact(input: &str) -> String {
    let mut out = input.to_string();

    for pattern in patterns() {
        out = pattern.regex.replace_all(&out, pattern.replace).to_string();
    }

    out
}

pub fn redact_json(value: Value) -> Value {
    match value {
        Value::String(value) => Value::String(redact(&value)),
        Value::Array(values) => Value::Array(values.into_iter().map(redact_json).collect()),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (redact(&key), redact_json(value)))
                .collect::<Map<_, _>>(),
        ),
        other => other,
    }
}

pub fn redact_event(event: AgentEvent) -> AgentEvent {
    match event {
        AgentEvent::TokenDelta { text } => AgentEvent::TokenDelta {
            text: redact(&text),
        },
        AgentEvent::ToolCallStarted {
            call_id,
            name,
            args,
        } => AgentEvent::ToolCallStarted {
            call_id,
            name,
            args: redact_json(args),
        },
        AgentEvent::ToolCallCompleted {
            call_id,
            ok,
            output,
        } => AgentEvent::ToolCallCompleted {
            call_id,
            ok,
            output: redact(&output),
        },
        AgentEvent::ApprovalRequested {
            call_id,
            reason,
            risk,
        } => AgentEvent::ApprovalRequested {
            call_id,
            reason: redact(&reason),
            risk,
        },
        AgentEvent::Error { code, message } => AgentEvent::Error {
            code,
            message: redact(&message),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::{redact, redact_event, redact_json};
    use conduit_core::event::AgentEvent;

    #[test]
    fn redacts_openai_keys() {
        let input = "key is sk-proj-abc123XYZ456def789GHJ012 and value";
        let out = redact(input);
        assert!(!out.contains("abc123"));
        assert!(out.contains("sk-proj-[REDACTED]"));
    }

    #[test]
    fn redacts_generic_sk_keys_with_dash_and_underscore() {
        let input = "key is sk-abc_def-ghi_jkl-mno_pqr1234.";
        let out = redact(input);
        assert!(!out.contains("abc_def"));
        assert!(out.contains("sk-[REDACTED]."));
    }

    #[test]
    fn redacts_consecutive_generic_sk_keys() {
        let input = "sk-aaaaaaaaaaaaaaaaaaaaaaaa sk-bbbbbbbbbbbbbbbbbbbbbbbb";
        let out = redact(input);
        assert!(!out.contains("aaaaaaaa"));
        assert!(!out.contains("bbbbbbbb"));
        assert_eq!(out, "sk-[REDACTED] sk-[REDACTED]");
    }

    #[test]
    fn redacts_anthropic_keys() {
        let input = "sk-ant-api03-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        let out = redact(input);
        assert!(out.contains("sk-ant-[REDACTED]"));
    }

    #[test]
    fn redacts_aws_keys() {
        let out = redact("AKIAIOSFODNN7EXAMPLE");
        assert!(out.contains("AKIA[REDACTED]"));
    }

    #[test]
    fn redacts_common_bearer_and_google_tokens() {
        let google = format!("{}{}", ["AI", "za"].concat(), "a".repeat(35));
        let bearer_token = "b".repeat(30);
        let bearer = format!("Authorization: {} {}", "Bearer", bearer_token);
        let out = redact(&format!("{google}\n{bearer}"));
        assert!(!out.contains(&google));
        assert!(!out.contains(&bearer_token));
        assert!(out.contains("AIza[REDACTED]"));
        assert!(out.contains("Bearer [REDACTED]"));
    }

    #[test]
    fn redacts_jwts_and_private_keys() {
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signaturepart000";
        let pem = "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----";
        let out = redact(&format!("{jwt}\n{pem}"));
        assert!(!out.contains("signaturepart000"));
        assert!(out.contains("jwt-[REDACTED]"));
        assert!(out.contains("[REDACTED]"));
    }

    #[test]
    fn preserves_non_secret_text() {
        assert_eq!(redact("hello world"), "hello world");
    }

    #[test]
    fn redacts_json_strings_and_keys() {
        let value = serde_json::json!({
            "sk-proj-abc123XYZ456def789GHJ012": "value sk-proj-abc123XYZ456def789GHJ012",
            "nested": ["sk-proj-abc123XYZ456def789GHJ012"]
        });
        let out = redact_json(value);
        let encoded = serde_json::to_string(&out).unwrap();
        assert!(!encoded.contains("abc123"));
        assert!(encoded.contains("sk-proj-[REDACTED]"));
    }

    #[test]
    fn redacts_streamed_agent_events() {
        let event = AgentEvent::ToolCallCompleted {
            call_id: "c1".into(),
            ok: true,
            output: "sk-proj-abc123XYZ456def789GHJ012".into(),
        };
        let out = redact_event(event);
        match out {
            AgentEvent::ToolCallCompleted { output, .. } => {
                assert!(!output.contains("abc123"));
                assert_eq!(output, "sk-proj-[REDACTED]");
            }
            _ => panic!("wrong variant"),
        }
    }
}
