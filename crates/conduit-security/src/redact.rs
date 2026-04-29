use regex::Regex;
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
                regex: Regex::new(r"sk-[A-Za-z0-9]{20,}").unwrap(),
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
                regex: Regex::new(r"xoxb-[A-Za-z0-9-]{20,}").unwrap(),
                replace: "xoxb-[REDACTED]",
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

#[cfg(test)]
mod tests {
    use super::redact;

    #[test]
    fn redacts_openai_keys() {
        let input = "key is sk-proj-abc123XYZ456def789GHJ012 and value";
        let out = redact(input);
        assert!(!out.contains("abc123"));
        assert!(out.contains("sk-proj-[REDACTED]"));
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
    fn preserves_non_secret_text() {
        assert_eq!(redact("hello world"), "hello world");
    }
}
