//! Bounded server-side diagnostics. Never use provider errors as chat replies.

use std::sync::OnceLock;

use regex::Regex;

/// Preserve the cause chain while removing credentials commonly embedded in
/// HTTP errors. Request/response bodies and prompts are not logged separately.
pub(crate) fn error_summary(error: &anyhow::Error) -> String {
    redact(&format!("{error:#}"))
}

fn redact(text: &str) -> String {
    static SECRETS: OnceLock<Regex> = OnceLock::new();
    let secrets = SECRETS.get_or_init(|| {
        Regex::new(concat!(
            r#"(?i)https?://[^\s\"<>]+|"#,
            r#"bearer\s+[^\s\",;]+|"#,
            r#"(?:access_token|refresh_token|api_key|token|password|secret)[\"']?\s*[:=]\s*[\"']?[^\s\"',;}]+|"#,
            r#"\b\d{6,}:[A-Za-z0-9_-]{20,}\b"#
        ))
        .expect("diagnostic credential pattern")
    });
    secrets
        .replace_all(text, "[redacted]")
        .chars()
        .take(2000)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retains_cause_and_status_without_credentials() {
        let error = anyhow::anyhow!(
            "502 Bad Gateway https://api.telegram.org/bot123456:fake_secret_value_long_enough/getUpdates Bearer private-bearer {{\"refresh_token\":\"private-refresh\"}}"
        ).context("LLM chat request failed");
        let summary = error_summary(&error);
        assert!(summary.contains("LLM chat request failed: 502 Bad Gateway"));
        for secret in [
            "fake_secret",
            "private-bearer",
            "private-refresh",
            "api.telegram.org",
        ] {
            assert!(!summary.contains(secret));
        }
        assert_eq!(redact(&"é".repeat(3000)).chars().count(), 2000);
    }
}
