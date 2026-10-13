//! Tool results as values (contract C5). Pure.

use serde_json::{Value, json};

/// Stable codes. Every code may appear as `error.code`; `CHARACTERS_REPLACED`
/// appears only in `warnings` (the file was stored).
pub mod codes {
    pub const INVALID_INPUT: &str = "invalid_input";
    pub const CREDENTIAL_MISSING: &str = "credential_missing";
    pub const UNAUTHORIZED: &str = "unauthorized";
    pub const RATE_LIMITED: &str = "rate_limited";
    pub const PROVIDER_ERROR: &str = "provider_error";
    pub const DOWNLOAD_FAILED: &str = "download_failed";
    pub const TOO_LARGE: &str = "too_large";
    pub const UNSUPPORTED_FORMAT: &str = "unsupported_format";
    pub const ARTIFACT_UNAVAILABLE: &str = "artifact_unavailable";
    pub const ARTIFACT_REJECTED: &str = "artifact_rejected";
    /// Warning only: text the format cannot encode was written as `?`.
    pub const CHARACTERS_REPLACED: &str = "characters_replaced";
}

/// A stored artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    pub id: String,
    pub mime_type: String,
    pub name: String,
}

/// A failure or a non-fatal warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub code: &'static str,
    pub message: String,
}

impl Failure {
    #[must_use]
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

fn artifact_json(a: &Artifact) -> Value {
    json!({ "id": a.id, "mime_type": a.mime_type, "name": a.name })
}

fn failure_json(f: &Failure) -> Value {
    json!({ "code": f.code, "message": f.message })
}

/// Success value. `artifacts` must not be empty; `artifact` is the first.
#[must_use]
pub fn success(artifacts: &[Artifact], warnings: &[Failure]) -> Value {
    json!({
        "ok": true,
        "artifact": artifacts.first().map(artifact_json),
        "artifacts": artifacts.iter().map(artifact_json).collect::<Vec<_>>(),
        "warnings": warnings.iter().map(failure_json).collect::<Vec<_>>(),
    })
}

/// Failure value.
#[must_use]
pub fn failure(f: &Failure) -> Value {
    json!({ "ok": false, "error": failure_json(f) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_carries_first_artifact_and_all() {
        let a = Artifact {
            id: "artifact://1".into(),
            mime_type: "image/png".into(),
            name: "a.png".into(),
        };
        let b = Artifact {
            id: "artifact://2".into(),
            mime_type: "image/png".into(),
            name: "b.png".into(),
        };
        let v = success(&[a, b], &[]);
        assert_eq!(v["ok"], true);
        assert_eq!(v["artifact"]["id"], "artifact://1");
        assert_eq!(v["artifact"]["mime_type"], "image/png");
        assert_eq!(v["artifacts"].as_array().unwrap().len(), 2);
        assert!(v.get("error").is_none());
    }

    #[test]
    fn failure_is_a_value_with_a_code() {
        let v = failure(&Failure::new(codes::TOO_LARGE, "x"));
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"]["code"], "too_large");
        assert_eq!(v["error"]["message"], "x");
        assert!(v.get("artifact").is_none());
    }
}
