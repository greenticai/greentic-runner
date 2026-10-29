use std::env;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use jsonschema::{Draft, Validator};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ValidationMode {
    Off,
    Warn,
    Error,
}

#[derive(Clone, Debug)]
pub struct ValidationConfig {
    pub mode: ValidationMode,
}

impl ValidationConfig {
    pub fn from_env() -> Self {
        if let Some(mode) = env::var("GREENTIC_VALIDATION").ok().and_then(parse_mode) {
            return Self { mode };
        }
        let ci = env::var("CI")
            .ok()
            .map(|value| value == "true" || value == "1")
            .unwrap_or(false);
        Self {
            mode: if ci {
                ValidationMode::Warn
            } else {
                ValidationMode::Off
            },
        }
    }

    pub fn with_mode(mut self, mode: ValidationMode) -> Self {
        self.mode = mode;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub code: String,
    pub path: String,
    pub message: String,
}

pub fn validate_component_envelope(envelope: &Value) -> Vec<ValidationIssue> {
    validate_with_required(envelope, &["component_id", "operation"])
}

pub fn validate_tool_envelope(envelope: &Value) -> Vec<ValidationIssue> {
    validate_with_required(envelope, &["tool_id", "operation"])
}

fn validate_with_required(envelope: &Value, required: &[&str]) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    for key in required {
        match envelope.get(*key) {
            None => issues.push(ValidationIssue {
                code: "missing_required_field".to_string(),
                path: format!("/{key}"),
                message: format!("missing required field `{key}`"),
            }),
            Some(Value::String(value)) if !value.trim().is_empty() => {}
            Some(_) => issues.push(ValidationIssue {
                code: "invalid_type".to_string(),
                path: format!("/{key}"),
                message: format!("`{key}` must be a non-empty string"),
            }),
        }
    }

    // The JSON schema also flags missing / mistyped required fields; those are
    // already reported above with a clearer code, so drop the duplicates.
    let already: Vec<(String, String)> = issues
        .iter()
        .map(|issue| (issue.code.clone(), issue.path.clone()))
        .collect();
    let is_duplicate = |schema_issue: &ValidationIssue| {
        let missing_dup = required.iter().any(|key| {
            schema_issue.message == format!("\"{key}\" is a required property")
                && already.iter().any(|(code, path)| {
                    code == "missing_required_field" && *path == format!("/{key}")
                })
        });
        let type_dup = already
            .iter()
            .any(|(code, path)| code == "invalid_type" && *path == schema_issue.path);
        missing_dup || type_dup
    };
    issues.extend(
        validate_schema(envelope)
            .into_iter()
            .filter(|i| !is_duplicate(i)),
    );
    issues.extend(validate_metadata(envelope));
    issues
}

fn validate_schema(envelope: &Value) -> Vec<ValidationIssue> {
    INVOCATION_SCHEMA
        .iter_errors(envelope)
        .map(|err| ValidationIssue {
            code: "schema_validation".to_string(),
            path: err.instance_path().to_string(),
            message: err.to_string(),
        })
        .collect()
}

fn validate_metadata(envelope: &Value) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let Some(metadata) = envelope.get("metadata") else {
        return issues;
    };
    let Some(map) = metadata.as_object() else {
        issues.push(ValidationIssue {
            code: "invalid_type".to_string(),
            path: "/metadata".to_string(),
            message: "metadata must be an object".to_string(),
        });
        return issues;
    };

    if let Some(value) = map.get("tenant_id")
        && !value.is_string()
    {
        issues.push(ValidationIssue {
            code: "invalid_type".to_string(),
            path: "/metadata/tenant_id".to_string(),
            message: "tenant_id must be a string".to_string(),
        });
    }

    if let Some(value) = map.get("trace_id")
        && !value.is_string()
    {
        issues.push(ValidationIssue {
            code: "invalid_type".to_string(),
            path: "/metadata/trace_id".to_string(),
            message: "trace_id must be a string".to_string(),
        });
    }

    if let Some(session) = map.get("session") {
        match session.as_object() {
            Some(session_map) => {
                if let Some(id) = session_map.get("id")
                    && !id.is_string()
                {
                    issues.push(ValidationIssue {
                        code: "invalid_type".to_string(),
                        path: "/metadata/session/id".to_string(),
                        message: "session.id must be a string".to_string(),
                    });
                }
            }
            None => {
                issues.push(ValidationIssue {
                    code: "invalid_type".to_string(),
                    path: "/metadata/session".to_string(),
                    message: "session must be an object".to_string(),
                });
            }
        }
    }

    issues
}

fn parse_mode(raw: String) -> Option<ValidationMode> {
    match raw.to_lowercase().as_str() {
        "off" => Some(ValidationMode::Off),
        "warn" => Some(ValidationMode::Warn),
        "error" => Some(ValidationMode::Error),
        _ => None,
    }
}

static INVOCATION_SCHEMA: Lazy<Validator> = Lazy::new(|| {
    let schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "component_id": { "type": "string" },
            "tool_id": { "type": "string" },
            "operation": { "type": "string" },
            "input": {},
            "config": {},
            "metadata": { "type": "object" }
        },
        "additionalProperties": true
    });
    jsonschema::options()
        .with_draft(Draft::Draft7)
        .build(&schema)
        .expect("invocation envelope schema compile")
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_component_requires_fields() {
        let envelope = json!({
            "component_id": "demo.component",
            "operation": "run",
            "metadata": { "tenant_id": "acme" }
        });
        let issues = validate_component_envelope(&envelope);
        assert!(issues.is_empty());
    }

    #[test]
    fn validate_reports_missing_fields() {
        let envelope = json!({ "operation": 123 });
        let issues = validate_component_envelope(&envelope);
        assert!(!issues.is_empty());
        assert!(
            issues
                .iter()
                .any(|issue| issue.code == "missing_required_field")
        );
    }

    #[test]
    fn missing_required_field_is_reported_once() {
        let issues = validate_component_envelope(&json!({ "operation": "run" }));
        let for_component: Vec<_> = issues
            .iter()
            .filter(|issue| issue.message.contains("component_id"))
            .collect();
        assert_eq!(
            for_component.len(),
            1,
            "duplicate reports: {for_component:?}"
        );
        assert_eq!(for_component[0].code, "missing_required_field");
    }
}
