use jsonschema::{Draft, Validator};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaValidationIssue {
    pub code: String,
    pub path: String,
    pub message_key: String,
    pub fallback: String,
}

pub fn validate_json_instance(
    schema: &Value,
    instance: &Value,
    strict: bool,
) -> Vec<SchemaValidationIssue> {
    let mut issues = Vec::new();
    let mut unsupported = Vec::new();
    collect_unsupported_constraints(schema, "", &mut unsupported);
    if strict && !unsupported.is_empty() {
        for path in unsupported {
            issues.push(SchemaValidationIssue {
                code: "unsupported_schema_constraint".to_string(),
                path,
                message_key: "runner.schema.unsupported_constraint".to_string(),
                fallback: "schema contains unsupported constraint".to_string(),
            });
        }
        return issues;
    }

    let validator = match compile_validator(schema) {
        Ok(validator) => validator,
        Err(err) => {
            issues.push(SchemaValidationIssue {
                code: "invalid_schema".to_string(),
                path: "/".to_string(),
                message_key: "runner.schema.invalid_schema".to_string(),
                fallback: format!("invalid schema: {err}"),
            });
            return issues;
        }
    };

    for err in validator.iter_errors(instance) {
        let path = err.instance_path().to_string();
        issues.push(SchemaValidationIssue {
            code: "schema_validation".to_string(),
            path: if path.is_empty() {
                "/".to_string()
            } else {
                path
            },
            message_key: "runner.schema.validation_failed".to_string(),
            fallback: err.to_string(),
        });
    }
    issues
}

fn compile_validator(schema: &Value) -> Result<Validator, String> {
    jsonschema::options()
        .with_draft(Draft::Draft7)
        .build(schema)
        .map_err(|err| err.to_string())
}

fn collect_unsupported_constraints(schema: &Value, path: &str, out: &mut Vec<String>) {
    let Some(map) = schema.as_object() else {
        return;
    };
    for key in ["pattern", "format", "patternProperties"] {
        if map.contains_key(key) {
            out.push(format!("{}/{}", path_or_root(path), key));
        }
    }
    for (key, value) in map {
        let next = format!("{}/{}", path_or_root(path), key);
        // Data, not schema: never scan instance values for keywords.
        if matches!(key.as_str(), "enum" | "const" | "default" | "examples") {
            continue;
        }
        // Keys of these maps are user-chosen NAMES (a property may legitimately
        // be called `format` or `pattern`); only their values are subschemas.
        if matches!(key.as_str(), "properties" | "$defs" | "definitions")
            && let Value::Object(named) = value
        {
            for (name, subschema) in named {
                let sub_path = format!("{next}/{name}");
                collect_unsupported_constraints(subschema, &sub_path, out);
            }
            continue;
        }
        match value {
            Value::Object(_) => collect_unsupported_constraints(value, &next, out),
            Value::Array(items) => {
                for (idx, item) in items.iter().enumerate() {
                    let item_path = format!("{}/{}", next, idx);
                    collect_unsupported_constraints(item, &item_path, out);
                }
            }
            _ => {}
        }
    }
}

fn path_or_root(path: &str) -> &str {
    if path.is_empty() { "" } else { path }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn properties_named_like_keywords_are_not_flagged() {
        let schema = json!({
            "type": "object",
            "properties": {
                "format": { "type": "string" },
                "pattern": { "type": "string" },
                "mode": { "enum": ["format", "pattern"] }
            },
            "$defs": { "format": { "type": "integer" } }
        });
        let mut out = Vec::new();
        collect_unsupported_constraints(&schema, "", &mut out);
        assert!(out.is_empty(), "unexpected flags: {out:?}");
    }

    #[test]
    fn a_real_pattern_under_a_property_is_still_flagged() {
        let schema = json!({
            "properties": { "format": { "type": "string", "pattern": "^a$" } }
        });
        let mut out = Vec::new();
        collect_unsupported_constraints(&schema, "", &mut out);
        assert_eq!(out, vec!["/properties/format/pattern".to_string()]);
    }

    #[test]
    fn strict_mode_rejects_unsupported_constraints() {
        let schema = json!({
            "type": "object",
            "properties": {
                "id": {
                    "type": "string",
                    "pattern": "^[a-z]+$"
                }
            }
        });
        let issues = validate_json_instance(&schema, &json!({"id": "abc"}), true);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "unsupported_schema_constraint");
    }

    #[test]
    fn valid_instance_returns_no_issues() {
        let schema = json!({
            "type": "object",
            "required": ["message"],
            "properties": {
                "message": { "type": "string" }
            },
            "additionalProperties": false
        });
        let issues = validate_json_instance(&schema, &json!({"message": "ok"}), true);
        assert!(issues.is_empty());
    }

    #[test]
    fn invalid_instance_reports_schema_issue() {
        let schema = json!({
            "type": "object",
            "required": ["message"],
            "properties": {
                "message": { "type": "string" }
            },
            "additionalProperties": false
        });
        let issues = validate_json_instance(&schema, &json!({"message": 42}), true);
        assert!(!issues.is_empty());
        assert_eq!(issues[0].code, "schema_validation");
    }
}
