use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::pack::PackRuntime;

#[derive(Debug, Clone)]
pub struct IntrospectedContract {
    pub selected_operation: String,
    pub input_schema: Value,
    pub output_schema: Value,
    pub config_schema: Value,
    pub describe_hash: String,
    pub schema_hash: String,
}

#[derive(Serialize)]
struct DescribeHashMaterial<'a> {
    component_ref: &'a str,
    operation: &'a str,
    world: &'a str,
    input_schema: &'a Value,
    output_schema: &'a Value,
}

#[derive(Serialize)]
struct SchemaHashMaterial<'a> {
    component_ref: &'a str,
    operation: &'a str,
    input_schema: &'a Value,
    output_schema: &'a Value,
    config_schema: &'a Value,
}

pub fn introspect_component_contract(
    pack: &PackRuntime,
    component_ref: &str,
    requested_operation: &str,
) -> Result<Option<IntrospectedContract>> {
    let Some(manifest) = pack.component_manifest(component_ref) else {
        return Ok(None);
    };
    if !manifest.world.contains("greentic:component@0.6.0") {
        return Ok(None);
    }

    let describe_payload = pack
        .describe_component_contract_v0_6(component_ref)?
        .ok_or_else(|| anyhow::anyhow!("component does not export 0.6 describe()"))?;

    let selected = select_operation_from_describe(&describe_payload, requested_operation)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "operation `{}` not present in 0.6 describe payload for component `{}`",
                requested_operation,
                component_ref
            )
        })?;

    let input_schema = extract_operation_schema(&describe_payload, &selected, "input")
        .map(canonicalize_json)
        .unwrap_or(Value::Null);
    let output_schema = extract_operation_schema(&describe_payload, &selected, "output")
        .map(canonicalize_json)
        .unwrap_or(Value::Null);
    let config_schema = extract_config_schema(&describe_payload)
        .map(canonicalize_json)
        .unwrap_or(Value::Null);

    let describe_material = DescribeHashMaterial {
        component_ref,
        operation: selected.as_str(),
        world: manifest.world.as_str(),
        input_schema: &input_schema,
        output_schema: &output_schema,
    };
    let describe_hash = sha256_prefixed(
        &serde_cbor::to_vec(&describe_material)
            .context("failed to serialise describe hash material")?,
    );

    let schema_material = SchemaHashMaterial {
        component_ref,
        operation: selected.as_str(),
        input_schema: &input_schema,
        output_schema: &output_schema,
        config_schema: &config_schema,
    };
    let schema_hash = sha256_prefixed(
        &serde_cbor::to_vec(&schema_material)
            .context("failed to serialise schema hash material")?,
    );

    Ok(Some(IntrospectedContract {
        selected_operation: selected,
        input_schema,
        output_schema,
        config_schema,
        describe_hash,
        schema_hash,
    }))
}

fn select_operation_from_describe(payload: &Value, requested_operation: &str) -> Option<String> {
    let ops = payload.get("operations")?.as_array()?;
    // Search EVERY operation for the requested name, then for `run`, then fall
    // back to the first one. (`find_map(..).filter(..)` would only ever have
    // inspected the first operation.)
    let named = |wanted: &str| {
        ops.iter()
            .filter_map(operation_name)
            .find(|name| *name == wanted)
            .map(ToString::to_string)
    };
    named(requested_operation)
        .or_else(|| named("run"))
        .or_else(|| {
            ops.first()
                .and_then(operation_name)
                .map(ToString::to_string)
        })
}

fn operation_name(value: &Value) -> Option<&str> {
    value
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| value.get("name").and_then(Value::as_str))
}

fn extract_operation_schema(payload: &Value, operation: &str, side: &str) -> Option<Value> {
    let ops = payload.get("operations")?.as_array()?;
    let op = ops
        .iter()
        .find(|entry| operation_name(entry) == Some(operation))?;
    let side_obj = op.get(side)?;
    side_obj
        .get("schema")
        .cloned()
        .or_else(|| op.get(format!("{side}_schema")).cloned())
}

fn extract_config_schema(payload: &Value) -> Option<Value> {
    payload.get("config_schema").cloned()
}

pub(crate) fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!("sha256:{}", to_hex(&digest))
}

fn to_hex(digest: &[u8]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut ordered = serde_json::Map::new();
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                let normalized = map
                    .get(&key)
                    .cloned()
                    .map(canonicalize_json)
                    .unwrap_or(Value::Null);
                ordered.insert(key, normalized);
            }
            Value::Object(ordered)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonicalize_json).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn selects_the_requested_operation_even_when_it_is_not_first() {
        let payload =
            json!({ "operations": [{ "id": "alpha" }, { "id": "beta" }, { "id": "run" }] });
        assert_eq!(
            select_operation_from_describe(&payload, "beta").as_deref(),
            Some("beta")
        );
    }

    #[test]
    fn falls_back_to_run_then_to_the_first_operation() {
        let with_run = json!({ "operations": [{ "id": "alpha" }, { "name": "run" }] });
        assert_eq!(
            select_operation_from_describe(&with_run, "missing").as_deref(),
            Some("run")
        );
        let without_run = json!({ "operations": [{ "id": "alpha" }, { "id": "beta" }] });
        assert_eq!(
            select_operation_from_describe(&without_run, "missing").as_deref(),
            Some("alpha")
        );
    }
}
