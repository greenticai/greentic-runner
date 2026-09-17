//! Runner-host implementation of `greentic_aw_runtime::ComponentInvoker`.
//!
//! Exposes the components of the loaded packs to an agentic worker as LLM tools
//! and invokes them over the canonical `PackRuntime` component host — the same
//! instantiation / WASI / secrets path a normal flow `component.*` node uses.
//! This is the runner-host half of the `component:` tool seam; the aw-runtime
//! half (`ComponentToolSource`/`ComponentToolCatalog`) depends only on the
//! trait + JSON, so this is where `PackRuntime`/`wasmtime` enter.
//!
//! The runtime is built per operator config, so the operator `tenant` is
//! captured at construction and stamped on the component exec context.

#![cfg(feature = "agentic-worker")]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use greentic_aw_runtime::{ComponentInvoker, ComponentOperation};
use serde_json::Value;

use crate::component_api::node::{ExecCtx as ComponentExecCtx, TenantCtx as ComponentTenantCtx};
use crate::pack::PackRuntime;
use crate::runner::invocation::{InvocationMeta, build_invocation_envelope};

/// Synthetic flow id stamped on component tool invocations so the host
/// component runtime sees a stable origin for an agentic-worker tool call.
const COMPONENT_TOOL_FLOW_ID: &str = "dw.agent";

/// `ComponentInvoker` backed by the loaded packs' component host.
pub struct PackRuntimeComponentInvoker {
    packs: Vec<Arc<PackRuntime>>,
    tenant: String,
    env: String,
}

impl PackRuntimeComponentInvoker {
    /// Construct over the operator's loaded packs + tenant. `GREENTIC_ENV`
    /// (default `local`) selects the invocation env, matching `PackRuntime`.
    pub fn new(packs: Vec<Arc<PackRuntime>>, tenant: String) -> Self {
        let env = std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_string());
        Self { packs, tenant, env }
    }
}

/// Synthesize an LLM-facing description (the component manifest carries no
/// per-operation prose today).
fn describe_operation(component_ref: &str, operation: &str) -> String {
    format!("Invoke operation '{operation}' of greentic component '{component_ref}'.")
}

/// Map a component's manifest operations to LLM-facing tool operations.
fn map_operations(
    component_ref: &str,
    operations: &[greentic_types::ComponentOperation],
) -> Vec<ComponentOperation> {
    operations
        .iter()
        .map(|op| ComponentOperation {
            component_ref: component_ref.to_string(),
            operation: op.name.clone(),
            description: describe_operation(component_ref, &op.name),
            parameters: op.input_schema.clone(),
        })
        .collect()
}

/// Build the component-host exec context for an agentic-worker tool call.
fn build_exec_ctx(tenant: &str, component_ref: &str) -> ComponentExecCtx {
    ComponentExecCtx {
        tenant: ComponentTenantCtx {
            tenant: tenant.to_string(),
            team: None,
            user: None,
            trace_id: None,
            i18n_id: None,
            correlation_id: None,
            deadline_unix_ms: None,
            attempt: 1,
            idempotency_key: None,
        },
        i18n_id: None,
        flow_id: COMPONENT_TOOL_FLOW_ID.to_string(),
        node_id: Some(component_ref.to_string()),
    }
}

/// Wrap the LLM-supplied JSON args into the component invocation envelope the
/// host runtime expects (mirrors the flow-engine component callsite).
fn build_invocation_input(
    env: &str,
    tenant: &str,
    component_ref: &str,
    operation: &str,
    args_json: &str,
) -> Result<String, String> {
    let payload: Value = serde_json::from_str(args_json)
        .map_err(|e| format!("invalid component tool arguments: {e}"))?;
    let meta = InvocationMeta {
        env,
        tenant,
        flow_id: COMPONENT_TOOL_FLOW_ID,
        node_id: Some(component_ref),
        provider_id: None,
        session_id: None,
        attempt: 1,
    };
    let envelope = build_invocation_envelope(meta, operation, payload)
        .map_err(|e| format!("build invocation envelope: {e}"))?;
    serde_json::to_string(&envelope).map_err(|e| format!("encode invocation envelope: {e}"))
}

impl ComponentInvoker for PackRuntimeComponentInvoker {
    fn list_operations(&self) -> Vec<ComponentOperation> {
        let mut out = Vec::new();
        for pack in &self.packs {
            for (component_ref, manifest) in pack.component_manifest_entries() {
                out.extend(map_operations(component_ref, &manifest.operations));
            }
        }
        out
    }

    fn invoke<'a>(
        &'a self,
        component_ref: &'a str,
        operation: &'a str,
        args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let Some(pack) = self
                .packs
                .iter()
                .find(|p| p.contains_component(component_ref))
            else {
                return Err(format!(
                    "component '{component_ref}' not found in any loaded pack"
                ));
            };
            let exec_ctx = build_exec_ctx(&self.tenant, component_ref);
            let input_json = build_invocation_input(
                &self.env,
                &self.tenant,
                component_ref,
                operation,
                args_json,
            )?;
            pack.invoke_component(component_ref, exec_ctx, operation, None, input_json)
                .await
                .map_err(|e| format!("component '{component_ref}' invoke failed: {e}"))
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    fn gt_op(name: &str, input_schema: Value) -> greentic_types::ComponentOperation {
        greentic_types::ComponentOperation {
            name: name.to_string(),
            input_schema,
            output_schema: json!(null),
        }
    }

    #[test]
    fn describe_operation_names_component_and_operation() {
        let d = describe_operation("greentic.refund", "issue_refund");
        assert!(d.contains("greentic.refund"), "got: {d}");
        assert!(d.contains("issue_refund"), "got: {d}");
    }

    #[test]
    fn map_operations_projects_name_and_input_schema() {
        let params =
            json!({ "type": "object", "properties": { "order_id": { "type": "string" } } });
        let ops = vec![
            gt_op("issue_refund", params.clone()),
            gt_op("lookup_order", json!({ "type": "object" })),
        ];
        let mapped = map_operations("greentic.refund", &ops);

        assert_eq!(mapped.len(), 2);
        let refund = mapped
            .iter()
            .find(|o| o.operation == "issue_refund")
            .expect("issue_refund mapped");
        assert_eq!(refund.component_ref, "greentic.refund");
        assert_eq!(refund.parameters, params);
        assert!(!refund.description.is_empty());
    }

    #[test]
    fn build_exec_ctx_stamps_tenant_and_flow() {
        let ctx = build_exec_ctx("acme", "greentic.refund");
        assert_eq!(ctx.tenant.tenant, "acme");
        assert_eq!(ctx.flow_id, COMPONENT_TOOL_FLOW_ID);
        assert_eq!(ctx.node_id.as_deref(), Some("greentic.refund"));
    }

    #[test]
    fn build_invocation_input_wraps_args_as_envelope() {
        let input = build_invocation_input(
            "local",
            "acme",
            "greentic.refund",
            "issue_refund",
            r#"{"order_id":"42"}"#,
        )
        .expect("envelope built");
        let parsed: Value = serde_json::from_str(&input).expect("valid json");
        assert_eq!(parsed["op"], json!("issue_refund"), "got: {parsed}");
        assert_eq!(
            parsed["flow_id"],
            json!(COMPONENT_TOOL_FLOW_ID),
            "got: {parsed}"
        );
    }

    #[test]
    fn build_invocation_input_rejects_bad_args() {
        let err = build_invocation_input("local", "acme", "c", "op", "not json")
            .expect_err("invalid args must error");
        assert!(
            err.contains("invalid component tool arguments"),
            "got: {err}"
        );
    }
}
