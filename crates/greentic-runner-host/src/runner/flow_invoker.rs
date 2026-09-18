//! Runner-host implementation of `greentic_aw_runtime::FlowInvoker`.
//!
//! Exposes same-pack flows to an agentic worker as LLM tools and invokes them
//! via `PackRuntime::run_flow_for_tool`, which guarantees non-interactive
//! (non-pausing) execution. This is the runner-host half of the `flow:` tool
//! seam; the aw-runtime half (`FlowToolSource`/`FlowToolCatalog`) depends only
//! on the trait + JSON, so this is where `PackRuntime` enters.

#![cfg(feature = "agentic-worker")]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use greentic_aw_runtime::{FlowInvoker, FlowOperation};

use crate::pack::PackRuntime;

/// `FlowInvoker` backed by the operator's loaded packs. Resolves a `flow_ref`
/// to a same-pack flow and runs it non-interactively via
/// `PackRuntime::run_flow_for_tool`.
pub struct PackRuntimeFlowInvoker {
    packs: Vec<Arc<PackRuntime>>,
    #[allow(dead_code)]
    tenant: String,
}

impl PackRuntimeFlowInvoker {
    /// Construct over the operator's loaded packs and tenant identifier.
    pub fn new(packs: Vec<Arc<PackRuntime>>, tenant: String) -> Self {
        Self { packs, tenant }
    }
}

impl FlowInvoker for PackRuntimeFlowInvoker {
    /// List every flow exposed as an agent tool. Uses the sync
    /// `PackRuntime::flow_descriptors()` accessor so this fn can remain
    /// synchronous without risking a `block_on`-in-async-context panic.
    fn list_flows(&self) -> Vec<FlowOperation> {
        let mut out = Vec::new();
        for pack in &self.packs {
            for descriptor in pack.flow_descriptors() {
                out.push(FlowOperation {
                    flow_ref: descriptor.id.clone(),
                    description: descriptor
                        .description
                        .clone()
                        .unwrap_or_else(|| descriptor.id.clone()),
                    // Full input-schema introspection is not available via a
                    // cheap sync path in this slice — use the open-object
                    // fallback. Tracked for a follow-up once a sync schema
                    // accessor exists.
                    parameters: serde_json::json!({ "type": "object" }),
                });
            }
        }
        out
    }

    fn invoke<'a>(
        &'a self,
        flow_ref: &'a str,
        args_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let input: serde_json::Value = serde_json::from_str(args_json)
                .map_err(|e| format!("invalid JSON args for flow '{flow_ref}': {e}"))?;

            for pack in &self.packs {
                // Use the sync descriptor list for pack-membership check so we
                // do not await inside a linear search (the pack that owns the
                // flow_ref is also the one we run it on).
                let owns = pack.flow_descriptors().iter().any(|d| d.id == flow_ref);
                if owns {
                    return pack.run_flow_for_tool(flow_ref, input).await;
                }
            }

            Err(format!("flow '{flow_ref}' not found in any loaded pack"))
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invoke_unknown_flow_returns_not_found_error() {
        let invoker = PackRuntimeFlowInvoker::new(Vec::new(), "acme".into());
        let out = invoker.invoke("nope", "{}").await;
        assert!(
            out.is_err(),
            "unknown flow must Err (folded to error value by the catalog)"
        );
        assert!(
            out.unwrap_err().contains("nope"),
            "error message must name the missing flow_ref"
        );
    }

    #[tokio::test]
    async fn list_flows_returns_empty_for_no_packs() {
        let invoker = PackRuntimeFlowInvoker::new(Vec::new(), "acme".into());
        let ops = invoker.list_flows();
        assert!(ops.is_empty(), "no packs → no flows exposed");
    }

    #[tokio::test]
    async fn invoke_invalid_json_returns_descriptive_error() {
        let invoker = PackRuntimeFlowInvoker::new(Vec::new(), "acme".into());
        let out = invoker.invoke("some-flow", "not-json").await;
        assert!(out.is_err());
        let msg = out.unwrap_err();
        assert!(
            msg.contains("invalid JSON args"),
            "error should mention JSON parse failure, got: {msg}"
        );
    }
}
