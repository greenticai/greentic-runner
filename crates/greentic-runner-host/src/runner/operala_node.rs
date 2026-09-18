//! Bridges an `operala.call` flow node into the in-process deep-worker
//! runtime (`greentic-dw-operala-invoker`), mirroring `agent_node`'s
//! [`AgentNodeHandler`](crate::runner::agent_node::AgentNodeHandler) seam for
//! `dw.agent`.
//!
//! The trait itself is unconditional (like `AgentNodeHandler`) so the engine
//! can hold `Option<Arc<dyn OperalaNodeHandler>>` regardless of build
//! features; the concrete [`RuntimeOperalaNodeHandler`] impl (wrapping
//! `DeepWorkerInvoker`) is feature-gated behind `desktop-agent-ephemeral` —
//! the same feature the designer's offline Test-chat sidecar already builds
//! with — so `operala.call` nodes run with NO NATS in that build. Server
//! builds without that feature keep the existing NATS
//! `RemoteDispatchHandler` fallback (`execute_remote_dispatch`) untouched.

use anyhow::Result;
use serde_json::Value;

/// Bridges an `operala.call` flow node into an in-process deep-worker
/// runtime. The engine holds this as a trait object so `engine.rs` stays
/// free of deep-worker construction details, exactly like
/// [`AgentNodeHandler`](crate::runner::agent_node::AgentNodeHandler).
#[async_trait::async_trait]
pub trait OperalaNodeHandler: Send + Sync {
    /// Execute one deep-worker dispatch. `target` is the node's routing
    /// target; `operation` and `input` come from the node's rendered payload
    /// (the same `{await, operation, input}` contract
    /// [`FlowEngine::execute_remote_dispatch`](super::engine) parses for the
    /// NATS path — `operation` must be `""` or `"run"`). Returns the node
    /// output JSON.
    async fn execute(
        &self,
        tenant: &str,
        env: &str,
        target: &str,
        operation: &str,
        session_id: &str,
        input: &Value,
    ) -> Result<Value>;
}

/// Resolve the deep-worker's `(provider, model)` for an `operala.call`
/// dispatch. The worker's own binding — `input.llm.{provider,model}`, stamped
/// into the node by `greentic-dw-authoring` — wins; otherwise the process-level
/// env fallback (`GREENTIC_LLM_PROVIDER`/`_MODEL`) is used. When NEITHER is
/// available the call errors instead of guessing a provider: a wrong default
/// silently sends the key to the wrong API (e.g. a DeepSeek key to OpenAI → 401).
///
/// Its only production caller is `dw::RuntimeOperalaNodeHandler::build_invoker`
/// below, which lives in the `desktop-agent-ephemeral`-gated `mod dw`; cfg-gate
/// this the same way (plus `test`, for the unit tests at the bottom of this
/// file) so it isn't flagged dead in builds where that feature is off (e.g. a
/// lean `--no-default-features --features verify` build).
#[cfg(any(feature = "desktop-agent-ephemeral", test))]
fn resolve_operala_provider_model(
    input: &Value,
    fallback_provider: Option<&str>,
    fallback_model: Option<&str>,
) -> Result<(String, String)> {
    let node_llm = input.get("llm");
    let node_field = |name: &str| -> Option<String> {
        node_llm
            .and_then(|llm| llm.get(name))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let clean_fallback = |value: Option<&str>| -> Option<String> {
        value
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let provider = node_field("provider").or_else(|| clean_fallback(fallback_provider));
    let model = node_field("model").or_else(|| clean_fallback(fallback_model));
    match (provider, model) {
        (Some(provider), Some(model)) => Ok((provider, model)),
        _ => Err(anyhow::anyhow!(
            "worker LLM config missing: operala.call node carries no \
             `input.llm.{{provider,model}}` and GREENTIC_LLM_PROVIDER/_MODEL are unset"
        )),
    }
}

// ---------------------------------------------------------------------------
// desktop-agent-ephemeral feature: DeepWorkerInvoker-backed handler
// ---------------------------------------------------------------------------

#[cfg(feature = "desktop-agent-ephemeral")]
mod dw {
    use std::sync::Arc;

    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use greentic_dw_operala_bridge::OperalaDispatchInvoker;
    use greentic_dw_operala_invoker::DeepWorkerInvoker;
    use serde_json::Value;

    use super::{OperalaNodeHandler, resolve_operala_provider_model};

    /// Production [`OperalaNodeHandler`]: builds the deep-worker's LLM from the
    /// WORKER's own `input.llm` provider/model binding, resolved per dispatch,
    /// and runs the `DeepLoopCoordinator` in-process via [`DeepWorkerInvoker`]
    /// (the invoker itself runs it on `tokio::task::spawn_blocking`) — no NATS
    /// transport, no `greentic-dw-operala-bridge` wire hop involved. Holding
    /// only the credential material (not a pre-built invoker) lets one sidecar
    /// serve workers bound to different providers/models.
    pub struct RuntimeOperalaNodeHandler {
        api_key: String,
        base_url: Option<String>,
        fallback_provider: Option<String>,
        fallback_model: Option<String>,
    }

    impl RuntimeOperalaNodeHandler {
        pub fn new(
            api_key: String,
            base_url: Option<String>,
            fallback_provider: Option<String>,
            fallback_model: Option<String>,
        ) -> Self {
            Self {
                api_key,
                base_url,
                fallback_provider,
                fallback_model,
            }
        }

        /// Build a [`DeepWorkerInvoker`] whose LLM provider/model come from the
        /// dispatched node's `input.llm` (else the env fallback). Errors when
        /// neither is present rather than guessing a provider.
        fn build_invoker(&self, input: &Value) -> Result<DeepWorkerInvoker> {
            let (provider, model) = resolve_operala_provider_model(
                input,
                self.fallback_provider.as_deref(),
                self.fallback_model.as_deref(),
            )?;
            let provider_kind: greentic_llm::ProviderKind = provider.parse().map_err(|_| {
                anyhow::anyhow!("unknown operala LLM provider '{provider}' (from worker config)")
            })?;
            // `Credential` is `ZeroizeOnDrop` (implements Drop), so struct-update
            // syntax cannot move out of `Default::default()` — build a default
            // and set the fields we have (mirrors the dw.agent backend).
            #[allow(clippy::field_reassign_with_default)]
            let credential = {
                let mut credential = greentic_llm::Credential::default();
                credential.api_key = self.api_key.clone();
                credential.base_url = self.base_url.clone();
                credential
            };
            let backend = greentic_llm::RigBackend::new(provider_kind, &model, &credential)
                .with_context(|| {
                    format!("building operala LLM provider '{provider}' model '{model}'")
                })?;
            let llm: Arc<dyn greentic_llm::LlmProvider> = Arc::new(backend);
            Ok(DeepWorkerInvoker::new(llm))
        }
    }

    #[async_trait]
    impl OperalaNodeHandler for RuntimeOperalaNodeHandler {
        async fn execute(
            &self,
            tenant: &str,
            env: &str,
            target: &str,
            operation: &str,
            session_id: &str,
            input: &Value,
        ) -> Result<Value> {
            let invoker = self.build_invoker(input)?;
            let idempotency_key = (!session_id.trim().is_empty()).then_some(session_id);
            let outcome = invoker
                .invoke(
                    tenant,
                    env,
                    target,
                    operation,
                    input.clone(),
                    idempotency_key,
                )
                .await
                .with_context(|| format!("in-process operala dispatch to '{target}' failed"))?;
            // Mirror the NATS `operala.call` response shape closely enough for
            // flow templates to read `{{node.reply}}`/`{{node.output}}`
            // regardless of dispatch mode: `reply` is the deep-worker's
            // `output.reply` when present, else the raw `output`.
            let reply = outcome
                .output
                .get("reply")
                .cloned()
                .unwrap_or_else(|| outcome.output.clone());
            Ok(serde_json::json!({
                "ok": outcome.ok,
                "reply": reply,
                "output": outcome.output,
            }))
        }
    }
}

#[cfg(feature = "desktop-agent-ephemeral")]
pub use dw::RuntimeOperalaNodeHandler;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolve_provider_model_prefers_node_llm() {
        let input = json!({ "llm": { "provider": "deepseek", "model": "deepseek-chat" } });
        let got = resolve_operala_provider_model(&input, None, None)
            .expect("node input.llm must resolve provider/model");
        assert_eq!(got, ("deepseek".to_string(), "deepseek-chat".to_string()));
    }

    #[test]
    fn resolve_provider_model_falls_back_to_env_when_node_absent() {
        let input = json!({ "user_text": "hi" });
        let got = resolve_operala_provider_model(&input, Some("openai"), Some("gpt-4o-mini"))
            .expect("env fallback must resolve when node carries no llm");
        assert_eq!(got, ("openai".to_string(), "gpt-4o-mini".to_string()));
    }

    #[test]
    fn resolve_provider_model_errors_when_neither_node_nor_env() {
        let input = json!({ "user_text": "hi" });
        let err = resolve_operala_provider_model(&input, None, None)
            .expect_err("missing worker LLM config must error, never guess a provider");
        assert!(
            err.to_string().contains("worker LLM config missing"),
            "error must name the missing worker LLM config, got: {err}"
        );
    }
}
