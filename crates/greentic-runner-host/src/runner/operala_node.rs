//! Bridges an `operala.call` flow node into the in-process deep-worker
//! runtime (`greentic-dw-operala-invoker`), mirroring `agent_node`'s
//! [`AgentNodeHandler`](crate::runner::agent_node::AgentNodeHandler) seam for
//! `dw.agent`.
//!
//! The trait itself is unconditional (like `AgentNodeHandler`) so the engine
//! can hold `Option<Arc<dyn OperalaNodeHandler>>` regardless of build
//! features; the concrete [`RuntimeOperalaNodeHandler`] impl (wrapping
//! `DeepWorkerInvoker`) is feature-gated behind `operala-in-process` —
//! the same feature the designer's offline Test-chat sidecar already builds
//! with (via `desktop-agent-ephemeral`, which implies it) — so `operala.call`
//! nodes run with NO NATS in that build. Builds without that feature — and
//! any build with `GREENTIC_OPERALA_DISPATCH=nats` — keep the NATS
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
/// below, which lives in the `operala-in-process`-gated `mod dw`; cfg-gate
/// this the same way (plus `test`, for the unit tests at the bottom of this
/// file) so it isn't flagged dead in builds where that feature is off (e.g. a
/// lean `--no-default-features --features verify` build).
#[cfg(any(feature = "operala-in-process", test))]
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

/// Env var choosing how `operala.call` nodes dispatch in a build compiled with
/// `operala-in-process`. Mirrors `GREENTIC_AW_DISPATCH` for `dw.agent`.
#[cfg(feature = "operala-in-process")]
pub const OPERALA_DISPATCH_ENV: &str = "GREENTIC_OPERALA_DISPATCH";

/// Whether `operala.call` should run in-process, given the raw value of
/// [`OPERALA_DISPATCH_ENV`]. Only `nats` (any case, surrounding whitespace
/// ignored) opts out and keeps the NATS `RemoteDispatchHandler` path; unset or
/// any other value keeps the in-process default — the handler is then still
/// wired only when an LLM key resolves.
///
/// Pure over its argument so it is unit-testable without mutating the process
/// environment; cfg-gated like [`resolve_operala_provider_model`] so a lean
/// build does not flag it dead.
#[cfg(any(feature = "operala-in-process", test))]
#[must_use]
pub fn operala_dispatch_in_process(env_value: Option<&str>) -> bool {
    !env_value.is_some_and(|value| value.trim().eq_ignore_ascii_case("nats"))
}

// ---------------------------------------------------------------------------
// operala-in-process feature: DeepWorkerInvoker-backed handler
// ---------------------------------------------------------------------------

#[cfg(feature = "operala-in-process")]
mod dw {
    use std::sync::Arc;

    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use greentic_dw_operala_bridge::OperalaDispatchInvoker;
    use greentic_dw_operala_invoker::DeepWorkerInvoker;
    use serde_json::Value;

    use super::{OperalaNodeHandler, resolve_operala_provider_model};
    use greentic_dw_operala_invoker::DeepWorkerTools;

    use crate::runner::operala_tools::OperalaToolContext;

    /// Builds the deep worker's LLM from a resolved `(provider, model)`.
    /// Injectable so a test can drive the whole handler with a scripted model.
    type LlmFactory =
        Arc<dyn Fn(&str, &str) -> Result<Arc<dyn greentic_llm::LlmProvider>> + Send + Sync>;

    /// Production [`OperalaNodeHandler`]: builds the deep-worker's LLM from the
    /// WORKER's own `input.llm` provider/model binding, resolved per dispatch,
    /// and runs the `DeepLoopCoordinator` in-process via [`DeepWorkerInvoker`]
    /// (the invoker itself runs it on `tokio::task::spawn_blocking`) — no NATS
    /// transport, no `greentic-dw-operala-bridge` wire hop involved. Holding
    /// only the credential material (not a pre-built invoker) lets one sidecar
    /// serve workers bound to different providers/models.
    pub struct RuntimeOperalaNodeHandler {
        llm_factory: LlmFactory,
        fallback_provider: Option<String>,
        fallback_model: Option<String>,
        /// The worker's bound tools. `None` when this `TenantRuntime` built no
        /// `AgentRuntime` (no agents, or no state store) — the deep worker
        /// then runs tool-less, as it did before tools existed.
        tools: Option<Arc<OperalaToolContext>>,
        /// The `TenantRuntime`'s shared billing sink. `None` leaves the deep
        /// worker's own reasoning-loop calls unmetered (as before this field).
        billing_meter: Option<Arc<dyn greentic_aw_runtime::billing::BillingMeter>>,
        /// The deployed unit (`bundle_id`) those calls are attributed to.
        project_id: Option<String>,
    }

    impl RuntimeOperalaNodeHandler {
        pub fn new(
            api_key: String,
            base_url: Option<String>,
            fallback_provider: Option<String>,
            fallback_model: Option<String>,
        ) -> Self {
            let llm_factory: LlmFactory = Arc::new(move |provider: &str, model: &str| {
                rig_llm(&api_key, base_url.as_deref(), provider, model)
            });
            Self {
                llm_factory,
                fallback_provider,
                fallback_model,
                tools: None,
                billing_meter: None,
                project_id: None,
            }
        }

        /// Bill every LLM call a deep worker's own loop makes (plan, execute,
        /// reflect, reply) through `meter`, attributed to the deployed unit
        /// `project_id`. The invoker exposes no usage itself, so the handler
        /// wraps the provider it hands it — see [`crate::runner::metered_llm`].
        #[must_use]
        pub fn with_billing_meter(
            mut self,
            meter: Option<Arc<dyn greentic_aw_runtime::billing::BillingMeter>>,
            project_id: Option<String>,
        ) -> Self {
            self.billing_meter = meter;
            self.project_id = project_id;
            self
        }

        /// Replace the LLM construction, for tests that script the model.
        #[cfg(test)]
        pub(crate) fn with_llm_factory(mut self, llm_factory: LlmFactory) -> Self {
            self.llm_factory = llm_factory;
            self
        }

        /// Give deep workers access to their agent's bound tools.
        #[must_use]
        pub fn with_tool_context(mut self, tools: Option<Arc<OperalaToolContext>>) -> Self {
            self.tools = tools;
            self
        }

        /// Build a [`DeepWorkerInvoker`] whose LLM provider/model come from the
        /// dispatched node's `input.llm` (else the env fallback). Errors when
        /// neither is present rather than guessing a provider.
        fn build_invoker(
            &self,
            input: &Value,
            scope: &DispatchScope<'_>,
        ) -> Result<DeepWorkerInvoker> {
            let (provider, model) = resolve_operala_provider_model(
                input,
                self.fallback_provider.as_deref(),
                self.fallback_model.as_deref(),
            )?;
            let llm = (self.llm_factory)(&provider, &model)?;
            Ok(DeepWorkerInvoker::new(self.metered(llm, input, scope)))
        }

        /// Wrap the worker's LLM so every completed call is billed, when a
        /// billing sink is installed; otherwise hand it back untouched.
        pub(crate) fn metered(
            &self,
            llm: Arc<dyn greentic_llm::LlmProvider>,
            input: &Value,
            scope: &DispatchScope<'_>,
        ) -> Arc<dyn greentic_llm::LlmProvider> {
            let Some(meter) = &self.billing_meter else {
                return llm;
            };
            let tenant = greentic_aw_runtime::tenant::TenantContext::new(scope.tenant, scope.env)
                .with_project_id(self.project_id.clone());
            Arc::new(crate::runner::metered_llm::MeteredLlmProvider::new(
                llm,
                Arc::clone(meter),
                tenant,
                crate::runner::metered_llm::deep_worker_agent_id(
                    input,
                    scope.target,
                    scope.operation,
                ),
            ))
        }
    }

    /// Who an `operala.call` dispatch runs for, as far as billing cares.
    pub(crate) struct DispatchScope<'a> {
        pub(crate) tenant: &'a str,
        pub(crate) env: &'a str,
        pub(crate) target: &'a str,
        pub(crate) operation: &'a str,
    }

    /// The production LLM: a `greentic_llm::RigBackend` for the worker's
    /// provider/model over the resolved credential.
    fn rig_llm(
        api_key: &str,
        base_url: Option<&str>,
        provider: &str,
        model: &str,
    ) -> Result<Arc<dyn greentic_llm::LlmProvider>> {
        let provider_kind: greentic_llm::ProviderKind = provider.parse().map_err(|_| {
            anyhow::anyhow!("unknown operala LLM provider '{provider}' (from worker config)")
        })?;
        // `Credential` is `ZeroizeOnDrop` (implements Drop), so struct-update
        // syntax cannot move out of `Default::default()` — build a default
        // and set the fields we have (mirrors the dw.agent backend).
        #[allow(clippy::field_reassign_with_default)]
        let credential = {
            let mut credential = greentic_llm::Credential::default();
            credential.api_key = api_key.to_string();
            credential.base_url = base_url.map(str::to_string);
            credential
        };
        let backend = greentic_llm::RigBackend::new(provider_kind, model, &credential)
            .with_context(|| {
                format!("building operala LLM provider '{provider}' model '{model}'")
            })?;
        Ok(Arc::new(backend))
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
            let invoker = self.build_invoker(
                input,
                &DispatchScope {
                    tenant,
                    env,
                    target,
                    operation,
                },
            )?;
            let deep_worker_tools = match &self.tools {
                Some(ctx) => ctx.tools_for(tenant, env, target, operation, input).await,
                None => None,
            };
            // Resolved through the same AgentRuntime a dw.agent step uses.
            if let Some(tools) = &deep_worker_tools {
                tracing::debug!(
                    target,
                    tools = tools.list().len(),
                    "operala.call deep worker runs with its agent's tools"
                );
            }
            let invoker = invoker
                .with_tools(deep_worker_tools.map(|tools| tools as Arc<dyn DeepWorkerTools>));
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

    /// What the runtime should do with `operala.call` in this process.
    pub enum OperalaSelection {
        /// `GREENTIC_OPERALA_DISPATCH=nats`: keep the NATS `RemoteDispatchHandler`.
        Nats,
        /// In-process requested, but no LLM key resolved (env or store).
        NoKey,
        /// Wire this handler into the `FlowEngine`.
        InProcess(Arc<dyn OperalaNodeHandler>),
    }

    /// Decide how `operala.call` dispatches and, for the in-process path,
    /// build the handler. `resolve_key` runs only when the in-process path is
    /// selected, so `nats` never touches the secrets store.
    pub async fn select_operala_handler<F, Fut>(
        dispatch_env: Option<&str>,
        resolve_key: F,
        base_url: Option<String>,
        fallback_provider: Option<String>,
        fallback_model: Option<String>,
    ) -> OperalaSelection
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Option<String>>,
    {
        select_operala_handler_with_tools(
            dispatch_env,
            resolve_key,
            base_url,
            fallback_provider,
            fallback_model,
            None,
        )
        .await
    }

    /// [`select_operala_handler`], giving the in-process handler access to the
    /// deep workers' bound tools through `tools`.
    pub async fn select_operala_handler_with_tools<F, Fut>(
        dispatch_env: Option<&str>,
        resolve_key: F,
        base_url: Option<String>,
        fallback_provider: Option<String>,
        fallback_model: Option<String>,
        tools: Option<Arc<OperalaToolContext>>,
    ) -> OperalaSelection
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Option<String>>,
    {
        select_operala_handler_metered(
            dispatch_env,
            resolve_key,
            base_url,
            fallback_provider,
            fallback_model,
            tools,
            None,
            None,
        )
        .await
    }

    /// [`select_operala_handler_with_tools`], also billing the deep worker's
    /// own reasoning-loop calls through `billing_meter`, attributed to the
    /// deployed unit `project_id` (see
    /// [`RuntimeOperalaNodeHandler::with_billing_meter`]).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn select_operala_handler_metered<F, Fut>(
        dispatch_env: Option<&str>,
        resolve_key: F,
        base_url: Option<String>,
        fallback_provider: Option<String>,
        fallback_model: Option<String>,
        tools: Option<Arc<OperalaToolContext>>,
        billing_meter: Option<Arc<dyn greentic_aw_runtime::billing::BillingMeter>>,
        project_id: Option<String>,
    ) -> OperalaSelection
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Option<String>>,
    {
        if !super::operala_dispatch_in_process(dispatch_env) {
            return OperalaSelection::Nats;
        }
        match resolve_key().await {
            Some(api_key) => OperalaSelection::InProcess(Arc::new(
                RuntimeOperalaNodeHandler::new(
                    api_key,
                    base_url,
                    fallback_provider,
                    fallback_model,
                )
                .with_tool_context(tools)
                .with_billing_meter(billing_meter, project_id),
            )),
            None => OperalaSelection::NoKey,
        }
    }
}

#[cfg(feature = "operala-in-process")]
pub use dw::{
    OperalaSelection, RuntimeOperalaNodeHandler, select_operala_handler,
    select_operala_handler_with_tools,
};

#[cfg(feature = "operala-in-process")]
pub(crate) use dw::select_operala_handler_metered;

#[cfg(all(test, feature = "operala-in-process"))]
pub(crate) use dw::DispatchScope;

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

    #[test]
    fn operala_dispatch_defaults_to_in_process_when_unset() {
        assert!(operala_dispatch_in_process(None));
    }

    #[test]
    fn operala_dispatch_nats_forces_nats_case_insensitively() {
        assert!(!operala_dispatch_in_process(Some("nats")));
        assert!(!operala_dispatch_in_process(Some(" NATS ")));
        assert!(!operala_dispatch_in_process(Some("Nats")));
    }

    #[test]
    fn operala_dispatch_any_other_value_stays_in_process() {
        // Mirrors `dw_agent_dispatch_mode`: only `nats` opts out; a typo or an
        // explicit `inprocess` keeps the default, never silently disables it.
        assert!(operala_dispatch_in_process(Some("")));
        assert!(operala_dispatch_in_process(Some("inprocess")));
        assert!(operala_dispatch_in_process(Some("nat")));
    }
}

#[cfg(all(test, feature = "operala-in-process"))]
mod in_process_selection_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use serde_json::json;

    use super::{OperalaSelection, select_operala_handler};

    #[tokio::test]
    async fn nats_skips_wiring_and_never_resolves_a_key() {
        let resolved = Arc::new(AtomicBool::new(false));
        let probe = Arc::clone(&resolved);
        let selection = select_operala_handler(
            Some("NATS"),
            move || {
                probe.store(true, Ordering::SeqCst);
                async { Some("sk-test".to_string()) }
            },
            None,
            Some("openai".to_string()),
            Some("gpt-4o-mini".to_string()),
        )
        .await;
        assert!(matches!(selection, OperalaSelection::Nats));
        assert!(
            !resolved.load(Ordering::SeqCst),
            "GREENTIC_OPERALA_DISPATCH=nats must not read an LLM key from the secrets store"
        );
    }

    #[tokio::test]
    async fn unset_dispatch_with_a_key_wires_the_real_handler() {
        let selection = select_operala_handler(
            None,
            || async { Some("sk-test".to_string()) },
            None,
            None,
            None,
        )
        .await;
        let OperalaSelection::InProcess(handler) = selection else {
            panic!("a resolved key with dispatch unset must wire the in-process handler");
        };
        // It is the DeepWorkerInvoker-backed handler: with no node `input.llm`
        // and no fallback it refuses before any network call, naming the gap.
        let err = handler
            .execute("demo", "local", "w1", "run", "s1", &json!({ "goal": "hi" }))
            .await
            .expect_err("no provider/model anywhere must error, never guess");
        assert!(
            err.to_string().contains("worker LLM config missing"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn other_dispatch_values_with_a_key_stay_in_process() {
        let selection = select_operala_handler(
            Some("inprocess"),
            || async { Some("sk-test".to_string()) },
            None,
            Some("openai".to_string()),
            Some("gpt-4o-mini".to_string()),
        )
        .await;
        assert!(matches!(selection, OperalaSelection::InProcess(_)));
    }

    #[tokio::test]
    async fn no_key_leaves_the_nats_fallback() {
        let selection = select_operala_handler(None, || async { None }, None, None, None).await;
        assert!(matches!(selection, OperalaSelection::NoKey));
    }
}
