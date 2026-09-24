//! Billing for a deep worker's OWN reasoning-loop LLM calls.
//!
//! An in-process `operala.call` deep worker runs in
//! `greentic_dw_operala_invoker::DeepWorkerInvoker`, which is handed a raw
//! `greentic_llm::LlmProvider` and never goes through
//! `greentic_aw_runtime::AgentRuntime` — so none of its planning, execution,
//! reflection or reply-writing calls reached a `BillingMeter`. The invoker
//! exposes no usage of its own, and it does not need to: every one of those
//! calls goes through the provider it was handed, and a
//! `greentic_llm::ChatResponse` carries the provider-reported `usage`. So the
//! handler wraps the provider in [`MeteredLlmProvider`], which emits one event
//! per completed `chat` through the same sink `dw.agent` uses.
//!
//! What it cannot see: `chat_stream` (the invoker never calls it; passed
//! through unmetered) and a response whose backend reported no usage (the rig
//! backend always does — a `None` is skipped rather than recorded as zero, so
//! a zero in the ledger always means the provider said zero).

use std::sync::Arc;

use async_trait::async_trait;
use greentic_aw_runtime::billing::BillingMeter;
use greentic_aw_runtime::tenant::TenantContext;
use greentic_llm::{Capabilities, ChatRequest, ChatResponse, ChatStream, LlmError, LlmProvider};
use serde_json::Value;

/// An [`LlmProvider`] that bills every completed `chat` through `meter`.
pub(crate) struct MeteredLlmProvider {
    inner: Arc<dyn LlmProvider>,
    meter: Arc<dyn BillingMeter>,
    tenant: TenantContext,
    agent_id: String,
}

impl MeteredLlmProvider {
    pub(crate) fn new(
        inner: Arc<dyn LlmProvider>,
        meter: Arc<dyn BillingMeter>,
        tenant: TenantContext,
        agent_id: String,
    ) -> Self {
        Self {
            inner,
            meter,
            tenant,
            agent_id,
        }
    }
}

#[async_trait]
impl LlmProvider for MeteredLlmProvider {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn provider_name(&self) -> &'static str {
        self.inner.provider_name()
    }

    fn model(&self) -> &str {
        self.inner.model()
    }

    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
        let response = self.inner.chat(req).await?;
        if let Some(usage) = &response.usage {
            // The model the provider SAYS it used, else the one configured.
            let model = if usage.model.trim().is_empty() {
                self.inner.model()
            } else {
                usage.model.as_str()
            };
            // `emit` is fire-and-forget by contract; an error is still only
            // ever a warning — billing never fails a deep worker's step.
            if let Err(error) = self
                .meter
                .emit(
                    &self.tenant,
                    usage.input_tokens,
                    usage.output_tokens,
                    &self.agent_id,
                    model,
                )
                .await
            {
                tracing::warn!(%error, agent_id = %self.agent_id, "deep-worker usage not recorded");
            }
        }
        Ok(response)
    }

    async fn chat_stream(&self, req: ChatRequest) -> Result<ChatStream, LlmError> {
        self.inner.chat_stream(req).await
    }
}

/// The agent id a deep worker's usage is attributed to: the node's
/// `input.agent_id` (what greentic-dw-authoring stamps), else the dispatch
/// target, else the operation. Descriptive only — the unit, not the agent,
/// is what `project_id` groups by.
pub(crate) fn deep_worker_agent_id(input: &Value, target: &str, operation: &str) -> String {
    let stamped = input
        .get("agent_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty());
    stamped
        .or_else(|| Some(target.trim()).filter(|t| !t.is_empty()))
        .unwrap_or_else(|| operation.trim())
        .to_string()
}

#[cfg(test)]
#[path = "metered_llm_tests.rs"]
mod tests;
