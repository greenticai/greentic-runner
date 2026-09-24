#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use greentic_aw_runtime::billing::BillingError;
use greentic_llm::{FinishReason, Usage};
use serde_json::json;

use super::*;

/// (tenant_id, env_id, project_id, tokens_in, tokens_out, agent_id, model)
type Emit = (String, String, Option<String>, u64, u64, String, String);

#[derive(Default)]
struct RecordingMeter {
    calls: Mutex<Vec<Emit>>,
}

impl BillingMeter for RecordingMeter {
    fn emit<'a>(
        &'a self,
        tenant: &'a TenantContext,
        input_tokens: u64,
        output_tokens: u64,
        agent_id: &'a str,
        model: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), BillingError>> + Send + 'a>> {
        self.calls.lock().unwrap().push((
            tenant.tenant_id.clone(),
            tenant.env_id.clone(),
            tenant.project_id.clone(),
            input_tokens,
            output_tokens,
            agent_id.to_string(),
            model.to_string(),
        ));
        Box::pin(async { Ok(()) })
    }

    fn over_budget<'a>(
        &'a self,
        _tenant: &'a TenantContext,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(std::future::ready(false))
    }
}

/// Answers every `chat` with `reply`, reporting `usage`; `fail` makes it error.
struct StubLlm {
    usage: Option<Usage>,
    fail: bool,
}

#[async_trait]
impl LlmProvider for StubLlm {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            chat: true,
            tools: true,
            streaming: false,
            vision: false,
            system_prompt: true,
        }
    }

    fn provider_name(&self) -> &'static str {
        "stub"
    }

    fn model(&self) -> &str {
        "configured-model"
    }

    async fn chat(&self, _req: ChatRequest) -> Result<ChatResponse, LlmError> {
        if self.fail {
            return Err(LlmError::Config("boom".into()));
        }
        Ok(ChatResponse {
            content: "ok".into(),
            tool_calls: vec![],
            finish_reason: FinishReason::Stop,
            usage: self.usage.clone(),
        })
    }

    async fn chat_stream(&self, _req: ChatRequest) -> Result<ChatStream, LlmError> {
        Err(LlmError::Config("no stream".into()))
    }
}

fn request() -> ChatRequest {
    ChatRequest {
        messages: vec![],
        tools: vec![],
        tool_choice: None,
        max_tokens: None,
        temperature: None,
    }
}

fn metered(llm: StubLlm, meter: Arc<RecordingMeter>) -> MeteredLlmProvider {
    MeteredLlmProvider::new(
        Arc::new(llm),
        meter,
        TenantContext::new("acme", "prod").with_project_id(Some("support-bot".into())),
        "researcher".into(),
    )
}

#[tokio::test]
async fn a_completed_chat_is_billed_once_with_the_reported_usage() {
    let meter = Arc::new(RecordingMeter::default());
    let llm = metered(
        StubLlm {
            usage: Some(Usage {
                model: "gpt-4o-mini-2024".into(),
                input_tokens: 40,
                output_tokens: 9,
            }),
            fail: false,
        },
        Arc::clone(&meter),
    );
    let response = llm.chat(request()).await.expect("chat passes through");
    assert_eq!(response.content, "ok");

    let calls = meter.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "one completed chat → one billing emit");
    assert_eq!(
        calls[0],
        (
            "acme".into(),
            "prod".into(),
            Some("support-bot".into()),
            40,
            9,
            "researcher".into(),
            "gpt-4o-mini-2024".into(),
        )
    );
}

#[tokio::test]
async fn a_blank_reported_model_falls_back_to_the_configured_one() {
    let meter = Arc::new(RecordingMeter::default());
    let llm = metered(
        StubLlm {
            usage: Some(Usage {
                model: " ".into(),
                input_tokens: 1,
                output_tokens: 2,
            }),
            fail: false,
        },
        Arc::clone(&meter),
    );
    llm.chat(request()).await.unwrap();
    assert_eq!(meter.calls.lock().unwrap()[0].6, "configured-model");
}

#[tokio::test]
async fn no_reported_usage_records_nothing_rather_than_a_zero() {
    let meter = Arc::new(RecordingMeter::default());
    let llm = metered(
        StubLlm {
            usage: None,
            fail: false,
        },
        Arc::clone(&meter),
    );
    llm.chat(request()).await.unwrap();
    assert!(meter.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_failed_chat_is_propagated_and_not_billed() {
    let meter = Arc::new(RecordingMeter::default());
    let llm = metered(
        StubLlm {
            usage: None,
            fail: true,
        },
        Arc::clone(&meter),
    );
    assert!(llm.chat(request()).await.is_err());
    assert!(meter.calls.lock().unwrap().is_empty());
}

#[test]
fn the_wrapper_reports_the_inner_providers_identity() {
    let meter = Arc::new(RecordingMeter::default());
    let llm = metered(
        StubLlm {
            usage: None,
            fail: false,
        },
        meter,
    );
    assert_eq!(llm.provider_name(), "stub");
    assert_eq!(llm.model(), "configured-model");
    // The invoker gates its tool loop on this: wrapping must not hide tools.
    assert!(llm.capabilities().tools);
}

#[test]
fn the_agent_id_prefers_the_stamped_one() {
    assert_eq!(
        deep_worker_agent_id(&json!({"agent_id": " writer "}), "t", "run"),
        "writer"
    );
    assert_eq!(
        deep_worker_agent_id(&json!({"agent_id": "  "}), "target", "run"),
        "target"
    );
    assert_eq!(deep_worker_agent_id(&json!({}), " ", "planner"), "planner");
}

// ── the operala.call handler wires it ───────────────────────────────────────

use crate::runner::operala_node::{DispatchScope, RuntimeOperalaNodeHandler};

fn scope() -> DispatchScope<'static> {
    DispatchScope {
        tenant: "acme",
        env: "prod",
        target: "researcher",
        operation: "run",
    }
}

#[tokio::test]
async fn the_handler_bills_the_deep_workers_llm_through_its_installed_sink() {
    let meter = Arc::new(RecordingMeter::default());
    let handler = RuntimeOperalaNodeHandler::new("unused".into(), None, None, None)
        .with_billing_meter(
            Some(Arc::clone(&meter) as Arc<dyn BillingMeter>),
            Some("support-bot".into()),
        );
    let llm = handler.metered(
        Arc::new(StubLlm {
            usage: Some(Usage {
                model: "m".into(),
                input_tokens: 5,
                output_tokens: 6,
            }),
            fail: false,
        }),
        &json!({"agent_id": "writer"}),
        &scope(),
    );
    llm.chat(request()).await.unwrap();

    let calls = meter.calls.lock().unwrap();
    assert_eq!(
        calls[0],
        (
            "acme".into(),
            "prod".into(),
            Some("support-bot".into()),
            5,
            6,
            "writer".into(),
            "m".into(),
        )
    );
}

#[test]
fn without_a_sink_the_handler_hands_the_llm_back_untouched() {
    let handler = RuntimeOperalaNodeHandler::new("unused".into(), None, None, None);
    let inner: Arc<dyn LlmProvider> = Arc::new(StubLlm {
        usage: None,
        fail: false,
    });
    let out = handler.metered(Arc::clone(&inner), &json!({}), &scope());
    assert!(Arc::ptr_eq(&inner, &out), "no sink → no wrapper");
}
