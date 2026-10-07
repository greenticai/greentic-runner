//! An attachment reference on `AgentInput` reaches the persisted user message.

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::LlmError;
use greentic_aw_runtime::llm::{LlmBackend, LlmRequest, LlmResponse};
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::state::{AgentStateStore, ChatMessage};
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, AttachmentKind, AttachmentRef,
    LlmProviderRef, ParkedTextPolicy,
};

struct ReplyLlm;

impl LlmBackend for ReplyLlm {
    fn complete<'a>(
        &'a self,
        _req: LlmRequest,
    ) -> Pin<Box<dyn Future<Output = Result<LlmResponse, LlmError>> + Send + 'a>> {
        Box::pin(async {
            Ok(LlmResponse {
                content: Some("ok".into()),
                tool_calls: vec![],
                tokens_in: 1,
                tokens_out: 1,
            })
        })
    }
}

#[tokio::test]
async fn attachments_reach_the_persisted_user_message_as_references() {
    let cp = MockConfigProvider::new();
    let tenant = TenantContext::new("acme", "prod");
    cp.insert(
        &tenant,
        "agent",
        AgentConfig {
            agent_id: "agent".into(),
            system_prompt: "sys".into(),
            tools: vec![],
            llm: LlmProviderRef {
                provider: "mock".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: AgentLimits {
                max_iter: 2,
                timeout: Duration::from_secs(30),
                ..AgentLimits::default()
            },
            memory: None,
            knowledge: None,
            guardrails: vec![],
            conversational: false,
            opening_message: None,
            on_text_while_parked: ParkedTextPolicy::default(),
        },
    );
    let store = Arc::new(MockAgentStateStore::new());
    let runtime = AgentRuntime::new(
        Arc::new(cp),
        store.clone(),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        Arc::new(ReplyLlm),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    );
    let id = format!("artifact://{}", "a".repeat(64));
    let input = AgentInput {
        text: "look".into(),
        attachments: vec![AttachmentRef {
            id: id.clone(),
            mime_type: "image/png".into(),
            name: Some("a.png".into()),
            size_bytes: Some(3),
            kind: AttachmentKind::Image,
            text_ref: None,
        }],
        ..Default::default()
    };
    runtime
        .step(tenant.clone(), "s1", "agent", input)
        .await
        .unwrap();
    let state = store.load(&tenant, "s1").await.unwrap();
    let first_user = state.messages.iter().find_map(|m| match m {
        ChatMessage::User { attachments, .. } => Some(attachments.clone()),
        _ => None,
    });
    assert_eq!(first_user.unwrap()[0].id, id);
}
