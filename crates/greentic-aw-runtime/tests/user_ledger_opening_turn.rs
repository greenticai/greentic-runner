//! Shared context Phase C: a turn in which the visitor said nothing (the
//! auto-start turn a WebChat conversation opens with) READS the user ledger
//! but APPENDS nothing. Without this rule every conversation open wrote a
//! history row that records nothing the visitor did, and that row then
//! appeared in the visitor's history in every unit of the environment.

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::LlmError;
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::tenant::{TenantContext, VerifiedCaller};
use greentic_aw_runtime::user_ledger::{LedgerEvent, LedgerFuture, LedgerMode, UserLedger};
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, LlmProviderRef, UserLedgerBinding,
};
use serde_json::json;

const TENANT: &str = "acme";

#[derive(Default)]
struct StubLedger {
    reads: Mutex<Vec<String>>,
    appends: Mutex<Vec<(String, String, String)>>,
}

impl UserLedger for StubLedger {
    fn read<'a>(
        &'a self,
        subject: &'a greentic_aw_runtime::user_ledger::LedgerSubject,
        _limit: u32,
    ) -> LedgerFuture<'a, Vec<LedgerEvent>> {
        self.reads.lock().unwrap().push(subject.sub().to_string());
        Box::pin(async {
            Ok(vec![LedgerEvent::new(
                "unit-1",
                "reply",
                "PREVIOUS-BOOKING",
                "2026-10-05T10:00:00Z",
            )])
        })
    }
    fn append<'a>(
        &'a self,
        subject: &'a greentic_aw_runtime::user_ledger::LedgerSubject,
        kind: &'a str,
        summary: &'a str,
    ) -> LedgerFuture<'a, ()> {
        self.appends
            .lock()
            .unwrap()
            .push((subject.sub().into(), kind.into(), summary.into()));
        Box::pin(async { Ok(()) })
    }
}

fn verified(sub: &str) -> TenantContext {
    TenantContext::new(TENANT, "prod").with_caller(Some(VerifiedCaller {
        user_verified: true,
        sub: Some(sub.into()),
        ..VerifiedCaller::default()
    }))
}

fn agent() -> AgentConfig {
    AgentConfig {
        on_text_while_parked: Default::default(),
        agent_id: "helper".into(),
        system_prompt: "sys-helper".into(),
        tools: vec![],
        llm: LlmProviderRef {
            provider: "mock".into(),
            model: "m".into(),
            credential_ref: None,
        },
        limits: AgentLimits {
            max_iter: 4,
            timeout: Duration::from_secs(60),
            ..AgentLimits::default()
        },
        memory: None,
        knowledge: None,
        guardrails: vec![],
        conversational: false,
        // No opening message: the empty turn reaches the LLM, which is the
        // path that used to append.
        opening_message: None,
    }
}

fn reply(text: &str) -> Result<LlmResponse, LlmError> {
    Ok(LlmResponse {
        content: Some(text.into()),
        tool_calls: vec![],
        tokens_in: 1,
        tokens_out: 1,
    })
}

fn runtime(llm: Arc<MockLlmBackend>, ledger: Arc<StubLedger>) -> AgentRuntime {
    let cp = MockConfigProvider::new();
    cp.insert(&TenantContext::new(TENANT, "prod"), "helper", agent());
    let rt = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm,
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    );
    let dyn_ledger: Arc<dyn UserLedger> = ledger;
    rt.with_user_ledger(Some(Arc::new(UserLedgerBinding::new(
        TENANT,
        dyn_ledger,
        HashMap::from([("helper".to_string(), LedgerMode::ReadWrite)]),
    ))))
}

async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
}

/// Runs one read-write step with `input` and returns (reads, appends, the
/// system prompt the LLM saw).
async fn run(input: AgentInput) -> (Vec<String>, Vec<(String, String, String)>, String) {
    let llm = Arc::new(MockLlmBackend::new(vec![reply("Hello! How can I help?")]));
    let ledger = Arc::new(StubLedger::default());
    let rt = runtime(llm.clone(), ledger.clone());
    let out = rt
        .step(verified("sub-1"), "s1", "helper", input)
        .await
        .unwrap();
    assert_eq!(
        out.reply, "Hello! How can I help?",
        "the turn still answers"
    );
    settle().await;
    let prompt = llm.seen_system_prompts.lock().unwrap()[0].clone();
    let reads = ledger.reads.lock().unwrap().clone();
    let appends = ledger.appends.lock().unwrap().clone();
    (reads, appends, prompt)
}

fn text(t: &str) -> AgentInput {
    AgentInput {
        text: t.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn an_empty_opening_turn_reads_the_history_and_appends_nothing() {
    let (reads, appends, prompt) = run(text("")).await;
    assert_eq!(reads, vec!["sub-1".to_string()], "the greeting may use it");
    assert!(prompt.contains("PREVIOUS-BOOKING"));
    assert!(appends.is_empty(), "nothing the visitor did: {appends:?}");
}

#[tokio::test]
async fn a_whitespace_or_invisible_only_message_appends_nothing() {
    for blank in ["   ", "\n\t \r\n", "\u{200B}\u{FEFF}", " \u{2060} "] {
        let (reads, appends, _) = run(text(blank)).await;
        assert_eq!(reads.len(), 1, "{blank:?}: still read");
        assert!(appends.is_empty(), "{blank:?}: appended {appends:?}");
    }
}

#[tokio::test]
async fn a_real_message_still_appends_the_reply_unchanged() {
    let (reads, appends, _) = run(text("book a table")).await;
    assert_eq!(reads.len(), 1);
    assert_eq!(
        appends,
        vec![(
            "sub-1".to_string(),
            "reply".to_string(),
            "Hello! How can I help?".to_string()
        )]
    );
}

/// A card submit carries no text but IS something the visitor did.
#[tokio::test]
async fn an_empty_message_with_a_submit_payload_still_appends() {
    let (_, appends, _) = run(AgentInput {
        text: String::new(),
        resume_payload: Some(json!({ "choice": "yes" })),
        ..Default::default()
    })
    .await;
    assert_eq!(appends.len(), 1, "a submit is visitor content: {appends:?}");
}
