//! Shared context Phase C: `GREENTIC_AW_USER_LEDGER=0` turns the user ledger
//! off at run time even when a binding is installed. Its own test binary
//! because the switch is a process environment variable: one test, no
//! parallel test in this process can observe it.

#![cfg(feature = "test-mock")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::tenant::{TenantContext, VerifiedCaller};
use greentic_aw_runtime::user_ledger::{LedgerEvent, LedgerFuture, LedgerMode, UserLedger};
use greentic_aw_runtime::{
    AgentConfig, AgentInput, AgentLimits, AgentRuntime, LlmProviderRef, UserLedgerBinding,
};

/// Counts every call; answers with one event so a read would show.
#[derive(Default)]
struct CountingLedger {
    calls: Mutex<u32>,
}

impl UserLedger for CountingLedger {
    fn read<'a>(&'a self, _s: &'a str, _l: u32) -> LedgerFuture<'a, Vec<LedgerEvent>> {
        *self.calls.lock().unwrap() += 1;
        Box::pin(async {
            Ok(vec![LedgerEvent::new(
                "u",
                "reply",
                "PREVIOUS-BOOKING",
                "2026-10-05T10:00:00Z",
            )])
        })
    }
    fn append<'a>(&'a self, _s: &'a str, _k: &'a str, _m: &'a str) -> LedgerFuture<'a, ()> {
        *self.calls.lock().unwrap() += 1;
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn the_kill_switch_makes_zero_ledger_calls() {
    // Safety: the only test in this binary, so nothing reads the environment
    // concurrently.
    unsafe {
        std::env::set_var("GREENTIC_AW_USER_LEDGER", "off");
    }
    let tenant = TenantContext::new("acme", "prod").with_caller(Some(VerifiedCaller {
        user_verified: true,
        sub: Some("sub-1".into()),
        ..VerifiedCaller::default()
    }));
    let cp = MockConfigProvider::new();
    cp.insert(
        &TenantContext::new("acme", "prod"),
        "helper",
        AgentConfig {
            agent_id: "helper".into(),
            system_prompt: "sys-helper".into(),
            tools: vec![],
            llm: LlmProviderRef {
                provider: "mock".into(),
                model: "m".into(),
                credential_ref: None,
            },
            limits: AgentLimits {
                max_iter: 2,
                timeout: Duration::from_secs(60),
                ..AgentLimits::default()
            },
            memory: None,
            knowledge: None,
            guardrails: vec![],
            conversational: false,
            opening_message: None,
        },
    );
    let llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
        content: Some("ok".into()),
        tool_calls: vec![],
        tokens_in: 1,
        tokens_out: 1,
    })]));
    let ledger = Arc::new(CountingLedger::default());
    let dyn_ledger: Arc<dyn UserLedger> = ledger.clone();
    let rt = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        llm.clone(),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_user_ledger(Some(Arc::new(UserLedgerBinding::new(
        "acme",
        dyn_ledger,
        HashMap::from([("helper".to_string(), LedgerMode::ReadWrite)]),
    ))));
    let out = rt
        .step(
            tenant,
            "s1",
            "helper",
            AgentInput {
                text: "hi".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(out.reply, "ok");
    assert_eq!(*ledger.calls.lock().unwrap(), 0, "no read, no append");
    assert_eq!(
        *llm.seen_system_prompts.lock().unwrap(),
        vec!["sys-helper".to_string()]
    );
}
