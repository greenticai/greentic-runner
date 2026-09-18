//! Integration tests proving that the guardrail chain is wired into `run_step`.
//!
//! These tests exercise the fail-closed inbound path: when a mandatory
//! guardrail cannot be resolved from the capability registry, `run_step` must
//! return `AgentError::GuardrailDenied` *before* any LLM call is attempted.
//! This is achievable without a populated registry or a live LLM because the
//! `ExtensionRuntime::for_test()` has an empty capability registry — perfect
//! for triggering the fail-closed branch.

#![cfg(feature = "test-mock")]

use std::sync::Arc;
use std::time::Duration;

use greentic_aw_runtime::config::{AgentConfig, AgentLimits, GuardrailRef, LlmProviderRef};
use greentic_aw_runtime::cost::MockTokenMeter;
use greentic_aw_runtime::error::AgentError;
use greentic_aw_runtime::guardrail::{GuardrailDirection, StaticGuardrailPolicy};
use greentic_aw_runtime::llm::LlmResponse;
use greentic_aw_runtime::mock::{
    MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
};
use greentic_aw_runtime::tenant::TenantContext;
use greentic_aw_runtime::{AgentInput, AgentRuntime};

/// Build a minimal `AgentRuntime` with a single LLM script entry (which should
/// never be reached in fail-closed tests) and a supplied mandatory guardrail ref.
fn build_runtime_with_mandatory_guardrail(mandatory_cap_id: &str) -> (AgentRuntime, TenantContext) {
    let mandatory = vec![GuardrailRef {
        cap_id: mandatory_cap_id.to_string(),
        offer_id: None,
        config: serde_json::Value::Null,
        mode: greentic_aw_runtime::config::GuardrailMode::Enforce,
    }];

    let llm_script = vec![Ok(LlmResponse {
        content: Some("should never arrive".into()),
        tool_calls: vec![],
        tokens_in: 1,
        tokens_out: 1,
    })];

    let config = AgentConfig {
        agent_id: "a".into(),
        system_prompt: "test".into(),
        tools: vec![],
        guardrails: vec![],
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
        conversational: false,
        opening_message: None,
    };

    let tc = TenantContext::new("acme", "prod");
    let cp = MockConfigProvider::new();
    cp.insert(&tc, "a", config);

    // `ExtensionRuntime::for_test()` initialises an empty capability registry —
    // any mandatory cap_id will fail to resolve, triggering the fail-closed path.
    let ext = Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());

    let runtime = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        ext,
        Arc::new(MockLlmBackend::new(llm_script)),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    )
    .with_guardrails(
        Arc::new(StaticGuardrailPolicy(mandatory)),
        // AcceptAllEvaluator would never be reached because assemble_chain
        // errors out before run_chain is called.
        Arc::new(greentic_aw_runtime::guardrail::AcceptAllEvaluator),
    );

    (runtime, tc)
}

/// When a mandatory guardrail cap_id has no offering in the (empty)
/// `ExtensionRuntime::for_test()` registry, `assemble_chain` returns `Err` and
/// `run_step` must fail closed with `AgentError::GuardrailDenied { direction:
/// Inbound, code: "internal", .. }` *before* the LLM is invoked.
#[tokio::test]
async fn fail_closed_mandatory_unresolved_returns_guardrail_denied() {
    // Use the namespace:path format required by `CapabilityId::from_str`.
    let (runtime, tc) = build_runtime_with_mandatory_guardrail("greentic:guardrail/required-pii");

    let result = runtime
        .step(
            tc,
            "session-guardrail-1",
            "a",
            AgentInput {
                text: "hello — please process this".into(),
                conversational: false,
            },
        )
        .await;

    match result {
        Err(AgentError::GuardrailDenied {
            direction,
            code,
            message,
            ..
        }) => {
            assert_eq!(
                direction,
                GuardrailDirection::Inbound,
                "fail-closed error must carry Inbound direction"
            );
            assert_eq!(
                code, "internal",
                "fail-closed error must use the 'internal' code"
            );
            assert!(
                !message.is_empty(),
                "fail-closed error must carry a non-empty message"
            );
        }
        other => panic!("expected AgentError::GuardrailDenied (inbound/internal), got: {other:?}"),
    }
}

/// Regression guard: when there are NO mandatory guardrails (default policy) and
/// the agent config has no guardrail refs, `run_step` must still succeed and
/// return the LLM reply unchanged.
#[tokio::test]
async fn no_mandatory_guardrails_passes_through() {
    let config = AgentConfig {
        agent_id: "a".into(),
        system_prompt: "test".into(),
        tools: vec![],
        guardrails: vec![],
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
        conversational: false,
        opening_message: None,
    };

    let tc = TenantContext::new("acme", "prod");
    let cp = MockConfigProvider::new();
    cp.insert(&tc, "a", config);

    let runtime = AgentRuntime::new(
        Arc::new(cp),
        Arc::new(MockAgentStateStore::new()),
        Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
        Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
            content: Some("all good".into()),
            tool_calls: vec![],
            tokens_in: 3,
            tokens_out: 3,
        })])),
        Arc::new(MockTelemetry::new()),
        Arc::new(MockTokenMeter::new(0)),
        Arc::new(NoopToolLedger),
        None,
    );
    // Uses the default NoMandatoryGuardrails + AcceptAllEvaluator — no
    // injection needed.

    let output = runtime
        .step(
            tc,
            "session-guardrail-2",
            "a",
            AgentInput {
                text: "hi".into(),
                conversational: false,
            },
        )
        .await
        .expect("no guardrails configured — step must succeed");

    assert_eq!(output.reply, "all good");
}

/// When a mandatory guardrail cap_id has no offering in the (empty)
/// `ExtensionRuntime::for_test()` registry, `assemble_chain` returns `Err`
/// and `run_step` must fail closed with `AgentError::GuardrailDenied {
/// direction: Inbound, code: "internal", .. }` regardless of which evaluator
/// is supplied — the evaluator is never reached because `assemble_chain` errors
/// out before `run_chain` is called.
///
/// Note: the genuine evaluator-deny-with-resolved-chain path is covered by the
/// Task 10 e2e (real PII component) and the `run_chain` unit tests in
/// `guardrail.rs`. Building a resolved-chain integration test here is deferred
/// to Task 10 because it requires the ExtensionRuntime injection API from
/// Task 7.
#[tokio::test]
async fn mandatory_ref_with_empty_registry_fails_closed() {
    // Use the namespace:path format required by `CapabilityId::from_str`.
    let (runtime, tc) = build_runtime_with_mandatory_guardrail("greentic:guardrail/pii");

    let result = runtime
        .step(
            tc,
            "session-guardrail-3",
            "a",
            AgentInput {
                text: "sensitive input".into(),
                conversational: false,
            },
        )
        .await;

    match result {
        Err(AgentError::GuardrailDenied {
            direction,
            code,
            message,
            ..
        }) => {
            assert_eq!(
                direction,
                GuardrailDirection::Inbound,
                "fail-closed error must carry Inbound direction"
            );
            assert_eq!(
                code, "internal",
                "fail-closed error must use the 'internal' code"
            );
            assert!(
                !message.is_empty(),
                "fail-closed error must carry a non-empty message"
            );
        }
        other => panic!("expected AgentError::GuardrailDenied (inbound/internal), got: {other:?}"),
    }
}

/// A policy stub that always returns `Err(Unavailable)` — simulates an admin
/// server being down at policy-fetch time.
struct FailingPolicy;

impl greentic_aw_runtime::guardrail::GuardrailPolicy for FailingPolicy {
    fn mandatory_guardrails<'a>(
        &'a self,
        _t: &'a greentic_aw_runtime::tenant::TenantContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        Vec<greentic_aw_runtime::config::GuardrailRef>,
                        greentic_aw_runtime::guardrail::GuardrailPolicyError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            Err(
                greentic_aw_runtime::guardrail::GuardrailPolicyError::Unavailable(
                    "admin down".into(),
                ),
            )
        })
    }
}

/// When the policy provider itself returns `Err`, the loop must fail closed
/// with `AgentError::GuardrailDenied { code: "internal", .. }` before any
/// LLM call is attempted.
#[tokio::test]
async fn failing_policy_fails_closed_with_guardrail_denied() {
    let tc = TenantContext::new("acme", "prod");
    let config = greentic_aw_runtime::config::AgentConfig {
        agent_id: "a".into(),
        system_prompt: "test".into(),
        tools: vec![],
        guardrails: vec![],
        llm: greentic_aw_runtime::config::LlmProviderRef {
            provider: "mock".into(),
            model: "m".into(),
            credential_ref: None,
        },
        limits: greentic_aw_runtime::config::AgentLimits {
            max_iter: 4,
            timeout: std::time::Duration::from_secs(60),
            ..greentic_aw_runtime::config::AgentLimits::default()
        },
        memory: None,
        knowledge: None,
        conversational: false,
        opening_message: None,
    };

    let cp = greentic_aw_runtime::mock::MockConfigProvider::new();
    cp.insert(&tc, "a", config);

    let llm_script = vec![Ok(greentic_aw_runtime::llm::LlmResponse {
        content: Some("should never arrive".into()),
        tool_calls: vec![],
        tokens_in: 1,
        tokens_out: 1,
    })];

    let ext = std::sync::Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap());
    let runtime = AgentRuntime::new(
        std::sync::Arc::new(cp),
        std::sync::Arc::new(greentic_aw_runtime::mock::MockAgentStateStore::new()),
        ext,
        std::sync::Arc::new(greentic_aw_runtime::mock::MockLlmBackend::new(llm_script)),
        std::sync::Arc::new(greentic_aw_runtime::mock::MockTelemetry::new()),
        std::sync::Arc::new(greentic_aw_runtime::cost::MockTokenMeter::new(0)),
        std::sync::Arc::new(greentic_aw_runtime::mock::NoopToolLedger),
        None,
    )
    .with_guardrails(
        std::sync::Arc::new(FailingPolicy),
        std::sync::Arc::new(greentic_aw_runtime::guardrail::AcceptAllEvaluator),
    );

    let result = runtime
        .step(
            tc,
            "session-fail-policy",
            "a",
            AgentInput {
                text: "hello".into(),
                conversational: false,
            },
        )
        .await;

    match result {
        Err(AgentError::GuardrailDenied {
            direction,
            code,
            message,
            ..
        }) => {
            assert_eq!(
                direction,
                GuardrailDirection::Inbound,
                "policy-unavailable error must carry Inbound direction"
            );
            assert_eq!(
                code, "internal",
                "policy-unavailable error must use the 'internal' code"
            );
            assert!(
                !message.is_empty(),
                "policy-unavailable error must carry a non-empty message"
            );
        }
        other => panic!("expected AgentError::GuardrailDenied (inbound/internal), got: {other:?}"),
    }
}
