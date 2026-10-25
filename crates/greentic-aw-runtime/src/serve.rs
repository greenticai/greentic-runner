//! NATS-consuming serve mode for the agentic-worker runtime.
//!
//! This is the agentic-side counterpart to the runner's `agentic.call` flow
//! node (the out-of-process dispatch path). It mirrors the proven
//! `greentic-sorx` pattern: a long-lived [`AgentDispatchInvoker`] wraps an
//! [`AgentRuntime`] and the [`aw_event_bridge::run_bridge`] consumer turns
//! `greentic.agentic.request.v1` messages into one `AgentRuntime::step` call,
//! publishing the reply on `greentic.agentic.response.v1`.
//!
//! Gated behind the `serve` feature so the core library has no NATS dependency.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use aw_event_bridge::{
    AgentDispatchInvoker, DispatchIdentity, InvokeOutcome, run_bridge, run_bridge_jetstream,
    valid_dispatch_id,
};
use serde_json::{Value, json};

use crate::dispatch_ledger::{DispatchLedger, NoopDispatchLedger};
use crate::error::TerminationReason;
use crate::tenant::TenantContext;
use crate::{AgentInput, AgentRuntime};

/// Production [`AgentDispatchInvoker`] wrapping a shared [`AgentRuntime`].
///
/// Maps the dispatch contract onto one Plan-Act-Observe step:
/// * `target` -> `agent_id`
/// * `input` -> [`AgentInput`] (the `user_text` field is extracted exactly as
///   the in-process `agent_node` handler does)
/// * the correlation/idempotency hint -> session id (so the same logical
///   conversation resumes the same agentic state)
///
/// The successful [`AgentOutput`] is serialised to
/// `{ "reply", "trail", "terminated_by" }`, matching the in-process node output
/// shape so downstream flow nodes see an identical payload regardless of path.
///
/// [`AgentOutput`]: crate::AgentOutput
///
/// # Dispatch-level idempotency (Task 2.3)
///
/// When `idempotency_key` is `Some(key)`, [`invoke`] checks the
/// [`DispatchLedger`] first. A cache hit returns the stored output without
/// calling `runtime.step`, so JetStream at-least-once redelivery never
/// re-runs the LLM step.
///
/// PR2 defaults to [`NoopDispatchLedger`] (no caching). The **production**
/// Redis-backed ledger should be wired in
/// `greentic-runner-host::agent_node::build_agent_runtime`, where a Redis
/// `ConnectionManager` is already available: construct a
/// [`crate::RedisDispatchLedger`] there and pass it via
/// `RuntimeAgentDispatchInvoker::with_ledger`. This is the designated
/// follow-up after PR2 lands.
pub struct RuntimeAgentDispatchInvoker {
    runtime: Arc<AgentRuntime>,
    ledger: Arc<dyn DispatchLedger>,
}

impl RuntimeAgentDispatchInvoker {
    /// Wrap a shared [`AgentRuntime`] in a dispatch invoker.
    ///
    /// Uses [`NoopDispatchLedger`] (no cross-redelivery caching). See
    /// [`RuntimeAgentDispatchInvoker::with_ledger`] to supply a real ledger.
    #[must_use]
    pub fn new(runtime: Arc<AgentRuntime>) -> Self {
        Self {
            runtime,
            ledger: Arc::new(NoopDispatchLedger),
        }
    }

    /// Wrap a shared [`AgentRuntime`] with an explicit dispatch ledger.
    ///
    /// Use this to inject a [`crate::RedisDispatchLedger`] in production or
    /// an [`crate::dispatch_ledger::InMemoryDispatchLedger`] in tests.
    #[must_use]
    pub fn with_ledger(runtime: Arc<AgentRuntime>, ledger: Arc<dyn DispatchLedger>) -> Self {
        Self { runtime, ledger }
    }
}

/// Extract the user text from the opaque dispatch input.
///
/// Accepts either `{"user_text": "..."}` (the runner's `agentic.call` node
/// shape) or `{"text": "..."}` (raw [`AgentInput`] shape); a bare JSON string is
/// also accepted. Returns an empty string when no text is present so a tool-only
/// or system-prompt-only step can still run.
fn extract_user_text(input: &Value) -> String {
    input
        .get("user_text")
        .or_else(|| input.get("text"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| input.as_str().map(str::to_string))
        .unwrap_or_default()
}

/// Whether a step that ended for `reason` produced a usable result.
///
/// A step cut short by the wall-clock timeout, the iteration cap or an error
/// ends with an empty reply; reporting that as `ok: true` made the caller's
/// flow carry on as if the agent had answered. The output keeps its
/// `{reply, trail, terminated_by}` shape either way, so a consumer can read
/// `terminated_by` to tell which limit was hit.
fn step_succeeded(reason: &TerminationReason) -> bool {
    !matches!(
        reason,
        TerminationReason::Timeout
            | TerminationReason::MaxIterations
            | TerminationReason::Error
            | TerminationReason::TokenBudgetExceeded
    )
}

/// The structured error for a step that ended without a usable reply: code is
/// the `terminated_by` reason (`timeout`, `max_iterations`, ...), so the engine
/// can say why instead of a generic fallback. The partial reply stays in
/// `output`. `None` when the step succeeded.
fn step_failure(ok: bool, output: &Value) -> Option<aw_event_bridge::DispatchError> {
    if ok {
        return None;
    }
    let code = output
        .get("terminated_by")
        .and_then(Value::as_str)
        .unwrap_or("error")
        .to_string();
    Some(aw_event_bridge::DispatchError {
        message: format!("agent step ended without a reply: {code}"),
        code,
    })
}

/// Resolve the session id for the step.
///
/// Prefers an explicit `session_id` field in the input; otherwise falls back to
/// the dispatch correlation/idempotency hint (which the runner derives from the
/// flow session). A final synthetic default keeps the function total.
fn resolve_session_id(input: &Value, idempotency_key: Option<&str>) -> String {
    input
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| idempotency_key.map(str::to_string))
        .filter(|hint| !hint.is_empty())
        .unwrap_or_else(|| "agentic-dispatch".to_string())
}

/// The ledger key for one dispatch: tenant + env + the publisher's per-dispatch
/// id. `None` when there is no well-formed dispatch id, in which case the
/// ledger is not used at all.
///
/// The key is NOT the correlation/idempotency hint: that is the conversation's
/// session hint, identical for every turn, so keying on it replayed turn 1's
/// reply as turn 2's. The dispatch id is minted once per publish, so a broker
/// redelivery of the same message hits the entry while the next turn misses.
/// `tenant` and `env` cannot contain `:` (validated), so the join is unambiguous.
fn ledger_key(tenant: &str, env: &str, dispatch_id: Option<&str>) -> Option<String> {
    let id = dispatch_id.filter(|id| valid_dispatch_id(id))?;
    Some(format!("{tenant}:{env}:{id}"))
}

#[async_trait]
impl AgentDispatchInvoker for RuntimeAgentDispatchInvoker {
    async fn invoke(
        &self,
        tenant: &str,
        env: &str,
        target: &str,
        operation: &str,
        input: Value,
        idempotency_key: Option<&str>,
    ) -> Result<InvokeOutcome> {
        // No dispatch id: nothing safely identifies a redelivery, so no dedup.
        self.invoke_identified(
            tenant,
            env,
            target,
            operation,
            input,
            DispatchIdentity {
                idempotency_key,
                dispatch_id: None,
            },
        )
        .await
    }

    async fn invoke_identified(
        &self,
        tenant: &str,
        env: &str,
        target: &str,
        _operation: &str,
        input: Value,
        ident: DispatchIdentity<'_>,
    ) -> Result<InvokeOutcome> {
        let idempotency_key = ident.idempotency_key;
        // Tenant, env and session ids arrive from the wire and become Redis key
        // segments: refuse the ones that could alias another tenant's keys.
        crate::tenant::validate_scope_id("tenant", tenant).map_err(anyhow::Error::msg)?;
        crate::tenant::validate_scope_id("env", env).map_err(anyhow::Error::msg)?;

        // --- dispatch-level idempotency (Task 2.3) ---
        // A hit means this is a redelivery of the SAME published message:
        // return the cached output without re-running the (expensive +
        // side-effectful) LLM step.
        let ledger_key = ledger_key(tenant, env, ident.dispatch_id);
        if let Some(key) = ledger_key.as_deref() {
            match self.ledger.get(key).await {
                Ok(Some(cached)) => {
                    tracing::debug!(key, "dispatch ledger hit; returning cached output");
                    return Ok(InvokeOutcome {
                        ok: true,
                        output: cached,
                        events: vec![],
                        error: None,
                    });
                }
                Ok(None) => {}
                Err(e) => {
                    // Best-effort: a ledger read error does not abort the step.
                    tracing::warn!(key, error = %e, "dispatch ledger get failed; proceeding without cache");
                }
            }
        }

        let user_text = extract_user_text(&input);
        let session_id = resolve_session_id(&input, idempotency_key);
        crate::tenant::validate_session_id(&session_id).map_err(anyhow::Error::msg)?;
        let tenant_ctx = TenantContext::new(tenant, env);
        // Forward-compatible: honour a `conversational` flag on the dispatch
        // input if the caller (engine remote-dispatch) supplies it, so the
        // out-of-process path can offer `end_conversation` for a conversational
        // flow node too. Defaults false when absent.
        let conversational = input
            .get("conversational")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);

        let output = self
            .runtime
            .step(
                tenant_ctx,
                &session_id,
                target,
                AgentInput {
                    text: user_text,
                    conversational,
                    resume_payload: None,
                    attachments: Vec::new(),
                },
            )
            .await
            .with_context(|| format!("agentic step failed for agent '{target}'"))?;

        let outcome_output = json!({
            "reply": output.reply,
            "trail": output.trail,
            "terminated_by": output.terminated_by,
        });

        let ok = step_succeeded(&output.terminated_by);

        // Record a successful result for future redeliveries (best-effort: a
        // ledger write error is logged but does NOT fail the dispatch). A
        // timed-out / exhausted step is not cached, so a retry can succeed.
        if ok
            && let Some(key) = ledger_key.as_deref()
            && let Err(e) = self.ledger.record(key, outcome_output.clone()).await
        {
            tracing::warn!(key, error = %e, "dispatch ledger record failed; redelivery will re-run step");
        }

        let error = step_failure(ok, &outcome_output);
        Ok(InvokeOutcome {
            ok,
            output: outcome_output,
            events: vec![],
            error,
        })
    }

    async fn step_timeout(&self, tenant: &str, env: &str, target: &str) -> Option<Duration> {
        let tenant_ctx = TenantContext::new(tenant, env);
        match self
            .runtime
            .config_provider
            .agent_config(&tenant_ctx, target)
            .await
        {
            Ok(config) => Some(config.limits.timeout),
            Err(_) => None,
        }
    }
}

/// Whether the agentic serve consumer uses JetStream (durable) vs core-NATS.
///
/// Default ON; set `GREENTIC_AW_JETSTREAM=0|false|no|off` to force the legacy
/// core-NATS path.
#[must_use]
pub fn use_jetstream(get_env: impl Fn(&str) -> Option<String>) -> bool {
    match get_env("GREENTIC_AW_JETSTREAM") {
        Some(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        None => true,
    }
}

/// Parse the comma-separated warm-pack list from the environment. Trims blanks;
/// empty/unset → no targets. Pure over `get_env` for testability.
#[must_use]
pub fn warm_targets(get_env: impl Fn(&str) -> Option<String>) -> Vec<String> {
    get_env("GREENTIC_AW_WARM_PACKS")
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Best-effort cold-start warm hook. Today it logs the intended warm targets so
/// operators can confirm the env is set; the actual cwasm/pack pre-load is baked
/// into the aw-serve image (see the infra runbook). Non-fatal: a warm failure
/// must never block serving. This is a seam — extend to trigger the pack cache
/// load once that API is exposed to this crate.
pub fn warm_on_start(get_env: impl Fn(&str) -> Option<String>) {
    let targets = warm_targets(get_env);
    if targets.is_empty() {
        tracing::debug!("aw serve: no warm targets (GREENTIC_AW_WARM_PACKS unset)");
    } else {
        tracing::info!(
            count = targets.len(),
            ?targets,
            "aw serve: warm targets configured"
        );
    }
}

/// Connect to NATS at `nats_url` and serve agentic dispatch requests forever
/// using an explicit dispatch ledger.
///
/// This is the production entry-point: the runner host supplies a
/// [`crate::dispatch_ledger::RedisDispatchLedger`] so that JetStream
/// at-least-once redeliveries are short-circuited without re-running the LLM
/// step. Callers that do not need Redis-backed idempotency (e.g. the
/// `aw-serve` test binary) can call [`serve`], which injects a
/// [`NoopDispatchLedger`] and is otherwise identical.
///
/// Blocks until the subscription stream ends or the process is signalled.
pub async fn serve_with_ledger(
    nats_url: &str,
    runtime: Arc<AgentRuntime>,
    ledger: Arc<dyn DispatchLedger>,
) -> Result<()> {
    warm_on_start(|k| std::env::var(k).ok());
    // The URL may carry `user:pass@`; only the redacted form is ever logged
    // or put in an error context.
    let nats_url_display = crate::url_redact::redact_url(nats_url);
    let client = async_nats::connect(nats_url)
        .await
        .with_context(|| format!("connecting to NATS at {nats_url_display}"))?;
    tracing::info!(
        nats_url = %nats_url_display,
        subject = aw_event_bridge::request_topic(aw_event_bridge::RUNTIME_NAME),
        "aw event bridge connected; serving agentic dispatch"
    );
    let invoker = Arc::new(RuntimeAgentDispatchInvoker::with_ledger(runtime, ledger));
    if use_jetstream(|k| std::env::var(k).ok()) {
        tracing::info!(nats_url = %nats_url_display, "aw serve: JetStream durable consumer");
        run_bridge_jetstream(client, invoker).await
    } else {
        tracing::info!(nats_url = %nats_url_display, "aw serve: core-NATS consumer (legacy)");
        run_bridge(client, invoker).await
    }
}

/// Connect to NATS at `nats_url` and serve agentic dispatch requests forever.
///
/// Uses [`NoopDispatchLedger`]: idempotency is disabled; JetStream
/// redeliveries will re-run the LLM step. This is the behaviour preserved
/// for the `aw-serve` test binary and any caller that does not have a Redis
/// instance available.
///
/// For production serving with Redis-backed idempotency, use
/// [`serve_with_ledger`] directly (the runner host does this when
/// `GREENTIC_AW_REDIS_URL` is set).
///
/// Blocks until the subscription stream ends or the process is signalled.
pub async fn serve(nats_url: &str, runtime: Arc<AgentRuntime>) -> Result<()> {
    serve_with_ledger(nats_url, runtime, Arc::new(NoopDispatchLedger)).await
}

/// Build a credit-free, broker-free [`AgentRuntime`] that returns a canned reply
/// for any agent id, using the `test-mock` test doubles.
///
/// This is the key to a live e2e (runner `agentic.call` -> aw serve over NATS)
/// without real LLM credits or Redis: every dispatched step resolves the agent
/// against an in-memory config provider and returns `reply` from a scripted mock
/// LLM. `reply` is repeated so a session can take several steps.
#[cfg(feature = "test-mock")]
#[must_use]
// A test-double builder returning `Arc<AgentRuntime>`: it has no error channel,
// and its one fallible call cannot fail here (see the comment at the call site).
// Matches how `mock.rs` and `dispatch_ledger.rs` treat the same situation.
#[allow(clippy::expect_used)]
pub fn build_test_mock_runtime(agent_id: &str, reply: &str) -> Arc<AgentRuntime> {
    use crate::cost::MockTokenMeter;
    use crate::llm::LlmResponse;
    use crate::mock::{
        MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
    };
    use crate::tools::ToolLedger;
    use crate::{AgentConfig, AgentLimits, LlmProviderRef};

    // A generous scripted queue so multi-turn sessions don't exhaust it.
    let scripted = (0..64)
        .map(|_| {
            Ok(LlmResponse {
                content: Some(reply.to_string()),
                tool_calls: vec![],
                tokens_in: 1,
                tokens_out: 1,
            })
        })
        .collect();
    let llm = Arc::new(MockLlmBackend::new(scripted));
    let store = Arc::new(MockAgentStateStore::new());
    let telemetry = Arc::new(MockTelemetry::new());

    // Register the agent for every tenant the test might use. The mock keys by
    // `tenant.key_prefix():agent_id`; we register the common defaults so the
    // dispatched tenant/env resolves.
    let config_provider = MockConfigProvider::new();
    let agent_config = AgentConfig {
        agent_id: agent_id.to_string(),
        system_prompt: "test-mock agent".to_string(),
        tools: vec![],
        guardrails: vec![],
        llm: LlmProviderRef {
            provider: "mock".to_string(),
            model: "mock".to_string(),
            credential_ref: None,
        },
        limits: AgentLimits::default(),
        memory: None,
        knowledge: None,
        conversational: false,
        opening_message: None,
        on_text_while_parked: Default::default(),
    };
    for (tenant, env) in [
        ("default", "default"),
        ("acme", "prod"),
        ("t", "e"),
        ("sorx", "default"),
    ] {
        config_provider.insert(
            &TenantContext::new(tenant, env),
            agent_id,
            agent_config.clone(),
        );
    }
    let config_provider = Arc::new(config_provider);

    let token_meter = Arc::new(MockTokenMeter::new(0));
    let ledger: Arc<dyn ToolLedger> = Arc::new(NoopToolLedger);
    // ext-runtime v1.3.2 (SDK contract research.4 bump) made `for_test()`
    // fallible. It only fails on real construction errors (e.g. a malformed
    // extensions root); the trivial no-extensions test double built here
    // cannot hit that path, so `expect` is safe.
    let ext_runtime = Arc::new(
        greentic_ext_runtime::ExtensionRuntime::for_test()
            .expect("ExtensionRuntime::for_test() must construct a no-extensions test double"),
    );

    Arc::new(AgentRuntime::new(
        config_provider,
        store,
        ext_runtime,
        llm,
        telemetry,
        token_meter,
        ledger,
        None,
    ))
}

#[cfg(test)]
mod warm_targets_tests {
    use super::warm_targets;

    #[test]
    fn warm_targets_parses_csv_and_empty() {
        assert_eq!(warm_targets(|_| None), Vec::<String>::new());
        assert_eq!(
            warm_targets(|k| (k == "GREENTIC_AW_WARM_PACKS").then(|| "a, b ,c".to_string())),
            vec!["a", "b", "c"]
        );
        assert_eq!(
            warm_targets(|k| (k == "GREENTIC_AW_WARM_PACKS").then(|| "".to_string())),
            Vec::<String>::new()
        );
    }
}

#[cfg(test)]
mod env_gate_tests {
    use super::use_jetstream;

    #[test]
    fn jetstream_default_on_unless_disabled() {
        assert!(use_jetstream(|_| None)); // default ON
        assert!(!use_jetstream(
            |k| (k == "GREENTIC_AW_JETSTREAM").then(|| "0".to_string())
        ));
        assert!(!use_jetstream(
            |k| (k == "GREENTIC_AW_JETSTREAM").then(|| "off".to_string())
        ));
        assert!(use_jetstream(
            |k| (k == "GREENTIC_AW_JETSTREAM").then(|| "on".to_string())
        ));
    }
}

#[cfg(all(test, feature = "test-mock"))]
mod tests {
    use super::*;

    #[test]
    fn limit_terminations_are_not_reported_as_success() {
        assert!(step_succeeded(&TerminationReason::FinalReply));
        assert!(step_succeeded(&TerminationReason::ConversationEnded));
        assert!(step_succeeded(&TerminationReason::AwaitingToolInput));
        assert!(!step_succeeded(&TerminationReason::Timeout));
        assert!(!step_succeeded(&TerminationReason::MaxIterations));
        assert!(!step_succeeded(&TerminationReason::Error));
    }

    #[test]
    fn failed_step_carries_terminated_by_as_error_code_and_keeps_partial_reply() {
        let output = json!({"reply": "partial", "trail": [], "terminated_by": "timeout"});
        let error = step_failure(false, &output);
        assert_eq!(error.as_ref().map(|e| e.code.as_str()), Some("timeout"));
        assert!(error.is_some_and(|e| e.message.contains("timeout")));
        assert_eq!(output["reply"], json!("partial"));
        assert!(step_failure(true, &output).is_none());
    }

    #[test]
    fn extract_user_text_accepts_node_and_raw_shapes() {
        assert_eq!(extract_user_text(&json!({"user_text": "hi"})), "hi");
        assert_eq!(extract_user_text(&json!({"text": "yo"})), "yo");
        assert_eq!(extract_user_text(&json!("bare")), "bare");
        assert_eq!(extract_user_text(&json!({"other": 1})), "");
    }

    #[test]
    fn resolve_session_id_prefers_explicit_then_idempotency() {
        assert_eq!(
            resolve_session_id(&json!({"session_id": "s1"}), Some("corr")),
            "s1"
        );
        assert_eq!(resolve_session_id(&json!({}), Some("corr")), "corr");
        assert_eq!(resolve_session_id(&json!({}), Some("")), "agentic-dispatch");
        assert_eq!(resolve_session_id(&json!({}), None), "agentic-dispatch");
    }

    #[tokio::test]
    #[allow(clippy::expect_used)] // test asserts the mock path succeeds
    async fn mock_invoker_returns_reply_output() {
        let runtime = build_test_mock_runtime("greeter", "pong");
        let invoker = RuntimeAgentDispatchInvoker::new(runtime);

        let outcome = invoker
            .invoke(
                "acme",
                "prod",
                "greeter",
                "",
                json!({"user_text": "ping"}),
                Some("sess-1::pack=p::flow=f"),
            )
            .await
            .expect("mock invoke succeeds");

        assert!(outcome.ok);
        assert_eq!(outcome.output["reply"], json!("pong"));
        assert_eq!(outcome.output["terminated_by"], json!("final_reply"));
    }

    // --- Task 2.3: dispatch-level idempotency tests ---

    /// Redelivery scenario: ledger pre-seeded with a sentinel value for "k1".
    /// The invoker MUST return the sentinel without calling `runtime.step`.
    /// We prove step was skipped because the mock runtime returns "pong" for any
    /// step call — if it were called, the output would be "pong", not "CACHED".
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn redelivery_returns_cached_without_rerunning_step() {
        use crate::dispatch_ledger::InMemoryDispatchLedger;

        let ledger = Arc::new(InMemoryDispatchLedger::with(
            "acme:prod:d1",
            json!({"reply": "CACHED", "trail": [], "terminated_by": "final_reply"}),
        ));
        let invoker = RuntimeAgentDispatchInvoker::with_ledger(
            build_test_mock_runtime("greeter", "pong"),
            ledger,
        );

        let out = invoker
            .invoke_identified(
                "acme",
                "prod",
                "greeter",
                "",
                json!({"user_text": "hi"}),
                DispatchIdentity {
                    idempotency_key: Some("sess-k1"),
                    dispatch_id: Some("d1"),
                },
            )
            .await
            .expect("cached invoke succeeds");

        assert!(out.ok);
        // Sentinel value from the ledger, NOT "pong" from the mock step.
        assert_eq!(
            out.output["reply"],
            json!("CACHED"),
            "expected cached sentinel, got runtime reply"
        );
    }

    /// Cache-miss scenario: ledger is empty, step runs, result is recorded.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn miss_runs_step_and_records() {
        use crate::dispatch_ledger::InMemoryDispatchLedger;

        let ledger = Arc::new(InMemoryDispatchLedger::default());
        let invoker = RuntimeAgentDispatchInvoker::with_ledger(
            build_test_mock_runtime("greeter", "pong"),
            ledger.clone(),
        );

        let out = invoker
            .invoke_identified(
                "acme",
                "prod",
                "greeter",
                "",
                json!({"user_text": "hi"}),
                DispatchIdentity {
                    idempotency_key: Some("sess-k2"),
                    dispatch_id: Some("d2"),
                },
            )
            .await
            .expect("fresh invoke succeeds");

        assert!(out.ok);
        assert_eq!(out.output["reply"], json!("pong"), "step ran and replied");
        let stored = ledger.stored("acme:prod:d2");
        assert!(stored.is_some(), "result was recorded in the ledger");
        assert_eq!(
            stored.expect("stored entry present")["reply"],
            json!("pong"),
            "recorded value matches step output"
        );
    }

    /// The regression: two TURNS of one conversation share the correlation /
    /// idempotency hint but carry different dispatch ids. Both must run; turn 2
    /// must not replay turn 1's cached reply.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn two_turns_with_one_correlation_id_both_run() {
        use crate::dispatch_ledger::InMemoryDispatchLedger;

        let ledger = Arc::new(InMemoryDispatchLedger::with(
            "acme:prod:turn-1",
            json!({"reply": "TURN1", "trail": [], "terminated_by": "final_reply"}),
        ));
        let invoker = RuntimeAgentDispatchInvoker::with_ledger(
            build_test_mock_runtime("greeter", "pong"),
            ledger.clone(),
        );
        let turn = |dispatch_id: &'static str| DispatchIdentity {
            idempotency_key: Some("sess-1::pack=p::flow=f"),
            dispatch_id: Some(dispatch_id),
        };

        let second = invoker
            .invoke_identified(
                "acme",
                "prod",
                "greeter",
                "",
                json!({"user_text": "again"}),
                turn("turn-2"),
            )
            .await
            .expect("turn 2 runs");
        assert_eq!(second.output["reply"], json!("pong"), "turn 2 ran the step");
        assert!(ledger.stored("acme:prod:turn-2").is_some());

        // Redelivery of turn 1 still dedups, and of turn 2 returns turn 2's.
        let redelivered = invoker
            .invoke_identified(
                "acme",
                "prod",
                "greeter",
                "",
                json!({"user_text": "hi"}),
                turn("turn-1"),
            )
            .await
            .expect("redelivery");
        assert_eq!(redelivered.output["reply"], json!("TURN1"));
    }

    /// The ledger is scoped by tenant and env: the same dispatch id under
    /// another tenant is a miss.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn ledger_is_scoped_by_tenant_and_env() {
        use crate::dispatch_ledger::InMemoryDispatchLedger;

        let ledger = Arc::new(InMemoryDispatchLedger::with(
            "acme:prod:d1",
            json!({"reply": "ACME", "trail": [], "terminated_by": "final_reply"}),
        ));
        let invoker = RuntimeAgentDispatchInvoker::with_ledger(
            build_test_mock_runtime("greeter", "pong"),
            ledger,
        );
        let out = invoker
            .invoke_identified(
                "globex",
                "prod",
                "greeter",
                "",
                json!({"user_text": "hi"}),
                DispatchIdentity {
                    idempotency_key: Some("sess"),
                    dispatch_id: Some("d1"),
                },
            )
            .await
            .err()
            .expect("a miss reaches the step; the mock only knows tenant acme");
        assert!(
            format!("{out:#}").contains("agentic step failed"),
            "{out:#}"
        );
    }

    #[test]
    fn ledger_key_requires_a_well_formed_dispatch_id() {
        assert_eq!(
            ledger_key("acme", "prod", Some("abc-1_X")).as_deref(),
            Some("acme:prod:abc-1_X")
        );
        assert_eq!(ledger_key("acme", "prod", None), None);
        assert_eq!(ledger_key("acme", "prod", Some("")), None);
        assert_eq!(ledger_key("acme", "prod", Some("a:b")), None);
    }

    /// No key → no ledger interaction; step runs normally.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn no_idempotency_key_runs_step_without_ledger() {
        use crate::dispatch_ledger::InMemoryDispatchLedger;

        let ledger = Arc::new(InMemoryDispatchLedger::default());
        let invoker = RuntimeAgentDispatchInvoker::with_ledger(
            build_test_mock_runtime("greeter", "pong"),
            ledger.clone(),
        );

        let out = invoker
            .invoke(
                "acme",
                "prod",
                "greeter",
                "",
                json!({"user_text": "hi"}),
                None,
            )
            .await
            .expect("no-key invoke succeeds");

        assert!(out.ok);
        assert_eq!(out.output["reply"], json!("pong"));
        // No key → nothing recorded.
        assert!(ledger.stored("k-absent").is_none());
    }
}
