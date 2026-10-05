//! The host half of a playbook TURN: wiring `PlaybookTurnFn` to
//! `AgentRuntime::step`.
//!
//! `greentic_aw_runtime::playbook_source` declares the request, the result and
//! the closure type and deliberately does not run the turn — a playbook turn is
//! an LLM loop, which IS `AgentRuntime`, and the dispatch path is reached from
//! that loop, so passing the runtime into the function its own loop calls would
//! be circular. This module is the host end, and it is the same shape
//! `graph_node::build_agent_turn` already uses for a graph node: synthesize an
//! `AgentConfig`, put it behind a single-entry `InMemoryConfigProvider`, build a
//! lightweight `AgentRuntime` over the SAME sources the calling worker has, and
//! `step` it.
//!
//! # A playbook cannot call a playbook, by construction
//!
//! The nested runtime is built with every tool source EXCEPT a playbook one. So
//! a skill's own turn advertises no `playbook:` tool at all and recursion is not
//! reachable — rather than reachable and bounded by a depth counter, which is a
//! thing to get wrong. Lifting this means deciding what a cycle costs first.
//!
//! # A playbook is stateless, so the turn carries nothing forward
//!
//! `memory` and `knowledge` are `None` and the session id is unique per call.
//! Two concurrent calls of one skill must not share a conversation, and a skill
//! must not accumulate one across calls: that is what makes it a procedure
//! rather than an agent.

#![cfg(feature = "agentic-worker")]

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};

use greentic_aw_runtime::config::{AgentConfig, AgentLimits};
use greentic_aw_runtime::config_provider::InMemoryConfigProvider;
use greentic_aw_runtime::cost::TokenMeter;
use greentic_aw_runtime::{
    AgentInput, AgentRuntime, AgentStateStore, ComponentToolSource, FlowToolSource, LlmBackend,
    PlaybookLlmTier, PlaybookSource, PlaybookToolSource, PlaybookTurnFn, PlaybookTurnRequest,
    PlaybookTurnResult, Telemetry, ToolLedger,
};

use crate::pack::PackRuntime;
use crate::runner::playbook_invoker::PackRuntimePlaybookSource;

/// Tier requirements we could not verify, warned once per playbook per process.
static UNVERIFIED_TIER_WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

/// Everything a nested playbook turn needs, cloned from the calling worker's
/// own runtime so the two share one set of TTL caches rather than each
/// re-enumerating the packs.
#[derive(Clone)]
pub struct PlaybookTurnHost {
    pub state_store: Arc<dyn AgentStateStore>,
    pub ext_runtime: Arc<greentic_ext_runtime::ExtensionRuntime>,
    pub llm: Arc<dyn LlmBackend>,
    pub telemetry: Arc<dyn Telemetry>,
    pub token_meter: Arc<dyn TokenMeter>,
    pub ledger: Arc<dyn ToolLedger>,
    pub mcp: Option<Arc<greentic_aw_runtime::McpToolSource>>,
    pub components: Option<Arc<ComponentToolSource>>,
    pub flows: Option<Arc<FlowToolSource>>,
    pub sorla: Option<Arc<greentic_aw_runtime::SorlaToolSource>>,
    pub a2a: Option<Arc<greentic_aw_runtime::A2aToolSource>>,
}

/// Say once per playbook that its tier was not checked.
///
/// The contract requires a host that cannot tell whether the caller's model
/// meets the tier to SAY so rather than assume it does, and runner-host has no
/// tier-to-model catalogue to check against. Only `Reasoning` is warned: it is
/// the tier where a silent mismatch changes an answer's quality without
/// changing anything observable, which is exactly the failure the contract's
/// "fail rather than downgrade" rule is about.
fn warn_unverified_tier(req: &PlaybookTurnRequest) {
    if req.llm.tier != PlaybookLlmTier::Reasoning {
        return;
    }
    let mut seen = UNVERIFIED_TIER_WARNED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !seen.insert(req.playbook_id.clone()) {
        return;
    }
    tracing::warn!(
        playbook = %req.playbook_id,
        provider = %req.caller_llm.provider,
        model = %req.caller_llm.model,
        "playbook asks for the reasoning tier and this host cannot verify the \
         caller's model meets it; running it anyway"
    );
}

/// The `AgentConfig` one playbook turn runs as.
///
/// `system_prompt` is the document's instructions verbatim — a playbook IS its
/// instructions, so nothing wraps them. `tools` is the set aw-runtime already
/// narrowed to `intersect(caller, document)`; this side must not widen it.
fn config_for(req: &PlaybookTurnRequest, agent_id: &str) -> AgentConfig {
    AgentConfig {
        agent_id: agent_id.to_string(),
        system_prompt: req.instructions.clone(),
        tools: req.tools.clone(),
        guardrails: req.guardrails.clone(),
        llm: req.caller_llm.clone(),
        limits: AgentLimits::default(),
        memory: None,
        knowledge: None,
        conversational: false,
        opening_message: None,
    }
}

/// The user turn a playbook is handed: its caller's arguments.
///
/// Serialized rather than templated into the prompt, because the arguments are
/// the LLM's own structured output and the schema the document declared is what
/// it produced them against.
fn input_for(req: &PlaybookTurnRequest) -> AgentInput {
    AgentInput {
        text: serde_json::to_string(&req.input).unwrap_or_else(|_| req.input.to_string()),
        ..Default::default()
    }
}

/// Build the playbook tool source for the packs a worker is running with, or
/// `None` when none of them carries a playbook.
///
/// `None` keeps every existing worker byte-identical: no source means a
/// `playbook:` ref resolves to nothing and is reported by `missing_tools`,
/// exactly as an unwired `flow:` ref is.
pub(crate) fn playbook_source_from_packs(
    packs: &[Arc<PackRuntime>],
    host: PlaybookTurnHost,
) -> Option<Arc<PlaybookToolSource>> {
    if std::env::var("GREENTIC_AW_PLAYBOOK_TOOLS").ok().as_deref() == Some("0") {
        tracing::info!("GREENTIC_AW_PLAYBOOK_TOOLS=0; playbook tool source disabled");
        return None;
    }
    if packs.is_empty() {
        return None;
    }
    let source = PackRuntimePlaybookSource::new(packs.to_vec());
    // Asking once here is what keeps a pack with no playbooks from paying for
    // this feature at all — and the reader warns per document, so a malformed
    // one is reported at boot rather than on the first call.
    if source.list_playbooks().is_empty() {
        return None;
    }
    let turn = build_turn(host);
    Some(Arc::new(PlaybookToolSource::new(Arc::new(source), turn)))
}

/// Wire one playbook turn to `AgentRuntime::step`.
///
/// **Public because a host that runs a worker with NO pack still needs this
/// exact turn effect.** [`playbook_source_from_packs`] above is the pack-backed
/// door, and it is the only one a deployed runtime needs; greentic-designer's
/// Test chat builds its `AgentRuntime` from a composed form instead and resolves
/// a `playbook:` ref from its own workspace store, so it supplies its own
/// [`PlaybookSource`] and needs only the turn. Every type in
/// [`PlaybookTurnHost`] is already publicly re-exported, so the struct was
/// reachable from outside this crate while the one function consuming it was
/// not.
///
/// It is exported rather than reimplemented because the composition here
/// carries a property a caller cannot be relied on to reproduce: the nested
/// runtime is built with every tool source EXCEPT playbooks, so a playbook
/// cannot call a playbook *by construction* rather than by a depth counter
/// somebody has to remember to check.
/// `the_turn_host_carries_no_playbook_source` is what pins it.
pub fn build_turn(host: PlaybookTurnHost) -> PlaybookTurnFn {
    Arc::new(move |req: PlaybookTurnRequest| {
        let host = host.clone();
        Box::pin(async move {
            warn_unverified_tier(&req);

            let agent_id = format!("playbook.{}", req.playbook_id);
            // Unique per call: a playbook is stateless, so two concurrent calls
            // of one skill must not share a conversation and no call may
            // inherit another's.
            let session_id = format!("playbook__{}__{}", req.playbook_id, ulid::Ulid::new());

            let mut provider = InMemoryConfigProvider::new();
            provider.insert(&req.tenant, &agent_id, config_for(&req, &agent_id));

            // Every tool source the caller has, and deliberately NOT a playbook
            // one — see this module's header.
            let runtime = AgentRuntime::new(
                Arc::new(provider),
                host.state_store.clone(),
                host.ext_runtime.clone(),
                host.llm.clone(),
                host.telemetry.clone(),
                host.token_meter.clone(),
                host.ledger.clone(),
                host.mcp.clone(),
            )
            .with_component_source(host.components.clone())
            .with_flow_source(host.flows.clone())
            .with_sorla_source(host.sorla.clone())
            .with_a2a_source(host.a2a.clone());

            let out = runtime
                .step(req.tenant.clone(), &session_id, &agent_id, input_for(&req))
                .await
                .map_err(|e| format!("playbook '{}' failed: {e}", req.playbook_id))?;

            Ok(PlaybookTurnResult { reply: out.reply })
        })
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use greentic_aw_runtime::config::{LlmProviderRef, ToolRef};
    use greentic_aw_runtime::{PlaybookLlmRequirement, TenantContext};

    fn tool(ext: &str, name: &str) -> ToolRef {
        ToolRef {
            extension_id: ext.into(),
            tool_name: name.into(),
            description: None,
            input_schema: None,
            usage_note: None,
        }
    }

    fn request(tier: PlaybookLlmTier) -> PlaybookTurnRequest {
        PlaybookTurnRequest {
            tenant: TenantContext::new("acme", "prod"),
            playbook_id: "refund".into(),
            instructions: "Follow the refund policy.".into(),
            llm: PlaybookLlmRequirement {
                tier,
                requires: vec![],
            },
            tools: vec![tool("greentic.billing", "refund")],
            guardrails: vec![],
            caller_llm: LlmProviderRef {
                provider: "anthropic".into(),
                model: "claude-3-haiku".into(),
                credential_ref: Some("cred-1".into()),
            },
            input: serde_json::json!({ "order_id": "A-1" }),
        }
    }

    /// A playbook IS its instructions: nothing wraps or prepends them.
    #[test]
    fn the_instructions_become_the_system_prompt_verbatim() {
        let cfg = config_for(&request(PlaybookLlmTier::Balanced), "playbook.refund");
        assert_eq!(cfg.system_prompt, "Follow the refund policy.");
        assert_eq!(cfg.agent_id, "playbook.refund");
    }

    /// The narrowing happened in aw-runtime; this side must not widen it.
    #[test]
    fn the_turn_runs_with_exactly_the_narrowed_tools() {
        let cfg = config_for(&request(PlaybookLlmTier::Balanced), "playbook.refund");
        assert_eq!(cfg.tools, vec![tool("greentic.billing", "refund")]);
    }

    /// A playbook names no model, so it runs on its caller's — credential and
    /// all, since that is what the workspace configured.
    #[test]
    fn the_turn_runs_on_the_callers_own_model() {
        let cfg = config_for(&request(PlaybookLlmTier::Reasoning), "playbook.refund");
        assert_eq!(cfg.llm.provider, "anthropic");
        assert_eq!(cfg.llm.model, "claude-3-haiku");
        assert_eq!(cfg.llm.credential_ref.as_deref(), Some("cred-1"));
    }

    /// Stateless by definition: a skill accumulates no conversation and reads
    /// no knowledge base of its own.
    #[test]
    fn a_playbook_turn_carries_no_memory_and_no_knowledge() {
        let cfg = config_for(&request(PlaybookLlmTier::Balanced), "playbook.refund");
        assert!(cfg.memory.is_none(), "a playbook must not remember");
        assert!(
            cfg.knowledge.is_none(),
            "a playbook has no corpus of its own"
        );
        assert!(!cfg.conversational);
    }

    #[test]
    fn the_arguments_become_the_user_turn() {
        let input = input_for(&request(PlaybookLlmTier::Balanced));
        let parsed: serde_json::Value =
            serde_json::from_str(&input.text).expect("the turn text is the arguments as JSON");
        assert_eq!(parsed["order_id"], "A-1");
    }

    /// The contract requires a host that cannot check the tier to say so. Only
    /// the reasoning tier is worth a line, and only once per playbook.
    #[test]
    fn only_an_unverifiable_reasoning_tier_is_warned_and_only_once() {
        let req = request(PlaybookLlmTier::Reasoning);
        warn_unverified_tier(&req);
        let mut seen = UNVERIFIED_TIER_WARNED
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            seen.contains("refund"),
            "a reasoning playbook must be recorded as warned"
        );
        // A second call is a no-op: the id is already in the set.
        assert!(!seen.insert("refund".to_string()));
        drop(seen);

        let mut fast = request(PlaybookLlmTier::Fast);
        fast.playbook_id = "quick".into();
        warn_unverified_tier(&fast);
        let seen = UNVERIFIED_TIER_WARNED
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            !seen.contains("quick"),
            "a fast-tier playbook needs no warning"
        );
    }

    /// The no-recursion property, as close to compile-time as a test gets:
    /// `PlaybookTurnHost` has no playbook field, so the nested runtime this
    /// module builds cannot be given one. If a field is ever added, this test
    /// is where the decision has to be re-made.
    #[test]
    fn the_turn_host_carries_no_playbook_source() {
        let host = PlaybookTurnHost {
            state_store: Arc::new(greentic_aw_runtime::mock::MockAgentStateStore::new()),
            ext_runtime: Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
            llm: Arc::new(greentic_aw_runtime::mock::MockLlmBackend::new(vec![])),
            telemetry: Arc::new(greentic_aw_runtime::mock::MockTelemetry::new()),
            token_meter: Arc::new(greentic_aw_runtime::cost::MockTokenMeter::new(0)),
            ledger: Arc::new(greentic_aw_runtime::mock::NoopToolLedger),
            mcp: None,
            components: None,
            flows: None,
            sorla: None,
            a2a: None,
        };
        // Compiles only while the struct has exactly these fields; the absence
        // of a `playbooks` one is the property.
        let _ = build_turn(host);
    }

    struct RefundSource;
    impl greentic_aw_runtime::PlaybookSource for RefundSource {
        fn list_playbooks(&self) -> Vec<greentic_aw_runtime::PlaybookOperation> {
            vec![greentic_aw_runtime::PlaybookOperation {
                playbook_id: "refund".into(),
                description: "Refund".into(),
                parameters: serde_json::json!({ "type": "object" }),
                instructions: "Follow the refund policy.".into(),
                llm: PlaybookLlmRequirement::default(),
                allow_list: vec![],
                guardrails: vec![],
            }]
        }
    }

    /// The real `build_turn` awaits its nested step inline, so it inherits the
    /// caller's context; the `playbook:` arm narrows it to the binding's mode.
    async fn inner_prompt(mode: Option<greentic_aw_runtime::ShareMode>) -> String {
        use greentic_aw_runtime::llm::LlmResponse;
        use greentic_aw_runtime::mock::{MockConfigProvider, MockLlmBackend};
        use greentic_aw_runtime::state::ToolCallRecord;

        let inner_llm = Arc::new(MockLlmBackend::new(vec![Ok(LlmResponse {
            content: Some("refunded".into()),
            tool_calls: vec![],
            tokens_in: 1,
            tokens_out: 1,
        })]));
        let host = PlaybookTurnHost {
            state_store: Arc::new(greentic_aw_runtime::mock::MockAgentStateStore::new()),
            ext_runtime: Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
            llm: inner_llm.clone(),
            telemetry: Arc::new(greentic_aw_runtime::mock::MockTelemetry::new()),
            token_meter: Arc::new(greentic_aw_runtime::cost::MockTokenMeter::new(0)),
            ledger: Arc::new(greentic_aw_runtime::mock::NoopToolLedger),
            mcp: None,
            components: None,
            flows: None,
            sorla: None,
            a2a: None,
        };
        let outer_llm = Arc::new(MockLlmBackend::new(vec![
            Ok(LlmResponse {
                content: None,
                tool_calls: vec![ToolCallRecord {
                    call_id: "c1".into(),
                    extension_id: "playbook:refund".into(),
                    tool_name: "run".into(),
                    args: serde_json::json!({}),
                }],
                tokens_in: 1,
                tokens_out: 1,
            }),
            Ok(LlmResponse {
                content: Some("done".into()),
                tool_calls: vec![],
                tokens_in: 1,
                tokens_out: 1,
            }),
        ]));
        let tenant = TenantContext::new("acme", "prod");
        let cp = MockConfigProvider::new();
        let mut cfg = config_for(&request(PlaybookLlmTier::Balanced), "outer");
        cfg.system_prompt = "sys".into();
        cfg.tools = vec![tool("playbook:refund", "run")];
        cp.insert(&tenant, "outer", cfg);
        let policy = mode.map(|m| {
            let mut modes = greentic_aw_runtime::BindingModes::new();
            modes.insert("playbook:refund".to_string(), m);
            let mut agents = std::collections::HashMap::new();
            agents.insert("outer".to_string(), modes);
            Arc::new(greentic_aw_runtime::SharePolicy::new(agents))
        });
        let outer = AgentRuntime::new(
            Arc::new(cp),
            Arc::new(greentic_aw_runtime::mock::MockAgentStateStore::new()),
            Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().unwrap()),
            outer_llm,
            Arc::new(greentic_aw_runtime::mock::MockTelemetry::new()),
            Arc::new(greentic_aw_runtime::cost::MockTokenMeter::new(0)),
            Arc::new(greentic_aw_runtime::mock::NoopToolLedger),
            None,
        )
        .with_playbook_source(Some(Arc::new(
            greentic_aw_runtime::PlaybookToolSource::new(Arc::new(RefundSource), build_turn(host)),
        )))
        .with_share_policy(policy);
        let trace = Arc::new(greentic_aw_runtime::RunTrace::new());
        trace.append("host", "reply", "SEED-PB");
        greentic_aw_runtime::RunContext::scope(
            greentic_aw_runtime::RunContext::new("acme", trace),
            outer.step(tenant, "s-outer", "outer", AgentInput::default()),
        )
        .await
        .unwrap();
        inner_llm.seen_system_prompts.lock().unwrap()[0].clone()
    }

    #[tokio::test]
    async fn the_real_playbook_turn_sees_the_caller_only_when_its_binding_allows() {
        let read = inner_prompt(Some(greentic_aw_runtime::ShareMode::Read)).await;
        assert!(read.contains("SEED-PB"), "{read}");
        assert_eq!(
            inner_prompt(None).await,
            "Follow the refund policy.",
            "default none"
        );
        assert_eq!(
            inner_prompt(Some(greentic_aw_runtime::ShareMode::None)).await,
            "Follow the refund policy.",
            "an explicit none binding"
        );
    }
}
