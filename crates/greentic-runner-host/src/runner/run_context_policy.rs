//! Build the agent runtime's [`SharePolicy`] from the packs'
//! `assets/run-context.json` sidecars (shared context, Phase A2). Twin of
//! `a2a_pack_source`, in its own file because `agent_node.rs` is already over
//! 4000 lines.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use greentic_aw_runtime::{BindingModes, ShareMode, SharePolicy};

/// The sharing policy for one runtime, or `None` when no pack carries a
/// sidecar. `packs` is one revision's pack list; the first pack naming an
/// agent wins, as for A2A routes. An agent key outside `known_agents` is
/// warned about: it is almost always a key written under the wrong id (the
/// display name instead of the agent-map key), which shares nothing silently.
pub(crate) fn share_policy_from_packs<'a>(
    packs: &[Arc<crate::pack::PackRuntime>],
    known_agents: impl IntoIterator<Item = &'a str>,
) -> Option<Arc<SharePolicy>> {
    let known: HashSet<&str> = known_agents.into_iter().collect();
    let mut agents: HashMap<String, BindingModes> = HashMap::new();
    for pack in packs {
        let Some(declared) = pack.run_context() else {
            continue;
        };
        for (agent_id, bindings) in declared.agents() {
            if agents.contains_key(agent_id) {
                continue;
            }
            if !known.contains(agent_id.as_str()) {
                tracing::warn!(
                    agent = %agent_id,
                    "run-context: sidecar names an agent this runtime does not carry; \
                     its bindings will never match a step"
                );
            }
            let modes: BindingModes = bindings
                .iter()
                .filter_map(|(binding, mode)| ShareMode::parse(mode).map(|m| (binding.clone(), m)))
                .collect();
            agents.insert(agent_id.clone(), modes);
        }
    }
    if agents.is_empty() {
        return None;
    }
    tracing::info!(
        agents = agents.len(),
        "run-context sharing policy constructed"
    );
    Some(Arc::new(SharePolicy::new(agents)))
}

/// Kill switch: `GREENTIC_AW_RUN_CONTEXT` set to `0`, `false`, `off` or `no`
/// (any case, surrounding whitespace ignored) stops the run context from
/// opening. Opt-out, like `GREENTIC_AW_FLOW_TOOLS`: the switch ON is the
/// sidecar's presence, because no environment variable reaches a k8s or Cloud
/// Run workload from the designer.
///
/// Any value other than `0`/`false`/`off`/`no` (trimmed, case-insensitive) means
/// ON, the empty string and garbage included.
pub(crate) fn run_context_enabled() -> bool {
    let value = std::env::var("GREENTIC_AW_RUN_CONTEXT")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase());
    !matches!(value.as_deref(), Some("0" | "false" | "off" | "no"))
}

/// Whether a `dw.agent` turn opens the run's first context: none is open yet
/// (a nested `dw.agent` reuses the outer one), this agent's policy shares at
/// least one binding, and the kill switch is not set.
///
/// Only `RuntimeAgentNodeHandler::execute_with_resume` asks. Other callers of
/// `AgentRuntime::step` (graph `run_agent_step`, `playbook_turn`, designer
/// hosts, `serve.rs`) deliberately open no context and run as before.
pub(crate) fn should_open_scope(
    runtime: &greentic_aw_runtime::AgentRuntime,
    agent_id: &str,
) -> bool {
    greentic_aw_runtime::RunContext::current().is_none()
        && runtime
            .share_policy()
            .is_some_and(|p| p.has_sharing_binding(agent_id))
        && run_context_enabled()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use greentic_aw_runtime::ShareMode;
    use std::collections::HashMap;

    fn pack_with(sidecar: Option<&str>) -> (tempfile::TempDir, Arc<crate::pack::PackRuntime>) {
        let dir = tempfile::tempdir().unwrap();
        if let Some(body) = sidecar {
            std::fs::create_dir_all(dir.path().join("assets")).unwrap();
            std::fs::write(dir.path().join("assets/run-context.json"), body).unwrap();
        }
        let pack = Arc::new(crate::pack::tests::pack_runtime_for_dir(dir.path()));
        (dir, pack)
    }

    #[test]
    fn no_sidecar_means_no_policy() {
        let (_d, pack) = pack_with(None);
        assert!(share_policy_from_packs(&[pack], ["a"]).is_none());
    }

    #[test]
    fn the_first_pack_naming_an_agent_wins() {
        let (_d1, p1) = pack_with(Some(r#"{"a":{"flow:x":"read"}}"#));
        let (_d2, p2) = pack_with(Some(r#"{"a":{"flow:x":"read_write"}}"#));
        let policy = share_policy_from_packs(&[p1, p2], ["a"]).unwrap();
        assert_eq!(
            policy.for_agent("a").unwrap().get("flow:x"),
            Some(&ShareMode::Read)
        );
        assert!(policy.has_sharing_binding("a"));
    }

    #[test]
    fn an_unknown_agent_key_is_warned_and_kept_inert() {
        let (_d, pack) = pack_with(Some(r#"{"Support Bot":{"flow:x":"read"}}"#));
        let policy = share_policy_from_packs(&[pack], ["support-bot"]).unwrap();
        assert!(
            !policy.has_sharing_binding("support-bot"),
            "a mismatched key shares nothing"
        );
    }

    use std::future::Future;
    use std::pin::Pin;

    use greentic_aw_runtime::cost::MockTokenMeter;
    use greentic_aw_runtime::llm::LlmResponse;
    use greentic_aw_runtime::mock::{
        MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
    };
    use greentic_aw_runtime::state::ToolCallRecord;
    use greentic_aw_runtime::{
        AgentConfig, AgentLimits, AgentRuntime, BindingModes, FlowInvokeOutcome, FlowInvoker,
        FlowOperation, FlowToolSource, LlmProviderRef, RunContext, TenantContext, ToolRef,
    };
    use serde_json::{Value, json};

    use crate::runner::agent_node::{AgentNodeHandler, RuntimeAgentNodeHandler};

    struct OkFlow;
    impl FlowInvoker for OkFlow {
        fn list_flows(&self) -> Vec<FlowOperation> {
            vec![FlowOperation {
                flow_ref: "x".into(),
                description: "x".into(),
                parameters: json!({ "type": "object" }),
            }]
        }
        fn invoke<'a>(
            &'a self,
            _f: &'a str,
            _a: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            Box::pin(async { Ok(json!({ "ok": 1 })) })
        }
        fn invoke_interactive<'a>(
            &'a self,
            _f: &'a str,
            _a: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
            Box::pin(async { Ok(FlowInvokeOutcome::Completed(json!({ "ok": 1 }))) })
        }
        fn resume<'a>(
            &'a self,
            _f: &'a str,
            _s: Value,
            _i: Value,
        ) -> Pin<Box<dyn Future<Output = Result<FlowInvokeOutcome, String>> + Send + 'a>> {
            Box::pin(async { Err("no resume".to_string()) })
        }
    }

    /// Agent `a` calls flow `x` once, then replies. With a scope open, its
    /// second request carries its own tool outcome in a `<run_context>` block.
    fn handler(mode: Option<ShareMode>) -> (RuntimeAgentNodeHandler, Arc<MockLlmBackend>) {
        let llm = Arc::new(MockLlmBackend::new(vec![
            Ok(LlmResponse {
                content: None,
                tool_calls: vec![ToolCallRecord {
                    call_id: "c1".into(),
                    extension_id: "flow:x".into(),
                    tool_name: "x".into(),
                    args: json!({}),
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
        let cp = MockConfigProvider::new();
        cp.insert(
            &TenantContext::new("acme", "prod"),
            "a",
            AgentConfig {
                on_text_while_parked: Default::default(),
                agent_id: "a".into(),
                system_prompt: "sys".into(),
                tools: vec![ToolRef {
                    extension_id: "flow:x".into(),
                    tool_name: "x".into(),
                    description: None,
                    input_schema: None,
                    usage_note: None,
                }],
                guardrails: vec![],
                llm: LlmProviderRef {
                    provider: "mock".into(),
                    model: "m".into(),
                    credential_ref: None,
                },
                limits: AgentLimits::default(),
                memory: None,
                knowledge: None,
                conversational: false,
                opening_message: None,
            },
        );
        let policy = mode.map(|m| {
            let mut modes = BindingModes::new();
            modes.insert("flow:x".to_string(), m);
            let mut agents = HashMap::new();
            agents.insert("a".to_string(), modes);
            Arc::new(SharePolicy::new(agents))
        });
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
        .with_flow_source(Some(Arc::new(FlowToolSource::new(Arc::new(OkFlow)))))
        .with_share_policy(policy);
        (
            RuntimeAgentNodeHandler::new(Arc::new(rt), None, None, None),
            llm,
        )
    }

    async fn second_prompt(mode: Option<ShareMode>) -> String {
        let (h, llm) = handler(mode);
        h.execute(
            "acme",
            "prod",
            "a",
            "s1",
            &json!({ "user_text": "go" }),
            false,
            None,
        )
        .await
        .unwrap();
        llm.seen_system_prompts.lock().unwrap()[1].clone()
    }

    #[tokio::test]
    #[serial_test::serial]
    #[allow(unsafe_code)]
    async fn the_handler_opens_a_context_only_for_an_agent_that_shares() {
        // SAFETY: #[serial] serializes env-mutating tests (crate convention).
        unsafe {
            std::env::remove_var("GREENTIC_AW_RUN_CONTEXT");
        }
        assert!(
            second_prompt(Some(ShareMode::Read))
                .await
                .contains("<run_context>")
        );
        assert_eq!(second_prompt(Some(ShareMode::None)).await, "sys");
        assert_eq!(
            second_prompt(None).await,
            "sys",
            "no sidecar: today's prompt"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    #[allow(unsafe_code)]
    async fn the_kill_switch_accepts_every_spelling() {
        /// Removes the variable on drop, so a failing assertion cannot leak it.
        struct EnvGuard;
        impl Drop for EnvGuard {
            #[allow(unsafe_code)]
            fn drop(&mut self) {
                // SAFETY: #[serial] serializes env-mutating tests (crate convention).
                unsafe {
                    std::env::remove_var("GREENTIC_AW_RUN_CONTEXT");
                }
            }
        }
        let _guard = EnvGuard;
        for value in ["0", "false", "OFF", " no ", "No"] {
            // SAFETY: #[serial] serializes env-mutating tests (crate convention).
            unsafe {
                std::env::set_var("GREENTIC_AW_RUN_CONTEXT", value);
            }
            assert!(!run_context_enabled(), "{value:?}");
            assert_eq!(
                second_prompt(Some(ShareMode::Read)).await,
                "sys",
                "{value:?}"
            );
        }
        unsafe {
            std::env::set_var("GREENTIC_AW_RUN_CONTEXT", "1");
        }
        assert!(run_context_enabled());
        for value in ["", "maybe"] {
            unsafe {
                std::env::set_var("GREENTIC_AW_RUN_CONTEXT", value);
            }
            assert!(run_context_enabled(), "{value:?} means ON");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    #[allow(unsafe_code)]
    async fn a_nested_handler_reuses_the_open_context() {
        // SAFETY: #[serial] serializes env-mutating tests (crate convention).
        unsafe {
            std::env::remove_var("GREENTIC_AW_RUN_CONTEXT");
        }
        let prompt = RunContext::scope(
            RunContext::detached("acme"),
            second_prompt(Some(ShareMode::Read)),
        )
        .await;
        assert_eq!(
            prompt, "sys",
            "an open (detached) context is not replaced by a fresh one"
        );
    }
}
