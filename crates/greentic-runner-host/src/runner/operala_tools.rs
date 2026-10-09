//! Tool access for in-process deep workers (`operala.call`).
//!
//! A deep worker runs its own reasoning loop inside
//! `greentic-dw-operala-invoker`, so it cannot use the agent loop's tool
//! dispatch directly. This module hands it the SAME tool surface a `dw.agent`
//! step sees: the [`AgentRuntime`] built for this `TenantRuntime` (catalogs,
//! secrets scope, ledger) resolves the worker's declared tools into a
//! [`ToolSession`], and [`AwDeepWorkerTools`] adapts that session to the
//! deep-worker tool contract.
//!
//! The contract itself is `greentic_dw_operala_invoker::DeepWorkerTools`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use greentic_aw_runtime::config::AgentConfig;
use greentic_aw_runtime::{AgentRuntime, TenantContext, ToolSession, ToolSessionError};
use greentic_dw_operala_invoker::{DeepWorkerTools, ToolSpec};
use serde_json::{Value, json};

/// The deep-worker tool contract over an aw-runtime [`ToolSession`].
pub struct AwDeepWorkerTools {
    session: ToolSession,
}

impl AwDeepWorkerTools {
    #[must_use]
    pub fn new(session: ToolSession) -> Self {
        Self { session }
    }
}

#[async_trait]
impl DeepWorkerTools for AwDeepWorkerTools {
    fn list(&self) -> Vec<ToolSpec> {
        self.session
            .schemas()
            .into_iter()
            .map(|schema| ToolSpec {
                name: schema.wire_name,
                description: schema.description,
                parameters: schema.parameters,
            })
            .collect()
    }

    /// A name outside the agent's allow-list is answered as a tool-level
    /// error the model can read — the same observation the agent loop records
    /// for a blocked call. Only a failed DISPATCH is an `Err`: the invoker
    /// then does not cache it, so a transient extension failure stays
    /// retryable, exactly as the loop leaves it out of the ledger.
    async fn call(&self, name: &str, args: Value) -> Result<Value> {
        match self.session.call(name, args).await {
            Ok(value) => Ok(value),
            Err(ToolSessionError::NotAllowed { .. }) => Ok(json!({
                "error": format!("tool '{name}' is not allowed for this agent")
            })),
            Err(ToolSessionError::Dispatch(error)) => Err(anyhow::Error::new(error)),
        }
    }
}

/// Why an `operala.call` could not be tied to one of the pack's agents.
#[derive(Debug, PartialEq, Eq)]
pub enum UnresolvedAgent {
    /// The call named an agent (`input.agent_id`, `target` or `operation`)
    /// this runtime does not carry.
    UnknownExplicit(String),
    /// The runtime carries no agents at all.
    NoAgents,
    /// Nothing named an agent and more than one exists.
    Ambiguous(usize),
}

/// The dispatch verb of the `{await, operation, input}` contract. It names an
/// action, not an agent, so it never counts as naming one.
const RUN_OPERATION: &str = "run";

/// Which agent an `operala.call` dispatch belongs to.
///
/// The FIRST non-empty name among `input.agent_id`, the node's `target` and
/// its `operation` decides, and it must name an agent this runtime carries;
/// an unknown name is a refusal, never a fall-through. In production `target`
/// is the node's operation — the worker id — so it is almost always set, and
/// falling back to "the only agent" for an unknown one would hand worker D
/// the tools, secrets and unit of worker A.
///
/// Only when nothing names an agent at all (`operation` empty or `run`) does
/// the single agent of the runtime apply.
pub fn resolve_tool_agent<'a>(
    agent_ids: &'a [String],
    input: &Value,
    target: &str,
    operation: &str,
) -> std::result::Result<&'a str, UnresolvedAgent> {
    let explicit = input
        .get("agent_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let operation = operation.trim();
    let operation = if operation == RUN_OPERATION {
        ""
    } else {
        operation
    };
    let named = [explicit.trim(), target.trim(), operation]
        .into_iter()
        .find(|name| !name.is_empty());
    if let Some(name) = named {
        return agent_ids
            .iter()
            .find(|id| id.as_str() == name)
            .map(String::as_str)
            .ok_or_else(|| UnresolvedAgent::UnknownExplicit(name.to_string()));
    }
    match agent_ids {
        [] => Err(UnresolvedAgent::NoAgents),
        [only] => Ok(only.as_str()),
        many => Err(UnresolvedAgent::Ambiguous(many.len())),
    }
}

/// Everything an `operala.call` needs to resolve its worker's tools: the
/// `AgentRuntime` of this `TenantRuntime`, the pack's agent ids, and the
/// deployed unit (`bundle_id`) every `dw.agent` step is stamped with.
pub struct OperalaToolContext {
    runtime: Arc<AgentRuntime>,
    agent_ids: Vec<String>,
    project_id: Option<String>,
}

impl OperalaToolContext {
    #[must_use]
    pub fn new(
        runtime: Arc<AgentRuntime>,
        agents: &HashMap<String, AgentConfig>,
        project_id: Option<String>,
    ) -> Self {
        let mut agent_ids: Vec<String> = agents.keys().cloned().collect();
        agent_ids.sort();
        Self {
            runtime,
            agent_ids,
            project_id,
        }
    }

    /// The tenant context a `dw.agent` step of this runtime would run under,
    /// minus the caller (an `operala.call` carries no stamped caller block, so
    /// its tool calls are stamped anonymous — the restrictive direction).
    fn tenant_context(&self, tenant: &str, env: &str) -> TenantContext {
        TenantContext::new(tenant, env).with_project_id(self.project_id.clone())
    }

    /// The deep worker's tools for one dispatch, or `None` — with a warning —
    /// when the call cannot be tied to an agent or its config cannot be read.
    /// `None` also when the agent resolves no tools at all: the worker then
    /// runs exactly as it did before tools existed.
    pub async fn tools_for(
        &self,
        tenant: &str,
        env: &str,
        target: &str,
        operation: &str,
        input: &Value,
    ) -> Option<Arc<AwDeepWorkerTools>> {
        let agent_id = match resolve_tool_agent(&self.agent_ids, input, target, operation) {
            Ok(agent_id) => agent_id,
            Err(reason) => {
                tracing::warn!(
                    target,
                    operation,
                    ?reason,
                    "operala.call could not be tied to an agent; the deep worker runs without tools"
                );
                return None;
            }
        };
        let tenant_ctx = self.tenant_context(tenant, env);
        let session = match self
            .runtime
            .tool_session_for_agent(&tenant_ctx, agent_id)
            .await
        {
            // No session id on purpose. The invoker names no call id, so
            // every call gets a fresh one and a ledger lookup could never
            // hit; with a session id set, each call would only WRITE a ledger
            // record nobody reads. Replay protection within a run comes from
            // the dw invoker's own per-run cache, keyed by (name, canonical
            // args).
            Ok(session) => session,
            Err(error) => {
                tracing::warn!(
                    agent_id, %error,
                    "operala.call agent config could not be read; the deep worker runs without tools"
                );
                return None;
            }
        };
        if session.is_empty() {
            return None;
        }
        Some(Arc::new(AwDeepWorkerTools::new(session)))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn ids(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn an_explicit_agent_id_wins_over_target_and_operation() {
        let agents = ids(&["planner", "writer"]);
        let got = resolve_tool_agent(
            &agents,
            &json!({ "agent_id": "writer" }),
            "planner",
            "planner",
        );
        assert_eq!(got, Ok("writer"));
    }

    #[test]
    fn an_unknown_explicit_agent_id_is_refused_not_redirected() {
        // The only agent must NOT be substituted: the caller named a
        // different one, and handing it this agent's tools is the failure.
        let agents = ids(&["planner"]);
        let got = resolve_tool_agent(&agents, &json!({ "agent_id": "ghost" }), "", "run");
        assert_eq!(got, Err(UnresolvedAgent::UnknownExplicit("ghost".into())));
    }

    #[test]
    fn an_unknown_target_is_refused_even_with_a_single_agent() {
        // Production shape: `target` is the worker id. Deep worker D must
        // never run with the sole agent A's tools, secrets and unit.
        let agents = ids(&["A"]);
        let got = resolve_tool_agent(&agents, &json!({}), "D", "run");
        assert_eq!(got, Err(UnresolvedAgent::UnknownExplicit("D".into())));
    }

    #[test]
    fn an_unknown_operation_is_refused_even_with_a_single_agent() {
        let agents = ids(&["A"]);
        let got = resolve_tool_agent(&agents, &json!({}), "", "D");
        assert_eq!(got, Err(UnresolvedAgent::UnknownExplicit("D".into())));
    }

    #[test]
    fn the_first_name_decides_and_a_later_match_does_not_rescue_it() {
        let agents = ids(&["planner", "writer"]);
        assert_eq!(
            resolve_tool_agent(&agents, &json!({}), "writer", "planner"),
            Ok("writer")
        );
        assert_eq!(
            resolve_tool_agent(&agents, &json!({}), "", "planner"),
            Ok("planner")
        );
        assert_eq!(
            resolve_tool_agent(&agents, &json!({}), "operala", "planner"),
            Err(UnresolvedAgent::UnknownExplicit("operala".into()))
        );
    }

    #[test]
    fn the_only_agent_is_used_only_when_nothing_names_one() {
        let agents = ids(&["solo"]);
        assert_eq!(
            resolve_tool_agent(&agents, &json!({ "agent_id": "  " }), "", "run"),
            Ok("solo")
        );
        assert_eq!(resolve_tool_agent(&agents, &json!({}), " ", ""), Ok("solo"));
    }

    #[test]
    fn several_agents_and_no_name_is_ambiguous() {
        let agents = ids(&["a", "b", "c"]);
        assert_eq!(
            resolve_tool_agent(&agents, &json!({}), "", "run"),
            Err(UnresolvedAgent::Ambiguous(3))
        );
    }

    #[test]
    fn no_agents_resolves_nothing() {
        assert_eq!(
            resolve_tool_agent(&[], &json!({}), "", "run"),
            Err(UnresolvedAgent::NoAgents)
        );
    }

    mod session {
        use std::future::Future;
        use std::pin::Pin;

        use greentic_aw_runtime::cost::MockTokenMeter;
        use greentic_aw_runtime::mock::{
            MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry, NoopToolLedger,
        };
        use greentic_aw_runtime::{FlowInvoker, FlowOperation, FlowToolSource};
        use serde_json::json;

        use super::super::*;

        struct EchoFlow;

        impl FlowInvoker for EchoFlow {
            fn list_flows(&self) -> Vec<FlowOperation> {
                vec![FlowOperation {
                    flow_ref: "lookup".into(),
                    description: "Look things up".into(),
                    parameters: json!({ "type": "object" }),
                }]
            }

            fn invoke<'a>(
                &'a self,
                flow_ref: &'a str,
                args_json: &'a str,
            ) -> Pin<Box<dyn Future<Output = std::result::Result<Value, String>> + Send + 'a>>
            {
                let out = json!({ "flow": flow_ref, "args": args_json });
                Box::pin(async move { Ok(out) })
            }
        }

        fn agent(id: &str, tools: Value) -> AgentConfig {
            serde_json::from_value(json!({
                "agent_id": id,
                "system_prompt": "sys",
                "tools": tools,
                "llm": { "provider": "mock", "model": "m" }
            }))
            .expect("agent config")
        }

        fn context(agents: &HashMap<String, AgentConfig>) -> OperalaToolContext {
            let configs = MockConfigProvider::new();
            let tenant = TenantContext::new("acme", "prod");
            for (id, cfg) in agents {
                configs.insert(&tenant, id, cfg.clone());
            }
            let runtime = AgentRuntime::new(
                Arc::new(configs),
                Arc::new(MockAgentStateStore::new()),
                Arc::new(greentic_ext_runtime::ExtensionRuntime::for_test().expect("ext runtime")),
                Arc::new(MockLlmBackend::new(vec![])),
                Arc::new(MockTelemetry::new()),
                Arc::new(MockTokenMeter::new(0)),
                Arc::new(NoopToolLedger),
                None,
            )
            .with_flow_source(Some(Arc::new(FlowToolSource::new(Arc::new(EchoFlow)))));
            OperalaToolContext::new(Arc::new(runtime), agents, Some("bundle-1".into()))
        }

        #[tokio::test]
        async fn the_resolved_agents_tools_are_listed_and_callable() {
            let mut agents = HashMap::new();
            agents.insert(
                "researcher".to_string(),
                agent(
                    "researcher",
                    json!([{ "extension_id": "flow:lookup", "tool_name": "look_up" }]),
                ),
            );
            let ctx = context(&agents);
            let tools = ctx
                .tools_for("acme", "prod", "researcher", "run", &json!({}))
                .await
                .expect("the only agent's tools resolve");
            let listed = tools.list();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].description, "Look things up");
            let out = tools
                .call(&listed[0].name, json!({ "q": "x" }))
                .await
                .expect("a declared tool dispatches");
            assert_eq!(out["flow"], "lookup");
            let refused = tools
                .call("greentic_DOT_mail_FN_send", json!({}))
                .await
                .expect("an undeclared tool is a tool-level answer, not an Err");
            assert!(
                refused["error"]
                    .as_str()
                    .is_some_and(|e| e.contains("not allowed")),
                "got {refused}"
            );
        }

        #[tokio::test]
        async fn an_agent_without_resolvable_tools_yields_none() {
            let mut agents = HashMap::new();
            agents.insert("bare".to_string(), agent("bare", json!([])));
            let ctx = context(&agents);
            assert!(
                ctx.tools_for("acme", "prod", "", "run", &json!({}))
                    .await
                    .is_none()
            );
        }

        #[tokio::test]
        async fn an_ambiguous_call_yields_none() {
            let mut agents = HashMap::new();
            let tools = json!([{ "extension_id": "flow:lookup", "tool_name": "look_up" }]);
            agents.insert("a".to_string(), agent("a", tools.clone()));
            agents.insert("b".to_string(), agent("b", tools));
            let ctx = context(&agents);
            assert!(
                ctx.tools_for("acme", "prod", "", "run", &json!({}))
                    .await
                    .is_none()
            );
        }

        /// Replays queued replies, one per `chat`; a reply of the form
        /// `!tool:<name>:<json>` becomes a single tool call. Records the tool
        /// names each request offered.
        struct ScriptedLlm {
            replies: std::sync::Mutex<std::collections::VecDeque<String>>,
            offered: std::sync::Mutex<Vec<Vec<String>>>,
        }

        #[async_trait]
        impl greentic_llm::LlmProvider for ScriptedLlm {
            fn capabilities(&self) -> greentic_llm::Capabilities {
                greentic_llm::Capabilities {
                    chat: true,
                    tools: true,
                    streaming: false,
                    vision: false,
                    system_prompt: true,
                }
            }

            fn provider_name(&self) -> &'static str {
                "scripted"
            }

            fn model(&self) -> &str {
                "scripted-model"
            }

            async fn chat(
                &self,
                req: greentic_llm::ChatRequest,
            ) -> std::result::Result<greentic_llm::ChatResponse, greentic_llm::LlmError>
            {
                self.offered
                    .lock()
                    .expect("lock")
                    .push(req.tools.iter().map(|tool| tool.name.clone()).collect());
                let reply = self
                    .replies
                    .lock()
                    .expect("lock")
                    .pop_front()
                    .unwrap_or_default();
                if let Some(call) = reply.strip_prefix("!tool:") {
                    let (name, args) = call.split_once(':').expect("!tool:<name>:<json>");
                    return Ok(greentic_llm::ChatResponse {
                        content: String::new(),
                        tool_calls: vec![greentic_llm::ToolCall {
                            id: "call-1".into(),
                            name: name.to_string(),
                            arguments: serde_json::from_str(args).expect("tool args"),
                        }],
                        finish_reason: greentic_llm::FinishReason::ToolCalls,
                        usage: None,
                    });
                }
                Ok(greentic_llm::ChatResponse {
                    content: reply,
                    tool_calls: vec![],
                    finish_reason: greentic_llm::FinishReason::Stop,
                    usage: None,
                })
            }

            async fn chat_stream(
                &self,
                _req: greentic_llm::ChatRequest,
            ) -> std::result::Result<greentic_llm::ChatStream, greentic_llm::LlmError> {
                use futures::StreamExt;
                Ok(
                    futures::stream::iter(vec![Ok(greentic_llm::StreamEvent::Done {
                        finish_reason: greentic_llm::FinishReason::Stop,
                    })])
                    .boxed(),
                )
            }
        }

        /// A one-step plan in the planner's serde shape.
        fn one_step_plan() -> String {
            json!({
                "plan_id": "p", "goal": "g", "status": "active", "revision": 1,
                "assumptions": [], "constraints": [],
                "success_criteria": ["task completed"],
                "steps": [{
                    "step_id": "s1", "title": "Step s1", "kind": "tool_call",
                    "status": "ready", "depends_on": [], "assigned_agent": null,
                    "inputs_schema_ref": null, "output_schema_ref": null, "retry_count": 0
                }],
                "edges": [], "metadata": {}
            })
            .to_string()
        }

        /// The whole `operala.call` path: the handler resolves the worker's
        /// tools through the AgentRuntime, hands them to the invoker, the
        /// scripted model calls one by its wire name, and the flow's result
        /// reaches the reply's input.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn the_handler_hands_the_workers_tools_to_the_deep_worker() {
            use crate::runner::operala_node::{OperalaNodeHandler, RuntimeOperalaNodeHandler};

            let mut agents = HashMap::new();
            agents.insert(
                "researcher".to_string(),
                agent(
                    "researcher",
                    json!([{ "extension_id": "flow:lookup", "tool_name": "look_up" }]),
                ),
            );
            let ctx = Arc::new(context(&agents));
            let wire = greentic_aw_runtime::wire_tool_name("flow:lookup", "look_up");
            let llm = Arc::new(ScriptedLlm {
                replies: std::sync::Mutex::new(
                    vec![
                        one_step_plan(),
                        r#"[{"step_id":"s1","action":"execute"}]"#.to_string(),
                        format!("!tool:{wire}:{{\"q\":\"order 42\"}}"),
                        "Order 42 was found.".to_string(),
                        "[]".to_string(),
                        "Order 42 exists.".to_string(),
                    ]
                    .into(),
                ),
                offered: std::sync::Mutex::new(Vec::new()),
            });
            let scripted = Arc::clone(&llm);
            let handler = RuntimeOperalaNodeHandler::new("unused".into(), None, None, None)
                .with_tool_context(Some(ctx))
                .with_llm_factory(Arc::new(move |_provider: &str, _model: &str| {
                    Ok(Arc::clone(&scripted) as Arc<dyn greentic_llm::LlmProvider>)
                }));

            let out = handler
                .execute(
                    "acme",
                    "prod",
                    "researcher",
                    "run",
                    "s-1",
                    &json!({
                        "goal": "find order 42",
                        "llm": { "provider": "openai", "model": "m" },
                        "deep_worker": { "reflection": false }
                    }),
                )
                .await
                .expect("operala.call runs");

            assert_eq!(out["ok"], true, "{out}");
            assert_eq!(out["reply"], "Order 42 exists.");
            assert_eq!(out["output"]["tool_calls_used"], 1, "{out}");
            let offered = llm.offered.lock().expect("lock").clone();
            assert!(
                offered.iter().any(|names| names == &vec![wire.clone()]),
                "the deep worker must be offered the agent's tool, got {offered:?}"
            );
        }
    }
}
