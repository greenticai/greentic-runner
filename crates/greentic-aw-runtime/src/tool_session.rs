//! Resolved tool surface for one agent, usable OUTSIDE the agent loop.
//!
//! The Plan-Act-Observe loop in [`crate::r#loop`] resolves five per-tenant tool
//! catalogs (MCP, component, flow, SoRLa, A2A), advertises the agent's
//! allow-listed tools to the LLM, and dispatches the calls the model makes.
//! A caller that drives its OWN reasoning loop — the in-process deep worker
//! behind `operala.call` — needs exactly that surface and nothing else.
//!
//! [`ToolCatalogs`] is the single place the catalogs are resolved, listed and
//! dispatched through; the agent loop and [`ToolSession`] both call it, so the
//! two cannot drift apart on which catalog a prefix resolves from or on what
//! reaches the model. [`ToolSession`] adds what an external loop cannot do for
//! itself: stable wire names ([`crate::ToolNameCodec`]), the allow-list check,
//! and the idempotency ledger.
//!
//! Host built-ins (`recall_memory`, `remember`/`recall`, `end_conversation`)
//! are deliberately NOT part of a session: they are properties of the agent
//! loop's own conversation state, which an external loop does not have.

use std::sync::Arc;

use greentic_ext_runtime::ExtensionRuntime;
use serde_json::Value;
use tracing::warn;

use crate::AgentRuntime;
use crate::a2a_source::{A2aContinuations, A2aToolCatalog};
use crate::component_source::ComponentToolCatalog;
use crate::config::ToolRef;
use crate::error::{AgentError, ConfigError};
use crate::flow_source::FlowToolCatalog;
use crate::llm::LlmToolSchema;
use crate::mcp_source::McpToolCatalog;
use crate::sorla_source::SorlaToolCatalog;
use crate::state::ToolCallRecord;
use crate::tenant::TenantContext;
use crate::tool_wire_name::{ToolNameCodec, wire_tool_name};
use crate::tools::{
    MissingTool, ToolLedger, dispatch_tool_call_in_conversation, is_tool_allowed,
    list_tools_for_llm, missing_tools,
};

/// The five per-tenant tool catalogs, resolved once for a tenant.
///
/// Every source is infallible (it degrades to an empty catalog on an
/// admin/server failure) and TTL-cached, so resolving once per step does not
/// re-hit the network. A `None` source yields a `None` catalog, which makes
/// that prefix resolve to nothing.
#[derive(Clone, Default)]
pub(crate) struct ToolCatalogs {
    pub(crate) mcp: Option<Arc<McpToolCatalog>>,
    pub(crate) components: Option<Arc<ComponentToolCatalog>>,
    pub(crate) flows: Option<Arc<FlowToolCatalog>>,
    pub(crate) sorla: Option<Arc<SorlaToolCatalog>>,
    pub(crate) a2a: Option<Arc<A2aToolCatalog>>,
}

impl ToolCatalogs {
    /// Resolve every catalog the runtime has a source for, for `tenant`.
    pub(crate) async fn resolve(runtime: &AgentRuntime, tenant: &TenantContext) -> Self {
        let mcp = match runtime.mcp.as_ref() {
            Some(src) => Some(src.catalog(tenant).await),
            None => None,
        };
        let components = match runtime.components.as_ref() {
            Some(src) => Some(src.catalog(tenant).await),
            None => None,
        };
        let flows = match runtime.flows.as_ref() {
            Some(src) => Some(src.catalog(tenant).await),
            None => None,
        };
        let sorla = match runtime.sorla.as_ref() {
            Some(src) => Some(src.catalog(tenant).await),
            None => None,
        };
        // A2A is not tenant-scoped: the source owns its own transport and card
        // cache.
        let a2a = match runtime.a2a.as_ref() {
            Some(src) => Some(src.catalog().await),
            None => None,
        };
        Self {
            mcp,
            components,
            flows,
            sorla,
            a2a,
        }
    }

    /// The LLM-facing schemas for `allowed`, as [`list_tools_for_llm`] builds
    /// them. Unresolvable tools are dropped (and logged) there.
    pub(crate) fn list_for_llm(
        &self,
        ext_runtime: &ExtensionRuntime,
        allowed: &[ToolRef],
    ) -> Vec<LlmToolSchema> {
        list_tools_for_llm(
            ext_runtime,
            self.mcp.as_deref(),
            self.components.as_deref(),
            self.flows.as_deref(),
            self.sorla.as_deref(),
            self.a2a.as_deref(),
            allowed,
        )
    }

    /// Declared tools that will not reach the LLM, with a reason each.
    pub(crate) fn missing(
        &self,
        ext_runtime: &ExtensionRuntime,
        allowed: &[ToolRef],
    ) -> Vec<MissingTool> {
        missing_tools(
            ext_runtime,
            self.mcp.as_deref(),
            self.components.as_deref(),
            self.flows.as_deref(),
            self.sorla.as_deref(),
            self.a2a.as_deref(),
            allowed,
        )
    }

    /// Dispatch one (already allow-list checked) call.
    ///
    /// `a2a_continuations` is the caller's own per-conversation record of the
    /// remote A2A exchanges it has open. The agent loop passes the one on its
    /// [`crate::state::ConversationState`], which is what makes a follow-up
    /// resume the same remote task.
    pub(crate) async fn dispatch(
        &self,
        ext_runtime: Arc<ExtensionRuntime>,
        call: ToolCallRecord,
        tenant: &TenantContext,
        a2a_continuations: Option<&mut A2aContinuations>,
    ) -> Result<Value, AgentError> {
        dispatch_tool_call_in_conversation(
            ext_runtime,
            self.mcp.clone(),
            self.components.clone(),
            self.flows.clone(),
            self.sorla.clone(),
            self.a2a.clone(),
            call,
            tenant,
            a2a_continuations,
        )
        .await
    }
}

/// One tool as an external reasoning loop should present it to its model.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolSessionSchema {
    /// The provider-safe function name — pass it back to [`ToolSession::call`].
    pub wire_name: String,
    pub description: String,
    /// JSON schema of the tool's arguments.
    pub parameters: Value,
}

/// Why [`ToolSession::call`] refused or failed.
#[derive(Debug, thiserror::Error)]
pub enum ToolSessionError {
    /// The name decodes to a tool this agent did not declare. The loop's
    /// equivalent is `AgentStep::ToolCallBlocked`.
    #[error("tool '{wire_name}' ({extension_id}/{tool_name}) is not allowed for this agent")]
    NotAllowed {
        wire_name: String,
        extension_id: String,
        tool_name: String,
    },
    /// The dispatch itself failed (a WASM extension error). The `mcp:`,
    /// `component:`, `flow:`, `sorla:` and `a2a:` arms never fail this way —
    /// they report errors as an `{"error": …}` value instead.
    #[error(transparent)]
    Dispatch(#[from] AgentError),
}

/// The resolved tool surface of one agent for one tenant.
///
/// Built by [`AgentRuntime::tool_session`]. Dispatch goes through the same
/// catalogs, tenant context (secrets scope, caller stamp, project/unit id) and
/// idempotency ledger the agent loop uses.
pub struct ToolSession {
    ext_runtime: Arc<ExtensionRuntime>,
    catalogs: ToolCatalogs,
    tenant: TenantContext,
    allowed: Vec<ToolRef>,
    schemas: Vec<LlmToolSchema>,
    codec: ToolNameCodec,
    ledger: Arc<dyn ToolLedger>,
    session_id: Option<String>,
    /// The remote A2A exchanges this session has open.
    ///
    /// In memory, and only for the life of the session — an external
    /// reasoning loop keeps no [`crate::state::ConversationState`], so there
    /// is nowhere durable to put them. That is enough for the case that
    /// matters here: a deep worker that asks a remote agent something, gets
    /// `input-required`, and answers it within the same run. It is NOT
    /// enough across a restart, and saying so is better than pretending the
    /// agent loop's guarantee extends here.
    a2a: tokio::sync::Mutex<A2aContinuations>,
}

impl ToolSession {
    /// Scope the idempotency ledger to a conversation. Without a session id
    /// the ledger is not consulted — its keys are per-session, and an
    /// invented one would never match a replay anyway.
    #[must_use]
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        let id = session_id.into();
        self.session_id = (!id.trim().is_empty()).then_some(id);
        self
    }

    /// Every tool the model may call, in declaration order, each under a
    /// UNIQUE wire name. A second tool encoding to a name already listed —
    /// the same tool declared twice, or a digest collision — is left out, so
    /// the list agrees with the codec, which keeps the first binding.
    #[must_use]
    pub fn schemas(&self) -> Vec<ToolSessionSchema> {
        let mut seen = std::collections::HashSet::new();
        self.schemas
            .iter()
            .filter_map(|s| {
                let wire_name = wire_tool_name(&s.extension_id, &s.tool_name);
                seen.insert(wire_name.clone()).then(|| ToolSessionSchema {
                    wire_name,
                    description: s.description.clone(),
                    parameters: s.parameters.clone(),
                })
            })
            .collect()
    }

    /// True when no declared tool resolved to a schema.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    /// Dispatch a call the model made, by wire name, under a fresh call id.
    pub async fn call(&self, wire_name: &str, args: Value) -> Result<Value, ToolSessionError> {
        let call_id = format!("dw-{}", uuid::Uuid::new_v4());
        self.call_with_id(&call_id, wire_name, args).await
    }

    /// Dispatch a call under a caller-supplied `call_id`. With a session id
    /// set, a result already recorded for that id is returned without
    /// re-dispatching, and a successful result is recorded — the same
    /// idempotency the agent loop gives every call.
    pub async fn call_with_id(
        &self,
        call_id: &str,
        wire_name: &str,
        args: Value,
    ) -> Result<Value, ToolSessionError> {
        let (extension_id, tool_name) = self.codec.decode(wire_name);
        let call = ToolCallRecord {
            call_id: call_id.to_string(),
            extension_id,
            tool_name,
            args,
        };
        if !is_tool_allowed(&call, &self.allowed) {
            warn!(
                wire_name, extension = %call.extension_id, tool = %call.tool_name,
                "tool session refused a call outside the agent's allow-list"
            );
            return Err(ToolSessionError::NotAllowed {
                wire_name: wire_name.to_string(),
                extension_id: call.extension_id,
                tool_name: call.tool_name,
            });
        }

        if let Some(session_id) = self.session_id.as_deref() {
            match self.ledger.get(&self.tenant, session_id, call_id).await {
                Ok(Some(cached)) => return Ok(cached),
                Ok(None) => {}
                Err(e) => {
                    warn!(error = %e, "ledger get failed; dispatching without idempotency");
                }
            }
        }

        let result = {
            let mut a2a = self.a2a.lock().await;
            self.catalogs
                .dispatch(self.ext_runtime.clone(), call, &self.tenant, Some(&mut a2a))
                .await?
        };

        if let Some(session_id) = self.session_id.as_deref()
            && let Err(e) = self
                .ledger
                .record(&self.tenant, session_id, call_id, result.clone())
                .await
        {
            warn!(error = %e, "ledger record failed; continuing");
        }
        Ok(result)
    }
}

impl AgentRuntime {
    /// Resolve `tools` for `tenant` into a [`ToolSession`] an external
    /// reasoning loop can list and call.
    ///
    /// Resolution is identical to one agent-loop step: the same catalogs and
    /// the same schemas, through the shared [`ToolCatalogs`]. Tools that do
    /// not resolve are dropped from [`ToolSession::schemas`] (and
    /// logged) exactly as the loop drops them from the LLM request.
    pub async fn tool_session(&self, tenant: &TenantContext, tools: &[ToolRef]) -> ToolSession {
        let catalogs = ToolCatalogs::resolve(self, tenant).await;
        let schemas = catalogs.list_for_llm(&self.ext_runtime, tools);
        let codec = ToolNameCodec::for_tools(&schemas);
        ToolSession {
            ext_runtime: self.ext_runtime.clone(),
            catalogs,
            tenant: tenant.clone(),
            allowed: tools.to_vec(),
            schemas,
            codec,
            ledger: self.ledger.clone(),
            session_id: None,
            a2a: tokio::sync::Mutex::new(A2aContinuations::default()),
        }
    }

    /// [`AgentRuntime::tool_session`] for the tools `agent_id` declares, with
    /// the agent's config resolved through the runtime's own config provider —
    /// the same lookup (registry layer, manifest tool overlay, cache) an agent
    /// step makes, so the session sees the tool list the loop would.
    pub async fn tool_session_for_agent(
        &self,
        tenant: &TenantContext,
        agent_id: &str,
    ) -> Result<ToolSession, ConfigError> {
        let config = self.config_provider.agent_config(tenant, agent_id).await?;
        Ok(self.tool_session(tenant, &config.tools).await)
    }
}

#[cfg(all(test, feature = "test-mock"))]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;
    use crate::component_source::{ComponentInvoker, ComponentOperation, ComponentToolSource};
    use crate::cost::MockTokenMeter;
    use crate::flow_source::{FlowInvoker, FlowOperation, FlowToolSource};
    use crate::kv::MemoryKv;
    use crate::mock::{MockAgentStateStore, MockConfigProvider, MockLlmBackend, MockTelemetry};
    use crate::tools::KvToolLedger;

    /// An OCI component ref: its colon, slashes and `@` force the SANITISED
    /// wire-name branch, which only the codec can decode.
    const OCI_REF: &str = "oci://ghcr.io/acme/refund@sha256:abc";

    /// Records every invocation and answers with a counter, so a test can tell
    /// a fresh dispatch from a ledger replay.
    #[derive(Default)]
    struct RecordingComponent {
        calls: Mutex<Vec<(String, String, Value)>>,
    }

    impl ComponentInvoker for RecordingComponent {
        fn list_operations(&self) -> Vec<ComponentOperation> {
            vec![ComponentOperation {
                component_ref: OCI_REF.into(),
                operation: "issue_refund".into(),
                description: "Issue a refund".into(),
                parameters: json!({
                    "type": "object",
                    "properties": { "order_id": { "type": "string" } }
                }),
            }]
        }

        fn invoke<'a>(
            &'a self,
            component_ref: &'a str,
            operation: &'a str,
            args_json: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            let args: Value = serde_json::from_str(args_json).unwrap_or(Value::Null);
            let mut calls = self.calls.lock().unwrap();
            calls.push((component_ref.into(), operation.into(), args));
            let n = calls.len();
            Box::pin(async move { Ok(json!({ "refund_id": format!("r-{n}") })) })
        }
    }

    struct EchoFlow;

    impl FlowInvoker for EchoFlow {
        fn list_flows(&self) -> Vec<FlowOperation> {
            vec![FlowOperation {
                flow_ref: "lookup".into(),
                description: "Look things up".into(),
                parameters: json!({ "type": "object", "properties": { "q": { "type": "string" } } }),
            }]
        }

        fn invoke<'a>(
            &'a self,
            flow_ref: &'a str,
            args_json: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            let out = json!({ "flow": flow_ref, "args": args_json });
            Box::pin(async move { Ok(out) })
        }
    }

    fn tool_ref(extension_id: &str, tool_name: &str) -> ToolRef {
        ToolRef {
            extension_id: extension_id.into(),
            tool_name: tool_name.into(),
            description: None,
            input_schema: None,
            usage_note: None,
        }
    }

    fn component_ref() -> ToolRef {
        tool_ref(&format!("component:{OCI_REF}"), "issue_refund")
    }

    fn runtime(component: Arc<RecordingComponent>) -> AgentRuntime {
        let ledger = Arc::new(KvToolLedger::new(Arc::new(MemoryKv::new())));
        AgentRuntime::new(
            Arc::new(MockConfigProvider::new()),
            Arc::new(MockAgentStateStore::new()),
            Arc::new(ExtensionRuntime::for_test().unwrap()),
            Arc::new(MockLlmBackend::new(vec![])),
            Arc::new(MockTelemetry::new()),
            Arc::new(MockTokenMeter::new(0)),
            ledger,
            None,
        )
        .with_component_source(Some(Arc::new(ComponentToolSource::new(component))))
        .with_flow_source(Some(Arc::new(FlowToolSource::new(Arc::new(EchoFlow)))))
    }

    fn wire_of(session: &ToolSession, description: &str) -> String {
        session
            .schemas()
            .into_iter()
            .find(|s| s.description == description)
            .unwrap_or_else(|| panic!("no schema described {description:?}"))
            .wire_name
    }

    #[tokio::test]
    async fn schemas_list_every_resolvable_tool_under_a_wire_safe_name() {
        let rt = runtime(Arc::new(RecordingComponent::default()));
        let tenant = TenantContext::new("acme", "prod");
        let tools = vec![
            component_ref(),
            tool_ref("flow:lookup", "look_up"),
            // Declared twice: listed once, since names must be unique.
            tool_ref("flow:lookup", "look_up"),
            // Declared but resolvable from no catalog: dropped, as the loop does.
            tool_ref("component:ghost", "nothing"),
        ];
        let session = rt.tool_session(&tenant, &tools).await;
        let schemas = session.schemas();
        assert_eq!(schemas.len(), 2, "got {schemas:?}");
        for s in &schemas {
            assert!(crate::is_wire_safe(&s.wire_name), "{}", s.wire_name);
        }
        let refund = schemas
            .iter()
            .find(|s| s.description == "Issue a refund")
            .expect("component tool listed");
        assert_eq!(
            refund.parameters["properties"]["order_id"]["type"],
            "string"
        );
        assert!(!session.is_empty());
    }

    #[tokio::test]
    async fn a_sanitised_wire_name_decodes_back_to_the_component_and_dispatches() {
        let component = Arc::new(RecordingComponent::default());
        let rt = runtime(component.clone());
        let tenant = TenantContext::new("acme", "prod");
        let session = rt.tool_session(&tenant, &[component_ref()]).await;
        let wire = wire_of(&session, "Issue a refund");
        assert_ne!(
            wire,
            crate::encode_tool_name(&format!("component:{OCI_REF}"), "issue_refund"),
            "the fixture must exercise the sanitised branch"
        );

        let out = session
            .call(&wire, json!({ "order_id": "o-9" }))
            .await
            .expect("an allowed component call dispatches");
        assert_eq!(out, json!({ "refund_id": "r-1" }));

        let calls = component.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, OCI_REF);
        assert_eq!(calls[0].1, "issue_refund");
        assert_eq!(calls[0].2["order_id"], "o-9");
        // Same dispatch path as the loop: the component arm stamps the caller.
        assert_eq!(calls[0].2["_caller"]["user_verified"], false);
    }

    #[tokio::test]
    async fn a_session_for_an_agent_uses_the_tools_its_config_declares() {
        let component = Arc::new(RecordingComponent::default());
        let tenant = TenantContext::new("acme", "prod");
        let configs = MockConfigProvider::new();
        let cfg: crate::config::AgentConfig = serde_json::from_value(json!({
            "agent_id": "worker",
            "system_prompt": "sys",
            "tools": [{ "extension_id": "flow:lookup", "tool_name": "look_up" }],
            "llm": { "provider": "mock", "model": "m" }
        }))
        .unwrap();
        configs.insert(&tenant, "worker", cfg);
        let rt = AgentRuntime::new(
            Arc::new(configs),
            Arc::new(MockAgentStateStore::new()),
            Arc::new(ExtensionRuntime::for_test().unwrap()),
            Arc::new(MockLlmBackend::new(vec![])),
            Arc::new(MockTelemetry::new()),
            Arc::new(MockTokenMeter::new(0)),
            Arc::new(crate::mock::NoopToolLedger),
            None,
        )
        .with_component_source(Some(Arc::new(ComponentToolSource::new(component))))
        .with_flow_source(Some(Arc::new(FlowToolSource::new(Arc::new(EchoFlow)))));

        let session = rt.tool_session_for_agent(&tenant, "worker").await.unwrap();
        let names: Vec<String> = session
            .schemas()
            .into_iter()
            .map(|s| s.description)
            .collect();
        assert_eq!(names, vec!["Look things up".to_string()]);
        assert!(rt.tool_session_for_agent(&tenant, "nobody").await.is_err());
    }

    #[tokio::test]
    async fn a_flow_tool_dispatches_through_the_shared_catalogs() {
        let rt = runtime(Arc::new(RecordingComponent::default()));
        let tenant = TenantContext::new("acme", "prod");
        let session = rt
            .tool_session(&tenant, &[tool_ref("flow:lookup", "look_up")])
            .await;
        let wire = wire_of(&session, "Look things up");
        let out = session.call(&wire, json!({ "q": "x" })).await.unwrap();
        assert_eq!(out["flow"], "lookup");
    }

    #[tokio::test]
    async fn a_tool_outside_the_allow_list_is_refused_without_dispatching() {
        let component = Arc::new(RecordingComponent::default());
        let rt = runtime(component.clone());
        let tenant = TenantContext::new("acme", "prod");
        // The catalog CAN resolve the component, but this agent declared only
        // the flow — so its name must not dispatch.
        let session = rt
            .tool_session(&tenant, &[tool_ref("flow:lookup", "look_up")])
            .await;
        let component_wire = wire_tool_name(&format!("component:{OCI_REF}"), "issue_refund");
        let err = session
            .call(&component_wire, json!({ "order_id": "o-1" }))
            .await
            .expect_err("an undeclared tool must be refused");
        assert!(
            matches!(err, ToolSessionError::NotAllowed { .. }),
            "got {err:?}"
        );
        // An invented name that decodes via the plain split is refused too.
        let err = session
            .call("greentic_DOT_mail_FN_send", json!({}))
            .await
            .expect_err("an invented tool must be refused");
        match err {
            ToolSessionError::NotAllowed {
                extension_id,
                tool_name,
                ..
            } => {
                assert_eq!(extension_id, "greentic.mail");
                assert_eq!(tool_name, "send");
            }
            other => panic!("expected NotAllowed, got {other:?}"),
        }
        assert!(component.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn with_a_session_id_a_replayed_call_id_reuses_the_ledger_result() {
        let component = Arc::new(RecordingComponent::default());
        let rt = runtime(component.clone());
        let tenant = TenantContext::new("acme", "prod");
        let session = rt
            .tool_session(&tenant, &[component_ref()])
            .await
            .with_session_id("s-1");
        let wire = wire_of(&session, "Issue a refund");
        let first = session
            .call_with_id("c-1", &wire, json!({ "order_id": "o-1" }))
            .await
            .unwrap();
        let replay = session
            .call_with_id("c-1", &wire, json!({ "order_id": "o-1" }))
            .await
            .unwrap();
        assert_eq!(first, replay);
        assert_eq!(
            component.calls.lock().unwrap().len(),
            1,
            "replay must not re-dispatch"
        );

        // Fresh ids, and no session scoping, dispatch every time.
        let unscoped = rt.tool_session(&tenant, &[component_ref()]).await;
        unscoped
            .call_with_id("c-1", &wire, json!({ "order_id": "o-1" }))
            .await
            .unwrap();
        unscoped
            .call_with_id("c-1", &wire, json!({ "order_id": "o-1" }))
            .await
            .unwrap();
        assert_eq!(component.calls.lock().unwrap().len(), 3);
    }
}
