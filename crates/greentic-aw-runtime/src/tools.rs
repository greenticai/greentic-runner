//! Tool resolution + dispatch helpers.
//!
//! `ExtensionRuntime::invoke_tool` is a synchronous `fn` performing
//! Wasmtime WASM dispatch (CPU-bound, may block seconds). Every call
//! site MUST wrap it in `tokio::task::spawn_blocking` (spec §5.3). This
//! module calls it for agent tool dispatch; `llm_extension::RuntimeInvoker`
//! also calls it (likewise wrapped) when the LLM runs through an extension.
//!
//! Each tool call is recorded in Redis by `tool_call_id` BEFORE the
//! result is committed so a state-save failure cannot cause a
//! double-dispatch on the next `step()` (idempotency ledger).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use greentic_ext_runtime::ExtensionRuntime;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};

use crate::component_source::ComponentToolCatalog;
use crate::config::ToolRef;
use crate::error::{AgentError, StateError};
use crate::llm::LlmToolSchema;
use crate::mcp_source::McpToolCatalog;
use crate::state::ToolCallRecord;
use crate::tenant::TenantContext;

/// Whether the agent may call this tool — exact (extension_id, tool_name) match.
pub fn is_tool_allowed(call: &ToolCallRecord, allowed: &[ToolRef]) -> bool {
    allowed
        .iter()
        .any(|t| t.extension_id == call.extension_id && t.tool_name == call.tool_name)
}

/// Map allow-listed tools to LLM-facing schemas via
/// `ExtensionRuntime::list_tools`. Tools whose extension is not loaded,
/// or that the extension doesn't expose, are logged and skipped (the
/// LLM simply won't see them). `input_schema_json` is parsed into a
/// JSON Value for the LLM tool parameters; on parse failure an empty
/// object schema is used.
///
/// Tools whose `extension_id` starts with `"mcp:"` are resolved from the
/// per-tenant [`McpToolCatalog`] (`mcp`) instead of the extension runtime: the
/// suffix after `"mcp:"` is the MCP `server_id`, and the catalog supplies the
/// LLM-facing `description`/`parameters`. An mcp ref with no matching catalog
/// entry (or no catalog at all) is logged and dropped, mirroring the
/// extension-runtime "tool not found" path.
///
/// Tools whose `extension_id` starts with `"component:"` are resolved the same
/// way from the per-tenant [`ComponentToolCatalog`] (`components`): the suffix
/// is the `component_ref` and `tool_name` the operation, and the catalog
/// supplies the operation's `description`/`parameters`. A `component:` ref with
/// no matching catalog entry (or no catalog) is likewise logged and dropped.
pub fn list_tools_for_llm(
    ext_runtime: &ExtensionRuntime,
    mcp: Option<&McpToolCatalog>,
    components: Option<&ComponentToolCatalog>,
    allowed: &[ToolRef],
) -> Vec<LlmToolSchema> {
    let mut out = Vec::with_capacity(allowed.len());
    for t in allowed {
        if let Some(server_id) = t.extension_id.strip_prefix("mcp:") {
            match mcp.and_then(|c| c.tool_entry(server_id, &t.tool_name)) {
                Some(entry) => out.push(LlmToolSchema {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    description: entry.description.clone(),
                    parameters: entry.parameters.clone(),
                }),
                None => tracing::warn!(
                    extension = %t.extension_id, tool = %t.tool_name,
                    "mcp tool not found in catalog; dropping from LLM tool list"
                ),
            }
            continue;
        }
        if let Some(component_ref) = t.extension_id.strip_prefix("component:") {
            match components.and_then(|c| c.tool_entry(component_ref, &t.tool_name)) {
                Some(entry) => out.push(LlmToolSchema {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    description: entry.description.clone(),
                    parameters: entry.parameters.clone(),
                }),
                None => tracing::warn!(
                    extension = %t.extension_id, tool = %t.tool_name,
                    "component tool not found in catalog; dropping from LLM tool list"
                ),
            }
            continue;
        }
        match ext_runtime.list_tools(&t.extension_id) {
            Ok(defs) => {
                if let Some(def) = defs.into_iter().find(|d| d.name == t.tool_name) {
                    let parameters: serde_json::Value = serde_json::from_str(
                        &def.input_schema_json,
                    )
                    .unwrap_or_else(|_| serde_json::json!({"type": "object", "properties": {}}));
                    out.push(LlmToolSchema {
                        extension_id: t.extension_id.clone(),
                        tool_name: t.tool_name.clone(),
                        description: def.description,
                        parameters,
                    });
                } else {
                    tracing::warn!(
                        extension = %t.extension_id, tool = %t.tool_name,
                        "tool not found in extension; dropping from LLM tool list"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    extension = %t.extension_id, error = %e,
                    "extension list_tools failed; skipping"
                );
            }
        }
    }
    out
}

/// A tool an agent declared that will NOT be visible to the LLM, with a
/// human-readable reason. Produced by [`missing_tools`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingTool {
    pub extension_id: String,
    pub tool_name: String,
    pub reason: String,
}

/// Preflight check: of the `allowed` tools an agent declares, return those that
/// cannot be resolved to a live LLM schema, each with a reason.
///
/// This mirrors the resolution logic in [`list_tools_for_llm`] exactly, but
/// reports failures instead of dropping them silently. Callers use it to warn
/// the operator at startup — otherwise an agent whose tools all failed to load
/// runs with an empty tool set and hallucinates tool results.
pub fn missing_tools(
    ext_runtime: &ExtensionRuntime,
    mcp: Option<&McpToolCatalog>,
    components: Option<&ComponentToolCatalog>,
    allowed: &[ToolRef],
) -> Vec<MissingTool> {
    let mut missing = Vec::new();
    for t in allowed {
        if let Some(server_id) = t.extension_id.strip_prefix("mcp:") {
            if mcp
                .and_then(|c| c.tool_entry(server_id, &t.tool_name))
                .is_none()
            {
                missing.push(MissingTool {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    reason: "MCP tool not found in the tenant catalog".to_string(),
                });
            }
            continue;
        }
        if let Some(component_ref) = t.extension_id.strip_prefix("component:") {
            if components
                .and_then(|c| c.tool_entry(component_ref, &t.tool_name))
                .is_none()
            {
                missing.push(MissingTool {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    reason: "component tool not found in the catalog".to_string(),
                });
            }
            continue;
        }
        match ext_runtime.list_tools(&t.extension_id) {
            Ok(defs) => {
                if !defs.iter().any(|d| d.name == t.tool_name) {
                    missing.push(MissingTool {
                        extension_id: t.extension_id.clone(),
                        tool_name: t.tool_name.clone(),
                        reason: "extension loaded but does not expose this tool".to_string(),
                    });
                }
            }
            Err(e) => {
                missing.push(MissingTool {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    reason: format!("extension failed to load: {e}"),
                });
            }
        }
    }
    missing
}

// NOTE: the research lane threads a `HostCallContext` (tenant + user_email)
// into every tool call so an extension host can resolve the LLM provider
// per-tenant. This lane's `greentic-ext-runtime` exposes only `invoke_tool`,
// with no context parameter and no `HostCallContext` type, so tool calls here
// carry no per-tenant routing hint. Restore the richer call once the host-port
// surface lands on this lane.

/// Dispatch a single tool call. Wraps the blocking `invoke_tool` in
/// `tokio::task::spawn_blocking` so the async executor thread is never
/// stalled. Returns the tool result as a JSON Value.
///
/// Calls whose `extension_id` starts with `"mcp:"` route through the
/// per-tenant [`McpToolCatalog`] (`mcp`) instead: the suffix is the MCP
/// `server_id`, and dispatch goes over HTTP via
/// [`crate::mcp_source::dispatch_route`]. An MCP call NEVER yields `Err` — a
/// missing route or remote failure is surfaced as an `{"error": ...}` value so
/// the LLM observes it as a normal tool result.
///
/// Calls whose `extension_id` starts with `"component:"` route through the
/// per-tenant [`ComponentToolCatalog`] (`components`): the suffix is the
/// `component_ref` and dispatch goes to the host component invoker via
/// [`ComponentToolCatalog::dispatch`]. Like the mcp path it NEVER yields `Err`
/// — an unknown operation or a missing catalog becomes an `{"error": ...}`
/// value. Other ids keep the existing blocking WASM path.
/// Reserved argument key carrying the session-established caller.
///
/// Host-owned: [`stamp_caller`] OVERWRITES it on every dispatch, so an LLM that
/// writes one into its tool arguments cannot be believed. Same contract as the
/// designer's `_tenant_overlay`, which has shipped on exactly this basis.
pub const CALLER_ARG_KEY: &str = "_caller";

/// Replace `args`' caller block with the one the SESSION established.
///
/// Three properties, and each one is load-bearing:
///
/// 1. **Unconditional.** The key is written on every dispatch, including for an
///    anonymous turn, where it is written as `user_verified: false`. Writing it
///    only when a caller is known would make "anonymous" and "this runtime is
///    too old to say" the same observation — and a tool that cannot tell those
///    apart is a tool that trusts whatever an older runtime left behind.
/// 2. **Overwrite, never merge.** Whatever the model composed under this key is
///    discarded. A merge would let the model contribute fields the host did not
///    set, which is the whole attack: a worker once sent
///    `sub: "user@example.com", principals: ["employee"]` for a caller who had
///    sent neither.
/// 3. **Non-object arguments are left alone.** A tool whose arguments are an
///    array or a scalar has nowhere to put the key, and wrapping them would
///    change the shape the tool declared. It gets no stamp, which reads as
///    "too old to say" — correct, because it is equally unable to receive one.
///
/// ## The version-skew hole this does NOT close
///
/// Trust here rests on the host overwriting the key. A runtime PREDATING this
/// function stamps nothing, so a model-written `_caller` survives to a tool
/// that has learned to trust the key. The contract is therefore: **an absent
/// `_caller` means unverified**, and a tool must not treat a present one as
/// authoritative unless the runtime it runs on is known to stamp. Closing it
/// properly means the tool PULLING identity from a host import rather than
/// receiving it in arguments, which is a WIT change and a rebuild of every
/// extension; this ships the contract without one. Do not describe this as
/// unforgeable — it is unforgeable on a runtime that stamps.
fn stamp_caller(args: &mut serde_json::Value, caller: &crate::tenant::VerifiedCaller) {
    let Some(map) = args.as_object_mut() else {
        return;
    };
    map.insert(
        CALLER_ARG_KEY.to_string(),
        serde_json::to_value(caller).unwrap_or(serde_json::Value::Null),
    );
}

pub async fn dispatch_tool_call(
    ext_runtime: Arc<ExtensionRuntime>,
    mcp: Option<Arc<McpToolCatalog>>,
    components: Option<Arc<ComponentToolCatalog>>,
    call: ToolCallRecord,
    // Read for the caller stamp. `invoke_tool` still takes no HostCallContext
    // on this lane, so the tenant reaches the tool through the stamped
    // argument rather than through a context parameter.
    tenant: &TenantContext,
) -> Result<serde_json::Value, AgentError> {
    // Established once, applied to every LOCAL arm below. Not to the `mcp:`
    // arm: that dispatches over HTTP to a server outside this deployment, and
    // forwarding a caller's identity to a third party is a disclosure decision
    // for whoever configures that server, not a plumbing detail of this one.
    let caller = tenant.caller_or_anonymous();
    if let Some(server_id) = call.extension_id.strip_prefix("mcp:") {
        let value = match mcp
            .as_deref()
            .and_then(|c| c.route(server_id, &call.tool_name))
        {
            Some(route) => {
                let args = call.args.to_string();
                crate::mcp_source::dispatch_route(route, &args).await
            }
            None => {
                tracing::warn!(
                    server = %server_id,
                    tool = %call.tool_name,
                    "mcp call has no route in the tenant catalog; returning error value"
                );
                serde_json::json!({
                    "error": format!("unknown mcp tool '{}/{}'", server_id, call.tool_name)
                })
            }
        };
        return Ok(value);
    }

    if let Some(component_ref) = call.extension_id.strip_prefix("component:") {
        let value = match components.as_deref() {
            Some(cat) => {
                let mut args_value = call.args.clone();
                stamp_caller(&mut args_value, &caller);
                let args = args_value.to_string();
                cat.dispatch(component_ref, &call.tool_name, &args).await
            }
            None => {
                tracing::warn!(
                    component = %component_ref,
                    tool = %call.tool_name,
                    "component call has no catalog wired; returning error value"
                );
                serde_json::json!({
                    "error": format!(
                        "unknown component tool '{}/{}'",
                        component_ref, call.tool_name
                    )
                })
            }
        };
        return Ok(value);
    }

    let mut args_value = call.args.clone();
    stamp_caller(&mut args_value, &caller);
    let args_json = args_value.to_string();
    let extension_id = call.extension_id.clone();
    let tool_name = call.tool_name.clone();
    let raw = tokio::task::spawn_blocking(move || {
        ext_runtime.invoke_tool(&extension_id, &tool_name, &args_json)
    })
    .await
    .map_err(|e| AgentError::ToolDispatch(format!("join: {e}")))?
    .map_err(|e| AgentError::ToolDispatch(format!("invoke: {e}")))?;
    serde_json::from_str(&raw).map_err(|e| AgentError::ToolDispatch(format!("decode: {e}")))
}

#[cfg(test)]
mod caller_stamp_tests {
    use super::*;
    use crate::tenant::VerifiedCaller;

    fn verified() -> VerifiedCaller {
        VerifiedCaller {
            user_verified: true,
            sub: Some("u-1@acme".into()),
            groups: vec!["engineering".into()],
            team: Some("platform".into()),
            role: Some("member".into()),
        }
    }

    /// The attack this exists to stop, verbatim: a worker sent
    /// `sub: "user@example.com"`, `principals: ["employee"]` for a caller who
    /// had sent neither. Whatever the model writes under the key is discarded.
    #[test]
    fn a_model_written_caller_block_is_discarded() {
        let mut args = serde_json::json!({
            "query": "salaries",
            "_caller": { "user_verified": true, "sub": "user@example.com",
                         "groups": ["admin"], "role": "administrator" }
        });
        stamp_caller(&mut args, &verified());
        assert_eq!(args["_caller"]["sub"], "u-1@acme");
        assert_eq!(
            args["_caller"]["groups"],
            serde_json::json!(["engineering"])
        );
        assert_eq!(args["_caller"]["role"], "member");
        // The model's own arguments are untouched.
        assert_eq!(args["query"], "salaries");
    }

    /// Unconditional: an anonymous turn is stamped too, as `user_verified:
    /// false`. Skipping the stamp here would make "anonymous" and "this
    /// runtime is too old to say" indistinguishable, and a tool that cannot
    /// tell those apart trusts whatever an older runtime left behind.
    #[test]
    fn an_anonymous_turn_is_still_stamped_as_unverified() {
        let mut args = serde_json::json!({ "query": "x" });
        stamp_caller(&mut args, &VerifiedCaller::default());
        assert_eq!(args["_caller"]["user_verified"], false);
        assert!(args["_caller"].get("sub").is_none(), "no subject to assert");
    }

    /// A forged block on an anonymous turn is still erased — the case where a
    /// model would otherwise promote itself from nothing.
    #[test]
    fn a_forged_block_cannot_survive_an_anonymous_turn() {
        let mut args = serde_json::json!({
            "_caller": { "user_verified": true, "sub": "root", "role": "administrator" }
        });
        stamp_caller(&mut args, &VerifiedCaller::default());
        assert_eq!(args["_caller"]["user_verified"], false);
        assert!(args["_caller"].get("role").is_none());
    }

    /// Non-object arguments keep the shape the tool declared. Wrapping them to
    /// make room for the key would change what the tool receives.
    #[test]
    fn non_object_arguments_are_left_alone() {
        let mut args = serde_json::json!(["a", "b"]);
        stamp_caller(&mut args, &verified());
        assert_eq!(args, serde_json::json!(["a", "b"]));
    }

    /// `TenantContext` with no caller must resolve to anonymous rather than to
    /// "skip the stamp" — the `Option` is deliberately not exposed to the
    /// dispatch path.
    #[test]
    fn a_tenant_without_a_caller_resolves_to_anonymous() {
        let t = TenantContext::new("acme", "prod");
        assert_eq!(t.caller_or_anonymous(), VerifiedCaller::default());
        assert!(!t.caller_or_anonymous().user_verified);
    }
}

/// Idempotency ledger entry stored under
/// `aw:{tenant}:{env}:{session}:tool_calls:{call_id}` (TTL 7 days).
#[derive(Serialize, Deserialize, Clone)]
pub struct ToolLedgerEntry {
    pub result: serde_json::Value,
}

pub fn ledger_key(tenant: &TenantContext, session_id: &str, call_id: &str) -> String {
    format!("{}:{session_id}:tool_calls:{call_id}", tenant.key_prefix())
}

/// Idempotency ledger for tool calls. Records tool results keyed by
/// `tool_call_id` so a state-save failure does not cause a duplicate
/// dispatch (re-sending the same email, etc.) on the next `step()`.
///
/// Dyn-safe (`Arc<dyn ToolLedger>`); production uses [`RedisToolLedger`],
/// tests use `NoopToolLedger` (from the `test-mock` module).
pub trait ToolLedger: Send + Sync {
    /// Return a previously-recorded result for this call_id, or `None`.
    fn get<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        call_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<serde_json::Value>, StateError>> + Send + 'a>>;

    /// Record a tool result (TTL 7 days).
    fn record<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        call_id: &'a str,
        result: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>>;
}

const LEDGER_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Production tool ledger backed by a multiplexed `ConnectionManager`.
///
/// Shares the Redis instance with `RedisAgentStateStore` via
/// `RedisAgentStateStore::manager()`. The manager is `Clone` (cheap,
/// reference-counted) so per-call clones open no new connections.
pub struct RedisToolLedger {
    manager: ConnectionManager,
}

impl RedisToolLedger {
    pub fn new(manager: ConnectionManager) -> Self {
        Self { manager }
    }
}

impl ToolLedger for RedisToolLedger {
    fn get<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        call_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<serde_json::Value>, StateError>> + Send + 'a>>
    {
        Box::pin(async move {
            let key = ledger_key(tenant, session_id, call_id);
            let mut conn = self.manager.clone();
            let raw: Option<String> = conn
                .get(&key)
                .await
                .map_err(|e| StateError::Redis(format!("ledger get: {e}")))?;
            match raw {
                Some(json) => {
                    let entry: ToolLedgerEntry = serde_json::from_str(&json)
                        .map_err(|e| StateError::Decode(format!("ledger decode: {e}")))?;
                    Ok(Some(entry.result))
                }
                None => Ok(None),
            }
        })
    }

    fn record<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        call_id: &'a str,
        result: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<(), StateError>> + Send + 'a>> {
        Box::pin(async move {
            let key = ledger_key(tenant, session_id, call_id);
            let entry = ToolLedgerEntry { result };
            let json = serde_json::to_string(&entry)
                .map_err(|e| StateError::Decode(format!("ledger encode: {e}")))?;
            let mut conn = self.manager.clone();
            let _: () = conn
                .set_ex(&key, json, LEDGER_TTL_SECS)
                .await
                .map_err(|e| StateError::Redis(format!("ledger set_ex: {e}")))?;
            Ok(())
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn is_tool_allowed_returns_true_for_exact_match() {
        let allowed = vec![ToolRef {
            extension_id: "http".into(),
            tool_name: "fetch".into(),
        }];
        let call = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "http".into(),
            tool_name: "fetch".into(),
            args: serde_json::json!({}),
        };
        assert!(is_tool_allowed(&call, &allowed));
    }

    #[test]
    fn is_tool_allowed_returns_false_for_unauthorized_tool() {
        let allowed = vec![ToolRef {
            extension_id: "http".into(),
            tool_name: "fetch".into(),
        }];
        let call = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "http".into(),
            tool_name: "post".into(),
            args: serde_json::json!({}),
        };
        assert!(!is_tool_allowed(&call, &allowed));
    }

    #[test]
    fn ledger_key_includes_tenant_env_session_callid() {
        let tc = TenantContext::new("acme", "prod");
        let key = ledger_key(&tc, "sess-1", "call-abc");
        assert_eq!(key, "aw:acme:prod:sess-1:tool_calls:call-abc");
    }

    #[test]
    fn list_tools_for_llm_with_no_extensions_returns_empty() {
        // for_test runtime has no extensions loaded → list_tools errors
        // (NotFound) for every ext → all skipped → empty result.
        let rt = crate::test_support::extension_runtime();
        let allowed = vec![ToolRef {
            extension_id: "http".into(),
            tool_name: "fetch".into(),
        }];
        let schemas = list_tools_for_llm(&rt, None, None, &allowed);
        assert!(schemas.is_empty());
    }

    #[test]
    fn missing_tools_reports_unloaded_extension() {
        // No extensions loaded → the declared tool cannot resolve and is
        // reported as missing with a load-failure reason (instead of being
        // dropped silently, which is what causes hallucinated tool results).
        let rt = crate::test_support::extension_runtime();
        let allowed = vec![ToolRef {
            extension_id: "greentic.hubspot".into(),
            tool_name: "hubspot_contacts".into(),
        }];
        let missing = missing_tools(&rt, None, None, &allowed);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].extension_id, "greentic.hubspot");
        assert_eq!(missing[0].tool_name, "hubspot_contacts");
        assert!(
            missing[0].reason.contains("failed to load"),
            "got: {}",
            missing[0].reason
        );
    }

    #[test]
    fn missing_tools_reports_mcp_tool_absent_from_catalog() {
        let rt = crate::test_support::extension_runtime();
        let allowed = vec![ToolRef {
            extension_id: "mcp:github".into(),
            tool_name: "create_issue".into(),
        }];
        // No catalog provided → the mcp tool is unresolvable.
        let missing = missing_tools(&rt, None, None, &allowed);
        assert_eq!(missing.len(), 1);
        assert!(
            missing[0].reason.contains("MCP tool not found"),
            "got: {}",
            missing[0].reason
        );
    }

    use std::collections::HashMap;

    use crate::mcp_source::{McpRoute, McpToolCatalog, McpToolEntry, route_for_tests};

    /// Build a one-tool catalog whose route (when present) aims at
    /// `transport_url`. Pass `with_route = false` to register only the schema
    /// (list-side) without a dispatch route.
    fn catalog_with(
        server: &str,
        tool: &str,
        description: &str,
        parameters: serde_json::Value,
        transport_url: Option<&str>,
    ) -> McpToolCatalog {
        let mut tools: HashMap<(String, String), McpToolEntry> = HashMap::new();
        tools.insert(
            (server.to_string(), tool.to_string()),
            McpToolEntry {
                description: description.to_string(),
                parameters,
            },
        );
        let mut routes: HashMap<(String, String), McpRoute> = HashMap::new();
        if let Some(url) = transport_url {
            routes.insert(
                (server.to_string(), tool.to_string()),
                route_for_tests(server, tool, url),
            );
        }
        McpToolCatalog::for_tests(tools, routes)
    }

    /// Mount the minimal MCP JSON-RPC contract (initialize, initialized,
    /// tools/call) on a fresh wiremock server returning `call_result`.
    /// Replicated from `mcp_source` tests — only the few mount lines needed
    /// to exercise dispatch.
    async fn fake_mcp_call_server(call_result: serde_json::Value) -> wiremock::MockServer {
        use wiremock::matchers::{body_partial_json, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({ "method": "initialize" }),
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Mcp-Session-Id", "sess-1")
                    .set_body_json(serde_json::json!({
                        "jsonrpc": "2.0", "id": 1,
                        "result": {
                            "protocolVersion": "2025-06-18",
                            "serverInfo": { "name": "fake", "version": "1.0.0" }
                        }
                    })),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({ "method": "notifications/initialized" }),
            ))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({ "method": "tools/call" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 3,
                "result": call_result
            })))
            .mount(&server)
            .await;
        server
    }

    #[test]
    fn mcp_ref_listed_from_catalog() {
        // A catalog-backed mcp: ref is emitted as an LlmToolSchema with the
        // catalog's description/parameters; the ext_runtime is never consulted
        // for it (the for_test runtime has no extensions loaded).
        let rt = crate::test_support::extension_runtime();
        let params = serde_json::json!({
            "type": "object",
            "properties": { "id": { "type": "string" } }
        });
        let catalog = catalog_with("s1", "get_issue", "Get an issue", params.clone(), None);

        let allowed = vec![
            ToolRef {
                extension_id: "mcp:s1".into(),
                tool_name: "get_issue".into(),
            },
            // Absent from the catalog → dropped (warn), not panicked.
            ToolRef {
                extension_id: "mcp:s1".into(),
                tool_name: "missing".into(),
            },
        ];

        let schemas = list_tools_for_llm(&rt, Some(&catalog), None, &allowed);
        assert_eq!(schemas.len(), 1, "only the catalog-backed ref is emitted");
        let s = &schemas[0];
        assert_eq!(s.extension_id, "mcp:s1");
        assert_eq!(s.tool_name, "get_issue");
        assert_eq!(s.description, "Get an issue");
        assert_eq!(s.parameters, params);
    }

    #[test]
    fn non_mcp_ref_unchanged() {
        // A normal ref still routes through ext_runtime. The for_test runtime
        // loads no extensions, so list_tools errors → the ref is skipped,
        // exactly as in `list_tools_for_llm_with_no_extensions_returns_empty`.
        // The presence of a catalog must not change that path.
        //
        // The catalog deliberately contains an entry keyed by the FULL
        // non-mcp extension id — if the mcp branch ever matched non-`mcp:`
        // ids and consulted the catalog, this entry would be emitted and the
        // empty assertion below would catch the regression.
        let rt = crate::test_support::extension_runtime();
        let catalog = catalog_with(
            "greentic.tavily",
            "search",
            "decoy: must never be emitted for a non-mcp ref",
            serde_json::json!({}),
            None,
        );
        let allowed = vec![ToolRef {
            extension_id: "greentic.tavily".into(),
            tool_name: "search".into(),
        }];
        let schemas = list_tools_for_llm(&rt, Some(&catalog), None, &allowed);
        assert!(
            schemas.is_empty(),
            "non-mcp ref still goes through ext_runtime (unloaded → dropped)"
        );
    }

    #[test]
    fn is_tool_allowed_matches_mcp_ref() {
        // Exact (mcp:s1, get_issue) match works with no change to the fn.
        let allowed = vec![ToolRef {
            extension_id: "mcp:s1".into(),
            tool_name: "get_issue".into(),
        }];
        let call = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "mcp:s1".into(),
            tool_name: "get_issue".into(),
            args: serde_json::json!({}),
        };
        assert!(is_tool_allowed(&call, &allowed));

        let other = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "mcp:s1".into(),
            tool_name: "search_code".into(),
            args: serde_json::json!({}),
        };
        assert!(!is_tool_allowed(&other, &allowed));
    }

    #[tokio::test]
    async fn dispatch_routes_mcp_ref() {
        // Route present → calls the fake MCP server and returns its output.
        let mcp = fake_mcp_call_server(serde_json::json!({
            "structuredContent": { "ok": 1 }
        }))
        .await;
        let uri = mcp.uri();
        let catalog = Arc::new(catalog_with(
            "s1",
            "get_issue",
            "Get an issue",
            serde_json::json!({}),
            Some(&uri),
        ));
        let rt = Arc::new(crate::test_support::extension_runtime());

        let call = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "mcp:s1".into(),
            tool_name: "get_issue".into(),
            args: serde_json::json!({}),
        };
        let tc = TenantContext::new("t", "e");
        let out = dispatch_tool_call(rt.clone(), Some(catalog.clone()), None, call, &tc)
            .await
            .expect("mcp dispatch never returns Err");
        assert_eq!(out, serde_json::json!({ "ok": 1 }), "got: {out}");

        // Route missing → shaped error value, still Ok.
        let missing = ToolCallRecord {
            call_id: "c2".into(),
            extension_id: "mcp:s1".into(),
            tool_name: "no_such".into(),
            args: serde_json::json!({}),
        };
        let out = dispatch_tool_call(rt.clone(), Some(catalog), None, missing, &tc)
            .await
            .expect("missing mcp route still returns Ok");
        assert_eq!(
            out,
            serde_json::json!({ "error": "unknown mcp tool 's1/no_such'" }),
            "got: {out}"
        );

        // A non-mcp call still hits the ext_runtime path (unloaded → Err).
        let non_mcp = ToolCallRecord {
            call_id: "c3".into(),
            extension_id: "greentic.absent".into(),
            tool_name: "nope".into(),
            args: serde_json::json!({}),
        };
        let res = dispatch_tool_call(rt, None, None, non_mcp, &tc).await;
        assert!(
            res.is_err(),
            "non-mcp dispatch against an unloaded extension must error"
        );
    }

    use crate::component_source::ComponentToolCatalog;
    use crate::component_source::test_support::{FakeInvoker, one_tool};

    #[test]
    fn component_ref_listed_from_catalog() {
        // A catalog-backed component: ref is emitted as an LlmToolSchema with
        // the catalog's description/parameters; ext_runtime is never consulted.
        let rt = crate::test_support::extension_runtime();
        let params = serde_json::json!({
            "type": "object",
            "properties": { "order_id": { "type": "string" } }
        });
        let invoker = Arc::new(FakeInvoker::new(vec![], Ok(serde_json::json!({}))));
        let catalog = ComponentToolCatalog::for_tests(
            one_tool(
                "greentic.refund",
                "issue_refund",
                "Issue a refund",
                params.clone(),
            ),
            invoker,
        );

        let allowed = vec![
            ToolRef {
                extension_id: "component:greentic.refund".into(),
                tool_name: "issue_refund".into(),
            },
            // Absent from the catalog → dropped (warn), not panicked.
            ToolRef {
                extension_id: "component:greentic.refund".into(),
                tool_name: "missing".into(),
            },
        ];

        let schemas = list_tools_for_llm(&rt, None, Some(&catalog), &allowed);
        assert_eq!(schemas.len(), 1, "only the catalog-backed ref is emitted");
        let s = &schemas[0];
        assert_eq!(s.extension_id, "component:greentic.refund");
        assert_eq!(s.tool_name, "issue_refund");
        assert_eq!(s.description, "Issue a refund");
        assert_eq!(s.parameters, params);
    }

    #[test]
    fn non_component_ref_unaffected_by_catalog() {
        // A plain ext ref still routes through ext_runtime even when a
        // component catalog is present. The decoy entry is keyed by the FULL
        // non-prefixed id — if the component branch ever matched it, this entry
        // would leak into the list and the empty assertion would catch it.
        let rt = crate::test_support::extension_runtime();
        let invoker = Arc::new(FakeInvoker::new(vec![], Ok(serde_json::json!({}))));
        let catalog = ComponentToolCatalog::for_tests(
            one_tool(
                "greentic.tavily",
                "search",
                "decoy: must never be emitted for a non-component ref",
                serde_json::json!({}),
            ),
            invoker,
        );
        let allowed = vec![ToolRef {
            extension_id: "greentic.tavily".into(),
            tool_name: "search".into(),
        }];
        let schemas = list_tools_for_llm(&rt, None, Some(&catalog), &allowed);
        assert!(
            schemas.is_empty(),
            "non-component ref still goes through ext_runtime (unloaded → dropped)"
        );
    }

    #[tokio::test]
    async fn dispatch_routes_component_ref() {
        // Catalog entry present → routes to the invoker and returns its value.
        let invoker = Arc::new(FakeInvoker::new(
            vec![],
            Ok(serde_json::json!({ "refund_id": "r-1" })),
        ));
        let catalog = Arc::new(ComponentToolCatalog::for_tests(
            one_tool(
                "greentic.refund",
                "issue_refund",
                "Issue a refund",
                serde_json::json!({}),
            ),
            invoker,
        ));
        let rt = Arc::new(crate::test_support::extension_runtime());

        let tc = TenantContext::new("t", "e");
        let call = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "component:greentic.refund".into(),
            tool_name: "issue_refund".into(),
            args: serde_json::json!({}),
        };
        let out = dispatch_tool_call(rt.clone(), None, Some(catalog.clone()), call, &tc)
            .await
            .expect("component dispatch never returns Err");
        assert_eq!(out, serde_json::json!({ "refund_id": "r-1" }), "got: {out}");

        // Unknown op → shaped error value, still Ok.
        let missing = ToolCallRecord {
            call_id: "c2".into(),
            extension_id: "component:greentic.refund".into(),
            tool_name: "no_such".into(),
            args: serde_json::json!({}),
        };
        let out = dispatch_tool_call(rt.clone(), None, Some(catalog), missing, &tc)
            .await
            .expect("missing component op still returns Ok");
        assert!(out.to_string().contains("error"), "got: {out}");

        // No component catalog wired → shaped error value, still Ok (mirrors
        // the mcp branch's "no route" behaviour).
        let no_cat = ToolCallRecord {
            call_id: "c3".into(),
            extension_id: "component:greentic.refund".into(),
            tool_name: "issue_refund".into(),
            args: serde_json::json!({}),
        };
        let out = dispatch_tool_call(rt, None, None, no_cat, &tc)
            .await
            .expect("component dispatch with no catalog still returns Ok");
        assert!(out.to_string().contains("error"), "got: {out}");
    }
}
