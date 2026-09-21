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
use greentic_ext_runtime::host_ports::HostCallContext;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};

use crate::a2a_source::A2aToolCatalog;
use crate::component_source::ComponentToolCatalog;
use crate::config::ToolRef;
use crate::error::{AgentError, StateError};
use crate::flow_source::FlowToolCatalog;
use crate::kv::AwKv;
use crate::llm::LlmToolSchema;
use crate::mcp_source::McpToolCatalog;
use crate::sorla_source::SorlaToolCatalog;
use crate::state::ToolCallRecord;
use crate::tenant::TenantContext;

/// Append an author `usage_note` to a resolved tool description. A `None` or
/// whitespace-only note is a no-op (no trailing whitespace, no empty block).
fn with_usage_note(description: String, note: &Option<String>) -> String {
    match note {
        Some(n) if !n.trim().is_empty() => format!("{description}\n\n{}", n.trim()),
        _ => description,
    }
}

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
/// LLM-facing `description`/`parameters`. When the catalog has no entry, the
/// ref's own `description`/`input_schema` are used — the schema the pack's
/// `AgentConfig` snapshotted at authoring time — so a deployed runner with no
/// admin credentials still advertises the tool. The catalog WINS whenever it has
/// an entry: it was probed from the live server this run. An mcp ref with
/// neither is logged and dropped, mirroring the extension-runtime "tool not
/// found" path.
///
/// Tools whose `extension_id` starts with `"component:"` are resolved the same
/// way from the per-tenant [`ComponentToolCatalog`] (`components`): the suffix
/// is the `component_ref` and `tool_name` the operation, and the catalog
/// supplies the operation's `description`/`parameters`. A `component:` ref with
/// no matching catalog entry (or no catalog) is likewise logged and dropped.
///
/// Tools whose `extension_id` starts with `"sorla:"` are resolved the same way
/// from the per-tenant [`SorlaToolCatalog`] (`sorla`): the suffix is the SoR
/// `pack` and `tool_name` the action, and the catalog supplies the action's
/// `description`/`parameters`. A `sorla:` ref with no matching catalog entry
/// (or no catalog) is likewise logged and dropped.
///
/// Tools whose `extension_id` starts with `"flow:"` are resolved from the
/// per-tenant [`FlowToolCatalog`] (`flows`): the suffix after `"flow:"` is the
/// `flow_ref`, which is the sole key (no operation). The catalog supplies the
/// LLM-facing `description`/`parameters`. A `flow:` ref with no matching catalog
/// entry (or no catalog) is likewise logged and dropped.
///
/// Tools whose `extension_id` starts with `"a2a:"` are resolved from the
/// per-tenant [`A2aToolCatalog`] (`a2a`): the suffix after `"a2a:"` is the
/// `agent_id`. Unlike every other prefix here, description and schema resolve
/// in DIFFERENT directions: the catalog's description wins when present — it
/// was fetched from the agent's own live card this run, the same reasoning as
/// the `mcp:` branch above — but the schema can ONLY come from the `ToolRef`'s
/// own author contract, because an A2A `AgentSkill` carries no input schema at
/// all. A ref with no `input_schema` is dropped regardless of the catalog.
pub fn list_tools_for_llm(
    ext_runtime: &ExtensionRuntime,
    mcp: Option<&McpToolCatalog>,
    components: Option<&ComponentToolCatalog>,
    flows: Option<&FlowToolCatalog>,
    sorla: Option<&SorlaToolCatalog>,
    a2a: Option<&A2aToolCatalog>,
    allowed: &[ToolRef],
) -> Vec<LlmToolSchema> {
    let mut out = Vec::with_capacity(allowed.len());
    for t in allowed {
        if let Some(server_id) = t.extension_id.strip_prefix("mcp:") {
            let entry = mcp.and_then(|c| c.tool_entry(server_id, &t.tool_name));
            // Catalog FIRST, the author contract second — deliberately the
            // OPPOSITE order from the `flow:` branch below. A catalog entry was
            // probed from the live server this run, whereas `ToolRef` carries a
            // schema snapshotted into the pack at authoring time; preferring the
            // pack would downgrade every deployment that does have a working
            // admin source to a staler schema. The fallback exists so a deployed
            // runner with NO admin credentials still advertises the tool at all
            // instead of dropping it (spec §4.2).
            let description = entry
                .map(|e| e.description.clone())
                .or_else(|| t.description.clone());
            let parameters = entry
                .map(|e| e.parameters.clone())
                .or_else(|| t.input_schema.clone());
            match (description, parameters) {
                (Some(description), Some(parameters)) => out.push(LlmToolSchema {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    description: with_usage_note(description, &t.usage_note),
                    parameters,
                }),
                _ => tracing::warn!(
                    extension = %t.extension_id, tool = %t.tool_name,
                    "mcp tool has neither a catalog entry nor an author contract; \
                     dropping from LLM tool list"
                ),
            }
            continue;
        }
        if let Some(component_ref) = t.extension_id.strip_prefix("component:") {
            match components.and_then(|c| c.tool_entry(component_ref, &t.tool_name)) {
                Some(entry) => out.push(LlmToolSchema {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    description: with_usage_note(entry.description.clone(), &t.usage_note),
                    parameters: entry.parameters.clone(),
                }),
                None => tracing::warn!(
                    extension = %t.extension_id, tool = %t.tool_name,
                    "component tool not found in catalog; dropping from LLM tool list"
                ),
            }
            continue;
        }
        if let Some(pack) = t.extension_id.strip_prefix("sorla:") {
            match sorla.and_then(|c| c.tool_entry(pack, &t.tool_name)) {
                Some(entry) => out.push(LlmToolSchema {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    description: with_usage_note(entry.description.clone(), &t.usage_note),
                    parameters: entry.parameters.clone(),
                }),
                None => tracing::warn!(
                    extension = %t.extension_id, tool = %t.tool_name,
                    "sorla tool not found in catalog; dropping from LLM tool list"
                ),
            }
            continue;
        }
        if let Some(flow_ref) = t.extension_id.strip_prefix("flow:") {
            let entry = flows.and_then(|c| c.tool_entry(flow_ref));
            let description = t
                .description
                .clone()
                .or_else(|| entry.map(|e| e.description.clone()));
            let parameters = t
                .input_schema
                .clone()
                .or_else(|| entry.map(|e| e.parameters.clone()));
            match (description, parameters) {
                (Some(description), Some(parameters)) => out.push(LlmToolSchema {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    description: with_usage_note(description, &t.usage_note),
                    parameters,
                }),
                _ => tracing::warn!(
                    extension = %t.extension_id, tool = %t.tool_name,
                    "flow tool has neither an author contract nor a catalog entry; dropping from LLM tool list"
                ),
            }
            continue;
        }
        if let Some(agent_id) = t.extension_id.strip_prefix("a2a:") {
            // Only a configured agent is a tool at all: dispatch refuses any
            // other, so advertising one would offer the model a call that can
            // only fail. Then the card wins on description — it is the agent's
            // own live statement about itself, as the mcp catalogue is — and
            // the schema can only come from the author contract, since an
            // `AgentSkill` carries none.
            let Some(catalog) = a2a.filter(|c| c.is_configured(agent_id)) else {
                tracing::warn!(
                    extension = %t.extension_id, tool = %t.tool_name,
                    "a2a agent is not configured for this worker; dropping from LLM tool list"
                );
                continue;
            };
            let description = catalog
                .tool_entry(agent_id)
                .map(|e| e.description.clone())
                .or_else(|| t.description.clone());
            match (description, t.input_schema.clone()) {
                (Some(description), Some(parameters)) => out.push(LlmToolSchema {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    description: with_usage_note(description, &t.usage_note),
                    parameters,
                }),
                _ => tracing::warn!(
                    extension = %t.extension_id, tool = %t.tool_name,
                    "a2a tool has no input schema on its author contract; \
                     dropping from LLM tool list"
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
                        description: with_usage_note(def.description, &t.usage_note),
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
    flows: Option<&FlowToolCatalog>,
    sorla: Option<&SorlaToolCatalog>,
    a2a: Option<&A2aToolCatalog>,
    allowed: &[ToolRef],
) -> Vec<MissingTool> {
    let mut missing = Vec::new();
    for t in allowed {
        if let Some(server_id) = t.extension_id.strip_prefix("mcp:") {
            // Mirrors `list_tools_for_llm`'s mcp branch exactly: a catalog entry
            // resolves the tool, and so does a complete author contract
            // (description AND input schema) on the `ToolRef` itself. Without
            // this second arm `preflight_warn_tools` would warn loudly about a
            // pack-schema'd tool that now works.
            let resolvable = mcp
                .and_then(|c| c.tool_entry(server_id, &t.tool_name))
                .is_some()
                || (t.description.is_some() && t.input_schema.is_some());
            if !resolvable {
                missing.push(MissingTool {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    reason: "MCP tool not found in the tenant catalog, and the agent config \
                             carries no description + input schema for it"
                        .to_string(),
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
        if let Some(pack) = t.extension_id.strip_prefix("sorla:") {
            if sorla
                .and_then(|c| c.tool_entry(pack, &t.tool_name))
                .is_none()
            {
                missing.push(MissingTool {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    reason: "sorla tool not found in the catalog".to_string(),
                });
            }
            continue;
        }
        if let Some(flow_ref) = t.extension_id.strip_prefix("flow:") {
            if flows.and_then(|c| c.tool_entry(flow_ref)).is_none() {
                missing.push(MissingTool {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    reason: "flow tool not found in the catalog".to_string(),
                });
            }
            continue;
        }
        if let Some(agent_id) = t.extension_id.strip_prefix("a2a:") {
            // Mirrors `list_tools_for_llm`'s a2a branch exactly: the agent must
            // be configured; then the description may come from the card OR
            // the author contract, but the schema has only one source.
            let reason = match a2a.filter(|c| c.is_configured(agent_id)) {
                None => Some("a2a agent is not configured for this worker"),
                Some(catalog) => {
                    let described =
                        catalog.tool_entry(agent_id).is_some() || t.description.is_some();
                    (!(described && t.input_schema.is_some())).then_some(
                        "a2a agent card unavailable, or the agent config carries \
                         no input schema for it",
                    )
                }
            };
            if let Some(reason) = reason {
                missing.push(MissingTool {
                    extension_id: t.extension_id.clone(),
                    tool_name: t.tool_name.clone(),
                    reason: reason.to_string(),
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

/// Build a [`HostCallContext`] from the per-step [`TenantContext`].
///
/// The extension host (e.g. the designer's `DesignerLlmBridge`) uses
/// `ctx.tenant` to resolve the LLM provider per-tenant and `ctx.user_email`
/// for optional per-user override (present only in interactive test-chat steps;
/// `None` for autonomous workers).
pub(crate) fn host_ctx_from_tenant(t: &TenantContext) -> HostCallContext {
    HostCallContext {
        tenant: if t.tenant_id.is_empty() {
            None
        } else {
            Some(t.tenant_id.clone())
        },
        user_email: t.user_email.clone(),
    }
}

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

/// Dispatch a single tool call. Wraps the blocking `invoke_tool_ctx` in
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
/// value.
///
/// Calls whose `extension_id` starts with `"sorla:"` route through the
/// per-tenant [`SorlaToolCatalog`] (`sorla`): the suffix is the SoR `pack`
/// and dispatch goes to the host SoRX interact client via
/// [`SorlaToolCatalog::dispatch`]. Like the mcp/component paths it NEVER
/// yields `Err` — an unknown action or a missing catalog becomes an
/// `{"error": ...}` value. Other ids keep the existing blocking WASM path.
///
/// Calls whose `extension_id` starts with `"a2a:"` route through the
/// per-tenant [`A2aToolCatalog`] (`a2a`): the suffix is the `agent_id` and
/// dispatch goes over HTTP via [`A2aToolCatalog::dispatch`], which is
/// attempted even for an agent absent from the catalog (the listing side can
/// still advertise it from the author contract). Like the mcp/component/sorla
/// paths it NEVER yields `Err` — an unknown or unreachable agent becomes an
/// `{"error": ...}` value.
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_tool_call(
    ext_runtime: Arc<ExtensionRuntime>,
    mcp: Option<Arc<McpToolCatalog>>,
    components: Option<Arc<ComponentToolCatalog>>,
    flows: Option<Arc<FlowToolCatalog>>,
    sorla: Option<Arc<SorlaToolCatalog>>,
    a2a: Option<Arc<A2aToolCatalog>>,
    call: ToolCallRecord,
    tenant: &TenantContext,
) -> Result<serde_json::Value, AgentError> {
    // Established once, applied to the extension and component arms below —
    // the same arms #760 stamps on develop. Not to the `mcp:` arm: that
    // dispatches over HTTP to a server outside this deployment, and forwarding
    // a caller's identity to a third party is a disclosure decision for whoever
    // configures that server, not a plumbing detail of this one. The `flow:`
    // and `sorla:` arms (research-only) are left unstamped too: a flow's input
    // is flow data, which is exactly the position a caller must not travel
    // through, and SoRX is its own service boundary. The `a2a:` arm is
    // likewise unstamped, for the same reason as `mcp:`: an A2A agent is a
    // third-party server outside this deployment.
    let caller = tenant.caller_or_anonymous();
    if let Some(server_id) = call.extension_id.strip_prefix("mcp:") {
        // `resolve_route` takes the catalog's exact `(server, tool)` entry when
        // it has one and otherwise stamps the tool onto a pack-carried
        // server-level route. An admin-built catalog registers no server-level
        // routes, so its behaviour here is unchanged.
        let value = match mcp
            .as_deref()
            .and_then(|c| c.resolve_route(server_id, &call.tool_name))
        {
            Some(route) => {
                let args = call.args.to_string();
                let scope = match mcp.as_deref().and_then(|c| c.secrets()) {
                    Some(manager) => {
                        crate::mcp_scope::McpCallScope::with_secrets(tenant.clone(), manager)
                    }
                    None => crate::mcp_scope::McpCallScope::new(tenant.clone()),
                };
                crate::mcp_source::dispatch_route(&route, &args, &scope).await
            }
            None => {
                tracing::warn!(
                    server = %server_id,
                    tool = %call.tool_name,
                    "mcp call has no route in the tenant catalog; returning error value"
                );
                // A per-server diagnostic (only the pack path records one) names
                // the real cause; without it a missing credential and an
                // unregistered server are indistinguishable to the operator.
                serde_json::json!({
                    "error": match mcp.as_deref().and_then(|c| c.server_error(server_id)) {
                        Some(detail) => format!(
                            "mcp tool '{}/{}' is unavailable: mcp server '{}' has {detail}",
                            server_id, call.tool_name, server_id
                        ),
                        None => format!("unknown mcp tool '{}/{}'", server_id, call.tool_name),
                    }
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

    if let Some(pack) = call.extension_id.strip_prefix("sorla:") {
        let value = match sorla.as_deref() {
            Some(cat) => {
                let args = call.args.to_string();
                cat.dispatch(pack, &call.tool_name, &args).await
            }
            None => {
                tracing::warn!(
                    pack = %pack, tool = %call.tool_name,
                    "sorla call has no catalog wired; returning error value"
                );
                serde_json::json!({ "error": format!("unknown sorla tool '{}/{}'", pack, call.tool_name) })
            }
        };
        return Ok(value);
    }

    if let Some(flow_ref) = call.extension_id.strip_prefix("flow:") {
        let value = match flows.as_deref() {
            Some(cat) => cat.dispatch(flow_ref, &call.args.to_string()).await,
            None => {
                tracing::warn!(flow = %flow_ref, "flow call has no catalog wired; returning error value");
                serde_json::json!({ "error": format!("unknown flow tool '{flow_ref}'") })
            }
        };
        return Ok(value);
    }

    if let Some(agent_id) = call.extension_id.strip_prefix("a2a:") {
        let value = match a2a.as_deref() {
            Some(catalog) => catalog.dispatch(agent_id, &call.args).await,
            None => {
                tracing::warn!(agent = %agent_id, "a2a call has no catalog wired; returning error value");
                serde_json::json!({ "error": format!("unknown a2a agent '{agent_id}'") })
            }
        };
        return Ok(value);
    }

    let mut args_value = call.args.clone();
    stamp_caller(&mut args_value, &caller);
    let args_json = args_value.to_string();
    let extension_id = call.extension_id.clone();
    let tool_name = call.tool_name.clone();
    let ctx = host_ctx_from_tenant(tenant);
    let raw = tokio::task::spawn_blocking(move || {
        ext_runtime.invoke_tool_ctx(&extension_id, &tool_name, &args_json, &ctx)
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

const KV_LEDGER_TTL: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// Tool-call idempotency ledger over [`AwKv`] (Redis-free). Same key format
/// and 7-day TTL as [`RedisToolLedger`].
pub struct KvToolLedger {
    kv: Arc<dyn AwKv>,
}

impl KvToolLedger {
    pub fn new(kv: Arc<dyn AwKv>) -> Self {
        Self { kv }
    }
}

impl ToolLedger for KvToolLedger {
    fn get<'a>(
        &'a self,
        tenant: &'a TenantContext,
        session_id: &'a str,
        call_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<serde_json::Value>, StateError>> + Send + 'a>>
    {
        Box::pin(async move {
            let key = ledger_key(tenant, session_id, call_id);
            match self.kv.get(&key).await? {
                Some(bytes) => {
                    let entry: ToolLedgerEntry = serde_json::from_slice(&bytes)
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
            let bytes = serde_json::to_vec(&ToolLedgerEntry { result })
                .map_err(|e| StateError::Decode(format!("ledger encode: {e}")))?;
            self.kv.set_ex(&key, bytes, KV_LEDGER_TTL).await
        })
    }
}

#[cfg(test)]
mod ctx_tests {
    use super::*;
    use crate::tenant::TenantContext;

    #[test]
    fn host_ctx_carries_tenant_and_optional_user() {
        let c1 = host_ctx_from_tenant(&TenantContext::new("acme", "prod"));
        assert_eq!(c1.tenant.as_deref(), Some("acme"));
        assert_eq!(c1.user_email, None);
        let c2 = host_ctx_from_tenant(
            &TenantContext::new("acme", "prod").with_user_email(Some("u@x.com".into())),
        );
        assert_eq!(c2.user_email.as_deref(), Some("u@x.com"));
        let c3 = host_ctx_from_tenant(&TenantContext::new("", ""));
        assert_eq!(
            c3.tenant, None,
            "empty tenant_id must map to None, not Some(\"\")"
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn with_usage_note_appends_when_present() {
        let d = with_usage_note(
            "Base description.".into(),
            &Some("Use for VIP customers.".into()),
        );
        assert_eq!(d, "Base description.\n\nUse for VIP customers.");
    }

    #[test]
    fn with_usage_note_noop_when_none_or_blank() {
        assert_eq!(with_usage_note("Base.".into(), &None), "Base.");
        assert_eq!(
            with_usage_note("Base.".into(), &Some("   ".into())),
            "Base."
        );
    }

    #[test]
    fn is_tool_allowed_returns_true_for_exact_match() {
        let allowed = vec![ToolRef {
            extension_id: "http".into(),
            tool_name: "fetch".into(),
            description: None,
            input_schema: None,
            usage_note: None,
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
            description: None,
            input_schema: None,
            usage_note: None,
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
        let rt = ExtensionRuntime::for_test().unwrap();
        let allowed = vec![ToolRef {
            extension_id: "http".into(),
            tool_name: "fetch".into(),
            description: None,
            input_schema: None,
            usage_note: None,
        }];
        let schemas = list_tools_for_llm(&rt, None, None, None, None, None, &allowed);
        assert!(schemas.is_empty());
    }

    #[test]
    fn missing_tools_reports_unloaded_extension() {
        // No extensions loaded → the declared tool cannot resolve and is
        // reported as missing with a load-failure reason (instead of being
        // dropped silently, which is what causes hallucinated tool results).
        let rt = ExtensionRuntime::for_test().unwrap();
        let allowed = vec![ToolRef {
            extension_id: "greentic.hubspot".into(),
            tool_name: "hubspot_contacts".into(),
            description: None,
            input_schema: None,
            usage_note: None,
        }];
        let missing = missing_tools(&rt, None, None, None, None, None, &allowed);
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
        let rt = ExtensionRuntime::for_test().unwrap();
        let allowed = vec![ToolRef {
            extension_id: "mcp:github".into(),
            tool_name: "create_issue".into(),
            description: None,
            input_schema: None,
            usage_note: None,
        }];
        // No catalog provided → the mcp tool is unresolvable.
        let missing = missing_tools(&rt, None, None, None, None, None, &allowed);
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
        McpToolCatalog::from_parts(tools, routes, None)
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
        let rt = ExtensionRuntime::for_test().unwrap();
        let params = serde_json::json!({
            "type": "object",
            "properties": { "id": { "type": "string" } }
        });
        let catalog = catalog_with("s1", "get_issue", "Get an issue", params.clone(), None);

        let allowed = vec![
            ToolRef {
                extension_id: "mcp:s1".into(),
                tool_name: "get_issue".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            },
            // Absent from the catalog → dropped (warn), not panicked.
            ToolRef {
                extension_id: "mcp:s1".into(),
                tool_name: "missing".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            },
        ];

        let schemas = list_tools_for_llm(&rt, Some(&catalog), None, None, None, None, &allowed);
        assert_eq!(schemas.len(), 1, "only the catalog-backed ref is emitted");
        let s = &schemas[0];
        assert_eq!(s.extension_id, "mcp:s1");
        assert_eq!(s.tool_name, "get_issue");
        assert_eq!(s.description, "Get an issue");
        assert_eq!(s.parameters, params);
    }

    /// The author contract a pack's `AgentConfig` carries for an `mcp:` binding.
    fn mcp_ref_with_contract(server: &str, tool: &str) -> ToolRef {
        ToolRef {
            extension_id: format!("mcp:{server}"),
            tool_name: tool.into(),
            description: Some("Pack-carried description".into()),
            input_schema: Some(serde_json::json!({
                "type": "object",
                "properties": { "pack_arg": { "type": "string" } }
            })),
            usage_note: None,
        }
    }

    #[test]
    fn mcp_ref_listed_from_tool_ref_when_catalog_has_no_entry() {
        // The deployed-runner case: no admin credentials, so no catalog at all.
        // The schema the designer snapshotted into the pack's `AgentConfig` is
        // what reaches the LLM. Before this, the tool was silently dropped.
        let rt = ExtensionRuntime::for_test().unwrap();
        let allowed = vec![mcp_ref_with_contract("s1", "get_issue")];

        let schemas = list_tools_for_llm(&rt, None, None, None, None, None, &allowed);
        assert_eq!(schemas.len(), 1, "the author contract resolves the tool");
        assert_eq!(schemas[0].extension_id, "mcp:s1");
        assert_eq!(schemas[0].tool_name, "get_issue");
        assert_eq!(schemas[0].description, "Pack-carried description");
        assert_eq!(
            schemas[0].parameters["properties"]["pack_arg"]["type"],
            "string"
        );
    }

    #[test]
    fn mcp_catalog_entry_wins_over_tool_ref_contract() {
        // REGRESSION GUARD for every existing admin-backed deployment: when the
        // catalog has an entry it was probed from the live server this run, and
        // it must be emitted byte-for-byte even though the ref also carries a
        // (possibly stale) snapshot. Flipping the precedence would silently
        // downgrade every deployment that has a working admin source.
        let rt = ExtensionRuntime::for_test().unwrap();
        let live_params = serde_json::json!({
            "type": "object",
            "properties": { "live_arg": { "type": "number" } }
        });
        let catalog = catalog_with("s1", "get_issue", "Live description", live_params, None);
        let allowed = vec![mcp_ref_with_contract("s1", "get_issue")];

        let schemas = list_tools_for_llm(&rt, Some(&catalog), None, None, None, None, &allowed);
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].description, "Live description");
        assert_eq!(
            schemas[0].parameters["properties"]["live_arg"]["type"], "number",
            "the catalog's parameters must survive intact"
        );
        assert!(
            schemas[0].parameters["properties"]
                .get("pack_arg")
                .is_none(),
            "the pack snapshot must not leak into a catalog-resolved schema"
        );
    }

    #[test]
    fn mcp_ref_with_partial_contract_and_no_catalog_is_dropped() {
        // Both halves are required, mirroring the `flow:` branch: a description
        // with no schema cannot be offered to the LLM as a callable tool.
        let rt = ExtensionRuntime::for_test().unwrap();
        let mut description_only = mcp_ref_with_contract("s1", "get_issue");
        description_only.input_schema = None;
        let mut schema_only = mcp_ref_with_contract("s1", "list_issues");
        schema_only.description = None;
        let bare = ToolRef {
            extension_id: "mcp:s1".into(),
            tool_name: "nothing".into(),
            description: None,
            input_schema: None,
            usage_note: None,
        };

        let schemas = list_tools_for_llm(
            &rt,
            None,
            None,
            None,
            None,
            None,
            &[description_only, schema_only, bare],
        );
        assert!(schemas.is_empty(), "got: {schemas:?}");
    }

    #[test]
    fn mcp_ref_contract_still_takes_the_usage_note() {
        // The usage note is an author addendum to whichever description won; it
        // must not be lost on the new fallback arm.
        let rt = ExtensionRuntime::for_test().unwrap();
        let mut t = mcp_ref_with_contract("s1", "get_issue");
        t.usage_note = Some("Only for open issues.".into());

        let schemas = list_tools_for_llm(&rt, None, None, None, None, None, &[t]);
        assert_eq!(schemas.len(), 1);
        assert_eq!(
            schemas[0].description,
            "Pack-carried description\n\nOnly for open issues."
        );
    }

    #[test]
    fn missing_tools_accepts_mcp_tool_resolved_by_its_author_contract() {
        // `preflight_warn_tools` would otherwise warn loudly at startup about a
        // tool that `list_tools_for_llm` now resolves and offers.
        let rt = ExtensionRuntime::for_test().unwrap();
        let allowed = vec![mcp_ref_with_contract("s1", "get_issue")];
        assert!(missing_tools(&rt, None, None, None, None, None, &allowed).is_empty());
    }

    #[test]
    fn missing_tools_still_reports_mcp_tool_with_no_contract_and_no_catalog() {
        let rt = ExtensionRuntime::for_test().unwrap();
        let mut half = mcp_ref_with_contract("s1", "get_issue");
        half.input_schema = None;
        let missing = missing_tools(&rt, None, None, None, None, None, &[half]);
        assert_eq!(missing.len(), 1);
        assert!(
            missing[0].reason.contains("MCP tool not found"),
            "got: {}",
            missing[0].reason
        );
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
        let rt = ExtensionRuntime::for_test().unwrap();
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
            description: None,
            input_schema: None,
            usage_note: None,
        }];
        let schemas = list_tools_for_llm(&rt, Some(&catalog), None, None, None, None, &allowed);
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
            description: None,
            input_schema: None,
            usage_note: None,
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
        let rt = Arc::new(ExtensionRuntime::for_test().unwrap());

        let call = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "mcp:s1".into(),
            tool_name: "get_issue".into(),
            args: serde_json::json!({}),
        };
        let tc = TenantContext::new("t", "e");
        let out = dispatch_tool_call(
            rt.clone(),
            Some(catalog.clone()),
            None,
            None,
            None,
            None,
            call,
            &tc,
        )
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
        let out = dispatch_tool_call(
            rt.clone(),
            Some(catalog),
            None,
            None,
            None,
            None,
            missing,
            &tc,
        )
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
        let res = dispatch_tool_call(rt, None, None, None, None, None, non_mcp, &tc).await;
        assert!(
            res.is_err(),
            "non-mcp dispatch against an unloaded extension must error"
        );
    }

    use crate::component_source::ComponentToolCatalog;
    use crate::component_source::test_support::{FakeInvoker, one_tool};
    use crate::flow_source::{FlowInvoker, FlowOperation, FlowToolCatalog};
    use crate::sorla_source::test_support::FakeInvoker as SorlaFakeInvoker;
    use crate::sorla_source::{SorlaToolCatalog, SorlaToolEntry};

    struct FakeFlowInvoker;
    impl FlowInvoker for FakeFlowInvoker {
        fn list_flows(&self) -> Vec<FlowOperation> {
            vec![FlowOperation {
                flow_ref: "lookup".into(),
                description: "Look things up".into(),
                parameters: serde_json::json!({ "type": "object", "properties": { "q": { "type": "integer" } } }),
            }]
        }
        fn invoke<'a>(
            &'a self,
            flow_ref: &'a str,
            args_json: &'a str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
        > {
            Box::pin(async move {
                if flow_ref == "lookup" {
                    Ok(serde_json::json!({ "echoed": args_json }))
                } else {
                    Err(format!("flow '{flow_ref}' not found"))
                }
            })
        }
    }

    fn ext_runtime_stub() -> ExtensionRuntime {
        ExtensionRuntime::for_test().unwrap()
    }

    fn test_flow_invoker() -> FakeFlowInvoker {
        FakeFlowInvoker
    }

    #[test]
    fn flow_tool_prefers_author_contract_over_catalog() {
        // Catalog has flow "lookup" with its own description + parameters.
        // ToolRef carries author overrides — these must win.
        let flows = Arc::new(FlowToolCatalog::from_invoker(Arc::new(test_flow_invoker())));
        let allowed = vec![ToolRef {
            extension_id: "flow:lookup".into(),
            tool_name: "look_up".into(),
            description: Some("Author description".into()),
            input_schema: Some(
                serde_json::json!({"type":"object","properties":{"q":{"type":"string"}}}),
            ),
            usage_note: None,
        }];
        let schemas = list_tools_for_llm(
            &ext_runtime_stub(),
            None,
            None,
            Some(&flows),
            None,
            None,
            &allowed,
        );
        let s = schemas
            .iter()
            .find(|s| s.extension_id == "flow:lookup")
            .expect("flow tool listed");
        assert_eq!(
            s.description, "Author description",
            "override description must win over catalog"
        );
        assert_eq!(
            s.parameters["properties"]["q"]["type"], "string",
            "override schema must win over catalog"
        );
    }

    #[test]
    fn flow_tool_falls_back_to_catalog_when_no_override() {
        // ToolRef has no override — the catalog entry must be used.
        let flows = Arc::new(FlowToolCatalog::from_invoker(Arc::new(test_flow_invoker())));
        let allowed = vec![ToolRef {
            extension_id: "flow:lookup".into(),
            tool_name: "look_up".into(),
            description: None,
            input_schema: None,
            usage_note: None,
        }];
        let schemas = list_tools_for_llm(
            &ext_runtime_stub(),
            None,
            None,
            Some(&flows),
            None,
            None,
            &allowed,
        );
        assert!(
            schemas.iter().any(|s| s.extension_id == "flow:lookup"),
            "with no override the catalog entry must still be used to list the tool"
        );
    }

    #[test]
    fn list_tools_appends_usage_note_for_flow_tool() {
        // Flow branch: ToolRef.description supplies the base description
        // directly (no live runtime needed), so this proves the usage_note
        // reaches the LLM-facing schema without an ExtensionRuntime fixture.
        let flows = Arc::new(FlowToolCatalog::from_invoker(Arc::new(test_flow_invoker())));
        let allowed = vec![ToolRef {
            extension_id: "flow:lookup".into(),
            tool_name: "look_up".into(),
            description: Some("Base.".into()),
            input_schema: Some(serde_json::json!({"type":"object","properties":{}})),
            usage_note: Some("note-Z".into()),
        }];
        let schemas = list_tools_for_llm(
            &ext_runtime_stub(),
            None,
            None,
            Some(&flows),
            None,
            None,
            &allowed,
        );
        let s = schemas
            .iter()
            .find(|s| s.extension_id == "flow:lookup")
            .expect("flow tool listed");
        assert!(s.description.contains("Base."), "got: {}", s.description);
        assert!(s.description.contains("note-Z"), "got: {}", s.description);
    }

    #[tokio::test]
    async fn flow_prefixed_tool_is_listed_and_dispatched() {
        let flows = Arc::new(FlowToolCatalog::from_invoker(Arc::new(FakeFlowInvoker)));
        let rt = ExtensionRuntime::for_test().unwrap();
        let allowed = vec![ToolRef {
            extension_id: "flow:lookup".into(),
            tool_name: "look_up".into(),
            description: None,
            input_schema: None,
            usage_note: None,
        }];
        let schemas = list_tools_for_llm(&rt, None, None, Some(&flows), None, None, &allowed);
        assert!(
            schemas
                .iter()
                .any(|s| s.extension_id == "flow:lookup" && s.tool_name == "look_up"),
            "flow: tool must appear in listed schemas"
        );

        let call = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "flow:lookup".into(),
            tool_name: "look_up".into(),
            args: serde_json::json!({ "q": 1 }),
        };
        let rt_arc = Arc::new(ExtensionRuntime::for_test().unwrap());
        let tc = TenantContext::new("t", "e");
        let out = dispatch_tool_call(rt_arc, None, None, Some(flows), None, None, call, &tc)
            .await
            .expect("flow dispatch must not return Err");
        assert!(
            out.get("error").is_none(),
            "known flow must dispatch, got {out}"
        );
    }

    #[test]
    fn component_ref_listed_from_catalog() {
        // A catalog-backed component: ref is emitted as an LlmToolSchema with
        // the catalog's description/parameters; ext_runtime is never consulted.
        let rt = ExtensionRuntime::for_test().unwrap();
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
                description: None,
                input_schema: None,
                usage_note: None,
            },
            // Absent from the catalog → dropped (warn), not panicked.
            ToolRef {
                extension_id: "component:greentic.refund".into(),
                tool_name: "missing".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            },
        ];

        let schemas = list_tools_for_llm(&rt, None, Some(&catalog), None, None, None, &allowed);
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
        let rt = ExtensionRuntime::for_test().unwrap();
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
            description: None,
            input_schema: None,
            usage_note: None,
        }];
        let schemas = list_tools_for_llm(&rt, None, Some(&catalog), None, None, None, &allowed);
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
        let rt = Arc::new(ExtensionRuntime::for_test().unwrap());

        let tc = TenantContext::new("t", "e");
        let call = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "component:greentic.refund".into(),
            tool_name: "issue_refund".into(),
            args: serde_json::json!({}),
        };
        let out = dispatch_tool_call(
            rt.clone(),
            None,
            Some(catalog.clone()),
            None,
            None,
            None,
            call,
            &tc,
        )
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
        let out = dispatch_tool_call(
            rt.clone(),
            None,
            Some(catalog),
            None,
            None,
            None,
            missing,
            &tc,
        )
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
        let out = dispatch_tool_call(rt, None, None, None, None, None, no_cat, &tc)
            .await
            .expect("component dispatch with no catalog still returns Ok");
        assert!(out.to_string().contains("error"), "got: {out}");
    }

    #[test]
    fn list_includes_sorla_ref() {
        // A catalog-backed sorla: ref is emitted as an LlmToolSchema with the
        // catalog's description/parameters; ext_runtime is never consulted.
        let rt = ExtensionRuntime::for_test().unwrap();
        let params = serde_json::json!({
            "type": "object",
            "properties": { "amount": { "type": "number" } }
        });
        let invoker = Arc::new(SorlaFakeInvoker::new(vec![], Ok(serde_json::json!({}))));
        let mut tools = HashMap::new();
        tools.insert(
            ("landlord".to_string(), "record_rent_payment".to_string()),
            SorlaToolEntry {
                description: "Record a rent payment".to_string(),
                parameters: params.clone(),
            },
        );
        let catalog = SorlaToolCatalog::for_tests(tools, invoker);

        let allowed = vec![
            ToolRef {
                extension_id: "sorla:landlord".into(),
                tool_name: "record_rent_payment".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            },
            // Absent from the catalog → dropped (warn), not panicked.
            ToolRef {
                extension_id: "sorla:landlord".into(),
                tool_name: "missing".into(),
                description: None,
                input_schema: None,
                usage_note: None,
            },
        ];

        let schemas = list_tools_for_llm(&rt, None, None, None, Some(&catalog), None, &allowed);
        assert_eq!(schemas.len(), 1, "only the catalog-backed ref is emitted");
        let s = &schemas[0];
        assert_eq!(s.extension_id, "sorla:landlord");
        assert_eq!(s.tool_name, "record_rent_payment");
        assert_eq!(s.description, "Record a rent payment");
        assert_eq!(s.parameters, params);
    }

    #[tokio::test]
    async fn dispatch_routes_sorla_ref() {
        // Catalog entry present → routes to the invoker and returns its value.
        let invoker = Arc::new(SorlaFakeInvoker::new(
            vec![],
            Ok(serde_json::json!({"ok":true})),
        ));
        let mut tools = HashMap::new();
        tools.insert(
            ("landlord".to_string(), "record_rent_payment".to_string()),
            SorlaToolEntry {
                description: "Record a rent payment".to_string(),
                parameters: serde_json::json!({ "type": "object" }),
            },
        );
        let catalog = Arc::new(SorlaToolCatalog::for_tests(tools, invoker));
        let rt = Arc::new(ExtensionRuntime::for_test().unwrap());

        let tc = TenantContext::new("t", "e");
        let call = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "sorla:landlord".into(),
            tool_name: "record_rent_payment".into(),
            args: serde_json::json!({}),
        };
        let out = dispatch_tool_call(rt, None, None, None, Some(catalog), None, call, &tc)
            .await
            .expect("sorla dispatch never returns Err");
        assert_eq!(out, serde_json::json!({"ok":true}), "got: {out}");
    }

    fn a2a_ref(description: Option<&str>, input_schema: Option<serde_json::Value>) -> ToolRef {
        ToolRef {
            extension_id: "a2a:my-agent".into(),
            tool_name: "call".into(),
            description: description.map(str::to_string),
            input_schema,
            usage_note: None,
        }
    }

    #[test]
    fn an_a2a_ref_is_advertised_with_the_cards_description_and_the_authors_schema() {
        // The card wins on description: it is the agent's live statement
        // about itself. The schema can ONLY be the author contract's — an
        // AgentSkill has none — so the ref's own input_schema must survive
        // even though its description is overridden by the card.
        let rt = ExtensionRuntime::for_test().unwrap();
        let params = serde_json::json!({
            "type": "object",
            "properties": { "q": { "type": "string" } }
        });
        let allowed = vec![a2a_ref(Some("local"), Some(params.clone()))];
        let catalog = A2aToolCatalog::for_tests(&[("my-agent", "from the card")], &[]);

        let schemas = list_tools_for_llm(&rt, None, None, None, None, Some(&catalog), &allowed);
        assert_eq!(schemas.len(), 1, "got: {schemas:?}");
        let s = &schemas[0];
        assert_eq!(s.extension_id, "a2a:my-agent");
        assert_eq!(s.description, "from the card");
        assert_eq!(s.parameters, params);
    }

    #[test]
    fn an_a2a_ref_without_an_author_schema_is_not_advertised() {
        // No input_schema anywhere, so the tool cannot be called correctly —
        // an AgentSkill carries no schema, so there is no fallback source.
        let rt = ExtensionRuntime::for_test().unwrap();
        let allowed = vec![a2a_ref(Some("local"), None)];
        let catalog = A2aToolCatalog::for_tests(&[("my-agent", "from the card")], &[]);

        let schemas = list_tools_for_llm(&rt, None, None, None, None, Some(&catalog), &allowed);
        assert!(schemas.is_empty(), "got: {schemas:?}");
    }

    #[test]
    fn an_a2a_ref_falls_back_to_its_own_description_when_the_agent_is_unreachable() {
        // The agent is configured, but its card could not be fetched this run.
        // The ref's own description and schema still advertise the tool.
        let rt = ExtensionRuntime::for_test().unwrap();
        let params = serde_json::json!({"type": "object", "properties": {}});
        let allowed = vec![a2a_ref(Some("own description"), Some(params.clone()))];
        let catalog = A2aToolCatalog::for_tests(&[], &["my-agent"]);

        let schemas = list_tools_for_llm(&rt, None, None, None, None, Some(&catalog), &allowed);
        assert_eq!(schemas.len(), 1, "got: {schemas:?}");
        let s = &schemas[0];
        assert_eq!(s.description, "own description");
        assert_eq!(s.parameters, params);
    }

    #[test]
    fn a_resolvable_a2a_ref_is_not_reported_as_missing() {
        // Without the a2a arm this falls through to the extension runtime and
        // reports "extension failed to load" for a tool that works — a loud,
        // wrong warning on every turn.
        let rt = ExtensionRuntime::for_test().unwrap();
        let params = serde_json::json!({"type": "object", "properties": {}});
        let allowed = vec![a2a_ref(None, Some(params))];
        let catalog = A2aToolCatalog::for_tests(&[("my-agent", "from the card")], &[]);

        let missing = missing_tools(&rt, None, None, None, None, Some(&catalog), &allowed);
        assert!(missing.is_empty(), "got: {missing:?}");
    }

    #[test]
    fn an_a2a_ref_with_no_schema_and_no_catalog_entry_is_reported_as_missing() {
        // Genuinely unusable: no card, no author schema. Must be reported,
        // and the reason must name a2a rather than extensions.
        let rt = ExtensionRuntime::for_test().unwrap();
        let allowed = vec![a2a_ref(None, None)];

        let missing = missing_tools(&rt, None, None, None, None, None, &allowed);
        assert_eq!(missing.len(), 1, "got: {missing:?}");
        assert_eq!(missing[0].extension_id, "a2a:my-agent");
        assert!(
            missing[0].reason.contains("a2a"),
            "got: {}",
            missing[0].reason
        );
    }

    #[test]
    fn an_a2a_ref_for_an_unconfigured_agent_is_neither_advertised_nor_silent() {
        // A full author contract is not enough: dispatch refuses an agent the
        // worker was not configured with, so listing it would offer the model
        // a call that can only fail. The listing and the preflight check must
        // agree — dropped from one, reported by the other.
        let rt = ExtensionRuntime::for_test().unwrap();
        let params = serde_json::json!({"type": "object", "properties": {}});
        let allowed = vec![a2a_ref(Some("own description"), Some(params))];
        let other_agent = A2aToolCatalog::for_tests(&[("someone-else", "x")], &[]);

        for catalog in [None, Some(&other_agent)] {
            let schemas = list_tools_for_llm(&rt, None, None, None, None, catalog, &allowed);
            assert!(
                schemas.is_empty(),
                "must not be advertised, got: {schemas:?}"
            );

            let missing = missing_tools(&rt, None, None, None, None, catalog, &allowed);
            assert_eq!(missing.len(), 1, "must be reported, got: {missing:?}");
            assert!(
                missing[0].reason.contains("not configured"),
                "got: {}",
                missing[0].reason
            );
        }
    }

    #[tokio::test]
    async fn dispatching_an_a2a_call_with_no_catalog_yields_an_error_value_naming_the_agent() {
        // With no catalogue wired at all, dispatch must still return `Ok` with
        // an `{"error": ...}` value the LLM can observe — never `Err`, never a
        // panic.
        let rt = Arc::new(ExtensionRuntime::for_test().unwrap());
        let tc = TenantContext::new("t", "e");
        let call = ToolCallRecord {
            call_id: "c1".into(),
            extension_id: "a2a:my-agent".into(),
            tool_name: "call".into(),
            args: serde_json::json!({ "message": "hi" }),
        };
        let out = dispatch_tool_call(rt, None, None, None, None, None, call, &tc)
            .await
            .expect("a2a dispatch with no catalog must not return Err");
        let error = out
            .get("error")
            .and_then(|v| v.as_str())
            .expect("expected an error value");
        assert!(error.contains("my-agent"), "got: {error}");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod kv_ledger_tests {
    use super::*;
    use crate::kv::MemoryKv;
    use std::sync::Arc;

    #[tokio::test]
    async fn record_then_get_replays_result() {
        let ledger = KvToolLedger::new(Arc::new(MemoryKv::new()));
        let t = TenantContext::new("acme", "prod");
        assert!(ledger.get(&t, "sess", "call1").await.unwrap().is_none());
        ledger
            .record(&t, "sess", "call1", serde_json::json!({"ok": true}))
            .await
            .unwrap();
        let got = ledger.get(&t, "sess", "call1").await.unwrap();
        assert_eq!(got, Some(serde_json::json!({"ok": true})));
    }
}
