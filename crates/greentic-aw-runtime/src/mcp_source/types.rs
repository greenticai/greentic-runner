//! Type definitions for the per-tenant agentic-worker MCP tool catalog.
//!
//! Contains the wire types, domain types, and the immutable catalog snapshot
//! ([`McpToolCatalog`]). Transport and dispatch logic lives in `source.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use greentic_secrets_lib::SecretsManager;
use secrecy::SecretString;
use serde::Deserialize;

/// How long a built catalog is reused before a refetch is considered.
pub(super) const CATALOG_TTL: Duration = Duration::from_secs(5 * 60);

/// Per-server budget for a catalog PROBE (connect → initialize → tools/list).
///
/// Deliberately short. A probe only asks "is this server reachable and what
/// does it offer?", and it runs for every server on the catalog path, so a dead
/// or hanging one must be skipped quickly rather than holding up the servers
/// that do work.
pub(super) const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default budget for a tool CALL (connect → initialize → tools/call).
///
/// This is a different question from the probe budget and needs a different
/// answer: the tool is doing real work. A desktop agent driving an application
/// to produce a quote routinely takes tens of seconds, and sharing the probe's
/// 5s made every such tool permanently uncallable — reported only as
/// `tool call timed out after 5s`, which reads like a network fault rather than
/// a budget that was never meant to cover work.
pub(super) const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(120);

/// Operator override for [`DEFAULT_CALL_TIMEOUT`], in whole seconds.
pub(super) const CALL_TIMEOUT_ENV: &str = "GREENTIC_AW_MCP_CALL_TIMEOUT_SECS";

/// Resolve the tool-call budget from an already-read env value.
///
/// Pure so it can be tested without mutating the process environment. Anything
/// unusable — absent, unparseable, or zero — falls back to the default rather
/// than failing: a malformed knob must not make every MCP tool uncallable,
/// which is the failure this whole budget split exists to remove. A zero would
/// time out every call instantly, so it is treated as unusable rather than
/// honoured as "no time at all".
pub(super) fn call_timeout_from(raw: Option<&str>) -> Duration {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_CALL_TIMEOUT)
}

/// The tool-call budget for this process.
pub(super) fn call_timeout() -> Duration {
    call_timeout_from(std::env::var(CALL_TIMEOUT_ENV).ok().as_deref())
}

/// Role a server must carry to be exposed to agentic workers (the agent-loop
/// MCP path).
pub const MCP_ROLE_AGENTIC_WORKER: &str = "agentic_worker";

/// Role a server must carry to be exposed to the flow-execution MCP path
/// (`mcp:<server>/<tool>` flow nodes). Distinct from
/// [`MCP_ROLE_AGENTIC_WORKER`] so the operator can authorize a server for one
/// surface without the other.
pub const MCP_ROLE_FLOW_EDITOR: &str = "flow_editor";

/// Who an [`McpToolSource`] is asking the admin on behalf of.
///
/// The admin resolves RBAC from request headers, so a caller that sends only a
/// bearer forces that bearer to imply the tenant — i.e. a tenant-scoped
/// `gtc_live_*` token per tenant, captured once at construction. That works for
/// a runner process dedicated to one tenant and does NOT work for an embedding
/// host serving many tenants from one process (the designer): its Run Demo
/// builds a host per tenant, but a process-global token can only name one of
/// them, so every tenant would read the same tenant's servers.
///
/// Supplying an identity moves the tenant from the credential to the request,
/// letting such a host present the service key it already holds and say per
/// call who it is acting for.
///
/// [`McpToolSource`]: super::source::McpToolSource
#[derive(Clone, Debug)]
pub struct McpCallerIdentity {
    pub tenant_slug: String,
    pub user_email: String,
    /// The caller's team. `None` for a caller that belongs to no team (an
    /// operator session spans tenants), which sends no team header at all.
    pub team_slug: Option<String>,
}

impl McpCallerIdentity {
    pub fn new(tenant_slug: impl Into<String>, user_email: impl Into<String>) -> Self {
        Self {
            tenant_slug: tenant_slug.into(),
            user_email: user_email.into(),
            team_slug: None,
        }
    }

    /// Name the team this caller is acting in. MCP servers are stored per-team
    /// on the admin, so omitting a team the caller really has resolves a
    /// different server set than that team's authoring UI listed.
    pub fn with_team(mut self, team_slug: impl Into<String>) -> Self {
        self.team_slug = Some(team_slug.into());
        self
    }
}

/// Which transport backs an MCP server row. `http` is the default (existing
/// remote behavior); `local-wasm` runs a `wasix:mcp` component in-process.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Transport {
    #[default]
    Http,
    LocalWasm,
}

/// Wire row from `/api/v1/designer/tenant/me/mcp-servers`.
#[derive(Deserialize)]
pub(super) struct WireRow {
    pub(super) id: String,
    #[allow(dead_code)]
    pub(super) name: String,
    /// `Option` because the admin serializes `transport_url: null` for
    /// `local-wasm` servers (they have no URL). A bare `String` here makes
    /// serde reject the WHOLE server list on the first local-wasm row, silently
    /// dropping every MCP tool for that tenant. `#[serde(default)]` also tolerates
    /// the field being absent. Mapped to `""` in `parse_rows`; the local-wasm
    /// branch never parses it as a URL.
    #[serde(default)]
    pub(super) transport_url: Option<String>,
    pub(super) auth_header_name: Option<String>,
    pub(super) auth_token: Option<String>,
    /// `None` = expose every tool; `Some([])` = none; else whitelist of raw names.
    pub(super) allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    pub(super) roles: Vec<String>,
    /// Transport discriminator; defaults to [`Transport::Http`] when absent so
    /// every existing row continues to work without changes.
    #[serde(default)]
    pub(super) transport: Transport,
    /// For `local-wasm` rows: the component reference (e.g. `weather.component`).
    pub(super) component_ref: Option<String>,
    /// For `local-wasm` rows: optional pinned component version.
    pub(super) component_version: Option<String>,
    /// For `local-wasm` rows: optional pinned component content digest (hex).
    #[serde(default)]
    pub(super) component_digest: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct WireBody {
    pub(super) servers: Vec<WireRow>,
}

/// One enabled MCP server, with its token sealed in a [`SecretString`].
pub(super) struct ParsedServer {
    pub(super) id: String,
    pub(super) transport_url: String,
    pub(super) auth_header_name: Option<String>,
    pub(super) auth_token: Option<SecretString>,
    pub(super) allowed_tools: Option<Vec<String>>,
    pub(super) roles: Vec<String>,
    pub(super) transport: Transport,
    pub(super) component_ref: Option<String>,
    pub(super) component_version: Option<String>,
    pub(super) component_digest: Option<String>,
}

/// LLM-facing schema for one MCP tool: enough for Task 2 to build an
/// `LlmToolSchema`.
#[derive(Clone, Debug)]
pub struct McpToolEntry {
    pub description: String,
    pub parameters: serde_json::Value,
}

/// Everything needed to re-connect and invoke one MCP tool.
///
/// The `auth_token` is redacted from [`Debug`] (hand-written below) and must
/// never be logged or serialized.
#[derive(Clone)]
pub struct McpRoute {
    pub(super) server_id: String,
    pub(super) transport_url: String,
    pub(super) auth_header_name: Option<String>,
    pub(super) auth_token: Option<SecretString>,
    pub(super) raw_tool_name: String,
    pub transport: Transport,
    pub component_ref: Option<String>,
    pub component_version: Option<String>,
    pub component_digest: Option<String>,
}

impl McpRoute {
    /// Build a route from parts supplied by an embedding host (a pack-carried
    /// record), rather than from an admin row. The raw tool name is set per
    /// call by the dispatcher, so it starts empty.
    ///
    /// This exists because [`McpRoute`]'s fields are `pub(super)`: a host that
    /// resolves a route WITHOUT the admin catalog — a deployed runner reading
    /// `assets/mcp-routes.json` out of its own pack — otherwise has no way to
    /// express one.
    ///
    /// `auth_token` is sealed in a [`SecretString`] on the way in and stays
    /// redacted from [`Debug`], exactly as it is for an admin-built route.
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        server_id: &str,
        transport_url: &str,
        auth_header_name: Option<&str>,
        auth_token: Option<&str>,
        transport: &str,
        component_ref: Option<&str>,
        component_version: Option<&str>,
        component_digest: Option<&str>,
    ) -> Self {
        Self {
            server_id: server_id.to_string(),
            transport_url: transport_url.to_string(),
            auth_header_name: auth_header_name.map(str::to_string),
            auth_token: auth_token.map(SecretString::from),
            raw_tool_name: String::new(),
            transport: if transport == "local-wasm" {
                Transport::LocalWasm
            } else {
                Transport::Http
            },
            component_ref: component_ref.map(str::to_string),
            component_version: component_version.map(str::to_string),
            component_digest: component_digest.map(str::to_string),
        }
    }

    /// Name the tool this route is about to call.
    #[must_use]
    pub fn with_tool(mut self, raw_tool_name: &str) -> Self {
        self.raw_tool_name = raw_tool_name.to_string();
        self
    }
}

impl std::fmt::Debug for McpRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpRoute")
            .field("server_id", &self.server_id)
            .field("transport_url", &self.transport_url)
            .field("auth_header_name", &self.auth_header_name)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("raw_tool_name", &self.raw_tool_name)
            .field("transport", &self.transport)
            .field("component_ref", &self.component_ref)
            .field("component_version", &self.component_version)
            .field("component_digest", &self.component_digest)
            .finish()
    }
}

/// Non-secret route material for one server, as carried by a `.gtpack`'s
/// `assets/mcp-routes.json` sidecar.
///
/// The runner-host side owns the deserialization (`runner::mcp_pack_routes`);
/// this is the shape it hands to [`super::McpToolSource::from_pack_routes`]. It
/// carries no token — `auth_team` names the scope the token is SEALED at, not
/// the token itself.
#[derive(Clone, Debug, Default)]
pub struct McpPackRoute {
    pub server_id: String,
    /// `"http"` or `"local-wasm"`.
    pub transport: String,
    pub transport_url: Option<String>,
    pub auth_header_name: Option<String>,
    /// Team slug the authoring session resolved this server's token under.
    /// `None` means the tenant-default `_` scope.
    pub auth_team: Option<String>,
    pub component_ref: Option<String>,
    pub component_version: Option<String>,
    pub component_digest: Option<String>,
}

/// Immutable per-tenant view of the agentic-worker MCP tool surface.
pub struct McpToolCatalog {
    /// `(server_id, raw_tool_name)` → LLM-facing tool schema.
    pub(super) tools: HashMap<(String, String), McpToolEntry>,
    /// `(server_id, raw_tool_name)` → dispatch route.
    pub(super) routes: HashMap<(String, String), McpRoute>,
    /// `server_id` → dispatch route with no tool name bound yet. Populated only
    /// on the pack-backed path, where the tool names a server offers are never
    /// discovered (no probe); see [`McpToolCatalog::with_server_routes`].
    pub(super) server_routes: HashMap<String, McpRoute>,
    /// `server_id` → why this server has no usable route this run. Lets the
    /// dispatch error name the real cause instead of "unknown mcp tool".
    pub(super) server_errors: HashMap<String, String>,
    pub(super) fetched_at: Instant,
    /// Tenant secrets manager carried over from the [`super::McpToolSource`]
    /// that built this catalog, if any, so a `local-wasm` dispatch route
    /// reached through this catalog can build a secrets-bearing
    /// [`crate::mcp_scope::McpCallScope`].
    pub(super) secrets: Option<Arc<dyn SecretsManager>>,
}

impl McpToolCatalog {
    pub(super) fn empty() -> Self {
        Self {
            tools: HashMap::new(),
            routes: HashMap::new(),
            server_routes: HashMap::new(),
            server_errors: HashMap::new(),
            fetched_at: Instant::now(),
            secrets: None,
        }
    }

    /// Iterate every `(server_id, raw_tool_name)` tool key with its schema.
    /// Task 2 uses this to build the LLM tool list.
    pub fn tools(&self) -> impl Iterator<Item = (&(String, String), &McpToolEntry)> {
        self.tools.iter()
    }

    /// Number of tools in the catalog.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Whether the catalog exposes no tools.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// LLM-facing schema for one tool, if present.
    pub fn tool_entry(&self, server_id: &str, tool: &str) -> Option<&McpToolEntry> {
        self.tools.get(&(server_id.to_string(), tool.to_string()))
    }

    /// Dispatch route for one tool, if the catalog holds an exact entry for it.
    pub fn route(&self, server_id: &str, tool: &str) -> Option<&McpRoute> {
        self.routes.get(&(server_id.to_string(), tool.to_string()))
    }

    /// Dispatch route for one tool, falling back to the server-level route when
    /// there is no exact entry.
    ///
    /// The exact entry always WINS: an admin-built catalog probed the server and
    /// knows every `(server_id, raw_tool_name)` pair, and its route carries that
    /// server's own `allowed_tools` filtering by construction. The fallback only
    /// fires on the pack-backed path, which never probes and therefore only ever
    /// knows the SERVER — the tool name arrives from the agent's own `ToolRef`
    /// and is stamped on here.
    pub fn resolve_route(&self, server_id: &str, tool: &str) -> Option<McpRoute> {
        if let Some(route) = self.route(server_id, tool) {
            return Some(route.clone());
        }
        self.server_routes
            .get(server_id)
            .map(|route| route.clone().with_tool(tool))
    }

    /// Why `server_id` has no usable route this run, when that is known.
    /// `None` on any admin-built catalog, which records no such diagnostics.
    pub fn server_error(&self, server_id: &str) -> Option<&str> {
        self.server_errors.get(server_id).map(String::as_str)
    }

    /// Tenant secrets manager carried over from the source that built this
    /// catalog, if any. `local-wasm` dispatch call sites use this to build a
    /// secrets-bearing [`crate::mcp_scope::McpCallScope`].
    pub fn secrets(&self) -> Option<Arc<dyn SecretsManager>> {
        self.secrets.clone()
    }

    /// Build a catalog directly from tool/route maps, bypassing the admin fetch
    /// and the MCP probe.
    ///
    /// Public for the same reason [`McpRoute::from_parts`] is: an embedding host
    /// that resolves its MCP surface WITHOUT the admin — a deployed runner
    /// reading `assets/mcp-routes.json` out of its own pack — otherwise has no
    /// way to express a catalog at all, because the fields are `pub(super)` and
    /// the only other constructor performs a network fetch plus a per-server
    /// probe.
    ///
    /// Tests use it for the same reason, so there is exactly one constructor
    /// rather than a test twin with the same body.
    pub fn from_parts(
        tools: HashMap<(String, String), McpToolEntry>,
        routes: HashMap<(String, String), McpRoute>,
        secrets: Option<Arc<dyn SecretsManager>>,
    ) -> Self {
        Self {
            tools,
            routes,
            server_routes: HashMap::new(),
            server_errors: HashMap::new(),
            fetched_at: Instant::now(),
            secrets,
        }
    }

    /// Attach per-SERVER fallback routes and their per-server diagnostics.
    /// Used by the pack-backed source; see [`McpToolCatalog::resolve_route`].
    #[must_use]
    pub fn with_server_routes(
        mut self,
        server_routes: HashMap<String, McpRoute>,
        server_errors: HashMap<String, String>,
    ) -> Self {
        self.server_routes = server_routes;
        self.server_errors = server_errors;
        self
    }
}

/// Build a dispatch route pointing at `transport_url` for `(server_id, tool)`.
/// Test-only constructor: `McpRoute` fields are private, so downstream test
/// code uses this to aim a route at a fake MCP server.
#[cfg(test)]
pub(crate) fn route_for_tests(server_id: &str, tool: &str, transport_url: &str) -> McpRoute {
    McpRoute::from_parts(
        server_id,
        transport_url,
        None,
        None,
        "http",
        None,
        None,
        None,
    )
    .with_tool(tool)
}
