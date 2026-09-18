//! [`McpToolSource`] — per-tenant, TTL-gated source of agentic-worker MCP
//! tool catalogs — plus all transport-level helpers (connect, list, dispatch).

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use secrecy::ExposeSecret;
use serde_json::json;

use greentic_mcp_client::{McpAuth, McpClientOptions, McpHttpClient, McpToolDef};

use crate::mcp_scope::McpCallScope;
use crate::tenant::TenantContext;

use super::types::{
    CATALOG_TTL, MCP_ROLE_AGENTIC_WORKER, McpCallerIdentity, McpPackRoute, McpRoute,
    McpToolCatalog, McpToolEntry, PROBE_TIMEOUT, ParsedServer, Transport, WireBody, call_timeout,
};

/// Per-tenant, TTL-gated source of agentic-worker MCP tool catalogs.
///
/// Mirrors [`crate::http_provider::HttpConfigProvider`]: the admin origin and
/// a tenant `gtc_live_*` bearer token are captured at construction; the tenant
/// is implied by the token, with the [`TenantContext`] used only as the cache
/// key.
///
/// An embedding host that serves many tenants from one process cannot use a
/// tenant-implying token; it attaches an [`McpCallerIdentity`] instead, which
/// names the tenant per request. See [`McpToolSource::with_identity`].
pub struct McpToolSource {
    base_url: String,
    token: String,
    identity: Option<McpCallerIdentity>,
    client: reqwest::Client,
    cache: DashMap<String, Arc<McpToolCatalog>>,
    secrets: Option<Arc<dyn greentic_secrets_lib::SecretsManager>>,
    /// When set, catalogs are built from these pack-carried records instead of
    /// fetched from the admin. Mutually exclusive with `base_url`/`token`, which
    /// are empty on this path — see [`McpToolSource::from_pack_routes`].
    pack_routes: Option<Vec<McpPackRoute>>,
}

impl McpToolSource {
    /// `base_url` is the admin origin (no trailing slash needed); `token` is a
    /// tenant `gtc_live_*` key.
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        // A per-request timeout bounds a hung admin so a slow registry cannot
        // block the step indefinitely (mirrors `HttpConfigProvider::new`).
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            identity: None,
            client,
            cache: DashMap::new(),
            secrets: None,
            pack_routes: None,
        }
    }

    /// Build a source backed by the route material a `.gtpack` carries in
    /// `assets/mcp-routes.json`, for a deployed runner that has no admin
    /// credentials at all.
    ///
    /// Differences from the admin path, each deliberate:
    ///
    /// - **No fetch, no probe.** The sidecar names the server; the tool NAME and
    ///   SCHEMA arrive from the agent's own `ToolRef` (spec §4.2 S2). Probing
    ///   would make the tool list depend on the server being reachable at step
    ///   time, so a momentarily-down server would silently shrink the agent's
    ///   tool set rather than fail a call honestly.
    /// - **No role filter.** The sidecar's id set IS the authorization decision,
    ///   already made twice upstream: the operator granted the server its role
    ///   in the admin, and the author bound the tool in the composer.
    /// - **The token still re-resolves per catalog build**, from the secrets
    ///   backend, under the existing `CATALOG_TTL`. That is why this is a
    ///   *source* and not a bare catalog: a catalog built once at construction
    ///   would need a process restart to pick up a rotated token.
    ///
    /// `allowed_tools` is NOT re-checked here. The admin row can whitelist tool
    /// names and the pack path consults no whitelist — true for flow nodes today
    /// and for agent tools now. Bounded (the authored binding is already a
    /// subset of what was allowed at authoring time) and tracked in spec §9 as a
    /// fix for BOTH paths at once or neither.
    pub fn from_pack_routes(
        routes: Vec<McpPackRoute>,
        secrets: Option<Arc<dyn greentic_secrets_lib::SecretsManager>>,
    ) -> Self {
        let mut source = Self::new(String::new(), String::new());
        source.secrets = secrets;
        source.pack_routes = Some(routes);
        source
    }

    /// Name the tenant + user this source asks the admin on behalf of, so the
    /// bearer no longer has to imply them (see [`McpCallerIdentity`]).
    ///
    /// Without this the source keeps its existing behaviour exactly: bearer
    /// only, tenant implied by the token.
    pub fn with_identity(mut self, identity: McpCallerIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Same as [`McpToolSource::new`] plus the tenant secrets manager, so
    /// `local-wasm` components dispatched from this source's catalogs can read
    /// their credentials.
    pub fn with_secrets(
        base_url: impl Into<String>,
        token: impl Into<String>,
        secrets: Arc<dyn greentic_secrets_lib::SecretsManager>,
    ) -> Self {
        let mut source = Self::new(base_url, token);
        source.secrets = Some(secrets);
        source
    }

    pub fn secrets(&self) -> Option<Arc<dyn greentic_secrets_lib::SecretsManager>> {
        self.secrets.clone()
    }

    /// Stable per-tenant, per-role cache key. `TenantContext` exposes no single
    /// opaque id, so the `(tenant_id, env_id)` pair is joined — the same pair
    /// `TenantContext::key_prefix` is built from. The `role` is appended so the
    /// agentic-worker and flow-editor catalogs for the same tenant do not
    /// collide in the shared cache.
    fn cache_key(tenant: &TenantContext, role: &str) -> String {
        format!("{}:{}:{}", tenant.tenant_id, tenant.env_id, role)
    }

    /// Return the tenant's agentic-worker MCP tool catalog (filtered to the
    /// [`MCP_ROLE_AGENTIC_WORKER`] role), rebuilding when stale or absent.
    ///
    /// Convenience wrapper over [`catalog_for_role`] preserving the agent-loop
    /// call site. See [`catalog_for_role`] for the resilience contract.
    ///
    /// [`catalog_for_role`]: McpToolSource::catalog_for_role
    pub async fn catalog(&self, tenant: &TenantContext) -> Arc<McpToolCatalog> {
        self.catalog_for_role(tenant, MCP_ROLE_AGENTIC_WORKER).await
    }

    /// Return the tenant's MCP tool catalog filtered to servers carrying
    /// `role`, rebuilding when stale or absent. Use [`MCP_ROLE_FLOW_EDITOR`]
    /// for the flow-execution path and [`MCP_ROLE_AGENTIC_WORKER`] for the
    /// agent loop.
    ///
    /// Infallible by contract: any admin network or non-200 response degrades
    /// to an empty (cached) catalog with a `warn`, and a dead/slow MCP server
    /// is skipped with a `warn` while the catalog still returns the servers
    /// that worked.
    pub async fn catalog_for_role(
        &self,
        tenant: &TenantContext,
        role: &str,
    ) -> Arc<McpToolCatalog> {
        let key = Self::cache_key(tenant, role);

        if let Some(entry) = self.cache.get(&key) {
            let snap = entry.value();
            if snap.fetched_at.elapsed() < CATALOG_TTL {
                return snap.clone();
            }
        }

        let built = Arc::new(match &self.pack_routes {
            Some(routes) => self.build_pack_catalog(routes, tenant).await,
            None => self.build_catalog(&key, role).await,
        });
        self.cache.insert(key, built.clone());
        built
    }

    /// Build a catalog from pack-carried records: resolve each `http` route's
    /// token from the secrets backend, register one route per SERVER, and skip
    /// the probe entirely. See [`McpToolSource::from_pack_routes`].
    ///
    /// Infallible by the same contract as the admin path: a server whose
    /// credential will not resolve is recorded as a per-server diagnostic and
    /// left routeless, so the eventual dispatch error names the URI and the
    /// `SECRETS_BACKEND=broker` requirement instead of an opaque
    /// "unknown mcp tool".
    async fn build_pack_catalog(
        &self,
        routes: &[McpPackRoute],
        tenant: &TenantContext,
    ) -> McpToolCatalog {
        let mut server_routes = std::collections::HashMap::new();
        let mut server_errors = std::collections::HashMap::new();

        for record in routes {
            // Only an `http` route reads a token. A `local-wasm` route has no
            // HTTP credential at all — admin writes no `mcp/<server_id>` entry
            // for one — and the component reads its own secrets through the
            // `McpCallScope` the caller attaches. Reading unconditionally would
            // fail every local-wasm pack route on a credential that is not
            // supposed to exist.
            let is_http = record.transport != "local-wasm";
            let token = match self.secrets.as_ref().filter(|_| is_http) {
                Some(manager) => {
                    match crate::mcp_secrets::read_mcp_secret(
                        manager.as_ref(),
                        &tenant.tenant_id,
                        record.auth_team.as_deref(),
                        &record.server_id,
                    )
                    .await
                    {
                        Ok(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
                        Err(miss) => {
                            let detail = miss.to_string();
                            tracing::warn!(
                                tenant = %tenant.tenant_id,
                                server = %record.server_id,
                                detail = %detail,
                                "pack-carried mcp route has no resolvable credential; \
                                 its tools will report the cause when called"
                            );
                            server_errors.insert(record.server_id.clone(), detail);
                            continue;
                        }
                    }
                }
                None => None,
            };

            server_routes.insert(
                record.server_id.clone(),
                McpRoute::from_parts(
                    &record.server_id,
                    record.transport_url.as_deref().unwrap_or_default(),
                    record.auth_header_name.as_deref(),
                    token.as_deref(),
                    &record.transport,
                    record.component_ref.as_deref(),
                    record.component_version.as_deref(),
                    record.component_digest.as_deref(),
                ),
            );
        }

        tracing::debug!(
            tenant = %tenant.tenant_id,
            servers = server_routes.len(),
            unresolved = server_errors.len(),
            "pack-backed MCP catalog built"
        );
        McpToolCatalog::from_parts(
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            self.secrets(),
        )
        .with_server_routes(server_routes, server_errors)
    }

    /// Fetch the admin rows and probe each server carrying `role`. Always
    /// returns a catalog (possibly empty); never errors.
    async fn build_catalog(&self, tenant_key: &str, role: &str) -> McpToolCatalog {
        let servers = match self.fetch_servers().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    tenant = %tenant_key,
                    error = %e,
                    "mcp-servers fetch failed; serving empty MCP catalog"
                );
                let mut catalog = McpToolCatalog::empty();
                catalog.secrets = self.secrets();
                return catalog;
            }
        };

        let mut catalog = McpToolCatalog::empty();
        catalog.secrets = self.secrets();
        for server in &servers {
            if !server.roles.iter().any(|r| r == role) {
                continue;
            }
            self.probe_server(&mut catalog, server, tenant_key).await;
        }
        catalog
    }

    /// GET the tenant's MCP servers from the admin. Returns an error string on
    /// any network failure or non-200 status (the caller degrades to empty).
    async fn fetch_servers(&self) -> Result<Vec<ParsedServer>, String> {
        let url = format!("{}/api/v1/designer/tenant/me/mcp-servers", self.base_url);
        let mut req = self.client.get(&url).bearer_auth(&self.token);
        if let Some(identity) = &self.identity {
            req = req
                .header("X-Greentic-Tenant", &identity.tenant_slug)
                .header("X-Greentic-User", &identity.user_email);
            if let Some(team) = &identity.team_slug {
                req = req.header("X-Greentic-Team", team);
            }
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("request failed: {e}"))?;

        let status = resp.status();
        if status.as_u16() != 200 {
            return Err(format!("admin returned status {}", status.as_u16()));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("decode failed: {e}"))?;
        parse_rows(body)
    }

    /// Probe one server and ingest its (filtered) tools into `catalog`. A
    /// timeout, connection error, or bad transport URL is skipped with a
    /// `warn` — the catalog keeps the servers that worked.
    async fn probe_server(
        &self,
        catalog: &mut McpToolCatalog,
        server: &ParsedServer,
        tenant_key: &str,
    ) {
        match tokio::time::timeout(PROBE_TIMEOUT, list_server_tools(server)).await {
            Ok(Ok(defs)) => ingest_server_tools(catalog, server, &defs),
            Ok(Err(e)) => tracing::warn!(
                tenant = %tenant_key,
                server = %server.id,
                error = %e,
                "mcp server probe failed; skipping its tools"
            ),
            Err(_) => tracing::warn!(
                tenant = %tenant_key,
                server = %server.id,
                "mcp server probe timed out after {}s; skipping its tools",
                PROBE_TIMEOUT.as_secs()
            ),
        }
    }
}

/// Parse the `{servers:[...]}` wire body into [`ParsedServer`]s.
pub(crate) fn parse_rows(body: serde_json::Value) -> Result<Vec<ParsedServer>, String> {
    use secrecy::SecretString;

    let wire: WireBody = serde_json::from_value(body).map_err(|e| e.to_string())?;
    Ok(wire
        .servers
        .into_iter()
        .map(|w| ParsedServer {
            id: w.id,
            // `null`/absent (local-wasm) → ""; the local-wasm branch never reads
            // it as a URL, and http rows always carry a real URL.
            transport_url: w.transport_url.unwrap_or_default(),
            auth_header_name: w.auth_header_name,
            auth_token: w.auth_token.map(SecretString::from),
            allowed_tools: w.allowed_tools,
            roles: w.roles,
            transport: w.transport,
            component_ref: w.component_ref,
            component_version: w.component_version,
            component_digest: w.component_digest,
        })
        .collect())
}

/// Apply the server's `allowed_tools` filter and insert each accepted tool's
/// schema + route, keyed `(server_id, raw_tool_name)`.
fn ingest_server_tools(catalog: &mut McpToolCatalog, server: &ParsedServer, defs: &[McpToolDef]) {
    for def in defs {
        if let Some(allow) = &server.allowed_tools
            && !allow.iter().any(|a| a == &def.name)
        {
            continue;
        }
        let key = (server.id.clone(), def.name.clone());
        catalog.tools.insert(
            key.clone(),
            McpToolEntry {
                description: def.description.clone(),
                parameters: def.input_schema.clone(),
            },
        );
        catalog.routes.insert(
            key,
            McpRoute {
                server_id: server.id.clone(),
                transport_url: server.transport_url.clone(),
                auth_header_name: server.auth_header_name.clone(),
                auth_token: server.auth_token.clone(),
                raw_tool_name: def.name.clone(),
                transport: server.transport,
                component_ref: server.component_ref.clone(),
                component_version: server.component_version.clone(),
                component_digest: server.component_digest.clone(),
            },
        );
    }
}

/// Build an [`McpAuth`] from an optional token + optional header name.
fn build_auth(token: Option<&secrecy::SecretString>, header_name: Option<&str>) -> Option<McpAuth> {
    token.map(|t| McpAuth {
        header_name: header_name.map(str::to_string),
        token: t.expose_secret().to_string(),
    })
}

fn client_opts(timeout: std::time::Duration) -> McpClientOptions {
    McpClientOptions {
        timeout,
        client_name: "greentic-aw-runtime".to_string(),
        client_version: env!("CARGO_PKG_VERSION").to_string(),
    }
}

/// Construct a client for a parsed server row. Synchronous: `McpHttpClient::new`
/// does no I/O (the network handshake happens in `initialize`/`list_tools`).
fn connect(server: &ParsedServer) -> Result<McpHttpClient, String> {
    let endpoint = url::Url::parse(&server.transport_url)
        .map_err(|e| format!("invalid transport_url '{}': {e}", server.transport_url))?;
    let auth = build_auth(
        server.auth_token.as_ref(),
        server.auth_header_name.as_deref(),
    );
    McpHttpClient::new(endpoint, auth, client_opts(PROBE_TIMEOUT)).map_err(|e| e.to_string())
}

/// Construct a client from a dispatch route.
fn connect_route(route: &McpRoute) -> Result<McpHttpClient, String> {
    let endpoint = url::Url::parse(&route.transport_url)
        .map_err(|e| format!("invalid transport_url '{}': {e}", route.transport_url))?;
    let auth = build_auth(route.auth_token.as_ref(), route.auth_header_name.as_deref());
    // The CALL budget, not the probe budget: this client carries the tool's own
    // work, and reqwest would otherwise cut it off at the probe's 5s regardless
    // of the outer timeout.
    McpHttpClient::new(endpoint, auth, client_opts(call_timeout())).map_err(|e| e.to_string())
}

/// Connect, handshake, and list a server's tools. Errors are stringified.
///
/// Branches on [`Transport`]: `Http` uses the existing `greentic-mcp-client`
/// path; `LocalWasm` runs the component in-process via [`crate::mcp_local`].
async fn list_server_tools(server: &ParsedServer) -> Result<Vec<McpToolDef>, String> {
    match server.transport {
        Transport::Http => {
            let mut client = connect(server)?;
            client.initialize().await.map_err(|e| e.to_string())?;
            client.list_tools().await.map_err(|e| e.to_string())
        }
        Transport::LocalWasm => {
            let component = server
                .component_ref
                .as_deref()
                .ok_or_else(|| "local-wasm server missing component_ref".to_string())?;
            // Lazy store-pull: ensure the wasm is downloaded and verified before
            // listing tools. Requires both version and digest to be present on
            // the wire row; a missing field is treated as a pull failure so the
            // server is skipped rather than silently running an unverified wasm.
            let version = server.component_version.as_deref().ok_or_else(|| {
                format!(
                    "local-wasm server '{}' missing component_version; cannot pull",
                    component
                )
            })?;
            let digest = server.component_digest.as_deref().ok_or_else(|| {
                format!(
                    "local-wasm server '{}' missing component_digest; cannot pull",
                    component
                )
            })?;
            crate::mcp_store_pull::ensure_cached(component, version, digest)
                .await
                .map_err(|e| format!("local-wasm store-pull failed for '{}': {e}", component))?;
            let tools = crate::mcp_local::local_list_tools(component).await;
            Ok(tools
                .into_iter()
                .map(|tool_def| McpToolDef {
                    name: tool_def.name,
                    // A local component has no title to carry: `ToolDef` in
                    // greentic-mcp-exec is name/description/schemas, and the WIT
                    // behind it declares no display name. So `None` here is the
                    // honest answer — this transport genuinely has nothing to
                    // offer — not a field being dropped on the way through, which
                    // is what it would be if the descriptor did carry one.
                    title: None,
                    description: tool_def.description,
                    input_schema: tool_def.input_schema,
                    // Carried through from the component's own descriptor.
                    // `None` still means "the component declared nothing", which
                    // is what `McpToolDef::output_schema` distinguishes from
                    // `{}` — so a component that declares no schema is reported
                    // exactly as it was before this became readable.
                    output_schema: tool_def.output_schema,
                })
                .collect())
        }
    }
}

/// Invoke an MCP tool through its route. Always returns a JSON [`Value`],
/// never panics — bad arguments, connection failures, and timeouts all become
/// `{"error": "..."}`.
pub async fn dispatch_route(
    route: &McpRoute,
    args: &str,
    scope: &McpCallScope,
) -> serde_json::Value {
    let parsed: serde_json::Value = match serde_json::from_str(args) {
        Ok(v) => v,
        Err(e) => return json!({ "error": format!("invalid tool arguments: {e}") }),
    };

    let budget = call_timeout();
    match tokio::time::timeout(budget, call_route(route, &parsed, scope)).await {
        Ok(Ok(value)) => value,
        Ok(Err(e)) => json!({ "error": e }),
        Err(_) => json!({
            "error": format!("tool call timed out after {}s", budget.as_secs())
        }),
    }
}

/// Invoke an MCP tool through its route. Errors are stringified.
///
/// Branches on [`Transport`]: `Http` uses the existing `greentic-mcp-client`
/// path; `LocalWasm` runs the component in-process via [`crate::mcp_local`].
/// [`dispatch_route`] wraps this with the timeout + `{"error": ...}` contract,
/// which now covers both transports.
async fn call_route(
    route: &McpRoute,
    args: &serde_json::Value,
    scope: &McpCallScope,
) -> Result<serde_json::Value, String> {
    match route.transport {
        Transport::Http => {
            let mut client = connect_route(route)?;
            client.initialize().await.map_err(|e| e.to_string())?;
            let out = client
                .call_tool(&route.raw_tool_name, args)
                .await
                .map_err(|e| e.to_string())?;
            Ok(out.to_value())
        }
        Transport::LocalWasm => {
            let component = route
                .component_ref
                .as_deref()
                .ok_or_else(|| "local-wasm route missing component_ref".to_string())?;
            // Lazy store-pull: guard the dispatch path the same way as the list
            // path. The catalog build would have already pulled for in-session
            // routes, but a route can also be constructed or replayed without a
            // prior catalog probe (e.g. from a persisted session snapshot).
            let version = route.component_version.as_deref().ok_or_else(|| {
                format!(
                    "local-wasm route '{}' missing component_version; cannot pull",
                    component
                )
            })?;
            let digest = route.component_digest.as_deref().ok_or_else(|| {
                format!(
                    "local-wasm route '{}' missing component_digest; cannot pull",
                    component
                )
            })?;
            crate::mcp_store_pull::ensure_cached(component, version, digest)
                .await
                .map_err(|e| {
                    format!(
                        "local-wasm store-pull failed for '{}' during dispatch: {e}",
                        component
                    )
                })?;
            Ok(
                crate::mcp_local::local_call_tool(component, &route.raw_tool_name, args, scope)
                    .await,
            )
        }
    }
}
