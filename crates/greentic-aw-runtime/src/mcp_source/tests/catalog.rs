//! Catalog-building, role filtering, TTL cache, and allowed-tools tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::mcp_source::source::{McpToolSource, dispatch_route};
use crate::mcp_source::types::{MCP_ROLE_AGENTIC_WORKER, MCP_ROLE_FLOW_EDITOR, McpCallerIdentity};
use crate::tenant::TenantContext;

// --- Shared helpers ---

pub(super) fn initialize_ok() -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("Mcp-Session-Id", "sess-1")
        .set_body_json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {
                "protocolVersion": "2025-06-18",
                "serverInfo": { "name": "fake", "version": "1.0.0" }
            }
        }))
}

/// Mount the 4-call MCP JSON-RPC contract on a fresh wiremock server.
pub(super) async fn fake_mcp_server(
    tools_json: serde_json::Value,
    call_result_json: serde_json::Value,
) -> MockServer {
    use wiremock::matchers::body_partial_json;

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "initialize" })))
        .respond_with(initialize_ok())
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({ "method": "notifications/initialized" }),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "tools/list" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 2,
            "result": { "tools": tools_json }
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "tools/call" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 3,
            "result": call_result_json
        })))
        .mount(&server)
        .await;
    server
}

pub(super) fn two_tools() -> serde_json::Value {
    json!([
        { "name": "get_issue", "description": "Get an issue",
          "inputSchema": { "type": "object", "properties": { "id": { "type": "string" } } } },
        { "name": "search_code", "description": "Search code",
          "inputSchema": { "type": "object" } }
    ])
}

/// Build a `{servers:[...]}` admin body for one agentic-worker server.
pub(super) fn admin_body_agentic(
    id: &str,
    url: &str,
    allowed: Option<Vec<&str>>,
    roles: Vec<&str>,
) -> serde_json::Value {
    json!({
        "servers": [
            {
                "id": id,
                "name": "Server",
                "transport_url": url,
                "auth_header_name": null,
                "auth_token": null,
                "allowed_tools": allowed.map(|v| v.into_iter().collect::<Vec<_>>()),
                "roles": roles,
            }
        ]
    })
}

/// Mount the admin `mcp-servers` endpoint returning `body`.
pub(super) async fn mount_admin(server: &MockServer, body: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .and(header("authorization", "Bearer gtc_live_x"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

pub(super) fn tenant() -> TenantContext {
    TenantContext::new("acme", "prod")
}

// --- Tests ---

#[tokio::test]
async fn source_fetches_and_filters_agentic_worker() {
    // One agentic_worker server (→ fake MCP) and one flow_editor server.
    let mcp = fake_mcp_server(two_tools(), json!({})).await;
    let admin = MockServer::start().await;
    let body = json!({
        "servers": [
            {
                "id": "worker", "name": "Worker", "transport_url": mcp.uri(),
                "auth_header_name": null, "auth_token": null,
                "allowed_tools": null, "roles": ["agentic_worker"]
            },
            {
                "id": "editor", "name": "Editor", "transport_url": "http://127.0.0.1:1/",
                "auth_header_name": null, "auth_token": null,
                "allowed_tools": null, "roles": ["flow_editor"]
            }
        ]
    });
    mount_admin(&admin, body).await;

    let source = McpToolSource::new(admin.uri(), "gtc_live_x");
    let catalog = source.catalog(&tenant()).await;

    // Only the agentic_worker server's two tools land.
    assert_eq!(catalog.len(), 2);
    assert!(catalog.tool_entry("worker", "get_issue").is_some());
    assert!(catalog.tool_entry("worker", "search_code").is_some());
    // The flow_editor server is never probed/ingested.
    assert!(catalog.tool_entry("editor", "get_issue").is_none());
}

#[tokio::test]
async fn catalog_for_role_filters_flow_editor() {
    // One flow_editor server (→ fake MCP) and one agentic_worker server.
    let mcp = fake_mcp_server(two_tools(), json!({})).await;
    let admin = MockServer::start().await;
    let body = json!({
        "servers": [
            {
                "id": "editor", "name": "Editor", "transport_url": mcp.uri(),
                "auth_header_name": null, "auth_token": null,
                "allowed_tools": null, "roles": ["flow_editor"]
            },
            {
                "id": "worker", "name": "Worker", "transport_url": "http://127.0.0.1:1/",
                "auth_header_name": null, "auth_token": null,
                "allowed_tools": null, "roles": ["agentic_worker"]
            }
        ]
    });
    mount_admin(&admin, body).await;

    let source = McpToolSource::new(admin.uri(), "gtc_live_x");
    let catalog = source
        .catalog_for_role(&tenant(), MCP_ROLE_FLOW_EDITOR)
        .await;

    // Only the flow_editor server's two tools land.
    assert_eq!(catalog.len(), 2);
    assert!(catalog.tool_entry("editor", "get_issue").is_some());
    assert!(catalog.tool_entry("editor", "search_code").is_some());
    // The agentic_worker server is never probed/ingested for this role.
    assert!(catalog.tool_entry("worker", "get_issue").is_none());
}

#[tokio::test]
async fn role_catalogs_are_cached_independently() {
    // Same admin exposes one server per role at distinct fake MCP servers;
    // the per-role cache keys must not collide.
    let editor_mcp = fake_mcp_server(two_tools(), json!({})).await;
    let admin = MockServer::start().await;
    let body = json!({
        "servers": [
            {
                "id": "editor", "name": "Editor", "transport_url": editor_mcp.uri(),
                "auth_header_name": null, "auth_token": null,
                "allowed_tools": null, "roles": ["flow_editor"]
            },
            {
                "id": "worker", "name": "Worker", "transport_url": "http://127.0.0.1:1/",
                "auth_header_name": null, "auth_token": null,
                "allowed_tools": null, "roles": ["agentic_worker"]
            }
        ]
    });
    mount_admin(&admin, body).await;

    let source = McpToolSource::new(admin.uri(), "gtc_live_x");
    let t = tenant();
    let editor = source.catalog_for_role(&t, MCP_ROLE_FLOW_EDITOR).await;
    let worker = source.catalog_for_role(&t, MCP_ROLE_AGENTIC_WORKER).await;
    // Distinct cache entries: flow-editor has tools, agentic-worker's dead
    // server yields none — proving the keys did not alias.
    assert_eq!(editor.len(), 2);
    assert_eq!(worker.len(), 0);
    assert!(!Arc::ptr_eq(&editor, &worker));
}

#[tokio::test]
async fn catalog_applies_allowed_tools() {
    let mcp = fake_mcp_server(two_tools(), json!({})).await;
    let admin = MockServer::start().await;
    mount_admin(
        &admin,
        admin_body_agentic(
            "s1",
            &mcp.uri(),
            Some(vec!["get_issue"]),
            vec!["agentic_worker"],
        ),
    )
    .await;

    let source = McpToolSource::new(admin.uri(), "gtc_live_x");
    let catalog = source.catalog(&tenant()).await;

    assert_eq!(catalog.len(), 1);
    assert!(catalog.tool_entry("s1", "get_issue").is_some());
    assert!(catalog.tool_entry("s1", "search_code").is_none());
}

#[tokio::test]
async fn unreachable_server_degrades() {
    let admin = MockServer::start().await;
    mount_admin(
        &admin,
        admin_body_agentic("s1", "http://127.0.0.1:1/", None, vec!["agentic_worker"]),
    )
    .await;

    let source = McpToolSource::new(admin.uri(), "gtc_live_x");
    // Must not panic and must return a catalog (with no tools for the
    // dead server).
    let catalog = source.catalog(&tenant()).await;
    assert_eq!(catalog.len(), 0);
    assert!(catalog.is_empty());
}

#[tokio::test]
async fn admin_unreachable_degrades_to_empty() {
    let admin = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&admin)
        .await;

    let source = McpToolSource::new(admin.uri(), "gtc_live_x");
    let catalog = source.catalog(&tenant()).await;
    assert!(catalog.is_empty());
}

#[tokio::test]
async fn ttl_cache_reuses_within_window() {
    let mcp = fake_mcp_server(two_tools(), json!({})).await;
    let admin = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .and(header("authorization", "Bearer gtc_live_x"))
        .respond_with(ResponseTemplate::new(200).set_body_json(admin_body_agentic(
            "s1",
            &mcp.uri(),
            None,
            vec!["agentic_worker"],
        )))
        .expect(1) // second call must hit the TTL cache
        .mount(&admin)
        .await;

    let source = McpToolSource::new(admin.uri(), "gtc_live_x");
    let t = tenant();
    let first = source.catalog(&t).await;
    let second = source.catalog(&t).await;
    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn dispatch_route_calls_server_and_wraps() {
    // Success: fake returns structuredContent.
    let mcp = fake_mcp_server(two_tools(), json!({ "structuredContent": { "ok": 1 } })).await;
    let admin = MockServer::start().await;
    mount_admin(
        &admin,
        admin_body_agentic(
            "s1",
            &mcp.uri(),
            Some(vec!["get_issue"]),
            vec!["agentic_worker"],
        ),
    )
    .await;

    let source = McpToolSource::new(admin.uri(), "gtc_live_x");
    let catalog = source.catalog(&tenant()).await;
    let route = catalog.route("s1", "get_issue").expect("route present");

    let scope = crate::mcp_scope::McpCallScope::new(TenantContext::new("acme", "prod"));
    let out = dispatch_route(route, "{}", &scope).await;
    // `ToolOutput::to_value` unwraps `structuredContent`, so the value is
    // the server's structured payload itself.
    assert_eq!(out, json!({ "ok": 1 }), "got: {out}");
    assert!(!out.to_string().contains("\"error\""), "got: {out}");

    // isError: fake returns an error envelope → Value contains "error".
    let mcp_err = fake_mcp_server(
        two_tools(),
        json!({ "isError": true, "content": [{ "type": "text", "text": "boom" }] }),
    )
    .await;
    let admin_err = MockServer::start().await;
    mount_admin(
        &admin_err,
        admin_body_agentic(
            "s1",
            &mcp_err.uri(),
            Some(vec!["get_issue"]),
            vec!["agentic_worker"],
        ),
    )
    .await;
    let source_err = McpToolSource::new(admin_err.uri(), "gtc_live_x");
    let catalog_err = source_err.catalog(&TenantContext::new("acme", "stg")).await;
    let route_err = catalog_err.route("s1", "get_issue").expect("route present");
    let scope_err = crate::mcp_scope::McpCallScope::new(TenantContext::new("acme", "prod"));
    let out_err = dispatch_route(route_err, "{}", &scope_err).await;
    assert!(out_err.to_string().contains("error"), "got: {out_err}");
}

#[test]
fn source_carries_secrets_into_its_catalog() {
    use crate::mcp_source::source::McpToolSource;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct FakeSecrets;
    #[async_trait]
    impl greentic_secrets_lib::SecretsManager for FakeSecrets {
        async fn read(&self, _: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            Ok(b"v".to_vec())
        }
        async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    let source = McpToolSource::with_secrets("http://admin.test", "tok", Arc::new(FakeSecrets));
    assert!(
        source.secrets().is_some(),
        "a source built with secrets must expose them to the catalogs it builds"
    );
}

/// A fake [`greentic_secrets_lib::SecretsManager`] used to prove the manager
/// itself (not just `Option::is_some`) rides from the source into a
/// real, network-built catalog.
struct FakeSecrets;

#[async_trait::async_trait]
impl greentic_secrets_lib::SecretsManager for FakeSecrets {
    async fn read(&self, _: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        Ok(b"v".to_vec())
    }
    async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
}

/// `build_catalog`'s normal-build branch (admin responds 200 with zero
/// servers) must still copy the source's secrets manager onto the catalog it
/// returns. This drives a REAL catalog through `catalog_for_role` against a
/// mock admin — unlike `source_carries_secrets_into_its_catalog`, which only
/// checks the source itself and never builds a catalog, so it stays green
/// even if the `catalog.secrets = self.secrets();` copy in `build_catalog` is
/// deleted.
#[tokio::test]
async fn build_catalog_normal_branch_carries_secrets() {
    use std::sync::Arc;

    let admin = MockServer::start().await;
    mount_admin(&admin, json!({ "servers": [] })).await;

    let source = McpToolSource::with_secrets(admin.uri(), "gtc_live_x", Arc::new(FakeSecrets));
    let catalog = source
        .catalog_for_role(&tenant(), MCP_ROLE_AGENTIC_WORKER)
        .await;

    assert!(catalog.is_empty());
    assert!(
        catalog.secrets().is_some(),
        "build_catalog's normal-build branch must copy the source's secrets \
         manager onto the returned catalog"
    );
}

/// `build_catalog`'s early-return "fetch failed" branch (admin responds
/// non-200) must ALSO copy the source's secrets manager onto the empty
/// catalog it returns.
#[tokio::test]
async fn build_catalog_fetch_failed_branch_carries_secrets() {
    use std::sync::Arc;

    let admin = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&admin)
        .await;

    let source = McpToolSource::with_secrets(admin.uri(), "gtc_live_x", Arc::new(FakeSecrets));
    let catalog = source
        .catalog_for_role(&tenant(), MCP_ROLE_AGENTIC_WORKER)
        .await;

    assert!(catalog.is_empty());
    assert!(
        catalog.secrets().is_some(),
        "build_catalog's fetch-failed branch must copy the source's secrets \
         manager onto the empty catalog it returns"
    );
}

/// A source built with an identity must send the `X-Greentic-Tenant` /
/// `X-Greentic-User` headers the admin resolves RBAC from.
///
/// Without them the bearer alone has to imply the tenant, which forces a
/// tenant-scoped `gtc_live_*` token per tenant — unusable from an embedding
/// host that serves many tenants from ONE process (the designer, whose Run
/// Demo builds a host per tenant but reads a single process-global env). With
/// them the same `gts_` service key the designer already holds works, and the
/// tenant travels per request instead of per process.
///
/// The assertion is behavioural, not a spy: the mock only answers when both
/// headers match, so a source that fails to send them gets no response, the
/// fetch fails, and `build_catalog` degrades to an EMPTY catalog.
#[tokio::test]
async fn identity_headers_are_sent_so_the_admin_can_resolve_the_tenant() {
    let mcp = fake_mcp_server(two_tools(), json!({})).await;
    let admin = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .and(header("authorization", "Bearer gts_x"))
        .and(header("x-greentic-tenant", "acme"))
        .and(header("x-greentic-user", "ops@acme.test"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "servers": [{
                "id": "worker", "name": "Worker", "transport_url": mcp.uri(),
                "auth_header_name": null, "auth_token": null,
                "allowed_tools": null, "roles": ["agentic_worker"]
            }]
        })))
        .mount(&admin)
        .await;

    let source = McpToolSource::new(admin.uri(), "gts_x")
        .with_identity(McpCallerIdentity::new("acme", "ops@acme.test"));
    let catalog = source.catalog(&tenant()).await;

    assert_eq!(
        catalog.len(),
        2,
        "the admin only answers when both identity headers are present; an \
         empty catalog means the source authenticated without saying which \
         tenant it was asking for"
    );
}

/// MCP servers are stored per-team on the admin, so an identity carrying a
/// team must say so — otherwise an embedding host resolves a DIFFERENT server
/// set at run time than the one its authoring UI listed, and the mismatch is
/// silent (a tool simply reports "not found in the catalog").
#[tokio::test]
async fn team_header_is_sent_when_the_identity_carries_a_team() {
    let mcp = fake_mcp_server(two_tools(), json!({})).await;
    let admin = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .and(header("x-greentic-team", "support"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "servers": [{
                "id": "worker", "name": "Worker", "transport_url": mcp.uri(),
                "auth_header_name": null, "auth_token": null,
                "allowed_tools": null, "roles": ["agentic_worker"]
            }]
        })))
        .mount(&admin)
        .await;

    let source = McpToolSource::new(admin.uri(), "gts_x")
        .with_identity(McpCallerIdentity::new("acme", "ops@acme.test").with_team("support"));
    let catalog = source.catalog(&tenant()).await;

    assert_eq!(
        catalog.len(),
        2,
        "the admin only answers when the team header names the caller's team"
    );
}

/// An identity with no team must send NO team header at all, rather than an
/// empty one. The designer leaves `team_slug` unset for operator sessions,
/// which span tenants and belong to no team; an empty `X-Greentic-Team` is a
/// different assertion from an absent one and the admin is entitled to reject
/// it.
#[tokio::test]
async fn a_team_less_identity_sends_no_team_header() {
    let mcp = fake_mcp_server(two_tools(), json!({})).await;
    let admin = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .and(|req: &wiremock::Request| req.headers.get("x-greentic-team").is_none())
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "servers": [{
                "id": "worker", "name": "Worker", "transport_url": mcp.uri(),
                "auth_header_name": null, "auth_token": null,
                "allowed_tools": null, "roles": ["agentic_worker"]
            }]
        })))
        .mount(&admin)
        .await;

    let source = McpToolSource::new(admin.uri(), "gts_x")
        .with_identity(McpCallerIdentity::new("acme", "ops@acme.test"));
    let catalog = source.catalog(&tenant()).await;

    assert_eq!(
        catalog.len(),
        2,
        "a team-less identity must send no team header; an empty one is a \
         different claim and the admin may reject it"
    );
}
