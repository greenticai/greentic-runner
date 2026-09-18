//! Pack-backed MCP source: a catalog built from `assets/mcp-routes.json`
//! records rather than an admin fetch + probe.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use greentic_secrets_lib::SecretsManager;
use serde_json::json;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::mcp_source::source::{McpToolSource, dispatch_route};
use crate::mcp_source::types::{McpPackRoute, McpToolCatalog, McpToolEntry, Transport};

use super::catalog::{fake_mcp_server, tenant, two_tools};

/// Secrets manager holding exactly the URIs it was handed, recording reads.
struct MapSecrets {
    entries: HashMap<String, Vec<u8>>,
    seen: Mutex<Vec<String>>,
}

impl MapSecrets {
    fn with(pairs: &[(&str, &str)]) -> Arc<Self> {
        Arc::new(Self {
            entries: pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.as_bytes().to_vec()))
                .collect(),
            seen: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl SecretsManager for MapSecrets {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        self.seen.lock().unwrap().push(path.to_string());
        self.entries
            .get(path)
            .cloned()
            .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
    }
    async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
}

fn http_record(server_id: &str, url: &str, auth_team: Option<&str>) -> McpPackRoute {
    McpPackRoute {
        server_id: server_id.to_string(),
        transport: "http".to_string(),
        transport_url: Some(url.to_string()),
        auth_header_name: None,
        auth_team: auth_team.map(str::to_string),
        ..McpPackRoute::default()
    }
}

#[tokio::test]
async fn pack_catalog_dispatches_with_the_token_read_from_secrets() {
    // The whole point of the slice: no admin, no probe — a sidecar record plus
    // a broker-resolved token is enough to call a real MCP server.
    let mcp = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("authorization", "Bearer pack-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {
                "protocolVersion": "2025-06-18",
                "serverInfo": { "name": "fake", "version": "1.0.0" },
                "structuredContent": { "ok": true }
            }
        })))
        .mount(&mcp)
        .await;

    let secrets = MapSecrets::with(&[("secrets://default/acme/_/mcp/srv-1", "pack-token")]);
    let source = McpToolSource::from_pack_routes(
        vec![http_record("srv-1", &mcp.uri(), None)],
        Some(secrets),
    );

    let catalog = source.catalog(&tenant()).await;
    // The tool name is never discovered from the server — it arrives from the
    // agent's own `ToolRef` and is stamped onto the server-level route here.
    let route = catalog
        .resolve_route("srv-1", "create_quote")
        .expect("a pack record yields a route for any tool the agent names");

    let scope = crate::mcp_scope::McpCallScope::new(tenant());
    let out = dispatch_route(&route, "{}", &scope).await;
    assert!(
        !out.to_string().contains("error"),
        "the bearer must reach the server; got: {out}"
    );
}

#[tokio::test]
async fn pack_catalog_prefers_the_records_auth_team_scope() {
    let mcp = fake_mcp_server(two_tools(), json!({})).await;
    let secrets = MapSecrets::with(&[
        ("secrets://default/acme/sales/mcp/srv-1", "team-token"),
        ("secrets://default/acme/_/mcp/srv-1", "tenant-token"),
    ]);
    let source = McpToolSource::from_pack_routes(
        vec![http_record("srv-1", &mcp.uri(), Some("sales"))],
        Some(secrets.clone()),
    );

    let catalog = source.catalog(&tenant()).await;
    assert!(catalog.resolve_route("srv-1", "get_issue").is_some());
    assert_eq!(
        secrets.seen.lock().unwrap().as_slice(),
        &["secrets://default/acme/sales/mcp/srv-1".to_string()],
        "the tenant-default scope must not be read once the team scope hits"
    );
}

#[tokio::test]
async fn pack_catalog_falls_back_to_the_tenant_default_scope() {
    // Every deployment that resolves today sits at `_`. Naming a team in the
    // sidecar must never stop such a token resolving.
    let mcp = fake_mcp_server(two_tools(), json!({})).await;
    let secrets = MapSecrets::with(&[("secrets://default/acme/_/mcp/srv-1", "tenant-token")]);
    let source = McpToolSource::from_pack_routes(
        vec![http_record("srv-1", &mcp.uri(), Some("sales"))],
        Some(secrets.clone()),
    );

    let catalog = source.catalog(&tenant()).await;
    assert!(catalog.resolve_route("srv-1", "get_issue").is_some());
    assert_eq!(secrets.seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_server_with_no_resolvable_credential_is_routeless_and_says_why() {
    let secrets = MapSecrets::with(&[]);
    let source = McpToolSource::from_pack_routes(
        vec![http_record(
            "srv-1",
            "https://mcp.example.com/",
            Some("sales"),
        )],
        Some(secrets),
    );

    let catalog = source.catalog(&tenant()).await;
    assert!(
        catalog.resolve_route("srv-1", "get_issue").is_none(),
        "dispatching with no credential would fail opaquely at the server"
    );
    let detail = catalog.server_error("srv-1").expect("a diagnostic");
    assert!(
        detail.contains("secrets://default/acme/sales/mcp/srv-1")
            && detail.contains("secrets://default/acme/_/mcp/srv-1"),
        "got: {detail}"
    );
    assert!(detail.contains("SECRETS_BACKEND=broker"), "got: {detail}");
}

#[tokio::test]
async fn a_local_wasm_record_needs_no_http_credential() {
    // Admin writes no `mcp/<server_id>` entry for a local-wasm server, so
    // reading one would fail every such route on a secret that is not supposed
    // to exist.
    let secrets = MapSecrets::with(&[]);
    let source = McpToolSource::from_pack_routes(
        vec![McpPackRoute {
            server_id: "srv-wasm".to_string(),
            transport: "local-wasm".to_string(),
            component_ref: Some("weather.component".to_string()),
            ..McpPackRoute::default()
        }],
        Some(secrets.clone()),
    );

    let catalog = source.catalog(&tenant()).await;
    let route = catalog
        .resolve_route("srv-wasm", "forecast")
        .expect("route");
    assert!(matches!(route.transport, Transport::LocalWasm));
    assert!(
        secrets.seen.lock().unwrap().is_empty(),
        "no HTTP credential read for a local-wasm route"
    );
}

#[tokio::test]
async fn the_pack_catalog_advertises_no_tools_of_its_own() {
    // It performs no probe, so it knows no tool names. The LLM tool list comes
    // from the agent's `ToolRef` schemas (see `tools::list_tools_for_llm`).
    let mcp = fake_mcp_server(two_tools(), json!({})).await;
    let secrets = MapSecrets::with(&[("secrets://default/acme/_/mcp/srv-1", "tok")]);
    let source = McpToolSource::from_pack_routes(
        vec![http_record("srv-1", &mcp.uri(), None)],
        Some(secrets),
    );

    let catalog = source.catalog(&tenant()).await;
    assert!(catalog.is_empty());
    assert!(catalog.tool_entry("srv-1", "get_issue").is_none());
}

#[test]
fn an_exact_catalog_route_wins_over_a_server_level_fallback() {
    // REGRESSION GUARD: an admin-built catalog registers exact
    // `(server, tool)` routes and no server-level ones, so `resolve_route`
    // must keep returning the exact entry — including its `allowed_tools`-
    // filtered set — rather than a broader fallback.
    let exact = crate::mcp_source::route_for_tests("srv-1", "get_issue", "https://exact.example/");
    let fallback = crate::mcp_source::types::McpRoute::from_parts(
        "srv-1",
        "https://fallback.example/",
        None,
        None,
        "http",
        None,
        None,
        None,
    );

    let mut routes = HashMap::new();
    routes.insert(("srv-1".to_string(), "get_issue".to_string()), exact);
    let mut server_routes = HashMap::new();
    server_routes.insert("srv-1".to_string(), fallback);

    let catalog = McpToolCatalog::from_parts(HashMap::new(), routes, None)
        .with_server_routes(server_routes, HashMap::new());

    let resolved = catalog.resolve_route("srv-1", "get_issue").unwrap();
    assert!(
        format!("{resolved:?}").contains("exact.example"),
        "the exact catalog route must win; got: {resolved:?}"
    );
    // A tool the exact map does not name still reaches the fallback.
    let other = catalog.resolve_route("srv-1", "search_code").unwrap();
    assert!(format!("{other:?}").contains("fallback.example"));
}

#[test]
fn an_admin_built_catalog_records_no_server_diagnostics() {
    // `server_error` must stay `None` on the admin path so the dispatch error
    // message there is unchanged.
    let mut tools = HashMap::new();
    tools.insert(
        ("srv-1".to_string(), "get_issue".to_string()),
        McpToolEntry {
            description: "Get an issue".to_string(),
            parameters: json!({}),
        },
    );
    let catalog = McpToolCatalog::from_parts(tools, HashMap::new(), None);
    assert!(catalog.server_error("srv-1").is_none());
    assert!(catalog.resolve_route("srv-1", "get_issue").is_none());
}
