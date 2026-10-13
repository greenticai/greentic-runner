//! `oauth_client_credentials` MCP routes (pack-carried and admin-listed).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use greentic_secrets_lib::SecretsManager;
use serde_json::json;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::mcp_source::source::{McpToolSource, dispatch_route};
use crate::mcp_source::types::McpPackRoute;
use crate::oauth::TokenProvider;

use super::catalog::{initialize_ok, tenant};

const CREDS: &str = r#"{"client_id":"cid","client_secret":"csecret-XYZ"}"#;
const URI: &str = "secrets://default/acme/_/mcp/srv";

struct Secrets(HashMap<String, Vec<u8>>);

#[async_trait]
impl SecretsManager for Secrets {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        self.0
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

fn secrets(value: &str) -> Arc<dyn SecretsManager> {
    Arc::new(Secrets(HashMap::from([(
        URI.to_string(),
        value.as_bytes().to_vec(),
    )])))
}

fn provider() -> Arc<TokenProvider> {
    let p = Arc::new(TokenProvider::new());
    p.allow_loopback_http_for_tests();
    p
}

/// An MCP server that accepts only `Bearer <good>`; everything else is 401.
async fn mcp_accepting(good: &str) -> MockServer {
    let server = MockServer::start().await;
    let bearer = format!("Bearer {good}");
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(initialize_ok())
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(
            json!({"method": "notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "tools/call"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 3,
            "result": {"content": [{"type": "text", "text": "done"}]}
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401))
        .with_priority(9)
        .mount(&server)
        .await;
    server
}

async fn idp_sequence(tokens: &[&str]) -> MockServer {
    let idp = MockServer::start().await;
    for (i, token) in tokens.iter().enumerate() {
        let last = i + 1 == tokens.len();
        let mock = Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"access_token": token, "token_type": "Bearer", "expires_in": 3600}),
            ))
            .with_priority(1 + i as u8);
        if last {
            mock.mount(&idp).await;
        } else {
            mock.up_to_n_times(1).mount(&idp).await;
        }
    }
    idp
}

fn record(mcp: &MockServer, idp: &MockServer) -> McpPackRoute {
    McpPackRoute {
        server_id: "srv".into(),
        transport: "http".into(),
        transport_url: Some(mcp.uri()),
        auth_mode: Some("oauth_client_credentials".into()),
        oauth_endpoint: Some(format!("{}/token", idp.uri())),
        ..McpPackRoute::default()
    }
}

fn scope() -> crate::mcp_scope::McpCallScope {
    crate::mcp_scope::McpCallScope::new(crate::tenant::TenantContext::new("acme", "prod"))
}

async fn call(source: &McpToolSource) -> serde_json::Value {
    let catalog = source.catalog(&tenant()).await;
    let route = catalog.resolve_route("srv", "echo").expect("route");
    dispatch_route(&route, "{}", &scope()).await
}

#[tokio::test]
async fn an_oauth_pack_route_calls_with_the_fetched_token() {
    let mcp = mcp_accepting("access-1").await;
    let idp = idp_sequence(&["access-1"]).await;
    let source = McpToolSource::from_pack_routes(vec![record(&mcp, &idp)], Some(secrets(CREDS)))
        .with_token_provider(provider());
    let out = call(&source).await;
    assert!(out.get("error").is_none(), "{out}");
    for req in mcp.received_requests().await.unwrap() {
        assert!(!String::from_utf8_lossy(&req.body).contains("csecret-XYZ"));
    }
}

#[tokio::test]
async fn a_refused_token_is_replaced_and_the_call_retried_once() {
    let mcp = mcp_accepting("access-2").await;
    let idp = idp_sequence(&["access-1", "access-2"]).await;
    let source = McpToolSource::from_pack_routes(vec![record(&mcp, &idp)], Some(secrets(CREDS)))
        .with_token_provider(provider());
    let out = call(&source).await;
    assert!(out.get("error").is_none(), "{out}");
    assert_eq!(
        idp.received_requests().await.unwrap().len(),
        2,
        "one refused token, one fresh"
    );
}

#[tokio::test]
async fn a_token_the_server_always_refuses_fails_after_one_retry() {
    let mcp = mcp_accepting("never-issued").await;
    let idp = idp_sequence(&["access-1"]).await;
    let source = McpToolSource::from_pack_routes(vec![record(&mcp, &idp)], Some(secrets(CREDS)))
        .with_token_provider(provider());
    let out = call(&source).await;
    assert!(out.get("error").is_some(), "{out}");
    assert_eq!(idp.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn an_unknown_mode_fails_only_that_server_and_names_it() {
    let mcp = mcp_accepting("x").await;
    let idp = idp_sequence(&["x"]).await;
    let mut bad = record(&mcp, &idp);
    bad.server_id = "bad".into();
    bad.auth_mode = Some("mutual_tls".into());
    let source =
        McpToolSource::from_pack_routes(vec![bad, record(&mcp, &idp)], Some(secrets(CREDS)))
            .with_token_provider(provider());
    let catalog = source.catalog(&tenant()).await;
    assert!(
        catalog.resolve_route("srv", "echo").is_some(),
        "the good one is kept"
    );
    assert!(catalog.resolve_route("bad", "echo").is_none());
    let why = catalog.server_error("bad").expect("diagnostic");
    assert!(why.contains("mutual_tls"), "{why}");
    assert!(idp.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn client_credentials_on_a_bearer_route_are_never_sent_as_a_token() {
    let mcp = mcp_accepting("x").await;
    let idp = idp_sequence(&["x"]).await;
    let mut r = record(&mcp, &idp);
    r.auth_mode = None;
    let source = McpToolSource::from_pack_routes(vec![r], Some(secrets(CREDS)))
        .with_token_provider(provider());
    let catalog = source.catalog(&tenant()).await;
    assert!(catalog.resolve_route("srv", "echo").is_none());
    let why = catalog.server_error("srv").expect("diagnostic");
    assert!(
        why.contains("client credentials") && !why.contains("csecret-XYZ"),
        "{why}"
    );
    assert!(mcp.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn an_oauth_route_without_credentials_or_endpoint_is_refused() {
    let mcp = mcp_accepting("x").await;
    let idp = idp_sequence(&["x"]).await;
    let mut no_endpoint = record(&mcp, &idp);
    no_endpoint.oauth_endpoint = None;
    for (r, s) in [(no_endpoint, CREDS), (record(&mcp, &idp), "plain-token")] {
        let source = McpToolSource::from_pack_routes(vec![r], Some(secrets(s)))
            .with_token_provider(provider());
        let catalog = source.catalog(&tenant()).await;
        assert!(catalog.resolve_route("srv", "echo").is_none());
        assert!(catalog.server_error("srv").is_some());
    }
}

#[tokio::test]
async fn an_admin_row_with_oauth_mode_probes_with_the_fetched_token() {
    let mcp = MockServer::start().await;
    let bearer = "Bearer access-1";
    for (m, resp) in [
        ("initialize", initialize_ok()),
        ("notifications/initialized", ResponseTemplate::new(202)),
        (
            "tools/list",
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": 2,
                "result": {"tools": [{"name": "echo", "description": "d",
                                       "inputSchema": {"type": "object"}}]}
            })),
        ),
    ] {
        Mock::given(method("POST"))
            .and(header("authorization", bearer))
            .and(body_partial_json(json!({"method": m})))
            .respond_with(resp)
            .mount(&mcp)
            .await;
    }
    let idp = idp_sequence(&["access-1"]).await;
    let admin = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/designer/tenant/me/mcp-servers"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"servers": [{
                "id": "s1", "name": "n", "transport_url": mcp.uri(),
                "auth_header_name": null, "auth_token": CREDS, "allowed_tools": null,
                "roles": ["agentic_worker"], "auth_mode": "oauth_client_credentials",
                "oauth_endpoint": format!("{}/token", idp.uri())
            }, {
                "id": "s2", "name": "n", "transport_url": mcp.uri(),
                "auth_header_name": null, "auth_token": "t", "allowed_tools": null,
                "roles": ["agentic_worker"], "auth_mode": "mystery"
            }]})),
        )
        .mount(&admin)
        .await;
    let source = McpToolSource::new(admin.uri(), "gtc_live_x").with_token_provider(provider());
    let catalog = source.catalog(&tenant()).await;
    assert!(catalog.tool_entry("s1", "echo").is_some());
    assert!(
        catalog.tool_entry("s2", "echo").is_none(),
        "unknown mode is skipped"
    );
}
