//! OAuth `client_credentials` on pack-carried MCP routes.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use greentic_secrets_lib::SecretsManager;
use serde_json::json;
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::mcp_scope::McpCallScope;
use crate::mcp_source::source::{McpToolSource, dispatch_route};
use crate::mcp_source::types::McpPackRoute;
use crate::oauth::TokenProvider;

use super::catalog::{initialize_ok, tenant};

const SECRET_URI: &str = "secrets://default/acme/_/mcp/srv-1";
const CREDS: &str = r#"{"client_id":"cid","client_secret":"csecret-XYZ"}"#;

struct MapSecrets(HashMap<String, Vec<u8>>);

#[async_trait]
impl SecretsManager for MapSecrets {
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
    Arc::new(MapSecrets(HashMap::from([(
        SECRET_URI.to_string(),
        value.as_bytes().to_vec(),
    )])))
}

fn provider() -> Arc<TokenProvider> {
    let provider = Arc::new(TokenProvider::new());
    provider.allow_loopback_http_for_tests();
    provider
}

/// A token endpoint handing out `tokens` in order, one per request.
async fn token_server(tokens: &[&str]) -> MockServer {
    let server = MockServer::start().await;
    for token in tokens {
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"access_token": token, "token_type": "Bearer", "expires_in": 3600}),
            ))
            .up_to_n_times(1)
            .mount(&server)
            .await;
    }
    server
}

/// An MCP server that answers only requests bearing `accepted`, 401 otherwise.
async fn mcp_server(accepted: &str) -> MockServer {
    let server = MockServer::start().await;
    let bearer = format!("Bearer {accepted}");
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
        .with_priority(10)
        .mount(&server)
        .await;
    server
}

fn record(mcp: &MockServer, tokens: &MockServer, mode: Option<&str>) -> McpPackRoute {
    McpPackRoute {
        server_id: "srv-1".to_string(),
        transport: "http".to_string(),
        transport_url: Some(mcp.uri()),
        auth_mode: mode.map(str::to_string),
        oauth_endpoint: Some(format!("{}/token", tokens.uri())),
        oauth_scope: Some("mcp.read".to_string()),
        ..McpPackRoute::default()
    }
}

async fn call(source: &McpToolSource) -> serde_json::Value {
    let catalog = source.catalog(&tenant()).await;
    let Some(route) = catalog.resolve_route("srv-1", "do_it") else {
        let reason = catalog
            .server_error("srv-1")
            .unwrap_or("no route")
            .to_string();
        return json!({"error": reason});
    };
    let scope = McpCallScope::new(crate::tenant::TenantContext::new("acme", "prod"));
    dispatch_route(&route, "{}", &scope).await
}

#[tokio::test]
async fn an_oauth_route_sends_the_fetched_token_as_a_bearer() {
    let tokens = token_server(&["tok-1"]).await;
    let mcp = mcp_server("tok-1").await;
    let source = McpToolSource::from_pack_routes(
        vec![record(&mcp, &tokens, Some("oauth_client_credentials"))],
        Some(secrets(CREDS)),
    )
    .with_token_provider(provider());
    let out = call(&source).await;
    assert!(out.get("error").is_none(), "{out}");
    assert_eq!(tokens.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_refused_token_is_dropped_and_the_call_retried_once() {
    // The first token is stale (the server only accepts tok-2).
    let tokens = token_server(&["tok-1", "tok-2"]).await;
    let mcp = mcp_server("tok-2").await;
    let source = McpToolSource::from_pack_routes(
        vec![record(&mcp, &tokens, Some("oauth_client_credentials"))],
        Some(secrets(CREDS)),
    )
    .with_token_provider(provider());
    let out = call(&source).await;
    assert!(out.get("error").is_none(), "{out}");
    assert_eq!(tokens.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_second_refusal_is_reported_not_looped() {
    let tokens = token_server(&["tok-1", "tok-2", "tok-3"]).await;
    let mcp = mcp_server("never").await;
    let source = McpToolSource::from_pack_routes(
        vec![record(&mcp, &tokens, Some("oauth_client_credentials"))],
        Some(secrets(CREDS)),
    )
    .with_token_provider(provider());
    let out = call(&source).await;
    assert!(out.get("error").is_some(), "{out}");
    assert_eq!(tokens.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_json_secret_on_a_bearer_route_fails_without_any_request() {
    let tokens = token_server(&["tok-1"]).await;
    let mcp = mcp_server("tok-1").await;
    for mode in [None, Some("bearer"), Some("header")] {
        let source = McpToolSource::from_pack_routes(
            vec![record(&mcp, &tokens, mode)],
            Some(secrets(CREDS)),
        )
        .with_token_provider(provider());
        let out = call(&source).await.to_string();
        assert!(out.contains("client credentials"), "{mode:?}: {out}");
        assert!(!out.contains("csecret-XYZ"));
    }
    assert!(mcp.received_requests().await.unwrap().is_empty());
    assert!(tokens.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn an_unknown_mode_fails_only_that_route() {
    let tokens = token_server(&["tok-1"]).await;
    let mcp = mcp_server("tok-1").await;
    let mut good = record(&mcp, &tokens, Some("oauth_client_credentials"));
    good.server_id = "srv-1".to_string();
    let mut bad = record(&mcp, &tokens, Some("mutual_tls"));
    bad.server_id = "srv-bad".to_string();
    let source = McpToolSource::from_pack_routes(vec![bad, good], Some(secrets(CREDS)))
        .with_token_provider(provider());
    let catalog = source.catalog(&tenant()).await;
    assert!(catalog.resolve_route("srv-bad", "x").is_none());
    assert!(
        catalog
            .server_error("srv-bad")
            .is_some_and(|e| e.contains("mutual_tls"))
    );
    assert!(catalog.resolve_route("srv-1", "do_it").is_some());
    assert!(mcp.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn an_oauth_route_without_readable_credentials_is_refused() {
    let tokens = token_server(&["tok-1"]).await;
    let mcp = mcp_server("tok-1").await;
    let source = McpToolSource::from_pack_routes(
        vec![record(&mcp, &tokens, Some("oauth_client_credentials"))],
        Some(secrets("plain-token")),
    )
    .with_token_provider(provider());
    let out = call(&source).await.to_string();
    assert!(out.contains("client credentials"), "{out}");
    assert!(mcp.received_requests().await.unwrap().is_empty());
}
