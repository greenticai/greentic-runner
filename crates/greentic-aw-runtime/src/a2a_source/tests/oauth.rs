//! OAuth `client_credentials` on A2A routes.

use std::sync::Arc;

use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{RPC_PATH, TestSecrets, mount_card, mount_raw_card, route};
use crate::a2a_source::{A2aRoute, A2aToolSource};
use crate::oauth::TokenProvider;

const SECRET_URI: &str = "secrets://default/acme/_/a2a/recipe";
const CREDS: &str = r#"{"client_id":"cid","client_secret":"csecret-XYZ"}"#;

fn provider() -> Arc<TokenProvider> {
    let provider = Arc::new(TokenProvider::new());
    provider.allow_loopback_http_for_tests();
    provider
}

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

/// An agent accepting only `Bearer <accepted>` on `SendMessage`.
async fn agent(accepted: &str) -> MockServer {
    let server = MockServer::start().await;
    mount_card(&server).await;
    let bearer = format!("Bearer {accepted}");
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .and(header("authorization", bearer.as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {"message": {"messageId": "m", "role": "ROLE_AGENT",
                                    "parts": [{"text": "ok"}]}}
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path(RPC_PATH))
        .respond_with(ResponseTemplate::new(401))
        .with_priority(10)
        .mount(&server)
        .await;
    server
}

fn oauth_route(agent: &MockServer, tokens: &MockServer, mode: &str) -> A2aRoute {
    A2aRoute {
        auth_mode: Some(mode.to_string()),
        oauth_endpoint: Some(format!("{}/token", tokens.uri())),
        oauth_scope: Some("a2a".to_string()),
        ..route("recipe", agent.uri(), None, None, true)
    }
}

fn source(routes: Vec<A2aRoute>, secret: &str) -> A2aToolSource {
    let secrets: Arc<dyn greentic_secrets_lib::SecretsManager> =
        TestSecrets::with(&[(SECRET_URI, secret)]);
    A2aToolSource::from_routes_with_provider(routes, Some(secrets), "acme", None, provider())
        .expect("buildable")
}

async fn ask(source: &A2aToolSource) -> serde_json::Value {
    source
        .catalog()
        .await
        .dispatch("recipe", &json!({"message": "hi"}))
        .await
}

#[tokio::test]
async fn an_oauth_agent_is_called_with_the_fetched_bearer() {
    let tokens = token_server(&["tok-1"]).await;
    let agent = agent("tok-1").await;
    let out = ask(&source(
        vec![oauth_route(&agent, &tokens, "oauth_client_credentials")],
        CREDS,
    ))
    .await;
    assert_eq!(out["reply"], "ok", "{out}");
    let gets = agent.received_requests().await.unwrap();
    assert!(
        gets.iter()
            .filter(|r| r.method.as_str() == "GET")
            .all(|r| r.headers.get("authorization").is_none()),
        "the card fetch stays unauthenticated"
    );
}

#[tokio::test]
async fn a_refused_token_is_replaced_and_the_post_retried_once() {
    let tokens = token_server(&["tok-1", "tok-2"]).await;
    let agent = agent("tok-2").await;
    let out = ask(&source(
        vec![oauth_route(&agent, &tokens, "oauth_client_credentials")],
        CREDS,
    ))
    .await;
    assert_eq!(out["reply"], "ok", "{out}");
    assert_eq!(tokens.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn a_json_secret_on_a_bearer_route_is_refused_before_any_post() {
    let tokens = token_server(&["tok-1"]).await;
    let agent = agent("tok-1").await;
    let out = ask(&source(vec![oauth_route(&agent, &tokens, "bearer")], CREDS))
        .await
        .to_string();
    assert!(out.contains("client credentials"), "{out}");
    assert!(!out.contains("csecret-XYZ"));
    assert!(tokens.received_requests().await.unwrap().is_empty());
    let posts = agent
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method.as_str() == "POST")
        .count();
    assert_eq!(posts, 0);
}

#[tokio::test]
async fn an_unknown_mode_is_refused_without_reading_the_secret_or_the_endpoint() {
    let tokens = token_server(&["tok-1"]).await;
    let agent = agent("tok-1").await;
    let out = ask(&source(
        vec![oauth_route(&agent, &tokens, "mutual_tls")],
        CREDS,
    ))
    .await
    .to_string();
    assert!(out.contains("mutual_tls"), "{out}");
    assert!(tokens.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_card_naming_another_host_never_gets_a_token_or_the_secret() {
    let tokens = token_server(&["tok-1"]).await;
    let agent = MockServer::start().await;
    let card = super::CARD.replace("PLACEHOLDER", "https://evil.example/a2a");
    mount_raw_card(&agent, card).await;
    let out = ask(&source(
        vec![oauth_route(&agent, &tokens, "oauth_client_credentials")],
        CREDS,
    ))
    .await
    .to_string();
    assert!(out.contains("differs from configured host"), "{out}");
    assert!(tokens.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_missing_oauth_endpoint_is_refused() {
    let tokens = token_server(&["tok-1"]).await;
    let agent = agent("tok-1").await;
    let mut r = oauth_route(&agent, &tokens, "oauth_client_credentials");
    r.oauth_endpoint = None;
    let out = ask(&source(vec![r], CREDS)).await.to_string();
    assert!(out.contains("oauth_endpoint"), "{out}");
}
