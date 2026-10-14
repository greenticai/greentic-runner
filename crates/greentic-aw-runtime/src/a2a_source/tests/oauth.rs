//! `oauth_client_credentials` routes: the agent receives a fetched access
//! token, never the stored client secret, and every misconfiguration refuses
//! the call before anything credentialed is sent.

use std::sync::Arc;

use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{TestSecrets, header_of, mount_card, mount_raw_card, mount_reply, route};
use crate::a2a_source::{A2aRoute, A2aToolSource};
use crate::oauth::TokenProvider;

const URI: &str = "secrets://default/acme/_/a2a/recipe";
const CREDS: &str = r#"{"client_id":"cid","client_secret":"csecret-XYZ"}"#;

fn ok_reply() -> serde_json::Value {
    json!({"jsonrpc": "2.0", "id": 1, "result": {"message": {
        "messageId": "m-2", "role": "ROLE_AGENT", "parts": [{"text": "ok"}]
    }}})
}

async fn token_server(token: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"access_token": token, "token_type": "Bearer", "expires_in": 3600}),
        ))
        .mount(&server)
        .await;
    server
}

fn oauth_route(agent: &MockServer, idp: &MockServer) -> A2aRoute {
    A2aRoute {
        auth_mode: Some("oauth_client_credentials".into()),
        oauth_endpoint: Some(format!("{}/token", idp.uri())),
        oauth_scope: Some("agent.call".into()),
        ..route("recipe", agent.uri(), None, None, true)
    }
}

fn source(routes: Vec<A2aRoute>, secrets: Arc<TestSecrets>) -> A2aToolSource {
    let provider = Arc::new(TokenProvider::new());
    provider.allow_loopback_http_for_tests();
    let secrets: Arc<dyn greentic_secrets_lib::SecretsManager> = secrets;
    A2aToolSource::from_routes_with_provider(routes, Some(secrets), "acme", None, provider)
        .expect("source")
}

async fn posts(server: &MockServer) -> Vec<wiremock::Request> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method.as_str() == "POST")
        .collect()
}

#[tokio::test]
async fn the_agent_gets_the_fetched_token_and_never_the_client_secret() {
    let agent = MockServer::start().await;
    mount_card(&agent).await;
    mount_reply(&agent, ok_reply()).await;
    let idp = token_server("access-1").await;
    let src = source(
        vec![oauth_route(&agent, &idp)],
        TestSecrets::with(&[(URI, CREDS)]),
    );
    let value = src
        .catalog()
        .await
        .dispatch("recipe", &json!({"message": "hi"}))
        .await;
    assert_eq!(value["status"], "completed", "{value}");

    let sent = posts(&agent).await;
    assert_eq!(sent.len(), 1);
    assert_eq!(
        header_of(&sent[0], "authorization").as_deref(),
        Some("Bearer access-1")
    );
    let wire = String::from_utf8_lossy(&sent[0].body).to_string();
    assert!(!wire.contains("csecret-XYZ"));
    // The token request carried the sidecar's scope.
    let token_req = &posts(&idp).await[0];
    assert!(
        String::from_utf8_lossy(&token_req.body).contains("scope=agent.call"),
        "{token_req:?}"
    );
}

#[tokio::test]
async fn a_refused_token_is_dropped_and_retried_exactly_once() {
    let agent = MockServer::start().await;
    mount_card(&agent).await;
    Mock::given(method("POST"))
        .and(path("/a2a"))
        .and(header("authorization", "Bearer access-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_reply()))
        .with_priority(1)
        .mount(&agent)
        .await;
    Mock::given(method("POST"))
        .and(path("/a2a"))
        .respond_with(ResponseTemplate::new(401))
        .with_priority(5)
        .mount(&agent)
        .await;
    let idp = MockServer::start().await;
    for (token, once) in [("access-1", true), ("access-2", false)] {
        let mock = Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"access_token": token, "token_type": "Bearer", "expires_in": 3600}),
            ))
            .with_priority(if once { 1 } else { 2 });
        if once {
            mock.up_to_n_times(1).mount(&idp).await;
        } else {
            mock.mount(&idp).await;
        }
    }
    let src = source(
        vec![oauth_route(&agent, &idp)],
        TestSecrets::with(&[(URI, CREDS)]),
    );
    let value = src
        .catalog()
        .await
        .dispatch("recipe", &json!({"message": "hi"}))
        .await;
    assert_eq!(value["status"], "completed", "{value}");
    assert_eq!(posts(&agent).await.len(), 2, "one refused, one retried");
    assert_eq!(posts(&idp).await.len(), 2);
}

#[tokio::test]
async fn a_second_401_is_reported_not_retried_again() {
    let agent = MockServer::start().await;
    mount_card(&agent).await;
    Mock::given(method("POST"))
        .and(path("/a2a"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&agent)
        .await;
    let idp = token_server("access-1").await;
    let src = source(
        vec![oauth_route(&agent, &idp)],
        TestSecrets::with(&[(URI, CREDS)]),
    );
    let value = src
        .catalog()
        .await
        .dispatch("recipe", &json!({"message": "hi"}))
        .await;
    assert_eq!(value["status"], "error", "{value}");
    assert_eq!(posts(&agent).await.len(), 2);
}

#[tokio::test]
async fn an_unknown_auth_mode_refuses_the_agent_before_anything_is_sent() {
    let agent = MockServer::start().await;
    mount_card(&agent).await;
    mount_reply(&agent, ok_reply()).await;
    let secrets = TestSecrets::with(&[(URI, "plain-token")]);
    let mut r = route("recipe", agent.uri(), None, None, true);
    r.auth_mode = Some("mutual_tls".into());
    let value = source(vec![r], secrets.clone())
        .catalog()
        .await
        .dispatch("recipe", &json!({"message": "hi"}))
        .await;
    assert_eq!(value["status"], "error");
    assert!(
        value["error"].as_str().unwrap().contains("mutual_tls"),
        "{value}"
    );
    assert!(posts(&agent).await.is_empty());
    assert_eq!(secrets.reads(), 0, "the secret is not even read");
}

#[tokio::test]
async fn client_credentials_on_a_bearer_route_fail_explicitly() {
    let agent = MockServer::start().await;
    mount_card(&agent).await;
    mount_reply(&agent, ok_reply()).await;
    for mode in [None, Some("bearer".to_string())] {
        let mut r = route("recipe", agent.uri(), None, None, true);
        r.auth_mode = mode;
        let value = source(vec![r], TestSecrets::with(&[(URI, CREDS)]))
            .catalog()
            .await
            .dispatch("recipe", &json!({"message": "hi"}))
            .await;
        let error = value["error"].as_str().unwrap_or_default().to_string();
        assert!(error.contains("client credentials"), "{value}");
        assert!(!error.contains("csecret-XYZ"));
    }
    assert!(posts(&agent).await.is_empty());
}

#[tokio::test]
async fn an_oauth_route_without_an_endpoint_or_with_a_bad_secret_is_refused() {
    let agent = MockServer::start().await;
    mount_card(&agent).await;
    let idp = token_server("access-1").await;
    let mut no_endpoint = oauth_route(&agent, &idp);
    no_endpoint.oauth_endpoint = None;
    let bad = oauth_route(&agent, &idp);
    for (r, secret) in [(no_endpoint, CREDS), (bad, "not-json")] {
        let value = source(vec![r], TestSecrets::with(&[(URI, secret)]))
            .catalog()
            .await
            .dispatch("recipe", &json!({"message": "hi"}))
            .await;
        assert_eq!(value["status"], "error", "{value}");
    }
    assert!(posts(&agent).await.is_empty());
    assert!(posts(&idp).await.is_empty());
}

#[tokio::test]
async fn the_agent_host_rule_still_applies_and_blocks_the_token_fetch() {
    let agent = MockServer::start().await;
    let card = super::CARD.replace("PLACEHOLDER", "https://elsewhere.example/a2a");
    mount_raw_card(&agent, card).await;
    let idp = token_server("access-1").await;
    let value = source(
        vec![oauth_route(&agent, &idp)],
        TestSecrets::with(&[(URI, CREDS)]),
    )
    .catalog()
    .await
    .dispatch("recipe", &json!({"message": "hi"}))
    .await;
    assert_eq!(value["status"], "error", "{value}");
    assert!(
        posts(&idp).await.is_empty(),
        "no token was minted for a foreign host"
    );
}
