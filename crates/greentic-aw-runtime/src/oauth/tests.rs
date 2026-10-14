//! Provider tests against a local HTTP stub. The stub is on loopback, so each
//! provider that talks to it opts in through the explicit test hook; one test
//! proves the default refuses.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use secrecy::{ExposeSecret, SecretString};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use super::{OAuthError, TokenProvider};

const KEY: &str = "acme\u{1f}_\u{1f}unit-a\u{1f}mcp\u{1f}srv-1";
const CLIENT_ID: &str = "client-abc";
const CLIENT_SECRET: &str = "s3cr3t-DO-NOT-LEAK";

/// A clock the test advances by hand.
#[derive(Clone)]
struct FakeClock {
    base: Instant,
    offset: Arc<Mutex<Duration>>,
}

impl FakeClock {
    fn new() -> Self {
        Self {
            base: Instant::now(),
            offset: Arc::new(Mutex::new(Duration::ZERO)),
        }
    }
    fn advance(&self, by: Duration) {
        *self.offset.lock().unwrap() += by;
    }
    fn provider(&self) -> TokenProvider {
        let this = self.clone();
        let provider =
            TokenProvider::with_clock(Arc::new(move || this.base + *this.offset.lock().unwrap()));
        provider.allow_loopback_http_for_tests();
        provider
    }
}

fn secret() -> SecretString {
    SecretString::from(CLIENT_SECRET.to_string())
}

async fn stub(expires_in: serde_json::Value, token: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": expires_in,
        })))
        .mount(&server)
        .await;
    server
}

fn endpoint(server: &MockServer) -> String {
    format!("{}/token", server.uri())
}

async fn fetch(provider: &TokenProvider, server: &MockServer) -> Result<String, OAuthError> {
    provider
        .token_for(KEY, &endpoint(server), None, None, CLIENT_ID, &secret())
        .await
        .map(|t| t.expose_secret().to_string())
}

async fn hits(server: &MockServer) -> usize {
    server.received_requests().await.unwrap().len()
}

#[tokio::test]
async fn the_first_call_fetches_and_the_second_reuses() {
    let server = stub(json!(3600), "tok-1").await;
    let provider = FakeClock::new().provider();
    assert_eq!(fetch(&provider, &server).await.unwrap(), "tok-1");
    assert_eq!(fetch(&provider, &server).await.unwrap(), "tok-1");
    assert_eq!(hits(&server).await, 1);
}

#[tokio::test]
async fn a_token_is_replaced_forty_five_seconds_before_it_expires() {
    let clock = FakeClock::new();
    let server = stub(json!(100), "tok-1").await;
    let provider = clock.provider();
    fetch(&provider, &server).await.unwrap();
    clock.advance(Duration::from_secs(54));
    fetch(&provider, &server).await.unwrap();
    assert_eq!(hits(&server).await, 1, "100 s minus the 45 s skew is 55 s");
    clock.advance(Duration::from_secs(2));
    fetch(&provider, &server).await.unwrap();
    assert_eq!(hits(&server).await, 2);
}

#[tokio::test]
async fn sixteen_concurrent_calls_make_one_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(250))
                .set_body_json(json!({"access_token": "tok-1", "expires_in": 3600})),
        )
        .mount(&server)
        .await;
    let provider = Arc::new(FakeClock::new().provider());
    let url = endpoint(&server);
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let provider = Arc::clone(&provider);
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            provider
                .token_for(KEY, &url, None, None, CLIENT_ID, &secret())
                .await
                .map(|t| t.expose_secret().to_string())
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap().unwrap(), "tok-1");
    }
    assert_eq!(hits(&server).await, 1);
}

#[tokio::test]
async fn invalidate_makes_the_next_call_fetch_again() {
    let server = stub(json!(3600), "tok-1").await;
    let provider = FakeClock::new().provider();
    fetch(&provider, &server).await.unwrap();
    provider.invalidate(KEY).await;
    fetch(&provider, &server).await.unwrap();
    assert_eq!(hits(&server).await, 2);
}

#[tokio::test]
async fn a_failing_endpoint_is_not_hammered() {
    let clock = FakeClock::new();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal secret detail"))
        .mount(&server)
        .await;
    let provider = clock.provider();
    let first = fetch(&provider, &server).await.unwrap_err();
    assert!(matches!(first, OAuthError::Status { status: 500, .. }));
    let second = fetch(&provider, &server).await.unwrap_err();
    assert!(matches!(second, OAuthError::Backoff(_)), "{second:?}");
    assert_eq!(hits(&server).await, 1);
    clock.advance(Duration::from_secs(16));
    let _ = fetch(&provider, &server).await;
    assert_eq!(hits(&server).await, 2, "retried once the window passed");
}

#[tokio::test]
async fn a_changed_secret_is_not_held_back_by_an_earlier_failure() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    let provider = FakeClock::new().provider();
    let _ = fetch(&provider, &server).await;
    let other = SecretString::from("rotated".to_string());
    let err = provider
        .token_for(KEY, &endpoint(&server), None, None, CLIENT_ID, &other)
        .await
        .unwrap_err();
    assert!(matches!(err, OAuthError::Status { status: 401, .. }));
    assert_eq!(hits(&server).await, 2);
}

#[tokio::test]
async fn a_rotated_secret_replaces_a_live_token_immediately() {
    let server = stub(json!(3600), "tok-1").await;
    let provider = FakeClock::new().provider();
    fetch(&provider, &server).await.unwrap();
    let other = SecretString::from("rotated".to_string());
    provider
        .token_for(KEY, &endpoint(&server), None, None, CLIENT_ID, &other)
        .await
        .unwrap();
    assert_eq!(hits(&server).await, 2);
}

#[tokio::test]
async fn expires_in_is_clamped_to_sixty_seconds_and_one_hour() {
    // 5 s is raised to 60 s: 60 - 45 = 15 s of cache.
    let clock = FakeClock::new();
    let server = stub(json!(5), "tok").await;
    let provider = clock.provider();
    fetch(&provider, &server).await.unwrap();
    clock.advance(Duration::from_secs(14));
    fetch(&provider, &server).await.unwrap();
    assert_eq!(hits(&server).await, 1);
    clock.advance(Duration::from_secs(2));
    fetch(&provider, &server).await.unwrap();
    assert_eq!(hits(&server).await, 2);

    // A day is cut to an hour: 3600 - 45 = 3555 s of cache.
    let clock = FakeClock::new();
    let server = stub(json!(86400), "tok").await;
    let provider = clock.provider();
    fetch(&provider, &server).await.unwrap();
    clock.advance(Duration::from_secs(3554));
    fetch(&provider, &server).await.unwrap();
    assert_eq!(hits(&server).await, 1);
    clock.advance(Duration::from_secs(2));
    fetch(&provider, &server).await.unwrap();
    assert_eq!(hits(&server).await, 2);
}

#[tokio::test]
async fn the_request_is_a_form_post_with_basic_client_auth() {
    let server = stub(json!(3600), "tok-1").await;
    let provider = FakeClock::new().provider();
    provider
        .token_for(
            KEY,
            &endpoint(&server),
            Some("read write"),
            Some("https://api.example"),
            CLIENT_ID,
            &secret(),
        )
        .await
        .unwrap();
    let requests = server.received_requests().await.unwrap();
    let req: &Request = &requests[0];
    let auth = req.headers.get("authorization").unwrap().to_str().unwrap();
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(auth.strip_prefix("Basic ").unwrap())
        .unwrap();
    assert_eq!(
        String::from_utf8(decoded).unwrap(),
        format!("{CLIENT_ID}:{CLIENT_SECRET}")
    );
    let body = String::from_utf8(req.body.clone()).unwrap();
    assert!(body.contains("grant_type=client_credentials"), "{body}");
    assert!(body.contains("scope=read+write"), "{body}");
    assert!(
        body.contains("audience=https%3A%2F%2Fapi.example"),
        "{body}"
    );
    assert!(
        !body.contains(CLIENT_SECRET),
        "the secret rides the Basic header, not the body"
    );
}

#[tokio::test]
async fn an_http_endpoint_is_refused_before_any_request() {
    let provider = TokenProvider::new();
    for url in [
        "http://auth.example.com/token",
        "ftp://auth.example.com/token",
    ] {
        let err = provider
            .token_for(KEY, url, None, None, CLIENT_ID, &secret())
            .await
            .unwrap_err();
        assert!(
            matches!(err, OAuthError::EndpointRefused(_)),
            "{url}: {err:?}"
        );
    }
}

#[tokio::test]
async fn userinfo_in_the_endpoint_is_refused() {
    let provider = TokenProvider::new();
    let err = provider
        .token_for(
            KEY,
            "https://user:pw@auth.example.com/token",
            None,
            None,
            CLIENT_ID,
            &secret(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, OAuthError::EndpointRefused(_)));
}

#[tokio::test]
async fn private_loopback_and_link_local_addresses_are_refused_by_default() {
    let provider = TokenProvider::new();
    for url in [
        "https://127.0.0.1/token",
        "https://[::1]/token",
        "https://10.1.2.3/token",
        "https://192.168.0.5/token",
        "https://172.16.9.9/token",
        "https://169.254.169.254/latest/meta-data",
        "https://100.64.0.1/token",
        "https://[fd00::1]/token",
        "https://[fe80::1]/token",
        "https://[::ffff:10.0.0.1]/token",
        "https://localhost/token",
    ] {
        let err = provider
            .token_for(KEY, url, None, None, CLIENT_ID, &secret())
            .await
            .unwrap_err();
        assert!(
            matches!(err, OAuthError::EndpointRefused(_)),
            "{url}: {err:?}"
        );
    }
}

#[tokio::test]
async fn loopback_works_only_with_the_explicit_test_hook() {
    let server = stub(json!(3600), "tok").await;
    // The stub is plain http on loopback: refused without the hook.
    let strict = TokenProvider::new();
    let err = strict
        .token_for(KEY, &endpoint(&server), None, None, CLIENT_ID, &secret())
        .await
        .unwrap_err();
    assert!(matches!(err, OAuthError::EndpointRefused(_)));
    assert_eq!(hits(&server).await, 0);
    let hooked = FakeClock::new().provider();
    assert!(fetch(&hooked, &server).await.is_ok());
}

#[tokio::test]
async fn a_redirect_is_not_followed() {
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"access_token": "evil"})))
        .mount(&target)
        .await;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(307).insert_header("location", format!("{}/token", target.uri())),
        )
        .mount(&server)
        .await;
    let provider = FakeClock::new().provider();
    let err = fetch(&provider, &server).await.unwrap_err();
    assert!(
        matches!(err, OAuthError::Status { status: 307, .. }),
        "{err:?}"
    );
    assert_eq!(hits(&target).await, 0, "the client secret never followed");
}

#[tokio::test]
async fn an_oversized_response_is_refused() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(70 * 1024)))
        .mount(&server)
        .await;
    let provider = FakeClock::new().provider();
    let err = fetch(&provider, &server).await.unwrap_err();
    assert!(matches!(err, OAuthError::Malformed(_)), "{err:?}");
}

#[tokio::test]
async fn unusable_token_responses_are_refused() {
    for body in [
        json!({"token_type": "Bearer"}),
        json!({"access_token": ""}),
        json!({"access_token": "has space"}),
        json!({"access_token": "ok", "token_type": "mac"}),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body.clone()))
            .mount(&server)
            .await;
        let provider = FakeClock::new().provider();
        let err = fetch(&provider, &server).await.unwrap_err();
        assert!(matches!(err, OAuthError::Malformed(_)), "{body}: {err:?}");
    }
}

#[tokio::test]
async fn nothing_secret_reaches_debug_or_error_text() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(
                json!({"error": "invalid_client", "error_description": CLIENT_SECRET}),
            ),
        )
        .mount(&server)
        .await;
    let provider = FakeClock::new().provider();
    let err = fetch(&provider, &server).await.unwrap_err();
    let shown = format!("{err} {err:?} {provider:?}");
    assert!(shown.contains("invalid_client"), "{shown}");
    assert!(!shown.contains(CLIENT_SECRET), "{shown}");

    let ok = stub(json!(3600), "tok-SECRET-VALUE").await;
    let provider = FakeClock::new().provider();
    let token = provider
        .token_for(KEY, &endpoint(&ok), None, None, CLIENT_ID, &secret())
        .await
        .unwrap();
    assert!(!format!("{token:?} {provider:?}").contains("tok-SECRET-VALUE"));
}

#[test]
fn the_forbidden_address_table() {
    use std::net::IpAddr;
    for bad in [
        "0.0.0.0",
        "127.0.0.1",
        "10.0.0.1",
        "172.31.255.255",
        "192.168.1.1",
        "169.254.0.1",
        "100.64.0.1",
        "224.0.0.1",
        "255.255.255.255",
        "::1",
        "::",
        "fc00::1",
        "fe80::1",
        "ff02::1",
        "::ffff:127.0.0.1",
    ] {
        assert!(
            super::provider::is_forbidden(bad.parse::<IpAddr>().unwrap()),
            "{bad}"
        );
    }
    for good in [
        "8.8.8.8",
        "1.1.1.1",
        "93.184.216.34",
        "2606:4700::1111",
        "172.32.0.1",
    ] {
        assert!(
            !super::provider::is_forbidden(good.parse::<IpAddr>().unwrap()),
            "{good}"
        );
    }
}
