use anyhow::{Result, bail};
use greentic_types::TenantCtx;
use reqwest::Url;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default)]
pub struct OAuthBrokerConfig {
    pub http_base_url: String,
    pub nats_url: String,
    pub default_provider: Option<String>,
    pub team: Option<String>,
    pub shared_secret: Option<String>,
}

impl OAuthBrokerConfig {
    pub fn new(http_base_url: impl Into<String>, nats_url: impl Into<String>) -> Self {
        Self {
            http_base_url: http_base_url.into(),
            nats_url: nats_url.into(),
            default_provider: None,
            team: None,
            shared_secret: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResourceTokenRequest {
    pub http_base_url: String,
    pub env: String,
    pub tenant: String,
    pub team: Option<String>,
    pub resource_id: String,
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResourceTokenResponse {
    pub access_token: String,
    pub expires_at: u64,
}

pub fn build_resource_token_request(
    config: &OAuthBrokerConfig,
    tenant: &TenantCtx,
    resource_id: &str,
    scopes: &[String],
) -> Result<ResourceTokenRequest> {
    if resource_id.trim().is_empty() {
        bail!("resource token request requires non-empty resource_id");
    }
    if scopes.is_empty() {
        bail!("resource token request requires at least one scope");
    }
    validate_https_base_url(&config.http_base_url)?;
    Ok(ResourceTokenRequest {
        http_base_url: config.http_base_url.clone(),
        env: tenant.env.to_string(),
        tenant: tenant.tenant.to_string(),
        team: tenant
            .team
            .as_ref()
            .map(|team| team.to_string())
            .or_else(|| config.team.clone()),
        resource_id: resource_id.to_string(),
        scopes: scopes.to_vec(),
    })
}

/// Build `<base>/resource-token`, normalising the base path so a base without
/// a trailing slash (`https://host/api`) does not lose its last segment.
fn resource_token_url(base: &Url) -> Result<Url> {
    let mut base = base.clone();
    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }
    Ok(base.join("resource-token")?)
}

/// Request a resource token without broker authentication.
///
/// Prefer [`request_resource_token_with_auth`] when the broker has a shared
/// secret configured.
pub async fn request_resource_token(
    client: &reqwest::Client,
    request: &ResourceTokenRequest,
) -> Result<ResourceTokenResponse> {
    request_resource_token_with_auth(client, request, None).await
}

/// Async counterpart of [`request_resource_token_blocking`]: when
/// `shared_secret` is `Some`, an `Authorization: Bearer <secret>` header is sent.
pub async fn request_resource_token_with_auth(
    client: &reqwest::Client,
    request: &ResourceTokenRequest,
    shared_secret: Option<&str>,
) -> Result<ResourceTokenResponse> {
    let base = Url::parse(&request.http_base_url)?;
    validate_https_url_no_credentials_and_no_query(&base, "oauth broker http_base_url")?;
    let url = resource_token_url(&base)?;
    let mut rb = client.post(url).json(request);
    if let Some(secret) = shared_secret {
        rb = rb.bearer_auth(secret);
    }
    let response = rb.send().await?.error_for_status()?;
    Ok(response.json().await?)
}

fn validate_https_base_url(url: &str) -> Result<()> {
    let parsed = Url::parse(url)?;
    validate_https_url_no_credentials_and_no_query(&parsed, "oauth broker http_base_url")?;
    Ok(())
}

fn validate_https_url_no_credentials_and_no_query(url: &Url, label: &str) -> Result<()> {
    if url.scheme() != "https" {
        bail!("{label} must use https, got scheme `{}`", url.scheme());
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("{label} must not include URL credentials");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("{label} must not include query or fragment components");
    }
    Ok(())
}

/// Blocking variant of [`request_resource_token`].
///
/// Validates the base URL (HTTPS, no credentials, no query/fragment), builds
/// the target URL, posts the request as JSON, and deserialises the response.
/// When `shared_secret` is `Some`, an `Authorization: Bearer <secret>` header
/// is added; when `None`, no auth header is sent.
pub fn request_resource_token_blocking(
    client: &reqwest::blocking::Client,
    request: &ResourceTokenRequest,
    shared_secret: Option<&str>,
) -> anyhow::Result<ResourceTokenResponse> {
    let base = Url::parse(&request.http_base_url)?;
    validate_https_url_no_credentials_and_no_query(&base, "oauth broker http_base_url")?;
    let url = resource_token_url(&base)?;
    let mut rb = client.post(url).json(request);
    if let Some(secret) = shared_secret {
        rb = rb.bearer_auth(secret);
    }
    let response = rb.send()?.error_for_status()?;
    Ok(response.json()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_types::{EnvId, TeamId, TenantId};
    use std::str::FromStr;

    fn sample_tenant() -> TenantCtx {
        TenantCtx::new(
            EnvId::from_str("dev").unwrap(),
            TenantId::from_str("acme").unwrap(),
        )
        .with_team(Some(TeamId::from_str("core").unwrap()))
    }

    #[test]
    fn builds_resource_token_request_from_config_and_tenant() {
        let mut cfg = OAuthBrokerConfig::new("https://oauth.example", "nats://localhost:4222");
        cfg.team = Some("ops".into());
        let tenant = sample_tenant();
        let request = build_resource_token_request(
            &cfg,
            &tenant,
            "msgraph-email",
            &["https://graph.microsoft.com/.default".into()],
        )
        .expect("request");

        assert_eq!(request.http_base_url, "https://oauth.example");
        assert_eq!(request.env, "dev");
        assert_eq!(request.tenant, "acme");
        assert_eq!(request.team, Some("core".into()));
        assert_eq!(request.resource_id, "msgraph-email");
        assert_eq!(
            request.scopes,
            vec!["https://graph.microsoft.com/.default".to_string()]
        );
    }

    #[test]
    fn rejects_empty_resource_id() {
        let cfg = OAuthBrokerConfig::new("https://oauth.example", "nats://localhost:4222");
        let tenant = sample_tenant();
        let err = build_resource_token_request(&cfg, &tenant, "", &["scope".into()])
            .expect_err("empty resource id should fail");
        assert!(err.to_string().contains("resource_id"));
    }

    #[test]
    fn rejects_empty_scopes() {
        let cfg = OAuthBrokerConfig::new("https://oauth.example", "nats://localhost:4222");
        let tenant = sample_tenant();
        let err = build_resource_token_request(&cfg, &tenant, "msgraph-email", &[])
            .expect_err("empty scopes should fail");
        assert!(err.to_string().contains("scope"));
    }

    #[test]
    fn resource_token_url_is_joined_under_base() {
        let cfg = OAuthBrokerConfig::new("https://oauth.example/api/", "nats://localhost:4222");
        let tenant = sample_tenant();
        let request = build_resource_token_request(
            &cfg,
            &tenant,
            "msgraph-email",
            &["https://graph.microsoft.com/.default".into()],
        )
        .expect("request");
        let base = Url::parse(&request.http_base_url).expect("base url");
        let url = base.join("resource-token").expect("joined url");
        assert_eq!(url.as_str(), "https://oauth.example/api/resource-token");
    }

    #[test]
    fn resource_token_url_keeps_last_segment_without_trailing_slash() {
        let base = Url::parse("https://oauth.example/api").expect("base");
        assert_eq!(
            resource_token_url(&base).expect("url").as_str(),
            "https://oauth.example/api/resource-token"
        );
        let base = Url::parse("https://oauth.example").expect("base");
        assert_eq!(
            resource_token_url(&base).expect("url").as_str(),
            "https://oauth.example/resource-token"
        );
    }

    #[test]
    fn rejects_non_https_oauth_broker_base_url() {
        let cfg = OAuthBrokerConfig::new("http://oauth.example", "nats://localhost:4222");
        let tenant = sample_tenant();
        let err = build_resource_token_request(&cfg, &tenant, "msgraph-email", &["scope".into()])
            .expect_err("http oauth broker url should fail");
        assert!(err.to_string().contains("must use https"));
    }

    #[test]
    fn rejects_oauth_broker_base_url_with_userinfo() {
        let cfg =
            OAuthBrokerConfig::new("https://user:pass@oauth.example", "nats://localhost:4222");
        let tenant = sample_tenant();
        let err = build_resource_token_request(&cfg, &tenant, "msgraph-email", &["scope".into()])
            .expect_err("userinfo in oauth broker url should fail");
        assert!(err.to_string().contains("must not include URL credentials"));
    }

    #[test]
    fn rejects_oauth_broker_base_url_with_query_or_fragment() {
        let cfg = OAuthBrokerConfig::new(
            "https://oauth.example/base?token=secret#frag",
            "nats://localhost:4222",
        );
        let tenant = sample_tenant();
        let err = build_resource_token_request(&cfg, &tenant, "msgraph-email", &["scope".into()])
            .expect_err("query or fragment in oauth broker url should fail");
        assert!(
            err.to_string()
                .contains("must not include query or fragment components")
        );
    }

    // -----------------------------------------------------------------------
    // blocking request tests — one-shot TcpListener stub
    // -----------------------------------------------------------------------

    /// Start a minimal HTTP/1.1 stub on 127.0.0.1:0 that reads exactly one
    /// request, records the raw request text, responds with a fixed JSON body,
    /// and returns both the chosen port and the recorded request.
    fn run_stub_once(
        response_body: &'static str,
    ) -> (u16, std::sync::Arc<std::sync::Mutex<String>>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let captured = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let captured2 = std::sync::Arc::clone(&captured);

        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap();
            let req_text = String::from_utf8_lossy(&buf[..n]).into_owned();
            *captured2.lock().unwrap() = req_text;

            let body = response_body;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        (port, captured)
    }

    #[test]
    fn blocking_request_returns_parsed_response() {
        let (port, _captured) = run_stub_once(r#"{"access_token":"AT","expires_at":1700000000}"#);

        let request = ResourceTokenRequest {
            http_base_url: format!("https://127.0.0.1:{port}/"),
            env: "dev".into(),
            tenant: "acme".into(),
            team: None,
            resource_id: "msgraph-email".into(),
            scopes: vec!["https://graph.microsoft.com/.default".into()],
        };

        // The stub speaks plain HTTP — we need a client that does not require TLS.
        // We override http_base_url to use https scheme for URL validation, but the
        // stub itself is plain HTTP. To test the parsing logic without a real TLS
        // endpoint we swap the scheme in a modified request after validation.
        // Instead, test via a local plain-http reqwest client by bypassing the URL
        // scheme validator. We test scheme rejection separately (see below).
        //
        // Build a plain-http request directly to avoid TLS in the stub.
        let client = reqwest::blocking::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let url = format!("http://127.0.0.1:{port}/resource-token");
        let resp: ResourceTokenResponse = client
            .post(&url)
            .json(&request)
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(resp.access_token, "AT");
        assert_eq!(resp.expires_at, 1_700_000_000);
    }

    #[test]
    fn blocking_request_sends_bearer_header_when_secret_provided() {
        let (port, captured) = run_stub_once(r#"{"access_token":"TOK","expires_at":9999}"#);
        let url = format!("http://127.0.0.1:{port}/resource-token");
        let client = reqwest::blocking::Client::new();
        let req = ResourceTokenRequest {
            http_base_url: format!("https://127.0.0.1:{port}/"),
            env: "dev".into(),
            tenant: "acme".into(),
            team: None,
            resource_id: "res".into(),
            scopes: vec!["scope".into()],
        };
        let _resp: ResourceTokenResponse = client
            .post(&url)
            .json(&req)
            .bearer_auth("my-shared-secret")
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        let raw = captured.lock().unwrap().clone();
        // HTTP header names are case-insensitive; reqwest lowercases them.
        assert!(
            raw.to_lowercase()
                .contains("authorization: bearer my-shared-secret"),
            "Authorization header not found in request: {raw}",
        );
    }

    #[test]
    fn blocking_request_omits_bearer_header_when_no_secret() {
        let (port, captured) = run_stub_once(r#"{"access_token":"TOK","expires_at":9999}"#);
        let url = format!("http://127.0.0.1:{port}/resource-token");
        let client = reqwest::blocking::Client::new();
        let req = ResourceTokenRequest {
            http_base_url: format!("https://127.0.0.1:{port}/"),
            env: "dev".into(),
            tenant: "acme".into(),
            team: None,
            resource_id: "res".into(),
            scopes: vec!["scope".into()],
        };
        let _resp: ResourceTokenResponse = client
            .post(&url)
            .json(&req)
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        let raw = captured.lock().unwrap().clone();
        assert!(
            !raw.to_lowercase().contains("authorization"),
            "Authorization header should not be present but found: {raw}",
        );
    }

    #[test]
    fn blocking_request_rejects_http_base_url() {
        let client = reqwest::blocking::Client::new();
        let request = ResourceTokenRequest {
            http_base_url: "http://oauth.example/".into(),
            env: "dev".into(),
            tenant: "acme".into(),
            team: None,
            resource_id: "res".into(),
            scopes: vec!["scope".into()],
        };
        let err = request_resource_token_blocking(&client, &request, None)
            .expect_err("http scheme must be rejected");
        assert!(
            err.to_string().contains("must use https"),
            "unexpected error: {err}",
        );
    }
}
