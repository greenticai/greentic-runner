//! `BrokerSecretsManager` — reads/writes per-tenant secrets over the
//! greentic-secrets-broker HTTP API. The runner uses this to resolve
//! per-tenant LLM credentials written by greentic-designer-admin.

use std::borrow::Cow;

use async_trait::async_trait;
use base64::Engine as _;
use greentic_secrets_lib::{SecretError, SecretsManager};
use reqwest::Client;
use serde::Deserialize;

/// Whether `value` may be used as one segment of a `secrets://` URI built from
/// pack-controlled input (MCP server id / auth team, SoR name). Same rule as
/// `greentic_aw_runtime::scoped_secrets::is_safe_segment`, which is
/// feature-gated and so cannot be called from the always-compiled pack parsers.
pub(crate) fn is_safe_secret_segment(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        && !value.starts_with('.')
        && !value.contains("..")
}

/// Check a tenant id ONCE, at config load, against every rule a later use of it
/// imposes: it is a `TenantId` (`[A-Za-z0-9._-]`), a host directory name
/// (`wasi::tenant_dir_segment`, at most 128 bytes) and a secrets-path segment
/// ([`is_safe_secret_segment`]: no leading `.`, no `..`). The secrets rule is the
/// strictest, so a `.hidden` or `a..b` tenant used to load and run, then fail
/// every secret read; it now fails here with a message naming the tenant.
pub(crate) fn validate_tenant_segment(tenant: &str) -> anyhow::Result<()> {
    if tenant.len() <= 128 && is_safe_secret_segment(tenant) {
        return Ok(());
    }
    anyhow::bail!(
        "invalid tenant id {tenant:?}: use 1-128 characters from [A-Za-z0-9._-], not starting with '.' \
         and without '..' (the id is also a secrets-path segment and a directory name)"
    )
}

fn is_plain_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment != "."
        && segment != ".."
        && !segment
            .chars()
            .any(|c| c.is_control() || matches!(c, '%' | '?' | '#' | '\\' | '/'))
}

/// Convert a canonical `secrets://{env}/{tenant}/{team}/{cat}/{name}` URI into
/// the broker `/v1/...` request path. Path segments are preserved verbatim
/// (the broker treats `_` as the tenant-wide team).
pub(crate) fn broker_path_from_uri(uri: &str) -> Result<String, SecretError> {
    let rest = uri
        .strip_prefix("secrets://")
        .ok_or_else(|| SecretError::Backend("not a secrets:// uri".into()))?;
    if rest.is_empty() {
        return Err(SecretError::Backend("empty secrets uri".into()));
    }
    // `url` collapses `.`/`..` segments (and their percent-encoded forms), so
    // an unvalidated segment could climb out of the tenant's path. Refuse
    // anything that is not a plain segment, then percent-encode the rest.
    let mut path = String::from("/v1");
    for segment in rest.split('/') {
        if !is_plain_segment(segment) {
            return Err(SecretError::Backend(
                "secrets uri contains an unsafe path segment".into(),
            ));
        }
        path.push('/');
        for byte in segment.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
                path.push(char::from(byte));
            } else {
                path.push_str(&format!("%{byte:02X}"));
            }
        }
    }
    Ok(path)
}

#[derive(Deserialize)]
struct SecretResponse {
    value: String,
    #[serde(default)]
    encoding: String, // "utf8" | "base64"
}

pub(crate) struct BrokerSecretsManager {
    client: Client,
    endpoint: String,
    token: String,
}

impl BrokerSecretsManager {
    pub(crate) fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_else(|err| {
                    tracing::warn!(
                        error = %err,
                        "failed to build the secrets broker http client; falling back to a default client WITHOUT the 10s timeout"
                    );
                    Client::new()
                }),
            endpoint: endpoint.into().trim_end_matches('/').to_string(),
            token: token.into().trim().to_string(),
        }
    }

    /// Attach the bearer credential only when there is one: an empty token
    /// must not become `Authorization: Bearer ` on the wire.
    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.token.is_empty() {
            request
        } else {
            request.bearer_auth(&self.token)
        }
    }
}

#[async_trait]
impl SecretsManager for BrokerSecretsManager {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        let url = format!("{}{}", self.endpoint, broker_path_from_uri(path)?);
        let resp = self
            .authorize(self.client.get(&url))
            .send()
            .await
            .map_err(|e| SecretError::Backend(Cow::Owned(e.without_url().to_string())))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(SecretError::NotFound(path.to_string()));
        }
        if resp.status() == reqwest::StatusCode::FORBIDDEN
            || resp.status() == reqwest::StatusCode::UNAUTHORIZED
        {
            return Err(SecretError::Permission(path.to_string()));
        }
        if !resp.status().is_success() {
            return Err(SecretError::Backend(Cow::Owned(format!(
                "broker {}",
                resp.status()
            ))));
        }
        let body: SecretResponse = resp
            .json()
            .await
            .map_err(|e| SecretError::Backend(Cow::Owned(e.without_url().to_string())))?;
        match body.encoding.as_str() {
            "base64" => base64::engine::general_purpose::STANDARD
                .decode(body.value.as_bytes())
                .map_err(|e| SecretError::Backend(Cow::Owned(e.to_string()))),
            _ => Ok(body.value.into_bytes()),
        }
    }

    async fn write(&self, path: &str, bytes: &[u8]) -> greentic_secrets_lib::Result<()> {
        let url = format!("{}{}", self.endpoint, broker_path_from_uri(path)?);
        let value = String::from_utf8(bytes.to_vec())
            .map_err(|e| SecretError::Backend(Cow::Owned(e.to_string())))?;
        let resp = self
            .authorize(self.client.put(&url))
            .json(&serde_json::json!({
                "visibility": "private",
                "content_type": "text",
                "encoding": "utf8",
                "value": value
            }))
            .send()
            .await
            .map_err(|e| SecretError::Backend(Cow::Owned(e.without_url().to_string())))?;
        if !resp.status().is_success() {
            return Err(SecretError::Backend(Cow::Owned(format!(
                "broker put {}",
                resp.status()
            ))));
        }
        Ok(())
    }

    async fn delete(&self, path: &str) -> greentic_secrets_lib::Result<()> {
        let url = format!("{}{}", self.endpoint, broker_path_from_uri(path)?);
        let resp = self
            .authorize(self.client.delete(&url))
            .send()
            .await
            .map_err(|e| SecretError::Backend(Cow::Owned(e.without_url().to_string())))?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            return Err(SecretError::Backend(Cow::Owned(format!(
                "broker delete {}",
                resp.status()
            ))));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_secrets_uri_to_broker_v1_path() {
        let p = broker_path_from_uri("secrets://default/acme/_/llm/abc-123").unwrap();
        assert_eq!(p, "/v1/default/acme/_/llm/abc-123");
    }

    #[test]
    fn rejects_traversal_and_reserved_characters_in_any_segment() {
        for bad in ["..", ".", "%2e%2e", "%2E", "a?b", "a#b", "a\\b", "a\nb", ""] {
            let uri = format!("secrets://default/acme/{bad}/mcp/k");
            assert!(broker_path_from_uri(&uri).is_err(), "team {bad:?}");
            let uri = format!("secrets://default/acme/_/mcp/{bad}");
            assert!(broker_path_from_uri(&uri).is_err(), "key {bad:?}");
        }
        // A `/` inside what the caller meant as one segment adds segments;
        // the `..` that results is still refused.
        assert!(broker_path_from_uri("secrets://default/acme/_/mcp/../../other/_/mcp/k").is_err());
    }

    #[test]
    fn percent_encodes_unusual_but_allowed_characters() {
        let p = broker_path_from_uri("secrets://default/acme/_/mcp/a b").unwrap();
        assert_eq!(p, "/v1/default/acme/_/mcp/a%20b");
    }

    #[test]
    fn safe_secret_segment_rule() {
        assert!(is_safe_secret_segment("srv-1.unit_x"));
        for bad in ["", "..", ".a", "a/b", "a?b", "a#b", "%2e%2e", "a\\b"] {
            assert!(!is_safe_secret_segment(bad), "{bad:?}");
        }
    }

    #[test]
    fn rejects_non_secrets_uri() {
        assert!(broker_path_from_uri("https://x/y").is_err());
    }

    #[tokio::test]
    async fn read_decodes_utf8_value() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/v1/default/acme/_/llm/abc"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "uri": "secrets://default/acme/_/llm/abc",
                    "version": 1,
                    "visibility": "private",
                    "content_type": "text",
                    "encoding": "utf8",
                    "value": "sk-xyz"
                })),
            )
            .mount(&server)
            .await;
        let mgr = BrokerSecretsManager::new(server.uri(), "tok");
        let bytes = mgr.read("secrets://default/acme/_/llm/abc").await.unwrap();
        assert_eq!(bytes, b"sk-xyz");
    }

    #[tokio::test]
    async fn empty_token_sends_no_authorization_header() -> anyhow::Result<()> {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "value": "v", "encoding": "utf8"
                })),
            )
            .mount(&server)
            .await;
        for token in ["", "   "] {
            let mgr = BrokerSecretsManager::new(server.uri(), token);
            mgr.read("secrets://default/acme/_/llm/abc").await?;
        }
        let with_token = BrokerSecretsManager::new(server.uri(), "tok");
        with_token.read("secrets://default/acme/_/llm/abc").await?;

        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].headers.get("authorization").is_none());
        assert!(requests[1].headers.get("authorization").is_none());
        assert_eq!(
            requests[2]
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok()),
            Some("Bearer tok")
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_decodes_base64_value() {
        // base64("sk-bin") == "c2stYmlu"
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/v1/default/acme/_/llm/bin"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "uri": "secrets://default/acme/_/llm/bin",
                    "version": 1,
                    "visibility": "private",
                    "content_type": "text",
                    "encoding": "base64",
                    "value": "c2stYmlu"
                })),
            )
            .mount(&server)
            .await;
        let mgr = BrokerSecretsManager::new(server.uri(), "tok");
        let bytes = mgr.read("secrets://default/acme/_/llm/bin").await.unwrap();
        assert_eq!(bytes, b"sk-bin");
    }
}
