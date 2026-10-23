//! `BrokerSecretsManager` — reads/writes per-tenant secrets over the
//! greentic-secrets-broker HTTP API. The runner uses this to resolve
//! per-tenant LLM credentials written by greentic-designer-admin.

use std::borrow::Cow;

use async_trait::async_trait;
use base64::Engine as _;
use greentic_secrets_lib::{SecretError, SecretsManager};
use reqwest::Client;
use serde::Deserialize;

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
    Ok(format!("/v1/{rest}"))
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
            token: token.into(),
        }
    }
}

#[async_trait]
impl SecretsManager for BrokerSecretsManager {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        let url = format!("{}{}", self.endpoint, broker_path_from_uri(path)?);
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| SecretError::Backend(Cow::Owned(e.to_string())))?;
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
            .map_err(|e| SecretError::Backend(Cow::Owned(e.to_string())))?;
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
            .client
            .put(&url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({
                "visibility": "private",
                "content_type": "text",
                "encoding": "utf8",
                "value": value
            }))
            .send()
            .await
            .map_err(|e| SecretError::Backend(Cow::Owned(e.to_string())))?;
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
            .client
            .delete(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| SecretError::Backend(Cow::Owned(e.to_string())))?;
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
