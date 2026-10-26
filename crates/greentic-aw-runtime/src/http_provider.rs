//! A [`ConfigProvider`] that pulls a full [`AgentConfig`] from the
//! greentic-designer-admin agent registry over HTTP, authed with a tenant
//! `gtc_live_*` bearer token. Tenant is implied by the token; `tenant` arg is
//! accepted for the trait but not used for the request.

use std::future::Future;
use std::pin::Pin;

use crate::config::AgentConfig;
use crate::config_provider::ConfigProvider;
use crate::error::ConfigError;
use crate::tenant::TenantContext;

/// Pulls `AgentConfig` from `{base}/api/v1/designer/agents/{agent_id}`.
pub struct HttpConfigProvider {
    base_url: String,
    token: String,
    client: reqwest::Client,
}

impl HttpConfigProvider {
    /// `base_url` is the admin origin (no trailing slash needed); `token` is a
    /// tenant `gtc_live_*` key.
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        // A per-request timeout matches the crate idiom (see llm_openai.rs) and
        // bounds a hung registry: without it the calling step() would block with
        // no upper bound, and the LayeredConfigProvider fallback cannot fire
        // until this future resolves.
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self::with_client(base_url, token, client)
    }

    fn with_client(
        base_url: impl Into<String>,
        token: impl Into<String>,
        client: reqwest::Client,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            client,
        }
    }
}

impl ConfigProvider for HttpConfigProvider {
    fn agent_config<'a>(
        &'a self,
        _tenant: &'a TenantContext,
        agent_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<AgentConfig, ConfigError>> + Send + 'a>> {
        Box::pin(async move {
            // `..` stays a traversal step even percent-encoded: never send it.
            if crate::tenant::is_dot_segment(agent_id) {
                return Err(ConfigError::AgentNotFound(agent_id.to_string()));
            }
            let url = format!(
                "{}/api/v1/designer/agents/{}",
                self.base_url,
                crate::tenant::percent_encode_segment(agent_id)
            );
            let resp = self
                .client
                .get(&url)
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(|e| {
                    ConfigError::Internal(format!(
                        "agent registry request failed: {}",
                        e.without_url()
                    ))
                })?;

            match resp.status().as_u16() {
                200 => resp.json::<AgentConfig>().await.map_err(|e| {
                    ConfigError::Misconfigured(format!("agent config decode: {}", e.without_url()))
                }),
                404 => Err(ConfigError::AgentNotFound(agent_id.to_string())),
                // Auth failures are operator-actionable misconfig, not a
                // transient fault — surface them (Misconfigured is NOT swallowed
                // by the LayeredConfigProvider fallback) rather than masking a
                // bad token behind a local fallback.
                401 | 403 => Err(ConfigError::Misconfigured(format!(
                    "agent registry auth rejected (status {})",
                    resp.status().as_u16()
                ))),
                other => Err(ConfigError::Internal(format!(
                    "agent registry returned status {other}"
                ))),
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn agent_config_json() -> serde_json::Value {
        serde_json::json!({
            "agent_id": "bot",
            "system_prompt": "be helpful",
            "tools": [{ "extension_id": "greentic.tavily", "tool_name": "web_search" }],
            "llm": { "provider": "openai", "model": "gpt-4o-mini" },
            "limits": {}
        })
    }

    #[tokio::test]
    async fn fetches_and_parses_agent_config() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/designer/agents/bot"))
            .and(header("authorization", "Bearer gtc_live_x"))
            .respond_with(ResponseTemplate::new(200).set_body_json(agent_config_json()))
            .mount(&server)
            .await;

        let provider = HttpConfigProvider::new(server.uri(), "gtc_live_x");
        let tenant = TenantContext::new("t", "e");
        let cfg = provider.agent_config(&tenant, "bot").await.unwrap();
        assert_eq!(cfg.agent_id, "bot");
        assert_eq!(cfg.llm.model, "gpt-4o-mini");
        assert_eq!(cfg.tools.len(), 1);
    }

    #[tokio::test]
    async fn maps_404_to_agent_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let provider = HttpConfigProvider::new(server.uri(), "gtc_live_x");
        let tenant = TenantContext::new("t", "e");
        let result = provider.agent_config(&tenant, "ghost").await;
        assert!(matches!(result, Err(ConfigError::AgentNotFound(_))));
    }

    #[tokio::test]
    async fn maps_5xx_to_internal() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let provider = HttpConfigProvider::new(server.uri(), "gtc_live_x");
        let tenant = TenantContext::new("t", "e");
        let result = provider.agent_config(&tenant, "bot").await;
        assert!(matches!(result, Err(ConfigError::Internal(_))));
    }

    #[tokio::test]
    async fn maps_malformed_body_to_misconfigured() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{not json"))
            .mount(&server)
            .await;
        let provider = HttpConfigProvider::new(server.uri(), "gtc_live_x");
        let tenant = TenantContext::new("t", "e");
        let result = provider.agent_config(&tenant, "bot").await;
        assert!(matches!(result, Err(ConfigError::Misconfigured(_))));
    }

    #[tokio::test]
    async fn decode_and_transport_errors_do_not_carry_the_request_url() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{not json"))
            .mount(&server)
            .await;
        let tenant = TenantContext::new("t", "e");
        // No proxy: HTTP(S)_PROXY / ALL_PROXY in the environment must not
        // reroute these loopback requests.
        let direct = || {
            reqwest::Client::builder()
                .no_proxy()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default()
        };
        let provider = HttpConfigProvider::with_client(server.uri(), "gtc_live_x", direct());
        let decode = provider.agent_config(&tenant, "bot").await.unwrap_err();
        let text = decode.to_string();
        assert!(text.contains("agent config decode"), "{text}");
        assert!(!text.contains("/api/v1/designer/agents"), "{text}");
        assert!(!text.contains(&server.uri()), "{text}");

        // Nothing listens on port 1: the send itself fails.
        let provider = HttpConfigProvider::with_client(
            "http://127.0.0.1:1/secret-path",
            "gtc_live_x",
            direct(),
        );
        let send = provider.agent_config(&tenant, "bot").await.unwrap_err();
        let text = send.to_string();
        assert!(text.contains("agent registry request failed"), "{text}");
        assert!(!text.contains("secret-path"), "{text}");
        assert!(!text.contains("/api/v1/designer/agents"), "{text}");
    }

    #[tokio::test]
    async fn maps_auth_rejection_to_misconfigured() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let provider = HttpConfigProvider::new(server.uri(), "gtc_live_bad");
        let tenant = TenantContext::new("t", "e");
        let result = provider.agent_config(&tenant, "bot").await;
        assert!(matches!(result, Err(ConfigError::Misconfigured(_))));
    }

    #[tokio::test]
    async fn an_agent_id_cannot_steer_the_request_path() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let provider = HttpConfigProvider::new(server.uri(), "tok");
        let tenant = TenantContext::new("t", "e");
        let err = provider
            .agent_config(&tenant, "../tenant/me/mcp-servers")
            .await
            .unwrap_err();
        assert!(matches!(err, ConfigError::AgentNotFound(_)));
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].url.path(),
            "/api/v1/designer/agents/..%2Ftenant%2Fme%2Fmcp-servers"
        );
        // A bare `..` is refused without any request being made.
        let err = provider.agent_config(&tenant, "..").await.unwrap_err();
        assert!(matches!(err, ConfigError::AgentNotFound(_)));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}
