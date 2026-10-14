//! How a route's sidecar record and its sealed secret combine into either a
//! static token or an OAuth client-credentials grant.

use std::sync::Arc;

use secrecy::SecretString;

use super::AUTH_MODE_OAUTH_CLIENT_CREDENTIALS;
use super::provider::{OAuthError, TokenProvider};

/// What a route's `auth_mode` selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// The stored secret is the token, sent as a bearer or under the route's
    /// own header name. This is every route written before `auth_mode`
    /// existed, and the explicit `"bearer"` / `"header"` values.
    Static,
    /// The stored secret is `{"client_id","client_secret"}`; a token is
    /// fetched from the route's `oauth_endpoint`.
    OAuthClientCredentials,
}

/// Parse a sidecar `auth_mode`. Absent or blank means [`AuthMode::Static`]
/// (today's behaviour). Anything unrecognised is an error that fails the
/// route closed: guessing would send a credential in a way the operator did
/// not choose.
pub fn parse_auth_mode(raw: Option<&str>) -> Result<AuthMode, String> {
    match raw.map(str::trim) {
        None | Some("") | Some("bearer") | Some("header") => Ok(AuthMode::Static),
        Some(AUTH_MODE_OAUTH_CLIENT_CREDENTIALS) => Ok(AuthMode::OAuthClientCredentials),
        Some(other) => {
            let shown: String = other.chars().filter(|c| !c.is_control()).take(40).collect();
            Err(format!(
                "unknown auth_mode '{shown}'; this runner understands bearer, header and \
                 {AUTH_MODE_OAUTH_CLIENT_CREDENTIALS}. The route is refused rather than \
                 sending its credential in a way that was not chosen"
            ))
        }
    }
}

/// The OAuth client id and secret, parsed from the sealed JSON secret.
#[derive(Clone)]
pub struct ClientCredentials {
    pub client_id: String,
    pub client_secret: SecretString,
}

impl std::fmt::Debug for ClientCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientCredentials")
            .field("client_id", &self.client_id)
            .field("client_secret", &"[REDACTED]")
            .finish()
    }
}

impl ClientCredentials {
    /// Parse `{"client_id": "...", "client_secret": "..."}`. Error text never
    /// echoes any part of the input.
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| "the stored client credentials are not valid UTF-8".to_string())?;
        let value: serde_json::Value = serde_json::from_str(text.trim()).map_err(|_| {
            "the stored client credentials are not JSON; expected \
             {\"client_id\":...,\"client_secret\":...}"
                .to_string()
        })?;
        let field = |name: &str| {
            value
                .get(name)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string)
                .ok_or_else(|| format!("the stored client credentials have no non-empty {name}"))
        };
        Ok(Self {
            client_id: field("client_id")?,
            client_secret: SecretString::from(field("client_secret")?),
        })
    }
}

/// The non-secret sidecar parameters of a route, as borrowed from whichever
/// record type carries them.
#[derive(Debug, Clone, Copy, Default)]
pub struct RouteAuthSpec<'a> {
    pub auth_mode: Option<&'a str>,
    pub oauth_endpoint: Option<&'a str>,
    pub oauth_scope: Option<&'a str>,
    pub oauth_audience: Option<&'a str>,
}

/// Everything a route needs to obtain a token at call time.
#[derive(Clone)]
pub struct RouteOAuth {
    key: String,
    endpoint: String,
    scope: Option<String>,
    audience: Option<String>,
    credentials: ClientCredentials,
    provider: Arc<TokenProvider>,
}

impl std::fmt::Debug for RouteOAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteOAuth")
            .field("key", &self.key)
            .field("endpoint", &self.endpoint)
            .field("scope", &self.scope)
            .field("audience", &self.audience)
            .field("credentials", &self.credentials)
            .finish()
    }
}

impl RouteOAuth {
    /// A fresh or cached access token for this route.
    pub async fn token(&self) -> Result<SecretString, OAuthError> {
        self.provider
            .token_for(
                &self.key,
                &self.endpoint,
                self.scope.as_deref(),
                self.audience.as_deref(),
                &self.credentials.client_id,
                &self.credentials.client_secret,
            )
            .await
    }

    /// The resource answered 401 to the token: drop it so the retry fetches a
    /// new one.
    pub async fn invalidate(&self) {
        self.provider.invalidate(&self.key).await;
    }

    /// The same grant served by another provider (tests inject one that may
    /// reach a loopback stub).
    #[must_use]
    pub fn with_provider(mut self, provider: Arc<TokenProvider>) -> Self {
        self.provider = provider;
        self
    }

    /// The cache key, for logs and tests. Not secret.
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// How a route authenticates, once its record and secret are combined.
#[derive(Debug, Clone)]
pub enum ResolvedAuth {
    /// The stored secret is the token (or there is none).
    Static { token: Option<String> },
    /// A token is fetched per call.
    OAuth(RouteOAuth),
}

/// `tenant/team/unit/category/id`, joined with a unit separator so no id can
/// forge another route's key. The cache key of the [`TokenProvider`].
pub fn route_token_key(
    category: &str,
    tenant: &str,
    team: Option<&str>,
    unit: Option<&str>,
    id: &str,
) -> String {
    [
        tenant,
        team.filter(|t| !t.is_empty()).unwrap_or("_"),
        unit.filter(|u| !u.is_empty()).unwrap_or("_"),
        category,
        id,
    ]
    .join("\u{1f}")
}

/// Combine a record's non-secret parameters with the secret read for it.
///
/// - [`AuthMode::Static`]: the secret is the token. A secret that is plainly
///   an OAuth client-credentials JSON is refused with an explicit message
///   instead of being sent as a token.
/// - [`AuthMode::OAuthClientCredentials`]: needs `oauth_endpoint` and a
///   parseable credentials secret; a missing secret is an error, never an
///   unauthenticated call.
///
/// `secret` is the UTF-8 text of what the secrets store returned, `None` when
/// this route reads none (a `local-wasm` route, or no secrets manager).
pub fn resolve_route_auth(
    spec: &RouteAuthSpec<'_>,
    secret: Option<&str>,
    key: String,
    provider: Arc<TokenProvider>,
) -> Result<ResolvedAuth, String> {
    match parse_auth_mode(spec.auth_mode)? {
        AuthMode::Static => {
            if let Some(secret) = secret
                && looks_like_client_credentials(secret)
            {
                return Err(
                    "the stored secret holds OAuth client credentials but the route's \
                     auth_mode is not oauth_client_credentials; refusing to send it as a token"
                        .to_string(),
                );
            }
            Ok(ResolvedAuth::Static {
                token: secret.map(str::to_string),
            })
        }
        AuthMode::OAuthClientCredentials => {
            let endpoint = spec
                .oauth_endpoint
                .map(str::trim)
                .filter(|e| !e.is_empty())
                .ok_or_else(|| {
                    "auth_mode oauth_client_credentials needs an oauth_endpoint".to_string()
                })?;
            let parsed = url::Url::parse(endpoint)
                .map_err(|_| "oauth_endpoint is not a valid URL".to_string())?;
            if !matches!(parsed.scheme(), "https" | "http") {
                return Err("oauth_endpoint must be an https URL".to_string());
            }
            let secret = secret.ok_or_else(|| {
                "auth_mode oauth_client_credentials needs stored client credentials, but none \
                 could be read"
                    .to_string()
            })?;
            let credentials = ClientCredentials::from_secret_bytes(secret.as_bytes())?;
            let clean = |v: Option<&str>| {
                v.map(str::trim)
                    .filter(|v| !v.is_empty() && !v.chars().any(char::is_control))
                    .map(str::to_string)
            };
            Ok(ResolvedAuth::OAuth(RouteOAuth {
                key,
                endpoint: endpoint.to_string(),
                scope: clean(spec.oauth_scope),
                audience: clean(spec.oauth_audience),
                credentials,
                provider,
            }))
        }
    }
}

fn looks_like_client_credentials(secret: &str) -> bool {
    let trimmed = secret.trim();
    trimmed.starts_with('{')
        && serde_json::from_str::<serde_json::Value>(trimmed)
            .ok()
            .is_some_and(|v| v.get("client_secret").is_some())
}
