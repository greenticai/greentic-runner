//! [`TokenProvider`]: fetch, cache and single-flight `client_credentials`
//! access tokens.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use dashmap::DashMap;
use futures::StreamExt;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderValue};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use super::cache::Slot;

/// Budget for the whole token request (connect, TLS, response).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// A token endpoint answer larger than this is refused unread.
const MAX_BODY_BYTES: usize = 64 * 1024;
/// A token longer than this cannot be a sane header value.
const MAX_TOKEN_BYTES: usize = 8 * 1024;
/// Shortest and longest lifetime we will honour, whatever the endpoint says.
const MIN_LIFETIME: Duration = Duration::from_secs(60);
const MAX_LIFETIME: Duration = Duration::from_secs(3600);
/// Used when the endpoint omits `expires_in`.
const DEFAULT_LIFETIME: Duration = Duration::from_secs(300);
/// A token is replaced this long before it expires.
const SKEW: Duration = Duration::from_secs(45);
/// After a failed exchange the endpoint is left alone for this long.
const NEGATIVE_TTL: Duration = Duration::from_secs(15);

/// Why a token could not be had. Never carries the client secret, an access
/// token, or the endpoint's response body.
#[derive(Debug, Clone, thiserror::Error)]
pub enum OAuthError {
    /// The endpoint, or where it resolves, is not one we will send a client
    /// secret to.
    #[error("oauth token endpoint refused: {0}")]
    EndpointRefused(String),
    /// The request could not be completed (connect, TLS, timeout, read).
    #[error("oauth token request failed: {0}")]
    Transport(String),
    /// The endpoint answered with a non-success status. `code` is the
    /// RFC 6749 `error` value when it was a short, plain token.
    #[error("oauth token endpoint answered HTTP {status}{}", code_suffix(.code))]
    Status { status: u16, code: Option<String> },
    /// The answer was not a usable token response.
    #[error("oauth token response unusable: {0}")]
    Malformed(String),
    /// A recent exchange for the same inputs failed; not retried yet.
    #[error("oauth token endpoint failed recently and is not being retried yet: {0}")]
    Backoff(String),
}

fn code_suffix(code: &Option<String>) -> String {
    match code {
        Some(code) => format!(" ({code})"),
        None => String::new(),
    }
}

/// Exchanges client credentials for access tokens and caches them per route.
pub struct TokenProvider {
    slots: DashMap<String, Arc<Mutex<Option<Slot>>>>,
    clock: Arc<dyn Fn() -> Instant + Send + Sync>,
    allow_loopback_http: AtomicBool,
}

impl std::fmt::Debug for TokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenProvider")
            .field("cached_keys", &self.slots.len())
            .finish_non_exhaustive()
    }
}

impl Default for TokenProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl TokenProvider {
    pub fn new() -> Self {
        Self::with_clock(Arc::new(Instant::now))
    }

    pub(crate) fn with_clock(clock: Arc<dyn Fn() -> Instant + Send + Sync>) -> Self {
        Self {
            slots: DashMap::new(),
            clock,
            allow_loopback_http: AtomicBool::new(false),
        }
    }

    /// The process-wide provider routes use unless one is injected.
    pub fn shared() -> Arc<TokenProvider> {
        static SHARED: OnceLock<Arc<TokenProvider>> = OnceLock::new();
        Arc::clone(SHARED.get_or_init(|| Arc::new(TokenProvider::new())))
    }

    /// TEST HOOK. Lets this provider talk plain `http` to a loopback address,
    /// which a local stub server needs. Production code never calls this, and
    /// nothing reads it from the environment, so a deployed runner cannot be
    /// talked into it.
    pub fn allow_loopback_http_for_tests(&self) {
        self.allow_loopback_http.store(true, Ordering::SeqCst);
    }

    /// The access token for `key`, fetching one only when none is cached.
    ///
    /// `key` identifies the route (tenant, team, unit and id). The cache also
    /// remembers a digest of the endpoint, scope, audience and credentials the
    /// token was fetched with, so a rotated secret takes effect on the next
    /// call instead of after the old token expires.
    ///
    /// Concurrent calls for one key share a single request.
    pub async fn token_for(
        &self,
        key: &str,
        endpoint: &str,
        scope: Option<&str>,
        audience: Option<&str>,
        client_id: &str,
        client_secret: &SecretString,
    ) -> Result<SecretString, OAuthError> {
        let fingerprint = fingerprint(endpoint, scope, audience, client_id, client_secret);
        let slot = self
            .slots
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(None)))
            .clone();
        let mut guard = slot.lock().await;

        let now = (self.clock)();
        if let Some(held) = guard.as_ref() {
            if let Some(token) = held.live_token(&fingerprint, now) {
                return Ok(token.clone());
            }
            if let Some(error) = held.recent_failure(&fingerprint, now) {
                return Err(OAuthError::Backoff(error.to_string()));
            }
        }

        let result = self
            .exchange(endpoint, scope, audience, client_id, client_secret)
            .await;
        let now = (self.clock)();
        match result {
            Ok((token, lifetime)) => {
                *guard = Some(Slot::Token {
                    fingerprint,
                    token: token.clone(),
                    valid_until: now + lifetime.saturating_sub(SKEW),
                });
                Ok(token)
            }
            Err(error) => {
                // A refusal is deterministic and costs nothing to repeat; only
                // failures that involved the network are held back.
                if !matches!(error, OAuthError::EndpointRefused(_)) {
                    *guard = Some(Slot::Failed {
                        fingerprint,
                        until: now + NEGATIVE_TTL,
                        error: error.clone(),
                    });
                }
                Err(error)
            }
        }
    }

    /// Forget the cached token for `key`: the resource rejected it (401), so
    /// the next [`token_for`](Self::token_for) fetches a new one. Callers retry
    /// exactly once.
    pub async fn invalidate(&self, key: &str) {
        let slot = self.slots.get(key).map(|entry| Arc::clone(entry.value()));
        if let Some(slot) = slot {
            *slot.lock().await = None;
        }
    }

    async fn exchange(
        &self,
        endpoint: &str,
        scope: Option<&str>,
        audience: Option<&str>,
        client_id: &str,
        client_secret: &SecretString,
    ) -> Result<(SecretString, Duration), OAuthError> {
        let (url, pinned) = self.vet_endpoint(endpoint).await?;

        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(REQUEST_TIMEOUT);
        if let (Some(host), Some(addrs)) = (url.host_str(), pinned.as_deref()) {
            // Connect to the addresses we just vetted, not to whatever the
            // name resolves to a moment later.
            builder = builder.resolve_to_addrs(host, addrs);
        }
        let client = builder
            .build()
            .map_err(|e| OAuthError::Transport(scrub(&e)))?;

        let mut form: Vec<(&str, &str)> = vec![("grant_type", "client_credentials")];
        if let Some(scope) = scope.filter(|s| !s.is_empty()) {
            form.push(("scope", scope));
        }
        if let Some(audience) = audience.filter(|a| !a.is_empty()) {
            form.push(("audience", audience));
        }

        let response = client
            .post(url)
            .header(AUTHORIZATION, basic_header(client_id, client_secret)?)
            .header(ACCEPT, "application/json")
            .form(&form)
            .send()
            .await
            .map_err(|e| OAuthError::Transport(scrub(&e)))?;

        let status = response.status();
        let body = read_capped(response).await?;
        if !status.is_success() {
            return Err(OAuthError::Status {
                status: status.as_u16(),
                code: error_code(&body),
            });
        }
        parse_token_response(&body)
    }

    /// Scheme, userinfo and address checks. Returns the parsed URL and, for a
    /// named host, the addresses to pin the connection to.
    async fn vet_endpoint(
        &self,
        endpoint: &str,
    ) -> Result<(url::Url, Option<Vec<SocketAddr>>), OAuthError> {
        let url = url::Url::parse(endpoint)
            .map_err(|_| refused("the endpoint is not a valid URL".to_string()))?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(refused("the endpoint carries userinfo".to_string()));
        }
        let host = url
            .host()
            .ok_or_else(|| refused("the endpoint has no host".to_string()))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| refused("the endpoint has no port".to_string()))?;
        let loopback_ok = self.allow_loopback_http.load(Ordering::SeqCst);

        let (addrs, literal) = match host {
            url::Host::Ipv4(ip) => (vec![SocketAddr::new(IpAddr::V4(ip), port)], true),
            url::Host::Ipv6(ip) => (vec![SocketAddr::new(IpAddr::V6(ip), port)], true),
            url::Host::Domain(name) => {
                // `localhost` is a name for loopback; resolve it like any other
                // so the address check decides.
                let resolved: Vec<SocketAddr> = tokio::net::lookup_host((name, port))
                    .await
                    .map_err(|_| refused(format!("the host '{name}' did not resolve")))?
                    .collect();
                if resolved.is_empty() {
                    return Err(refused(format!("the host '{name}' did not resolve")));
                }
                (resolved, false)
            }
        };

        let all_loopback = addrs.iter().all(|a| a.ip().is_loopback());
        match url.scheme() {
            "https" => {}
            "http" if loopback_ok && all_loopback => {}
            "http" => return Err(refused("only https endpoints are allowed".to_string())),
            other => return Err(refused(format!("scheme '{other}' is not allowed"))),
        }
        for addr in &addrs {
            if is_forbidden(addr.ip()) && !(loopback_ok && addr.ip().is_loopback()) {
                return Err(refused(
                    "the endpoint resolves to a private, loopback or link-local address"
                        .to_string(),
                ));
            }
        }
        Ok((url, (!literal).then_some(addrs)))
    }
}

fn refused(reason: String) -> OAuthError {
    OAuthError::EndpointRefused(reason)
}

/// A transport error with the URL stripped (it names the endpoint, which is
/// fine, but may carry a query string we did not write).
fn scrub(error: &reqwest::Error) -> String {
    let kind = if error.is_timeout() {
        "timed out"
    } else if error.is_connect() {
        "could not connect"
    } else if error.is_redirect() {
        "redirects are not followed"
    } else if error.is_body() || error.is_decode() {
        "the response could not be read"
    } else {
        "request error"
    };
    kind.to_string()
}

fn basic_header(client_id: &str, client_secret: &SecretString) -> Result<HeaderValue, OAuthError> {
    // RFC 6749 section 2.3.1: both halves are form-urlencoded before the
    // Basic encoding, so a `:` or `%` in either cannot shift the split.
    let enc = |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>();
    let raw = format!("{}:{}", enc(client_id), enc(client_secret.expose_secret()));
    let mut value = HeaderValue::from_str(&format!("Basic {}", BASE64.encode(raw)))
        .map_err(|_| OAuthError::Malformed("the client credentials are not encodable".into()))?;
    value.set_sensitive(true);
    Ok(value)
}

async fn read_capped(response: reqwest::Response) -> Result<Vec<u8>, OAuthError> {
    if response
        .content_length()
        .is_some_and(|len| len > MAX_BODY_BYTES as u64)
    {
        return Err(OAuthError::Malformed(
            "the response is larger than the allowed size".into(),
        ));
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| OAuthError::Transport(scrub(&e)))?;
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(OAuthError::Malformed(
                "the response is larger than the allowed size".into(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The RFC 6749 `error` value, when it is a short plain token. Anything else
/// in the body is dropped: it is the endpoint's free text and may echo input.
fn error_code(body: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let code = value.get("error")?.as_str()?;
    let plain = !code.is_empty()
        && code.len() <= 64
        && code
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    plain.then(|| code.to_string())
}

fn parse_token_response(body: &[u8]) -> Result<(SecretString, Duration), OAuthError> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| OAuthError::Malformed("the response is not JSON".into()))?;
    let token = value
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| OAuthError::Malformed("the response has no access_token".into()))?;
    if token.is_empty()
        || token.len() > MAX_TOKEN_BYTES
        || token.chars().any(|c| c.is_control() || c.is_whitespace())
    {
        return Err(OAuthError::Malformed(
            "the access_token cannot be sent as a header value".into(),
        ));
    }
    if let Some(kind) = value.get("token_type").and_then(|v| v.as_str())
        && !kind.eq_ignore_ascii_case("bearer")
    {
        return Err(OAuthError::Malformed("the token_type is not Bearer".into()));
    }
    let seconds = match value.get("expires_in") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.trim().parse::<f64>().ok(),
        Some(_) => None,
    };
    let lifetime = seconds
        .filter(|s| s.is_finite() && *s >= 0.0)
        .map(Duration::from_secs_f64)
        .unwrap_or(DEFAULT_LIFETIME)
        .clamp(MIN_LIFETIME, MAX_LIFETIME);
    Ok((SecretString::from(token.to_string()), lifetime))
}

fn fingerprint(
    endpoint: &str,
    scope: Option<&str>,
    audience: Option<&str>,
    client_id: &str,
    client_secret: &SecretString,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in [
        endpoint,
        scope.unwrap_or(""),
        audience.unwrap_or(""),
        client_id,
        client_secret.expose_secret(),
    ] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.finalize().into()
}

/// Addresses a client secret must never be sent to.
pub(super) fn is_forbidden(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => forbidden_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(mapped) => forbidden_v4(mapped),
            None => forbidden_v6(v6),
        },
    }
}

fn forbidden_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || a == 0
        || (a == 100 && (64..=127).contains(&b)) // CGNAT 100.64.0.0/10
        || (a == 192 && b == 0 && c == 0) // IETF protocol assignments
        || (a == 198 && (b == 18 || b == 19)) // benchmarking
        || a >= 240 // reserved
}

fn forbidden_v6(ip: Ipv6Addr) -> bool {
    let first = ip.segments()[0];
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (first & 0xfe00) == 0xfc00 // unique local fc00::/7
        || (first & 0xffc0) == 0xfe80 // link-local fe80::/10
}
