use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use axum::extract::{FromRef, FromRequestParts};
use axum::http::header::{AUTHORIZATION, HOST};
use axum::http::request::Parts;
use axum::http::{HeaderName, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use serde_json::json;
use sha2::Sha256;

use crate::http::auth::bearer_from_headers;
use crate::runner::ServerState;
use crate::runtime::TenantRuntime;

/// Tenant a request resolves to when nothing else names one.
///
/// Must equal `greentic_types::DEFAULT_TENANT`. It is duplicated rather than
/// imported because this crate is transitively pinned to greentic-types
/// `=1.1.2` by `greentic-ext-runtime =1.2.24`, so the floor cannot move until
/// that is republished. `routing_defaults_to_the_fleet_tenant_not_demo` guards
/// the copy; delete it in favour of the import once the pin is free.
///
/// Was `demo`, which disagreed with greentic-deployer and greentic-setup — the
/// host served requests in one namespace and read secrets from another.
pub const DEFAULT_TENANT: &str = "default";

#[derive(Clone)]
pub struct RoutingConfig {
    pub resolver: TenantResolver,
    pub default_tenant: String,
}

impl RoutingConfig {
    pub fn from_env() -> Self {
        Self::from_env_with_default(DEFAULT_TENANT.into())
    }

    pub fn from_env_with_default(default_tenant: String) -> Self {
        let default_tenant = std::env::var("DEFAULT_TENANT").unwrap_or(default_tenant);
        let resolver = match std::env::var("TENANT_RESOLVER") {
            Err(_) => TenantResolver::Env,
            Ok(value) => TenantResolver::from_str(&value, &default_tenant).unwrap_or_else(|err| {
                // Fail closed: an unknown mode must not silently degrade to
                // the single-tenant `env` mode. Every request is rejected
                // until the configuration is fixed.
                tracing::error!(error = %err, "invalid TENANT_RESOLVER, rejecting all tenant-routed requests");
                TenantResolver::Invalid(err.to_string())
            }),
        };
        Self {
            resolver,
            default_tenant,
        }
    }

    /// Fail startup on a configuration that must not run: `jwt` mode with no
    /// verification key and no explicit gateway opt-in, or an unsupported
    /// `TENANT_RESOLVER` value.
    pub fn startup_check(&self) -> Result<()> {
        match &self.resolver {
            TenantResolver::JwtUnconfigured(reason) => bail!("{reason}"),
            // A typo in TENANT_RESOLVER must not boot a host that rejects
            // every tenant-routed request.
            TenantResolver::Invalid(reason) => bail!("{reason}"),
            _ => Ok(()),
        }
    }

    /// Strict variant of [`Self::from_env_with_default`]: an invalid
    /// `TENANT_RESOLVER` value is returned as an error.
    pub fn try_from_env_with_default(default_tenant: String) -> Result<Self> {
        let cfg = Self::from_env_with_default(default_tenant);
        if let TenantResolver::Invalid(reason) = &cfg.resolver {
            bail!("{reason}");
        }
        Ok(cfg)
    }
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            resolver: TenantResolver::Env,
            default_tenant: DEFAULT_TENANT.into(),
        }
    }
}

/// How a request is mapped to a tenant.
///
/// # Trust model
///
/// None of the request-derived modes authenticate the caller:
///
/// * `Header` trusts the tenant header verbatim. It is only safe behind a
///   gateway that strips client-supplied copies and injects the header itself.
/// * `Jwt` decodes the token payload WITHOUT verifying the signature, expiry or
///   audience. It is what `TENANT_JWT_TRUST_GATEWAY=1` selects (and what
///   embedders constructing it directly get): verification is then the
///   upstream gateway's job.
/// * `JwtVerified` (selected from the environment when `TENANT_JWT_SECRET` is
///   set) verifies an HS256 signature, `exp`, `nbf` and optionally `iss`.
/// * `Host` trusts the `Host` header.
#[derive(Clone)]
pub enum TenantResolver {
    Host,
    Header(HeaderName),
    Jwt {
        header: HeaderName,
        claim: String,
    },
    /// Verified JWT mode: HS256 signature + `exp`/`nbf`/`iss` checks.
    JwtVerified {
        header: HeaderName,
        claim: String,
        verifier: JwtVerifier,
    },
    /// `TENANT_RESOLVER=jwt` with neither a verification key nor the explicit
    /// `TENANT_JWT_TRUST_GATEWAY=1` opt-in. Startup fails via
    /// [`RoutingConfig::startup_check`]; requests are rejected meanwhile.
    JwtUnconfigured(String),
    Env,
    /// `TENANT_RESOLVER` held an unsupported value; every request is rejected.
    Invalid(String),
}

/// The caller failed to authenticate (missing, malformed-as-credential,
/// unverifiable or expired bearer token). Carried inside the `anyhow::Error`
/// returned by [`TenantRouting::resolve`] so [`TenantRuntimeHandle`] can answer
/// 401 instead of 400; every other resolve error is malformed routing input
/// (400).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TenantAuthError(pub String);

/// Whether `err` is an authentication failure (401) rather than bad input (400).
pub fn is_auth_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<TenantAuthError>().is_some()
}

/// HTTP status for a [`TenantRouting::resolve`] failure.
pub fn resolve_error_status(err: &anyhow::Error) -> StatusCode {
    if is_auth_error(err) {
        StatusCode::UNAUTHORIZED
    } else {
        StatusCode::BAD_REQUEST
    }
}

/// Clock leeway applied to `exp` / `nbf`.
const JWT_LEEWAY_SECS: f64 = 60.0;

/// HS256 verifier for the `jwt` tenant resolver.
#[derive(Clone)]
pub struct JwtVerifier {
    secret: Vec<u8>,
    issuer: Option<String>,
    /// Accept tokens that carry no `exp` claim. Default `false`: a token that
    /// never expires is rejected. Opt out with `TENANT_JWT_ALLOW_NO_EXP`.
    allow_no_exp: bool,
}

impl JwtVerifier {
    pub fn new(secret: impl Into<Vec<u8>>, issuer: Option<String>) -> Self {
        Self {
            secret: secret.into(),
            issuer: issuer.filter(|v| !v.is_empty()),
            allow_no_exp: false,
        }
    }

    /// Accept tokens without an `exp` claim (`TENANT_JWT_ALLOW_NO_EXP`).
    pub fn with_allow_no_exp(mut self, allow: bool) -> Self {
        self.allow_no_exp = allow;
        self
    }

    /// Verify `token` and return its claims. `now` is unix seconds.
    fn verify(&self, token: &str, now: f64) -> Result<Value> {
        let mut parts = token.split('.');
        let (header_b64, payload_b64, sig_b64) =
            match (parts.next(), parts.next(), parts.next(), parts.next()) {
                (Some(h), Some(p), Some(s), None) => (h, p, s),
                _ => bail!("invalid jwt structure"),
            };
        let header: Value = serde_json::from_slice(&b64url(header_b64)?)?;
        // The configured algorithm is the only one accepted: `none`, RS256
        // (alg confusion against a shared secret) and anything else fail here.
        if header.get("alg").and_then(Value::as_str) != Some("HS256") {
            bail!("unsupported jwt alg");
        }
        let signature = b64url(sig_b64)?;
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&self.secret)
            .map_err(|_| anyhow!("invalid jwt secret"))?;
        mac.update(header_b64.as_bytes());
        mac.update(b".");
        mac.update(payload_b64.as_bytes());
        // `verify_slice` compares in constant time.
        mac.verify_slice(&signature)
            .map_err(|_| anyhow!("jwt signature mismatch"))?;

        let claims: Value = serde_json::from_slice(&b64url(payload_b64)?)?;
        match claims.get("exp") {
            Some(exp) => {
                let exp = exp
                    .as_f64()
                    .ok_or_else(|| anyhow!("jwt exp is not a number"))?;
                if now > exp + JWT_LEEWAY_SECS {
                    bail!("jwt expired");
                }
            }
            None if self.allow_no_exp => {}
            None => bail!("jwt has no exp claim"),
        }
        if let Some(nbf) = claims.get("nbf") {
            let nbf = nbf
                .as_f64()
                .ok_or_else(|| anyhow!("jwt nbf is not a number"))?;
            if now + JWT_LEEWAY_SECS < nbf {
                bail!("jwt not yet valid");
            }
        }
        if let Some(expected) = &self.issuer
            && claims.get("iss").and_then(Value::as_str) != Some(expected.as_str())
        {
            bail!("jwt issuer mismatch");
        }
        Ok(claims)
    }
}

fn b64url(segment: &str) -> Result<Vec<u8>> {
    Ok(URL_SAFE_NO_PAD.decode(segment.trim_end_matches('=').as_bytes())?)
}

fn now_unix() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn env_truthy(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref().map(str::trim),
        Some("1") | Some("true") | Some("yes") | Some("on")
    )
}

impl TenantResolver {
    /// Build the `jwt` resolver from explicit settings (testable without env).
    fn jwt(
        secret: Option<String>,
        issuer: Option<String>,
        trust_gateway: bool,
        allow_no_exp: bool,
    ) -> Self {
        let header = AUTHORIZATION;
        let claim = "tenant".to_string();
        if let Some(secret) = secret.filter(|v| !v.is_empty()) {
            return Self::JwtVerified {
                header,
                claim,
                verifier: JwtVerifier::new(secret.into_bytes(), issuer)
                    .with_allow_no_exp(allow_no_exp),
            };
        }
        if trust_gateway {
            tracing::warn!(
                "TENANT_RESOLVER=jwt with TENANT_JWT_TRUST_GATEWAY=1: bearer tokens are decoded \
                 WITHOUT signature verification; an upstream gateway must verify them"
            );
            return Self::Jwt { header, claim };
        }
        Self::JwtUnconfigured(
            "TENANT_RESOLVER=jwt requires TENANT_JWT_SECRET (HS256) to verify tokens, or \
             TENANT_JWT_TRUST_GATEWAY=1 to explicitly accept tokens already verified upstream"
                .into(),
        )
    }

    fn from_str(value: &str, _default: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "host" => Ok(Self::Host),
            "header" => Ok(Self::Header(HeaderName::from_static("x-greentic-tenant"))),
            "jwt" => Ok(Self::jwt(
                env_nonempty("TENANT_JWT_SECRET"),
                env_nonempty("TENANT_JWT_ISSUER"),
                env_truthy("TENANT_JWT_TRUST_GATEWAY"),
                env_truthy("TENANT_JWT_ALLOW_NO_EXP"),
            )),
            "env" => Ok(Self::Env),
            other => bail!("unsupported TENANT_RESOLVER `{other}`"),
        }
    }
}

#[derive(Clone)]
pub struct TenantRouting {
    resolver: TenantResolver,
    default_tenant: String,
}

impl TenantRouting {
    pub fn new(cfg: RoutingConfig) -> Self {
        Self {
            resolver: cfg.resolver,
            default_tenant: cfg.default_tenant,
        }
    }

    /// Return the configured default tenant identifier.
    pub fn default_tenant(&self) -> &str {
        &self.default_tenant
    }

    pub fn resolve(&self, parts: &Parts) -> Result<String> {
        match &self.resolver {
            TenantResolver::Env => Ok(self.default_tenant.clone()),
            TenantResolver::Host => {
                let host = parts
                    .headers
                    .get(HOST)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default();
                Ok(tenant_from_host(host).unwrap_or_else(|| self.default_tenant.clone()))
            }
            TenantResolver::Header(name) => {
                let tenant = parts
                    .headers
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| self.default_tenant.clone());
                Ok(tenant)
            }
            TenantResolver::Jwt { header, claim } => {
                let token = bearer_from_headers(&parts.headers, header).ok_or_else(|| {
                    anyhow::Error::new(TenantAuthError(
                        "authorization header missing or not a bearer token".into(),
                    ))
                })?;
                let tenant = decode_jwt_claim(&token, claim)
                    .map_err(|err| {
                        tracing::warn!(error = %err, "failed to decode jwt claim");
                        anyhow::Error::new(TenantAuthError("invalid bearer token".into()))
                    })?
                    .ok_or_else(|| anyhow!("bearer token has no `{claim}` claim"))?;
                Ok(tenant)
            }
            TenantResolver::JwtVerified {
                header,
                claim,
                verifier,
            } => {
                let token = bearer_from_headers(&parts.headers, header).ok_or_else(|| {
                    anyhow::Error::new(TenantAuthError(
                        "authorization header missing or not a bearer token".into(),
                    ))
                })?;
                let claims = verifier.verify(&token, now_unix()).map_err(|err| {
                    tracing::warn!(error = %err, "jwt verification failed");
                    anyhow::Error::new(TenantAuthError("invalid bearer token".into()))
                })?;
                claims
                    .get(claim)
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| anyhow!("bearer token has no `{claim}` claim"))
            }
            TenantResolver::JwtUnconfigured(reason) => {
                bail!("tenant resolver misconfigured: {reason}")
            }
            TenantResolver::Invalid(reason) => bail!("tenant resolver misconfigured: {reason}"),
        }
    }
}

/// First DNS label of a `Host` header value, with any port stripped.
fn tenant_from_host(host: &str) -> Option<String> {
    let host = host.trim();
    let name = if let Some(rest) = host.strip_prefix('[') {
        // IPv6 literal: `[::1]:8080`
        rest.split(']').next().unwrap_or_default()
    } else {
        host.split(':').next().unwrap_or_default()
    };
    name.split('.')
        .next()
        .filter(|segment| !segment.is_empty())
        .map(str::to_string)
}

fn decode_jwt_claim(token: &str, claim: &str) -> Result<Option<String>> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow!("invalid jwt structure"))?;
    let bytes = URL_SAFE_NO_PAD.decode(payload.as_bytes()).or_else(|_| {
        let padded = match payload.len() % 4 {
            2 => Some(format!("{payload}==")),
            3 => Some(format!("{payload}=")),
            _ => None,
        };
        if let Some(padded) = padded.as_deref() {
            URL_SAFE
                .decode(padded.as_bytes())
                .or_else(|_| STANDARD.decode(padded.as_bytes()))
        } else {
            URL_SAFE
                .decode(payload.as_bytes())
                .or_else(|_| STANDARD.decode(payload.as_bytes()))
        }
    })?;
    let value: Value = serde_json::from_slice(&bytes)?;
    Ok(value
        .get(claim)
        .and_then(|node| node.as_str())
        .map(|value| value.to_string()))
}

pub struct TenantRuntimeHandle {
    pub tenant: String,
    pub runtime: Arc<TenantRuntime>,
}

impl<S> FromRequestParts<S> for TenantRuntimeHandle
where
    ServerState: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, axum::Json<Value>);

    fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let server_state = ServerState::from_ref(state);
        async move {
            let tenant = server_state.routing.resolve(parts).map_err(|err| {
                // Authentication failures are 401; everything else here is
                // malformed routing input.
                let status = resolve_error_status(&err);
                (status, axum::Json(json!({ "error": err.to_string() })))
            })?;
            let runtime = server_state.active.load_pack(&tenant).ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    axum::Json(json!({ "error": "tenant not loaded" })),
                )
            })?;
            Ok(Self { tenant, runtime })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    #[test]
    fn host_resolver_picks_subdomain() {
        let routing = TenantRouting::new(RoutingConfig {
            resolver: TenantResolver::Host,
            default_tenant: "demo".into(),
        });
        let (parts, _) = Request::builder()
            .uri("http://foo.example.com/webhook")
            .header(HOST, "foo.example.com")
            .body(())
            .unwrap()
            .into_parts();
        let tenant = routing.resolve(&parts).unwrap();
        assert_eq!(tenant, "foo");
    }

    #[test]
    fn header_resolver_defaults() {
        let routing = TenantRouting::new(RoutingConfig {
            resolver: TenantResolver::Header(HeaderName::from_static("x-tenant")),
            default_tenant: "demo".into(),
        });
        let (parts, _) = Request::builder()
            .uri("http://localhost")
            .body(())
            .unwrap()
            .into_parts();
        let tenant = routing.resolve(&parts).unwrap();
        assert_eq!(tenant, "demo");
    }

    /// The tenant a request resolves to when nothing else names one. It was
    /// `demo` while greentic-deployer and greentic-setup bound deployments
    /// under `default`, so the host served requests in one namespace and read
    /// secrets from another.
    #[test]
    fn routing_defaults_to_the_fleet_tenant_not_demo() {
        assert_eq!(RoutingConfig::default().default_tenant, DEFAULT_TENANT);
        assert_ne!(RoutingConfig::default().default_tenant, "demo");
        // Pin the literal too: this is a copy of greentic_types::DEFAULT_TENANT
        // that exists only because the greentic-types floor is pinned.
        assert_eq!(DEFAULT_TENANT, "default");
    }

    #[test]
    fn from_env_with_default_uses_override() {
        let expected = std::env::var("DEFAULT_TENANT").unwrap_or_else(|_| "custom".into());
        let cfg = RoutingConfig::from_env_with_default("custom".into());
        assert_eq!(cfg.default_tenant, expected);
    }

    #[test]
    fn jwt_resolver_reads_tenant_claim() {
        let routing = TenantRouting::new(RoutingConfig {
            resolver: TenantResolver::Jwt {
                header: AUTHORIZATION,
                claim: "tenant".into(),
            },
            default_tenant: "demo".into(),
        });
        let payload = STANDARD.encode(br#"{"tenant":"jwt-tenant"}"#);
        let token = format!("ignored.{payload}.ignored");
        let (parts, _) = Request::builder()
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(())
            .unwrap()
            .into_parts();

        assert_eq!(routing.resolve(&parts).unwrap(), "jwt-tenant");
    }

    #[test]
    fn jwt_resolver_rejects_invalid_payload() {
        let routing = TenantRouting::new(RoutingConfig {
            resolver: TenantResolver::Jwt {
                header: AUTHORIZATION,
                claim: "tenant".into(),
            },
            default_tenant: "demo".into(),
        });
        let (parts, _) = Request::builder()
            .header(AUTHORIZATION, "Bearer invalid.token.payload")
            .body(())
            .unwrap()
            .into_parts();

        assert!(routing.resolve(&parts).is_err());
    }

    #[test]
    fn jwt_resolver_rejects_missing_claim() {
        let routing = TenantRouting::new(RoutingConfig {
            resolver: TenantResolver::Jwt {
                header: AUTHORIZATION,
                claim: "tenant".into(),
            },
            default_tenant: "demo".into(),
        });
        let payload = STANDARD.encode(br#"{"sub":"x"}"#);
        let (parts, _) = Request::builder()
            .header(AUTHORIZATION, format!("bearer a.{payload}.c"))
            .body(())
            .unwrap()
            .into_parts();
        assert!(routing.resolve(&parts).is_err());
    }

    const SECRET: &str = "test-secret";
    const NOW: f64 = 1_800_000_000.0;

    fn sign(header: &str, payload: &str, secret: &str) -> String {
        let h = URL_SAFE_NO_PAD.encode(header.as_bytes());
        let p = URL_SAFE_NO_PAD.encode(payload.as_bytes());
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(format!("{h}.{p}").as_bytes());
        let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        format!("{h}.{p}.{sig}")
    }

    fn hs256(payload: &str) -> String {
        sign(r#"{"alg":"HS256","typ":"JWT"}"#, payload, SECRET)
    }

    fn verifier(issuer: Option<&str>) -> JwtVerifier {
        JwtVerifier::new(SECRET.as_bytes().to_vec(), issuer.map(str::to_string))
    }

    #[test]
    fn jwt_verified_accepts_valid_token() {
        let token = hs256(r#"{"tenant":"acme","exp":1800000600,"nbf":1799999000}"#);
        let claims = verifier(None).verify(&token, NOW).unwrap();
        assert_eq!(claims["tenant"], "acme");
    }

    #[test]
    fn jwt_verified_rejects_bad_signature() {
        let token = sign(r#"{"alg":"HS256"}"#, r#"{"tenant":"acme"}"#, "other-secret");
        assert!(verifier(None).verify(&token, NOW).is_err());
    }

    #[test]
    fn jwt_verified_rejects_expired_beyond_leeway() {
        let token = hs256(r#"{"tenant":"acme","exp":1799999000}"#);
        assert!(verifier(None).verify(&token, NOW).is_err());
        // Within the 60s leeway it is still accepted.
        let token = hs256(r#"{"tenant":"acme","exp":1799999950}"#);
        assert!(verifier(None).verify(&token, NOW).is_ok());
    }

    #[test]
    fn jwt_verified_requires_exp_unless_opted_out() {
        let token = hs256(r#"{"tenant":"acme"}"#);
        let err = verifier(None).verify(&token, NOW).unwrap_err();
        assert!(err.to_string().contains("exp"));
        let lenient = verifier(None).with_allow_no_exp(true);
        assert!(lenient.verify(&token, NOW).is_ok());
        // With a present `exp` the opt-out changes nothing: still enforced.
        let expired = hs256(r#"{"tenant":"acme","exp":1}"#);
        assert!(lenient.verify(&expired, NOW).is_err());
    }

    #[test]
    fn jwt_verified_rejects_not_yet_valid() {
        let token = hs256(r#"{"tenant":"acme","exp":1800009000,"nbf":1800001000}"#);
        assert!(verifier(None).verify(&token, NOW).is_err());
    }

    #[test]
    fn jwt_verified_rejects_wrong_issuer() {
        let token = hs256(r#"{"tenant":"acme","exp":1800000600,"iss":"evil"}"#);
        assert!(verifier(Some("good")).verify(&token, NOW).is_err());
        let token = hs256(r#"{"tenant":"acme","exp":1800000600}"#);
        assert!(verifier(Some("good")).verify(&token, NOW).is_err());
        let token = hs256(r#"{"tenant":"acme","exp":1800000600,"iss":"good"}"#);
        assert!(verifier(Some("good")).verify(&token, NOW).is_ok());
    }

    #[test]
    fn jwt_verified_rejects_alg_none() {
        let h = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
        let p = URL_SAFE_NO_PAD.encode(br#"{"tenant":"acme"}"#);
        assert!(verifier(None).verify(&format!("{h}.{p}."), NOW).is_err());
        // alg=none with a (valid HS256) signature is rejected too.
        let token = sign(r#"{"alg":"none"}"#, r#"{"tenant":"acme"}"#, SECRET);
        assert!(verifier(None).verify(&token, NOW).is_err());
    }

    #[test]
    fn jwt_verified_rejects_alg_confusion() {
        let token = sign(r#"{"alg":"RS256"}"#, r#"{"tenant":"acme"}"#, SECRET);
        assert!(verifier(None).verify(&token, NOW).is_err());
        let token = sign(r#"{"alg":"HS512"}"#, r#"{"tenant":"acme"}"#, SECRET);
        assert!(verifier(None).verify(&token, NOW).is_err());
    }

    #[test]
    fn jwt_resolver_without_key_fails_startup() {
        let resolver = TenantResolver::jwt(None, None, false, false);
        assert!(matches!(resolver, TenantResolver::JwtUnconfigured(_)));
        let cfg = RoutingConfig {
            resolver,
            default_tenant: "demo".into(),
        };
        assert!(cfg.startup_check().is_err());
        let routing = TenantRouting::new(cfg);
        let (parts, _) = Request::builder().body(()).unwrap().into_parts();
        assert!(routing.resolve(&parts).is_err());
    }

    #[test]
    fn jwt_resolver_trust_gateway_opt_in_decodes_only() {
        let resolver = TenantResolver::jwt(None, None, true, false);
        assert!(matches!(resolver, TenantResolver::Jwt { .. }));
        let cfg = RoutingConfig {
            resolver,
            default_tenant: "demo".into(),
        };
        assert!(cfg.startup_check().is_ok());
        let routing = TenantRouting::new(cfg);
        let token = sign(r#"{"alg":"none"}"#, r#"{"tenant":"gw"}"#, "anything");
        let (parts, _) = Request::builder()
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(())
            .unwrap()
            .into_parts();
        assert_eq!(routing.resolve(&parts).unwrap(), "gw");
    }

    #[test]
    fn jwt_resolver_with_secret_verifies_end_to_end() {
        let resolver = TenantResolver::jwt(Some(SECRET.into()), None, false, false);
        let routing = TenantRouting::new(RoutingConfig {
            resolver,
            default_tenant: "demo".into(),
        });
        let good = hs256(r#"{"tenant":"acme","exp":4102444800}"#);
        let forged = sign(r#"{"alg":"HS256"}"#, r#"{"tenant":"acme"}"#, "bad");
        let req = |t: &str| {
            Request::builder()
                .header(AUTHORIZATION, format!("Bearer {t}"))
                .body(())
                .unwrap()
                .into_parts()
                .0
        };
        assert_eq!(routing.resolve(&req(&good)).unwrap(), "acme");
        assert!(routing.resolve(&req(&forged)).is_err());
    }

    #[test]
    fn jwt_verification_failures_are_auth_errors_but_missing_claim_is_not() {
        let routing = TenantRouting::new(RoutingConfig {
            resolver: TenantResolver::jwt(Some(SECRET.into()), None, false, false),
            default_tenant: "demo".into(),
        });
        let req = |auth: Option<String>| {
            let mut b = Request::builder();
            if let Some(a) = auth {
                b = b.header(AUTHORIZATION, a);
            }
            b.body(()).unwrap().into_parts().0
        };
        let forged = sign(
            r#"{"alg":"HS256"}"#,
            r#"{"tenant":"acme","exp":4102444800}"#,
            "bad",
        );
        let err = routing
            .resolve(&req(Some(format!("Bearer {forged}"))))
            .unwrap_err();
        assert!(is_auth_error(&err), "bad signature is a 401");
        assert_eq!(resolve_error_status(&err), StatusCode::UNAUTHORIZED);
        let expired = hs256(r#"{"tenant":"acme","exp":1}"#);
        let err = routing
            .resolve(&req(Some(format!("Bearer {expired}"))))
            .unwrap_err();
        assert!(is_auth_error(&err), "expired is a 401");
        let err = routing.resolve(&req(None)).unwrap_err();
        assert!(is_auth_error(&err), "missing bearer is a 401");
        // A valid token with no tenant claim is a routing problem, not authn.
        let no_claim = hs256(r#"{"sub":"x","exp":4102444800}"#);
        let err = routing
            .resolve(&req(Some(format!("Bearer {no_claim}"))))
            .unwrap_err();
        assert!(!is_auth_error(&err));
        assert_eq!(resolve_error_status(&err), StatusCode::BAD_REQUEST);
        // Misconfiguration stays a non-auth error.
        let invalid = TenantRouting::new(RoutingConfig {
            resolver: TenantResolver::Invalid("x".into()),
            default_tenant: "demo".into(),
        });
        assert!(!is_auth_error(&invalid.resolve(&req(None)).unwrap_err()));
    }

    #[test]
    fn host_resolver_strips_port() {
        assert_eq!(
            tenant_from_host("foo.example.com:8080").as_deref(),
            Some("foo")
        );
        assert_eq!(
            tenant_from_host("localhost:3000").as_deref(),
            Some("localhost")
        );
        assert_eq!(tenant_from_host("[::1]:8080").as_deref(), Some("::1"));
        assert_eq!(tenant_from_host(":8080"), None);
        assert_eq!(tenant_from_host(""), None);
    }

    #[test]
    fn invalid_resolver_fails_startup_check() {
        let cfg = RoutingConfig {
            resolver: TenantResolver::Invalid("unsupported TENANT_RESOLVER `hsot`".into()),
            default_tenant: "demo".into(),
        };
        let err = cfg.startup_check().unwrap_err();
        assert!(err.to_string().contains("hsot"));
        assert!(RoutingConfig::default().startup_check().is_ok());
    }

    #[test]
    fn invalid_resolver_value_is_an_error() {
        assert!(TenantResolver::from_str("bogus", "d").is_err());
        let routing = TenantRouting::new(RoutingConfig {
            resolver: TenantResolver::Invalid("bogus".into()),
            default_tenant: "demo".into(),
        });
        let (parts, _) = Request::builder().body(()).unwrap().into_parts();
        assert!(routing.resolve(&parts).is_err());
    }

    #[test]
    fn jwt_resolver_requires_bearer_prefix() {
        let routing = TenantRouting::new(RoutingConfig {
            resolver: TenantResolver::Jwt {
                header: AUTHORIZATION,
                claim: "tenant".into(),
            },
            default_tenant: "demo".into(),
        });
        let (parts, _) = Request::builder().body(()).unwrap().into_parts();

        assert!(routing.resolve(&parts).is_err());
    }
}
