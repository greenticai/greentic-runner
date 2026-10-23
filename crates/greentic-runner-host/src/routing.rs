use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use axum::extract::{FromRef, FromRequestParts};
use axum::http::header::{AUTHORIZATION, HOST};
use axum::http::request::Parts;
use axum::http::{HeaderName, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use serde_json::Value;
use serde_json::json;

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
///   audience. Signature verification is expected to be performed by an
///   upstream gateway / API manager; do not expose this mode to untrusted
///   networks directly. (Native JWKS verification is a deferred item.)
/// * `Host` trusts the `Host` header.
#[derive(Clone)]
pub enum TenantResolver {
    Host,
    Header(HeaderName),
    Jwt {
        header: HeaderName,
        claim: String,
    },
    Env,
    /// `TENANT_RESOLVER` held an unsupported value; every request is rejected.
    Invalid(String),
}

impl TenantResolver {
    fn from_str(value: &str, _default: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "host" => Ok(Self::Host),
            "header" => Ok(Self::Header(HeaderName::from_static("x-greentic-tenant"))),
            "jwt" => Ok(Self::Jwt {
                header: AUTHORIZATION,
                claim: "tenant".into(),
            }),
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
                let token = bearer_from_headers(&parts.headers, header)
                    .ok_or_else(|| anyhow!("authorization header missing or not a bearer token"))?;
                let tenant = decode_jwt_claim(&token, claim)
                    .map_err(|err| {
                        tracing::warn!(error = %err, "failed to decode jwt claim");
                        anyhow!("invalid bearer token")
                    })?
                    .ok_or_else(|| anyhow!("bearer token has no `{claim}` claim"))?;
                Ok(tenant)
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
                (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({ "error": err.to_string() })),
                )
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
