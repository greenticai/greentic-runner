//! Typed, versioned reader for the infrastructure connections the designer
//! stages into an environment's `op` store.
//!
//! The designer writes ONE JSON document per resource kind at
//! `secrets://default/<tenant>/_/infra/<canonical kind>` (category `infra`,
//! env segment pinned to `default`, no unit scope). This module reads it,
//! validates it with the same rules the admin and designer apply, and
//! re-emits a host-normalised, versioned document for the guest
//! (`to_guest_document`). See `docs/infra-connection-v1.md`.
//!
//! Credential hygiene: the credential is a [`SecretString`] from the moment it
//! is validated, `Debug` redacts it, and every parse failure is reported as a
//! fixed field name, never as serde's message (serde echoes offending values).

use std::collections::BTreeMap;
use std::net::Ipv6Addr;

use greentic_secrets_lib::{SecretError, SecretsManager};
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;

use crate::scoped_secrets::{TENANT_DEFAULT_TEAM, secret_uri};

/// Secret category the designer stages connections under.
pub const CATEGORY: &str = "infra";
/// Version of the guest-facing document this runtime emits and the highest
/// staged version it reads.
pub const SCHEMA_VERSION: u32 = 1;
/// Tenant every remote workload carries when nothing else is configured.
pub const REMOTE_TENANT: &str = "default";
/// Guest-facing URI prefix understood by `host.secrets.get`.
pub const URI_PREFIX: &str = "secret://infra/";

const HOST_MAX: usize = 253;
const SCHEME_MAX: usize = 16;
const CREDENTIAL_MAX: usize = 4096;
const EXTRAS_MAX: usize = 16;
const EXTRA_ITEM_MAX: usize = 256;

/// Why a connection could not be produced. Messages never contain a value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InfraConnectionError {
    #[error(
        "infra resource '{kind}' is not connected to this deployment (looked at {tried}); add it in workspace settings and deploy again"
    )]
    Missing { kind: String, tried: String },
    #[error("infra resource '{kind}' could not be read from the secret store: {reason}")]
    Unreadable { kind: String, reason: String },
    #[error("infra resource '{kind}' is malformed: {field}")]
    Invalid { kind: String, field: &'static str },
    #[error(
        "infra resource '{kind}' uses schema version {found}, this runtime reads up to {supported}"
    )]
    UnsupportedVersion {
        kind: String,
        found: u32,
        supported: u32,
    },
    #[error(
        "infra resource '{kind}' has no '{key}' setting; refusing to use an unscoped namespace"
    )]
    MissingExtra { kind: String, key: String },
    #[error("infra kind '{0}' is not a valid kind")]
    BadKind(String),
}

/// Canonical secret name of a kind (`vector-store/qdrant` -> `vector_store_qdrant`).
/// Idempotent.
pub fn canonical_name(kind: &str) -> String {
    greentic_secrets_lib::spec::canonical_secret_name(kind)
}

/// Friendly label for error text only; the runtime has no kind allow-list.
pub fn kind_label(name_or_kind: &str) -> String {
    match canonical_name(name_or_kind).as_str() {
        "vector_store_qdrant" => "Qdrant".to_string(),
        "cache_redis" => "Redis".to_string(),
        _ => name_or_kind.to_string(),
    }
}

/// Store URIs tried for a kind: the served tenant first, then `default`.
/// The env segment is pinned to `default`, never the flow env.
pub fn uri_candidates(tenant: &str, kind: &str) -> Result<Vec<String>, InfraConnectionError> {
    if kind.trim().is_empty() {
        return Err(InfraConnectionError::BadKind(kind.to_string()));
    }
    let name = canonical_name(kind);
    // `canonical_secret_name` maps empty or all-dropped input (`""`, `"***"`,
    // `".."`) to the literal "secret", which would read `infra/secret`. Accept
    // that name only when the caller really asked for "secret".
    if name.is_empty() || (name == "secret" && !kind.trim().eq_ignore_ascii_case("secret")) {
        return Err(InfraConnectionError::BadKind(kind.to_string()));
    }
    let mut tenants = vec![tenant];
    if tenant != REMOTE_TENANT {
        tenants.push(REMOTE_TENANT);
    }
    tenants
        .into_iter()
        .map(|t| {
            secret_uri(CATEGORY, t, Some(TENANT_DEFAULT_TEAM), &name)
                .map_err(|_| InfraConnectionError::BadKind(kind.to_string()))
        })
        .collect()
}

/// Does this URI address the infra category at all? True for any URI whose
/// first segment after `secret://` is `infra` (case-insensitive), well formed
/// or not, so a malformed request is refused by name instead of falling
/// through to the store scope or the env-var fallback.
pub fn is_infra_uri(uri: &str) -> bool {
    uri.strip_prefix("secret://")
        .map(|rest| rest.split('/').next().unwrap_or(""))
        .is_some_and(|first| first.eq_ignore_ascii_case(CATEGORY))
}

/// Error text for an infra-category URI that is not `secret://infra/<name>`.
pub fn malformed_uri_message(uri: &str) -> String {
    format!(
        "infra resource request '{uri}' is malformed; expected {URI_PREFIX}<name>. Fix the extension's secret declaration and deploy again"
    )
}

/// `secret://infra/<name>` -> `<name>`. Nested paths and empty names refused.
pub fn kind_from_guest_uri(uri: &str) -> Option<&str> {
    let name = uri.strip_prefix(URI_PREFIX)?;
    if name.is_empty() || name.contains('/') {
        None
    } else {
        Some(name)
    }
}

/// A validated connection. The credential is held as a [`SecretString`].
pub struct InfraConnection {
    kind: String,
    host: String,
    port: u16,
    scheme: String,
    credential: SecretString,
    extras: BTreeMap<String, String>,
}

impl std::fmt::Debug for InfraConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfraConnection")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("scheme", &self.scheme)
            .field("credential", &"<redacted>")
            .field("extras", &self.extras.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn valid_host(host: &str) -> bool {
    if host.is_empty() || host.len() > HOST_MAX {
        return false;
    }
    if host.contains(':') {
        return host.parse::<Ipv6Addr>().is_ok();
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    })
}

impl InfraConnection {
    /// Parse the staged document (or this module's own guest document).
    pub fn parse(kind: &str, bytes: &[u8]) -> Result<Self, InfraConnectionError> {
        let invalid = |field: &'static str| InfraConnectionError::Invalid {
            kind: kind.to_string(),
            field,
        };
        let doc: Value = serde_json::from_slice(bytes).map_err(|e| {
            tracing::debug!(class = ?e.classify(), "infra connection document is not JSON");
            invalid("document")
        })?;
        let Value::Object(mut map) = doc else {
            return Err(invalid("document"));
        };

        if let Some(v) = map.get("v") {
            let found = v
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .filter(|n| *n >= 1)
                .ok_or_else(|| invalid("v"))?;
            if found > SCHEMA_VERSION {
                return Err(InfraConnectionError::UnsupportedVersion {
                    kind: kind.to_string(),
                    found,
                    supported: SCHEMA_VERSION,
                });
            }
        }

        let host = match map.get("host") {
            Some(Value::String(h)) if valid_host(h) => h.clone(),
            _ => return Err(invalid("host")),
        };
        let port = match map.get("port").and_then(Value::as_u64) {
            Some(p) if (1..=65535).contains(&p) => p as u16,
            _ => return Err(invalid("port")),
        };
        let scheme = match map.get("scheme") {
            None => "https".to_string(),
            Some(Value::String(s))
                if !s.is_empty()
                    && s.len() <= SCHEME_MAX
                    && s.bytes().all(|b| b.is_ascii_alphanumeric()) =>
            {
                s.clone()
            }
            _ => return Err(invalid("scheme")),
        };
        let credential = match map.remove("credential") {
            // Residual-copy risk accepted: the parsed `Value` and the input
            // bytes briefly hold the credential un-zeroized.
            Some(Value::String(c)) if !c.is_empty() && c.len() <= CREDENTIAL_MAX => {
                SecretString::from(c)
            }
            _ => return Err(invalid("credential")),
        };
        let mut extras = BTreeMap::new();
        match map.get("extras") {
            None | Some(Value::Null) => {}
            Some(Value::Object(obj)) => {
                if obj.len() > EXTRAS_MAX {
                    return Err(invalid("extras"));
                }
                for (k, v) in obj {
                    match v {
                        Value::String(s)
                            if k.len() <= EXTRA_ITEM_MAX && s.len() <= EXTRA_ITEM_MAX =>
                        {
                            extras.insert(k.clone(), s.clone());
                        }
                        _ => return Err(invalid("extras")),
                    }
                }
            }
            Some(_) => return Err(invalid("extras")),
        }
        Ok(Self {
            kind: kind.to_string(),
            host,
            port,
            scheme,
            credential,
            extras,
        })
    }

    /// Host without IPv6 brackets.
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// `scheme://host:port`, IPv6 bracketed.
    pub fn base_url(&self) -> String {
        let scheme = self.scheme.to_ascii_lowercase();
        if self.host.contains(':') {
            format!("{scheme}://[{}]:{}", self.host, self.port)
        } else {
            format!("{scheme}://{}:{}", self.host, self.port)
        }
    }

    pub fn credential(&self) -> &SecretString {
        &self.credential
    }

    pub fn extra(&self, key: &str) -> Option<&str> {
        self.extras.get(key).map(String::as_str)
    }

    /// Like [`Self::extra`] but a blank or absent value is an error, so an
    /// extension fails closed instead of using an unscoped namespace.
    pub fn require_extra(&self, key: &str) -> Result<&str, InfraConnectionError> {
        match self.extra(key) {
            Some(v) if !v.trim().is_empty() => Ok(v),
            _ => Err(InfraConnectionError::MissingExtra {
                kind: self.kind.clone(),
                key: key.to_string(),
            }),
        }
    }

    /// The normalised v1 document handed to the guest.
    pub fn to_guest_document(&self, kind: &str) -> SecretString {
        let doc = serde_json::json!({
            "v": SCHEMA_VERSION,
            "kind": kind,
            "host": self.host,
            "port": self.port,
            "scheme": self.scheme,
            "base_url": self.base_url(),
            "credential": self.credential.expose_secret(),
            "extras": self.extras,
        });
        SecretString::from(doc.to_string())
    }
}

/// Read and validate the connection for `kind` as served to `tenant`.
pub async fn read(
    secrets: &dyn SecretsManager,
    tenant: &str,
    kind: &str,
) -> Result<InfraConnection, InfraConnectionError> {
    let uris = uri_candidates(tenant, kind)?;
    let mut hard_error = false;
    for uri in &uris {
        match secrets.read(uri).await {
            Ok(bytes) => return InfraConnection::parse(kind, &bytes),
            Err(SecretError::NotFound(_)) => {}
            Err(_) => hard_error = true,
        }
    }
    if hard_error {
        Err(InfraConnectionError::Unreadable {
            kind: kind.to_string(),
            reason: "secret store error".to_string(),
        })
    } else {
        Err(InfraConnectionError::Missing {
            kind: kind.to_string(),
            tried: uris.join(", "),
        })
    }
}

#[cfg(test)]
mod tests;
