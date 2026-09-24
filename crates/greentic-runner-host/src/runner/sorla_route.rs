//! Per-SoR route documents: where a SoR is and how to authenticate to it.
//!
//! The document lives in the secrets manager, never in a pack:
//! `secrets://default/<tenant>/<team|_>/sorla/<sor>` =
//! `{"url": …, "token": …?, "tenant": …}`. It is read on every call, so a SoR
//! that moved (a restarted local child on a new port, a redeployed service)
//! is followed without rebuilding anything.

use serde::Deserialize;

/// The secrets-manager category every SoR route document is sealed under
/// (see [`greentic_aw_runtime::scoped_secrets`]).
pub const SORLA_CATEGORY: &str = "sorla";

/// A resolved route to one SoR: where it is, and the bearer token (if any) a
/// call must carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SorlaRouteDoc {
    pub url: String,
    pub token: Option<String>,
    pub tenant: String,
}

/// Why a route document could not be resolved.
///
/// `Missing` carries the URIs that were tried, never a value. `Invalid`
/// carries only a reason — never the raw document, which may hold a token.
/// `Unavailable` means the secrets BACKEND itself failed to answer (denied,
/// unreachable, ...) — never a value either — and must never be treated as
/// "no route document": the caller cannot tell that apart from a SoR that
/// genuinely has no route, and folding the two together either lets NATS win
/// over a SoR that DOES have one, or misreports `sorla_route_missing` for a
/// SoR whose document could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SorlaRouteError {
    Missing(String),
    Invalid(String),
    Unavailable(String),
}

impl std::fmt::Display for SorlaRouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(tried) => write!(f, "no sorla route document ({tried})"),
            Self::Invalid(why) => write!(f, "sorla route document is invalid: {why}"),
            Self::Unavailable(why) => {
                write!(f, "sorla route document could not be read: {why}")
            }
        }
    }
}

impl std::error::Error for SorlaRouteError {}

/// The wire shape of a route document, before blank-string normalisation.
#[derive(Deserialize)]
struct RawDoc {
    url: Option<String>,
    token: Option<String>,
    tenant: Option<String>,
}

/// Resolve the route document for `sor`, scoped to `tenant` (and, when
/// known, `unit` — see [`greentic_aw_runtime::scoped_secrets::secret_uri_candidates`]
/// for the candidate order this tries).
///
/// This does NOT go through
/// [`greentic_aw_runtime::scoped_secrets::read_secret_for_unit`], even though
/// it tries the exact same candidates: that helper folds every read failure
/// — a genuine miss AND a backend failure alike — into one `SecretMiss`,
/// stringified, with no way to tell them apart afterwards. Reading them here
/// instead means every candidate's [`greentic_secrets_lib::SecretError`] is
/// inspected before it is thrown away: only `NotFound` on EVERY candidate is
/// "no route document" ([`SorlaRouteError::Missing`]); anything else
/// (permission denied, a backend that cannot answer at all, ...) is
/// [`SorlaRouteError::Unavailable`] and must never be read as "try NATS
/// instead" — the backend failing to answer says nothing about whether a
/// route document exists.
///
/// An absent or blank `token` resolves to `None`: a call built from this
/// document must never send an empty `Authorization: Bearer ` header.
pub async fn resolve_route(
    secrets: &dyn greentic_secrets_lib::SecretsManager,
    tenant: &str,
    unit: Option<&str>,
    sor: &str,
) -> Result<SorlaRouteDoc, SorlaRouteError> {
    let uris = greentic_aw_runtime::scoped_secrets::secret_uri_candidates(
        SORLA_CATEGORY,
        tenant,
        None,
        unit,
        sor,
    )
    .map_err(|error| SorlaRouteError::Invalid(format!("sor {sor}: {error}")))?;

    let mut bytes: Option<Vec<u8>> = None;
    let mut backend_error: Option<String> = None;
    for uri in &uris {
        match secrets.read(uri).await {
            Ok(b) => {
                bytes = Some(b);
                break;
            }
            Err(greentic_secrets_lib::SecretError::NotFound(_)) => {}
            Err(other) => {
                tracing::debug!(
                    sor = %sor,
                    uri = %uri,
                    error = %other,
                    "sorla: route document read failed (backend error, not a miss)"
                );
                backend_error = Some(other.to_string());
                break;
            }
        }
    }
    let bytes = match (bytes, backend_error) {
        (Some(bytes), _) => bytes,
        (None, Some(error)) => {
            return Err(SorlaRouteError::Unavailable(format!("sor {sor}: {error}")));
        }
        (None, None) => {
            tracing::debug!(sor = %sor, tried = %uris.join(" or "), "sorla: no route document at any scope");
            return Err(SorlaRouteError::Missing(format!(
                "sor {sor}: {}",
                uris.join(" or ")
            )));
        }
    };

    let raw: RawDoc = serde_json::from_slice(&bytes)
        .map_err(|_| SorlaRouteError::Invalid(format!("sor {sor}: not a JSON object")))?;

    let url = raw
        .url
        .map(|u| u.trim().trim_end_matches('/').to_string())
        .filter(|u| !u.is_empty())
        .ok_or_else(|| SorlaRouteError::Invalid(format!("sor {sor}: `url` is missing")))?;

    let tenant = raw
        .tenant
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .ok_or_else(|| SorlaRouteError::Invalid(format!("sor {sor}: `tenant` is missing")))?;

    let token = raw
        .token
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());

    Ok(SorlaRouteDoc { url, token, tenant })
}

/// An in-memory [`greentic_secrets_lib::SecretsManager`] for `sorla_route` and
/// `sorla_invoker` (A3) tests, so both can seed and later mutate the same
/// backing store a live `DynSecretsManager` handle points at — which is what
/// lets a test move a SoR to a new port mid-test the way a restarted child
/// would.
#[cfg(test)]
pub(crate) mod test_secrets {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// Cheap to clone: every clone shares the same backing maps.
    #[derive(Clone, Default)]
    pub(crate) struct Handle {
        entries: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        /// URIs that must fail with a BACKEND error (never `NotFound`) —
        /// distinct from an absent entry, which is an ordinary miss. Used by
        /// `resolve_route`'s `Unavailable` tests: a backend that cannot
        /// answer at all must never be read as "no route document".
        failing: Arc<Mutex<HashMap<String, String>>>,
    }

    // `set` and `manager` are unused by this module's own tests — they exist
    // for `sorla_invoker` (A3), which reuses this fake rather than defining
    // its own.
    #[allow(dead_code)]
    impl Handle {
        pub(crate) fn with(entries: &[(&str, &str)]) -> Self {
            let map = entries
                .iter()
                .map(|(uri, body)| ((*uri).to_string(), body.as_bytes().to_vec()))
                .collect();
            Self {
                entries: Arc::new(Mutex::new(map)),
                failing: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub(crate) fn set(&self, uri: &str, body: &str) {
            self.entries
                .lock()
                .unwrap()
                .insert(uri.to_string(), body.as_bytes().to_vec());
        }

        /// Make every future read of `uri` fail with `SecretError::Backend`
        /// (never `NotFound`) — a secrets backend that cannot answer at all,
        /// as opposed to one that answered "no such entry".
        pub(crate) fn fail(&self, uri: &str, message: &str) {
            self.failing
                .lock()
                .unwrap()
                .insert(uri.to_string(), message.to_string());
        }

        /// This handle, boxed as the runner's shared secrets-manager type.
        pub(crate) fn manager(&self) -> crate::secrets::DynSecretsManager {
            Arc::new(self.clone())
        }
    }

    #[async_trait::async_trait]
    impl greentic_secrets_lib::SecretsManager for Handle {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            if let Some(message) = self.failing.lock().unwrap().get(path).cloned() {
                return Err(greentic_secrets_lib::SecretError::Backend(message.into()));
            }
            self.entries
                .lock()
                .unwrap()
                .get(path)
                .cloned()
                .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
        }

        async fn write(&self, path: &str, bytes: &[u8]) -> greentic_secrets_lib::Result<()> {
            self.entries
                .lock()
                .unwrap()
                .insert(path.to_string(), bytes.to_vec());
            Ok(())
        }

        async fn delete(&self, path: &str) -> greentic_secrets_lib::Result<()> {
            self.entries.lock().unwrap().remove(path);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_secrets::Handle;
    use super::*;

    #[tokio::test]
    async fn resolves_a_tenant_default_document() {
        let s = Handle::with(&[(
            "secrets://default/acme/_/sorla/landlord",
            r#"{"url":"http://127.0.0.1:9000","tenant":"acme"}"#,
        )]);
        let doc = resolve_route(&s, "acme", None, "landlord")
            .await
            .expect("resolves");
        assert_eq!(doc.url, "http://127.0.0.1:9000");
        assert_eq!(doc.token, None);
        assert_eq!(doc.tenant, "acme");
    }

    #[tokio::test]
    async fn a_blank_token_reads_as_no_token() {
        let s = Handle::with(&[(
            "secrets://default/acme/_/sorla/landlord",
            r#"{"url":"http://x","token":"  ","tenant":"acme"}"#,
        )]);
        assert_eq!(
            resolve_route(&s, "acme", None, "landlord")
                .await
                .expect("resolves")
                .token,
            None
        );
    }

    #[tokio::test]
    async fn a_missing_document_names_what_was_tried() {
        let s = Handle::with(&[]);
        let err = resolve_route(&s, "acme", None, "landlord")
            .await
            .expect_err("missing");
        assert!(matches!(err, SorlaRouteError::Missing(_)));
        assert!(err.to_string().contains("sorla/landlord"));
    }

    /// A secrets backend that cannot answer at all (denied, unreachable, ...)
    /// must never be read as "no route document" — that would let a call
    /// silently fall back to NATS (or misreport `sorla_route_missing`) for a
    /// SoR that may well have a working route, the backend just could not be
    /// asked.
    #[tokio::test]
    async fn a_backend_failure_is_unavailable_never_missing() {
        let s = Handle::with(&[]);
        s.fail(
            "secrets://default/acme/_/sorla/landlord",
            "connection refused",
        );
        let err = resolve_route(&s, "acme", None, "landlord")
            .await
            .expect_err("a backend failure must not resolve");
        assert!(
            matches!(err, SorlaRouteError::Unavailable(_)),
            "got: {err:?}"
        );
    }

    #[tokio::test]
    async fn an_invalid_document_never_echoes_its_value() {
        let s = Handle::with(&[(
            "secrets://default/acme/_/sorla/landlord",
            r#"{"token":"s3cr3t"}"#,
        )]);
        let err = resolve_route(&s, "acme", None, "landlord")
            .await
            .expect_err("invalid");
        assert!(matches!(err, SorlaRouteError::Invalid(_)));
        assert!(!err.to_string().contains("s3cr3t"));
    }
}
