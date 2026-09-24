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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SorlaRouteError {
    Missing(String),
    Invalid(String),
}

impl std::fmt::Display for SorlaRouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(tried) => write!(f, "no sorla route document ({tried})"),
            Self::Invalid(why) => write!(f, "sorla route document is invalid: {why}"),
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
/// known, `unit` — see [`greentic_aw_runtime::scoped_secrets::read_secret_for_unit`]
/// for the candidate order this tries).
///
/// An absent or blank `token` resolves to `None`: a call built from this
/// document must never send an empty `Authorization: Bearer ` header.
pub async fn resolve_route(
    secrets: &dyn greentic_secrets_lib::SecretsManager,
    tenant: &str,
    unit: Option<&str>,
    sor: &str,
) -> Result<SorlaRouteDoc, SorlaRouteError> {
    let bytes = greentic_aw_runtime::scoped_secrets::read_secret_for_unit(
        secrets,
        SORLA_CATEGORY,
        tenant,
        None,
        unit,
        sor,
    )
    .await
    .map_err(|miss| SorlaRouteError::Missing(format!("sor {sor}: {miss}")))?;

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

    /// Cheap to clone: every clone shares the same backing map.
    #[derive(Clone, Default)]
    pub(crate) struct Handle(Arc<Mutex<HashMap<String, Vec<u8>>>>);

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
            Self(Arc::new(Mutex::new(map)))
        }

        pub(crate) fn set(&self, uri: &str, body: &str) {
            self.0
                .lock()
                .unwrap()
                .insert(uri.to_string(), body.as_bytes().to_vec());
        }

        /// This handle, boxed as the runner's shared secrets-manager type.
        pub(crate) fn manager(&self) -> crate::secrets::DynSecretsManager {
            Arc::new(self.clone())
        }
    }

    #[async_trait::async_trait]
    impl greentic_secrets_lib::SecretsManager for Handle {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            self.0
                .lock()
                .unwrap()
                .get(path)
                .cloned()
                .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
        }

        async fn write(&self, path: &str, bytes: &[u8]) -> greentic_secrets_lib::Result<()> {
            self.0
                .lock()
                .unwrap()
                .insert(path.to_string(), bytes.to_vec());
            Ok(())
        }

        async fn delete(&self, path: &str) -> greentic_secrets_lib::Result<()> {
            self.0.lock().unwrap().remove(path);
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
