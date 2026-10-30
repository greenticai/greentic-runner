//! Serves `secret://infra/<name>` to extension tools.
//!
//! The extension runtime's `host.secrets.get` hands the guest URI to
//! [`greentic_ext_runtime::SecretsBackend`]. For provider `infra` the answer is
//! NOT a store read of that URI: it is the typed, validated, host-normalised
//! connection document built by `greentic_aw_runtime::infra_connection`.
//!
//! Two rules: a missing entry is a named error and never falls back to an
//! environment variable (that would resurrect a localhost default), and no
//! error text carries a value.

use greentic_aw_runtime::infra_connection;
use greentic_ext_runtime::SecretsError;
use secrecy::ExposeSecret;

/// Resolve the guest document for `name` on a dedicated thread with its own
/// current-thread runtime (the caller may be inside the async runner, where a
/// nested `block_on` would panic).
pub(crate) fn resolve_blocking(
    secrets: crate::secrets::DynSecretsManager,
    tenant: &str,
    name: &str,
) -> Result<String, SecretsError> {
    let bridge_down = || {
        SecretsError::Backend(format!(
            "infra resource '{name}': secret store bridge unavailable"
        ))
    };
    let tenant = tenant.to_string();
    let owned_name = name.to_string();
    let result = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()?;
        Some(runtime.block_on(async move {
            let conn = infra_connection::read(secrets.as_ref(), &tenant, &owned_name).await?;
            Ok::<_, infra_connection::InfraConnectionError>(
                conn.to_guest_document(&owned_name)
                    .expose_secret()
                    .to_string(),
            )
        }))
    })
    .join()
    .ok()
    .flatten();
    match result {
        Some(Ok(doc)) => Ok(doc),
        // A routine "not connected" read is not an error: the extension falls
        // back to its legacy config, and `Backend` would be logged at ERROR by
        // the ext-runtime on every call. `NotFound` carries the full remedy
        // sentence (and the phrase the Qdrant extension's `classify_host_error`
        // matches). Genuine faults stay `Backend`.
        Some(Err(err @ infra_connection::InfraConnectionError::Missing { .. })) => {
            Err(SecretsError::NotFound(err.to_string()))
        }
        Some(Err(err)) => Err(SecretsError::Backend(err.to_string())),
        None => Err(bridge_down()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use greentic_aw_runtime::infra_connection::kind_from_guest_uri;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    struct Recording {
        entries: HashMap<String, Vec<u8>>,
        seen: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl greentic_secrets_lib::SecretsManager for Recording {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            self.seen.lock().unwrap().push(path.to_string());
            self.entries
                .get(path)
                .cloned()
                .ok_or_else(|| greentic_secrets_lib::SecretError::NotFound(path.to_string()))
        }
        async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    fn secrets(pairs: &[(&str, &str)]) -> Arc<Recording> {
        Arc::new(Recording {
            entries: pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.as_bytes().to_vec()))
                .collect(),
            seen: Mutex::new(Vec::new()),
        })
    }

    #[test]
    fn kind_from_guest_uri_accepts_only_secret_infra_prefix() {
        assert_eq!(kind_from_guest_uri("secret://infra/x"), Some("x"));
        assert_eq!(kind_from_guest_uri("secret://infra"), None);
        assert_eq!(kind_from_guest_uri("secret://infra/"), None);
        assert_eq!(kind_from_guest_uri("secret://infra/a/b"), None);
        assert_eq!(kind_from_guest_uri("secrets://default/x/_/infra/y"), None);
        assert_eq!(kind_from_guest_uri("secret://infrastructure/x"), None);
    }

    #[test]
    fn a_missing_entry_is_a_named_not_found_with_the_remedy() {
        let s = secrets(&[]);
        let err = resolve_blocking(s, "acme", "vector_store_qdrant").unwrap_err();
        let SecretsError::NotFound(msg) = err else {
            panic!("expected NotFound, got {err:?}");
        };
        assert!(msg.contains("vector_store_qdrant") && msg.contains("deploy again"));
        // Matched verbatim by the Qdrant extension's `classify_host_error`.
        assert!(msg.contains("is not connected to this deployment"), "{msg}");
    }

    #[test]
    fn resolves_the_normalised_document() {
        let s = secrets(&[(
            "secrets://default/default/_/infra/cache_redis",
            r#"{"host":"r.example.com","port":6379,"scheme":"rediss","credential":"pw"}"#,
        )]);
        let doc = resolve_blocking(s, "acme", "cache_redis").unwrap();
        let v: serde_json::Value = serde_json::from_str(&doc).unwrap();
        assert_eq!(v["v"], 1);
        assert_eq!(v["host"], "r.example.com");
        assert_eq!(v["base_url"], "rediss://r.example.com:6379");
    }
}
