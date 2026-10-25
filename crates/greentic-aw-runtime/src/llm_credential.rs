//! Resolves a per-tenant `BridgeCredential` from a worker's `LlmProviderRef`
//! and the tenant-scoped secrets manager. The secret URI matches what
//! greentic-designer-admin writes: `secrets://default/{tenant}/_/llm/{ref}`.

use std::sync::Arc;

use crate::config::LlmProviderRef;
use crate::error::LlmError;
use crate::llm_extension::BridgeCredential;
use greentic_secrets_lib::SecretsManager;

pub struct SecretsBackedCredentialResolver {
    secrets: Arc<dyn SecretsManager>,
    tenant: String,
}

impl SecretsBackedCredentialResolver {
    pub fn new(secrets: Arc<dyn SecretsManager>, tenant: impl Into<String>) -> Self {
        Self {
            secrets,
            tenant: tenant.into(),
        }
    }

    pub async fn resolve(&self, pr: &LlmProviderRef) -> Result<BridgeCredential, LlmError> {
        let cref = pr.credential_ref.as_deref().ok_or_else(|| {
            LlmError::BadRequest(format!("no credential_ref for provider `{}`", pr.provider))
        })?;
        let uri = crate::scoped_secrets::secret_uri("llm", &self.tenant, None, cref)
            .map_err(|e| LlmError::BadRequest(format!("credential_ref rejected: {e}")))?;
        let bytes = self
            .secrets
            .read(&uri)
            .await
            .map_err(|e| LlmError::BadRequest(format!("resolve credential: {e}")))?;
        let api_key = String::from_utf8(bytes)
            .map_err(|e| LlmError::BadRequest(format!("credential not utf8: {e}")))?;
        Ok(BridgeCredential {
            provider: pr.provider.clone(),
            model: pr.model.clone(),
            api_key,
            base_url: None,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::LlmProviderRef;
    use async_trait::async_trait;
    use std::sync::Arc;

    struct FakeSecrets;
    #[async_trait]
    impl greentic_secrets_lib::SecretsManager for FakeSecrets {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            assert_eq!(path, "secrets://default/acme/_/llm/cred-uuid");
            Ok(b"sk-live".to_vec())
        }
        async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn builds_bridge_credential_from_request_and_secret() {
        let resolver = SecretsBackedCredentialResolver::new(Arc::new(FakeSecrets), "acme");
        let pr = LlmProviderRef {
            provider: "anthropic".into(),
            model: "claude-3-5-sonnet-latest".into(),
            credential_ref: Some("cred-uuid".into()),
        };
        let cred = resolver.resolve(&pr).await.unwrap();
        assert_eq!(cred.provider, "anthropic");
        assert_eq!(cred.model, "claude-3-5-sonnet-latest");
        assert_eq!(cred.api_key, "sk-live");
        assert_eq!(cred.base_url, None);
    }

    #[tokio::test]
    async fn errors_when_credential_ref_missing() {
        let resolver = SecretsBackedCredentialResolver::new(Arc::new(FakeSecrets), "acme");
        let pr = LlmProviderRef {
            provider: "openai".into(),
            model: "gpt-4o".into(),
            credential_ref: None,
        };
        assert!(resolver.resolve(&pr).await.is_err());
    }

    #[tokio::test]
    async fn rejects_unsafe_credential_ref_without_reading() {
        for bad in [
            "..",
            "../other",
            "a/b",
            "%2e%2e",
            "vault://acme/x",
            ".hidden",
        ] {
            let resolver = SecretsBackedCredentialResolver::new(Arc::new(NeverRead), "acme");
            let pr = LlmProviderRef {
                provider: "openai".into(),
                model: "gpt-4o".into(),
                credential_ref: Some(bad.into()),
            };
            // NeverRead answers Ok if reached, so Err proves rejection before the read.
            assert!(resolver.resolve(&pr).await.is_err(), "{bad}");
        }
    }

    struct NeverRead;
    #[async_trait]
    impl greentic_secrets_lib::SecretsManager for NeverRead {
        async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            Ok(format!("REACHED {path}").into_bytes())
        }
        async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
        async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }
}
