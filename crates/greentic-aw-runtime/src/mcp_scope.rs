//! The (tenant, secrets) pair threaded from an MCP dispatch call site down to
//! `greentic-mcp-exec`. Both halves are required for a component's
//! `secret_get` to resolve: without the tenant the host returns
//! `missing-tenant-ctx`, without the store it returns `secrets-unavailable`.

use std::sync::Arc;

use greentic_mcp_exec::DynSecretsStore;
use greentic_secrets_lib::SecretsManager;
use greentic_types::TenantCtx;

use crate::mcp_secrets::McpSecretsStore;
use crate::tenant::TenantContext;

#[derive(Clone)]
pub struct McpCallScope {
    pub tenant: TenantContext,
    pub secrets: Option<Arc<dyn SecretsManager>>,
}

impl McpCallScope {
    /// Scope with no secrets access. Introspection and tests use this.
    pub fn new(tenant: TenantContext) -> Self {
        Self {
            tenant,
            secrets: None,
        }
    }

    pub fn with_secrets(tenant: TenantContext, secrets: Arc<dyn SecretsManager>) -> Self {
        Self {
            tenant,
            secrets: Some(secrets),
        }
    }

    /// `greentic-types` tenant for the exec request. `None` when the ids do not
    /// satisfy the shared format — the call then behaves exactly as it does
    /// today (no secrets), rather than failing the tool call.
    pub fn types_tenant(&self) -> Option<TenantCtx> {
        crate::knowledge::to_types_tenant(&self.tenant)
            .inspect_err(|e| {
                tracing::warn!(
                    tenant_id = %self.tenant.tenant_id,
                    env_id = %self.tenant.env_id,
                    error = %e,
                    "tenant ids do not satisfy the greentic-types tenant format; \
                     secret_get will report missing-tenant-ctx for this call"
                );
            })
            .ok()
    }

    pub fn exec_secrets_store(&self) -> Option<DynSecretsStore> {
        self.secrets
            .as_ref()
            .map(|m| Arc::new(McpSecretsStore::new(m.clone())) as DynSecretsStore)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[test]
    fn scope_without_secrets_yields_no_store() {
        let scope = McpCallScope::new(TenantContext::new("acme", "prod"));
        assert!(scope.exec_secrets_store().is_none());
        assert!(scope.types_tenant().is_some());
    }

    #[test]
    fn invalid_tenant_ids_yield_no_types_tenant() {
        let scope = McpCallScope::new(TenantContext::new("", ""));
        assert!(scope.types_tenant().is_none());
    }

    struct FakeSecrets;

    #[async_trait]
    impl SecretsManager for FakeSecrets {
        async fn read(&self, _path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
            Ok(b"fake-secret".to_vec())
        }
        async fn write(&self, _path: &str, _value: &[u8]) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
        async fn delete(&self, _path: &str) -> greentic_secrets_lib::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn scope_with_secrets_yields_exec_secrets_store() {
        let scope =
            McpCallScope::with_secrets(TenantContext::new("acme", "prod"), Arc::new(FakeSecrets));
        assert!(scope.exec_secrets_store().is_some());
    }
}
