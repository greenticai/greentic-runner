//! A local-wasm MCP component must be able to read a tenant secret. Before the
//! secrets wiring this returned `secrets-unavailable`; this test fails if the
//! store is ever unwired again.

use greentic_aw_runtime::mcp_scope::McpCallScope;
use greentic_aw_runtime::tenant::TenantContext;
use greentic_secrets_lib::SecretsManager;
use std::sync::Arc;

struct StaticSecrets;

#[async_trait::async_trait]
impl SecretsManager for StaticSecrets {
    async fn read(&self, path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
        assert_eq!(path, "secrets://prod/acme/_/mcp/example_key");
        Ok(b"sk-live".to_vec())
    }
    async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
    async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
        Ok(())
    }
}

#[test]
fn scope_with_secrets_produces_a_store_and_a_tenant() {
    let scope =
        McpCallScope::with_secrets(TenantContext::new("acme", "prod"), Arc::new(StaticSecrets));
    assert!(
        scope.exec_secrets_store().is_some(),
        "a secrets-carrying scope must yield an exec store"
    );
    assert!(
        scope.types_tenant().is_some(),
        "a secrets-carrying scope must yield a tenant; without it the host \
         answers missing-tenant-ctx and the store is never consulted"
    );
}
