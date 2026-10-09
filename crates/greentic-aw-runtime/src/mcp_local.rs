//! Local (in-process) `wasix:mcp` execution for the `local-wasm` MCP transport.
//!
//! Resolves a component from a versioned on-disk cache and runs its
//! `list-tools` / `call-tool` through `greentic-mcp-exec` (Wasmtime). The
//! executor is synchronous, so every entry point hops onto `spawn_blocking`.
//! Both functions are infallible by the rail's contract: list degrades to empty
//! with a `warn`; call returns `{"error": ...}` on any failure.

use std::collections::HashMap;
use std::path::PathBuf;

use greentic_mcp_exec::{
    ExecConfig, ExecRequest, RuntimePolicy, ToolDef, ToolStore, VerifyPolicy, exec, list_tools,
};
use serde_json::{Value, json};

use crate::mcp_scope::McpCallScope;

/// Directory holding cached local MCP `*.wasm` files.
///
/// Resolution order:
/// 1. `GREENTIC_MCP_LOCAL_CACHE_DIR` env var (explicit override).
/// 2. `$GREENTIC_EXTENSIONS_DIR/mcp-local` (standard extensions hierarchy).
/// 3. `./.mcp-local` (process-local fallback).
pub fn cache_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("GREENTIC_MCP_LOCAL_CACHE_DIR") {
        return PathBuf::from(dir);
    }
    if let Ok(root) = std::env::var("GREENTIC_EXTENSIONS_DIR") {
        return PathBuf::from(root).join("mcp-local");
    }
    PathBuf::from(".mcp-local")
}

/// Build an `ExecConfig` for a component, pinning the wasm digest when a
/// sidecar `<ref>.wasm.sha256` exists in the cache.
///
/// **Verified path** (store-pull, Task 2 has run): the sidecar exists, so the
/// config sets `allow_unverified: false` and `required_digests = {component_ref
/// => sha256(extension.wasm)}`. The key is the file stem — the exact string
/// mcp-exec uses when it resolves `component_ref.wasm` from the local store.
///
/// **Fallback path** (operator-seeded Phase-1 cache, no sidecar): the config
/// sets `allow_unverified: true` with an empty `required_digests`. This
/// preserves the Phase-1 behavior for caches that were seeded directly without
/// going through the store-pull path.
///
/// # Security rationale — post-pull on-disk tampering
///
/// Setting `allow_unverified: false` + `required_digests` is sufficient to
/// detect on-disk tampering of the cached `.wasm` file *after* a verified pull.
/// When `greentic-mcp-exec` resolves `<component_ref>.wasm` it **re-hashes the
/// wasm bytes at resolve time** and compares against every key in
/// `required_digests`; execution is refused if the hash does not match.
/// Therefore an attacker who gains write access to the cache directory after a
/// successful store-pull cannot silently substitute a malicious binary: the
/// execution-time hash check will catch the mismatch before the component is
/// instantiated, and `local_call_tool` will return `{"error": ...}` without
/// running the tampered code.
pub(crate) fn exec_config_for(component_ref: &str, scope: Option<&McpCallScope>) -> ExecConfig {
    let pinned_digest = read_pinned_digest(component_ref);
    let security = if let Some(wasm_digest) = pinned_digest {
        let mut required_digests = HashMap::new();
        required_digests.insert(component_ref.to_string(), wasm_digest);
        VerifyPolicy {
            allow_unverified: false,
            required_digests,
            trusted_signers: Vec::new(),
        }
    } else {
        // No sidecar: operator-seeded cache; maintain Phase-1 trust posture.
        VerifyPolicy {
            allow_unverified: true,
            required_digests: HashMap::new(),
            trusted_signers: Vec::new(),
        }
    };
    ExecConfig {
        store: ToolStore::LocalDir(cache_dir()),
        security,
        runtime: RuntimePolicy::default(),
        // Router tools commonly wrap REST/HTTP APIs (e.g. the generated
        // OpenAPI routers), so the component is granted outbound HTTP. "No HTTP
        // hop" refers to the host<->MCP transport, not the tool's own egress.
        http_enabled: true,
        secrets_store: scope.and_then(|s| s.exec_secrets_store()),
    }
}

/// Read the pinned wasm digest from the sidecar file `<cache>/<ref>.wasm.sha256`.
///
/// Returns `Some(hex_digest)` when the sidecar exists and is readable UTF-8;
/// `None` when the sidecar is absent (operator-seeded cache, no prior
/// store-pull). A missing sidecar (`ErrorKind::NotFound`) is treated silently as
/// the Phase-1 / operator-seeded case; any other read error (permission denied,
/// corrupt filesystem) is unexpected and is reported with a `warn` so the
/// operator can investigate without needing to trace back through silent fallbacks.
fn read_pinned_digest(component_ref: &str) -> Option<String> {
    let wasm_dest = cache_dir().join(format!("{component_ref}.wasm"));
    let sidecar = crate::mcp_store_pull::sidecar_path(&wasm_dest);
    match std::fs::read_to_string(&sidecar) {
        Ok(content) => {
            let trimmed = content.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            tracing::warn!(
                component = %component_ref,
                sidecar = %sidecar.display(),
                error = %e,
                "unexpected error reading wasm digest sidecar; falling back to allow_unverified"
            );
            None
        }
    }
}

/// List a local component's tools. Returns an empty vec and emits a `warn` on any failure.
// pre-existing: the spawned closure returns `greentic_mcp_exec::ExecError`, a large
// external error variant we neither own nor propagate (mapped to a warn here); boxing
// it would touch `list_tools`'s public signature, so we scope-allow instead.
#[allow(clippy::result_large_err)]
pub async fn local_list_tools(component_ref: &str) -> Vec<ToolDef> {
    let component = component_ref.to_string();
    let config = exec_config_for(component_ref, None);
    let res = tokio::task::spawn_blocking(move || list_tools(&component, &config)).await;
    match res {
        Ok(Ok(tools)) => tools,
        Ok(Err(e)) => {
            tracing::warn!(
                component = %component_ref,
                error = %e,
                "local mcp list_tools failed; skipping"
            );
            Vec::new()
        }
        Err(e) => {
            tracing::warn!(
                component = %component_ref,
                error = %e,
                "local mcp list_tools task panicked; skipping"
            );
            Vec::new()
        }
    }
}

/// Build the exec request for a local tool call. Pure, so the tenant wiring is
/// testable without a wasm component: a `None` tenant here is precisely the
/// defect that makes every `secret_get` fail with `missing-tenant-ctx`.
pub(crate) fn exec_request_for(
    component_ref: &str,
    tool: &str,
    args: &Value,
    scope: &McpCallScope,
) -> ExecRequest {
    ExecRequest {
        component: component_ref.to_string(),
        action: tool.to_string(),
        args: args.clone(),
        tenant: scope.types_tenant(),
    }
}

/// Call a local component's tool. Returns `{"error": ...}` on any failure; never panics.
// pre-existing: the spawned closure returns `greentic_mcp_exec::ExecError`, a large
// external error variant we neither own nor propagate (mapped to a JSON error here);
// boxing it would touch `exec`'s public signature, so we scope-allow instead.
#[allow(clippy::result_large_err)]
pub async fn local_call_tool(
    component_ref: &str,
    tool: &str,
    args: &Value,
    scope: &McpCallScope,
) -> Value {
    let config = exec_config_for(component_ref, Some(scope));
    let req = exec_request_for(component_ref, tool, args, scope);
    let res = tokio::task::spawn_blocking(move || exec(req, &config)).await;
    match res {
        Ok(Ok(value)) => value,
        Ok(Err(e)) => json!({ "error": format!("local mcp call failed: {e}") }),
        Err(e) => json!({ "error": format!("local mcp call panicked: {e}") }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, unsafe_code)]
mod tests {
    use super::*;

    /// Resolve a built router_echo wasm if present; else return None (test self-skips).
    fn fixture_wasm() -> Option<std::path::PathBuf> {
        let p = std::env::var("GREENTIC_MCP_ROUTER_ECHO_WASM")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../../greentic-mcp/target/wasm32-wasip2/release/router_echo.wasm")
            });
        p.exists().then_some(p)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn local_call_tool_runs_in_process() {
        let Some(src) = fixture_wasm() else {
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        // Safety: serial attribute ensures no concurrent env-var mutation between
        // the two tests that share GREENTIC_MCP_LOCAL_CACHE_DIR.
        unsafe { std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", dir.path()) };
        std::fs::copy(&src, dir.path().join("router_echo.wasm")).unwrap();

        let scope = McpCallScope::new(crate::tenant::TenantContext::new("acme", "prod"));
        let out = local_call_tool(
            "router_echo",
            "echo",
            &serde_json::json!({"message": "hi"}),
            &scope,
        )
        .await;
        assert!(!out.to_string().contains("\"error\""), "got: {out}");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn local_call_tool_missing_component_returns_error_value() {
        let dir = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", dir.path()) };
        let scope = McpCallScope::new(crate::tenant::TenantContext::new("acme", "prod"));
        let out = local_call_tool("nope", "echo", &serde_json::json!({}), &scope).await;
        assert!(out.to_string().contains("error"), "got: {out}");
    }

    #[test]
    #[serial_test::serial]
    fn cache_dir_uses_extensions_dir_fallback() {
        unsafe {
            std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
            std::env::set_var("GREENTIC_EXTENSIONS_DIR", "/tmp/ext");
        }
        assert_eq!(cache_dir(), PathBuf::from("/tmp/ext/mcp-local"));
        unsafe { std::env::remove_var("GREENTIC_EXTENSIONS_DIR") };
    }

    #[test]
    #[serial_test::serial]
    fn cache_dir_defaults_to_dot_mcp_local() {
        unsafe {
            std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR");
            std::env::remove_var("GREENTIC_EXTENSIONS_DIR");
        }
        assert_eq!(cache_dir(), PathBuf::from(".mcp-local"));
    }

    #[test]
    #[serial_test::serial]
    fn exec_config_verified_when_sidecar_present() {
        let dir = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", dir.path()) };
        // Write a sidecar digest file for "mycomp.wasm.sha256".
        let sidecar = dir.path().join("mycomp.wasm.sha256");
        std::fs::write(&sidecar, "abcdef1234567890").unwrap();
        let config = exec_config_for("mycomp", None);
        assert!(
            !config.security.allow_unverified,
            "sidecar present → allow_unverified must be false"
        );
        assert_eq!(
            config.security.required_digests.get("mycomp"),
            Some(&"abcdef1234567890".to_string()),
        );
        unsafe { std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR") };
    }

    #[test]
    #[serial_test::serial]
    fn exec_config_unverified_without_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var("GREENTIC_MCP_LOCAL_CACHE_DIR", dir.path()) };
        let config = exec_config_for("nosidecar", None);
        assert!(
            config.security.allow_unverified,
            "no sidecar → allow_unverified must be true"
        );
        assert!(config.security.required_digests.is_empty());
        unsafe { std::env::remove_var("GREENTIC_MCP_LOCAL_CACHE_DIR") };
    }

    #[test]
    fn exec_config_without_scope_has_no_secrets_store() {
        let config = exec_config_for("nosecrets", None);
        assert!(
            config.secrets_store.is_none(),
            "introspection must stay credential-free"
        );
    }

    #[test]
    fn exec_request_carries_tenant_from_scope() {
        use crate::mcp_scope::McpCallScope;
        use crate::tenant::TenantContext;
        use serde_json::json;

        let scope = McpCallScope::new(TenantContext::new("acme", "prod"));
        let req = exec_request_for("petstore", "list_pets", &json!({}), &scope);
        let tenant = req
            .tenant
            .expect("tenant must be populated; without it the host answers missing-tenant-ctx");
        assert_eq!(tenant.tenant.as_str(), "acme");
        assert_eq!(tenant.env.as_str(), "prod");
        assert_eq!(req.component, "petstore");
        assert_eq!(req.action, "list_pets");
    }

    #[test]
    fn exec_config_with_scope_carrying_secrets_has_a_store() {
        use crate::mcp_scope::McpCallScope;
        use crate::tenant::TenantContext;
        use async_trait::async_trait;
        use std::sync::Arc;

        struct FakeSecrets;
        #[async_trait]
        impl greentic_secrets_lib::SecretsManager for FakeSecrets {
            async fn read(&self, _: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
                Ok(b"v".to_vec())
            }
            async fn write(&self, _: &str, _: &[u8]) -> greentic_secrets_lib::Result<()> {
                Ok(())
            }
            async fn delete(&self, _: &str) -> greentic_secrets_lib::Result<()> {
                Ok(())
            }
        }

        let scope =
            McpCallScope::with_secrets(TenantContext::new("acme", "prod"), Arc::new(FakeSecrets));
        let config = exec_config_for("withsecrets", Some(&scope));
        assert!(
            config.secrets_store.is_some(),
            "the call path must receive a secrets store"
        );
    }
}
