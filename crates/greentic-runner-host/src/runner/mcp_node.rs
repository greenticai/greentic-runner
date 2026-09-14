//! Flow-execution MCP node (LOCKED ENCODING v2: `component == "mcp"`).
//!
//! The node carries `server`, `tool`, `arguments` and an optional `output`
//! state key in its payload/config — the same shape `greentic-flow` lowers
//! designer MCP nodes to. The payload is the source of truth.
//!
//! This is the SEPARATE flow-execution MCP path (role `flow_editor`), distinct
//! from the agent-loop MCP path in `greentic-aw-runtime` (role
//! `agentic_worker`). It reuses [`McpToolSource`] — the same per-tenant
//! admin-fetch → role-filter → probe → TTL-cache → `dispatch_route` machinery
//! the agent loop uses — only swapping the role filter to `flow_editor`.
//!
//! Resilience contract (MCP must never break a flow run): if MCP is
//! unconfigured (no admin env / opt-out) or the tool is unreachable, the node
//! produces a structured error value, never a panic and never an aborted
//! runtime. The engine binds that value into flow state exactly like a
//! successful call, so downstream nodes can branch on the `error` key.

#[cfg(feature = "agentic-worker")]
pub mod aw {
    use std::sync::Arc;

    use serde_json::{Value, json};

    use greentic_aw_runtime::{MCP_ROLE_FLOW_EDITOR, McpToolSource, TenantContext, dispatch_route};

    use crate::runner::mcp_pack_routes::{PackMcpRoute, PackMcpRoutes};

    /// Build an [`McpToolSource`] for the flow-execution path from the same
    /// admin credentials the agentic-worker registry uses
    /// (`GREENTIC_AW_ADMIN_ENDPOINT` + `GREENTIC_AW_ADMIN_TOKEN`).
    ///
    /// Mirrors `agent_node::mcp_source_from_env`: MCP is ON by default whenever
    /// the admin credentials are present, with `GREENTIC_AW_MCP=0` as the
    /// operator opt-out. Returns `None` on opt-out or when either credential is
    /// missing/empty, so a runner without MCP configured simply fails MCP nodes
    /// gracefully rather than constructing a useless source.
    pub(crate) fn source_from_env() -> Option<Arc<McpToolSource>> {
        if std::env::var("GREENTIC_AW_MCP").ok().as_deref() == Some("0") {
            tracing::info!("GREENTIC_AW_MCP=0; flow MCP node source disabled");
            return None;
        }
        let endpoint = std::env::var("GREENTIC_AW_ADMIN_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty())?;
        let token = std::env::var("GREENTIC_AW_ADMIN_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())?;
        tracing::info!(endpoint = %endpoint, "flow MCP node source constructed");
        Some(Arc::new(McpToolSource::new(endpoint, token)))
    }

    /// Build the secrets manager for the flow MCP path from the same
    /// `SECRETS_BACKEND` environment the host uses. Pure builder — no
    /// caching — so it can be exercised directly by unit tests. A failure
    /// returns `None` so an MCP tool call degrades to "no secrets" rather
    /// than failing the node.
    fn build_secrets_manager() -> Option<crate::secrets::DynSecretsManager> {
        match crate::secrets::SecretsBackend::from_env(std::env::var("SECRETS_BACKEND").ok())
            .and_then(|backend| backend.build_manager())
        {
            Ok(manager) => Some(manager),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "flow MCP secrets manager unavailable; local-wasm tools will run without secrets"
                );
                None
            }
        }
    }

    /// Build the secrets manager for the flow MCP path once per process and
    /// reuse it thereafter. Mirrors [`source_from_env`]'s boot-once
    /// semantics (see `runner/engine.rs`, constructed once at engine
    /// creation): the underlying manager (and, for the broker backend, its
    /// `reqwest::Client`) is built exactly once via [`build_secrets_manager`]
    /// and memoized in a process-level `OnceLock`; every call after the
    /// first returns a cheap clone of the cached `Arc`.
    pub(crate) fn secrets_from_env() -> Option<crate::secrets::DynSecretsManager> {
        static CACHED: std::sync::OnceLock<Option<crate::secrets::DynSecretsManager>> =
            std::sync::OnceLock::new();
        CACHED.get_or_init(build_secrets_manager).clone()
    }

    /// Which secrets manager an MCP credential resolves through, for BOTH the
    /// flow-node path (`invoke_with_secrets`, via `FlowEngine::execute_mcp`)
    /// and the agent path (`agent_node::mcp_secrets_manager`).
    ///
    /// Precedence, most specific first:
    ///
    /// 1. `requested` — `SECRETS_BACKEND`, when the operator set it. An
    ///    explicit instruction wins, and this is how a designer-spawned child
    ///    keeps using the broker `child_env` projects onto it. `greentic-start`
    ///    injects a manager whose kind is one of `dev-store` / `env` / `vault`
    ///    — there is no broker variant — so without this arm the projection
    ///    arrives intact and resolves against a manager that never heard of it.
    /// 2. `injected` — the manager the host built from the bundle's secrets
    ///    pack. A Cloud Run or Kubernetes workload sets no `SECRETS_BACKEND` at
    ///    all, so this is the only arm that can resolve a `secrets://` URI
    ///    there.
    /// 3. `env_built` — the env-derived default ([`secrets_from_env`]), for a
    ///    standalone runner with no host injection.
    ///
    /// # Why an unset variable must NOT pin the env backend
    ///
    /// `SecretsBackend::from_env` returns `Env` for an absent value, and the
    /// `env` backend looks a secret up by its literal path — so a
    /// `secrets://…` URI becomes a variable name nothing can set. Preferring
    /// it over an injected manager silently downgrades every host that
    /// deliberately supplies one, which is exactly what the flow path did in
    /// every Cloud Run and Kubernetes deployment, with no error anywhere.
    ///
    /// A `requested` backend that fails to BUILD falls back to `injected`
    /// rather than to nothing: a malformed broker endpoint should degrade to
    /// the host's own resolution, not strip the node of every credential.
    ///
    /// Split out from its callers so it is testable without mutating the
    /// process-wide `SECRETS_BACKEND` — [`secrets_from_env`] memoizes in a
    /// `OnceLock`, so a test driving this through the environment would pass or
    /// fail on which test ran first.
    ///
    /// `injected` is optional because the flow path may have neither manager.
    /// Whenever it is `Some` the result is `Some`, so the agent path's
    /// behaviour is unchanged.
    pub(crate) fn choose_mcp_secrets(
        requested: Option<&str>,
        env_built: Option<crate::secrets::DynSecretsManager>,
        injected: Option<&crate::secrets::DynSecretsManager>,
    ) -> Option<crate::secrets::DynSecretsManager> {
        let Some(requested) = requested.map(str::trim).filter(|value| !value.is_empty()) else {
            return injected.cloned().or(env_built);
        };
        match env_built {
            Some(manager) => {
                tracing::info!(
                    backend = %requested,
                    "MCP credentials resolve through the SECRETS_BACKEND manager"
                );
                Some(manager)
            }
            None => {
                tracing::warn!(
                    backend = %requested,
                    "SECRETS_BACKEND is set but its manager could not be built; \
                     MCP credentials fall back to the host's injected manager"
                );
                injected.cloned()
            }
        }
    }

    /// Build a dispatchable route from a pack-carried record, resolving the
    /// credential from the secrets backend.
    ///
    /// The token is NEVER carried by the pack — it is read from
    /// `secrets://default/<tenant>/<team>/mcp/<server_id>`, the URI
    /// greentic-designer-admin writes under the `mcp` category. That URI is
    /// built by [`greentic_aw_runtime::mcp_secrets::mcp_secret_uri`], now the
    /// SINGLE builder for the shape — this module used to carry a byte-for-byte
    /// second copy, which is how the flow path and the agent path would have
    /// started resolving different URIs for the same server with nothing
    /// failing.
    ///
    /// `read_mcp_secret` tries the record's `auth_team` scope before the
    /// tenant-default `_` scope, mirroring admin's own resolver precedence. A
    /// deployment whose token sits at `_` is unaffected; one whose token sits at
    /// a team scope starts working, because the deployed runtime carries no team
    /// of its own and so resolved `_` only.
    ///
    /// Only an `http` route reads a token here. A `local-wasm` route has no
    /// HTTP credential at all — admin writes no `mcp/<server_id>` entry for one
    /// — and the component reads its own secrets through the
    /// [`McpCallScope`](greentic_aw_runtime::mcp_scope::McpCallScope) the
    /// caller attaches. Reading unconditionally would fail every local-wasm
    /// pack route on a credential that is not supposed to exist.
    ///
    /// NOTE: this resolves only under `SECRETS_BACKEND=broker`. The `env`
    /// backend looks a variable up by the literal path, and a variable named
    /// `secrets://…` is not settable in Kubernetes — so the error below names
    /// every URI tried rather than reporting an opaque NotFound.
    async fn route_from_pack(
        route: &PackMcpRoute,
        secrets: Option<&crate::secrets::DynSecretsManager>,
        tenant: &str,
        team: Option<&str>,
    ) -> Result<greentic_aw_runtime::McpRoute, String> {
        let is_http = route.transport != "local-wasm";

        let token = match secrets.filter(|_| is_http) {
            Some(manager) => match greentic_aw_runtime::mcp_secrets::read_mcp_secret(
                manager.as_ref(),
                tenant,
                team,
                &route.server_id,
            )
            .await
            {
                Ok(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
                Err(miss) => {
                    return Err(format!("mcp server '{}' has {miss}", route.server_id));
                }
            },
            None => None,
        };

        Ok(greentic_aw_runtime::McpRoute::from_parts(
            &route.server_id,
            route.transport_url.as_deref().unwrap_or_default(),
            route.auth_header_name.as_deref(),
            token.as_deref(),
            &route.transport,
            route.component_ref.as_deref(),
            route.component_version.as_deref(),
            route.component_digest.as_deref(),
        ))
    }

    /// The `(tenant, secrets)` pair a dispatch runs under. `local-wasm` tools
    /// need both halves for their own `secret_get` to resolve.
    fn call_scope(
        tenant_ctx: TenantContext,
        secrets: Option<&crate::secrets::DynSecretsManager>,
    ) -> greentic_aw_runtime::mcp_scope::McpCallScope {
        match secrets {
            Some(manager) => greentic_aw_runtime::mcp_scope::McpCallScope::with_secrets(
                tenant_ctx,
                manager.clone(),
            ),
            None => greentic_aw_runtime::mcp_scope::McpCallScope::new(tenant_ctx),
        }
    }

    /// Invoke `tool` on `server_id` for `tenant`/`env` with `arguments`,
    /// preferring a pack-carried route and falling back to the flow-editor MCP
    /// catalog, with the secrets manager supplied explicitly.
    ///
    /// Infallible by contract: every failure path (no route anywhere, missing
    /// credential, server/tool not in the flow-editor catalog, transport error)
    /// returns a structured `{"error": "..."}` value. The caller binds the
    /// value as-is.
    ///
    /// The manager is a parameter rather than read from
    /// [`secrets_from_env`] because the caller — `FlowEngine::execute_mcp` —
    /// must be able to prefer the one its host injected: a Cloud Run or
    /// Kubernetes workload sets no `SECRETS_BACKEND`, so the env-derived
    /// manager there resolves a `secrets://` URI as a literal variable name and
    /// finds nothing. See
    /// [`choose_mcp_secrets`] for the precedence. It is also what lets a test
    /// control the backend at all, and the pack-route path is defined by which
    /// credential it resolves.
    #[allow(clippy::too_many_arguments)]
    pub async fn invoke_with_secrets(
        source: Option<&Arc<McpToolSource>>,
        pack_routes: Option<&PackMcpRoutes>,
        secrets: Option<&crate::secrets::DynSecretsManager>,
        tenant: &str,
        env: &str,
        team: Option<&str>,
        server_id: &str,
        tool: &str,
        arguments: &Value,
    ) -> Value {
        // `dispatch_route` takes the arguments as a JSON string and is itself
        // infallible (bad args / connect / timeout all become `{"error": ...}`).
        let args_str = arguments.to_string();
        let tenant_ctx = TenantContext::new(tenant, env);

        // A pack-carried route wins. This is what lets a deployed runner with
        // no admin credentials dispatch at all; falling through to the admin
        // catalog keeps every existing deployment and Run Demo unchanged.
        if let Some(route) = pack_routes.and_then(|routes| routes.get(server_id)) {
            return match route_from_pack(route, secrets, tenant, team).await {
                Ok(resolved) => {
                    let scope = call_scope(tenant_ctx, secrets);
                    dispatch_route(&resolved.with_tool(tool), &args_str, &scope).await
                }
                Err(e) => json!({ "error": e }),
            };
        }

        let Some(source) = source else {
            return json!({
                "error": "MCP is not configured on this runner (no route in the pack, and no \
                          GREENTIC_AW_ADMIN_ENDPOINT + GREENTIC_AW_ADMIN_TOKEN)"
            });
        };

        let catalog = source
            .catalog_for_role(&tenant_ctx, MCP_ROLE_FLOW_EDITOR)
            .await;

        let Some(route) = catalog.route(server_id, tool) else {
            return json!({
                "error": format!(
                    "mcp tool '{server_id}/{tool}' not found in the tenant's flow_editor catalog"
                )
            });
        };

        let scope = call_scope(tenant_ctx, secrets);
        dispatch_route(route, &args_str, &scope).await
    }

    #[cfg(test)]
    mod precedence_tests {
        use super::choose_mcp_secrets;
        use std::sync::Arc;

        /// A distinguishable no-op manager. These tests assert WHICH manager
        /// was chosen via `Arc::ptr_eq`, so all they need is two separately
        /// allocated instances of something implementing the trait.
        struct StubSecrets;

        #[async_trait::async_trait]
        impl greentic_secrets_lib::SecretsManager for StubSecrets {
            async fn read(&self, _path: &str) -> greentic_secrets_lib::Result<Vec<u8>> {
                Ok(Vec::new())
            }
            async fn write(&self, _path: &str, _bytes: &[u8]) -> greentic_secrets_lib::Result<()> {
                Ok(())
            }
            async fn delete(&self, _path: &str) -> greentic_secrets_lib::Result<()> {
                Ok(())
            }
        }

        fn manager() -> crate::secrets::DynSecretsManager {
            Arc::new(StubSecrets)
        }

        fn same(
            a: &crate::secrets::DynSecretsManager,
            b: &crate::secrets::DynSecretsManager,
        ) -> bool {
            Arc::ptr_eq(a, b)
        }

        /// The whole point. A Cloud Run or Kubernetes workload sets no
        /// `SECRETS_BACKEND`, and the host injects a manager that CAN resolve a
        /// `secrets://` URI. Falling back to the env default here is what made
        /// every deployed `mcp` node fail.
        #[test]
        fn an_unset_backend_keeps_the_injected_manager() {
            let injected = manager();
            let chosen = choose_mcp_secrets(None, Some(manager()), Some(&injected))
                .expect("an injected manager is always a choice");
            assert!(same(&chosen, &injected));
        }

        #[test]
        fn an_empty_backend_is_treated_as_unset() {
            let injected = manager();
            for requested in [Some(""), Some("   ")] {
                let chosen = choose_mcp_secrets(requested, Some(manager()), Some(&injected))
                    .expect("an injected manager is always a choice");
                assert!(
                    same(&chosen, &injected),
                    "requested={requested:?} must keep the injected manager"
                );
            }
        }

        /// The operator said so. A designer-spawned child gets
        /// `SECRETS_BACKEND=broker` from `child_env` and must keep using it.
        #[test]
        fn an_explicit_backend_wins_over_the_injected_manager() {
            let injected = manager();
            let env_built = manager();
            let chosen =
                choose_mcp_secrets(Some("broker"), Some(env_built.clone()), Some(&injected))
                    .expect("a built manager is a choice");
            assert!(same(&chosen, &env_built));
        }

        /// A malformed broker endpoint should degrade to the host's own
        /// resolution, not strip the node of every credential.
        #[test]
        fn an_unbuildable_backend_falls_back_to_the_injected_manager() {
            let injected = manager();
            let chosen = choose_mcp_secrets(Some("broker"), None, Some(&injected))
                .expect("the injected manager is the fallback");
            assert!(same(&chosen, &injected));
        }

        /// A standalone runner, which is today's behaviour for both paths.
        #[test]
        fn with_no_injected_manager_the_env_built_one_is_used() {
            let env_built = manager();
            let chosen = choose_mcp_secrets(None, Some(env_built.clone()), None)
                .expect("the env-built manager is the only choice");
            assert!(same(&chosen, &env_built));
        }

        #[test]
        fn with_nothing_available_there_is_no_manager() {
            assert!(choose_mcp_secrets(None, None, None).is_none());
        }
    }

    #[cfg(test)]
    mod tests {
        use super::build_secrets_manager;
        use serial_test::serial;
        use std::env;

        // The crate denies unsafe, but `std::env::set_var`/`remove_var` are
        // `unsafe` as of edition 2024. These helpers are only reachable from
        // `#[serial]` tests, so no other thread observes the environment
        // mid-mutation.
        #[allow(unsafe_code)]
        fn set(key: &str, val: &str) {
            // SAFETY: env-mutating tests are serialized via `#[serial]`.
            unsafe { env::set_var(key, val) };
        }
        #[allow(unsafe_code)]
        fn unset(key: &str) {
            // SAFETY: env-mutating tests are serialized via `#[serial]`.
            unsafe { env::remove_var(key) };
        }

        /// Restores a snapshot of env vars on drop, so a panicking assertion
        /// partway through a test body still restores `SECRETS_BACKEND`/
        /// `GREENTIC_ENV` and cannot leak state into other tests running in
        /// the same process (even under `#[serial]`, an unwind must not skip
        /// cleanup).
        struct EnvRestoreGuard {
            previous: Vec<(&'static str, Option<String>)>,
        }

        impl Drop for EnvRestoreGuard {
            fn drop(&mut self) {
                for (k, v) in &self.previous {
                    match v {
                        Some(value) => set(k, value),
                        None => unset(k),
                    }
                }
            }
        }

        /// Snapshot + apply a small set of env vars around a test body,
        /// restoring the snapshot via [`EnvRestoreGuard::drop`] even if the
        /// body panics.
        fn with_env<F: FnOnce()>(vars: &[(&'static str, Option<&str>)], body: F) {
            let _guard = EnvRestoreGuard {
                previous: vars.iter().map(|(k, _)| (*k, env::var(k).ok())).collect(),
            };
            for (k, v) in vars {
                match v {
                    Some(value) => set(k, value),
                    None => unset(k),
                }
            }
            body();
        }

        #[test]
        #[serial]
        fn secrets_from_env_none_when_backend_unsupported() {
            with_env(
                &[
                    ("SECRETS_BACKEND", Some("vault")),
                    ("GREENTIC_ENV", Some("local")),
                ],
                || {
                    assert!(
                        build_secrets_manager().is_none(),
                        "an unsupported SECRETS_BACKEND must degrade to no-secrets, not panic"
                    );
                },
            );
        }

        #[test]
        #[serial]
        fn secrets_from_env_none_when_env_backend_disallowed_in_prod() {
            with_env(
                &[
                    ("SECRETS_BACKEND", Some("env")),
                    ("GREENTIC_ENV", Some("prod")),
                ],
                || {
                    assert!(
                        build_secrets_manager().is_none(),
                        "the env secrets backend is dev/test-only; prod must yield no-secrets \
                         rather than failing the MCP node"
                    );
                },
            );
        }

        #[test]
        #[serial]
        fn secrets_from_env_some_when_env_backend_allowed() {
            with_env(
                &[
                    ("SECRETS_BACKEND", Some("env")),
                    ("GREENTIC_ENV", Some("local")),
                ],
                || {
                    assert!(
                        build_secrets_manager().is_some(),
                        "a valid SECRETS_BACKEND in an allowed env must produce a manager, \
                         so local-wasm tools actually gain secrets access"
                    );
                },
            );
        }
    }
}

#[cfg(feature = "agentic-worker")]
pub(crate) use aw::source_from_env;

use serde_json::Value;

/// Read a non-empty string field from a JSON object payload, trimming
/// surrounding whitespace. Returns `None` when the payload is not an object,
/// the key is absent, the value is not a string, or the trimmed value is empty.
pub(crate) fn str_field(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Extract the `(server, tool)` pair from an MCP node payload/config object
/// (LOCKED ENCODING v2). Both keys must be present and non-empty; otherwise the
/// caller falls back to the legacy `operation`/`mcp:` encoding.
pub(crate) fn server_tool_from_payload(payload: &Value) -> Option<(String, String)> {
    let server = str_field(payload, "server")?;
    let tool = str_field(payload, "tool")?;
    Some((server, tool))
}

#[cfg(test)]
mod tests {
    use super::{server_tool_from_payload, str_field};
    use serde_json::json;

    #[test]
    fn reads_server_and_tool_from_payload() {
        assert_eq!(
            server_tool_from_payload(&json!({ "server": "github", "tool": "get_issue" })),
            Some(("github".to_string(), "get_issue".to_string()))
        );
    }

    #[test]
    fn tolerates_dotted_tool_names_in_payload() {
        assert_eq!(
            server_tool_from_payload(&json!({ "server": "srv", "tool": "do.thing" })),
            Some(("srv".to_string(), "do.thing".to_string()))
        );
    }

    #[test]
    fn missing_or_empty_fields_yield_none() {
        assert_eq!(
            server_tool_from_payload(&json!({ "server": "github" })),
            None
        );
        assert_eq!(
            server_tool_from_payload(&json!({ "server": "", "tool": "get_issue" })),
            None
        );
        assert_eq!(
            server_tool_from_payload(&json!({ "server": "github", "tool": "   " })),
            None
        );
        assert_eq!(server_tool_from_payload(&json!("not an object")), None);
    }

    #[test]
    fn str_field_trims_and_rejects_empty() {
        assert_eq!(
            str_field(&json!({ "k": "  v  " }), "k"),
            Some("v".to_string())
        );
        assert_eq!(str_field(&json!({ "k": "" }), "k"), None);
        assert_eq!(str_field(&json!({ "k": 7 }), "k"), None);
        assert_eq!(str_field(&json!({}), "missing"), None);
    }
}
