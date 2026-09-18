pub mod adapt_events_email;
pub mod adapt_timer;
pub mod agent_node;
#[cfg(feature = "agentic-worker")]
pub(crate) mod aw_backends;
pub mod component_invoker;
pub mod contract_cache;
pub mod contract_introspection;
pub mod dispatch_listener;
pub mod engine;
pub mod flow_adapter;
#[cfg(feature = "agentic-worker")]
pub mod flow_invoker;
pub mod graph_node;
pub mod i18n;
pub mod invocation;
#[cfg(feature = "knowledge-chronicle")]
pub mod knowledge_corpus;
#[cfg(feature = "agentic-worker")]
pub mod knowledge_ext;
#[cfg(feature = "knowledge-chronicle")]
pub mod knowledge_mount;
#[cfg(feature = "long-term-chronicle")]
pub mod long_term_memory;
pub mod mcp_node;
pub mod mcp_pack_routes;
#[cfg(feature = "agentic-worker")]
pub mod mcp_warm_listener;
pub mod mocks;
pub mod operala_node;
pub mod operator;
#[cfg(feature = "agentic-worker")]
pub mod pack_extensions;
pub mod remote_dispatch;
pub mod runtime_session_resumer;
pub mod schema_validator;
#[cfg(feature = "agentic-worker")]
pub mod sorx_invoker;
pub mod templating;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::routing::{get, post};
use axum::{Router, serve};
use tokio::net::TcpListener;

use crate::host::RunnerHost;
use crate::http::{self, admin, auth::AdminAuth, health::HealthState};
use crate::routing::TenantRouting;
use crate::runtime::ActivePacks;
use crate::sql::SqlGateway;
use crate::watcher::PackReloadHandle;

pub struct HostServer {
    addr: SocketAddr,
    router: Router,
    _state: ServerState,
}

impl HostServer {
    pub fn new(
        port: u16,
        active: Arc<ActivePacks>,
        routing: TenantRouting,
        health: Arc<HealthState>,
        reload: Option<PackReloadHandle>,
        admin: AdminAuth,
        host: Arc<RunnerHost>,
    ) -> Result<Self> {
        Self::with_sql(port, active, routing, health, reload, admin, host, None)
    }

    /// Like [`HostServer::new`] but wires an optional [`SqlGateway`] into the
    /// server state.  When `sql_gateway` is `None` the server uses an empty
    /// gateway that returns 404/401 for all `/sql` routes — safe by default.
    // Builder param set mirrors the existing fields plus `host`; a config-struct
    // refactor is deferred. See B0 plan.
    #[allow(clippy::too_many_arguments)]
    pub fn with_sql(
        port: u16,
        active: Arc<ActivePacks>,
        routing: TenantRouting,
        health: Arc<HealthState>,
        reload: Option<PackReloadHandle>,
        admin: AdminAuth,
        host: Arc<RunnerHost>,
        sql_gateway: Option<SqlGateway>,
    ) -> Result<Self> {
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let sql = sql_gateway
            .unwrap_or_else(|| SqlGateway::new(std::collections::HashMap::new(), String::new()));
        let state = ServerState {
            active,
            routing,
            health,
            reload,
            admin,
            // Clone off `host` (not a fresh registry): the SSE stream
            // handler (writer) and `RuntimeAgentNodeHandler::execute`
            // (reader, wired in at `TenantRuntime` construction) must share
            // the exact same `Arc<DashMap<..>>` or a registration here would
            // never be seen by the agent step.
            #[cfg(feature = "agentic-worker")]
            stream_observers: host.stream_observers(),
            // Built here rather than reused from `host`: the per-tenant runtimes
            // are constructed later, inside `TenantRuntime`, with per-tenant
            // secrets. This blocks boot on a full extra set of WASM loads,
            // landing entirely in the pre-bind window — `/healthz` cannot answer
            // at all until this returns. Measured: ~80s to readiness with 81
            // extensions installed, on top of whatever the per-tenant load
            // already costs. A deployment health-gate must tolerate that delay.
            //
            // No packs are passed: pack-carried extensions are per-tenant, so
            // there is no process-wide answer for them (see the field's doc
            // comment).
            #[cfg(feature = "agentic-worker")]
            ext_runtime: crate::runner::agent_node::build_ext_runtime(
                std::sync::Arc::new(crate::runner::agent_node::EnvSecretsBackend),
                None,
                &[],
            ),
            host,
            sql,
        };
        let router = router(state.clone());
        Ok(Self {
            addr,
            router,
            _state: state,
        })
    }

    pub async fn serve(self) -> Result<()> {
        tracing::info!(addr = %self.addr, "starting host server");
        let listener = TcpListener::bind(self.addr).await?;
        serve(
            listener,
            self.router
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await?;
        Ok(())
    }
}

/// Assemble the full host router for `state`.
///
/// Extracted from [`HostServer::with_sql`] so tests can drive the *assembled*
/// router — route registration included — without binding a socket. Mirrors the
/// `sql::routes::router` precedent.
pub(crate) fn router(state: ServerState) -> Router {
    let router = Router::new()
        .route("/operator/op/invoke", post(operator::invoke))
        .route("/healthz", get(http::health::handler))
        .route("/admin/packs/status", get(admin::status))
        .route("/admin/packs/reload", post(admin::reload))
        .route("/agent/chat", post(crate::http::agent_chat::agent_chat));
    #[cfg(feature = "agentic-worker")]
    let router = router
        .route(
            "/agent/chat/stream",
            post(crate::http::agent_chat::agent_chat_stream),
        )
        .route("/admin/capabilities", get(admin::capabilities));
    router
        .route(
            "/sql/{conn}/schema",
            get(crate::sql::routes::schema_handler),
        )
        .route("/sql/{conn}/query", post(crate::sql::routes::query_handler))
        .with_state(state)
}

#[derive(Clone)]
pub struct ServerState {
    pub active: Arc<ActivePacks>,
    pub routing: TenantRouting,
    pub health: Arc<HealthState>,
    pub reload: Option<PackReloadHandle>,
    pub admin: AdminAuth,
    /// The runner host instance used by the `POST /agent/chat` handler to
    /// dispatch activity turns to loaded worker packs.
    pub host: Arc<RunnerHost>,
    /// SQL gateway for `/sql/{conn}/schema` and `/sql/{conn}/query`.
    /// Defaults to an empty gateway (returns 404/401 for all connections)
    /// when no SQL connections are configured.
    pub sql: SqlGateway,
    /// Session-id → active streaming observer (R2). Cloned from
    /// `host.stream_observers()` at server-build time so it is the exact
    /// same registry `RuntimeAgentNodeHandler::execute` reads from — a
    /// `POST /agent/chat/stream` handler inserts an observer here before
    /// dispatching a turn.
    #[cfg(feature = "agentic-worker")]
    pub stream_observers: crate::http::agent_stream::StreamObserverRegistry,
    /// Extension runtime backing `GET /admin/capabilities`, so an operator
    /// console can see which capabilities this runner actually has installed.
    ///
    /// Built once at server-build time. This is a *separate* instance from the
    /// per-tenant runtimes in `agent_node::build_ext_runtime`, which need
    /// per-tenant secrets backends — so it costs one extra set of WASM loads at
    /// boot. All tenants scan the same `GREENTIC_EXTENSIONS_DIR/design/`, so a
    /// process-level answer is correct for the ON-DISK half.
    ///
    /// It is NOT correct for the pack-carried half: extensions travelling inside
    /// a `.gtpack` (`crate::runner::pack_extensions`) belong to the tenant whose
    /// pack carries them, so this instance is built with no packs and reports
    /// only what the host has installed. An operator reading
    /// `GET /admin/capabilities` sees the host's floor, not any one tenant's
    /// total.
    ///
    /// `None` only when `ExtensionRuntime::new` itself fails. A missing/absent
    /// extension directory does NOT produce `None`: `scan_kind_dir` errors,
    /// logs a warning, and the runtime still comes back `Some` with an empty
    /// registry (`agent_node.rs:918-948`). Either way the handler reports an
    /// empty list rather than failing.
    #[cfg(feature = "agentic-worker")]
    pub ext_runtime: Option<std::sync::Arc<greentic_ext_runtime::ExtensionRuntime>>,
}

impl ServerState {
    /// Test-only builder: an empty host (`RunnerHost::for_test`, no loaded
    /// packs), default routing, and — when `agentic-worker` is enabled — an
    /// empty streaming-observer registry cloned from the same host. Mirrors
    /// the inline `state()` test helpers in `http/admin.rs` / `http/auth.rs`
    /// / `http/health.rs`, but lives here (next to the struct) so it's
    /// reusable from `http/agent_chat.rs`'s SSE core test.
    // Consumed by the `agentic-worker`-gated SSE core test in
    // `http/agent_chat.rs` and by `router_tests` in this file — gating on both
    // keeps a lean (no `agentic-worker`) test build free of dead-code warnings.
    #[cfg(all(test, feature = "agentic-worker"))]
    pub(crate) fn for_test() -> Self {
        let host = RunnerHost::for_test();
        Self {
            active: Arc::new(ActivePacks::new()),
            routing: TenantRouting::new(crate::routing::RoutingConfig::default()),
            health: Arc::new(HealthState::new()),
            reload: None,
            admin: AdminAuth::default(),
            #[cfg(feature = "agentic-worker")]
            stream_observers: host.stream_observers(),
            #[cfg(feature = "agentic-worker")]
            ext_runtime: None,
            host,
            sql: SqlGateway::new(std::collections::HashMap::new(), String::new()),
        }
    }
}

impl axum::extract::FromRef<ServerState> for SqlGateway {
    fn from_ref(state: &ServerState) -> Self {
        state.sql.clone()
    }
}

#[cfg(all(test, feature = "agentic-worker"))]
mod router_tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt as _;

    /// `AdminGuard` 500s without `ConnectInfo`; `oneshot` does not supply it.
    fn loopback() -> ConnectInfo<SocketAddr> {
        ConnectInfo("127.0.0.1:8080".parse::<SocketAddr>().unwrap())
    }

    #[tokio::test]
    async fn assembled_router_serves_admin_packs_status() {
        let app = router(ServerState::for_test());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/packs/status")
                    .extension(loopback())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Anti-regression for the greentic-designer #796 class of bug: a handler
    /// that exists but is never registered. Without the `.route(...)` line this
    /// returns 404.
    #[tokio::test]
    async fn assembled_router_serves_admin_capabilities() {
        let app = router(ServerState::for_test());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/capabilities")
                    .extension(loopback())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // `ServerState::for_test()` has no ext_runtime, so the list is empty —
        // but the envelope key must still be present and an array.
        assert_eq!(body, serde_json::json!({ "capabilities": [] }));
    }
}
