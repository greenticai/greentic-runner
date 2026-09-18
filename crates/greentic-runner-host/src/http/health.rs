use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Error;
use axum::Json;
use axum::extract::State;
use axum::response::IntoResponse;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::runner::ServerState;

#[derive(Default)]
pub struct HealthState {
    telemetry_ready: AtomicBool,
    secrets_ready: AtomicBool,
    meta: parking_lot::Mutex<HealthMeta>,
}

#[derive(Default, Clone)]
struct HealthMeta {
    last_reload: Option<OffsetDateTime>,
    last_error: Option<String>,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            telemetry_ready: AtomicBool::new(false),
            secrets_ready: AtomicBool::new(false),
            meta: parking_lot::Mutex::new(HealthMeta::default()),
        }
    }

    pub fn mark_telemetry_ready(&self) {
        self.telemetry_ready.store(true, Ordering::SeqCst);
    }

    pub fn mark_secrets_ready(&self) {
        self.secrets_ready.store(true, Ordering::SeqCst);
    }

    /// Mark all readiness checks as healthy.
    pub fn set_ready(&self) {
        self.mark_telemetry_ready();
        self.mark_secrets_ready();
    }

    pub fn record_reload_success(&self) {
        let mut meta = self.meta.lock();
        meta.last_reload = Some(OffsetDateTime::now_utc());
        meta.last_error = None;
    }

    pub fn record_reload_error(&self, err: &Error) {
        let mut meta = self.meta.lock();
        meta.last_error = Some(err.to_string());
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        let meta = self.meta.lock().clone();
        HealthSnapshot {
            telemetry_ready: self.telemetry_ready.load(Ordering::SeqCst),
            secrets_ready: self.secrets_ready.load(Ordering::SeqCst),
            last_reload: meta.last_reload,
            last_error: meta.last_error,
        }
    }
}

pub struct HealthSnapshot {
    pub telemetry_ready: bool,
    pub secrets_ready: bool,
    pub last_reload: Option<OffsetDateTime>,
    pub last_error: Option<String>,
}

pub async fn handler(State(state): State<ServerState>) -> impl IntoResponse {
    let snapshot = state.health.snapshot();
    let packs = state.active.len();
    let status = if snapshot.telemetry_ready && snapshot.secrets_ready && packs > 0 {
        "ok"
    } else {
        "degraded"
    };
    let last_reload = snapshot.last_reload.and_then(|ts| ts.format(&Rfc3339).ok());
    Json(serde_json::json!({
        "status": status,
        "telemetry_ready": snapshot.telemetry_ready,
        "secrets_ready": snapshot.secrets_ready,
        "active_packs": packs,
        "last_reload": last_reload,
        "last_error": snapshot.last_error,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::auth::AdminAuth;
    use crate::routing::{RoutingConfig, TenantRouting};
    use crate::runner::ServerState;
    use crate::runtime::ActivePacks;
    use axum::body::to_bytes;
    use axum::response::IntoResponse;
    use std::sync::Arc;

    fn state() -> ServerState {
        let host = crate::host::RunnerHost::for_test();
        ServerState {
            active: Arc::new(ActivePacks::new()),
            routing: TenantRouting::new(RoutingConfig::default()),
            health: Arc::new(HealthState::new()),
            reload: None,
            admin: AdminAuth::default(),
            #[cfg(feature = "agentic-worker")]
            stream_observers: host.stream_observers(),
            #[cfg(feature = "agentic-worker")]
            ext_runtime: None,
            host,
            sql: crate::sql::SqlGateway::new(std::collections::HashMap::new(), String::new()),
        }
    }

    #[test]
    fn snapshot_tracks_readiness_and_errors() {
        let health = HealthState::new();
        health.mark_telemetry_ready();
        health.record_reload_error(&anyhow::anyhow!("reload failed"));
        let before = health.snapshot();
        assert!(before.telemetry_ready);
        assert!(!before.secrets_ready);
        assert_eq!(before.last_error.as_deref(), Some("reload failed"));

        health.mark_secrets_ready();
        health.record_reload_success();
        let after = health.snapshot();
        assert!(after.telemetry_ready);
        assert!(after.secrets_ready);
        assert!(after.last_reload.is_some());
        assert!(after.last_error.is_none());
    }

    #[tokio::test]
    async fn handler_reports_degraded_without_active_packs() {
        let state = state();
        state.health.set_ready();

        let response = handler(State(state)).await.into_response();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json");

        assert_eq!(json["status"], "degraded");
        assert_eq!(json["telemetry_ready"], true);
        assert_eq!(json["secrets_ready"], true);
        assert_eq!(json["active_packs"], 0);
    }
}
