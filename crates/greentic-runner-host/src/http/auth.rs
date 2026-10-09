use std::net::SocketAddr;

use axum::Json;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::{FromRef, FromRequestParts};
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use serde_json::json;

use crate::runner::ServerState;

#[derive(Clone, Default)]
pub struct AdminAuth {
    token: Option<String>,
}

impl AdminAuth {
    pub fn new(token: Option<String>) -> Self {
        Self {
            token: token.filter(|v| !v.is_empty()),
        }
    }

    fn authorize(&self, addr: SocketAddr, bearer: Option<&str>) -> Result<(), StatusCode> {
        if let Some(expected) = &self.token {
            let token = bearer.ok_or(StatusCode::UNAUTHORIZED)?;
            if constant_time_eq(token.as_bytes(), expected.as_bytes()) {
                Ok(())
            } else {
                Err(StatusCode::UNAUTHORIZED)
            }
        } else if addr.ip().is_loopback() {
            Ok(())
        } else {
            Err(StatusCode::FORBIDDEN)
        }
    }
}

pub struct AdminGuard;

impl<S> FromRequestParts<S> for AdminGuard
where
    ServerState: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        let server_state = ServerState::from_ref(state);
        let admin = server_state.admin.clone();
        let addr = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|info| info.0);
        let bearer = extract_bearer(parts);

        async move {
            let addr = addr.ok_or((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "connect info unavailable" })),
            ))?;
            admin.authorize(addr, bearer.as_deref()).map_err(|status| {
                (
                    status,
                    Json(json!({
                        "error": if status == StatusCode::UNAUTHORIZED {
                            "admin token required"
                        } else {
                            "admin access restricted"
                        }
                    })),
                )
            })?;
            Ok(AdminGuard)
        }
    }
}

fn extract_bearer(parts: &Parts) -> Option<String> {
    let header = parts.headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, value) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    Some(value.trim().to_string())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&left, &right) in a.iter().zip(b.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::FromRef;
    use axum::extract::connect_info::ConnectInfo;
    use axum::http::Request;
    use std::sync::Arc;

    use crate::http::health::HealthState;
    use crate::routing::{RoutingConfig, TenantRouting};
    use crate::runner::ServerState;
    use crate::runtime::ActivePacks;

    #[derive(Clone)]
    struct AppState {
        server: ServerState,
    }

    impl FromRef<AppState> for ServerState {
        fn from_ref(input: &AppState) -> Self {
            input.server.clone()
        }
    }

    fn server_state(admin: AdminAuth) -> AppState {
        let host = crate::host::RunnerHost::for_test();
        AppState {
            server: ServerState {
                active: Arc::new(ActivePacks::new()),
                routing: TenantRouting::new(RoutingConfig::default()),
                health: Arc::new(HealthState::new()),
                reload: None,
                admin,
                #[cfg(feature = "agentic-worker")]
                stream_observers: host.stream_observers(),
                #[cfg(feature = "agentic-worker")]
                ext_runtime: None,
                host,
                sql: crate::sql::SqlGateway::new(std::collections::HashMap::new(), String::new()),
            },
        }
    }

    #[test]
    fn loopback_without_token_is_allowed() {
        let auth = AdminAuth::new(None);
        assert!(auth.authorize("127.0.0.1:0".parse().unwrap(), None).is_ok());
    }

    #[test]
    fn remote_without_token_is_forbidden() {
        let auth = AdminAuth::new(None);
        assert_eq!(
            auth.authorize("10.0.0.1:0".parse().unwrap(), None),
            Err(StatusCode::FORBIDDEN)
        );
    }

    #[test]
    fn token_requires_bearer() {
        let auth = AdminAuth {
            token: Some("secret".into()),
        };
        assert_eq!(
            auth.authorize("127.0.0.1:0".parse().unwrap(), None),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert!(
            auth.authorize("127.0.0.1:0".parse().unwrap(), Some("secret"))
                .is_ok()
        );
    }

    #[test]
    fn bearer_scheme_is_case_insensitive() {
        let (parts, _) = axum::http::Request::builder()
            .header(AUTHORIZATION, "bearer secret")
            .body(())
            .expect("request")
            .into_parts();

        assert_eq!(extract_bearer(&parts).as_deref(), Some("secret"));
    }

    #[test]
    fn non_bearer_authorization_header_is_rejected() {
        let (parts, _) = axum::http::Request::builder()
            .header(AUTHORIZATION, "Basic dXNlcjpzZWNyZXQ=")
            .body(())
            .expect("request")
            .into_parts();

        assert_eq!(extract_bearer(&parts), None);
    }

    #[test]
    fn empty_admin_token_is_treated_as_disabled() {
        let auth = AdminAuth::new(Some(String::new()));
        assert!(auth.authorize("127.0.0.1:0".parse().unwrap(), None).is_ok());
    }

    #[test]
    fn wrong_bearer_token_is_rejected() {
        let auth = AdminAuth::new(Some("secret".into()));
        assert_eq!(
            auth.authorize("127.0.0.1:0".parse().unwrap(), Some("wrong")),
            Err(StatusCode::UNAUTHORIZED)
        );
    }

    #[test]
    fn constant_time_eq_rejects_length_mismatch() {
        assert!(!constant_time_eq(b"short", b"longer"));
    }

    #[test]
    fn malformed_authorization_header_is_rejected() {
        let (parts, _) = axum::http::Request::builder()
            .header(AUTHORIZATION, "Bearer")
            .body(())
            .expect("request")
            .into_parts();

        assert_eq!(extract_bearer(&parts), None);
    }

    #[tokio::test]
    async fn admin_guard_rejects_missing_connect_info() {
        let (mut parts, _) = Request::builder().body(()).expect("request").into_parts();
        let state = server_state(AdminAuth::default());

        let rejection = match AdminGuard::from_request_parts(&mut parts, &state).await {
            Ok(_) => panic!("missing connect info should reject"),
            Err(rejection) => rejection,
        };

        assert_eq!(rejection.0, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(rejection.1.0["error"], "connect info unavailable");
    }

    #[tokio::test]
    async fn admin_guard_rejects_wrong_remote_token() {
        let (mut parts, _) = Request::builder()
            .header(AUTHORIZATION, "Bearer wrong")
            .body(())
            .expect("request")
            .into_parts();
        parts.extensions.insert(ConnectInfo(
            "10.0.0.2:8080".parse::<std::net::SocketAddr>().unwrap(),
        ));
        let state = server_state(AdminAuth::new(Some("secret".into())));

        let rejection = match AdminGuard::from_request_parts(&mut parts, &state).await {
            Ok(_) => panic!("wrong token should reject"),
            Err(rejection) => rejection,
        };

        assert_eq!(rejection.0, StatusCode::UNAUTHORIZED);
        assert_eq!(rejection.1.0["error"], "admin token required");
    }

    #[tokio::test]
    async fn admin_guard_allows_loopback_without_token_when_disabled() {
        let (mut parts, _) = Request::builder().body(()).expect("request").into_parts();
        parts.extensions.insert(ConnectInfo(
            "127.0.0.1:8080".parse::<std::net::SocketAddr>().unwrap(),
        ));
        let state = server_state(AdminAuth::default());

        AdminGuard::from_request_parts(&mut parts, &state)
            .await
            .expect("loopback should pass without token");
    }
}
