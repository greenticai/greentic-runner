use std::net::SocketAddr;

use axum::Json;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::{FromRef, FromRequestParts};
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName};
use serde_json::json;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::runner::ServerState;

#[derive(Clone, Default)]
pub struct AdminAuth {
    token: Option<String>,
    /// Dedicated `OPERATOR_TOKEN`; takes precedence over `token` for
    /// `/operator/op/invoke` only.
    operator_token: Option<String>,
}

/// Normalise a configured token the way a presented bearer is normalised
/// (trimmed): surrounding whitespace (a trailing newline from a secret file or
/// env) is dropped, and a token that is blank after trimming counts as unset.
pub(crate) fn normalize_token(token: Option<String>) -> Option<String> {
    token
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

impl AdminAuth {
    /// Build the admin guard.
    ///
    /// With no token, admin routes are reachable from loopback peers only.
    /// That check looks at the TCP peer address, so a reverse proxy running on
    /// the same host makes every request look like loopback. Set `ADMIN_TOKEN`
    /// whenever a local proxy fronts this process.
    pub fn new(token: Option<String>) -> Self {
        let token = normalize_token(token);
        if token.is_none() {
            tracing::warn!(
                "no admin token configured: /admin and /agent endpoints accept any loopback peer; \
                 a local reverse proxy would bypass this check, set ADMIN_TOKEN to require a bearer token"
            );
        }
        let operator_token = normalize_token(std::env::var("OPERATOR_TOKEN").ok());
        Self {
            token,
            operator_token,
        }
    }

    /// Override the dedicated operator token (`OPERATOR_TOKEN`).
    pub fn with_operator_token(mut self, token: Option<String>) -> Self {
        self.operator_token = normalize_token(token);
        self
    }

    /// Token guarding `/operator/op/invoke`: `OPERATOR_TOKEN` if set, else the
    /// admin token. `None` keeps the route open (backward compatible).
    fn effective_operator_token(&self) -> Option<&str> {
        self.operator_token.as_deref().or(self.token.as_deref())
    }

    /// Whether `/operator/op/invoke` requires a bearer token.
    pub fn operator_protected(&self) -> bool {
        self.effective_operator_token().is_some()
    }

    /// Check the operator token may arrive in
    /// EITHER `Authorization: Bearer` or the dedicated `X-Operator-Token`
    /// header. The second exists because `Authorization` is also what
    /// `TENANT_RESOLVER=jwt` reads, so a caller needing both a tenant JWT and
    /// the operator token could not send them together. Both candidates are
    /// compared in constant time; either matching is enough.
    fn authorize_operator_headers(
        &self,
        bearer: Option<&str>,
        header_token: Option<&str>,
    ) -> Result<(), StatusCode> {
        match self.effective_operator_token() {
            None => Ok(()),
            Some(expected) => {
                let bearer_ok = bearer
                    .map(|t| constant_time_eq(t.as_bytes(), expected.as_bytes()))
                    .unwrap_or(false);
                let header_ok = header_token
                    .map(|t| constant_time_eq(t.as_bytes(), expected.as_bytes()))
                    .unwrap_or(false);
                if bearer_ok || header_ok {
                    Ok(())
                } else {
                    Err(StatusCode::UNAUTHORIZED)
                }
            }
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

/// Route layer for `/operator/op/invoke`: 401 unless the request carries the
/// configured operator/admin bearer token. Open when no token is configured.
pub(crate) async fn require_operator(
    axum::extract::State(state): axum::extract::State<ServerState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let bearer = bearer_from_headers(request.headers(), &AUTHORIZATION);
    let header_token = request
        .headers()
        .get(OPERATOR_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty());
    match state
        .admin
        .authorize_operator_headers(bearer.as_deref(), header_token)
    {
        Ok(()) => next.run(request).await,
        Err(status) => {
            (status, Json(json!({ "error": "operator token required" }))).into_response()
        }
    }
}

/// Dedicated header carrying the operator token, for callers whose
/// `Authorization` header is already taken by a tenant JWT.
pub(crate) const OPERATOR_TOKEN_HEADER: &str = "x-operator-token";

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
    bearer_from_headers(&parts.headers, &AUTHORIZATION)
}

/// Shared bearer-token extractor: case-insensitive `Bearer` scheme, token
/// trimmed, empty tokens rejected. Used by admin auth and tenant routing.
pub(crate) fn bearer_from_headers(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    let header = headers.get(name)?.to_str().ok()?;
    let (scheme, value) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_string())
}

/// Compare secrets without leaking their length: both sides are hashed to a
/// fixed-size digest first, then compared with `subtle`.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let left = Sha256::digest(a);
    let right = Sha256::digest(b);
    left.ct_eq(&right).into()
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
    fn operator_open_without_token() {
        let auth = AdminAuth::default();
        assert!(!auth.operator_protected());
        assert!(auth.authorize_operator_headers(None, None).is_ok());
    }

    #[test]
    fn operator_requires_admin_token_when_set() {
        let auth = AdminAuth {
            token: Some("admin".into()),
            ..AdminAuth::default()
        };
        assert_eq!(
            auth.authorize_operator_headers(None, None),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert_eq!(
            auth.authorize_operator_headers(Some("nope"), None),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert!(auth.authorize_operator_headers(Some("admin"), None).is_ok());
    }

    #[test]
    fn operator_token_accepted_from_x_operator_token_or_bearer() {
        let auth = AdminAuth::new(Some("admin".into())).with_operator_token(Some("op".into()));
        // JWT in Authorization, operator token in the dedicated header.
        assert!(
            auth.authorize_operator_headers(Some("some.jwt.value"), Some("op"))
                .is_ok()
        );
        assert!(auth.authorize_operator_headers(Some("op"), None).is_ok());
        assert!(auth.authorize_operator_headers(None, Some("op")).is_ok());
        assert_eq!(
            auth.authorize_operator_headers(Some("jwt"), Some("wrong")),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert_eq!(
            auth.authorize_operator_headers(None, None),
            Err(StatusCode::UNAUTHORIZED)
        );
        // The admin token is not accepted when a dedicated operator token is set.
        assert_eq!(
            auth.authorize_operator_headers(None, Some("admin")),
            Err(StatusCode::UNAUTHORIZED)
        );
        let open = AdminAuth::new(None).with_operator_token(None);
        assert!(open.authorize_operator_headers(None, None).is_ok());
    }

    #[test]
    fn operator_token_takes_precedence_over_admin_token() {
        let auth = AdminAuth {
            token: Some("admin".into()),
            ..AdminAuth::default()
        }
        .with_operator_token(Some("op".into()));
        assert!(auth.authorize_operator_headers(Some("op"), None).is_ok());
        assert_eq!(
            auth.authorize_operator_headers(Some("admin"), None),
            Err(StatusCode::UNAUTHORIZED)
        );
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
            ..AdminAuth::default()
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
        assert!(!constant_time_eq(b"", b"longer"));
        assert!(constant_time_eq(b"same", b"same"));
    }

    #[test]
    fn configured_tokens_are_trimmed_and_blank_ones_dropped() {
        assert_eq!(
            normalize_token(Some("  secret\n".into())).as_deref(),
            Some("secret")
        );
        assert_eq!(normalize_token(Some(" \t\n".into())), None);
        assert_eq!(normalize_token(None), None);
        // A token configured with a trailing newline must match the (trimmed)
        // bearer a client presents.
        let auth = AdminAuth::new(Some("tok\n".into())).with_operator_token(Some(" op \n".into()));
        assert_eq!(auth.effective_operator_token(), Some("op"));
        assert_eq!(
            AdminAuth::new(Some("tok\n".into())).effective_operator_token(),
            Some("tok")
        );
        // Whitespace-only operator token falls back to the admin token.
        let blank_op = AdminAuth::new(Some("tok".into())).with_operator_token(Some("  ".into()));
        assert_eq!(blank_op.effective_operator_token(), Some("tok"));
    }

    #[test]
    fn bearer_extractor_trims_and_rejects_empty() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "BEARER   tok  ".parse().unwrap());
        assert_eq!(
            bearer_from_headers(&headers, &AUTHORIZATION).as_deref(),
            Some("tok")
        );
        headers.insert(AUTHORIZATION, "Bearer    ".parse().unwrap());
        assert_eq!(bearer_from_headers(&headers, &AUTHORIZATION), None);
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
