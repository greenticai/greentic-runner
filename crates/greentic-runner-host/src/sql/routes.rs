//! axum handlers for `/sql/<conn>/schema` and `/sql/<conn>/query`.
//!
//! Both handlers require a `Bearer <token>` `Authorization` header that
//! matches the gateway's configured token (constant-time compare, see
//! [`crate::sql::SqlGateway::check_token`]).  On success the schema is
//! cached per connection name so repeated `/schema` calls hit memory only.

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::json;

use crate::sql::{execute, introspect};

// ── Request body ──────────────────────────────────────────────────────────────

/// Body for `POST /sql/<conn>/query`.
#[derive(Deserialize)]
pub struct QueryBody {
    /// The SQL statement to execute (read-only).
    pub sql: String,
    /// Maximum number of rows to return (clamped to 1..=1000; default 100).
    #[serde(default = "default_max_rows")]
    pub max_rows: u32,
}

fn default_max_rows() -> u32 {
    100
}

// ── Bearer helper ─────────────────────────────────────────────────────────────

/// Extract the token from `Authorization: Bearer <token>` (shared extractor:
/// case-insensitive scheme, trimmed, empty rejected).
fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    crate::http::auth::bearer_from_headers(headers, &axum::http::header::AUTHORIZATION)
}

fn auth_ok(gateway: &crate::sql::SqlGateway, headers: &HeaderMap) -> bool {
    extract_bearer_token(headers).is_some_and(|token| gateway.check_token(&token))
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `GET /sql/<conn>/schema`
///
/// Returns the cached [`introspect::Schema`] for the named connection, or
/// introspects the database on first call and populates the cache.
///
/// # Responses
/// - `200 OK` + `Schema` JSON on success.
/// - `401 Unauthorized` when the `Authorization` header is missing or wrong.
/// - `404 Not Found` when `conn` is not configured in the gateway.
/// - `502 Bad Gateway` when introspection fails.
pub async fn schema_handler(
    State(gateway): State<crate::sql::SqlGateway>,
    headers: HeaderMap,
    Path(conn): Path<String>,
) -> impl IntoResponse {
    if !auth_ok(&gateway, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        )
            .into_response();
    }

    let Some(connection) = gateway.connection(&conn) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "unknown connection"})),
        )
            .into_response();
    };

    // Serve from cache when available (read lock — no contention on hot path).
    {
        let cache_read = gateway.cache().read().await;
        if let Some(cached_schema) = cache_read.get(&conn) {
            return (
                StatusCode::OK,
                Json(serde_json::to_value(cached_schema).unwrap_or_default()),
            )
                .into_response();
        }
    }

    // Cache miss: introspect, then populate under a write lock.
    match introspect::introspect(connection.engine, &connection.pool).await {
        Ok(schema) => {
            gateway
                .cache()
                .write()
                .await
                .insert(conn.clone(), schema.clone());
            (
                StatusCode::OK,
                Json(serde_json::to_value(&schema).unwrap_or_default()),
            )
                .into_response()
        }
        Err(introspect_error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": introspect_error})),
        )
            .into_response(),
    }
}

/// `POST /sql/<conn>/query`
///
/// Executes the SQL from `body.sql` read-only against the named connection,
/// returning at most `body.max_rows` (clamped to 1..=1000) rows.
///
/// # Responses
/// - `200 OK` + [`execute::QueryResult`] JSON on success.
/// - `401 Unauthorized` when the `Authorization` header is missing or wrong.
/// - `404 Not Found` when `conn` is not configured.
/// - `502 Bad Gateway` when the database returns an error.
pub async fn query_handler(
    State(gateway): State<crate::sql::SqlGateway>,
    headers: HeaderMap,
    Path(conn): Path<String>,
    Json(body): Json<QueryBody>,
) -> impl IntoResponse {
    if !auth_ok(&gateway, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        )
            .into_response();
    }

    let Some(connection) = gateway.connection(&conn) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "unknown connection"})),
        )
            .into_response();
    };

    let effective_max_rows = body.max_rows.clamp(1, 1000);

    match execute::run(
        connection.engine,
        &connection.pool,
        &body.sql,
        effective_max_rows,
    )
    .await
    {
        Ok(query_result) => (
            StatusCode::OK,
            Json(serde_json::to_value(&query_result).unwrap_or_default()),
        )
            .into_response(),
        Err(execute_error) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": execute_error})),
        )
            .into_response(),
    }
}

// ── Router builder ────────────────────────────────────────────────────────────

/// Build a self-contained axum [`Router`] serving the SQL gateway endpoints.
///
/// Mounts:
/// - `GET  /sql/{conn}/schema` → [`schema_handler`]
/// - `POST /sql/{conn}/query`  → [`query_handler`]
///
/// Returns a `Router<()>` (i.e. `Router` with no outstanding state parameter)
/// ready to pass directly to `axum::serve` for a dedicated SQL gateway port,
/// or to merge into another `Router<()>` via `Router::merge`.
///
/// # Example
/// ```no_run
/// # use greentic_runner_host::sql::SqlGateway;
/// # use std::collections::HashMap;
/// let gw = SqlGateway::new(HashMap::new(), "secret".to_string());
/// let app = greentic_runner_host::sql::routes::router(gw);
/// // axum::serve(listener, app).await.unwrap();
/// ```
pub fn router(gateway: crate::sql::SqlGateway) -> Router {
    Router::new()
        .route("/sql/{conn}/schema", get(schema_handler))
        .route("/sql/{conn}/query", post(query_handler))
        .with_state(gateway)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    use super::*;
    use crate::sql::config::Engine;
    use crate::sql::pool::ConnectionPool;
    use crate::sql::{SqlConnection, SqlGateway};

    /// Build a test [`Router`] backed by an in-memory SQLite gateway.
    ///
    /// Delegates to the public [`router`] helper so the helper is exercised by
    /// every existing test as well as the dedicated `router_helper_serves_routes`
    /// test below.
    async fn build_test_app(pool: sqlx::sqlite::SqlitePool, token: &str) -> Router {
        let mut connections = HashMap::new();
        connections.insert(
            "db".to_string(),
            SqlConnection {
                engine: Engine::Sqlite,
                pool: ConnectionPool::Sqlite(pool),
            },
        );
        let gateway = SqlGateway::new(connections, token.to_string());
        router(gateway)
    }

    /// Collect the response body bytes and deserialize as JSON.
    async fn collect_json(body: Body) -> serde_json::Value {
        let bytes = body.collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // ── /schema ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn schema_returns_200_with_valid_token() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE users (id INTEGER, email TEXT)")
            .execute(&pool)
            .await
            .unwrap();

        let app = build_test_app(pool, "secret").await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/sql/db/schema")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = collect_json(response.into_body()).await;
        assert_eq!(body["engine"], "sqlite");
        assert!(body["tables"].is_array());
        let tables = body["tables"].as_array().unwrap();
        assert!(tables.iter().any(|t| t["name"] == "users"));
    }

    #[tokio::test]
    async fn schema_returns_401_without_token() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();

        let app = build_test_app(pool, "secret").await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/sql/db/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn schema_returns_401_with_wrong_token() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();

        let app = build_test_app(pool, "secret").await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/sql/db/schema")
                    .header("authorization", "Bearer wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn schema_returns_404_for_unknown_connection() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();

        let app = build_test_app(pool, "tok").await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/sql/nope/schema")
                    .header("authorization", "Bearer tok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ── /query ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn query_returns_200_with_rows() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE items (id INTEGER, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO items VALUES (1,'alpha'),(2,'beta')")
            .execute(&pool)
            .await
            .unwrap();

        let app = build_test_app(pool, "tok").await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sql/db/query")
                    .header("authorization", "Bearer tok")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"SELECT id, name FROM items ORDER BY id","max_rows":10}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = collect_json(response.into_body()).await;
        assert_eq!(body["row_count"], 2);
        assert!(!body["truncated"].as_bool().unwrap());
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows[0][0], 1);
        assert_eq!(rows[0][1], "alpha");
    }

    #[tokio::test]
    async fn query_returns_401_without_token() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();

        let app = build_test_app(pool, "tok").await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sql/db/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"SELECT 1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn query_returns_404_for_unknown_connection() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();

        let app = build_test_app(pool, "tok").await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sql/nope/query")
                    .header("authorization", "Bearer tok")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"SELECT 1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn query_502_on_bad_sql() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();

        let app = build_test_app(pool, "tok").await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sql/db/query")
                    .header("authorization", "Bearer tok")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"NOT VALID SQL !!!"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    // ── router() helper ───────────────────────────────────────────────────────

    /// Verifies that the public `router()` builder wires the routes correctly:
    /// a valid Bearer token → 200, and a missing token → 401.
    #[tokio::test]
    async fn router_helper_serves_routes() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (id INTEGER)")
            .execute(&pool)
            .await
            .unwrap();

        let mut connections = HashMap::new();
        connections.insert(
            "db".to_string(),
            SqlConnection {
                engine: Engine::Sqlite,
                pool: ConnectionPool::Sqlite(pool),
            },
        );
        let gateway = SqlGateway::new(connections, "tok".to_string());
        let app = router(gateway);

        // 200 with valid token
        let ok_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/sql/db/schema")
                    .header("authorization", "Bearer tok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok_response.status(), StatusCode::OK);

        // 401 without token
        let unauth_response = app
            .oneshot(
                Request::builder()
                    .uri("/sql/db/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauth_response.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Full end-to-end composite ─────────────────────────────────────────────

    /// Drives schema + query through the same gateway (exercises cache path on
    /// the second schema call).
    #[tokio::test]
    async fn schema_and_query_e2e() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (id INTEGER, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO t VALUES (1,'a'),(2,'b')")
            .execute(&pool)
            .await
            .unwrap();

        let app = build_test_app(pool, "tok").await;

        // ── schema: OK with token ──────────────────────────────────────────
        let schema_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/sql/db/schema")
                    .header("authorization", "Bearer tok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schema_response.status(), 200);

        // ── schema: 401 without token ──────────────────────────────────────
        let unauth_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/sql/db/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauth_response.status(), 401);

        // ── schema: 404 unknown connection ─────────────────────────────────
        let not_found_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/sql/nope/schema")
                    .header("authorization", "Bearer tok")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(not_found_response.status(), 404);

        // ── query: returns rows ────────────────────────────────────────────
        let query_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sql/db/query")
                    .header("authorization", "Bearer tok")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"SELECT id, name FROM t ORDER BY id","max_rows":10}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(query_response.status(), 200);
        let body = collect_json(query_response.into_body()).await;
        assert_eq!(body["row_count"], 2);
    }
}
