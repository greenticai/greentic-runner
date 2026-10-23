//! Build read-only sqlx pools per engine from a resolved DSN.

use std::time::Duration;

use crate::sql::config::Engine;

pub enum ConnectionPool {
    Postgres(sqlx::PgPool),
    Mysql(sqlx::MySqlPool),
    Sqlite(sqlx::SqlitePool),
}

/// Build a pool for `engine` from the resolved `dsn`. SQLite opens read-only
/// when `read_only`; Postgres/MySQL enforce read-only per-query (see execute.rs)
/// and the operator should also use a read-only DB role.
/// Upper bound on waiting for a free pooled connection.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);
/// Server-side cap on one statement's runtime (Postgres/MySQL).
const STATEMENT_TIMEOUT_MS: u64 = 30_000;

pub async fn build(engine: Engine, dsn: &str, read_only: bool) -> Result<ConnectionPool, String> {
    match engine {
        Engine::Postgres => sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query(&format!("SET statement_timeout = {STATEMENT_TIMEOUT_MS}"))
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(dsn)
            .await
            .map(ConnectionPool::Postgres)
            .map_err(|e| format!("postgres connect: {e}")),
        Engine::Mysql => sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query(&format!(
                        "SET SESSION max_execution_time = {STATEMENT_TIMEOUT_MS}"
                    ))
                    .execute(conn)
                    .await?;
                    Ok(())
                })
            })
            .connect(dsn)
            .await
            .map(ConnectionPool::Mysql)
            .map_err(|e| format!("mysql connect: {e}")),
        Engine::Sqlite => {
            let opts = dsn
                .parse::<sqlx::sqlite::SqliteConnectOptions>()
                .map_err(|e| format!("sqlite dsn: {e}"))?
                .read_only(read_only);
            sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(5)
                .acquire_timeout(ACQUIRE_TIMEOUT)
                .connect_with(opts)
                .await
                .map(ConnectionPool::Sqlite)
                .map_err(|e| format!("sqlite connect: {e}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builds_sqlite_pool_readonly() {
        // Use in-memory SQLite — no filesystem dependencies, no temp-file races.
        // We open read-write first (mode=rwc) to create a schema, then verify
        // the read-only path produces a Sqlite variant.
        // Note: each "sqlite::memory:" URL is an independent anonymous database,
        // so we test the read_only flag by opening the same file-based URL.
        let dir = std::env::temp_dir();
        let path = dir.join(format!("sqlgw_pool_test_{}.db", std::process::id()));
        let rw_url = format!("sqlite://{}?mode=rwc", path.display());

        // Create the DB and write a table, then close the pool.
        {
            let rw = sqlx::sqlite::SqlitePool::connect(&rw_url).await.unwrap();
            sqlx::query("CREATE TABLE IF NOT EXISTS t (id INTEGER)")
                .execute(&rw)
                .await
                .unwrap();
            rw.close().await;
        }

        // Open the same file read-only through our builder.
        let ro_url = format!("sqlite://{}", path.display());
        let pool = build(Engine::Sqlite, &ro_url, true).await.unwrap();
        assert!(matches!(pool, ConnectionPool::Sqlite(_)));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn builds_sqlite_pool_in_memory() {
        // Secondary smoke test: in-memory SQLite opened without read-only flag
        // (read_only=false because ":memory:" file doesn't exist as a real path).
        let pool = build(Engine::Sqlite, "sqlite::memory:", false)
            .await
            .unwrap();
        assert!(matches!(pool, ConnectionPool::Sqlite(_)));
    }
}
