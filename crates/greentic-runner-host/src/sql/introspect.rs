//! Per-engine schema introspection → the gateway `Schema` shape that matches the
//! greentic.sql extension's `/schema` contract.

use serde::Serialize;
use sqlx::Row;

use crate::sql::config::Engine;
use crate::sql::pool::ConnectionPool;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Schema {
    pub engine: String,
    pub tables: Vec<Table>,
}

/// Introspect tables+columns. Postgres/MySQL use information_schema (public/
/// current schema); SQLite walks sqlite_master + PRAGMA table_info.
pub async fn introspect(engine: Engine, pool: &ConnectionPool) -> Result<Schema, String> {
    let tables = match (engine, pool) {
        (Engine::Postgres, ConnectionPool::Postgres(p)) => {
            let rows = sqlx::query(
                "SELECT table_name, column_name, data_type \
                 FROM information_schema.columns \
                 WHERE table_schema = 'public' \
                 ORDER BY table_name, ordinal_position",
            )
            .fetch_all(p)
            .await
            .map_err(|e| format!("postgres introspect: {e}"))?;
            group_columns(rows.iter().map(|r| {
                (
                    r.get::<String, _>("table_name"),
                    r.get::<String, _>("column_name"),
                    r.get::<String, _>("data_type"),
                )
            }))
        }
        (Engine::Mysql, ConnectionPool::Mysql(p)) => {
            let rows = sqlx::query(
                "SELECT table_name, column_name, data_type \
                 FROM information_schema.columns \
                 WHERE table_schema = DATABASE() \
                 ORDER BY table_name, ordinal_position",
            )
            .fetch_all(p)
            .await
            .map_err(|e| format!("mysql introspect: {e}"))?;
            group_columns(rows.iter().map(|r| {
                (
                    r.get::<String, _>("table_name"),
                    r.get::<String, _>("column_name"),
                    r.get::<String, _>("data_type"),
                )
            }))
        }
        (Engine::Sqlite, ConnectionPool::Sqlite(p)) => {
            let names = sqlx::query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .fetch_all(p)
            .await
            .map_err(|e| format!("sqlite tables: {e}"))?;
            let mut tables = Vec::new();
            for row in names {
                let table: String = row.get("name");
                let escaped = table.replace('"', "\"\"");
                let cols = sqlx::query(&format!("PRAGMA table_info(\"{escaped}\")"))
                    .fetch_all(p)
                    .await
                    .map_err(|e| format!("sqlite columns: {e}"))?;
                let columns = cols
                    .iter()
                    .map(|c| Column {
                        name: c.get("name"),
                        type_: c.get("type"),
                    })
                    .collect();
                tables.push(Table {
                    name: table,
                    columns,
                });
            }
            tables
        }
        _ => return Err("engine/pool mismatch".to_string()),
    };
    Ok(Schema {
        engine: engine_str(engine).to_string(),
        tables,
    })
}

fn engine_str(engine: Engine) -> &'static str {
    match engine {
        Engine::Postgres => "postgres",
        Engine::Mysql => "mysql",
        Engine::Sqlite => "sqlite",
    }
}

fn group_columns(rows: impl Iterator<Item = (String, String, String)>) -> Vec<Table> {
    let mut tables: Vec<Table> = Vec::new();
    for (table, column, type_) in rows {
        let column = Column {
            name: column,
            type_,
        };
        match tables.last_mut() {
            Some(current) if current.name == table => current.columns.push(column),
            _ => tables.push(Table {
                name: table,
                columns: vec![column],
            }),
        }
    }
    tables
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_columns_by_table() {
        let rows = vec![
            ("users".into(), "id".into(), "integer".into()),
            ("users".into(), "email".into(), "text".into()),
            ("orders".into(), "id".into(), "integer".into()),
        ];
        let tables = group_columns(rows.into_iter());
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "users");
        assert_eq!(tables[0].columns.len(), 2);
    }

    #[tokio::test]
    async fn introspects_sqlite() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE users (id INTEGER, email TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE orders (id INTEGER)")
            .execute(&pool)
            .await
            .unwrap();
        let cp = crate::sql::pool::ConnectionPool::Sqlite(pool);
        let schema = introspect(Engine::Sqlite, &cp).await.unwrap();
        assert_eq!(schema.engine, "sqlite");
        let users = schema.tables.iter().find(|t| t.name == "users").unwrap();
        assert_eq!(users.columns.len(), 2);
        assert!(users.columns.iter().any(|c| c.name == "email"));
    }

    #[tokio::test]
    async fn introspects_sqlite_quoted_table_name() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(r#"CREATE TABLE "weird name" (id INTEGER)"#)
            .execute(&pool)
            .await
            .unwrap();
        let cp = crate::sql::pool::ConnectionPool::Sqlite(pool);
        let schema = introspect(Engine::Sqlite, &cp).await.unwrap();
        let t = schema
            .tables
            .iter()
            .find(|t| t.name == "weird name")
            .unwrap();
        assert_eq!(t.columns.len(), 1);
    }
}
