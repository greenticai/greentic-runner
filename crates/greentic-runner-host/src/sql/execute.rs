//! Read-only query execution with row limit and JSON row mapping.
//!
//! # Row→JSON strategy (per engine)
//!
//! Postgres and MySQL cells are decoded by the column's reported type (sqlx
//! decoding is strict: asking for an `i64` from an `INT4` column is a type
//! mismatch, which an earlier "try each type in turn" strategy silently turned
//! into `NULL`). A SQL NULL is always `Value::Null`; a type this module cannot
//! render becomes the string `"<unsupported type NAME>"` instead of `NULL`, so
//! the caller can tell "no value" from "value we could not decode".
//!
//! ## SQLite
//! SQLite is dynamically typed, so values are tried in preference order:
//! `Option<i64>` → `Option<f64>` → `Option<String>` → `Option<Vec<u8>>`
//! (rendered as UTF-8 or base64 when not valid UTF-8) → `Value::Null`.
//!
//! ## Postgres
//! bool, int2/4/8, float4/8, text-likes, json/jsonb, bytea, date/time/timestamp
//! (ISO-8601 strings), uuid (hyphenated string) and numeric (exact decimal
//! string, to avoid precision loss).
//!
//! ## MySQL
//! integers (signed and unsigned), float/double, decimal (exact string),
//! date/time/datetime/timestamp (ISO-8601 strings), year, bit, json, text-likes
//! and binary types.
//!
//! # Row cap
//!
//! Rows are pulled from a stream and reading stops after `max_rows + 1` rows,
//! so a huge result set is never fully materialised.
//!
//! # Empty-result column extraction (known v1 limitation)
//!
//! Column names are extracted from each row via `row.columns()`. When the
//! result set is empty (zero rows), we return `columns: vec![]`. The upstream
//! caller (`/sql/<conn>/query`) documents this limitation; a future revision
//! can use `sqlx::describe` (compile-time or runtime) to get column metadata
//! without fetching rows.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures::{Stream, StreamExt, TryStreamExt};
use serde_json::Value;
use sqlx::{Column, Row, TypeInfo};

use crate::sql::config::Engine;
use crate::sql::pool::ConnectionPool;

// ── Public API ───────────────────────────────────────────────────────────────

/// The result of a read-only SQL query.
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryResult {
    /// Column names in SELECT order. Empty when the result set has zero rows
    /// (v1 limitation — see module docs).
    pub columns: Vec<String>,
    /// Each element is one row; each inner element is the JSON-mapped cell.
    pub rows: Vec<Vec<Value>>,
    /// Number of rows returned (after the cap is applied).
    pub row_count: u64,
    /// `true` when the result set was larger than `max_rows` and was trimmed.
    pub truncated: bool,
}

/// Execute `sql` read-only against `pool` (which must match `engine`),
/// returning at most `max_rows` rows mapped to JSON.
///
/// - **SQLite** pools are already opened read-only (Task 2); we fetch directly.
/// - **Postgres / MySQL**: we wrap the query in a read-only transaction and
///   always roll back, ensuring no writes can persist. The rollback also runs
///   when the query fails; a connection whose rollback fails is closed rather
///   than returned to the pool with an open transaction.
///
/// Returns `Err(String)` on any database or SQL error.
pub async fn run(
    engine: Engine,
    pool: &ConnectionPool,
    sql: &str,
    max_rows: u32,
) -> Result<QueryResult, String> {
    let cap = u64::from(max_rows);
    match (engine, pool) {
        (Engine::Sqlite, ConnectionPool::Sqlite(p)) => {
            // `read_only(true)` + `query_only` stop writes to the database file,
            // but not `VACUUM INTO 'new-file'`, `ATTACH`, nor a second
            // statement smuggled after the first, so refuse them up front.
            check_sqlite_read_only_sql(sql)?;
            let (rows, truncated) = collect_capped(sqlx::query(sql).fetch(p), cap)
                .await
                .map_err(map_err)?;
            Ok(map_rows(&rows, truncated, sqlite_cell_to_json))
        }
        (Engine::Postgres, ConnectionPool::Postgres(p)) => {
            // `Transaction` rolls back on drop, so an early `?` cannot leak it.
            let mut tx = p.begin().await.map_err(map_err)?;
            sqlx::query("SET TRANSACTION READ ONLY")
                .execute(&mut *tx)
                .await
                .map_err(map_err)?;
            let fetched = collect_capped(sqlx::query(sql).fetch(&mut *tx), cap).await;
            tx.rollback().await.ok();
            let (rows, truncated) = fetched.map_err(map_err)?;
            Ok(map_rows(&rows, truncated, pg_cell_to_json))
        }
        (Engine::Mysql, ConnectionPool::Mysql(p)) => {
            let mut conn = p.acquire().await.map_err(map_err)?;
            let fetched = mysql_read_only_fetch(&mut conn, sql, cap).await;
            // Always roll back, success or not. If the rollback itself fails
            // the connection may still be inside the transaction, so make sure
            // it is closed instead of going back to the pool.
            if sqlx::query("ROLLBACK").execute(&mut *conn).await.is_err() {
                conn.close_on_drop();
            }
            let (rows, truncated) = fetched.map_err(map_err)?;
            Ok(map_rows(&rows, truncated, mysql_cell_to_json))
        }
        _ => Err("engine/pool mismatch".to_string()),
    }
}

async fn mysql_read_only_fetch(
    conn: &mut sqlx::MySqlConnection,
    sql: &str,
    cap: u64,
) -> Result<(Vec<sqlx::mysql::MySqlRow>, bool), sqlx::Error> {
    sqlx::query("START TRANSACTION READ ONLY")
        .execute(&mut *conn)
        .await?;
    collect_capped(sqlx::query(sql).fetch(&mut *conn), cap).await
}

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Accept only ONE statement that starts with `SELECT`, `WITH` or `VALUES`.
///
/// SQLite has no read-only transaction mode like Postgres/MySQL, and sqlx
/// exposes no authorizer. `SqliteConnectOptions::read_only` opens the file
/// read-only, yet `VACUUM INTO '<path>'` still writes a NEW file, `ATTACH`
/// can open (and create) another database, and sqlx runs every statement of a
/// multi-statement string. Comments, quoted strings/identifiers and a trailing
/// `;` are skipped correctly, so `SELECT ';'` and `SELECT 1;` stay valid.
pub(crate) fn check_sqlite_read_only_sql(sql: &str) -> Result<(), String> {
    const ALLOWED_START: [&str; 3] = ["select", "with", "values"];
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut first_word: Option<String> = None;
    let mut statement_ended = false;
    while i < bytes.len() {
        match bytes[i] {
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < bytes.len() && !(bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            quote @ (b'\'' | b'"' | b'`' | b'[') => {
                if statement_ended {
                    return Err("only a single SQL statement is allowed".to_string());
                }
                if first_word.is_none() {
                    return Err("only SELECT queries are allowed".to_string());
                }
                let close = if quote == b'[' { b']' } else { quote };
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == close {
                        // A doubled quote is an escaped quote inside the literal.
                        if close != b']' && bytes.get(i + 1) == Some(&close) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b';' => {
                statement_ended = true;
                i += 1;
            }
            c if c.is_ascii_whitespace() => i += 1,
            _ => {
                if statement_ended {
                    return Err("only a single SQL statement is allowed".to_string());
                }
                if first_word.is_none() {
                    let start = i;
                    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                    {
                        i += 1;
                    }
                    if i == start {
                        return Err("only SELECT queries are allowed".to_string());
                    }
                    let word = sql[start..i].to_ascii_lowercase();
                    if !ALLOWED_START.contains(&word.as_str()) {
                        return Err("only SELECT queries are allowed".to_string());
                    }
                    first_word = Some(word);
                } else {
                    i += 1;
                }
            }
        }
    }
    if first_word.is_none() {
        return Err("empty SQL statement".to_string());
    }
    Ok(())
}

fn map_err(error: sqlx::Error) -> String {
    format!("query error: {error}")
}

/// Pull at most `cap + 1` items from `stream`; return `(kept, truncated)` where
/// `kept` has at most `cap` items and `truncated` says another item existed.
/// Reading stops as soon as the extra item arrives, so the rest of a large
/// result set is never fetched into memory.
async fn collect_capped<S, T, E>(stream: S, cap: u64) -> Result<(Vec<T>, bool), E>
where
    S: Stream<Item = Result<T, E>>,
{
    let limit = usize::try_from(cap).unwrap_or(usize::MAX);
    let mut rows: Vec<T> = stream.take(limit.saturating_add(1)).try_collect().await?;
    let truncated = rows.len() > limit;
    if truncated {
        rows.truncate(limit);
    }
    Ok((rows, truncated))
}

/// Shared row→[`QueryResult`] mapping for every engine.
fn map_rows<R: Row>(rows: &[R], truncated: bool, cell: impl Fn(&R, usize) -> Value) -> QueryResult {
    let row_count = u64::try_from(rows.len()).unwrap_or(u64::MAX);
    let columns: Vec<String> = rows
        .first()
        .map(|row| {
            row.columns()
                .iter()
                .map(|col| col.name().to_string())
                .collect()
        })
        .unwrap_or_default();
    let mapped_rows = rows
        .iter()
        .map(|row| (0..row.columns().len()).map(|i| cell(row, i)).collect())
        .collect();
    QueryResult {
        columns,
        rows: mapped_rows,
        row_count,
        truncated,
    }
}

/// Render a float as a JSON number; NaN / Inf are not representable → Null.
fn float_json(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// Widen an `f32` without exposing binary-representation noise
/// (`0.1f32` → `0.1`, not `0.10000000149011612`).
fn f32_json(value: f32) -> Value {
    match value.to_string().parse::<f64>() {
        Ok(wide) => float_json(wide),
        Err(_) => float_json(f64::from(value)),
    }
}

/// UTF-8 text when valid, otherwise base64.
fn bytes_json(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(utf8) => Value::String(utf8.to_string()),
        Err(_) => Value::String(BASE64.encode(bytes)),
    }
}

fn unsupported(type_name: &str) -> Value {
    Value::String(format!("<unsupported type {type_name}>"))
}

/// Decode one cell as `T`, mapping NULL to `Value::Null`. A decode error is
/// logged and surfaces as an explicit marker string (never a silent NULL).
fn decode_cell<'r, R, T>(row: &'r R, index: usize, convert: impl FnOnce(T) -> Value) -> Value
where
    R: Row,
    usize: sqlx::ColumnIndex<R>,
    T: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    match row.try_get::<Option<T>, _>(index) {
        Ok(Some(value)) => convert(value),
        Ok(None) => Value::Null,
        Err(error) => {
            tracing::warn!(column = index, %error, "sql gateway could not decode cell");
            Value::String("<undecodable value>".to_string())
        }
    }
}

/// Like [`decode_cell`] but skips sqlx's column/type compatibility check, for
/// types whose wire encoding we know is decodable as `T` (e.g. MySQL DECIMAL
/// is sent as text).
fn decode_cell_unchecked<'r, R, T>(
    row: &'r R,
    index: usize,
    convert: impl FnOnce(T) -> Value,
) -> Value
where
    R: Row,
    usize: sqlx::ColumnIndex<R>,
    T: sqlx::Decode<'r, R::Database>,
{
    match row.try_get_unchecked::<Option<T>, _>(index) {
        Ok(Some(value)) => convert(value),
        Ok(None) => Value::Null,
        Err(error) => {
            tracing::warn!(column = index, %error, "sql gateway could not decode cell");
            Value::String("<undecodable value>".to_string())
        }
    }
}

// ── SQLite ───────────────────────────────────────────────────────────────────

/// Map a single SQLite cell to a `serde_json::Value`.
///
/// Try-order: `i64` → `f64` → `String` → `Vec<u8>` (UTF-8 or base64) → Null.
fn sqlite_cell_to_json(row: &sqlx::sqlite::SqliteRow, index: usize) -> Value {
    // i64
    if let Ok(Some(integer_value)) = row.try_get::<Option<i64>, _>(index) {
        return Value::Number(integer_value.into());
    }
    // f64
    if let Ok(Some(float_value)) = row.try_get::<Option<f64>, _>(index) {
        return float_json(float_value);
    }
    // String (TEXT affinity)
    if let Ok(Some(text_value)) = row.try_get::<Option<String>, _>(index) {
        return Value::String(text_value);
    }
    // BLOB — render as UTF-8 when valid, otherwise base64
    if let Ok(Some(blob_value)) = row.try_get::<Option<Vec<u8>>, _>(index) {
        return bytes_json(&blob_value);
    }
    // NULL (all Ok(None) paths lead here, or explicit NULL)
    Value::Null
}

// ── Postgres ─────────────────────────────────────────────────────────────────

/// Map a single Postgres cell to a `serde_json::Value`, by column type.
fn pg_cell_to_json(row: &sqlx::postgres::PgRow, index: usize) -> Value {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

    let type_name = row.column(index).type_info().name().to_ascii_uppercase();
    match type_name.as_str() {
        "BOOL" => decode_cell::<_, bool>(row, index, Value::Bool),
        "INT2" => decode_cell::<_, i16>(row, index, |v| Value::from(i64::from(v))),
        "INT4" => decode_cell::<_, i32>(row, index, |v| Value::from(i64::from(v))),
        "INT8" => decode_cell::<_, i64>(row, index, Value::from),
        "FLOAT4" => decode_cell::<_, f32>(row, index, f32_json),
        "FLOAT8" => decode_cell::<_, f64>(row, index, float_json),
        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "\"CHAR\"" | "CITEXT" => {
            decode_cell::<_, String>(row, index, Value::String)
        }
        "JSON" | "JSONB" => decode_cell::<_, Value>(row, index, |v| v),
        "BYTEA" => decode_cell::<_, Vec<u8>>(row, index, |v| bytes_json(&v)),
        "DATE" => decode_cell::<_, NaiveDate>(row, index, |v| Value::String(v.to_string())),
        "TIME" => decode_cell::<_, NaiveTime>(row, index, |v| Value::String(v.to_string())),
        "TIMESTAMP" => {
            decode_cell::<_, NaiveDateTime>(row, index, |v| Value::String(v.to_string()))
        }
        "TIMESTAMPTZ" => {
            decode_cell::<_, DateTime<Utc>>(row, index, |v| Value::String(v.to_rfc3339()))
        }
        // The binary wire formats of these two are decoded by hand (no
        // uuid / bigdecimal sqlx features enabled in this crate).
        "UUID" => decode_cell_unchecked::<_, Vec<u8>>(row, index, |v| {
            pg_uuid_to_string(&v)
                .map(Value::String)
                .unwrap_or_else(|| unsupported("UUID"))
        }),
        "NUMERIC" => decode_cell_unchecked::<_, Vec<u8>>(row, index, |v| {
            pg_numeric_to_string(&v)
                .map(Value::String)
                .unwrap_or_else(|| unsupported("NUMERIC"))
        }),
        // Enums, XML and similar types travel as plain UTF-8 in the binary
        // protocol. Anything else (arrays, INTERVAL, INET, MONEY, ...) is
        // binary and fails the printable-text check, so it still surfaces as
        // the unsupported marker rather than as garbage.
        other => decode_cell_unchecked::<_, Vec<u8>>(row, index, |v| {
            printable_text(&v)
                .map(Value::String)
                .unwrap_or_else(|| unsupported(other))
        }),
    }
}

/// `bytes` as a string when they are valid UTF-8 without control characters
/// (tab, newline and carriage return excepted); `None` for binary payloads.
fn printable_text(bytes: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(bytes).ok()?;
    if text.is_empty()
        || text
            .chars()
            .any(|c| c.is_control() && !matches!(c, '\t' | '\n' | '\r'))
    {
        return None;
    }
    Some(text.to_string())
}

/// Hyphenated lowercase UUID from its 16-byte binary form.
fn pg_uuid_to_string(bytes: &[u8]) -> Option<String> {
    let bytes: [u8; 16] = bytes.try_into().ok()?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Some(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

/// Exact decimal string from Postgres' binary NUMERIC encoding:
/// `ndigits:i16, weight:i16, sign:u16, dscale:u16, digits:[i16; ndigits]`
/// where every digit is a base-10000 group.
fn pg_numeric_to_string(bytes: &[u8]) -> Option<String> {
    let header = bytes.get(..8)?;
    let ndigits = usize::from(u16::from_be_bytes([header[0], header[1]]));
    let weight = i32::from(i16::from_be_bytes([header[2], header[3]]));
    let sign = u16::from_be_bytes([header[4], header[5]]);
    let dscale = usize::from(u16::from_be_bytes([header[6], header[7]]));
    match sign {
        0xC000 => return Some("NaN".to_string()),
        0xD000 => return Some("Infinity".to_string()),
        0xF000 => return Some("-Infinity".to_string()),
        0x0000 | 0x4000 => {}
        _ => return None,
    }
    let body = bytes.get(8..8usize.checked_add(ndigits.checked_mul(2)?)?)?;
    let digits: Vec<i32> = body
        .chunks_exact(2)
        .map(|pair| i32::from(i16::from_be_bytes([pair[0], pair[1]])))
        .collect();
    // Digit `k` carries weight `weight - k` (base-10000 position).
    let digit_at = |k: i32| -> i32 {
        usize::try_from(k)
            .ok()
            .and_then(|k| digits.get(k).copied())
            .unwrap_or(0)
    };

    let mut out = String::new();
    if sign == 0x4000 && digits.iter().any(|d| *d != 0) {
        out.push('-');
    }
    if weight < 0 {
        out.push('0');
    } else {
        for k in 0..=weight {
            if k == 0 {
                out.push_str(&digit_at(k).to_string());
            } else {
                out.push_str(&format!("{:04}", digit_at(k)));
            }
        }
    }
    if dscale > 0 {
        let mut frac = String::new();
        let groups = i32::try_from(dscale.div_ceil(4)).ok()?;
        for m in 1..=groups {
            frac.push_str(&format!("{:04}", digit_at(weight.checked_add(m)?)));
        }
        frac.truncate(dscale);
        out.push('.');
        out.push_str(&frac);
    }
    Some(out)
}

// ── MySQL ────────────────────────────────────────────────────────────────────

/// Map a single MySQL cell to a `serde_json::Value`, by column type.
fn mysql_cell_to_json(row: &sqlx::mysql::MySqlRow, index: usize) -> Value {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

    let type_name = row.column(index).type_info().name().to_ascii_uppercase();
    match type_name.as_str() {
        // TINYINT(1) is MySQL's boolean.
        "BOOLEAN" => decode_cell_unchecked::<_, i64>(row, index, |v| Value::Bool(v != 0)),
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" | "YEAR" => {
            decode_cell_unchecked::<_, i64>(row, index, Value::from)
        }
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
        | "BIGINT UNSIGNED" => decode_cell_unchecked::<_, u64>(row, index, Value::from),
        "FLOAT" => decode_cell::<_, f32>(row, index, f32_json),
        "DOUBLE" => decode_cell::<_, f64>(row, index, float_json),
        // DECIMAL is sent as its exact text form.
        "DECIMAL" => decode_cell_unchecked::<_, String>(row, index, Value::String),
        "DATE" => decode_cell::<_, NaiveDate>(row, index, |v| Value::String(v.to_string())),
        "TIME" => decode_cell::<_, NaiveTime>(row, index, |v| Value::String(v.to_string())),
        "DATETIME" => decode_cell::<_, NaiveDateTime>(row, index, |v| Value::String(v.to_string())),
        "TIMESTAMP" => {
            decode_cell::<_, DateTime<Utc>>(row, index, |v| Value::String(v.to_rfc3339()))
        }
        "JSON" => decode_cell::<_, Value>(row, index, |v| v),
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET" => {
            decode_cell_unchecked::<_, String>(row, index, Value::String)
        }
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            decode_cell::<_, Vec<u8>>(row, index, |v| bytes_json(&v))
        }
        // BIT(n): big-endian bytes → unsigned integer (up to 64 bits).
        "BIT" => decode_cell_unchecked::<_, Vec<u8>>(row, index, |v| {
            if v.len() > 8 {
                return unsupported("BIT");
            }
            Value::from(v.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b)))
        }),
        other => unsupported(other),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::pool::ConnectionPool;

    // ── collect_capped ───────────────────────────────────────────────────────

    fn ok_stream(n: u32) -> impl Stream<Item = Result<u32, String>> {
        futures::stream::iter((0..n).map(Ok))
    }

    #[tokio::test]
    async fn collect_capped_truncates_when_over_limit() {
        let (kept, truncated) = collect_capped(ok_stream(5), 3).await.unwrap();
        assert_eq!(kept, vec![0, 1, 2]);
        assert!(truncated);
    }

    #[tokio::test]
    async fn collect_capped_keeps_all_when_under_or_at_limit() {
        let (kept, truncated) = collect_capped(ok_stream(2), 10).await.unwrap();
        assert_eq!(kept, vec![0, 1]);
        assert!(!truncated);
        let (kept, truncated) = collect_capped(ok_stream(3), 3).await.unwrap();
        assert_eq!(kept.len(), 3);
        assert!(!truncated);
    }

    #[tokio::test]
    async fn collect_capped_empty_not_truncated() {
        let (kept, truncated) = collect_capped(ok_stream(0), 5).await.unwrap();
        assert!(kept.is_empty());
        assert!(!truncated);
    }

    #[tokio::test]
    async fn collect_capped_stops_pulling_after_cap_plus_one() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let pulled = AtomicU32::new(0);
        let stream = futures::stream::iter(0..1_000u32).map(|n| {
            pulled.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(n)
        });
        let (kept, truncated) = collect_capped(stream, 4).await.unwrap();
        assert_eq!(kept.len(), 4);
        assert!(truncated);
        assert_eq!(
            pulled.load(Ordering::SeqCst),
            5,
            "must not drain the stream"
        );
    }

    #[tokio::test]
    async fn collect_capped_propagates_errors() {
        let stream = futures::stream::iter(vec![Ok(1u32), Err("boom".to_string())]);
        assert_eq!(collect_capped(stream, 10).await.unwrap_err(), "boom");
    }

    #[test]
    fn printable_text_accepts_text_and_rejects_binary_payloads() {
        assert_eq!(printable_text(b"happy").as_deref(), Some("happy"));
        assert_eq!(
            printable_text("naïve\tlabel".as_bytes()).as_deref(),
            Some("naïve\tlabel")
        );
        // INET (family 2, /32, not cidr, 4 bytes), array header, INTERVAL, empty.
        assert!(printable_text(&[2, 32, 0, 4, 10, 0, 0, 1]).is_none());
        assert!(printable_text(&[0, 0, 0, 1, 0, 0, 0, 0]).is_none());
        assert!(printable_text(&[0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0]).is_none());
        assert!(printable_text(&[0xff, 0xfe]).is_none());
        assert!(printable_text(b"").is_none());
    }

    // ── pg wire decoders ─────────────────────────────────────────────────────

    fn numeric(ndigits: u16, weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(ndigits.to_be_bytes());
        out.extend(weight.to_be_bytes());
        out.extend(sign.to_be_bytes());
        out.extend(dscale.to_be_bytes());
        for d in digits {
            out.extend(d.to_be_bytes());
        }
        out
    }

    #[test]
    fn pg_numeric_decodes_integers_and_fractions() {
        // 12345.6789 => groups [1, 2345, 6789], weight 1, dscale 4
        let v = numeric(3, 1, 0, 4, &[1, 2345, 6789]);
        assert_eq!(pg_numeric_to_string(&v).as_deref(), Some("12345.6789"));
        // -0.05 => groups [500], weight -1, dscale 2
        let v = numeric(1, -1, 0x4000, 2, &[500]);
        assert_eq!(pg_numeric_to_string(&v).as_deref(), Some("-0.05"));
        // 0.0001 => weight -1? digit 1 at weight -1 => [1] weight -1 gives 0.0001
        let v = numeric(1, -1, 0, 4, &[1]);
        assert_eq!(pg_numeric_to_string(&v).as_deref(), Some("0.0001"));
        // 10000 => groups [1], weight 1 -> "1" + "0000"
        let v = numeric(1, 1, 0, 0, &[1]);
        assert_eq!(pg_numeric_to_string(&v).as_deref(), Some("10000"));
        // zero
        let v = numeric(0, 0, 0, 0, &[]);
        assert_eq!(pg_numeric_to_string(&v).as_deref(), Some("0"));
        // NaN
        let v = numeric(0, 0, 0xC000, 0, &[]);
        assert_eq!(pg_numeric_to_string(&v).as_deref(), Some("NaN"));
    }

    #[test]
    fn pg_numeric_rejects_truncated_input() {
        assert_eq!(pg_numeric_to_string(&[0, 1]), None);
        let v = numeric(2, 0, 0, 0, &[1]);
        assert_eq!(pg_numeric_to_string(&v), None);
    }

    #[test]
    fn pg_uuid_formats_hyphenated() {
        let bytes = [
            0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17,
            0x40, 0x00,
        ];
        assert_eq!(
            pg_uuid_to_string(&bytes).as_deref(),
            Some("123e4567-e89b-12d3-a456-426614174000")
        );
        assert_eq!(pg_uuid_to_string(&bytes[..4]), None);
    }

    #[test]
    fn f32_json_avoids_widening_noise() {
        assert_eq!(f32_json(0.1), serde_json::json!(0.1));
        assert_eq!(float_json(f64::NAN), Value::Null);
    }

    // ── SQLite end-to-end ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn sqlite_query_caps_and_maps_integers_and_text() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (id INTEGER, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
            .execute(&pool)
            .await
            .unwrap();

        let cp = ConnectionPool::Sqlite(pool);
        let result = run(Engine::Sqlite, &cp, "SELECT id, name FROM t ORDER BY id", 2)
            .await
            .unwrap();

        assert_eq!(result.columns, vec!["id", "name"]);
        assert_eq!(result.rows.len(), 2);
        assert!(result.truncated);
        assert_eq!(result.row_count, 2);
        // Row 0: id=1, name="a"
        assert_eq!(result.rows[0][0], serde_json::json!(1));
        assert_eq!(result.rows[0][1], serde_json::json!("a"));
        // Row 1: id=2, name="b"
        assert_eq!(result.rows[1][0], serde_json::json!(2));
        assert_eq!(result.rows[1][1], serde_json::json!("b"));
    }

    #[tokio::test]
    async fn sqlite_query_maps_null_correctly() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (id INTEGER, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        // Third row has NULL name
        sqlx::query("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,NULL)")
            .execute(&pool)
            .await
            .unwrap();

        let cp = ConnectionPool::Sqlite(pool);
        let result = run(
            Engine::Sqlite,
            &cp,
            "SELECT id, name FROM t ORDER BY id",
            10,
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 3);
        assert!(!result.truncated);
        // Row 2: id=3, name=NULL
        assert_eq!(result.rows[2][0], serde_json::json!(3));
        assert_eq!(result.rows[2][1], Value::Null);
    }

    #[tokio::test]
    async fn sqlite_query_no_truncation_when_under_cap() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (id INTEGER)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO t VALUES (1),(2)")
            .execute(&pool)
            .await
            .unwrap();

        let cp = ConnectionPool::Sqlite(pool);
        let result = run(Engine::Sqlite, &cp, "SELECT id FROM t ORDER BY id", 100)
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 2);
        assert!(!result.truncated);
        assert_eq!(result.row_count, 2);
    }

    #[tokio::test]
    async fn sqlite_query_empty_result_has_empty_columns() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (id INTEGER, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        // No rows inserted

        let cp = ConnectionPool::Sqlite(pool);
        let result = run(Engine::Sqlite, &cp, "SELECT id, name FROM t", 10)
            .await
            .unwrap();

        // v1 known limitation: columns is empty when result set is empty
        assert_eq!(result.columns, Vec::<String>::new());
        assert_eq!(result.rows.len(), 0);
        assert!(!result.truncated);
        assert_eq!(result.row_count, 0);
    }

    #[test]
    fn sqlite_guard_accepts_single_select_shapes() {
        for sql in [
            "SELECT 1",
            "  select 1;",
            "SELECT ';' AS semi",
            "SELECT 'it''s; fine'",
            "-- lead\nSELECT 1 /* ; */ ;  -- trail",
            "WITH t AS (SELECT 1 AS a) SELECT a FROM t",
            "VALUES (1),(2)",
            r#"SELECT "a;b" FROM t"#,
        ] {
            assert!(check_sqlite_read_only_sql(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn sqlite_guard_refuses_writes_attach_vacuum_and_stacked_statements() {
        for sql in [
            "VACUUM INTO '/tmp/copy.db'",
            "ATTACH DATABASE '/tmp/x.db' AS x",
            "PRAGMA writable_schema = 1",
            "INSERT INTO t VALUES (1)",
            "DELETE FROM t",
            "/* hide */ DROP TABLE t",
            "SELECT 1; VACUUM INTO '/tmp/copy.db'",
            "SELECT 1; SELECT 2",
            "SELECT 1;;ATTACH 'x' AS y",
            "SELECT 1; -- c\n DROP TABLE t",
            "'SELECT'",
            "",
            "   -- only a comment",
        ] {
            assert!(check_sqlite_read_only_sql(sql).is_err(), "{sql}");
        }
    }

    #[tokio::test]
    async fn sqlite_run_refuses_vacuum_into_on_a_read_only_pool() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("src.db");
        {
            let rw = sqlx::sqlite::SqlitePool::connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(&db)
                    .create_if_missing(true),
            )
            .await
            .expect("rw pool");
            sqlx::query("CREATE TABLE t (id INTEGER)")
                .execute(&rw)
                .await
                .expect("create");
            rw.close().await;
        }
        let dsn = format!("sqlite://{}", db.display());
        let pool = crate::sql::pool::build(Engine::Sqlite, &dsn, true)
            .await
            .expect("read-only pool");
        let target = dir.path().join("copy.db");
        let sql = format!("VACUUM INTO '{}'", target.display());
        let err = run(Engine::Sqlite, &pool, &sql, 10)
            .await
            .expect_err("must be refused");
        assert!(err.contains("only SELECT"), "{err}");
        assert!(!target.exists(), "VACUUM INTO must not create a file");
        let ok = run(Engine::Sqlite, &pool, "SELECT count(*) AS n FROM t", 10)
            .await
            .expect("select still works");
        assert_eq!(ok.row_count, 1);
    }

    #[tokio::test]
    async fn sqlite_engine_pool_mismatch_returns_err() {
        // Using a Sqlite pool but passing Engine::Postgres should return an error.
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        let cp = ConnectionPool::Sqlite(pool);
        let result = run(Engine::Postgres, &cp, "SELECT 1", 10).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mismatch"));
    }

    #[tokio::test]
    async fn sqlite_query_maps_float_and_blob() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (val REAL, data BLOB)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO t VALUES (3.14, X'48454C4C4F')")
            .execute(&pool)
            .await
            .unwrap();

        let cp = ConnectionPool::Sqlite(pool);
        let result = run(Engine::Sqlite, &cp, "SELECT val, data FROM t", 10)
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0][0].is_number(), "REAL should map to a number");
        // BLOB X'48454C4C4F' is valid UTF-8 ("HELLO"), rendered as string.
        assert_eq!(result.rows[0][1], serde_json::json!("HELLO"));
    }

    #[tokio::test]
    async fn sqlite_query_maps_invalid_utf8_blob_as_base64() {
        let pool = sqlx::sqlite::SqlitePool::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (data BLOB)")
            .execute(&pool)
            .await
            .unwrap();
        // 0xFF 0xFE is not valid UTF-8.
        sqlx::query("INSERT INTO t VALUES (X'FFFE')")
            .execute(&pool)
            .await
            .unwrap();

        let cp = ConnectionPool::Sqlite(pool);
        let result = run(Engine::Sqlite, &cp, "SELECT data FROM t", 10)
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        // Invalid UTF-8 must be base64-encoded.
        assert_eq!(result.rows[0][0], serde_json::json!("//4="));
    }
}
