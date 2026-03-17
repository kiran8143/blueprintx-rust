// Author: Udaykiran Atta
// License: MIT

use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Column, Row, TypeInfo};
use thiserror::Error;
use chrono::NaiveDateTime;

use crate::schema::types::{
    DatabaseConnection, DatabasePool, DbDialect, DynamicRow, SqlValue,
};

/// Errors from database connection and query operations.
#[derive(Debug, Error)]
pub enum DbError {
    #[error("unsupported database URL scheme: {0}")]
    UnsupportedScheme(String),

    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("column not found: {0}")]
    ColumnNotFound(String),
}

/// Connect to a database by detecting the engine from the URL prefix.
///
/// Supported prefixes: `mysql://`, `postgres://` / `postgresql://`, `sqlite://` / `sqlite:`.
pub async fn connect(url: &str, pool_size: u32) -> Result<DatabaseConnection, DbError> {
    if url.starts_with("mysql://") {
        let pool = MySqlPoolOptions::new()
            .max_connections(pool_size)
            .connect(url)
            .await?;
        Ok(DatabaseConnection::new(
            DatabasePool::MySQL(pool),
            DbDialect::MySQL,
        ))
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let pool = PgPoolOptions::new()
            .max_connections(pool_size)
            .connect(url)
            .await?;
        Ok(DatabaseConnection::new(
            DatabasePool::PostgreSQL(pool),
            DbDialect::PostgreSQL,
        ))
    } else if url.starts_with("sqlite://") || url.starts_with("sqlite:") {
        let pool = SqlitePoolOptions::new()
            .max_connections(pool_size)
            .connect(url)
            .await?;
        Ok(DatabaseConnection::new(
            DatabasePool::SQLite(pool),
            DbDialect::SQLite,
        ))
    } else {
        let scheme = url.split("://").next().unwrap_or(url).to_string();
        Err(DbError::UnsupportedScheme(scheme))
    }
}

/// Execute a dynamic SQL query with string parameters, returning rows of
/// dynamically-typed values.
///
/// Parameters are bound positionally. For MySQL use `?`, for Postgres use
/// `$1`, `$2`, etc., and for SQLite use `?`.
pub async fn query_dynamic(
    conn: &DatabaseConnection,
    sql: &str,
    params: &[&str],
) -> Result<Vec<DynamicRow>, DbError> {
    match &conn.pool {
        DatabasePool::MySQL(pool) => {
            let mut query = sqlx::query(sql);
            for p in params {
                query = query.bind(*p);
            }
            let rows = query.fetch_all(pool).await?;
            rows.iter().map(extract_mysql_row).collect()
        }
        DatabasePool::PostgreSQL(pool) => {
            let mut query = sqlx::query(sql);
            for p in params {
                query = query.bind(*p);
            }
            let rows = query.fetch_all(pool).await?;
            rows.iter().map(extract_pg_row).collect()
        }
        DatabasePool::SQLite(pool) => {
            let mut query = sqlx::query(sql);
            for p in params {
                query = query.bind(*p);
            }
            let rows = query.fetch_all(pool).await?;
            rows.iter().map(extract_sqlite_row).collect()
        }
    }
}

/// Execute a dynamic SQL query returning at most one row.
pub async fn query_optional_dynamic(
    conn: &DatabaseConnection,
    sql: &str,
    params: &[&str],
) -> Result<Option<DynamicRow>, DbError> {
    match &conn.pool {
        DatabasePool::MySQL(pool) => {
            let mut query = sqlx::query(sql);
            for p in params { query = query.bind(*p); }
            match query.fetch_optional(pool).await? {
                Some(row) => Ok(Some(extract_mysql_row(&row)?)),
                None => Ok(None),
            }
        }
        DatabasePool::PostgreSQL(pool) => {
            let mut query = sqlx::query(sql);
            for p in params { query = query.bind(*p); }
            match query.fetch_optional(pool).await? {
                Some(row) => Ok(Some(extract_pg_row(&row)?)),
                None => Ok(None),
            }
        }
        DatabasePool::SQLite(pool) => {
            let mut query = sqlx::query(sql);
            for p in params { query = query.bind(*p); }
            match query.fetch_optional(pool).await? {
                Some(row) => Ok(Some(extract_sqlite_row(&row)?)),
                None => Ok(None),
            }
        }
    }
}

/// Execute a SQL statement (INSERT/UPDATE/DELETE) and return rows affected.
pub async fn execute_sql(
    conn: &DatabaseConnection,
    sql: &str,
    params: &[&str],
) -> Result<u64, DbError> {
    match &conn.pool {
        DatabasePool::MySQL(pool) => {
            let mut query = sqlx::query(sql);
            for p in params { query = query.bind(*p); }
            Ok(query.execute(pool).await?.rows_affected())
        }
        DatabasePool::PostgreSQL(pool) => {
            let mut query = sqlx::query(sql);
            for p in params { query = query.bind(*p); }
            Ok(query.execute(pool).await?.rows_affected())
        }
        DatabasePool::SQLite(pool) => {
            let mut query = sqlx::query(sql);
            for p in params { query = query.bind(*p); }
            Ok(query.execute(pool).await?.rows_affected())
        }
    }
}

/// Execute a COUNT-style query returning a single i64 value from the first column.
pub async fn query_scalar_i64(
    conn: &DatabaseConnection,
    sql: &str,
    params: &[&str],
) -> Result<i64, DbError> {
    match &conn.pool {
        DatabasePool::MySQL(pool) => {
            let mut query = sqlx::query(sql);
            for p in params { query = query.bind(*p); }
            let row = query.fetch_one(pool).await?;
            Ok(row.try_get::<i64, _>(0).unwrap_or(0))
        }
        DatabasePool::PostgreSQL(pool) => {
            let mut query = sqlx::query(sql);
            for p in params { query = query.bind(*p); }
            let row = query.fetch_one(pool).await?;
            Ok(row.try_get::<i64, _>(0).unwrap_or(0))
        }
        DatabasePool::SQLite(pool) => {
            let mut query = sqlx::query(sql);
            for p in params { query = query.bind(*p); }
            let row = query.fetch_one(pool).await?;
            Ok(row.try_get::<i64, _>(0).unwrap_or(0))
        }
    }
}

// ---------------------------------------------------------------------------
// Per-database row extraction
// ---------------------------------------------------------------------------

/// Extract all columns from a MySQL row into a DynamicRow.
fn extract_mysql_row(row: &sqlx::mysql::MySqlRow) -> Result<DynamicRow, DbError> {
    let mut dyn_row = DynamicRow::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = extract_mysql_value(row, col);
        dyn_row.push(name, value);
    }
    Ok(dyn_row)
}

/// Extract a single MySQL column value, coercing by type info.
fn extract_mysql_value(
    row: &sqlx::mysql::MySqlRow,
    col: &sqlx::mysql::MySqlColumn,
) -> SqlValue {
    use sqlx::ValueRef;
    let ordinal = col.ordinal();
    let type_name = col.type_info().name().to_ascii_uppercase();

    // Check for NULL using the raw value reference (type-agnostic).
    if let Ok(raw) = row.try_get_raw(ordinal) {
        if raw.is_null() {
            return SqlValue::Null;
        }
    }

    match type_name.as_str() {
        "BOOLEAN" | "BOOL" | "TINYINT(1)" => {
            match row.try_get::<Option<bool>, _>(ordinal) {
                Ok(Some(v)) => SqlValue::Bool(v),
                Ok(None) => SqlValue::Null,
                Err(_) => {
                    // Fallback: try as i64
                    match row.try_get::<Option<i64>, _>(ordinal) {
                        Ok(Some(v)) => SqlValue::Bool(v != 0),
                        _ => SqlValue::Null,
                    }
                }
            }
        }
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
            match row.try_get::<Option<i64>, _>(ordinal) {
                Ok(Some(v)) => SqlValue::Integer(v),
                _ => SqlValue::Null,
            }
        }
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
        | "BIGINT UNSIGNED" => match row.try_get::<Option<i64>, _>(ordinal) {
            Ok(Some(v)) => SqlValue::Integer(v),
            _ => SqlValue::Null,
        },
        "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" => {
            match row.try_get::<Option<f64>, _>(ordinal) {
                Ok(Some(v)) => SqlValue::Float(v),
                Ok(None) => SqlValue::Null,
                Err(_) => {
                    // DECIMAL may need string extraction
                    match row.try_get::<Option<String>, _>(ordinal) {
                        Ok(Some(s)) => SqlValue::String(s),
                        _ => SqlValue::Null,
                    }
                }
            }
        }
        "DATETIME" | "TIMESTAMP" => {
            match row.try_get::<Option<NaiveDateTime>, _>(ordinal) {
                Ok(Some(dt)) => SqlValue::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()),
                Ok(None) => SqlValue::Null,
                Err(_) => {
                    // Fallback: try as string
                    match row.try_get::<Option<String>, _>(ordinal) {
                        Ok(Some(s)) => SqlValue::String(s),
                        _ => SqlValue::Null,
                    }
                }
            }
        }
        "DATE" => {
            match row.try_get::<Option<chrono::NaiveDate>, _>(ordinal) {
                Ok(Some(d)) => SqlValue::String(d.format("%Y-%m-%d").to_string()),
                Ok(None) => SqlValue::Null,
                Err(_) => match row.try_get::<Option<String>, _>(ordinal) {
                    Ok(Some(s)) => SqlValue::String(s),
                    _ => SqlValue::Null,
                },
            }
        }
        "TIME" => {
            match row.try_get::<Option<chrono::NaiveTime>, _>(ordinal) {
                Ok(Some(t)) => SqlValue::String(t.format("%H:%M:%S").to_string()),
                Ok(None) => SqlValue::Null,
                Err(_) => match row.try_get::<Option<String>, _>(ordinal) {
                    Ok(Some(s)) => SqlValue::String(s),
                    _ => SqlValue::Null,
                },
            }
        }
        "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "TINYBLOB" | "BINARY" | "VARBINARY" => {
            match row.try_get::<Option<Vec<u8>>, _>(ordinal) {
                Ok(Some(v)) => SqlValue::Bytes(v),
                _ => SqlValue::Null,
            }
        }
        _ => {
            // Default: try as string first
            match row.try_get::<Option<String>, _>(ordinal) {
                Ok(Some(v)) => SqlValue::String(v),
                Ok(None) => SqlValue::Null,
                Err(_) => {
                    // Try NaiveDateTime (unrecognised datetime variants)
                    if let Ok(Some(dt)) = row.try_get::<Option<NaiveDateTime>, _>(ordinal) {
                        return SqlValue::String(dt.format("%Y-%m-%d %H:%M:%S").to_string());
                    }
                    // Final fallback: raw bytes
                    match row.try_get::<Option<Vec<u8>>, _>(ordinal) {
                        Ok(Some(v)) => SqlValue::Bytes(v),
                        _ => SqlValue::Null,
                    }
                }
            }
        }
    }
}

/// Extract all columns from a Postgres row into a DynamicRow.
fn extract_pg_row(row: &sqlx::postgres::PgRow) -> Result<DynamicRow, DbError> {
    let mut dyn_row = DynamicRow::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = extract_pg_value(row, col);
        dyn_row.push(name, value);
    }
    Ok(dyn_row)
}

/// Extract a single Postgres column value, coercing by type info.
fn extract_pg_value(
    row: &sqlx::postgres::PgRow,
    col: &sqlx::postgres::PgColumn,
) -> SqlValue {
    let ordinal = col.ordinal();
    let type_name = col.type_info().name().to_ascii_uppercase();

    match type_name.as_str() {
        "BOOL" => match row.try_get::<Option<bool>, _>(ordinal) {
            Ok(Some(v)) => SqlValue::Bool(v),
            _ => SqlValue::Null,
        },
        "INT2" | "INT4" => match row.try_get::<Option<i32>, _>(ordinal) {
            Ok(Some(v)) => SqlValue::Integer(v as i64),
            _ => SqlValue::Null,
        },
        "INT8" => match row.try_get::<Option<i64>, _>(ordinal) {
            Ok(Some(v)) => SqlValue::Integer(v),
            _ => SqlValue::Null,
        },
        "FLOAT4" => match row.try_get::<Option<f32>, _>(ordinal) {
            Ok(Some(v)) => SqlValue::Float(v as f64),
            _ => SqlValue::Null,
        },
        "FLOAT8" | "NUMERIC" => match row.try_get::<Option<f64>, _>(ordinal) {
            Ok(Some(v)) => SqlValue::Float(v),
            Ok(None) => SqlValue::Null,
            Err(_) => {
                // NUMERIC may need string extraction
                match row.try_get::<Option<String>, _>(ordinal) {
                    Ok(Some(s)) => SqlValue::String(s),
                    _ => SqlValue::Null,
                }
            }
        },
        "BYTEA" => match row.try_get::<Option<Vec<u8>>, _>(ordinal) {
            Ok(Some(v)) => SqlValue::Bytes(v),
            _ => SqlValue::Null,
        },
        _ => {
            // Default: try as string (covers VARCHAR, TEXT, UUID, JSON, timestamps)
            match row.try_get::<Option<String>, _>(ordinal) {
                Ok(Some(v)) => SqlValue::String(v),
                Ok(None) => SqlValue::Null,
                Err(_) => match row.try_get::<Option<Vec<u8>>, _>(ordinal) {
                    Ok(Some(v)) => SqlValue::Bytes(v),
                    _ => SqlValue::Null,
                },
            }
        }
    }
}

/// Extract all columns from a SQLite row into a DynamicRow.
fn extract_sqlite_row(row: &sqlx::sqlite::SqliteRow) -> Result<DynamicRow, DbError> {
    let mut dyn_row = DynamicRow::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = extract_sqlite_value(row, col);
        dyn_row.push(name, value);
    }
    Ok(dyn_row)
}

/// Extract a single SQLite column value.
///
/// SQLite is dynamically typed, so we try types in order of likelihood:
/// integer -> float -> text -> blob -> null.
fn extract_sqlite_value(
    row: &sqlx::sqlite::SqliteRow,
    col: &sqlx::sqlite::SqliteColumn,
) -> SqlValue {
    let ordinal = col.ordinal();
    let type_name = col.type_info().name().to_ascii_uppercase();

    match type_name.as_str() {
        "INTEGER" | "INT" | "BOOLEAN" => {
            match row.try_get::<Option<i64>, _>(ordinal) {
                Ok(Some(v)) => {
                    // SQLite booleans are stored as 0/1 integers
                    if type_name == "BOOLEAN" {
                        SqlValue::Bool(v != 0)
                    } else {
                        SqlValue::Integer(v)
                    }
                }
                _ => SqlValue::Null,
            }
        }
        "REAL" | "FLOAT" | "DOUBLE" => match row.try_get::<Option<f64>, _>(ordinal) {
            Ok(Some(v)) => SqlValue::Float(v),
            _ => SqlValue::Null,
        },
        "BLOB" => match row.try_get::<Option<Vec<u8>>, _>(ordinal) {
            Ok(Some(v)) => SqlValue::Bytes(v),
            _ => SqlValue::Null,
        },
        "NULL" => SqlValue::Null,
        _ => {
            // TEXT and everything else
            match row.try_get::<Option<String>, _>(ordinal) {
                Ok(Some(v)) => SqlValue::String(v),
                Ok(None) => SqlValue::Null,
                Err(_) => match row.try_get::<Option<Vec<u8>>, _>(ordinal) {
                    Ok(Some(v)) => SqlValue::Bytes(v),
                    _ => SqlValue::Null,
                },
            }
        }
    }
}
