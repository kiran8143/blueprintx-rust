// Author: Udaykiran Atta
// License: MIT

//! Schema introspection and model registry.
//!
//! Discovers table metadata at startup by querying `INFORMATION_SCHEMA`
//! (MySQL/Postgres) or `PRAGMA` (SQLite) and stores results in a
//! thread-safe [`ModelRegistry`] singleton.

pub mod introspector;
pub mod registry;
pub mod type_mapper;
pub mod types;

// Re-export commonly used types at the module level.
pub use types::{
    BlueprintError, ColumnMeta, ForeignKeyMeta, JsonType, SqlType, TableMeta, TypeMapping,
    ValidationError,
};

pub use registry::ModelRegistry;

use sqlx::{AnyPool, Row};

use crate::config::Config;
use crate::db;

// ---------------------------------------------------------------------------
// Helper: extract string from AnyRow, handling MySQL BLOB → String coercion.
// Azure MySQL returns INFORMATION_SCHEMA columns as binary/blob even after
// CAST(... AS CHAR).  We try String first, then fall back to raw bytes.
// ---------------------------------------------------------------------------

fn any_row_str(row: &sqlx::any::AnyRow, col: &str) -> String {
    match row.try_get::<String, _>(col) {
        Ok(s) => s,
        Err(_) => {
            // Fallback: read as raw bytes and decode as UTF-8
            match row.try_get::<Vec<u8>, _>(col) {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(_) => String::new(),
            }
        }
    }
}

fn any_row_i32(row: &sqlx::any::AnyRow, col: &str) -> i32 {
    row.try_get::<i32, _>(col).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Schema Introspection (AnyPool-based, used at application startup)
// ---------------------------------------------------------------------------

/// Run schema introspection against the connected database.
///
/// Discovers all user tables and their columns, primary keys, and foreign
/// keys.  Results are stored in [`ModelRegistry`].
pub async fn introspect() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::global();
    let pool = db::pool();
    let registry = ModelRegistry::instance();

    match cfg.db_engine_normalised() {
        "mysql" => introspect_mysql(pool, &cfg.db_name, registry).await?,
        "postgres" => introspect_postgres(pool, registry).await?,
        "sqlite" => introspect_sqlite(pool, registry).await?,
        other => {
            log::warn!("Unknown DB engine '{}' -- skipping introspection", other);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// MySQL introspection
// ---------------------------------------------------------------------------

async fn introspect_mysql(
    pool: &AnyPool,
    db_name: &str,
    registry: &ModelRegistry,
) -> Result<(), Box<dyn std::error::Error>> {
    use type_mapper::TypeMapper;

    // Discover tables
    // NOTE: Azure MySQL returns INFORMATION_SCHEMA columns as BLOB via the
    // `any` driver.  We use any_row_str() to handle BLOB→String coercion.
    let table_rows: Vec<sqlx::any::AnyRow> = sqlx::query(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
         ORDER BY TABLE_NAME",
    )
    .bind(db_name)
    .fetch_all(pool)
    .await?;

    let table_names: Vec<String> = table_rows
        .iter()
        .map(|r| any_row_str(r, "TABLE_NAME"))
        .collect();

    for table_name in &table_names {
        let col_rows: Vec<sqlx::any::AnyRow> = sqlx::query(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, \
             COALESCE(COLUMN_DEFAULT, '') AS COL_DEFAULT, ORDINAL_POSITION, \
             COALESCE(EXTRA, '') AS EXTRA \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
        )
        .bind(db_name)
        .bind(table_name.as_str())
        .fetch_all(pool)
        .await?;

        let mut columns = Vec::with_capacity(col_rows.len());
        let mut primary_keys: Vec<String> = Vec::new();

        for row in &col_rows {
            let col_name = any_row_str(row, "COLUMN_NAME");
            let data_type = any_row_str(row, "DATA_TYPE");
            let column_type = any_row_str(row, "COLUMN_TYPE");
            let nullable = any_row_str(row, "IS_NULLABLE");
            let col_key = any_row_str(row, "COLUMN_KEY");
            let default_val = any_row_str(row, "COL_DEFAULT");
            let extra = any_row_str(row, "EXTRA");
            let ordinal = any_row_i32(row, "ORDINAL_POSITION");

            let mapping = TypeMapper::map_mysql_type(&data_type, &column_type);
            let is_pk = col_key == "PRI";
            let is_auto = extra.contains("auto_increment");

            if is_pk {
                primary_keys.push(col_name.clone());
            }

            columns.push(ColumnMeta {
                name: col_name,
                raw_type: column_type,
                sql_type: mapping.sql_type,
                json_type: mapping.json_type,
                is_nullable: nullable == "YES",
                is_primary_key: is_pk,
                is_auto_increment: is_auto,
                default_value: if default_val.is_empty() {
                    None
                } else {
                    Some(default_val)
                },
                max_length: None,
                precision: None,
                scale: None,
                ordinal_position: ordinal,
                ..Default::default()
            });
        }

        let meta = TableMeta::new(
            table_name.clone(),
            db_name.to_string(),
            columns,
            primary_keys,
            Vec::new(),
        );

        registry.register_table(meta);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// PostgreSQL introspection
// ---------------------------------------------------------------------------

async fn introspect_postgres(
    pool: &AnyPool,
    registry: &ModelRegistry,
) -> Result<(), Box<dyn std::error::Error>> {
    use type_mapper::TypeMapper;

    let table_rows: Vec<sqlx::any::AnyRow> = sqlx::query(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_type = 'BASE TABLE' \
         ORDER BY table_name",
    )
    .fetch_all(pool)
    .await?;

    let table_names: Vec<String> = table_rows
        .iter()
        .map(|r| r.get::<String, _>("table_name"))
        .collect();

    for table_name in &table_names {
        let col_rows: Vec<sqlx::any::AnyRow> = sqlx::query(
            "SELECT column_name, data_type, COALESCE(udt_name, '') AS udt_name, is_nullable, \
             ordinal_position \
             FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = $1 \
             ORDER BY ordinal_position",
        )
        .bind(table_name.as_str())
        .fetch_all(pool)
        .await?;

        // Discover primary keys
        let pk_rows: Vec<sqlx::any::AnyRow> = sqlx::query(
            "SELECT a.attname \
             FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = $1::regclass AND i.indisprimary",
        )
        .bind(table_name.as_str())
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        let primary_keys: Vec<String> = pk_rows
            .iter()
            .map(|r| r.get::<String, _>("attname"))
            .collect();

        let mut columns = Vec::with_capacity(col_rows.len());

        for row in &col_rows {
            let col_name: String = row.get("column_name");
            let data_type: String = row.get("data_type");
            let udt_name: String = row.get("udt_name");
            let nullable: String = row.get("is_nullable");
            let ordinal: i32 = row.get("ordinal_position");

            let mapping = TypeMapper::map_postgres_type(&data_type, &udt_name);

            columns.push(ColumnMeta {
                name: col_name.clone(),
                raw_type: udt_name,
                sql_type: mapping.sql_type,
                json_type: mapping.json_type,
                is_nullable: nullable == "YES",
                is_primary_key: primary_keys.contains(&col_name),
                is_auto_increment: false,
                default_value: None,
                max_length: None,
                precision: None,
                scale: None,
                ordinal_position: ordinal,
                ..Default::default()
            });
        }

        let meta = TableMeta::new(
            table_name.clone(),
            "public".to_string(),
            columns,
            primary_keys,
            Vec::new(),
        );

        registry.register_table(meta);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// SQLite introspection
// ---------------------------------------------------------------------------

async fn introspect_sqlite(
    pool: &AnyPool,
    registry: &ModelRegistry,
) -> Result<(), Box<dyn std::error::Error>> {
    use type_mapper::TypeMapper;

    let table_rows: Vec<sqlx::any::AnyRow> = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(pool)
    .await?;

    let table_names: Vec<String> = table_rows
        .iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();

    for table_name in &table_names {
        // SQLite PRAGMA must use string interpolation (cannot bind in PRAGMA)
        let pragma_sql = format!("PRAGMA table_info(\"{}\")", table_name);
        let col_rows: Vec<sqlx::any::AnyRow> = sqlx::query(&pragma_sql)
            .fetch_all(pool)
            .await?;

        let mut columns = Vec::with_capacity(col_rows.len());
        let mut primary_keys: Vec<String> = Vec::new();

        for row in &col_rows {
            let cid: i32 = row.get("cid");
            let col_name: String = row.get("name");
            let col_type: String = row.get("type");
            let notnull: i32 = row.get("notnull");
            let default_val: String = row.try_get("dflt_value").unwrap_or_default();
            let pk: i32 = row.get("pk");

            let mapping = TypeMapper::map_sqlite_type(&col_type);

            if pk > 0 {
                primary_keys.push(col_name.clone());
            }

            columns.push(ColumnMeta {
                name: col_name,
                raw_type: col_type.clone(),
                sql_type: mapping.sql_type,
                json_type: mapping.json_type,
                is_nullable: notnull == 0,
                is_primary_key: pk > 0,
                is_auto_increment: pk > 0 && col_type.to_uppercase().contains("INTEGER"),
                default_value: if default_val.is_empty() {
                    None
                } else {
                    Some(default_val)
                },
                max_length: None,
                precision: None,
                scale: None,
                ordinal_position: cid,
                ..Default::default()
            });
        }

        let meta = TableMeta::new(
            table_name.clone(),
            "main".to_string(),
            columns,
            primary_keys,
            Vec::new(),
        );

        registry.register_table(meta);
    }

    Ok(())
}
