// Author: Udaykiran Atta
// License: MIT

use std::collections::HashMap;

use log::info;

use crate::db::connection::{query_dynamic, DbError};
use crate::schema::registry::ModelRegistry;
use crate::schema::type_mapper::TypeMapper;
use crate::schema::types::{
    ColumnMeta, DatabaseConnection, DbDialect, DynamicRow, ForeignKeyMeta,
    SqlValue, TableMeta,
};

// ---------------------------------------------------------------------------
// Helper: extract string/int from DynamicRow
// ---------------------------------------------------------------------------

/// Extract a string value from a DynamicRow column, returning empty string for NULL.
fn get_str(row: &DynamicRow, col: &str) -> String {
    match row.get(col) {
        Some(SqlValue::String(s)) => s.clone(),
        Some(SqlValue::Integer(i)) => i.to_string(),
        Some(SqlValue::Float(f)) => f.to_string(),
        Some(SqlValue::Bool(b)) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        _ => String::new(),
    }
}

/// Extract an optional string from a DynamicRow column.
fn get_opt_str(row: &DynamicRow, col: &str) -> Option<String> {
    match row.get(col) {
        Some(SqlValue::Null) | None => None,
        Some(SqlValue::String(s)) => Some(s.clone()),
        Some(SqlValue::Integer(i)) => Some(i.to_string()),
        Some(SqlValue::Float(f)) => Some(f.to_string()),
        Some(SqlValue::Bool(b)) => Some(if *b {
            "1".to_string()
        } else {
            "0".to_string()
        }),
        Some(SqlValue::Bytes(_)) => Some("<binary>".to_string()),
    }
}

/// Extract an integer from a DynamicRow column, returning 0 for missing/NULL.
fn get_int(row: &DynamicRow, col: &str) -> i32 {
    match row.get(col) {
        Some(SqlValue::Integer(i)) => *i as i32,
        Some(SqlValue::String(s)) => s.parse::<i32>().unwrap_or(0),
        _ => 0,
    }
}

/// Extract an optional i32 from a DynamicRow column.
fn get_opt_int(row: &DynamicRow, col: &str) -> Option<i32> {
    match row.get(col) {
        Some(SqlValue::Null) | None => None,
        Some(SqlValue::Integer(i)) => Some(*i as i32),
        Some(SqlValue::String(s)) => s.parse::<i32>().ok(),
        _ => None,
    }
}

/// Validate a SQLite table name to prevent SQL injection in PRAGMA statements.
fn is_valid_sqlite_table_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

// ===========================================================================
// MySQL introspection
// ===========================================================================

/// Discover all base table names in a MySQL schema.
///
/// Uses INFORMATION_SCHEMA.TABLES with parameterized schema filter.
async fn discover_tables_mysql(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<Vec<String>, DbError> {
    let sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
               WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' \
               ORDER BY TABLE_NAME";
    let rows = query_dynamic(conn, sql, &[schema]).await?;
    Ok(rows.iter().map(|r| get_str(r, "TABLE_NAME")).collect())
}

/// Discover all columns for all tables in a MySQL schema.
async fn discover_all_columns_mysql(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<HashMap<String, Vec<ColumnMeta>>, DbError> {
    let sql = "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, \
               COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, \
               CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, \
               NUMERIC_SCALE, COLUMN_TYPE, EXTRA \
               FROM INFORMATION_SCHEMA.COLUMNS \
               WHERE TABLE_SCHEMA = ? \
               ORDER BY TABLE_NAME, ORDINAL_POSITION";
    let rows = query_dynamic(conn, sql, &[schema]).await?;

    let mut all_columns: HashMap<String, Vec<ColumnMeta>> = HashMap::new();

    for row in &rows {
        let table_name = get_str(row, "TABLE_NAME");
        let data_type = get_str(row, "DATA_TYPE");
        let column_type = get_str(row, "COLUMN_TYPE");
        let extra = get_str(row, "EXTRA");

        let mapping = TypeMapper::map_mysql_type(&data_type, &column_type);

        let col = ColumnMeta {
            name: get_str(row, "COLUMN_NAME"),
            raw_type: column_type,
            sql_type: mapping.sql_type,
            json_type: mapping.json_type,
            ordinal_position: get_int(row, "ORDINAL_POSITION"),
            is_nullable: get_str(row, "IS_NULLABLE") == "YES",
            is_auto_increment: extra.contains("auto_increment"),
            default_value: get_opt_str(row, "COLUMN_DEFAULT"),
            max_length: get_opt_int(row, "CHARACTER_MAXIMUM_LENGTH"),
            precision: get_opt_int(row, "NUMERIC_PRECISION"),
            scale: get_opt_int(row, "NUMERIC_SCALE"),
            is_primary_key: false,
            json_key_bytes: Vec::new(),
        };

        all_columns.entry(table_name).or_default().push(col);
    }

    Ok(all_columns)
}

/// Discover all primary keys for all tables in a MySQL schema.
async fn discover_all_primary_keys_mysql(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<HashMap<String, Vec<String>>, DbError> {
    let sql = "SELECT TABLE_NAME, COLUMN_NAME \
               FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
               WHERE TABLE_SCHEMA = ? AND CONSTRAINT_NAME = 'PRIMARY' \
               ORDER BY TABLE_NAME, ORDINAL_POSITION";
    let rows = query_dynamic(conn, sql, &[schema]).await?;

    let mut all_pks: HashMap<String, Vec<String>> = HashMap::new();

    for row in &rows {
        let table_name = get_str(row, "TABLE_NAME");
        let column_name = get_str(row, "COLUMN_NAME");
        all_pks.entry(table_name).or_default().push(column_name);
    }

    Ok(all_pks)
}

/// Discover all foreign keys for all tables in a MySQL schema.
async fn discover_all_foreign_keys_mysql(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<HashMap<String, Vec<ForeignKeyMeta>>, DbError> {
    let sql = "SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, \
               REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
               FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
               WHERE TABLE_SCHEMA = ? AND REFERENCED_TABLE_NAME IS NOT NULL \
               ORDER BY TABLE_NAME, CONSTRAINT_NAME";
    let rows = query_dynamic(conn, sql, &[schema]).await?;

    let mut all_fks: HashMap<String, Vec<ForeignKeyMeta>> = HashMap::new();

    for row in &rows {
        let table_name = get_str(row, "TABLE_NAME");
        let fk = ForeignKeyMeta {
            constraint_name: get_str(row, "CONSTRAINT_NAME"),
            column_name: get_str(row, "COLUMN_NAME"),
            referenced_table: get_str(row, "REFERENCED_TABLE_NAME"),
            referenced_column: get_str(row, "REFERENCED_COLUMN_NAME"),
        };
        all_fks.entry(table_name).or_default().push(fk);
    }

    Ok(all_fks)
}

// ===========================================================================
// PostgreSQL introspection
// ===========================================================================

/// Discover all base table names in a Postgres schema.
async fn discover_tables_postgres(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<Vec<String>, DbError> {
    let sql = "SELECT table_name \
               FROM information_schema.tables \
               WHERE table_schema = $1 AND table_type = 'BASE TABLE' \
               ORDER BY table_name";
    let rows = query_dynamic(conn, sql, &[schema]).await?;
    Ok(rows.iter().map(|r| get_str(r, "table_name")).collect())
}

/// Discover all columns for all tables in a Postgres schema.
async fn discover_all_columns_postgres(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<HashMap<String, Vec<ColumnMeta>>, DbError> {
    let sql = "SELECT table_name, column_name, ordinal_position, column_default, \
               is_nullable, data_type, udt_name, character_maximum_length, \
               numeric_precision, numeric_scale, is_identity \
               FROM information_schema.columns \
               WHERE table_schema = $1 \
               ORDER BY table_name, ordinal_position";
    let rows = query_dynamic(conn, sql, &[schema]).await?;

    let mut all_columns: HashMap<String, Vec<ColumnMeta>> = HashMap::new();

    for row in &rows {
        let table_name = get_str(row, "table_name");
        let data_type = get_str(row, "data_type");
        let udt_name = get_str(row, "udt_name");

        let mapping = TypeMapper::map_postgres_type(&data_type, &udt_name);

        let column_default = get_opt_str(row, "column_default");
        let is_identity = get_str(row, "is_identity");

        // Auto-increment detection: nextval() sequences or identity columns
        let is_auto_increment = column_default
            .as_ref()
            .map(|d| d.starts_with("nextval("))
            .unwrap_or(false)
            || is_identity == "YES";

        let col = ColumnMeta {
            name: get_str(row, "column_name"),
            raw_type: udt_name,
            sql_type: mapping.sql_type,
            json_type: mapping.json_type,
            ordinal_position: get_int(row, "ordinal_position"),
            is_nullable: get_str(row, "is_nullable") == "YES",
            is_auto_increment,
            default_value: column_default,
            max_length: get_opt_int(row, "character_maximum_length"),
            precision: get_opt_int(row, "numeric_precision"),
            scale: get_opt_int(row, "numeric_scale"),
            is_primary_key: false,
            json_key_bytes: Vec::new(),
        };

        all_columns.entry(table_name).or_default().push(col);
    }

    Ok(all_columns)
}

/// Discover all primary keys for all tables in a Postgres schema.
async fn discover_all_primary_keys_postgres(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<HashMap<String, Vec<String>>, DbError> {
    let sql = "SELECT tc.table_name, kcu.column_name, kcu.ordinal_position \
               FROM information_schema.table_constraints AS tc \
               JOIN information_schema.key_column_usage AS kcu \
               ON tc.constraint_name = kcu.constraint_name \
               AND tc.table_schema = kcu.table_schema \
               WHERE tc.table_schema = $1 AND tc.constraint_type = 'PRIMARY KEY' \
               ORDER BY tc.table_name, kcu.ordinal_position";
    let rows = query_dynamic(conn, sql, &[schema]).await?;

    let mut all_pks: HashMap<String, Vec<String>> = HashMap::new();

    for row in &rows {
        let table_name = get_str(row, "table_name");
        let column_name = get_str(row, "column_name");
        all_pks.entry(table_name).or_default().push(column_name);
    }

    Ok(all_pks)
}

/// Discover all foreign keys for all tables in a Postgres schema.
async fn discover_all_foreign_keys_postgres(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<HashMap<String, Vec<ForeignKeyMeta>>, DbError> {
    let sql = "SELECT tc.table_name, kcu.column_name, tc.constraint_name, \
               ccu.table_name AS referenced_table_name, \
               ccu.column_name AS referenced_column_name \
               FROM information_schema.table_constraints AS tc \
               JOIN information_schema.key_column_usage AS kcu \
               ON tc.constraint_name = kcu.constraint_name \
               AND tc.table_schema = kcu.table_schema \
               JOIN information_schema.constraint_column_usage AS ccu \
               ON tc.constraint_name = ccu.constraint_name \
               AND tc.table_schema = ccu.table_schema \
               WHERE tc.table_schema = $1 AND tc.constraint_type = 'FOREIGN KEY' \
               ORDER BY tc.table_name, tc.constraint_name";
    let rows = query_dynamic(conn, sql, &[schema]).await?;

    let mut all_fks: HashMap<String, Vec<ForeignKeyMeta>> = HashMap::new();

    for row in &rows {
        let table_name = get_str(row, "table_name");
        let fk = ForeignKeyMeta {
            column_name: get_str(row, "column_name"),
            constraint_name: get_str(row, "constraint_name"),
            referenced_table: get_str(row, "referenced_table_name"),
            referenced_column: get_str(row, "referenced_column_name"),
        };
        all_fks.entry(table_name).or_default().push(fk);
    }

    Ok(all_fks)
}

// ===========================================================================
// SQLite introspection
// ===========================================================================

/// Discover all user tables in a SQLite database (excluding internal tables).
async fn discover_tables_sqlite(conn: &DatabaseConnection) -> Result<Vec<String>, DbError> {
    let sql = "SELECT name FROM sqlite_master \
               WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
               ORDER BY name";
    let rows = query_dynamic(conn, sql, &[]).await?;
    Ok(rows.iter().map(|r| get_str(r, "name")).collect())
}

/// Discover all columns for all tables in a SQLite database.
///
/// Uses PRAGMA table_info for each discovered table. Table names are validated
/// before interpolation to prevent SQL injection.
async fn discover_all_columns_sqlite(
    conn: &DatabaseConnection,
) -> Result<HashMap<String, Vec<ColumnMeta>>, DbError> {
    let tables = discover_tables_sqlite(conn).await?;
    let mut all_columns: HashMap<String, Vec<ColumnMeta>> = HashMap::new();

    for table_name in &tables {
        if !is_valid_sqlite_table_name(table_name) {
            log::warn!("Skipping table with invalid name: {}", table_name);
            continue;
        }

        let pragma_sql = format!("PRAGMA table_info('{}')", table_name);
        let rows = query_dynamic(conn, &pragma_sql, &[]).await?;

        let mut columns = Vec::with_capacity(rows.len());

        for row in &rows {
            let raw_type = get_str(row, "type");
            let mapping = TypeMapper::map_sqlite_type(&raw_type);

            let notnull = get_int(row, "notnull");
            let pk = get_int(row, "pk");

            // SQLite INTEGER PRIMARY KEY is auto-increment by nature
            let is_auto_increment = pk > 0 && raw_type.eq_ignore_ascii_case("INTEGER");

            let col = ColumnMeta {
                name: get_str(row, "name"),
                raw_type,
                sql_type: mapping.sql_type,
                json_type: mapping.json_type,
                ordinal_position: get_int(row, "cid"),
                is_nullable: notnull == 0,
                is_primary_key: pk > 0,
                is_auto_increment,
                default_value: get_opt_str(row, "dflt_value"),
                max_length: None,
                precision: None,
                scale: None,
                json_key_bytes: Vec::new(),
            };

            columns.push(col);
        }

        all_columns.insert(table_name.clone(), columns);
    }

    Ok(all_columns)
}

/// Discover all primary keys for all tables in a SQLite database.
async fn discover_all_primary_keys_sqlite(
    conn: &DatabaseConnection,
) -> Result<HashMap<String, Vec<String>>, DbError> {
    let tables = discover_tables_sqlite(conn).await?;
    let mut all_pks: HashMap<String, Vec<String>> = HashMap::new();

    for table_name in &tables {
        if !is_valid_sqlite_table_name(table_name) {
            log::warn!("Skipping table with invalid name: {}", table_name);
            continue;
        }

        let pragma_sql = format!("PRAGMA table_info('{}')", table_name);
        let rows = query_dynamic(conn, &pragma_sql, &[]).await?;

        // Collect PK columns ordered by their pk value (composite PK order)
        let mut pk_columns: Vec<(i32, String)> = Vec::new();

        for row in &rows {
            let pk = get_int(row, "pk");
            if pk > 0 {
                pk_columns.push((pk, get_str(row, "name")));
            }
        }

        // Sort by pk value to preserve composite primary key order
        pk_columns.sort_by_key(|(order, _)| *order);

        if !pk_columns.is_empty() {
            let keys: Vec<String> = pk_columns.into_iter().map(|(_, name)| name).collect();
            all_pks.insert(table_name.clone(), keys);
        }
    }

    Ok(all_pks)
}

/// Discover all foreign keys for all tables in a SQLite database.
async fn discover_all_foreign_keys_sqlite(
    conn: &DatabaseConnection,
) -> Result<HashMap<String, Vec<ForeignKeyMeta>>, DbError> {
    let tables = discover_tables_sqlite(conn).await?;
    let mut all_fks: HashMap<String, Vec<ForeignKeyMeta>> = HashMap::new();

    for table_name in &tables {
        if !is_valid_sqlite_table_name(table_name) {
            log::warn!("Skipping table with invalid name: {}", table_name);
            continue;
        }

        let pragma_sql = format!("PRAGMA foreign_key_list('{}')", table_name);
        let rows = query_dynamic(conn, &pragma_sql, &[]).await?;

        let mut foreign_keys = Vec::new();

        for row in &rows {
            let id = get_int(row, "id");
            let fk = ForeignKeyMeta {
                column_name: get_str(row, "from"),
                referenced_table: get_str(row, "table"),
                referenced_column: get_str(row, "to"),
                constraint_name: format!("fk_{}", id),
            };
            foreign_keys.push(fk);
        }

        if !foreign_keys.is_empty() {
            all_fks.insert(table_name.clone(), foreign_keys);
        }
    }

    Ok(all_fks)
}

// ===========================================================================
// Unified public interface
// ===========================================================================

/// Discover all base table names in the given schema.
pub async fn discover_tables(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<Vec<String>, DbError> {
    match conn.dialect {
        DbDialect::MySQL => discover_tables_mysql(conn, schema).await,
        DbDialect::PostgreSQL => discover_tables_postgres(conn, schema).await,
        DbDialect::SQLite => discover_tables_sqlite(conn).await,
    }
}

/// Discover all columns for all tables in the given schema.
pub async fn discover_all_columns(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<HashMap<String, Vec<ColumnMeta>>, DbError> {
    match conn.dialect {
        DbDialect::MySQL => discover_all_columns_mysql(conn, schema).await,
        DbDialect::PostgreSQL => discover_all_columns_postgres(conn, schema).await,
        DbDialect::SQLite => discover_all_columns_sqlite(conn).await,
    }
}

/// Discover all primary keys for all tables in the given schema.
pub async fn discover_all_primary_keys(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<HashMap<String, Vec<String>>, DbError> {
    match conn.dialect {
        DbDialect::MySQL => discover_all_primary_keys_mysql(conn, schema).await,
        DbDialect::PostgreSQL => discover_all_primary_keys_postgres(conn, schema).await,
        DbDialect::SQLite => discover_all_primary_keys_sqlite(conn).await,
    }
}

/// Discover all foreign keys for all tables in the given schema.
pub async fn discover_all_foreign_keys(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<HashMap<String, Vec<ForeignKeyMeta>>, DbError> {
    match conn.dialect {
        DbDialect::MySQL => discover_all_foreign_keys_mysql(conn, schema).await,
        DbDialect::PostgreSQL => discover_all_foreign_keys_postgres(conn, schema).await,
        DbDialect::SQLite => discover_all_foreign_keys_sqlite(conn).await,
    }
}

/// Full schema introspection: discovers tables, columns, primary keys, and
/// foreign keys, then populates the global ModelRegistry.
///
/// Mirrors the C++ SchemaIntrospector::introspectSchema logic exactly.
pub async fn introspect_schema(
    conn: &DatabaseConnection,
    schema: &str,
) -> Result<(), DbError> {
    let tables = discover_tables(conn, schema).await?;
    let all_columns = discover_all_columns(conn, schema).await?;
    let all_primary_keys = discover_all_primary_keys(conn, schema).await?;
    let all_foreign_keys = discover_all_foreign_keys(conn, schema).await?;

    let mut total_columns: usize = 0;
    let registry = ModelRegistry::instance();

    for table_name in &tables {
        let mut columns = all_columns.get(table_name).cloned().unwrap_or_default();
        let primary_keys = all_primary_keys.get(table_name).cloned().unwrap_or_default();
        let foreign_keys = all_foreign_keys.get(table_name).cloned().unwrap_or_default();

        // Mark primary key columns
        for col in &mut columns {
            if primary_keys.contains(&col.name) {
                col.is_primary_key = true;
            }
        }

        total_columns += columns.len();

        // TableMeta::new pre-computes column_index and json_key_bytes
        let meta = TableMeta::new(
            table_name.clone(),
            schema.to_string(),
            columns,
            primary_keys,
            foreign_keys,
        );

        registry.register_table(meta);
    }

    info!(
        "Schema introspection complete: {} tables, {} columns",
        tables.len(),
        total_columns
    );

    Ok(())
}
