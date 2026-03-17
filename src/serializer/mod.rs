// Author: Udaykiran Atta
// License: MIT

//! JSON serialization and request validation for database rows.
//!
//! Three sub-modules:
//!
//! - **`json`** -- High-performance row-to-JSON conversion using both
//!   `serde_json::Value` and a manual `Vec<u8>` writer with `itoa`/`ryu`.
//! - **`request_validator`** -- Validates request bodies against table
//!   metadata for create/update operations.
//!
//! The legacy `row_to_json` / `row_to_json_raw` functions (operating on
//! `sqlx::any::AnyRow`) are kept below for backward compatibility with the
//! AnyPool-based code path.

pub mod json;
pub mod request_validator;

// Re-export the most common entry points at the module level.
pub use json::{serialize_row, serialize_rows, write_row_json, write_rows_json};
pub use request_validator::{validate_create, validate_update};

// =========================================================================
// Legacy AnyRow serializer (backward-compatible with AnyPool path)
// =========================================================================

use serde_json::Value;
use sqlx::{Column, Row, ValueRef};

use crate::schema::types::{SqlType, TableMeta};

/// Serialize a single `sqlx::any::AnyRow` to a JSON object.
///
/// Uses `meta` to determine the correct JSON type for each column.
/// Falls back to string representation for unknown or unmappable types.
pub fn row_to_json(row: &sqlx::any::AnyRow, meta: &TableMeta) -> Value {
    let mut obj = serde_json::Map::with_capacity(meta.columns.len());

    for col_meta in &meta.columns {
        let name = &col_meta.name;

        let value = match extract_value(row, name, col_meta.sql_type) {
            Some(v) => v,
            None => Value::Null,
        };

        obj.insert(name.clone(), value);
    }

    Value::Object(obj)
}

/// Serialize a raw `AnyRow` to JSON without type metadata.
///
/// All values are serialized via best-effort type detection.
pub fn row_to_json_raw(row: &sqlx::any::AnyRow) -> Value {
    let mut obj = serde_json::Map::new();

    for col in row.columns() {
        let name = col.name().to_string();
        let value = if let Ok(v) = row.try_get::<String, _>(col.ordinal()) {
            serde_json::json!(v)
        } else if let Ok(v) = row.try_get::<i64, _>(col.ordinal()) {
            serde_json::json!(v)
        } else if let Ok(v) = row.try_get::<f64, _>(col.ordinal()) {
            serde_json::json!(v)
        } else if let Ok(v) = row.try_get::<bool, _>(col.ordinal()) {
            serde_json::json!(v)
        } else {
            Value::Null
        };
        obj.insert(name, value);
    }

    Value::Object(obj)
}

/// Extract a typed value from an AnyRow column.
fn extract_value(
    row: &sqlx::any::AnyRow,
    col_name: &str,
    sql_type: SqlType,
) -> Option<Value> {
    let col_idx = row.columns().iter().position(|c| c.name() == col_name)?;

    if row.try_get_raw(col_idx).ok()?.is_null() {
        return Some(Value::Null);
    }

    match sql_type {
        SqlType::Integer => {
            if let Ok(v) = row.try_get::<i64, _>(col_idx) {
                Some(serde_json::json!(v))
            } else if let Ok(v) = row.try_get::<i32, _>(col_idx) {
                Some(serde_json::json!(v))
            } else if let Ok(v) = row.try_get::<String, _>(col_idx) {
                v.parse::<i64>().ok().map(|n| serde_json::json!(n))
            } else {
                None
            }
        }
        SqlType::Float => {
            if let Ok(v) = row.try_get::<f64, _>(col_idx) {
                Some(serde_json::json!(v))
            } else if let Ok(v) = row.try_get::<String, _>(col_idx) {
                v.parse::<f64>().ok().map(|n| serde_json::json!(n))
            } else {
                None
            }
        }
        SqlType::Decimal => {
            if let Ok(v) = row.try_get::<String, _>(col_idx) {
                if let Ok(n) = v.parse::<f64>() {
                    Some(serde_json::json!(n))
                } else {
                    Some(serde_json::json!(v))
                }
            } else if let Ok(v) = row.try_get::<f64, _>(col_idx) {
                Some(serde_json::json!(v))
            } else {
                None
            }
        }
        SqlType::Boolean => {
            if let Ok(v) = row.try_get::<bool, _>(col_idx) {
                Some(serde_json::json!(v))
            } else if let Ok(v) = row.try_get::<i32, _>(col_idx) {
                Some(serde_json::json!(v != 0))
            } else if let Ok(v) = row.try_get::<String, _>(col_idx) {
                Some(serde_json::json!(
                    v == "1" || v.eq_ignore_ascii_case("true")
                ))
            } else {
                None
            }
        }
        SqlType::DateTime | SqlType::Date | SqlType::Time | SqlType::String | SqlType::Uuid => {
            row.try_get::<String, _>(col_idx)
                .ok()
                .map(|v| serde_json::json!(v))
        }
        SqlType::Json => {
            if let Ok(v) = row.try_get::<String, _>(col_idx) {
                Some(
                    serde_json::from_str(&v)
                        .unwrap_or_else(|_| serde_json::json!(v)),
                )
            } else {
                None
            }
        }
        SqlType::Binary | SqlType::Unknown => row
            .try_get::<String, _>(col_idx)
            .ok()
            .map(|v| serde_json::json!(v)),
    }
}
