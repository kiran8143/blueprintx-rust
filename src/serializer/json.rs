// Author: Udaykiran Atta
// License: MIT

//! High-performance JSON serialization for database rows.
//!
//! Two serialization paths:
//!
//! 1. **serde_json path** (`serialize_row`, `serialize_rows`) -- returns
//!    `serde_json::Value` objects.  Convenient, integrates with Actix
//!    responders, but allocates.
//!
//! 2. **Manual writer path** (`write_row_json`, `write_rows_json`) -- writes
//!    directly into a `Vec<u8>` buffer using `itoa`/`ryu` for numeric
//!    formatting and pre-escaped JSON keys.  Avoids intermediate
//!    `serde_json::Value` allocations for maximum throughput.

use crate::schema::types::{ColumnMeta, DynamicRow, SqlType, SqlValue, TableMeta};

// =========================================================================
// serde_json path
// =========================================================================

/// Serialize a single `DynamicRow` to a `serde_json::Value` object.
///
/// Uses `meta` to determine the correct JSON type for each column:
/// - `SqlType::Integer` -> JSON number
/// - `SqlType::Float` / `Decimal` -> JSON number
/// - `SqlType::Boolean` -> JSON bool
/// - `SqlType::Json` -> parsed JSON (not raw string)
/// - `SqlType::String` / `DateTime` / `Date` / `Time` / `Uuid` -> JSON string
/// - null -> JSON null
pub fn serialize_row(row: &DynamicRow, meta: &TableMeta) -> serde_json::Value {
    let mut obj = serde_json::Map::with_capacity(row.columns.len());

    for (col_name, value) in &row.columns {
        let json_val = if let Some(col_meta) = meta.get_column(col_name) {
            value_to_json(value, col_meta)
        } else {
            // Column not in metadata -- fall back to string.
            value_to_json_string(value)
        };
        obj.insert(col_name.clone(), json_val);
    }

    serde_json::Value::Object(obj)
}

/// Serialize a slice of `DynamicRow` to a JSON array.
pub fn serialize_rows(rows: &[DynamicRow], meta: &TableMeta) -> serde_json::Value {
    serde_json::Value::Array(rows.iter().map(|r| serialize_row(r, meta)).collect())
}

/// Map a single `SqlValue` to `serde_json::Value` using column metadata.
fn value_to_json(value: &SqlValue, col: &ColumnMeta) -> serde_json::Value {
    match value {
        SqlValue::Null => serde_json::Value::Null,

        SqlValue::Integer(i) => match col.sql_type {
            SqlType::Boolean => serde_json::Value::Bool(*i != 0),
            _ => serde_json::json!(*i),
        },

        SqlValue::Float(f) => serde_json::json!(*f),

        SqlValue::Bool(b) => serde_json::Value::Bool(*b),

        SqlValue::String(s) => match col.sql_type {
            SqlType::Json => {
                // Parse JSON columns into actual JSON structures.
                serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.clone()))
            }
            SqlType::Integer => {
                // Some drivers return integers as strings.
                if let Ok(n) = s.parse::<i64>() {
                    serde_json::json!(n)
                } else {
                    serde_json::Value::String(s.clone())
                }
            }
            SqlType::Float | SqlType::Decimal => {
                if let Ok(n) = s.parse::<f64>() {
                    serde_json::json!(n)
                } else {
                    serde_json::Value::String(s.clone())
                }
            }
            SqlType::Boolean => {
                let lower = s.to_ascii_lowercase();
                serde_json::Value::Bool(lower == "true" || lower == "1" || lower == "t")
            }
            _ => serde_json::Value::String(s.clone()),
        },

        SqlValue::Bytes(b) => {
            // Attempt to decode as UTF-8, else hex-encode.
            match std::str::from_utf8(b) {
                Ok(s) => serde_json::Value::String(s.to_string()),
                Err(_) => serde_json::Value::String(hex_encode(b)),
            }
        }
    }
}

/// Fall-back: convert a `SqlValue` to a JSON string representation.
fn value_to_json_string(value: &SqlValue) -> serde_json::Value {
    match value {
        SqlValue::Null => serde_json::Value::Null,
        SqlValue::Integer(i) => serde_json::json!(*i),
        SqlValue::Float(f) => serde_json::json!(*f),
        SqlValue::Bool(b) => serde_json::Value::Bool(*b),
        SqlValue::String(s) => serde_json::Value::String(s.clone()),
        SqlValue::Bytes(b) => {
            match std::str::from_utf8(b) {
                Ok(s) => serde_json::Value::String(s.to_string()),
                Err(_) => serde_json::Value::String(hex_encode(b)),
            }
        }
    }
}

// =========================================================================
// Manual JSON writer (high-performance, zero-allocation path)
// =========================================================================

/// Write a single `DynamicRow` as a JSON object directly into `buf`.
///
/// Uses `itoa` for integers and `ryu` for floats.  Pre-escaped JSON keys
/// from `ColumnMeta::json_key_bytes` are written as raw bytes for maximum
/// throughput.
pub fn write_row_json(buf: &mut Vec<u8>, row: &DynamicRow, meta: &TableMeta) {
    buf.push(b'{');

    let mut first = true;
    for (col_name, value) in &row.columns {
        if !first {
            buf.push(b',');
        }
        first = false;

        // Write the key -- use pre-escaped bytes if available.
        if let Some(col_meta) = meta.get_column(col_name) {
            if !col_meta.json_key_bytes.is_empty() {
                buf.extend_from_slice(&col_meta.json_key_bytes);
            } else {
                write_json_key(buf, col_name);
            }
            write_value(buf, value, col_meta);
        } else {
            write_json_key(buf, col_name);
            write_value_raw(buf, value);
        }
    }

    buf.push(b'}');
}

/// Write a slice of `DynamicRow` as a JSON array directly into `buf`.
pub fn write_rows_json(buf: &mut Vec<u8>, rows: &[DynamicRow], meta: &TableMeta) {
    buf.push(b'[');

    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        write_row_json(buf, row, meta);
    }

    buf.push(b']');
}

// =========================================================================
// Manual writer: internal helpers
// =========================================================================

/// Write `"key":` to the buffer with JSON escaping.
fn write_json_key(buf: &mut Vec<u8>, key: &str) {
    buf.push(b'"');
    write_json_escaped(buf, key);
    buf.push(b'"');
    buf.push(b':');
}

/// Write a value using column type information.
fn write_value(buf: &mut Vec<u8>, value: &SqlValue, col: &ColumnMeta) {
    match value {
        SqlValue::Null => buf.extend_from_slice(b"null"),

        SqlValue::Integer(i) => match col.sql_type {
            SqlType::Boolean => {
                if *i != 0 {
                    buf.extend_from_slice(b"true");
                } else {
                    buf.extend_from_slice(b"false");
                }
            }
            _ => {
                let mut itoa_buf = itoa::Buffer::new();
                buf.extend_from_slice(itoa_buf.format(*i).as_bytes());
            }
        },

        SqlValue::Float(f) => {
            let mut ryu_buf = ryu::Buffer::new();
            buf.extend_from_slice(ryu_buf.format(*f).as_bytes());
        }

        SqlValue::Bool(b) => {
            if *b {
                buf.extend_from_slice(b"true");
            } else {
                buf.extend_from_slice(b"false");
            }
        }

        SqlValue::String(s) => match col.sql_type {
            SqlType::Json => {
                // JSON columns: write raw bytes -- the value is already
                // valid JSON from the database.  If it fails to parse as
                // valid JSON, fall back to a quoted string.
                if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                    buf.extend_from_slice(s.as_bytes());
                } else {
                    buf.push(b'"');
                    write_json_escaped(buf, s);
                    buf.push(b'"');
                }
            }
            SqlType::Integer => {
                // Drivers that return ints as text -- write unquoted.
                if s.parse::<i64>().is_ok() {
                    buf.extend_from_slice(s.as_bytes());
                } else {
                    buf.push(b'"');
                    write_json_escaped(buf, s);
                    buf.push(b'"');
                }
            }
            SqlType::Float | SqlType::Decimal => {
                if s.parse::<f64>().is_ok() {
                    buf.extend_from_slice(s.as_bytes());
                } else {
                    buf.push(b'"');
                    write_json_escaped(buf, s);
                    buf.push(b'"');
                }
            }
            SqlType::Boolean => {
                let lower = s.to_ascii_lowercase();
                if lower == "true" || lower == "1" || lower == "t" {
                    buf.extend_from_slice(b"true");
                } else {
                    buf.extend_from_slice(b"false");
                }
            }
            _ => {
                buf.push(b'"');
                write_json_escaped(buf, s);
                buf.push(b'"');
            }
        },

        SqlValue::Bytes(b) => {
            match std::str::from_utf8(b) {
                Ok(s) => {
                    buf.push(b'"');
                    write_json_escaped(buf, s);
                    buf.push(b'"');
                }
                Err(_) => {
                    buf.push(b'"');
                    let hex = hex_encode(b);
                    buf.extend_from_slice(hex.as_bytes());
                    buf.push(b'"');
                }
            }
        }
    }
}

/// Write a value without column type information (fallback).
fn write_value_raw(buf: &mut Vec<u8>, value: &SqlValue) {
    match value {
        SqlValue::Null => buf.extend_from_slice(b"null"),
        SqlValue::Integer(i) => {
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(*i).as_bytes());
        }
        SqlValue::Float(f) => {
            let mut ryu_buf = ryu::Buffer::new();
            buf.extend_from_slice(ryu_buf.format(*f).as_bytes());
        }
        SqlValue::Bool(b) => {
            if *b {
                buf.extend_from_slice(b"true");
            } else {
                buf.extend_from_slice(b"false");
            }
        }
        SqlValue::String(s) => {
            buf.push(b'"');
            write_json_escaped(buf, s);
            buf.push(b'"');
        }
        SqlValue::Bytes(b) => {
            match std::str::from_utf8(b) {
                Ok(s) => {
                    buf.push(b'"');
                    write_json_escaped(buf, s);
                    buf.push(b'"');
                }
                Err(_) => {
                    buf.push(b'"');
                    let hex = hex_encode(b);
                    buf.extend_from_slice(hex.as_bytes());
                    buf.push(b'"');
                }
            }
        }
    }
}

/// Write a JSON-escaped string to the buffer (handles `"`, `\`, and
/// control characters).
fn write_json_escaped(buf: &mut Vec<u8>, s: &str) {
    for byte in s.bytes() {
        match byte {
            b'"' => {
                buf.push(b'\\');
                buf.push(b'"');
            }
            b'\\' => {
                buf.push(b'\\');
                buf.push(b'\\');
            }
            b'\n' => {
                buf.push(b'\\');
                buf.push(b'n');
            }
            b'\r' => {
                buf.push(b'\\');
                buf.push(b'r');
            }
            b'\t' => {
                buf.push(b'\\');
                buf.push(b't');
            }
            0x00..=0x1f => {
                // Control characters as \u00XX
                buf.extend_from_slice(b"\\u00");
                buf.push(HEX[(byte >> 4) as usize]);
                buf.push(HEX[(byte & 0x0f) as usize]);
            }
            _ => buf.push(byte),
        }
    }
}

static HEX: [u8; 16] = *b"0123456789abcdef";

/// Simple hex encoder for binary data.
fn hex_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        out.push(HEX[(*byte >> 4) as usize] as char);
        out.push(HEX[(*byte & 0x0f) as usize] as char);
    }
    out
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{ColumnMeta, JsonType};

    fn make_meta() -> TableMeta {
        TableMeta::new(
            "test".to_string(),
            "public".to_string(),
            vec![
                ColumnMeta {
                    name: "id".into(),
                    raw_type: "int4".into(),
                    sql_type: SqlType::Integer,
                    json_type: JsonType::Number,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "name".into(),
                    raw_type: "varchar".into(),
                    sql_type: SqlType::String,
                    json_type: JsonType::String,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "active".into(),
                    raw_type: "bool".into(),
                    sql_type: SqlType::Boolean,
                    json_type: JsonType::Boolean,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "score".into(),
                    raw_type: "float8".into(),
                    sql_type: SqlType::Float,
                    json_type: JsonType::Number,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "tags".into(),
                    raw_type: "jsonb".into(),
                    sql_type: SqlType::Json,
                    json_type: JsonType::Object,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "deleted_at".into(),
                    raw_type: "timestamp".into(),
                    sql_type: SqlType::DateTime,
                    json_type: JsonType::String,
                    ..Default::default()
                },
            ],
            vec!["id".to_string()],
            Vec::new(),
        )
    }

    fn make_row() -> DynamicRow {
        let mut row = DynamicRow::new();
        row.push("id".into(), SqlValue::Integer(42));
        row.push("name".into(), SqlValue::String("Alice".into()));
        row.push("active".into(), SqlValue::Bool(true));
        row.push("score".into(), SqlValue::Float(3.14));
        row.push(
            "tags".into(),
            SqlValue::String(r#"["rust","api"]"#.into()),
        );
        row.push("deleted_at".into(), SqlValue::Null);
        row
    }

    #[test]
    fn test_serialize_row() {
        let meta = make_meta();
        let row = make_row();
        let json = serialize_row(&row, &meta);

        assert_eq!(json["id"], serde_json::json!(42));
        assert_eq!(json["name"], serde_json::json!("Alice"));
        assert_eq!(json["active"], serde_json::json!(true));
        assert_eq!(json["score"], serde_json::json!(3.14));
        // JSON column should be parsed, not a raw string
        assert_eq!(json["tags"], serde_json::json!(["rust", "api"]));
        assert_eq!(json["deleted_at"], serde_json::Value::Null);
    }

    #[test]
    fn test_serialize_rows() {
        let meta = make_meta();
        let rows = vec![make_row(), make_row()];
        let json = serialize_rows(&rows, &meta);
        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_write_row_json() {
        let meta = make_meta();
        let row = make_row();
        let mut buf = Vec::new();
        write_row_json(&mut buf, &row, &meta);
        let s = String::from_utf8(buf).unwrap();

        // Parse it back to verify it's valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed["id"], serde_json::json!(42));
        assert_eq!(parsed["name"], serde_json::json!("Alice"));
        assert_eq!(parsed["active"], serde_json::json!(true));
        assert_eq!(parsed["deleted_at"], serde_json::Value::Null);
        // JSON column should be embedded, not quoted
        assert_eq!(parsed["tags"], serde_json::json!(["rust", "api"]));
    }

    #[test]
    fn test_write_rows_json() {
        let meta = make_meta();
        let rows = vec![make_row()];
        let mut buf = Vec::new();
        write_rows_json(&mut buf, &rows, &meta);
        let s = String::from_utf8(buf).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_json_escape_special_chars() {
        let meta = make_meta();
        let mut row = DynamicRow::new();
        row.push("id".into(), SqlValue::Integer(1));
        row.push(
            "name".into(),
            SqlValue::String("He said \"hello\"\nand left".into()),
        );
        row.push("active".into(), SqlValue::Bool(false));
        row.push("score".into(), SqlValue::Float(0.0));
        row.push("tags".into(), SqlValue::Null);
        row.push("deleted_at".into(), SqlValue::Null);

        let mut buf = Vec::new();
        write_row_json(&mut buf, &row, &meta);
        let s = String::from_utf8(buf).unwrap();

        // Must be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(
            parsed["name"],
            serde_json::json!("He said \"hello\"\nand left")
        );
    }

    #[test]
    fn test_integer_as_string_column() {
        let meta = make_meta();
        let mut row = DynamicRow::new();
        row.push("id".into(), SqlValue::String("99".into()));
        row.push("name".into(), SqlValue::String("Bob".into()));
        row.push("active".into(), SqlValue::String("true".into()));
        row.push("score".into(), SqlValue::String("2.71".into()));
        row.push("tags".into(), SqlValue::Null);
        row.push("deleted_at".into(), SqlValue::Null);

        let json = serialize_row(&row, &meta);
        // Integer column with string value should be coerced to number
        assert_eq!(json["id"], serde_json::json!(99));
        // Boolean column with string value should be coerced
        assert_eq!(json["active"], serde_json::json!(true));
        // Float column with string value should be coerced
        assert_eq!(json["score"], serde_json::json!(2.71));
    }

    #[test]
    fn test_empty_row() {
        let meta = make_meta();
        let row = DynamicRow::new();
        let json = serialize_row(&row, &meta);
        assert!(json.as_object().unwrap().is_empty());
    }

    #[test]
    fn test_empty_rows_array() {
        let meta = make_meta();
        let rows: Vec<DynamicRow> = Vec::new();
        let mut buf = Vec::new();
        write_rows_json(&mut buf, &rows, &meta);
        assert_eq!(String::from_utf8(buf).unwrap(), "[]");
    }
}
