// Author: Udaykiran Atta
// License: MIT

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// SQL type abstraction across all supported databases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SqlType {
    Integer,
    Float,
    Decimal,
    String,
    Boolean,
    DateTime,
    Date,
    Time,
    Binary,
    Json,
    Uuid,
    Unknown,
}

/// JSON wire-format type for serialization decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JsonType {
    Number,
    String,
    Boolean,
    Null,
    Object,
    Array,
}

/// Type mapping result from database-specific type strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TypeMapping {
    pub sql_type: SqlType,
    pub json_type: JsonType,
}

/// Metadata for a single database column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: ::std::string::String,
    pub raw_type: ::std::string::String,
    pub sql_type: SqlType,
    pub json_type: JsonType,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub is_auto_increment: bool,
    pub default_value: Option<::std::string::String>,
    pub max_length: Option<i32>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
    pub ordinal_position: i32,
    /// Pre-escaped JSON key bytes (includes surrounding quotes and colon).
    /// Populated at schema load time for the manual JSON writer.
    /// e.g. `"column_name":` stored as raw bytes.
    #[serde(skip)]
    pub json_key_bytes: Vec<u8>,
}

impl Default for ColumnMeta {
    fn default() -> Self {
        Self {
            name: ::std::string::String::new(),
            raw_type: ::std::string::String::new(),
            sql_type: SqlType::Unknown,
            json_type: JsonType::String,
            is_nullable: true,
            is_primary_key: false,
            is_auto_increment: false,
            default_value: None,
            max_length: None,
            precision: None,
            scale: None,
            ordinal_position: 0,
            json_key_bytes: Vec::new(),
        }
    }
}

impl ColumnMeta {
    /// Populate `json_key_bytes` from `name`.
    /// Produces `"column_name":` as raw bytes for zero-copy JSON writing.
    pub fn precompute_json_key(&mut self) {
        let mut buf = Vec::with_capacity(self.name.len() + 3);
        buf.push(b'"');
        for byte in self.name.bytes() {
            match byte {
                b'"' => {
                    buf.push(b'\\');
                    buf.push(b'"');
                }
                b'\\' => {
                    buf.push(b'\\');
                    buf.push(b'\\');
                }
                _ => buf.push(byte),
            }
        }
        buf.push(b'"');
        buf.push(b':');
        self.json_key_bytes = buf;
    }
}

/// Metadata for a foreign key relationship.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyMeta {
    pub column_name: ::std::string::String,
    pub referenced_table: ::std::string::String,
    pub referenced_column: ::std::string::String,
    pub constraint_name: ::std::string::String,
}

/// Metadata for an entire table including columns, keys, and relationships.
#[derive(Debug, Clone)]
pub struct TableMeta {
    pub name: ::std::string::String,
    pub schema: ::std::string::String,
    pub columns: Vec<ColumnMeta>,
    pub primary_keys: Vec<::std::string::String>,
    pub foreign_keys: Vec<ForeignKeyMeta>,
    /// Fast lookup: column name -> index into `columns`.
    column_index: HashMap<::std::string::String, usize>,
}

impl TableMeta {
    /// Create a new `TableMeta` and build the column index.
    /// Pre-computes JSON key bytes for every column.
    pub fn new(
        name: ::std::string::String,
        schema: ::std::string::String,
        mut columns: Vec<ColumnMeta>,
        primary_keys: Vec<::std::string::String>,
        foreign_keys: Vec<ForeignKeyMeta>,
    ) -> Self {
        for col in &mut columns {
            col.precompute_json_key();
        }
        let column_index = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Self {
            name,
            schema,
            columns,
            primary_keys,
            foreign_keys,
            column_index,
        }
    }

    /// Build from raw fields (no pre-computation). Useful when deserializing.
    /// Calls `new()` internally so indexes and JSON keys are computed.
    pub fn build(
        name: ::std::string::String,
        schema: ::std::string::String,
        columns: Vec<ColumnMeta>,
        primary_keys: Vec<::std::string::String>,
        foreign_keys: Vec<ForeignKeyMeta>,
    ) -> Self {
        Self::new(name, schema, columns, primary_keys, foreign_keys)
    }

    /// Look up a column by name.  O(1) via hash map.
    pub fn get_column(&self, col_name: &str) -> Option<&ColumnMeta> {
        self.column_index.get(col_name).map(|&i| &self.columns[i])
    }

    /// Check whether a column with the given name exists.
    pub fn has_column(&self, col_name: &str) -> bool {
        self.column_index.contains_key(col_name)
    }

    /// Check whether a column is a "generic" framework-managed auto-field
    /// (excluded from required-field checks on create).
    pub fn is_generic_field(col_name: &str) -> bool {
        const GENERIC_FIELDS: &[&str] = &[
            "id",
            "code",
            "created_at",
            "updated_at",
            "created_by",
            "modified_by",
            "deleted_at",
            "deleted_by",
            "status",
        ];
        GENERIC_FIELDS.contains(&col_name)
    }
}

// ---------------------------------------------------------------------------
// Dynamic row - a database result row with heterogeneous column values
// ---------------------------------------------------------------------------

/// A single value from a database result row.
#[derive(Debug, Clone)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Float(f64),
    Bool(bool),
    String(::std::string::String),
    /// Raw bytes (for BLOB/Binary columns).
    Bytes(Vec<u8>),
}

impl SqlValue {
    /// Return `true` if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }

    /// Interpret the value as a string suitable for use as a SQL parameter.
    pub fn as_param_string(&self) -> Option<::std::string::String> {
        match self {
            SqlValue::Null => None,
            SqlValue::Integer(v) => Some(v.to_string()),
            SqlValue::Float(v) => Some(v.to_string()),
            SqlValue::Bool(v) => Some(if *v { "true".into() } else { "false".into() }),
            SqlValue::String(v) => Some(v.clone()),
            SqlValue::Bytes(v) => ::std::string::String::from_utf8(v.clone()).ok(),
        }
    }
}

/// A row returned from a dynamic query.  Preserves column order from the
/// result set so the serializer can iterate columns positionally.
#[derive(Debug, Clone)]
pub struct DynamicRow {
    /// (column_name, value) pairs in result-set order.
    pub columns: Vec<(::std::string::String, SqlValue)>,
}

impl DynamicRow {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Push a named column and its value.
    pub fn push(&mut self, column: ::std::string::String, value: SqlValue) {
        self.columns.push((column, value));
    }

    /// Get a value by column name.
    pub fn get(&self, name: &str) -> Option<&SqlValue> {
        self.columns
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v)
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Is the row empty?
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

impl Default for DynamicRow {
    fn default() -> Self {
        Self::new()
    }
}

impl serde::Serialize for DynamicRow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for (name, val) in &self.columns {
            match val {
                SqlValue::Null => map.serialize_entry(name, &())?,
                SqlValue::Integer(i) => map.serialize_entry(name, i)?,
                SqlValue::Float(f) => map.serialize_entry(name, f)?,
                SqlValue::Bool(b) => map.serialize_entry(name, b)?,
                SqlValue::String(s) => map.serialize_entry(name, s)?,
                SqlValue::Bytes(b) => {
                    // Serialize bytes as base64 string.
                    let encoded: ::std::string::String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                    map.serialize_entry(name, &encoded)?;
                }
            }
        }
        map.end()
    }
}

impl From<DynamicRow> for serde_json::Value {
    fn from(row: DynamicRow) -> serde_json::Value {
        let mut obj = serde_json::Map::with_capacity(row.columns.len());
        for (name, val) in row.columns {
            let json_val = match val {
                SqlValue::Null => serde_json::Value::Null,
                SqlValue::Integer(i) => serde_json::json!(i),
                SqlValue::Float(f) => serde_json::json!(f),
                SqlValue::Bool(b) => serde_json::json!(b),
                SqlValue::String(s) => serde_json::json!(s),
                SqlValue::Bytes(b) => {
                    let hex: ::std::string::String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                    serde_json::json!(hex)
                }
            };
            obj.insert(name, json_val);
        }
        serde_json::Value::Object(obj)
    }
}

// ---------------------------------------------------------------------------
// Database dialect
// ---------------------------------------------------------------------------

/// Supported SQL dialects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DbDialect {
    MySQL,
    PostgreSQL,
    SQLite,
}

impl DbDialect {
    /// Parse from the normalised engine name produced by
    /// `Config::db_engine_normalised`.
    pub fn from_engine(engine: &str) -> Self {
        match engine {
            "mysql" => DbDialect::MySQL,
            "postgres" => DbDialect::PostgreSQL,
            "sqlite" => DbDialect::SQLite,
            _ => DbDialect::MySQL,
        }
    }
}

// ---------------------------------------------------------------------------
// Database connection abstraction
// ---------------------------------------------------------------------------

/// Wrapper around the actual SQLx pool that abstracts the dialect.
#[derive(Debug, Clone)]
pub enum DatabasePool {
    MySQL(sqlx::MySqlPool),
    PostgreSQL(sqlx::PgPool),
    SQLite(sqlx::SqlitePool),
}

/// A shareable database connection handle.
#[derive(Debug, Clone)]
pub struct DatabaseConnection {
    pub pool: DatabasePool,
    pub dialect: DbDialect,
}

impl DatabaseConnection {
    pub fn new(pool: DatabasePool, dialect: DbDialect) -> Self {
        Self { pool, dialect }
    }

    pub fn is_mysql(&self) -> bool {
        self.dialect == DbDialect::MySQL
    }

    pub fn is_postgres(&self) -> bool {
        self.dialect == DbDialect::PostgreSQL
    }

    pub fn is_sqlite(&self) -> bool {
        self.dialect == DbDialect::SQLite
    }
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors produced by the query builder or serializer layer.
#[derive(Debug)]
pub enum BlueprintError {
    /// A query or builder usage error (invalid column, no table set, etc.).
    Query(::std::string::String),
    /// A database execution error from SQLx.
    Database(sqlx::Error),
    /// A serialization error.
    Serialization(::std::string::String),
    /// A validation error (multiple field-level errors).
    Validation(Vec<ValidationError>),
}

impl std::fmt::Display for BlueprintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlueprintError::Query(msg) => write!(f, "query error: {msg}"),
            BlueprintError::Database(err) => write!(f, "database error: {err}"),
            BlueprintError::Serialization(msg) => write!(f, "serialization error: {msg}"),
            BlueprintError::Validation(errs) => {
                write!(f, "validation errors: [")?;
                for (i, e) in errs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{e}")?;
                }
                write!(f, "]")
            }
        }
    }
}

impl std::error::Error for BlueprintError {}

impl From<sqlx::Error> for BlueprintError {
    fn from(err: sqlx::Error) -> Self {
        BlueprintError::Database(err)
    }
}

// ---------------------------------------------------------------------------
// Validation error
// ---------------------------------------------------------------------------

/// A single field-level validation error.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub field: ::std::string::String,
    pub code: ::std::string::String,
    pub message: ::std::string::String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} ({})", self.field, self.message, self.code)
    }
}

impl ValidationError {
    pub fn new(
        field: impl Into<::std::string::String>,
        code: impl Into<::std::string::String>,
        message: impl Into<::std::string::String>,
    ) -> Self {
        Self {
            field: field.into(),
            code: code.into(),
            message: message.into(),
        }
    }

    /// Convert a list of validation errors to a serde_json array.
    pub fn to_json_array(errors: &[ValidationError]) -> serde_json::Value {
        serde_json::Value::Array(
            errors
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "field": e.field,
                        "code": e.code,
                        "message": e.message,
                    })
                })
                .collect(),
        )
    }
}
