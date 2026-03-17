// Author: Udaykiran Atta
// License: MIT

use crate::schema::types::{JsonType, SqlType, TypeMapping};

/// Maps database-specific type strings to unified SqlType/JsonType pairs.
///
/// Each mapper mirrors the exact logic from the C++ TypeMapper, ensuring
/// identical type resolution across the Rust and C++ implementations.
pub struct TypeMapper;

impl TypeMapper {
    /// Map a MySQL column type to unified types.
    ///
    /// `data_type` is the INFORMATION_SCHEMA DATA_TYPE (e.g. "int", "varchar").
    /// `column_type` is the full COLUMN_TYPE (e.g. "tinyint(1)", "varchar(255)").
    pub fn map_mysql_type(data_type: &str, column_type: &str) -> TypeMapping {
        let dt = data_type.to_ascii_uppercase();

        // MySQL BOOLEAN is tinyint(1) -- must check column_type
        if dt == "TINYINT" {
            if column_type.contains("tinyint(1)") {
                return TypeMapping {
                    sql_type: SqlType::Boolean,
                    json_type: JsonType::Boolean,
                };
            }
            return TypeMapping {
                sql_type: SqlType::Integer,
                json_type: JsonType::Number,
            };
        }

        if dt == "INT" || dt == "BIGINT" || dt == "SMALLINT" || dt == "MEDIUMINT" {
            return TypeMapping {
                sql_type: SqlType::Integer,
                json_type: JsonType::Number,
            };
        }
        if dt == "FLOAT" || dt == "DOUBLE" {
            return TypeMapping {
                sql_type: SqlType::Float,
                json_type: JsonType::Number,
            };
        }
        if dt == "DECIMAL" || dt == "NUMERIC" {
            return TypeMapping {
                sql_type: SqlType::Decimal,
                json_type: JsonType::Number,
            };
        }
        if dt == "VARCHAR"
            || dt == "CHAR"
            || dt == "TEXT"
            || dt == "MEDIUMTEXT"
            || dt == "LONGTEXT"
            || dt == "TINYTEXT"
            || dt == "ENUM"
            || dt == "SET"
        {
            return TypeMapping {
                sql_type: SqlType::String,
                json_type: JsonType::String,
            };
        }
        if dt == "DATETIME" || dt == "TIMESTAMP" {
            return TypeMapping {
                sql_type: SqlType::DateTime,
                json_type: JsonType::String,
            };
        }
        if dt == "DATE" {
            return TypeMapping {
                sql_type: SqlType::Date,
                json_type: JsonType::String,
            };
        }
        if dt == "TIME" {
            return TypeMapping {
                sql_type: SqlType::Time,
                json_type: JsonType::String,
            };
        }
        if dt == "JSON" {
            return TypeMapping {
                sql_type: SqlType::Json,
                json_type: JsonType::Object,
            };
        }
        if dt == "BLOB"
            || dt == "MEDIUMBLOB"
            || dt == "LONGBLOB"
            || dt == "TINYBLOB"
            || dt == "BINARY"
            || dt == "VARBINARY"
        {
            return TypeMapping {
                sql_type: SqlType::Binary,
                json_type: JsonType::String,
            };
        }

        TypeMapping {
            sql_type: SqlType::Unknown,
            json_type: JsonType::String,
        }
    }

    /// Map a PostgreSQL column type to unified types.
    ///
    /// `data_type` is the information_schema data_type (e.g. "character varying").
    /// `udt_name` is the user-defined type name (e.g. "int4", "uuid", "jsonb").
    pub fn map_postgres_type(data_type: &str, udt_name: &str) -> TypeMapping {
        let udt = udt_name.to_ascii_uppercase();

        if udt == "BOOL" {
            return TypeMapping {
                sql_type: SqlType::Boolean,
                json_type: JsonType::Boolean,
            };
        }
        if udt == "INT2" || udt == "INT4" || udt == "INT8" || udt == "SERIAL" || udt == "BIGSERIAL"
        {
            return TypeMapping {
                sql_type: SqlType::Integer,
                json_type: JsonType::Number,
            };
        }
        if udt == "FLOAT4" || udt == "FLOAT8" {
            return TypeMapping {
                sql_type: SqlType::Float,
                json_type: JsonType::Number,
            };
        }
        if udt == "NUMERIC" {
            return TypeMapping {
                sql_type: SqlType::Decimal,
                json_type: JsonType::Number,
            };
        }
        if udt == "UUID" {
            return TypeMapping {
                sql_type: SqlType::Uuid,
                json_type: JsonType::String,
            };
        }
        if udt == "JSON" || udt == "JSONB" {
            return TypeMapping {
                sql_type: SqlType::Json,
                json_type: JsonType::Object,
            };
        }
        if udt == "TIMESTAMP" || udt == "TIMESTAMPTZ" {
            return TypeMapping {
                sql_type: SqlType::DateTime,
                json_type: JsonType::String,
            };
        }
        if udt == "DATE" {
            return TypeMapping {
                sql_type: SqlType::Date,
                json_type: JsonType::String,
            };
        }
        if udt == "TIME" || udt == "TIMETZ" {
            return TypeMapping {
                sql_type: SqlType::Time,
                json_type: JsonType::String,
            };
        }
        if udt == "BYTEA" {
            return TypeMapping {
                sql_type: SqlType::Binary,
                json_type: JsonType::String,
            };
        }

        // Fall back to data_type for text types
        let dt = data_type.to_ascii_uppercase();
        if dt == "CHARACTER VARYING" || dt == "CHARACTER" || dt == "TEXT" {
            return TypeMapping {
                sql_type: SqlType::String,
                json_type: JsonType::String,
            };
        }

        TypeMapping {
            sql_type: SqlType::Unknown,
            json_type: JsonType::String,
        }
    }

    /// Map a SQLite declared type to unified types using SQLite type affinity rules.
    ///
    /// `declared_type` is the type string from PRAGMA table_info (e.g. "INTEGER", "TEXT").
    pub fn map_sqlite_type(declared_type: &str) -> TypeMapping {
        let upper = declared_type.to_ascii_uppercase();

        // SQLite type affinity rules (section 3.1 of SQLite docs)
        if upper.contains("INT") {
            return TypeMapping {
                sql_type: SqlType::Integer,
                json_type: JsonType::Number,
            };
        }
        if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
            return TypeMapping {
                sql_type: SqlType::String,
                json_type: JsonType::String,
            };
        }
        if upper.contains("BLOB") || upper.is_empty() {
            return TypeMapping {
                sql_type: SqlType::Binary,
                json_type: JsonType::String,
            };
        }
        if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            return TypeMapping {
                sql_type: SqlType::Float,
                json_type: JsonType::Number,
            };
        }
        if upper.contains("BOOL") {
            return TypeMapping {
                sql_type: SqlType::Boolean,
                json_type: JsonType::Boolean,
            };
        }
        if upper.contains("DATE") || upper.contains("TIME") {
            return TypeMapping {
                sql_type: SqlType::DateTime,
                json_type: JsonType::String,
            };
        }

        // Default: NUMERIC affinity
        TypeMapping {
            sql_type: SqlType::Decimal,
            json_type: JsonType::Number,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- MySQL type mapping tests ---

    #[test]
    fn mysql_tinyint1_is_boolean() {
        let m = TypeMapper::map_mysql_type("tinyint", "tinyint(1)");
        assert_eq!(m.sql_type, SqlType::Boolean);
        assert_eq!(m.json_type, JsonType::Boolean);
    }

    #[test]
    fn mysql_tinyint4_is_integer() {
        let m = TypeMapper::map_mysql_type("tinyint", "tinyint(4)");
        assert_eq!(m.sql_type, SqlType::Integer);
        assert_eq!(m.json_type, JsonType::Number);
    }

    #[test]
    fn mysql_int_types() {
        for dt in &["int", "bigint", "smallint", "mediumint"] {
            let m = TypeMapper::map_mysql_type(dt, dt);
            assert_eq!(m.sql_type, SqlType::Integer, "failed for {}", dt);
        }
    }

    #[test]
    fn mysql_float_types() {
        for dt in &["float", "double"] {
            let m = TypeMapper::map_mysql_type(dt, dt);
            assert_eq!(m.sql_type, SqlType::Float, "failed for {}", dt);
        }
    }

    #[test]
    fn mysql_decimal_types() {
        for dt in &["decimal", "numeric"] {
            let m = TypeMapper::map_mysql_type(dt, dt);
            assert_eq!(m.sql_type, SqlType::Decimal, "failed for {}", dt);
        }
    }

    #[test]
    fn mysql_string_types() {
        for dt in &[
            "varchar", "char", "text", "mediumtext", "longtext", "tinytext", "enum", "set",
        ] {
            let m = TypeMapper::map_mysql_type(dt, dt);
            assert_eq!(m.sql_type, SqlType::String, "failed for {}", dt);
        }
    }

    #[test]
    fn mysql_datetime_types() {
        let m = TypeMapper::map_mysql_type("datetime", "datetime");
        assert_eq!(m.sql_type, SqlType::DateTime);
        let m = TypeMapper::map_mysql_type("timestamp", "timestamp");
        assert_eq!(m.sql_type, SqlType::DateTime);
    }

    #[test]
    fn mysql_date_type() {
        let m = TypeMapper::map_mysql_type("date", "date");
        assert_eq!(m.sql_type, SqlType::Date);
    }

    #[test]
    fn mysql_time_type() {
        let m = TypeMapper::map_mysql_type("time", "time");
        assert_eq!(m.sql_type, SqlType::Time);
    }

    #[test]
    fn mysql_json_type() {
        let m = TypeMapper::map_mysql_type("json", "json");
        assert_eq!(m.sql_type, SqlType::Json);
        assert_eq!(m.json_type, JsonType::Object);
    }

    #[test]
    fn mysql_binary_types() {
        for dt in &[
            "blob",
            "mediumblob",
            "longblob",
            "tinyblob",
            "binary",
            "varbinary",
        ] {
            let m = TypeMapper::map_mysql_type(dt, dt);
            assert_eq!(m.sql_type, SqlType::Binary, "failed for {}", dt);
        }
    }

    #[test]
    fn mysql_unknown_type() {
        let m = TypeMapper::map_mysql_type("geometry", "geometry");
        assert_eq!(m.sql_type, SqlType::Unknown);
        assert_eq!(m.json_type, JsonType::String);
    }

    // --- Postgres type mapping tests ---

    #[test]
    fn pg_bool() {
        let m = TypeMapper::map_postgres_type("boolean", "bool");
        assert_eq!(m.sql_type, SqlType::Boolean);
        assert_eq!(m.json_type, JsonType::Boolean);
    }

    #[test]
    fn pg_int_types() {
        for udt in &["int2", "int4", "int8", "serial", "bigserial"] {
            let m = TypeMapper::map_postgres_type("integer", udt);
            assert_eq!(m.sql_type, SqlType::Integer, "failed for {}", udt);
        }
    }

    #[test]
    fn pg_float_types() {
        let m = TypeMapper::map_postgres_type("double precision", "float8");
        assert_eq!(m.sql_type, SqlType::Float);
        let m = TypeMapper::map_postgres_type("real", "float4");
        assert_eq!(m.sql_type, SqlType::Float);
    }

    #[test]
    fn pg_numeric() {
        let m = TypeMapper::map_postgres_type("numeric", "numeric");
        assert_eq!(m.sql_type, SqlType::Decimal);
    }

    #[test]
    fn pg_uuid() {
        let m = TypeMapper::map_postgres_type("uuid", "uuid");
        assert_eq!(m.sql_type, SqlType::Uuid);
        assert_eq!(m.json_type, JsonType::String);
    }

    #[test]
    fn pg_json_and_jsonb() {
        let m = TypeMapper::map_postgres_type("json", "json");
        assert_eq!(m.sql_type, SqlType::Json);
        assert_eq!(m.json_type, JsonType::Object);
        let m = TypeMapper::map_postgres_type("jsonb", "jsonb");
        assert_eq!(m.sql_type, SqlType::Json);
        assert_eq!(m.json_type, JsonType::Object);
    }

    #[test]
    fn pg_timestamp_types() {
        let m = TypeMapper::map_postgres_type("timestamp without time zone", "timestamp");
        assert_eq!(m.sql_type, SqlType::DateTime);
        let m = TypeMapper::map_postgres_type("timestamp with time zone", "timestamptz");
        assert_eq!(m.sql_type, SqlType::DateTime);
    }

    #[test]
    fn pg_date() {
        let m = TypeMapper::map_postgres_type("date", "date");
        assert_eq!(m.sql_type, SqlType::Date);
    }

    #[test]
    fn pg_time_types() {
        let m = TypeMapper::map_postgres_type("time without time zone", "time");
        assert_eq!(m.sql_type, SqlType::Time);
        let m = TypeMapper::map_postgres_type("time with time zone", "timetz");
        assert_eq!(m.sql_type, SqlType::Time);
    }

    #[test]
    fn pg_bytea() {
        let m = TypeMapper::map_postgres_type("bytea", "bytea");
        assert_eq!(m.sql_type, SqlType::Binary);
    }

    #[test]
    fn pg_text_fallback() {
        let m = TypeMapper::map_postgres_type("character varying", "varchar");
        assert_eq!(m.sql_type, SqlType::String);
        let m = TypeMapper::map_postgres_type("character", "bpchar");
        // "bpchar" is not matched by udt, falls to data_type "CHARACTER" -> String
        assert_eq!(m.sql_type, SqlType::String);
        let m = TypeMapper::map_postgres_type("text", "text");
        assert_eq!(m.sql_type, SqlType::String);
    }

    #[test]
    fn pg_unknown() {
        let m = TypeMapper::map_postgres_type("USER-DEFINED", "citext");
        assert_eq!(m.sql_type, SqlType::Unknown);
    }

    // --- SQLite type mapping tests ---

    #[test]
    fn sqlite_integer_affinity() {
        let m = TypeMapper::map_sqlite_type("INTEGER");
        assert_eq!(m.sql_type, SqlType::Integer);
        let m = TypeMapper::map_sqlite_type("BIGINT");
        assert_eq!(m.sql_type, SqlType::Integer);
        let m = TypeMapper::map_sqlite_type("TINYINT");
        assert_eq!(m.sql_type, SqlType::Integer);
    }

    #[test]
    fn sqlite_text_affinity() {
        let m = TypeMapper::map_sqlite_type("TEXT");
        assert_eq!(m.sql_type, SqlType::String);
        let m = TypeMapper::map_sqlite_type("VARCHAR(255)");
        assert_eq!(m.sql_type, SqlType::String);
        let m = TypeMapper::map_sqlite_type("CLOB");
        assert_eq!(m.sql_type, SqlType::String);
    }

    #[test]
    fn sqlite_blob_affinity() {
        let m = TypeMapper::map_sqlite_type("BLOB");
        assert_eq!(m.sql_type, SqlType::Binary);
        // Empty declared type -> BLOB affinity
        let m = TypeMapper::map_sqlite_type("");
        assert_eq!(m.sql_type, SqlType::Binary);
    }

    #[test]
    fn sqlite_real_affinity() {
        let m = TypeMapper::map_sqlite_type("REAL");
        assert_eq!(m.sql_type, SqlType::Float);
        let m = TypeMapper::map_sqlite_type("FLOAT");
        assert_eq!(m.sql_type, SqlType::Float);
        let m = TypeMapper::map_sqlite_type("DOUBLE");
        assert_eq!(m.sql_type, SqlType::Float);
    }

    #[test]
    fn sqlite_boolean() {
        let m = TypeMapper::map_sqlite_type("BOOLEAN");
        assert_eq!(m.sql_type, SqlType::Boolean);
        assert_eq!(m.json_type, JsonType::Boolean);
    }

    #[test]
    fn sqlite_datetime() {
        let m = TypeMapper::map_sqlite_type("DATETIME");
        assert_eq!(m.sql_type, SqlType::DateTime);
        let m = TypeMapper::map_sqlite_type("TIMESTAMP");
        assert_eq!(m.sql_type, SqlType::DateTime);
    }

    #[test]
    fn sqlite_numeric_default() {
        // Something that doesn't match any affinity rule -> NUMERIC -> Decimal
        let m = TypeMapper::map_sqlite_type("DECIMAL(10,2)");
        assert_eq!(m.sql_type, SqlType::Decimal);
        assert_eq!(m.json_type, JsonType::Number);
    }
}
