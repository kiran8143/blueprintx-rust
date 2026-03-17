// Author: Udaykiran Atta
// License: MIT

//! Request body validation against table metadata.
//!
//! Equivalent to the C++ `RequestValidator` in the Drogon blueprint.
//! Validates JSON bodies for create/update operations by checking:
//!
//! - Required fields (non-nullable without defaults, excluding auto-fields)
//! - Unknown fields (not in the table schema)
//! - Type compatibility (JSON type vs. column's expected JSON type)
//! - Max-length constraints (for string columns)
//! - Immutable fields (primary keys cannot be updated)

use crate::schema::types::{ColumnMeta, JsonType, TableMeta, ValidationError};

/// Auto-generated/framework-managed fields excluded from required-field checks
/// on create operations.
const AUTO_FIELDS: &[&str] = &[
    "id",
    "code",
    "created_at",
    "updated_at",
    "created_by",
    "modified_by",
    "deleted_at",
    "deleted_by",
];

// =========================================================================
// Public API
// =========================================================================

/// Validate a JSON body for a CREATE (INSERT) operation.
///
/// Checks:
/// 1. Required non-nullable columns without defaults must be present
///    (auto-fields like `id`, `created_at`, etc. are excluded).
/// 2. All provided fields must exist in the table schema.
/// 3. Non-null values must be type-compatible with the column.
/// 4. String values must not exceed `max_length` when set.
pub fn validate_create(
    body: &serde_json::Value,
    meta: &TableMeta,
) -> Vec<ValidationError> {
    validate_create_with_auto_fields(body, meta, AUTO_FIELDS)
}

/// Like `validate_create`, but with a custom list of auto-fields.
pub fn validate_create_with_auto_fields(
    body: &serde_json::Value,
    meta: &TableMeta,
    auto_fields: &[&str],
) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    let obj = match body.as_object() {
        Some(o) => o,
        None => {
            errors.push(ValidationError::new(
                "_body",
                "INVALID_TYPE",
                "Request body must be a JSON object",
            ));
            return errors;
        }
    };

    // 1. Check required columns
    for col in &meta.columns {
        // Skip auto-increment columns
        if col.is_auto_increment {
            continue;
        }
        // Skip framework auto-fields
        if auto_fields.contains(&col.name.as_str()) {
            continue;
        }

        let present = obj.contains_key(&col.name);
        let is_null = present && obj[&col.name].is_null();

        // Required: non-nullable, no default, and not present or null
        if !col.is_nullable && col.default_value.is_none() && (!present || is_null) {
            errors.push(ValidationError::new(
                &col.name,
                "REQUIRED",
                format!("Field '{}' is required", col.name),
            ));
            continue;
        }

        // Skip further checks if the field is absent or null
        if !present || is_null {
            continue;
        }

        // 2. Type compatibility
        if !is_type_compatible(&obj[&col.name], col) {
            errors.push(ValidationError::new(
                &col.name,
                "INVALID_TYPE",
                format!("Field '{}' expects type {}", col.name, col.raw_type),
            ));
        }

        // 3. Max-length check
        if exceeds_max_length(&obj[&col.name], col) {
            errors.push(ValidationError::new(
                &col.name,
                "TOO_LONG",
                format!(
                    "Field '{}' exceeds max length {}",
                    col.name,
                    col.max_length.unwrap_or(0)
                ),
            ));
        }
    }

    // 4. Check for unknown fields
    for key in obj.keys() {
        if !meta.has_column(key) {
            errors.push(ValidationError::new(
                key,
                "UNKNOWN_FIELD",
                format!(
                    "Field '{}' does not exist in table '{}'",
                    key, meta.name
                ),
            ));
        }
    }

    errors
}

/// Validate a JSON body for an UPDATE operation.
///
/// Checks:
/// 1. All provided fields must exist in the table schema.
/// 2. Primary key fields cannot be updated.
/// 3. Non-null values must be type-compatible.
/// 4. String values must not exceed `max_length`.
///
/// Unlike `validate_create`, no required-field checks are performed
/// (partial updates are allowed).
pub fn validate_update(
    body: &serde_json::Value,
    meta: &TableMeta,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    let obj = match body.as_object() {
        Some(o) => o,
        None => {
            errors.push(ValidationError::new(
                "_body",
                "INVALID_TYPE",
                "Request body must be a JSON object",
            ));
            return errors;
        }
    };

    for (key, val) in obj {
        // 1. Unknown field check
        if !meta.has_column(key) {
            errors.push(ValidationError::new(
                key,
                "UNKNOWN_FIELD",
                format!(
                    "Field '{}' does not exist in table '{}'",
                    key, meta.name
                ),
            ));
            continue;
        }

        let col = match meta.get_column(key) {
            Some(c) => c,
            None => continue,
        };

        // 2. Primary key immutability
        if col.is_primary_key {
            errors.push(ValidationError::new(
                &col.name,
                "IMMUTABLE",
                format!("Primary key field '{}' cannot be updated", col.name),
            ));
            continue;
        }

        // Skip further checks if the value is null (setting to NULL is
        // fine if the column is nullable -- the DB will enforce that).
        if val.is_null() {
            continue;
        }

        // 3. Type compatibility
        if !is_type_compatible(val, col) {
            errors.push(ValidationError::new(
                &col.name,
                "INVALID_TYPE",
                format!("Field '{}' expects type {}", col.name, col.raw_type),
            ));
        }

        // 4. Max-length check
        if exceeds_max_length(val, col) {
            errors.push(ValidationError::new(
                &col.name,
                "TOO_LONG",
                format!(
                    "Field '{}' exceeds max length {}",
                    col.name,
                    col.max_length.unwrap_or(0)
                ),
            ));
        }
    }

    errors
}

// =========================================================================
// Private helpers
// =========================================================================

/// Check if a JSON value is compatible with the column's expected JSON type.
fn is_type_compatible(value: &serde_json::Value, col: &ColumnMeta) -> bool {
    if value.is_null() && col.is_nullable {
        return true;
    }

    match col.json_type {
        JsonType::Number => value.is_number(),
        JsonType::String => value.is_string(),
        JsonType::Boolean => value.is_boolean(),
        JsonType::Object => value.is_object(),
        JsonType::Array => value.is_array(),
        JsonType::Null => true,
    }
}

/// Check if a string value exceeds the column's max_length.
fn exceeds_max_length(value: &serde_json::Value, col: &ColumnMeta) -> bool {
    match col.max_length {
        None => false,
        Some(max) => match value.as_str() {
            None => false,
            Some(s) => s.len() > max as usize,
        },
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{ColumnMeta, SqlType};

    fn make_meta() -> TableMeta {
        TableMeta::new(
            "users".to_string(),
            "public".to_string(),
            vec![
                ColumnMeta {
                    name: "id".into(),
                    raw_type: "int4".into(),
                    sql_type: SqlType::Integer,
                    json_type: JsonType::Number,
                    is_nullable: false,
                    is_primary_key: true,
                    is_auto_increment: true,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "name".into(),
                    raw_type: "varchar(100)".into(),
                    sql_type: SqlType::String,
                    json_type: JsonType::String,
                    is_nullable: false,
                    max_length: Some(100),
                    ..Default::default()
                },
                ColumnMeta {
                    name: "email".into(),
                    raw_type: "varchar(255)".into(),
                    sql_type: SqlType::String,
                    json_type: JsonType::String,
                    is_nullable: false,
                    max_length: Some(255),
                    ..Default::default()
                },
                ColumnMeta {
                    name: "age".into(),
                    raw_type: "int4".into(),
                    sql_type: SqlType::Integer,
                    json_type: JsonType::Number,
                    is_nullable: true,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "created_at".into(),
                    raw_type: "timestamp".into(),
                    sql_type: SqlType::DateTime,
                    json_type: JsonType::String,
                    is_nullable: true,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "status".into(),
                    raw_type: "varchar".into(),
                    sql_type: SqlType::String,
                    json_type: JsonType::String,
                    is_nullable: true,
                    default_value: Some("active".into()),
                    ..Default::default()
                },
            ],
            vec!["id".to_string()],
            Vec::new(),
        )
    }

    // ---- validate_create tests ----

    #[test]
    fn create_valid_body() {
        let meta = make_meta();
        let body = serde_json::json!({
            "name": "Alice",
            "email": "alice@example.com",
        });
        let errors = validate_create(&body, &meta);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn create_missing_required_field() {
        let meta = make_meta();
        let body = serde_json::json!({
            "name": "Alice",
            // email is missing
        });
        let errors = validate_create(&body, &meta);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "email");
        assert_eq!(errors[0].code, "REQUIRED");
    }

    #[test]
    fn create_null_required_field() {
        let meta = make_meta();
        let body = serde_json::json!({
            "name": "Alice",
            "email": null,
        });
        let errors = validate_create(&body, &meta);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "email");
        assert_eq!(errors[0].code, "REQUIRED");
    }

    #[test]
    fn create_unknown_field() {
        let meta = make_meta();
        let body = serde_json::json!({
            "name": "Alice",
            "email": "alice@example.com",
            "nonexistent": "value",
        });
        let errors = validate_create(&body, &meta);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "nonexistent");
        assert_eq!(errors[0].code, "UNKNOWN_FIELD");
    }

    #[test]
    fn create_wrong_type() {
        let meta = make_meta();
        let body = serde_json::json!({
            "name": 12345,  // should be string
            "email": "alice@example.com",
        });
        let errors = validate_create(&body, &meta);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "name");
        assert_eq!(errors[0].code, "INVALID_TYPE");
    }

    #[test]
    fn create_too_long() {
        let meta = make_meta();
        let long_name = "x".repeat(101);
        let body = serde_json::json!({
            "name": long_name,
            "email": "alice@example.com",
        });
        let errors = validate_create(&body, &meta);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "name");
        assert_eq!(errors[0].code, "TOO_LONG");
    }

    #[test]
    fn create_auto_fields_not_required() {
        let meta = make_meta();
        // id, created_at, status are all auto-fields or have defaults,
        // so not providing them should be fine.
        let body = serde_json::json!({
            "name": "Alice",
            "email": "alice@example.com",
        });
        let errors = validate_create(&body, &meta);
        assert!(errors.is_empty());
    }

    #[test]
    fn create_nullable_field_optional() {
        let meta = make_meta();
        // age is nullable, so not providing it is fine
        let body = serde_json::json!({
            "name": "Alice",
            "email": "alice@example.com",
        });
        let errors = validate_create(&body, &meta);
        assert!(errors.is_empty());
    }

    #[test]
    fn create_non_object_body() {
        let meta = make_meta();
        let body = serde_json::json!("not an object");
        let errors = validate_create(&body, &meta);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, "INVALID_TYPE");
    }

    // ---- validate_update tests ----

    #[test]
    fn update_valid_body() {
        let meta = make_meta();
        let body = serde_json::json!({
            "name": "Bob",
        });
        let errors = validate_update(&body, &meta);
        assert!(errors.is_empty());
    }

    #[test]
    fn update_primary_key_immutable() {
        let meta = make_meta();
        let body = serde_json::json!({
            "id": 999,
        });
        let errors = validate_update(&body, &meta);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "id");
        assert_eq!(errors[0].code, "IMMUTABLE");
    }

    #[test]
    fn update_unknown_field() {
        let meta = make_meta();
        let body = serde_json::json!({
            "nonexistent": "value",
        });
        let errors = validate_update(&body, &meta);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, "UNKNOWN_FIELD");
    }

    #[test]
    fn update_null_is_ok() {
        let meta = make_meta();
        let body = serde_json::json!({
            "age": null,
        });
        let errors = validate_update(&body, &meta);
        assert!(errors.is_empty());
    }

    #[test]
    fn update_wrong_type() {
        let meta = make_meta();
        let body = serde_json::json!({
            "age": "not a number",
        });
        let errors = validate_update(&body, &meta);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "age");
        assert_eq!(errors[0].code, "INVALID_TYPE");
    }

    #[test]
    fn update_non_object_body() {
        let meta = make_meta();
        let body = serde_json::json!(42);
        let errors = validate_update(&body, &meta);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].code, "INVALID_TYPE");
    }

    // ---- ValidationError::to_json_array ----

    #[test]
    fn test_errors_to_json() {
        let errors = vec![
            ValidationError::new("name", "REQUIRED", "Field 'name' is required"),
            ValidationError::new("age", "INVALID_TYPE", "Field 'age' expects type int4"),
        ];
        let json = ValidationError::to_json_array(&errors);
        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 2);
        assert_eq!(json[0]["field"], "name");
        assert_eq!(json[0]["code"], "REQUIRED");
        assert_eq!(json[1]["field"], "age");
    }
}
