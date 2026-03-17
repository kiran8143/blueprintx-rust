//! Mass-assignment protection.
//!
//! Strips dangerous fields from user-supplied JSON before it reaches the
//! database layer.  Every table gets a default blocklist; per-table overrides
//! are supported via the global [`BLOCKED_FIELDS`] map.

use dashmap::DashMap;
use serde_json::Value;
use std::sync::OnceLock;

// ---------------------------------------------------------------------------
// Default blocked fields
// ---------------------------------------------------------------------------

/// Fields that are *always* stripped unless explicitly overridden per-table.
const DEFAULT_BLOCKED: &[&str] = &[
    "id",
    "created_at",
    "updated_at",
    "created_by",
    "modified_by",
    "deleted_at",
    "deleted_by",
    "status",
];

// ---------------------------------------------------------------------------
// Per-table blocklist registry (thread-safe singleton)
// ---------------------------------------------------------------------------

/// Global registry mapping `table_name -> Vec<blocked_field_name>`.
///
/// If a table has no explicit entry the [`DEFAULT_BLOCKED`] list is used.
fn blocked_fields() -> &'static DashMap<String, Vec<String>> {
    static MAP: OnceLock<DashMap<String, Vec<String>>> = OnceLock::new();
    MAP.get_or_init(DashMap::new)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Sanitise `input` by removing every blocked field for `table`.
///
/// Returns a new JSON value with offending keys stripped.  Non-object values
/// are returned unchanged.
pub fn sanitize(table: &str, input: &Value) -> Value {
    let obj = match input.as_object() {
        Some(o) => o,
        None => return input.clone(),
    };

    let map = blocked_fields();
    let blocked: Vec<String> = match map.get(table) {
        Some(entry) => entry.value().clone(),
        None => DEFAULT_BLOCKED.iter().map(|s| (*s).to_string()).collect(),
    };

    let mut out = serde_json::Map::with_capacity(obj.len());
    for (key, val) in obj {
        if !blocked.iter().any(|b| b == key) {
            out.insert(key.clone(), val.clone());
        }
    }

    Value::Object(out)
}

/// Add a single field to the blocklist for `table`.
///
/// If the table has no entry yet, the default blocklist is copied first, then
/// the new field is appended (if not already present).
pub fn add_blocked_field(table: &str, field: &str) {
    let map = blocked_fields();
    let mut entry = map
        .entry(table.to_string())
        .or_insert_with(|| DEFAULT_BLOCKED.iter().map(|s| (*s).to_string()).collect());

    let field_str = field.to_string();
    if !entry.contains(&field_str) {
        entry.push(field_str);
    }
}

/// Replace the blocklist for `table` entirely.
pub fn set_blocked_fields(table: &str, fields: Vec<String>) {
    blocked_fields().insert(table.to_string(), fields);
}

/// Return the current blocklist for `table`.
///
/// Falls back to the default list when no per-table override exists.
pub fn get_blocked_fields(table: &str) -> Vec<String> {
    let map = blocked_fields();
    match map.get(table) {
        Some(entry) => entry.value().clone(),
        None => DEFAULT_BLOCKED.iter().map(|s| (*s).to_string()).collect(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sanitize_strips_default_fields() {
        let input = json!({
            "id": 1,
            "name": "Alice",
            "created_at": "2025-01-01",
            "email": "a@b.com"
        });

        let cleaned = sanitize("users", &input);
        let obj = cleaned.as_object().unwrap();

        assert!(!obj.contains_key("id"));
        assert!(!obj.contains_key("created_at"));
        assert!(obj.contains_key("name"));
        assert!(obj.contains_key("email"));
    }

    #[test]
    fn sanitize_non_object_passthrough() {
        let input = json!("just a string");
        let result = sanitize("any", &input);
        assert_eq!(result, input);
    }

    #[test]
    fn custom_blocklist_per_table() {
        set_blocked_fields("orders", vec!["id".into(), "secret_field".into()]);

        let input = json!({
            "id": 99,
            "secret_field": "hidden",
            "amount": 42.5
        });

        let cleaned = sanitize("orders", &input);
        let obj = cleaned.as_object().unwrap();

        assert!(!obj.contains_key("id"));
        assert!(!obj.contains_key("secret_field"));
        assert!(obj.contains_key("amount"));

        // Clean up for other tests
        blocked_fields().remove("orders");
    }

    #[test]
    fn add_blocked_field_appends() {
        add_blocked_field("products", "internal_code");
        let fields = get_blocked_fields("products");
        assert!(fields.contains(&"internal_code".to_string()));
        assert!(fields.contains(&"id".to_string())); // default still present

        // Clean up
        blocked_fields().remove("products");
    }

    #[test]
    fn get_blocked_fields_returns_defaults() {
        let fields = get_blocked_fields("nonexistent_table");
        assert_eq!(fields.len(), DEFAULT_BLOCKED.len());
    }
}
