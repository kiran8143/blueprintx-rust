//! Audit trail injection.
//!
//! Automatically stamps `created_at`, `updated_at`, `created_by`, and
//! `modified_by` on JSON payloads before they reach the database, but only
//! when the target table actually contains those columns (checked via
//! [`TableMeta`]).

use chrono::Utc;
use serde_json::Value;

use super::TableMeta;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Inject audit fields for a **CREATE** operation.
///
/// Sets `created_at`, `updated_at`, `created_by`, and `modified_by` when
/// those columns exist in `meta`.  Timestamps use MySQL-compatible
/// `YYYY-MM-DD HH:MM:SS` format in UTC.
pub fn inject_create(data: &mut Value, user_id: &str, meta: &TableMeta) {
    let obj = match data.as_object_mut() {
        Some(o) => o,
        None => return,
    };

    let now = now_mysql();

    if meta.has_column("created_at") {
        obj.insert("created_at".into(), Value::String(now.clone()));
    }
    if meta.has_column("updated_at") {
        obj.insert("updated_at".into(), Value::String(now));
    }
    if meta.has_column("created_by") {
        obj.insert("created_by".into(), Value::String(user_id.to_string()));
    }
    if meta.has_column("modified_by") {
        obj.insert("modified_by".into(), Value::String(user_id.to_string()));
    }
}

/// Inject audit fields for an **UPDATE** operation.
///
/// Sets `updated_at` and `modified_by` only; creation fields are left
/// untouched.
pub fn inject_update(data: &mut Value, user_id: &str, meta: &TableMeta) {
    let obj = match data.as_object_mut() {
        Some(o) => o,
        None => return,
    };

    let now = now_mysql();

    if meta.has_column("updated_at") {
        obj.insert("updated_at".into(), Value::String(now));
    }
    if meta.has_column("modified_by") {
        obj.insert("modified_by".into(), Value::String(user_id.to_string()));
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Current UTC time formatted as `YYYY-MM-DD HH:MM:SS` (MySQL DATETIME).
#[inline]
fn now_mysql() -> String {
    Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn full_meta() -> TableMeta {
        TableMeta::new(
            "users",
            vec![
                "id".into(),
                "name".into(),
                "created_at".into(),
                "updated_at".into(),
                "created_by".into(),
                "modified_by".into(),
            ],
        )
    }

    fn partial_meta() -> TableMeta {
        TableMeta::new(
            "logs",
            vec!["id".into(), "message".into(), "created_at".into()],
        )
    }

    #[test]
    fn inject_create_sets_all_audit_fields() {
        let mut data = json!({"name": "Alice"});
        inject_create(&mut data, "user-42", &full_meta());

        let obj = data.as_object().unwrap();
        assert!(obj.contains_key("created_at"));
        assert!(obj.contains_key("updated_at"));
        assert_eq!(obj["created_by"], "user-42");
        assert_eq!(obj["modified_by"], "user-42");

        // Timestamp format: YYYY-MM-DD HH:MM:SS (19 chars)
        let ts = obj["created_at"].as_str().unwrap();
        assert_eq!(ts.len(), 19);
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[10..11], " ");
    }

    #[test]
    fn inject_create_respects_partial_meta() {
        let mut data = json!({"message": "hello"});
        inject_create(&mut data, "user-1", &partial_meta());

        let obj = data.as_object().unwrap();
        assert!(obj.contains_key("created_at"));
        assert!(!obj.contains_key("updated_at"));
        assert!(!obj.contains_key("created_by"));
        assert!(!obj.contains_key("modified_by"));
    }

    #[test]
    fn inject_update_only_sets_update_fields() {
        let mut data = json!({"name": "Bob"});
        inject_update(&mut data, "user-7", &full_meta());

        let obj = data.as_object().unwrap();
        assert!(obj.contains_key("updated_at"));
        assert_eq!(obj["modified_by"], "user-7");
        assert!(!obj.contains_key("created_at"));
        assert!(!obj.contains_key("created_by"));
    }

    #[test]
    fn inject_on_non_object_is_noop() {
        let mut data = json!(42);
        inject_create(&mut data, "user-1", &full_meta());
        assert_eq!(data, json!(42));
    }
}
