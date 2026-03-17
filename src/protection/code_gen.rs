//! UUID code generation.
//!
//! Automatically injects a UUID v4 into the `code` column when the table has
//! one and the incoming payload does not already supply a value.

use serde_json::Value;
use uuid::Uuid;

use super::TableMeta;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generate a new UUID v4 string (lowercase hex with hyphens).
#[inline]
pub fn generate_uuid() -> String {
    Uuid::new_v4().to_string()
}

/// If the table has a `code` column and `data` does not already contain one,
/// generate a UUID v4 and inject it.
pub fn inject_code(data: &mut Value, meta: &TableMeta) {
    if !meta.has_column("code") {
        return;
    }

    let obj = match data.as_object_mut() {
        Some(o) => o,
        None => return,
    };

    // Only inject if the caller did not supply `code` (or it is null/empty).
    let should_inject = match obj.get("code") {
        None => true,
        Some(Value::Null) => true,
        Some(Value::String(s)) if s.is_empty() => true,
        _ => false,
    };

    if should_inject {
        obj.insert("code".into(), Value::String(generate_uuid()));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn meta_with_code() -> TableMeta {
        TableMeta::new("items", vec!["id".into(), "code".into(), "name".into()])
    }

    fn meta_without_code() -> TableMeta {
        TableMeta::new("logs", vec!["id".into(), "message".into()])
    }

    #[test]
    fn generate_uuid_format() {
        let id = generate_uuid();
        // UUID v4: 8-4-4-4-12 hex chars = 36 chars total
        assert_eq!(id.len(), 36);
        assert_eq!(&id[8..9], "-");
        assert_eq!(&id[13..14], "-");
        assert_eq!(&id[18..19], "-");
        assert_eq!(&id[23..24], "-");
    }

    #[test]
    fn inject_code_when_missing() {
        let mut data = json!({"name": "Widget"});
        inject_code(&mut data, &meta_with_code());

        let code = data["code"].as_str().unwrap();
        assert_eq!(code.len(), 36);
    }

    #[test]
    fn inject_code_when_null() {
        let mut data = json!({"name": "Gadget", "code": null});
        inject_code(&mut data, &meta_with_code());

        let code = data["code"].as_str().unwrap();
        assert_eq!(code.len(), 36);
    }

    #[test]
    fn inject_code_when_empty_string() {
        let mut data = json!({"name": "Thing", "code": ""});
        inject_code(&mut data, &meta_with_code());

        let code = data["code"].as_str().unwrap();
        assert_eq!(code.len(), 36);
    }

    #[test]
    fn inject_code_preserves_existing() {
        let mut data = json!({"name": "Custom", "code": "MY-CODE-123"});
        inject_code(&mut data, &meta_with_code());

        assert_eq!(data["code"], "MY-CODE-123");
    }

    #[test]
    fn inject_code_noop_without_column() {
        let mut data = json!({"message": "hi"});
        inject_code(&mut data, &meta_without_code());

        assert!(!data.as_object().unwrap().contains_key("code"));
    }

    #[test]
    fn inject_code_noop_on_non_object() {
        let mut data = json!([1, 2, 3]);
        inject_code(&mut data, &meta_with_code());
        assert_eq!(data, json!([1, 2, 3]));
    }
}
