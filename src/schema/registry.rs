// Author: Udaykiran Atta
// License: MIT

use dashmap::DashMap;
use std::sync::OnceLock;

use crate::schema::types::TableMeta;

/// Thread-safe, singleton-style model registry that holds introspected table
/// metadata for the entire application lifetime.
///
/// Uses `DashMap` for lock-free concurrent reads with exclusive writes.
/// Wrapped in `OnceLock` for lazy, thread-safe initialization.
pub struct ModelRegistry {
    tables: DashMap<String, TableMeta>,
}

impl ModelRegistry {
    /// Access the global ModelRegistry singleton.
    pub fn instance() -> &'static ModelRegistry {
        static INSTANCE: OnceLock<ModelRegistry> = OnceLock::new();
        INSTANCE.get_or_init(|| ModelRegistry {
            tables: DashMap::new(),
        })
    }

    /// Register (or replace) a table in the registry.
    pub fn register_table(&self, meta: TableMeta) {
        let name = meta.name.clone();
        self.tables.insert(name, meta);
    }

    /// Look up a table by name. Returns `None` if not registered.
    pub fn get_table(
        &self,
        name: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, String, TableMeta>> {
        self.tables.get(name)
    }

    /// Number of registered tables.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Total number of columns across all registered tables.
    pub fn total_column_count(&self) -> usize {
        self.tables.iter().map(|e| e.columns.len()).sum()
    }

    /// Get all table names, sorted alphabetically.
    pub fn get_table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.iter().map(|e| e.key().clone()).collect();
        names.sort();
        names
    }

    /// Alias for [`get_table_names`] -- used in startup logging.
    pub fn table_names(&self) -> Vec<String> {
        self.get_table_names()
    }

    /// Remove all registered tables.
    pub fn clear(&self) {
        self.tables.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{ColumnMeta, JsonType, SqlType};

    fn make_test_table(name: &str, col_count: usize) -> TableMeta {
        let columns: Vec<ColumnMeta> = (0..col_count)
            .map(|i| ColumnMeta {
                name: format!("col_{}", i),
                raw_type: "TEXT".into(),
                sql_type: SqlType::String,
                json_type: JsonType::String,
                ordinal_position: i as i32,
                ..Default::default()
            })
            .collect();

        TableMeta::new(
            name.into(),
            "test".into(),
            columns,
            vec!["col_0".into()],
            vec![],
        )
    }

    #[test]
    fn register_and_retrieve() {
        let reg = ModelRegistry {
            tables: DashMap::new(),
        };

        reg.register_table(make_test_table("users", 3));
        reg.register_table(make_test_table("orders", 5));

        assert_eq!(reg.table_count(), 2);
        assert_eq!(reg.total_column_count(), 8);

        let users = reg.get_table("users").expect("users should exist");
        assert_eq!(users.columns.len(), 3);
        assert_eq!(users.primary_keys, vec!["col_0"]);

        assert!(reg.get_table("nonexistent").is_none());
    }

    #[test]
    fn get_table_names_sorted() {
        let reg = ModelRegistry {
            tables: DashMap::new(),
        };

        reg.register_table(make_test_table("zebra", 1));
        reg.register_table(make_test_table("alpha", 1));
        reg.register_table(make_test_table("middle", 1));

        let names = reg.get_table_names();
        assert_eq!(names, vec!["alpha", "middle", "zebra"]);
    }

    #[test]
    fn clear_removes_all() {
        let reg = ModelRegistry {
            tables: DashMap::new(),
        };

        reg.register_table(make_test_table("t1", 2));
        reg.register_table(make_test_table("t2", 3));
        assert_eq!(reg.table_count(), 2);

        reg.clear();
        assert_eq!(reg.table_count(), 0);
        assert_eq!(reg.total_column_count(), 0);
    }

    #[test]
    fn register_replaces_existing() {
        let reg = ModelRegistry {
            tables: DashMap::new(),
        };

        reg.register_table(make_test_table("users", 3));
        assert_eq!(reg.total_column_count(), 3);

        // Replace with different column count
        reg.register_table(make_test_table("users", 7));
        assert_eq!(reg.table_count(), 1);
        assert_eq!(reg.total_column_count(), 7);
    }
}
