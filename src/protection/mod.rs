//! Protection layer: mass-assignment guards, audit trail injection, and code generation.

pub mod audit;
pub mod code_gen;
pub mod field_guard;

/// Lightweight metadata about a database table's columns.
///
/// Used by [`audit`] and [`code_gen`] to decide which fields to inject.
/// Construct once per table (e.g. from an `INFORMATION_SCHEMA` query) and
/// share via `Arc`.
#[derive(Debug, Clone)]
pub struct TableMeta {
    /// The table name (e.g. `"users"`).
    pub table_name: String,
    /// Column names present in the table, in definition order.
    pub columns: Vec<String>,
}

impl TableMeta {
    /// Create a new `TableMeta`.
    pub fn new(table_name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns,
        }
    }

    /// Returns `true` if the table has a column with the given name.
    #[inline]
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c == name)
    }
}
