// Author: Udaykiran Atta
// License: MIT

//! Fluent query builder with multi-dialect SQL generation.
//!
//! Equivalent to the C++ `QueryBuilder` in the Drogon blueprint.  Generates
//! parameterised SQL for MySQL, PostgreSQL, and SQLite, validating column
//! names against [`TableMeta`] from the schema registry.
//!
//! **All values are bound via placeholders** -- no string interpolation of
//! user-supplied data ever reaches the SQL string.

use crate::db::connection::{query_dynamic, DbError};
use crate::schema::types::{DatabaseConnection, DbDialect, DynamicRow, SqlValue, TableMeta};

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// A single WHERE clause condition.
#[derive(Debug, Clone)]
pub struct WhereCondition {
    pub column: String,
    /// Comparison operator: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `LIKE`,
    /// `NOT LIKE`, `IN`, `IS NULL`, `IS NOT NULL`.
    pub op: String,
    /// Parameter values.  Empty for IS NULL / IS NOT NULL.
    pub values: Vec<String>,
    /// True when the condition is IS NULL or IS NOT NULL (no bind params).
    pub is_null_check: bool,
}

/// An ORDER BY clause entry.
#[derive(Debug, Clone)]
pub struct OrderByClause {
    pub column: String,
    /// `ASC` or `DESC`.
    pub direction: String,
}

/// Allowed comparison operators (validated at build time).
const VALID_OPS: &[&str] = &[
    "=", "!=", "<>", "<", ">", "<=", ">=", "LIKE", "NOT LIKE",
];

// ---------------------------------------------------------------------------
// QueryBuilder
// ---------------------------------------------------------------------------

/// Fluent, dialect-aware query builder.
///
/// Uses `&DatabaseConnection` from `schema::types` and delegates execution
/// to `db::connection::query_dynamic`, which dispatches to the correct SQLx
/// pool based on the dialect.
pub struct QueryBuilder<'a> {
    conn: &'a DatabaseConnection,
    table_name: String,
    table_meta: Option<&'a TableMeta>,
    select_columns: Vec<String>,
    where_conditions: Vec<WhereCondition>,
    order_by_clauses: Vec<OrderByClause>,
    limit_val: Option<usize>,
    offset_val: Option<usize>,
}

impl<'a> QueryBuilder<'a> {
    /// Create a new query builder bound to the given database connection.
    pub fn new(conn: &'a DatabaseConnection) -> Self {
        Self {
            conn,
            table_name: String::new(),
            table_meta: None,
            select_columns: Vec::new(),
            where_conditions: Vec::new(),
            order_by_clauses: Vec::new(),
            limit_val: None,
            offset_val: None,
        }
    }

    // ------------------------------------------------------------------
    // Table
    // ------------------------------------------------------------------

    /// Set the target table and associated metadata.
    ///
    /// # Errors
    /// Returns an error if the table name is empty.
    pub fn table(mut self, name: &str, meta: &'a TableMeta) -> Result<Self, String> {
        if name.is_empty() {
            return Err("table name cannot be empty".into());
        }
        self.table_name = name.to_string();
        self.table_meta = Some(meta);
        Ok(self)
    }

    // ------------------------------------------------------------------
    // SELECT columns
    // ------------------------------------------------------------------

    /// Specify which columns to select (default is `*`).
    ///
    /// # Errors
    /// Returns an error if any column does not exist in the table metadata.
    pub fn select(mut self, columns: &[&str]) -> Result<Self, String> {
        for col in columns {
            self.validate_column(col)?;
        }
        self.select_columns = columns.iter().map(|c| c.to_string()).collect();
        Ok(self)
    }

    // ------------------------------------------------------------------
    // WHERE conditions
    // ------------------------------------------------------------------

    /// Add `WHERE column = value`.
    pub fn where_eq(self, column: &str, value: impl Into<String>) -> Self {
        self.where_op_unchecked(column, "=", value.into())
    }

    /// Add `WHERE column <op> value` with operator validation.
    ///
    /// # Errors
    /// Returns an error if the operator is not in the allowed list, or if
    /// the column does not exist.
    pub fn where_op(
        mut self,
        column: &str,
        op: &str,
        value: impl Into<String>,
    ) -> Result<Self, String> {
        self.validate_column(column)?;
        let upper = op.to_ascii_uppercase();
        if !VALID_OPS.contains(&upper.as_str()) {
            return Err(format!("invalid operator: {op}"));
        }
        self.where_conditions.push(WhereCondition {
            column: column.to_string(),
            op: upper,
            values: vec![value.into()],
            is_null_check: false,
        });
        Ok(self)
    }

    /// Add `WHERE column IS NULL`.
    pub fn where_null(mut self, column: &str) -> Self {
        self.where_conditions.push(WhereCondition {
            column: column.to_string(),
            op: "IS NULL".to_string(),
            values: Vec::new(),
            is_null_check: true,
        });
        self
    }

    /// Add `WHERE column IS NOT NULL`.
    pub fn where_not_null(mut self, column: &str) -> Self {
        self.where_conditions.push(WhereCondition {
            column: column.to_string(),
            op: "IS NOT NULL".to_string(),
            values: Vec::new(),
            is_null_check: true,
        });
        self
    }

    /// Add `WHERE column IN (v1, v2, ...)`.
    ///
    /// # Errors
    /// Returns an error if `values` is empty or the column does not exist.
    pub fn where_in(
        mut self,
        column: &str,
        values: &[impl AsRef<str>],
    ) -> Result<Self, String> {
        self.validate_column(column)?;
        if values.is_empty() {
            return Err("where_in requires at least one value".into());
        }
        self.where_conditions.push(WhereCondition {
            column: column.to_string(),
            op: "IN".to_string(),
            values: values.iter().map(|v| v.as_ref().to_string()).collect(),
            is_null_check: false,
        });
        Ok(self)
    }

    // ------------------------------------------------------------------
    // ORDER BY
    // ------------------------------------------------------------------

    /// Add an ORDER BY clause.
    ///
    /// # Errors
    /// Returns an error if the column does not exist or the direction is
    /// invalid.
    pub fn order_by(mut self, column: &str, direction: &str) -> Result<Self, String> {
        self.validate_column(column)?;
        let dir = direction.to_ascii_uppercase();
        if dir != "ASC" && dir != "DESC" {
            return Err(format!(
                "order direction must be ASC or DESC, got: {direction}"
            ));
        }
        self.order_by_clauses.push(OrderByClause {
            column: column.to_string(),
            direction: dir,
        });
        Ok(self)
    }

    // ------------------------------------------------------------------
    // LIMIT / OFFSET
    // ------------------------------------------------------------------

    /// Set the LIMIT clause.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit_val = Some(n);
        self
    }

    /// Set the OFFSET clause.
    pub fn offset(mut self, n: usize) -> Self {
        self.offset_val = Some(n);
        self
    }

    // ------------------------------------------------------------------
    // Reset
    // ------------------------------------------------------------------

    /// Reset the builder for reuse, keeping the connection.
    pub fn reset(mut self) -> Self {
        self.table_name.clear();
        self.table_meta = None;
        self.select_columns.clear();
        self.where_conditions.clear();
        self.order_by_clauses.clear();
        self.limit_val = None;
        self.offset_val = None;
        self
    }

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    /// Return the current table metadata (set after `table()`).
    pub fn get_table_meta(&self) -> Option<&TableMeta> {
        self.table_meta
    }

    /// Return the current table name.
    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    // ==================================================================
    // Execution methods (async)
    // ==================================================================

    /// Execute a SELECT query, returning `Vec<DynamicRow>`.
    pub async fn execute_select(&self) -> Result<Vec<DynamicRow>, DbError> {
        self.require_table("SELECT")?;

        let mut params: Vec<String> = Vec::new();
        let mut idx: usize = 1;

        // SELECT clause
        let select_part = if self.select_columns.is_empty() {
            "*".to_string()
        } else {
            self.select_columns
                .iter()
                .map(|c| self.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut sql = format!(
            "SELECT {} FROM {}",
            select_part,
            self.quote_identifier(&self.table_name)
        );

        self.build_where_clause(&mut sql, &mut params, &mut idx);
        self.build_order_by_clause(&mut sql);
        self.build_limit_offset_clause(&mut sql);

        self.exec_dynamic(&sql, &params).await
    }

    /// Execute an INSERT, returning the inserted row(s).
    ///
    /// For PostgreSQL/SQLite this uses `RETURNING *`.
    /// For MySQL this uses `LAST_INSERT_ID()` to fetch the inserted row.
    pub async fn execute_insert(
        &self,
        data: &serde_json::Value,
    ) -> Result<Vec<DynamicRow>, DbError> {
        self.require_table("INSERT")?;

        let obj = data
            .as_object()
            .ok_or_else(|| DbError::ColumnNotFound("INSERT data must be a non-empty JSON object".into()))?;
        if obj.is_empty() {
            return Err(DbError::ColumnNotFound(
                "INSERT data must be a non-empty JSON object".into(),
            ));
        }

        let mut columns = Vec::new();
        let mut placeholders = Vec::new();
        let mut params: Vec<String> = Vec::new();
        let mut idx: usize = 1;

        for (key, val) in obj {
            self.validate_column(key).map_err(DbError::ColumnNotFound)?;
            columns.push(self.quote_identifier(key));
            placeholders.push(self.make_placeholder(&mut idx));
            params.push(json_value_to_string(val));
        }

        let mut sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.quote_identifier(&self.table_name),
            columns.join(", "),
            placeholders.join(", "),
        );

        if !self.is_mysql() {
            sql.push_str(" RETURNING *");
        }

        let result = self.exec_dynamic(&sql, &params).await?;

        // MySQL: RETURNING not supported -- query the inserted row via
        // LAST_INSERT_ID().
        if self.is_mysql() {
            let meta = self.table_meta.unwrap();
            if !meta.primary_keys.is_empty() {
                let id_rows = self
                    .exec_dynamic("SELECT LAST_INSERT_ID() AS id", &[] as &[String])
                    .await?;
                if let Some(id_row) = id_rows.first() {
                    if let Some(id_val) = id_row.get("id") {
                        let id_str = sql_value_to_string(id_val);
                        let pk = &meta.primary_keys[0];
                        let mut select_idx: usize = 1;
                        let ph = self.make_placeholder(&mut select_idx);
                        let select_sql = format!(
                            "SELECT * FROM {} WHERE {} = {}",
                            self.quote_identifier(&self.table_name),
                            self.quote_identifier(pk),
                            ph,
                        );
                        return self.exec_dynamic(&select_sql, &[id_str]).await;
                    }
                }
            }
            return Ok(result);
        }

        Ok(result)
    }

    /// Execute an UPDATE with the current WHERE conditions, returning the
    /// updated row(s).
    ///
    /// For PostgreSQL/SQLite this uses `RETURNING *`.
    /// For MySQL this fetches the updated row via a follow-up SELECT.
    pub async fn execute_update(
        &self,
        data: &serde_json::Value,
    ) -> Result<Vec<DynamicRow>, DbError> {
        self.require_table("UPDATE")?;

        let obj = data
            .as_object()
            .ok_or_else(|| DbError::ColumnNotFound("UPDATE data must be a non-empty JSON object".into()))?;
        if obj.is_empty() {
            return Err(DbError::ColumnNotFound(
                "UPDATE data must be a non-empty JSON object".into(),
            ));
        }

        let mut set_clauses = Vec::new();
        let mut params: Vec<String> = Vec::new();
        let mut idx: usize = 1;

        for (key, val) in obj {
            self.validate_column(key).map_err(DbError::ColumnNotFound)?;
            if val.is_null() {
                // SET col = NULL -- no bind parameter needed
                set_clauses.push(format!("{} = NULL", self.quote_identifier(key)));
            } else {
                set_clauses.push(format!(
                    "{} = {}",
                    self.quote_identifier(key),
                    self.make_placeholder(&mut idx),
                ));
                params.push(json_value_to_string(val));
            }
        }

        let mut sql = format!(
            "UPDATE {} SET {}",
            self.quote_identifier(&self.table_name),
            set_clauses.join(", "),
        );

        // WHERE clause -- placeholder index continues from SET params.
        self.build_where_clause(&mut sql, &mut params, &mut idx);

        if !self.is_mysql() {
            sql.push_str(" RETURNING *");
        }

        let result = self.exec_dynamic(&sql, &params).await?;

        // MySQL: RETURNING not supported -- query the updated row.
        if self.is_mysql() && !self.where_conditions.is_empty() {
            let meta = self.table_meta.unwrap();
            if !meta.primary_keys.is_empty() {
                let pk = &meta.primary_keys[0];
                for cond in &self.where_conditions {
                    if cond.column == *pk && !cond.values.is_empty() {
                        let mut select_idx: usize = 1;
                        let ph = self.make_placeholder(&mut select_idx);
                        let select_sql = format!(
                            "SELECT * FROM {} WHERE {} = {}",
                            self.quote_identifier(&self.table_name),
                            self.quote_identifier(pk),
                            ph,
                        );
                        return self
                            .exec_dynamic(&select_sql, &[cond.values[0].clone()])
                            .await;
                    }
                }
            }
            return Ok(result);
        }

        Ok(result)
    }

    /// Execute a DELETE with the current WHERE conditions.
    ///
    /// For PostgreSQL/SQLite this uses `RETURNING *`.
    /// For MySQL this returns an empty vec (rows are gone).
    pub async fn execute_delete(&self) -> Result<Vec<DynamicRow>, DbError> {
        self.require_table("DELETE")?;

        let mut params: Vec<String> = Vec::new();
        let mut idx: usize = 1;

        let mut sql = format!(
            "DELETE FROM {}",
            self.quote_identifier(&self.table_name),
        );

        self.build_where_clause(&mut sql, &mut params, &mut idx);

        if !self.is_mysql() {
            sql.push_str(" RETURNING *");
        }

        self.exec_dynamic(&sql, &params).await
    }

    /// Execute a COUNT query with the current WHERE conditions.
    pub async fn execute_count(&self) -> Result<usize, DbError> {
        self.require_table("COUNT")?;

        let mut params: Vec<String> = Vec::new();
        let mut idx: usize = 1;

        let mut sql = format!(
            "SELECT COUNT(*) AS count FROM {}",
            self.quote_identifier(&self.table_name),
        );

        self.build_where_clause(&mut sql, &mut params, &mut idx);

        let rows = self.exec_dynamic(&sql, &params).await?;
        if let Some(row) = rows.first() {
            if let Some(val) = row.get("count") {
                let s = sql_value_to_string(val);
                return s
                    .parse::<usize>()
                    .map_err(|e| DbError::ColumnNotFound(e.to_string()));
            }
        }
        Ok(0)
    }

    // ==================================================================
    // Private helpers
    // ==================================================================

    fn require_table(&self, operation: &str) -> Result<(), DbError> {
        if self.table_name.is_empty() || self.table_meta.is_none() {
            return Err(DbError::ColumnNotFound(format!(
                "no table specified for {operation}"
            )));
        }
        Ok(())
    }

    fn validate_column(&self, column: &str) -> Result<(), String> {
        if let Some(meta) = self.table_meta {
            if !meta.has_column(column) {
                return Err(format!(
                    "column '{}' does not exist in table '{}'",
                    column, self.table_name
                ));
            }
        }
        Ok(())
    }

    fn is_mysql(&self) -> bool {
        self.conn.dialect == DbDialect::MySQL
    }

    /// Quote an identifier: backticks for MySQL, double-quotes for
    /// Postgres/SQLite.
    fn quote_identifier(&self, name: &str) -> String {
        if self.is_mysql() {
            format!("`{}`", name.replace('`', "``"))
        } else {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
    }

    /// Generate a placeholder and advance the index.
    fn make_placeholder(&self, idx: &mut usize) -> String {
        if self.is_mysql() {
            *idx += 1;
            "?".to_string()
        } else {
            let ph = format!("${}", *idx);
            *idx += 1;
            ph
        }
    }

    /// Build the WHERE clause and append it to `sql`, pushing params.
    fn build_where_clause(
        &self,
        sql: &mut String,
        params: &mut Vec<String>,
        idx: &mut usize,
    ) {
        if self.where_conditions.is_empty() {
            return;
        }

        sql.push_str(" WHERE ");

        for (i, cond) in self.where_conditions.iter().enumerate() {
            if i > 0 {
                sql.push_str(" AND ");
            }

            if cond.is_null_check {
                // IS NULL / IS NOT NULL -- no bind parameters
                sql.push_str(&self.quote_identifier(&cond.column));
                sql.push(' ');
                sql.push_str(&cond.op);
            } else if cond.op == "IN" {
                sql.push_str(&self.quote_identifier(&cond.column));
                sql.push_str(" IN (");
                for (j, val) in cond.values.iter().enumerate() {
                    if j > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&self.make_placeholder(idx));
                    params.push(val.clone());
                }
                sql.push(')');
            } else {
                sql.push_str(&self.quote_identifier(&cond.column));
                sql.push(' ');
                sql.push_str(&cond.op);
                sql.push(' ');
                sql.push_str(&self.make_placeholder(idx));
                params.push(cond.values[0].clone());
            }
        }
    }

    /// Build the ORDER BY clause.
    fn build_order_by_clause(&self, sql: &mut String) {
        if self.order_by_clauses.is_empty() {
            return;
        }
        sql.push_str(" ORDER BY ");
        for (i, clause) in self.order_by_clauses.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&self.quote_identifier(&clause.column));
            sql.push(' ');
            sql.push_str(&clause.direction);
        }
    }

    /// Build LIMIT / OFFSET clause.
    fn build_limit_offset_clause(&self, sql: &mut String) {
        if let Some(limit) = self.limit_val {
            sql.push_str(" LIMIT ");
            sql.push_str(&limit.to_string());
        }
        if let Some(offset) = self.offset_val {
            sql.push_str(" OFFSET ");
            sql.push_str(&offset.to_string());
        }
    }

    /// Execute dynamic SQL via the connection abstraction.
    async fn exec_dynamic(
        &self,
        sql: &str,
        params: &[impl AsRef<str>],
    ) -> Result<Vec<DynamicRow>, DbError> {
        log::debug!("QueryBuilder SQL: {}", sql);
        let param_refs: Vec<&str> = params.iter().map(|p| p.as_ref()).collect();
        query_dynamic(self.conn, sql, &param_refs).await
    }

    /// Internal: add a WHERE condition without validation (used by the
    /// ergonomic `where_eq` which skips validation for common use).
    fn where_op_unchecked(mut self, column: &str, op: &str, value: String) -> Self {
        self.where_conditions.push(WhereCondition {
            column: column.to_string(),
            op: op.to_string(),
            values: vec![value],
            is_null_check: false,
        });
        self
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

/// Convert a `serde_json::Value` to a string suitable for SQL binding.
fn json_value_to_string(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        // For objects/arrays, serialise to compact JSON string
        other => other.to_string(),
    }
}

/// Convert a `SqlValue` to a string representation.
fn sql_value_to_string(val: &SqlValue) -> String {
    match val {
        SqlValue::Null => String::new(),
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        SqlValue::String(s) => s.clone(),
        SqlValue::Bytes(b) => String::from_utf8_lossy(b).to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::{ColumnMeta, JsonType, SqlType};

    /// Build a simple test TableMeta.
    fn make_test_meta() -> TableMeta {
        TableMeta::new(
            "users".to_string(),
            "public".to_string(),
            vec![
                ColumnMeta {
                    name: "id".to_string(),
                    raw_type: "int4".to_string(),
                    sql_type: SqlType::Integer,
                    json_type: JsonType::Number,
                    is_nullable: false,
                    is_primary_key: true,
                    is_auto_increment: true,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "name".to_string(),
                    raw_type: "varchar".to_string(),
                    sql_type: SqlType::String,
                    json_type: JsonType::String,
                    is_nullable: false,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "email".to_string(),
                    raw_type: "varchar".to_string(),
                    sql_type: SqlType::String,
                    json_type: JsonType::String,
                    is_nullable: false,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "status".to_string(),
                    raw_type: "varchar".to_string(),
                    sql_type: SqlType::String,
                    json_type: JsonType::String,
                    is_nullable: true,
                    ..Default::default()
                },
                ColumnMeta {
                    name: "age".to_string(),
                    raw_type: "int4".to_string(),
                    sql_type: SqlType::Integer,
                    json_type: JsonType::Number,
                    is_nullable: true,
                    ..Default::default()
                },
            ],
            vec!["id".to_string()],
            Vec::new(),
        )
    }

    #[test]
    fn test_validate_column_rejects_unknown() {
        let meta = make_test_meta();
        assert!(meta.has_column("id"));
        assert!(meta.has_column("name"));
        assert!(!meta.has_column("nonexistent"));
    }

    #[test]
    fn test_valid_operators() {
        for op in VALID_OPS {
            assert!(VALID_OPS.contains(op), "operator {} should be valid", op);
        }
        assert!(!VALID_OPS.contains(&"DROP TABLE"));
    }

    #[test]
    fn test_json_value_to_string() {
        assert_eq!(json_value_to_string(&serde_json::json!(null)), "");
        assert_eq!(json_value_to_string(&serde_json::json!(true)), "true");
        assert_eq!(json_value_to_string(&serde_json::json!(false)), "false");
        assert_eq!(json_value_to_string(&serde_json::json!(42)), "42");
        assert_eq!(json_value_to_string(&serde_json::json!(3.14)), "3.14");
        assert_eq!(json_value_to_string(&serde_json::json!("hello")), "hello");
        assert_eq!(
            json_value_to_string(&serde_json::json!({"a": 1})),
            "{\"a\":1}"
        );
    }

    #[test]
    fn test_where_condition_null_check() {
        let cond = WhereCondition {
            column: "deleted_at".to_string(),
            op: "IS NULL".to_string(),
            values: Vec::new(),
            is_null_check: true,
        };
        assert!(cond.is_null_check);
        assert!(cond.values.is_empty());
    }

    #[test]
    fn test_where_in_condition() {
        let cond = WhereCondition {
            column: "status".to_string(),
            op: "IN".to_string(),
            values: vec!["active".to_string(), "pending".to_string()],
            is_null_check: false,
        };
        assert_eq!(cond.values.len(), 2);
    }

    #[test]
    fn test_sql_value_to_string() {
        assert_eq!(sql_value_to_string(&SqlValue::Null), "");
        assert_eq!(sql_value_to_string(&SqlValue::Integer(42)), "42");
        assert_eq!(sql_value_to_string(&SqlValue::Float(3.14)), "3.14");
        assert_eq!(sql_value_to_string(&SqlValue::Bool(true)), "true");
        assert_eq!(
            sql_value_to_string(&SqlValue::String("hello".into())),
            "hello"
        );
    }
}
