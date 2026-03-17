// Author: Udaykiran Atta
// License: MIT

//! SQL query builder with multi-dialect support.
//!
//! Generates parameterised SQL for MySQL, PostgreSQL, and SQLite, validating
//! column names against table metadata from the schema registry.

pub mod builder;

pub use builder::{OrderByClause, QueryBuilder, WhereCondition};
