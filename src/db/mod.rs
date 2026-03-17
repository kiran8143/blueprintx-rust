// Author: Udaykiran Atta
// License: MIT

//! Database connection pool management.
//!
//! Wraps SQLx pool creation behind an engine-aware abstraction so the rest of
//! the application works uniformly with MySQL, PostgreSQL, or SQLite.

pub mod connection;

// Re-export commonly used types from the canonical locations.
pub use connection::{connect, query_dynamic, query_optional_dynamic, execute_sql, query_scalar_i64, DbError};
pub use crate::schema::types::{
    DatabaseConnection, DatabasePool, DbDialect, DynamicRow, SqlValue,
};

use sqlx::AnyPool;
use sqlx::any::AnyPoolOptions;
use std::sync::OnceLock;
use std::time::Duration;

use crate::config::Config;

/// Global database pool singleton (for the AnyPool-based path used by
/// schema introspection with raw sqlx queries).
static POOL: OnceLock<AnyPool> = OnceLock::new();

/// Global typed database connection (for QueryBuilder + controller path).
static CONNECTION: OnceLock<DatabaseConnection> = OnceLock::new();

/// Initialise the global database connection pool.
///
/// Creates both the `AnyPool` (for raw sqlx queries in schema introspection)
/// and the typed `DatabaseConnection` (for QueryBuilder/controllers).
///
/// Reads connection URL, pool size, and timeout from [`Config`].
/// Must be called once from `main()` after [`Config::init`].
///
/// # Errors
/// Returns `sqlx::Error` if the pool cannot be created.
pub async fn init_pool() -> Result<(), sqlx::Error> {
    let cfg = Config::global();

    // Install the SQLx any-database drivers at runtime.
    sqlx::any::install_default_drivers();

    let url = cfg.db_url();

    // Create AnyPool for raw sqlx usage (schema introspection).
    let any_pool = AnyPoolOptions::new()
        .max_connections(cfg.db_pool_size)
        .acquire_timeout(Duration::from_secs_f64(cfg.db_timeout))
        .connect(&url)
        .await?;

    POOL.set(any_pool)
        .map_err(|_| sqlx::Error::Configuration("Pool already initialised".into()))?;

    // Create typed DatabaseConnection for QueryBuilder / controllers.
    let typed_conn = connect(&url, cfg.db_pool_size).await
        .map_err(|e| sqlx::Error::Configuration(format!("Typed pool init failed: {e}").into()))?;

    CONNECTION.set(typed_conn)
        .map_err(|_| sqlx::Error::Configuration("Connection already initialised".into()))?;

    Ok(())
}

/// Return a reference to the global AnyPool.
///
/// Used by schema introspection which issues raw sqlx queries.
///
/// # Panics
/// Panics if [`init_pool`] has not been called.
pub fn pool() -> &'static AnyPool {
    POOL.get().expect("db::init_pool() must be called before db::pool()")
}

/// Return a reference to the global typed `DatabaseConnection`.
///
/// Used by `QueryBuilder` and controllers for CRUD operations.
///
/// # Panics
/// Panics if [`init_pool`] has not been called.
pub fn connection() -> &'static DatabaseConnection {
    CONNECTION.get().expect("db::init_pool() must be called before db::connection()")
}

/// Convenience: run `SELECT 1` as a health check.
pub async fn ping() -> Result<(), sqlx::Error> {
    sqlx::query("SELECT 1").execute(pool()).await?;
    Ok(())
}
