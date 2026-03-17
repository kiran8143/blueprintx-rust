// Author: Udaykiran Atta
// License: MIT

//! Rust Blueprint -- high-performance REST API framework.
//!
//! Module tree declarations.  Each module corresponds to a layer in the
//! C++ Drogon blueprint architecture.

pub mod config;
pub mod db;
pub mod schema;
pub mod query;
pub mod serializer;
pub mod protection;
pub mod middleware;
pub mod cache;
pub mod api;
pub mod controllers;
