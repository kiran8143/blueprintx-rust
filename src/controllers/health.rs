// Author: Udaykiran Atta
// License: MIT

//! Health check endpoint.
//!
//! `GET /health` returns `{ "status": "ok" }` with a 200 when the database
//! is reachable, or `{ "status": "error", "message": "..." }` with a 503
//! when the database is unreachable.

use actix_web::{web, HttpResponse};
use serde_json::json;

use crate::db;

/// `GET /health`
///
/// Pings the database and returns overall service health.
pub async fn check() -> HttpResponse {
    match db::ping().await {
        Ok(()) => HttpResponse::Ok().json(json!({
            "status": "ok"
        })),
        Err(e) => {
            log::error!("Health check failed: {}", e);
            HttpResponse::ServiceUnavailable().json(json!({
                "status": "error",
                "message": format!("Database unreachable: {}", e)
            }))
        }
    }
}

/// Register health routes on the given `ServiceConfig`.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.route("/health", web::get().to(check));
}
