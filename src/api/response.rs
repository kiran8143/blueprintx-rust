// Author: Udaykiran Atta
// License: MIT

//! Uniform API response builders.
//!
//! Success responses return the data directly as JSON.
//! Error responses return `{ "error": { "message": "...", "status": N } }`.
//!
//! Mirrors the C++ `ApiResponse` class from the Drogon blueprint.

use actix_web::HttpResponse;
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Success responses
// ---------------------------------------------------------------------------

/// 200 OK -- return `data` directly as JSON body.
pub fn ok(data: Value) -> HttpResponse {
    HttpResponse::Ok().json(data)
}

/// 201 Created -- return `data` directly as JSON body.
pub fn created(data: Value) -> HttpResponse {
    HttpResponse::Created().json(data)
}

// ---------------------------------------------------------------------------
// Error responses
// ---------------------------------------------------------------------------

/// Build a standard error envelope.
fn error_body(message: &str, status: u16) -> Value {
    json!({
        "error": {
            "message": message,
            "status": status
        }
    })
}

/// Build an error envelope with additional details.
fn error_body_with_details(message: &str, status: u16, details: Value) -> Value {
    json!({
        "error": {
            "message": message,
            "status": status,
            "details": details
        }
    })
}

/// 400 Bad Request.
pub fn bad_request(msg: &str) -> HttpResponse {
    HttpResponse::BadRequest().json(error_body(msg, 400))
}

/// 401 Unauthorized.
pub fn unauthorized(msg: &str) -> HttpResponse {
    HttpResponse::Unauthorized().json(error_body(msg, 401))
}

/// 404 Not Found.
pub fn not_found(msg: &str) -> HttpResponse {
    HttpResponse::NotFound().json(error_body(msg, 404))
}

/// 409 Conflict (e.g. duplicate key).
pub fn conflict(msg: &str) -> HttpResponse {
    HttpResponse::Conflict().json(error_body(msg, 409))
}

/// 422 Unprocessable Entity -- validation errors.
///
/// `errors` should be a JSON array of `{ field, code, message }` objects.
pub fn validation_error(errors: Value) -> HttpResponse {
    HttpResponse::build(actix_web::http::StatusCode::UNPROCESSABLE_ENTITY)
        .json(error_body_with_details("Validation failed", 422, errors))
}

/// 429 Too Many Requests.
///
/// Includes a `Retry-After` header with the specified number of seconds.
pub fn too_many_requests(retry_after: u64) -> HttpResponse {
    HttpResponse::TooManyRequests()
        .insert_header(("Retry-After", retry_after.to_string()))
        .json(error_body("Too many requests", 429))
}

/// 500 Internal Server Error.
pub fn internal_error(msg: &str) -> HttpResponse {
    HttpResponse::InternalServerError().json(error_body(msg, 500))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::body::MessageBody;
    use serde_json::json;

    /// Helper to extract JSON from HttpResponse.
    fn extract_json(resp: HttpResponse) -> Value {
        let body = resp.into_body().try_into_bytes().unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[test]
    fn ok_returns_data_directly() {
        let data = json!({"name": "Alice", "age": 30});
        let resp = ok(data.clone());
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(extract_json(resp), data);
    }

    #[test]
    fn created_returns_data_directly() {
        let data = json!({"id": 1});
        let resp = created(data.clone());
        assert_eq!(resp.status().as_u16(), 201);
        assert_eq!(extract_json(resp), data);
    }

    #[test]
    fn bad_request_has_error_envelope() {
        let resp = bad_request("Invalid input");
        assert_eq!(resp.status().as_u16(), 400);
        let body = extract_json(resp);
        assert_eq!(body["error"]["message"], "Invalid input");
        assert_eq!(body["error"]["status"], 400);
    }

    #[test]
    fn unauthorized_has_error_envelope() {
        let resp = unauthorized("Token expired");
        assert_eq!(resp.status().as_u16(), 401);
        let body = extract_json(resp);
        assert_eq!(body["error"]["message"], "Token expired");
        assert_eq!(body["error"]["status"], 401);
    }

    #[test]
    fn not_found_has_error_envelope() {
        let resp = not_found("Record not found");
        assert_eq!(resp.status().as_u16(), 404);
        let body = extract_json(resp);
        assert_eq!(body["error"]["message"], "Record not found");
        assert_eq!(body["error"]["status"], 404);
    }

    #[test]
    fn conflict_has_error_envelope() {
        let resp = conflict("Record already exists");
        assert_eq!(resp.status().as_u16(), 409);
        let body = extract_json(resp);
        assert_eq!(body["error"]["message"], "Record already exists");
        assert_eq!(body["error"]["status"], 409);
    }

    #[test]
    fn validation_error_includes_details() {
        let errors = json!([{"field": "email", "code": "required", "message": "Email is required"}]);
        let resp = validation_error(errors.clone());
        assert_eq!(resp.status().as_u16(), 422);
        let body = extract_json(resp);
        assert_eq!(body["error"]["message"], "Validation failed");
        assert_eq!(body["error"]["details"], errors);
    }

    #[test]
    fn too_many_requests_includes_retry_after() {
        let resp = too_many_requests(30);
        assert_eq!(resp.status().as_u16(), 429);
        assert_eq!(
            resp.headers().get("Retry-After").unwrap().to_str().unwrap(),
            "30"
        );
    }

    #[test]
    fn internal_error_has_error_envelope() {
        let resp = internal_error("Something broke");
        assert_eq!(resp.status().as_u16(), 500);
        let body = extract_json(resp);
        assert_eq!(body["error"]["message"], "Something broke");
        assert_eq!(body["error"]["status"], 500);
    }
}
