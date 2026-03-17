//! JWT Bearer-token authentication middleware for Actix-web.
//!
//! Extracts the `Authorization: Bearer <token>` header, validates it with
//! HS256 using the configured secret, and stores the decoded [`Claims`] in
//! request extensions so downstream handlers can access them with
//! `req.extensions().get::<Claims>()`.
//!
//! Certain paths are skipped (health-check, docs, login, register).

use std::future::{ready, Future, Ready};
use std::pin::Pin;
use std::rc::Rc;

use actix_web::body::EitherBody;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::HttpMessage;
use actix_web::http::header::AUTHORIZATION;
use actix_web::HttpResponse;
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Claims
// ---------------------------------------------------------------------------

/// JWT payload stored in request extensions after successful validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,        // user_id
    pub role: String,       // e.g. "admin", "user"
    pub exp: usize,         // expiration (epoch seconds)
    #[serde(default)]
    pub iat: usize,         // issued at
}

impl Claims {
    /// Convenience accessor for the user id (`sub` field).
    #[inline]
    pub fn user_id(&self) -> &str {
        &self.sub
    }
}

// ---------------------------------------------------------------------------
// Paths that bypass authentication
// ---------------------------------------------------------------------------

/// Returns `true` when the path does **not** require a valid JWT.
fn is_skip_path(path: &str) -> bool {
    matches!(path, "/health" | "/api/docs")
        || path.starts_with("/api/login")
        || path.starts_with("/api/register")
}

// ---------------------------------------------------------------------------
// Middleware factory (implements Transform)
// ---------------------------------------------------------------------------

/// Actix-web middleware factory.  Wrap your `App` with `.wrap(JwtAuth::new(secret))`.
#[derive(Clone)]
pub struct JwtAuth {
    secret: Rc<String>,
}

impl JwtAuth {
    pub fn new(secret: impl Into<String>) -> Self {
        Self {
            secret: Rc::new(secret.into()),
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for JwtAuth
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = actix_web::Error;
    type InitError = ();
    type Transform = JwtAuthMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(JwtAuthMiddleware {
            service: Rc::new(service),
            secret: Rc::clone(&self.secret),
        }))
    }
}

// ---------------------------------------------------------------------------
// Middleware service (implements Service)
// ---------------------------------------------------------------------------

pub struct JwtAuthMiddleware<S> {
    service: Rc<S>,
    secret: Rc<String>,
}

impl<S, B> Service<ServiceRequest> for JwtAuthMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = actix_web::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(
        &self,
        ctx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let service = Rc::clone(&self.service);
        let secret = Rc::clone(&self.secret);

        Box::pin(async move {
            // Skip authentication for whitelisted paths.
            if is_skip_path(req.path()) {
                return service
                    .call(req)
                    .await
                    .map(|res| res.map_into_left_body());
            }

            // Extract Bearer token.
            let token = req
                .headers()
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(|t| t.trim());

            let token = match token {
                Some(t) if !t.is_empty() => t,
                _ => {
                    let resp = HttpResponse::Unauthorized()
                        .json(serde_json::json!({
                            "error": "Missing or invalid Authorization header"
                        }));
                    return Ok(req.into_response(resp).map_into_right_body());
                }
            };

            // Validate token.
            let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
            validation.validate_exp = true;

            match decode::<Claims>(
                token,
                &DecodingKey::from_secret(secret.as_bytes()),
                &validation,
            ) {
                Ok(token_data) => {
                    // Store claims in request extensions.
                    req.extensions_mut().insert(token_data.claims);
                    service
                        .call(req)
                        .await
                        .map(|res| res.map_into_left_body())
                }
                Err(e) => {
                    log::warn!("JWT validation failed: {e}");
                    let resp = HttpResponse::Unauthorized()
                        .json(serde_json::json!({
                            "error": "Invalid or expired token"
                        }));
                    Ok(req.into_response(resp).map_into_right_body())
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_paths() {
        assert!(is_skip_path("/health"));
        assert!(is_skip_path("/api/docs"));
        assert!(is_skip_path("/api/login"));
        assert!(is_skip_path("/api/login/google"));
        assert!(is_skip_path("/api/register"));
        assert!(is_skip_path("/api/register/verify"));
        assert!(!is_skip_path("/api/users"));
        assert!(!is_skip_path("/api/orders/123"));
    }

    #[test]
    fn claims_user_id() {
        let c = Claims {
            sub: "u-42".into(),
            role: "admin".into(),
            exp: 9999999999,
            iat: 0,
        };
        assert_eq!(c.user_id(), "u-42");
    }
}
