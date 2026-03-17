//! Per-IP sliding-window rate limiter middleware for Actix-web.
//!
//! Uses a [`DashMap`] of `IpAddr -> Vec<Instant>` to track request timestamps
//! per client.  When the window is exhausted a `429 Too Many Requests`
//! response is returned with a `Retry-After` header.
//!
//! A background task periodically purges stale entries so the map does not
//! grow without bound.
//!
//! Configuration is read from environment variables:
//!
//! | Variable                 | Default |
//! |--------------------------|---------|
//! | `RATE_LIMIT_MAX`         | 100     |
//! | `RATE_LIMIT_WINDOW_SECS` | 60      |

use std::future::{ready, Future, Ready};
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::body::EitherBody;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::HttpResponse;
use dashmap::DashMap;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// Per-IP request log.  Each entry is the `Instant` of a request that falls
/// within the current sliding window.
type IpLog = DashMap<IpAddr, Vec<Instant>>;

/// Configuration + shared state for the rate limiter.
#[derive(Clone)]
struct RateLimiterState {
    log: Arc<IpLog>,
    max_requests: usize,
    window: Duration,
}

// ---------------------------------------------------------------------------
// Middleware factory
// ---------------------------------------------------------------------------

/// Actix-web middleware factory.
///
/// ```ignore
/// App::new()
///     .wrap(RateLimiter::from_env())
/// ```
#[derive(Clone)]
pub struct RateLimiter {
    state: RateLimiterState,
}

impl RateLimiter {
    /// Build a rate limiter with explicit settings.
    pub fn new(max_requests: usize, window_secs: u64) -> Self {
        let state = RateLimiterState {
            log: Arc::new(DashMap::new()),
            max_requests,
            window: Duration::from_secs(window_secs),
        };

        // Spawn background cleanup every `window_secs`.
        let cleanup_state = state.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(window_secs.max(10)));
            loop {
                interval.tick().await;
                cleanup(&cleanup_state);
            }
        });

        Self { state }
    }

    /// Build from environment variables (see module docs for variable names).
    pub fn from_env() -> Self {
        let max: usize = std::env::var("RATE_LIMIT_MAX")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        let window_secs: u64 = std::env::var("RATE_LIMIT_WINDOW_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60);

        Self::new(max, window_secs)
    }
}

// ---------------------------------------------------------------------------
// Background cleanup
// ---------------------------------------------------------------------------

/// Remove entries whose most recent timestamp is older than the window, and
/// prune stale timestamps from entries that are still active.
fn cleanup(state: &RateLimiterState) {
    let cutoff = Instant::now() - state.window;
    state.log.retain(|_ip, timestamps| {
        timestamps.retain(|t| *t > cutoff);
        !timestamps.is_empty()
    });
}

// ---------------------------------------------------------------------------
// IP extraction
// ---------------------------------------------------------------------------

/// Best-effort client IP: prefer `X-Forwarded-For`, fall back to peer addr.
fn client_ip(req: &ServiceRequest) -> IpAddr {
    // X-Forwarded-For: first entry is the original client.
    if let Some(xff) = req.headers().get("X-Forwarded-For") {
        if let Ok(val) = xff.to_str() {
            if let Some(first) = val.split(',').next() {
                if let Ok(ip) = first.trim().parse::<IpAddr>() {
                    return ip;
                }
            }
        }
    }

    req.peer_addr()
        .map(|a| a.ip())
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))
}

// ---------------------------------------------------------------------------
// Transform impl
// ---------------------------------------------------------------------------

impl<S, B> Transform<S, ServiceRequest> for RateLimiter
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = actix_web::Error;
    type InitError = ();
    type Transform = RateLimiterMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RateLimiterMiddleware {
            service: std::rc::Rc::new(service),
            state: self.state.clone(),
        }))
    }
}

// ---------------------------------------------------------------------------
// Service impl
// ---------------------------------------------------------------------------

pub struct RateLimiterMiddleware<S> {
    service: std::rc::Rc<S>,
    state: RateLimiterState,
}

impl<S, B> Service<ServiceRequest> for RateLimiterMiddleware<S>
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
        let service = std::rc::Rc::clone(&self.service);
        let state = self.state.clone();

        Box::pin(async move {
            let ip = client_ip(&req);
            let now = Instant::now();
            let cutoff = now - state.window;

            // Atomic check-and-record.
            let mut entry = state.log.entry(ip).or_insert_with(Vec::new);
            // Prune old timestamps in-place.
            entry.retain(|t| *t > cutoff);

            if entry.len() >= state.max_requests {
                // Compute Retry-After: time until the oldest entry expires.
                let retry_after = entry
                    .first()
                    .map(|oldest| {
                        let elapsed = now.duration_since(*oldest);
                        if state.window > elapsed {
                            (state.window - elapsed).as_secs()
                        } else {
                            1
                        }
                    })
                    .unwrap_or(1);

                drop(entry); // release DashMap lock

                let resp = HttpResponse::TooManyRequests()
                    .insert_header(("Retry-After", retry_after.to_string()))
                    .json(serde_json::json!({
                        "error": "Too many requests",
                        "retry_after_secs": retry_after,
                    }));
                return Ok(req.into_response(resp).map_into_right_body());
            }

            entry.push(now);
            drop(entry); // release DashMap lock before calling inner service

            service
                .call(req)
                .await
                .map(|res| res.map_into_left_body())
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
    fn cleanup_removes_stale() {
        let state = RateLimiterState {
            log: Arc::new(DashMap::new()),
            max_requests: 5,
            window: Duration::from_millis(50),
        };

        let old = Instant::now() - Duration::from_millis(200);
        let recent = Instant::now();

        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        state.log.insert(ip, vec![old, recent]);

        cleanup(&state);

        let entry = state.log.get(&ip).unwrap();
        assert_eq!(entry.len(), 1);
    }

    #[test]
    fn cleanup_removes_empty_entries() {
        let state = RateLimiterState {
            log: Arc::new(DashMap::new()),
            max_requests: 5,
            window: Duration::from_millis(10),
        };

        let ip: IpAddr = "10.0.0.2".parse().unwrap();
        let old = Instant::now() - Duration::from_millis(100);
        state.log.insert(ip, vec![old]);

        cleanup(&state);

        assert!(state.log.get(&ip).is_none());
    }
}
