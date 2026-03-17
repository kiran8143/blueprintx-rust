//! Two-tier cache manager (L1 in-process + L2 Redis).
//!
//! * **L1** uses [`moka::future::Cache`] -- bounded, TTL-based, zero-copy on
//!   hits, no network hop.
//! * **L2** is an **optional** Redis connection.  When `REDIS_HOST` is not
//!   set the manager silently operates as L1-only.
//!
//! ## Promotion
//!
//! On a `get` call the manager checks L1 first.  On L1 miss + L2 hit the
//! value is *promoted* into L1 so subsequent reads are local.
//!
//! ## Cache key
//!
//! Built from `METHOD + path + SHA-256(sorted query string)` so identical
//! requests always map to the same key regardless of query-param ordering.

use moka::future::Cache;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Configuration constants (sensible defaults, overridable via env)
// ---------------------------------------------------------------------------

/// Default L1 max capacity (number of entries).
const L1_MAX_CAPACITY: u64 = 10_000;

/// Default L1 TTL in seconds.
const L1_TTL_SECS: u64 = 300; // 5 minutes

// ---------------------------------------------------------------------------
// CacheManager
// ---------------------------------------------------------------------------

/// Thread-safe two-tier cache manager.
///
/// Clone is cheap (interior `Arc`s).
#[derive(Clone)]
pub struct CacheManager {
    l1: Cache<String, Vec<u8>>,
    l2: Option<Arc<tokio::sync::Mutex<redis::aio::MultiplexedConnection>>>,
}

impl CacheManager {
    /// Create a new manager.
    ///
    /// * Reads `REDIS_HOST`, `REDIS_PORT` (default 6379) from env.
    /// * If `REDIS_HOST` is unset or connection fails, L2 is disabled.
    /// * Reads `CACHE_L1_MAX` (default 10 000) and `CACHE_L1_TTL_SECS`
    ///   (default 300) for L1 tuning.
    pub async fn new() -> Self {
        let l1_max: u64 = std::env::var("CACHE_L1_MAX")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(L1_MAX_CAPACITY);
        let l1_ttl: u64 = std::env::var("CACHE_L1_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(L1_TTL_SECS);

        let l1 = Cache::builder()
            .max_capacity(l1_max)
            .time_to_live(Duration::from_secs(l1_ttl))
            .build();

        let l2 = Self::connect_redis().await;

        Self { l1, l2 }
    }

    /// Try to establish a Redis connection; returns `None` on any failure.
    async fn connect_redis(
    ) -> Option<Arc<tokio::sync::Mutex<redis::aio::MultiplexedConnection>>> {
        let host = std::env::var("REDIS_HOST").ok()?;
        let port: u16 = std::env::var("REDIS_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(6379);

        let url = format!("redis://{host}:{port}");
        let client = redis::Client::open(url.as_str()).ok()?;

        match client.get_multiplexed_tokio_connection().await {
            Ok(conn) => {
                log::info!("Cache L2: connected to Redis at {host}:{port}");
                Some(Arc::new(tokio::sync::Mutex::new(conn)))
            }
            Err(e) => {
                log::warn!("Cache L2: Redis unavailable ({e}), running L1-only");
                None
            }
        }
    }

    // -----------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------

    /// Fetch cached data.
    ///
    /// Check order: L1 -> L2.  L2 hits are promoted to L1.
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        // L1 check.
        if let Some(val) = self.l1.get(key).await {
            return Some(val);
        }

        // L2 check.
        if let Some(ref l2) = self.l2 {
            let mut conn = l2.lock().await;
            let result: redis::RedisResult<Option<Vec<u8>>> =
                redis::cmd("GET").arg(key).query_async(&mut *conn).await;

            if let Ok(Some(data)) = result {
                // Promote to L1.
                self.l1.insert(key.to_string(), data.clone()).await;
                return Some(data);
            }
        }

        None
    }

    // -----------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------

    /// Store data in both tiers.
    ///
    /// `ttl` controls the Redis TTL; L1 TTL is governed by its builder config.
    pub async fn put(&self, key: &str, data: Vec<u8>, ttl: Duration) {
        // L1
        self.l1.insert(key.to_string(), data.clone()).await;

        // L2
        if let Some(ref l2) = self.l2 {
            let mut conn = l2.lock().await;
            let ttl_secs = ttl.as_secs().max(1);
            let _: redis::RedisResult<()> = redis::cmd("SETEX")
                .arg(key)
                .arg(ttl_secs)
                .arg(&data)
                .query_async(&mut *conn)
                .await;
        }
    }

    // -----------------------------------------------------------------
    // Invalidation
    // -----------------------------------------------------------------

    /// Invalidate every entry whose key contains `table_name`.
    ///
    /// L1 is scanned synchronously; L2 uses `SCAN` + `DEL` to avoid blocking
    /// the Redis event loop with `KEYS`.
    pub async fn invalidate_table(&self, table_name: &str) {
        // L1 -- iterate and remove matching keys.
        // moka's `invalidate_entries_if` is not available on async cache, so
        // we collect keys first, then remove.
        let pattern = table_name.to_string();
        self.l1
            .invalidate_entries_if(move |k: &String, _v: &Vec<u8>| k.contains(&pattern))
            .expect("predicate invalidation failed");

        // Run L1 pending tasks so removals are applied.
        self.l1.run_pending_tasks().await;

        // L2 -- scan + delete.
        if let Some(ref l2) = self.l2 {
            let mut conn = l2.lock().await;
            let scan_pattern = format!("*{table_name}*");
            let mut cursor: u64 = 0;
            loop {
                let result: redis::RedisResult<(u64, Vec<String>)> = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&scan_pattern)
                    .arg("COUNT")
                    .arg(200)
                    .query_async(&mut *conn)
                    .await;

                match result {
                    Ok((next_cursor, keys)) => {
                        if !keys.is_empty() {
                            let _: redis::RedisResult<()> =
                                redis::cmd("DEL")
                                    .arg(&keys)
                                    .query_async(&mut *conn)
                                    .await;
                        }
                        cursor = next_cursor;
                        if cursor == 0 {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // Key builder
    // -----------------------------------------------------------------

    /// Build a deterministic cache key from an HTTP request.
    ///
    /// Format: `cache:{METHOD}:{path}:{sha256(sorted_query)}`.
    ///
    /// Only `GET` requests should be cached; callers should check the method
    /// before calling this.
    pub fn build_cache_key(method: &str, path: &str, query: Option<&str>) -> String {
        let query_hash = match query {
            Some(q) if !q.is_empty() => {
                // Sort query params for deterministic key.
                let mut pairs: Vec<&str> = q.split('&').collect();
                pairs.sort_unstable();
                let sorted = pairs.join("&");

                let mut hasher = Sha256::new();
                hasher.update(sorted.as_bytes());
                let digest = hasher.finalize();
                hex_encode(&digest[..8]) // first 8 bytes = 16 hex chars
            }
            _ => "none".to_string(),
        };

        format!("cache:{method}:{path}:{query_hash}")
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Minimal hex encoder (avoids pulling in the `hex` crate for 10 lines).
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_cache_key_deterministic() {
        let k1 = CacheManager::build_cache_key("GET", "/api/users", Some("page=1&limit=10"));
        let k2 = CacheManager::build_cache_key("GET", "/api/users", Some("limit=10&page=1"));
        // Same params, different order -> same key (sorted).
        assert_eq!(k1, k2);
    }

    #[test]
    fn build_cache_key_no_query() {
        let k = CacheManager::build_cache_key("GET", "/api/health", None);
        assert!(k.starts_with("cache:GET:/api/health:"));
        assert!(k.ends_with(":none"));
    }

    #[test]
    fn build_cache_key_different_paths() {
        let k1 = CacheManager::build_cache_key("GET", "/api/users", None);
        let k2 = CacheManager::build_cache_key("GET", "/api/orders", None);
        assert_ne!(k1, k2);
    }

    #[test]
    fn build_cache_key_includes_method() {
        let k1 = CacheManager::build_cache_key("GET", "/api/items", None);
        let k2 = CacheManager::build_cache_key("POST", "/api/items", None);
        assert_ne!(k1, k2);
    }

    #[test]
    fn hex_encode_correctness() {
        assert_eq!(hex_encode(&[0x0a, 0xff, 0x00]), "0aff00");
    }

    #[tokio::test]
    async fn l1_put_and_get() {
        // No REDIS_HOST set, so L2 is None.
        let mgr = CacheManager::new().await;
        let key = "cache:GET:/test:none";
        let data = b"hello".to_vec();

        mgr.put(key, data.clone(), Duration::from_secs(60)).await;
        let got = mgr.get(key).await;
        assert_eq!(got, Some(data));
    }

    #[tokio::test]
    async fn l1_miss_returns_none() {
        let mgr = CacheManager::new().await;
        let got = mgr.get("cache:GET:/nonexistent:none").await;
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn invalidate_table_removes_matching() {
        let mgr = CacheManager::new().await;

        mgr.put(
            "cache:GET:/api/users:abc",
            b"data1".to_vec(),
            Duration::from_secs(60),
        )
        .await;
        mgr.put(
            "cache:GET:/api/orders:def",
            b"data2".to_vec(),
            Duration::from_secs(60),
        )
        .await;

        mgr.invalidate_table("users").await;

        assert!(mgr.get("cache:GET:/api/users:abc").await.is_none());
        assert!(mgr.get("cache:GET:/api/orders:def").await.is_some());
    }
}
