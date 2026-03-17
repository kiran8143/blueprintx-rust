//! Environment configuration loader.
//!
//! Thread-safe singleton that reads from `.env` at process start.
//! Equivalent to the C++ EnvConfig used in the Drogon blueprint.

use std::env;
use std::sync::OnceLock;

/// Global singleton instance.
static INSTANCE: OnceLock<Config> = OnceLock::new();

/// Application configuration backed by environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    // ---- Server ----
    pub port: u16,
    pub host: String,
    pub environment: String,

    // ---- Database ----
    pub db_engine: String,
    pub db_host: String,
    pub db_port: u16,
    pub db_name: String,
    pub db_user: String,
    pub db_password: String,
    pub db_pool_size: u32,
    pub db_timeout: f64,

    // ---- JWT ----
    pub jwt_secret: String,
    pub jwt_expiry_seconds: i64,

    // ---- CORS ----
    pub cors_origins: Vec<String>,
    pub cors_methods: Vec<String>,
    pub cors_headers: Vec<String>,

    // ---- Logging ----
    pub log_level: String,
}

impl Config {
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    /// Initialise the global singleton.  Call once from `main()` after
    /// `dotenv::dotenv().ok()`.
    pub fn init() {
        INSTANCE.get_or_init(|| Config::from_env());
    }

    /// Return a reference to the global configuration.
    ///
    /// # Panics
    /// Panics if [`Config::init`] has not been called.
    pub fn global() -> &'static Config {
        INSTANCE.get().expect("Config::init() must be called before Config::global()")
    }

    /// Build a `Config` by reading environment variables.
    fn from_env() -> Self {
        Self {
            // Server
            port: Self::get_int("PORT", 8080) as u16,
            host: Self::get("HOST", "0.0.0.0"),
            environment: Self::get("ENVIRONMENT", "development"),

            // Database
            db_engine: Self::get("DB_ENGINE", "mysql"),
            db_host: Self::get("DB_HOST", "localhost"),
            db_port: Self::get_int("DB_PORT", 3306) as u16,
            db_name: Self::get("DB_NAME", "app"),
            db_user: Self::get("DB_USER", "root"),
            db_password: Self::get("DB_PASSWORD", ""),
            db_pool_size: Self::get_int("DB_POOL_SIZE", 8) as u32,
            db_timeout: Self::get("DB_TIMEOUT", "30.0")
                .parse::<f64>()
                .unwrap_or(30.0),

            // JWT
            jwt_secret: Self::get("JWT_SECRET", "change-me"),
            jwt_expiry_seconds: Self::get_int("JWT_EXPIRY_SECONDS", 3600),

            // CORS
            cors_origins: Self::get_list("CORS_ORIGINS", "http://localhost:3000"),
            cors_methods: Self::get_list("CORS_METHODS", "GET,POST,PUT,DELETE,OPTIONS"),
            cors_headers: Self::get_list("CORS_HEADERS", "Content-Type,Authorization"),

            // Logging
            log_level: Self::get("LOG_LEVEL", "WARN"),
        }
    }

    // ------------------------------------------------------------------
    // Helpers (public so callers can read arbitrary env vars)
    // ------------------------------------------------------------------

    /// Read a string env var with a default.
    pub fn get(key: &str, default: &str) -> String {
        env::var(key)
            .map(|v| {
                // Strip inline comments: "mysql  # comment" -> "mysql"
                if let Some(pos) = v.find('#') {
                    v[..pos].trim().to_string()
                } else {
                    v.trim().to_string()
                }
            })
            .unwrap_or_else(|_| default.to_string())
    }

    /// Read an integer env var with a default.
    pub fn get_int(key: &str, default: i64) -> i64 {
        Self::get(key, &default.to_string())
            .parse::<i64>()
            .unwrap_or(default)
    }

    /// Read a comma-separated list env var.
    pub fn get_list(key: &str, default: &str) -> Vec<String> {
        let raw = Self::get(key, default);
        raw.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    // ------------------------------------------------------------------
    // Database URL builder
    // ------------------------------------------------------------------

    /// Return the normalised database engine name.
    ///
    /// Maps common aliases: `postgresql` / `postgres` -> `"postgres"`,
    /// `mysql` -> `"mysql"`, `sqlite3` / `sqlite` -> `"sqlite"`.
    pub fn db_engine_normalised(&self) -> &str {
        match self.db_engine.to_lowercase().as_str() {
            "postgresql" | "postgres" => "postgres",
            "mysql" | "mariadb" => "mysql",
            "sqlite3" | "sqlite" => "sqlite",
            other => {
                log::warn!("Unknown DB_ENGINE '{other}', falling back to mysql");
                "mysql"
            }
        }
    }

    /// Build the full database connection URL for SQLx.
    ///
    /// * MySQL:    `mysql://user:pass@host:port/db`
    /// * Postgres: `postgres://user:pass@host:port/db`
    /// * SQLite:   `sqlite:db_name.db` (file on disk)
    pub fn db_url(&self) -> String {
        match self.db_engine_normalised() {
            "sqlite" => {
                format!("sqlite:{}.db", self.db_name)
            }
            engine => {
                // URL-encode the password so special chars (like #) survive.
                let encoded_password = urlencoded(&self.db_password);
                format!(
                    "{engine}://{user}:{password}@{host}:{port}/{db}",
                    user = self.db_user,
                    password = encoded_password,
                    host = self.db_host,
                    port = self.db_port,
                    db = self.db_name,
                )
            }
        }
    }

    /// True when running in production.
    pub fn is_production(&self) -> bool {
        self.environment.eq_ignore_ascii_case("production")
    }
}

// ------------------------------------------------------------------
// Minimal percent-encoding for the password component of a URL.
// We only encode characters that are unsafe inside a userinfo segment
// (RFC 3986 section 3.2.1).  This avoids pulling in the `percent-encoding`
// crate for a single use.
// ------------------------------------------------------------------

fn urlencoded(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 2);
    for byte in input.bytes() {
        match byte {
            // unreserved chars + sub-delims that are safe in userinfo
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'.'
            | b'_'
            | b'~' => out.push(byte as char),
            _ => {
                out.push('%');
                out.push(char::from(HEX[(byte >> 4) as usize]));
                out.push(char::from(HEX[(byte & 0x0F) as usize]));
            }
        }
    }
    out
}

static HEX: [u8; 16] = *b"0123456789ABCDEF";

// ------------------------------------------------------------------
// Display
// ------------------------------------------------------------------

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Configuration ===")?;
        writeln!(f, "  Server:   {}:{}", self.host, self.port)?;
        writeln!(f, "  Env:      {}", self.environment)?;
        writeln!(f, "  DB:       {} @ {}:{}/{}", self.db_engine, self.db_host, self.db_port, self.db_name)?;
        writeln!(f, "  Pool:     {} connections", self.db_pool_size)?;
        writeln!(f, "  Log:      {}", self.log_level)?;
        writeln!(f, "  CORS:     {:?}", self.cors_origins)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_with_inline_comment() {
        env::set_var("TEST_CFG_COMMENTED", "mysql   # this is mysql");
        let val = Config::get("TEST_CFG_COMMENTED", "default");
        assert_eq!(val, "mysql");
        env::remove_var("TEST_CFG_COMMENTED");
    }

    #[test]
    fn test_get_int_fallback() {
        let val = Config::get_int("DEFINITELY_NOT_SET_12345", 42);
        assert_eq!(val, 42);
    }

    #[test]
    fn test_get_list() {
        env::set_var("TEST_CFG_LIST", "a, b , c");
        let val = Config::get_list("TEST_CFG_LIST", "x");
        assert_eq!(val, vec!["a", "b", "c"]);
        env::remove_var("TEST_CFG_LIST");
    }

    #[test]
    fn test_url_encode_special_chars() {
        // Special characters like '#' and '@' should be percent-encoded
        let encoded = urlencoded("p@ss#w0rd!");
        assert_eq!(encoded, "p%40ss%23w0rd%21");
    }

    #[test]
    fn test_db_url_mysql() {
        env::set_var("DB_ENGINE", "mysql");
        env::set_var("DB_HOST", "db.example.com");
        env::set_var("DB_PORT", "3306");
        env::set_var("DB_NAME", "testdb");
        env::set_var("DB_USER", "admin");
        env::set_var("DB_PASSWORD", "p@ss#w0rd");

        let cfg = Config::from_env();
        let url = cfg.db_url();
        assert!(url.starts_with("mysql://"));
        assert!(url.contains("admin"));
        assert!(url.contains("p%40ss%23w0rd")); // @ and # encoded
        assert!(url.contains("db.example.com:3306/testdb"));

        // Clean up
        for key in ["DB_ENGINE", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"] {
            env::remove_var(key);
        }
    }

    #[test]
    fn test_db_url_sqlite() {
        env::set_var("DB_ENGINE", "sqlite3");
        env::set_var("DB_NAME", "myapp");
        let cfg = Config::from_env();
        assert_eq!(cfg.db_url(), "sqlite:myapp.db");
        env::remove_var("DB_ENGINE");
        env::remove_var("DB_NAME");
    }
}
