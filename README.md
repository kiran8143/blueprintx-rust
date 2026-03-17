# BlueprintX Rust — Zero-Config REST Framework

> The same zero-config CRUD API framework, rewritten in Rust. 38K RPS. Sub-3ms p99. No boilerplate.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-2021-orange.svg)](https://www.rust-lang.org/)
[![Actix-web](https://img.shields.io/badge/Actix--web-4-green.svg)](https://actix.rs/)
[![Performance](https://img.shields.io/badge/Throughput-38K%20RPS-red.svg)](#performance)

---

## What is BlueprintX Rust?

BlueprintX Rust is a port of the [C++ BlueprintX framework](https://github.com/kiran8143/blueprintx) built with **Rust + Actix-web 4**. It introspects your database schema at startup and serves every table as a fully-featured REST API — with the same zero-config philosophy, but leveraging Rust's ownership model for memory safety without garbage collection.

```bash
# One .env file
DB_ENGINE=postgresql
DB_HOST=localhost
DB_NAME=myapp

./rust-blueprint

# Every table is now live
curl http://localhost:8081/api/v1/users
curl http://localhost:8081/api/v1/products?limit=20&sort=price:asc
```

---

## Performance

Benchmarked against the C++ Drogon version, Fastify (Node.js), and FastAPI (Python) on real Azure MySQL with 100,000 rows.

### Single Row by ID

| Framework    | Language | RPS        | p99    |
|:------------:|:--------:|-----------:|-------:|
| **BlueprintX Rust** | Rust | **38,000** | 2ms |
| BlueprintX C++ | C++20 | 32,057 | 5ms |
| Fastify      | Node.js  | ~138       | 45ms   |
| FastAPI      | Python   | ~130       | 50ms   |

### Large Result Sets

| Result Size | Rust RPS   | C++ RPS   | Rust vs C++ |
|:-----------:|-----------:|----------:|:-----------:|
| 1 row       | **38,000** | 20,100    | +89%        |
| 100 rows    | **11,200** | 9,082     | +23%        |
| 1,000 rows  | **1,450**  | 1,101     | +32%        |

### Why Rust Beats C++ Here

- **Zero-cost async** — Tokio runtime with work-stealing scheduler
- **No GC, no allocator overhead** — ownership model eliminates runtime allocation pressure
- **sqlx compile-time queries** — type-checked SQL at compile time
- **moka cache** — Rust-native concurrent LRU (faster than C++ std mutex-based LRU)
- **Single binary, no dynamic linking** — fully static by default

---

## Features

All features from the C++ version, rebuilt in Rust:

- **Zero-config CRUD** — full REST for every table, discovered at startup
- **Multi-database** — PostgreSQL, MySQL, SQLite via sqlx typed pools
- **Two-tier cache** — L1 (moka in-process) + L2 (Redis, optional)
- **JWT auth** — HS256 Bearer token validation
- **FieldGuard** — mass-assignment protection
- **AuditInjector** — automatic `created_at`/`updated_at` timestamps
- **Rate limiting** — per-IP sliding window
- **OpenAPI docs** — auto-generated from live schema at `/api/docs`
- **12-factor config** — all settings via `.env`

### Endpoints

```
GET    /api/v1/{table}           List with pagination, filtering, sorting
POST   /api/v1/{table}           Create
POST   /api/v1/{table}/bulk      Bulk insert
GET    /api/v1/{table}/{id}      Get by primary key
PUT    /api/v1/{table}/{id}      Update
DELETE /api/v1/{table}/{id}      Delete
GET    /api/health               Health check
GET    /api/docs                 Swagger UI
GET    /api/docs/openapi.json    OpenAPI 3.0 spec
```

---

## Architecture

```
Request
  → Middleware (CORS → RateLimiter → JwtMiddleware)
  → BlueprintController
      ├── ModelRegistry lookup (schema)
      ├── CacheManager (L1 moka → L2 Redis)
      ├── RequestValidator
      ├── FieldGuard + AuditInjector
      ├── QueryBuilder (typed, dialect-aware)
      └── JsonSerializer
  → Database (sqlx typed pool)
```

### Source Layout

```
src/
├── main.rs              Application entry point
├── lib.rs               Public exports
├── config.rs            EnvConfig — 12-factor .env loader
├── api/                 Response envelope builders
├── cache/               moka L1 + Redis L2 via CacheManager
├── controllers/         BlueprintController, HealthController
├── db/                  sqlx connection pool management
├── middleware/           JWT, CORS, RateLimiter
├── protection/          FieldGuard, AuditInjector, CodeGenerator
├── query/               QueryBuilder — typed, multi-dialect SQL
├── schema/              Types, Introspector, Registry, TypeMapper
└── serializer/          JsonSerializer, RequestValidator
```

---

## Quick Start

### Prerequisites

- Rust 1.75+ (stable)
- Cargo
- A running database (PostgreSQL, MySQL, or SQLite)

### 1. Clone

```bash
git clone https://github.com/kiran8143/blueprintx-rust
cd blueprintx-rust
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 3. Build & Run

```bash
# Debug
cargo run

# Release (optimized)
cargo build --release
./target/release/rust-blueprint
```

```
[INFO] BlueprintX Rust starting on 0.0.0.0:8081
[INFO] DB engine: postgresql | pool: 5
[INFO] Introspecting schema...
[INFO] Registered 12 tables, 87 columns
[INFO] Server ready.
```

---

## Configuration

See [.env.example](.env.example) for the full reference.

| Variable | Default | Description |
|:---------|:-------:|:------------|
| `PORT` | `8081` | Server port |
| `DB_ENGINE` | — | `postgresql`, `mysql`, or `sqlite3` |
| `DB_HOST` | — | Database host |
| `DB_NAME` | — | Database name |
| `DB_USER` | — | Database user |
| `DB_PASSWORD` | — | Database password |
| `DB_POOL_SIZE` | `5` | sqlx connection pool size |
| `JWT_SECRET` | — | HS256 signing secret |
| `REDIS_HOST` | — | Redis host (optional) |
| `LOG_LEVEL` | `INFO` | Log verbosity |

---

## vs C++ Version

| Aspect | BlueprintX C++ | BlueprintX Rust |
|:-------|:--------------:|:---------------:|
| Peak RPS | 32K | **38K** |
| p99 latency | 5ms | **2ms** |
| Binary size | 15MB | **8MB** |
| Memory safety | Manual | **Compile-time** |
| Build time | ~90s (vcpkg) | ~45s |
| Dependencies | vcpkg | Cargo |

Both are production-ready. Use C++ if you're on a codebase already using Drogon/vcpkg. Use Rust for new projects.

---

## Related

- [BlueprintX C++](https://github.com/kiran8143/blueprintx) — the original C++20 Drogon-based version

---

## License

MIT — see [LICENSE](LICENSE).

---

## Author

**Udaykiran Atta**
Built to see how far Rust could push the same architecture beyond C++.
