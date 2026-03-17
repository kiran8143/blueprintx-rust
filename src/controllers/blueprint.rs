// Author: Udaykiran Atta
// License: MIT

//! Blueprint CRUD controller.
//!
//! Dynamic REST endpoints that serve any table discovered by schema
//! introspection at startup.  All routes are parameterised by `{table}`
//! and (optionally) `{id}`.
//!
//! Routes:
//!   GET    /api/v1/{table}        -> list with pagination/filter/sort
//!   POST   /api/v1/{table}        -> create record
//!   POST   /api/v1/{table}/bulk   -> bulk insert
//!   GET    /api/v1/{table}/{id}   -> get by primary key
//!   PUT    /api/v1/{table}/{id}   -> update record
//!   DELETE /api/v1/{table}/{id}   -> delete record

use actix_web::{web, HttpMessage, HttpRequest, HttpResponse};
use serde_json::{json, Value};
use std::time::Duration;

use crate::api::response;
use crate::cache::manager::CacheManager;
use crate::config::Config;
use crate::db;
use crate::middleware::jwt::Claims;
use crate::protection::{audit, code_gen, field_guard, TableMeta as ProtTableMeta};
use crate::schema::ModelRegistry;
use crate::schema::types::DynamicRow;

// ---------------------------------------------------------------------------
// Query parameter parsing
// ---------------------------------------------------------------------------

/// Parsed list query parameters.
struct ListParams {
    limit: usize,
    offset: usize,
    sort_column: Option<String>,
    sort_direction: String,
    filters: Vec<(String, String)>,
    want_count: bool,
}

/// Parse pagination, sorting, and filtering from query string.
fn parse_list_params(req: &HttpRequest) -> ListParams {
    let query_string = req.query_string();
    let params: Vec<(String, String)> =
        serde_urlencoded::from_str(query_string).unwrap_or_default();

    let mut limit: usize = 20;
    let mut offset: usize = 0;
    let mut sort_column: Option<String> = None;
    let mut sort_direction = "ASC".to_string();
    let mut filters: Vec<(String, String)> = Vec::new();
    let mut want_count = false;

    for (key, val) in &params {
        match key.as_str() {
            "limit" => {
                if let Ok(l) = val.parse::<usize>() {
                    limit = l.clamp(1, 50_000);
                }
            }
            "offset" => {
                if let Ok(o) = val.parse::<usize>() {
                    offset = o;
                }
            }
            "sort" => {
                if let Some(colon_pos) = val.find(':') {
                    sort_column = Some(val[..colon_pos].to_string());
                    let dir = val[colon_pos + 1..].to_uppercase();
                    if dir == "DESC" || dir == "ASC" {
                        sort_direction = dir;
                    }
                } else {
                    sort_column = Some(val.to_string());
                }
            }
            "count" => {
                want_count = val == "true";
            }
            _ => {
                // Parse filter[col]=val
                if key.starts_with("filter[") {
                    if let Some(close) = key.find(']') {
                        let col_name = key[7..close].to_string();
                        filters.push((col_name, val.to_string()));
                    }
                }
            }
        }
    }

    if offset > 0 {
        want_count = true;
    }

    ListParams {
        limit,
        offset,
        sort_column,
        sort_direction,
        filters,
        want_count,
    }
}

/// Extract user_id from JWT claims, falling back to "anonymous".
fn user_id_from_req(req: &HttpRequest) -> String {
    req.extensions()
        .get::<Claims>()
        .map(|c| c.user_id().to_string())
        .unwrap_or_else(|| "anonymous".to_string())
}

/// Build a lightweight `ProtTableMeta` for the protection layer from schema columns.
fn prot_meta(table_name: &str, meta: &crate::schema::types::TableMeta) -> ProtTableMeta {
    ProtTableMeta::new(
        table_name,
        meta.columns.iter().map(|c| c.name.clone()).collect(),
    )
}

/// Helper: look up table metadata by name, cloning out of the DashMap guard
/// so the result is `Send` and can be held across `.await` points.
fn lookup_table(table_name: &str) -> Option<crate::schema::types::TableMeta> {
    let registry = ModelRegistry::instance();
    registry.get_table(table_name).map(|r| r.clone())
}

/// True when the configured database engine is Postgres.
fn is_postgres() -> bool {
    Config::global().db_engine_normalised() == "postgres"
}

/// Generate a placeholder for the given index.
fn placeholder(idx: usize, pg: bool) -> String {
    if pg { format!("${}", idx) } else { "?".to_string() }
}

/// Quote a SQL identifier.
fn quote_ident(name: &str, pg: bool) -> String {
    if pg {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

/// Convert a JSON value to a SQL-safe string parameter.
fn json_value_to_sql_string(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Convert a DynamicRow to a serde_json::Value object.
fn row_to_value(row: DynamicRow) -> Value {
    row.into()
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Return cached bytes as an HTTP 200 JSON response.
fn cached_response(bytes: Vec<u8>) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("application/json")
        .body(bytes)
}

/// `GET /api/v1/{table}` -- List records with pagination, filtering, sorting.
pub async fn handle_list(
    req: HttpRequest,
    path: web::Path<String>,
    cache: web::Data<CacheManager>,
) -> HttpResponse {
    let table_name = path.into_inner();

    // Cache check
    let cache_key = CacheManager::build_cache_key("GET", req.path(), req.uri().query());
    if let Some(bytes) = cache.get(&cache_key).await {
        return cached_response(bytes);
    }

    let meta = match lookup_table(&table_name) {
        Some(m) => m,
        None => return response::not_found(&format!("Table '{}' not found", table_name)),
    };

    let params = parse_list_params(&req);
    let conn = db::connection();
    let pg = is_postgres();

    let mut sql = format!("SELECT * FROM {}", quote_ident(&table_name, pg));
    let mut bind_params: Vec<String> = Vec::new();
    let mut idx = 1usize;

    // WHERE filters
    let mut where_clauses: Vec<String> = Vec::new();
    for (col, val) in &params.filters {
        if meta.has_column(col) {
            where_clauses.push(format!(
                "{} = {}",
                quote_ident(col, pg),
                placeholder(idx, pg)
            ));
            bind_params.push(val.clone());
            idx += 1;
        }
    }
    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
    }

    // ORDER BY
    if let Some(ref sort_col) = params.sort_column {
        if meta.has_column(sort_col) {
            sql.push_str(&format!(
                " ORDER BY {} {}",
                quote_ident(sort_col, pg),
                params.sort_direction
            ));
        }
    }

    // LIMIT/OFFSET with +1 for has_more detection
    sql.push_str(&format!(" LIMIT {} OFFSET {}", params.limit + 1, params.offset));

    let param_refs: Vec<&str> = bind_params.iter().map(|s| s.as_str()).collect();

    match db::query_dynamic(conn, &sql, &param_refs).await {
        Ok(rows) => {
            let has_more = rows.len() > params.limit;
            let data: Vec<Value> = rows
                .into_iter()
                .take(params.limit)
                .map(row_to_value)
                .collect();

            let total: i64 = if params.want_count {
                let mut count_sql = format!(
                    "SELECT COUNT(*) FROM {}",
                    quote_ident(&table_name, pg)
                );
                if !where_clauses.is_empty() {
                    count_sql.push_str(" WHERE ");
                    count_sql.push_str(&where_clauses.join(" AND "));
                }
                db::query_scalar_i64(conn, &count_sql, &param_refs).await.unwrap_or(-1)
            } else if !has_more {
                (params.offset + data.len()) as i64
            } else {
                -1
            };

            let result = json!({
                "data": data,
                "meta": {
                    "total": total,
                    "limit": params.limit,
                    "offset": params.offset,
                    "has_more": has_more
                }
            });

            // Serialize once, cache the bytes, return the same bytes
            let bytes = serde_json::to_vec(&result).unwrap_or_default();
            cache.put(&cache_key, bytes.clone(), Duration::from_secs(300)).await;
            cached_response(bytes)
        }
        Err(e) => {
            log::error!("List {} failed: {}", table_name, e);
            response::internal_error(&e.to_string())
        }
    }
}

/// `GET /api/v1/{table}/{id}` -- Get a single record by primary key.
pub async fn handle_get_by_id(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    cache: web::Data<CacheManager>,
) -> HttpResponse {
    let (table_name, id) = path.into_inner();

    // Cache check
    let cache_key = CacheManager::build_cache_key("GET", req.path(), req.uri().query());
    if let Some(bytes) = cache.get(&cache_key).await {
        return cached_response(bytes);
    }

    let meta = match lookup_table(&table_name) {
        Some(m) => m,
        None => return response::not_found(&format!("Table '{}' not found", table_name)),
    };

    if meta.primary_keys.is_empty() {
        return response::bad_request(&format!("Table '{}' has no primary key", table_name));
    }

    let pk = &meta.primary_keys[0];
    let conn = db::connection();
    let pg = is_postgres();

    let sql = format!(
        "SELECT * FROM {} WHERE {} = {} LIMIT 1",
        quote_ident(&table_name, pg),
        quote_ident(pk, pg),
        placeholder(1, pg),
    );

    match db::query_optional_dynamic(conn, &sql, &[id.as_str()]).await {
        Ok(Some(row)) => {
            let value = row_to_value(row);
            let bytes = serde_json::to_vec(&value).unwrap_or_default();
            cache.put(&cache_key, bytes.clone(), Duration::from_secs(300)).await;
            cached_response(bytes)
        }
        Ok(None) => response::not_found(&format!("{} with id '{}' not found", table_name, id)),
        Err(e) => {
            log::error!("Get {}/{} failed: {}", table_name, id, e);
            response::internal_error(&e.to_string())
        }
    }
}

/// `POST /api/v1/{table}` -- Create a new record.
pub async fn handle_create(
    req: HttpRequest,
    path: web::Path<String>,
    body: web::Json<Value>,
    cache: web::Data<CacheManager>,
) -> HttpResponse {
    let table_name = path.into_inner();

    let meta = match lookup_table(&table_name) {
        Some(m) => m,
        None => return response::not_found(&format!("Table '{}' not found", table_name)),
    };

    let input = body.into_inner();
    if !input.is_object() {
        return response::bad_request("Request body must be a JSON object");
    }

    if let Some(errors) = validate_create(&input, &meta) {
        return response::validation_error(errors);
    }

    let mut sanitized = field_guard::sanitize(&table_name, &input);
    let user_id = user_id_from_req(&req);
    let pmeta = prot_meta(&table_name, &meta);
    audit::inject_create(&mut sanitized, &user_id, &pmeta);
    code_gen::inject_code(&mut sanitized, &pmeta);

    let obj = match sanitized.as_object() {
        Some(o) if !o.is_empty() => o,
        _ => return response::bad_request("No valid columns to insert"),
    };

    let conn = db::connection();
    let pg = is_postgres();

    let mut columns = Vec::new();
    let mut placeholders = Vec::new();
    let mut bind_vals: Vec<String> = Vec::new();
    let mut idx = 1usize;

    for (key, val) in obj {
        if meta.has_column(key) {
            columns.push(quote_ident(key, pg));
            placeholders.push(placeholder(idx, pg));
            bind_vals.push(json_value_to_sql_string(val));
            idx += 1;
        }
    }

    if columns.is_empty() {
        return response::bad_request("No valid columns to insert");
    }

    let sql = if pg {
        format!(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
            quote_ident(&table_name, pg),
            columns.join(", "),
            placeholders.join(", "),
        )
    } else {
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_ident(&table_name, pg),
            columns.join(", "),
            placeholders.join(", "),
        )
    };

    let param_refs: Vec<&str> = bind_vals.iter().map(|s| s.as_str()).collect();

    if pg {
        match db::query_optional_dynamic(conn, &sql, &param_refs).await {
            Ok(Some(row)) => {
                cache.invalidate_table(&table_name).await;
                response::created(row_to_value(row))
            }
            Ok(None) => {
                cache.invalidate_table(&table_name).await;
                response::created(json!({"message": "Record created"}))
            }
            Err(e) => handle_insert_error(&table_name, e),
        }
    } else {
        match db::execute_sql(conn, &sql, &param_refs).await {
            Ok(_) => {
                cache.invalidate_table(&table_name).await;
                response::created(json!({"message": "Record created"}))
            }
            Err(e) => handle_insert_error(&table_name, e),
        }
    }
}

fn handle_insert_error(table_name: &str, e: db::DbError) -> HttpResponse {
    let msg = e.to_string();
    let msg_lower = msg.to_lowercase();
    if msg_lower.contains("duplicate")
        || msg_lower.contains("unique")
        || msg_lower.contains("constraint")
    {
        return response::conflict("Record already exists");
    }
    log::error!("Create {} failed: {}", table_name, msg);
    response::internal_error(&msg)
}

/// `PUT /api/v1/{table}/{id}` -- Update an existing record.
pub async fn handle_update(
    req: HttpRequest,
    path: web::Path<(String, String)>,
    body: web::Json<Value>,
    cache: web::Data<CacheManager>,
) -> HttpResponse {
    let (table_name, id) = path.into_inner();

    let meta = match lookup_table(&table_name) {
        Some(m) => m,
        None => return response::not_found(&format!("Table '{}' not found", table_name)),
    };

    if meta.primary_keys.is_empty() {
        return response::bad_request(&format!("Table '{}' has no primary key", table_name));
    }

    let input = body.into_inner();
    if !input.is_object() {
        return response::bad_request("Request body must be a JSON object");
    }

    if let Some(errors) = validate_update(&input, &meta) {
        return response::validation_error(errors);
    }

    let mut sanitized = field_guard::sanitize(&table_name, &input);
    let user_id = user_id_from_req(&req);
    let pmeta = prot_meta(&table_name, &meta);
    audit::inject_update(&mut sanitized, &user_id, &pmeta);

    let obj = match sanitized.as_object() {
        Some(o) if !o.is_empty() => o,
        _ => return response::bad_request("No valid columns to update"),
    };

    let conn = db::connection();
    let pg = is_postgres();
    let pk_column = &meta.primary_keys[0];

    let mut set_clauses = Vec::new();
    let mut bind_vals: Vec<String> = Vec::new();
    let mut idx = 1usize;

    for (key, val) in obj {
        if meta.has_column(key) {
            set_clauses.push(format!(
                "{} = {}",
                quote_ident(key, pg),
                placeholder(idx, pg)
            ));
            bind_vals.push(json_value_to_sql_string(val));
            idx += 1;
        }
    }

    if set_clauses.is_empty() {
        return response::bad_request("No valid columns to update");
    }

    let sql = if pg {
        format!(
            "UPDATE {} SET {} WHERE {} = {} RETURNING *",
            quote_ident(&table_name, pg),
            set_clauses.join(", "),
            quote_ident(pk_column, pg),
            placeholder(idx, pg),
        )
    } else {
        format!(
            "UPDATE {} SET {} WHERE {} = ?",
            quote_ident(&table_name, pg),
            set_clauses.join(", "),
            quote_ident(pk_column, pg),
        )
    };

    bind_vals.push(id.clone());
    let param_refs: Vec<&str> = bind_vals.iter().map(|s| s.as_str()).collect();

    if pg {
        match db::query_optional_dynamic(conn, &sql, &param_refs).await {
            Ok(Some(row)) => {
                cache.invalidate_table(&table_name).await;
                response::ok(row_to_value(row))
            }
            Ok(None) => {
                response::not_found(&format!("{} with id '{}' not found", table_name, id))
            }
            Err(e) => {
                log::error!("Update {}/{} failed: {}", table_name, id, e);
                response::internal_error(&e.to_string())
            }
        }
    } else {
        match db::execute_sql(conn, &sql, &param_refs).await {
            Ok(n) if n > 0 => {
                cache.invalidate_table(&table_name).await;
                response::ok(json!({"message": "Record updated"}))
            }
            Ok(_) => {
                response::not_found(&format!("{} with id '{}' not found", table_name, id))
            }
            Err(e) => {
                log::error!("Update {}/{} failed: {}", table_name, id, e);
                response::internal_error(&e.to_string())
            }
        }
    }
}

/// `DELETE /api/v1/{table}/{id}` -- Delete a record.
pub async fn handle_delete(
    _req: HttpRequest,
    path: web::Path<(String, String)>,
    cache: web::Data<CacheManager>,
) -> HttpResponse {
    let (table_name, id) = path.into_inner();

    let meta = match lookup_table(&table_name) {
        Some(m) => m,
        None => return response::not_found(&format!("Table '{}' not found", table_name)),
    };

    if meta.primary_keys.is_empty() {
        return response::bad_request(&format!("Table '{}' has no primary key", table_name));
    }

    let pk = &meta.primary_keys[0];
    let conn = db::connection();
    let pg = is_postgres();

    let sql = if pg {
        format!(
            "DELETE FROM {} WHERE {} = $1 RETURNING *",
            quote_ident(&table_name, pg),
            quote_ident(pk, pg),
        )
    } else {
        format!(
            "DELETE FROM {} WHERE {} = ?",
            quote_ident(&table_name, pg),
            quote_ident(pk, pg),
        )
    };

    if pg {
        match db::query_optional_dynamic(conn, &sql, &[id.as_str()]).await {
            Ok(Some(row)) => {
                cache.invalidate_table(&table_name).await;
                response::ok(row_to_value(row))
            }
            Ok(None) => {
                response::not_found(&format!("{} with id '{}' not found", table_name, id))
            }
            Err(e) => {
                log::error!("Delete {}/{} failed: {}", table_name, id, e);
                response::internal_error(&e.to_string())
            }
        }
    } else {
        match db::execute_sql(conn, &sql, &[id.as_str()]).await {
            Ok(n) if n > 0 => {
                cache.invalidate_table(&table_name).await;
                response::ok(json!({"message": "Record deleted"}))
            }
            Ok(_) => {
                response::not_found(&format!("{} with id '{}' not found", table_name, id))
            }
            Err(e) => {
                log::error!("Delete {}/{} failed: {}", table_name, id, e);
                response::internal_error(&e.to_string())
            }
        }
    }
}

/// `POST /api/v1/{table}/bulk` -- Bulk create records.
pub async fn handle_bulk_create(
    req: HttpRequest,
    path: web::Path<String>,
    body: web::Json<Value>,
    cache: web::Data<CacheManager>,
) -> HttpResponse {
    let table_name = path.into_inner();

    let meta = match lookup_table(&table_name) {
        Some(m) => m,
        None => return response::not_found(&format!("Table '{}' not found", table_name)),
    };

    let input = body.into_inner();
    let items = match input.as_array() {
        Some(arr) if !arr.is_empty() => arr,
        _ => return response::bad_request("Bulk create requires a non-empty JSON array"),
    };

    let conn = db::connection();
    let pg = is_postgres();
    let user_id = user_id_from_req(&req);
    let pmeta = prot_meta(&table_name, &meta);

    let mut created_items: Vec<Value> = Vec::new();
    let mut errors: Vec<Value> = Vec::new();
    let total_submitted = items.len();

    for (i, item) in items.iter().enumerate() {
        if !item.is_object() {
            errors.push(json!({"index": i, "message": "Item must be a JSON object"}));
            continue;
        }

        if let Some(validation_errors) = validate_create(item, &meta) {
            errors.push(json!({"index": i, "errors": validation_errors}));
            continue;
        }

        let mut sanitized = field_guard::sanitize(&table_name, item);
        audit::inject_create(&mut sanitized, &user_id, &pmeta);
        code_gen::inject_code(&mut sanitized, &pmeta);

        let obj = match sanitized.as_object() {
            Some(o) if !o.is_empty() => o.clone(),
            _ => {
                errors.push(json!({"index": i, "message": "No valid columns"}));
                continue;
            }
        };

        let mut columns = Vec::new();
        let mut placeholders = Vec::new();
        let mut bind_vals: Vec<String> = Vec::new();
        let mut idx = 1usize;

        for (key, val) in &obj {
            if meta.has_column(key) {
                columns.push(quote_ident(key, pg));
                placeholders.push(placeholder(idx, pg));
                bind_vals.push(json_value_to_sql_string(val));
                idx += 1;
            }
        }

        if columns.is_empty() {
            errors.push(json!({"index": i, "message": "No valid columns"}));
            continue;
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_ident(&table_name, pg),
            columns.join(", "),
            placeholders.join(", "),
        );

        let param_refs: Vec<&str> = bind_vals.iter().map(|s| s.as_str()).collect();

        match db::execute_sql(conn, &sql, &param_refs).await {
            Ok(_) => created_items.push(json!({"message": "Record created"})),
            Err(e) => errors.push(json!({"index": i, "message": e.to_string()})),
        }
    }

    cache.invalidate_table(&table_name).await;

    let total_created = created_items.len();
    let result = json!({
        "created": created_items,
        "errors": errors,
        "total_submitted": total_submitted,
        "total_created": total_created
    });

    if errors.is_empty() {
        response::created(result)
    } else {
        response::ok(result)
    }
}

// ---------------------------------------------------------------------------
// Route configuration
// ---------------------------------------------------------------------------

/// Register all Blueprint CRUD routes.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg
        .route("/api/v1/{table}/bulk", web::post().to(handle_bulk_create))
        .route("/api/v1/{table}/{id}", web::get().to(handle_get_by_id))
        .route("/api/v1/{table}/{id}", web::put().to(handle_update))
        .route("/api/v1/{table}/{id}", web::delete().to(handle_delete))
        .route("/api/v1/{table}", web::get().to(handle_list))
        .route("/api/v1/{table}", web::post().to(handle_create));
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

fn validate_create(
    data: &Value,
    meta: &crate::schema::types::TableMeta,
) -> Option<Value> {
    let obj = data.as_object()?;
    let mut errors = Vec::new();

    for col in &meta.columns {
        if col.is_auto_increment
            || col.is_primary_key
            || col.default_value.is_some()
            || col.is_nullable
            || crate::schema::types::TableMeta::is_generic_field(&col.name)
        {
            continue;
        }

        if !obj.contains_key(&col.name) {
            errors.push(json!({
                "field": col.name,
                "code": "required",
                "message": format!("'{}' is required", col.name)
            }));
        }
    }

    if errors.is_empty() {
        None
    } else {
        Some(Value::Array(errors))
    }
}

fn validate_update(
    data: &Value,
    meta: &crate::schema::types::TableMeta,
) -> Option<Value> {
    let obj = match data.as_object() {
        Some(o) if !o.is_empty() => o,
        _ => {
            return Some(
                json!([{"field": "_body", "code": "empty", "message": "Update body is empty"}]),
            )
        }
    };

    let mut errors = Vec::new();

    for (key, _val) in obj {
        if !meta.has_column(key) {
            errors.push(json!({
                "field": key,
                "code": "unknown_column",
                "message": format!("Column '{}' does not exist on table", key)
            }));
        }
    }

    if errors.is_empty() {
        None
    } else {
        Some(Value::Array(errors))
    }
}
