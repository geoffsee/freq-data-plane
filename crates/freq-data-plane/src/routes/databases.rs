use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use data_sdk::{
    AppliedMigration, DatabaseBinding, DatabaseKind, DatabaseRef, DatabaseRefStatus,
    NewAppliedMigration, NewDatabaseBinding, NewDatabaseRef,
};
use serde::Deserialize;
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/databases", post(create).get(list))
        .route("/databases/provision", post(provision))
        .route("/databases/{database_key}", get(get_by_key).delete(archive))
        .route("/databases/{database_key}/status", axum::routing::put(update_status))
        .route("/databases/{database_key}/migrate", post(migrate))
        .route("/databases/{database_key}/migrations", get(list_migrations))
        .route("/deployments/{deployment_key}/configure", post(configure))
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewDatabaseRef>,
) -> Result<(StatusCode, Json<DatabaseRef>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let db_ref = cp.create_database_ref(&body)?;
    Ok((StatusCode::CREATED, Json(db_ref)))
}

async fn list(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
) -> Result<Json<Vec<DatabaseRef>>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let refs = cp.list_database_refs()?;
    Ok(Json(refs))
}

async fn get_by_key(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(database_key): Path<String>,
) -> Result<Json<DatabaseRef>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let db_ref = cp.require_database_ref(&database_key)?;
    Ok(Json(db_ref))
}

#[derive(Deserialize)]
struct StatusUpdate {
    status: String,
}

async fn update_status(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(database_key): Path<String>,
    Json(body): Json<StatusUpdate>,
) -> Result<Json<DatabaseRef>, ApiError> {
    let status = DatabaseRefStatus::parse(&body.status)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let cp = state.control_plane.lock().unwrap();
    cp.update_database_ref_status(&database_key, status)?;
    let db_ref = cp.require_database_ref(&database_key)?;
    Ok(Json(db_ref))
}

async fn archive(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(database_key): Path<String>,
) -> Result<StatusCode, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    cp.require_database_ref(&database_key)?;
    cp.update_database_ref_status(&database_key, DatabaseRefStatus::Deleted)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ProvisionRequest {
    database_key: String,
    database_name: String,
    #[serde(rename = "type")]
    database_type: String,
}

async fn provision(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<ProvisionRequest>,
) -> Result<(StatusCode, Json<DatabaseRef>), ApiError> {
    let kind = match body.database_type.as_str() {
        "duckdb" => DatabaseKind::DuckdbFile,
        "sqlite" => DatabaseKind::SqliteFile,
        "postgres" => {
            return Err(ApiError::BadRequest(
                "postgres provisioning is not yet supported".to_string(),
            ))
        }
        other => {
            return Err(ApiError::BadRequest(format!(
                "unsupported database type: {other}"
            )))
        }
    };

    let ext = match kind {
        DatabaseKind::DuckdbFile => "duckdb",
        DatabaseKind::SqliteFile => "sqlite",
        _ => unreachable!(),
    };

    let db_dir = std::path::Path::new(&state.data_dir)
        .join(&state.tenant_key)
        .join("databases");
    std::fs::create_dir_all(&db_dir)
        .map_err(|e| ApiError::Internal(format!("failed to create data directory: {e}")))?;

    let db_path = db_dir.join(format!("{}.{ext}", body.database_key));
    let uri = db_path.to_string_lossy().to_string();

    // Create the actual database file
    match kind {
        DatabaseKind::DuckdbFile => {
            let conn = duckdb::Connection::open(&db_path)
                .map_err(|e| ApiError::Internal(format!("failed to create DuckDB file: {e}")))?;
            drop(conn);
        }
        DatabaseKind::SqliteFile => {
            // Create an empty file — SQLite will initialize it on first use
            std::fs::File::create(&db_path)
                .map_err(|e| ApiError::Internal(format!("failed to create SQLite file: {e}")))?;
        }
        _ => unreachable!(),
    }

    let cp = state.control_plane.lock().unwrap();
    let db_ref = cp.create_database_ref(&NewDatabaseRef {
        database_key: body.database_key,
        database_name: body.database_name,
        database_kind: kind,
        uri,
        attach_alias: None,
    })?;

    Ok((StatusCode::CREATED, Json(db_ref)))
}

#[derive(Clone, Deserialize)]
struct MigrationEntry {
    version: String,
    name: String,
    sql: String,
    checksum: String,
}

#[derive(Deserialize)]
struct MigrateRequest {
    migrations: Vec<MigrationEntry>,
}

#[derive(serde::Serialize)]
struct MigrateResponse {
    applied: Vec<AppliedMigration>,
    skipped: usize,
}

async fn migrate(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(database_key): Path<String>,
    Json(body): Json<MigrateRequest>,
) -> Result<Json<MigrateResponse>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let db_ref = cp.require_database_ref(&database_key)?;

    let already_applied = cp.list_applied_migrations(db_ref.database_ref_id)?;
    let applied_versions: std::collections::HashSet<String> =
        already_applied.into_iter().map(|m| m.version).collect();

    let mut applied = Vec::new();
    let mut skipped = 0usize;

    // Sort migrations by version
    let mut migrations = body.migrations;
    migrations.sort_by(|a, b| a.version.cmp(&b.version));

    for entry in &migrations {
        if applied_versions.contains(&entry.version) {
            skipped += 1;
            continue;
        }

        let record = cp.record_applied_migration(&NewAppliedMigration {
            database_ref_id: db_ref.database_ref_id,
            version: entry.version.clone(),
            name: entry.name.clone(),
            checksum: entry.checksum.clone(),
        })?;
        applied.push(record);
    }

    Ok(Json(MigrateResponse { applied, skipped }))
}

async fn list_migrations(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(database_key): Path<String>,
) -> Result<Json<Vec<AppliedMigration>>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let db_ref = cp.require_database_ref(&database_key)?;
    let migrations = cp.list_applied_migrations(db_ref.database_ref_id)?;
    Ok(Json(migrations))
}

// -- Configure endpoint (freq.toml orchestration) --

#[derive(Deserialize)]
struct SqlDbEntry {
    #[serde(rename = "type")]
    db_type: String,
    binding: String,
    #[serde(default)]
    migrations: Vec<MigrationEntry>,
}

#[derive(Deserialize)]
struct ConfigureRequest {
    sql_db: Vec<SqlDbEntry>,
}

#[derive(serde::Serialize)]
struct ConfigureResult {
    databases: Vec<DatabaseRef>,
    bindings: Vec<DatabaseBinding>,
    migrations_applied: usize,
}

async fn configure(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(deployment_key): Path<String>,
    Json(body): Json<ConfigureRequest>,
) -> Result<Json<ConfigureResult>, ApiError> {
    let mut databases = Vec::new();
    let mut bindings = Vec::new();
    let mut migrations_applied = 0usize;

    for entry in &body.sql_db {
        let kind = match entry.db_type.as_str() {
            "duckdb" => DatabaseKind::DuckdbFile,
            "sqlite" => DatabaseKind::SqliteFile,
            "postgres" => {
                return Err(ApiError::BadRequest(
                    "postgres provisioning is not yet supported".to_string(),
                ))
            }
            other => {
                return Err(ApiError::BadRequest(format!(
                    "unsupported database type: {other}"
                )))
            }
        };

        // Derive database_key from binding name (lowercase)
        let database_key = format!("{}-{}", deployment_key, entry.binding.to_lowercase());

        // Check if database already exists (scoped lock)
        let existing = {
            let cp = state.control_plane.lock().unwrap();
            cp.get_database_ref(&database_key)?
        };

        // Provision if not already existing
        let db_ref = match existing {
            Some(existing) => existing,
            None => {
                let ext = match kind {
                    DatabaseKind::DuckdbFile => "duckdb",
                    DatabaseKind::SqliteFile => "sqlite",
                    _ => unreachable!(),
                };

                let db_dir = std::path::Path::new(&state.data_dir)
                    .join(&state.tenant_key)
                    .join("databases");
                std::fs::create_dir_all(&db_dir).map_err(|e| {
                    ApiError::Internal(format!("failed to create data directory: {e}"))
                })?;

                let db_path = db_dir.join(format!("{database_key}.{ext}"));
                let uri = db_path.to_string_lossy().to_string();

                match kind {
                    DatabaseKind::DuckdbFile => {
                        let conn = duckdb::Connection::open(&db_path).map_err(|e| {
                            ApiError::Internal(format!("failed to create DuckDB file: {e}"))
                        })?;
                        drop(conn);
                    }
                    DatabaseKind::SqliteFile => {
                        std::fs::File::create(&db_path).map_err(|e| {
                            ApiError::Internal(format!("failed to create SQLite file: {e}"))
                        })?;
                    }
                    _ => unreachable!(),
                }

                let cp = state.control_plane.lock().unwrap();
                cp.create_database_ref(&NewDatabaseRef {
                    database_key: database_key.clone(),
                    database_name: entry.binding.clone(),
                    database_kind: kind,
                    uri,
                    attach_alias: None,
                })?
            }
        };

        // Migrations + bindings (scoped lock)
        {
            let cp = state.control_plane.lock().unwrap();

            // Run migrations (record in control plane)
            if !entry.migrations.is_empty() {
                let already = cp.list_applied_migrations(db_ref.database_ref_id)?;
                let applied_versions: std::collections::HashSet<String> =
                    already.into_iter().map(|m| m.version).collect();

                let mut sorted = entry.migrations.clone();
                sorted.sort_by(|a, b| a.version.cmp(&b.version));

                for m in &sorted {
                    if applied_versions.contains(&m.version) {
                        continue;
                    }
                    cp.record_applied_migration(&NewAppliedMigration {
                        database_ref_id: db_ref.database_ref_id,
                        version: m.version.clone(),
                        name: m.name.clone(),
                        checksum: m.checksum.clone(),
                    })?;
                    migrations_applied += 1;
                }
            }

            // Create binding (idempotent — skip if already exists)
            let existing_bindings = cp.list_bindings_for_deployment(&deployment_key)?;
            let already_bound = existing_bindings
                .iter()
                .any(|b| b.binding_name == entry.binding);

            if !already_bound {
                let binding = cp.create_database_binding(&NewDatabaseBinding {
                    database_ref_id: db_ref.database_ref_id,
                    deployment_key: deployment_key.clone(),
                    binding_name: entry.binding.clone(),
                })?;
                bindings.push(binding);
            }
        }

        databases.push(db_ref);
    }

    Ok(Json(ConfigureResult {
        databases,
        bindings,
        migrations_applied,
    }))
}
