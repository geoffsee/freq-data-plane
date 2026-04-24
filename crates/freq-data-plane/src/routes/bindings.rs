use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use data_sdk::{DatabaseBinding, NewDatabaseBinding};
use serde::Deserialize;
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/databases/{database_key}/bindings",
            post(create_binding).get(list_for_database),
        )
        .route(
            "/deployments/{deployment_key}/bindings",
            get(list_for_deployment),
        )
        .route("/bindings/{binding_id}", axum::routing::delete(delete))
}

#[derive(Deserialize)]
struct CreateBindingBody {
    deployment_key: String,
    binding_name: String,
}

async fn create_binding(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(database_key): Path<String>,
    Json(body): Json<CreateBindingBody>,
) -> Result<(StatusCode, Json<DatabaseBinding>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let db_ref = cp.require_database_ref(&database_key)?;
    let binding = cp.create_database_binding(&NewDatabaseBinding {
        database_ref_id: db_ref.database_ref_id,
        deployment_key: body.deployment_key,
        binding_name: body.binding_name,
    })?;
    Ok((StatusCode::CREATED, Json(binding)))
}

async fn list_for_database(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(database_key): Path<String>,
) -> Result<Json<Vec<DatabaseBinding>>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let db_ref = cp.require_database_ref(&database_key)?;
    let bindings = cp.list_bindings_for_database(db_ref.database_ref_id)?;
    Ok(Json(bindings))
}

async fn list_for_deployment(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(deployment_key): Path<String>,
) -> Result<Json<Vec<DatabaseBinding>>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let bindings = cp.list_bindings_for_deployment(&deployment_key)?;
    Ok(Json(bindings))
}

async fn delete(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(binding_id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    cp.delete_database_binding(binding_id)?;
    Ok(StatusCode::NO_CONTENT)
}
