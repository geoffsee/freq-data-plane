use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::post,
    Json, Router,
};
use data_sdk::{NewSchemaMapping, SchemaMapping};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/mappings", post(create))
        .route("/mappings/{mapping_key}", axum::routing::get(get_by_key))
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewSchemaMapping>,
) -> Result<(StatusCode, Json<SchemaMapping>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let mapping = cp.create_schema_mapping(&body)?;
    Ok((StatusCode::CREATED, Json(mapping)))
}

async fn get_by_key(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(mapping_key): Path<String>,
) -> Result<Json<SchemaMapping>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let mapping = cp.require_schema_mapping(&mapping_key)?;
    Ok(Json(mapping))
}
