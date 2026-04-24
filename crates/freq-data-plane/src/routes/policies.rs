use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::post,
    Json, Router,
};
use data_sdk::{AccessPolicy, NewAccessPolicy};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/policies", post(create))
        .route("/policies/{policy_key}", axum::routing::get(get_by_key))
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewAccessPolicy>,
) -> Result<(StatusCode, Json<AccessPolicy>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let policy = cp.create_access_policy(&body)?;
    Ok((StatusCode::CREATED, Json(policy)))
}

async fn get_by_key(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(policy_key): Path<String>,
) -> Result<Json<AccessPolicy>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let policy = cp.require_access_policy(&policy_key)?;
    Ok(Json(policy))
}
