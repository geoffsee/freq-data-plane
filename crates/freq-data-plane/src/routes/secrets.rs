use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::post,
    Json, Router,
};
use data_sdk::{NewSecretHandle, SecretHandle};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/secrets", post(create))
        .route("/secrets/{secret_key}", axum::routing::get(get_by_key))
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewSecretHandle>,
) -> Result<(StatusCode, Json<SecretHandle>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let secret = cp.create_secret(&body)?;
    Ok((StatusCode::CREATED, Json(secret)))
}

async fn get_by_key(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(secret_key): Path<String>,
) -> Result<Json<SecretHandle>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let secret = cp.require_secret(&secret_key)?;
    Ok(Json(secret))
}
