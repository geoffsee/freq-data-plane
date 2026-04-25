use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use data_sdk::{Asset, NewAsset};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/assets", get(list).post(create))
        .route("/assets/{asset_key}", get(get_by_key))
}

async fn list(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
) -> Result<Json<Vec<Asset>>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let assets = cp.list_assets()?;
    Ok(Json(assets))
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewAsset>,
) -> Result<(StatusCode, Json<Asset>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let asset = cp.create_asset(&body)?;
    Ok((StatusCode::CREATED, Json(asset)))
}

async fn get_by_key(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(asset_key): Path<String>,
) -> Result<Json<Asset>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let asset = cp.require_asset(&asset_key)?;
    Ok(Json(asset))
}
