use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use data_sdk::{NewPrincipal, Principal};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/principals", get(list).post(create))
        .route("/principals/{principal_key}", get(get_by_key))
}

async fn list(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
) -> Result<Json<Vec<Principal>>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let principals = cp.list_principals()?;
    Ok(Json(principals))
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewPrincipal>,
) -> Result<(StatusCode, Json<Principal>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let principal = cp.create_principal(&body)?;
    Ok((StatusCode::CREATED, Json(principal)))
}

async fn get_by_key(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(principal_key): Path<String>,
) -> Result<Json<Principal>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let principal = cp.require_principal(&principal_key)?;
    Ok(Json(principal))
}
