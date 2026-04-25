use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use data_sdk::{Delivery, NewDelivery};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/deliveries", get(list).post(create))
        .route("/deliveries/{delivery_key}", get(get_by_key))
}

async fn list(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
) -> Result<Json<Vec<Delivery>>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let deliveries = cp.list_deliveries()?;
    Ok(Json(deliveries))
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewDelivery>,
) -> Result<(StatusCode, Json<Delivery>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let delivery = cp.create_delivery(&body)?;
    Ok((StatusCode::CREATED, Json(delivery)))
}

async fn get_by_key(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(delivery_key): Path<String>,
) -> Result<Json<Delivery>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let delivery = cp.require_delivery(&delivery_key)?;
    Ok(Json(delivery))
}
