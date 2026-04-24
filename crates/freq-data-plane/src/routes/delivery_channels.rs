use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::post,
    Json, Router,
};
use data_sdk::{DeliveryChannel, NewDeliveryChannel};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/delivery-channels", post(create))
        .route(
            "/delivery-channels/{channel_key}",
            axum::routing::get(get_by_key),
        )
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewDeliveryChannel>,
) -> Result<(StatusCode, Json<DeliveryChannel>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let channel = cp.create_delivery_channel(&body)?;
    Ok((StatusCode::CREATED, Json(channel)))
}

async fn get_by_key(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(channel_key): Path<String>,
) -> Result<Json<DeliveryChannel>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let channel = cp.require_delivery_channel(&channel_key)?;
    Ok(Json(channel))
}
