use crate::state::AppState;
use axum::{routing::get, Json, Router};
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize)]
struct HealthResponse {
    status: String,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/health", get(health))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}
