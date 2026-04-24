use crate::state::AppState;
use axum::{http::header, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;
use std::sync::Arc;

const OPENAPI_YAML: &str = include_str!("../../../../openapi.yaml");

#[derive(Serialize)]
struct HealthResponse {
    status: String,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/health", get(health))
        .route("/openapi.yaml", get(openapi_yaml))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

async fn openapi_yaml() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/yaml")], OPENAPI_YAML)
}
