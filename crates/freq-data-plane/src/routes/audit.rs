use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use data_sdk::{AuditLogEntry, NewAuditLogEntry};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/audit", post(create))
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewAuditLogEntry>,
) -> Result<(StatusCode, Json<AuditLogEntry>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let entry = cp.create_audit_entry(&body)?;
    Ok((StatusCode::CREATED, Json(entry)))
}
