use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use data_sdk::{AuditLogEntry, NewAuditLogEntry};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct AuditListQuery {
    limit: Option<i64>,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/audit", get(list).post(create))
}

async fn list(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Query(params): Query<AuditListQuery>,
) -> Result<Json<Vec<AuditLogEntry>>, ApiError> {
    let limit = params.limit.unwrap_or(50).min(500);
    let cp = state.control_plane.lock().unwrap();
    let entries = cp.list_audit_entries(limit)?;
    Ok(Json(entries))
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
