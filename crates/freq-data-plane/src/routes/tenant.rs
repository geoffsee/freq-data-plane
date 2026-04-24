use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{extract::State, routing::get, Json, Router};
use data_sdk::{TenantProfile, TenantStatus};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct UpdateTenantStatusRequest {
    status: TenantStatus,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/tenant", get(get_tenant).put(update_tenant_status))
}

async fn get_tenant(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
) -> Result<Json<TenantProfile>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let tenant = cp.require_tenant(&state.tenant_key)?;
    Ok(Json(tenant))
}

async fn update_tenant_status(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<UpdateTenantStatusRequest>,
) -> Result<Json<TenantProfile>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    cp.update_tenant_status(&state.tenant_key, body.status)?;
    let tenant = cp.require_tenant(&state.tenant_key)?;
    Ok(Json(tenant))
}
