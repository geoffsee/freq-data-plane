use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use data_sdk::{NewRole, Role};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct GrantRoleRequest {
    role_key: String,
    granted_by_principal_key: Option<String>,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/roles", post(create))
        .route("/roles/{role_key}", get(get_by_key))
        .route(
            "/principals/{principal_key}/roles",
            get(list_for_principal).post(grant_to_principal),
        )
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewRole>,
) -> Result<(StatusCode, Json<Role>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let role = cp.create_role(&body)?;
    Ok((StatusCode::CREATED, Json(role)))
}

async fn get_by_key(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(role_key): Path<String>,
) -> Result<Json<Role>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let role = cp.require_role(&role_key)?;
    Ok(Json(role))
}

async fn list_for_principal(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(principal_key): Path<String>,
) -> Result<Json<Vec<Role>>, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let principal = cp.require_principal(&principal_key)?;
    let roles = cp.list_roles_for_principal(principal.principal_id)?;
    Ok(Json(roles))
}

async fn grant_to_principal(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Path(principal_key): Path<String>,
    Json(body): Json<GrantRoleRequest>,
) -> Result<StatusCode, ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let principal = cp.require_principal(&principal_key)?;
    let role = cp.require_role(&body.role_key)?;
    let granted_by = match &body.granted_by_principal_key {
        Some(key) => Some(cp.require_principal(key)?.principal_id),
        None => None,
    };
    cp.grant_role(principal.principal_id, role.role_id, granted_by)?;
    Ok(StatusCode::CREATED)
}
