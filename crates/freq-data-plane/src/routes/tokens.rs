use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

#[derive(Deserialize)]
struct CreateTokenRequest {
    label: String,
}

#[derive(Serialize)]
struct CreateTokenResponse {
    token_id: i64,
    label: String,
    raw_token: String,
}

#[derive(Serialize)]
struct TokenInfo {
    token_id: i64,
    label: String,
    created_at: String,
    revoked: bool,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/tokens", get(list).post(create))
        .route(
            "/api/tokens/{token_id}",
            axum::routing::delete(revoke),
        )
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(auth): ApiBearerOrSession,
    Json(payload): Json<CreateTokenRequest>,
) -> Result<Json<CreateTokenResponse>, StatusCode> {
    let label = payload.label.trim().to_string();
    if label.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let conn = state.db.lock().unwrap();
    let created = user_database::create_api_token(&conn, auth.user_id, &label)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    info!(username = %auth.username, label = %label, "api token created");

    Ok(Json(CreateTokenResponse {
        token_id: created.token.token_id,
        label: created.token.label,
        raw_token: created.raw_token,
    }))
}

async fn list(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(auth): ApiBearerOrSession,
) -> Result<Json<Vec<TokenInfo>>, StatusCode> {
    let conn = state.db.lock().unwrap();
    let tokens = user_database::list_api_tokens(&conn, auth.user_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(
        tokens
            .into_iter()
            .map(|t| TokenInfo {
                token_id: t.token_id,
                label: t.label,
                created_at: t.created_at,
                revoked: t.revoked_at.is_some(),
            })
            .collect(),
    ))
}

async fn revoke(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(auth): ApiBearerOrSession,
    Path(token_id): Path<i64>,
) -> StatusCode {
    let conn = state.db.lock().unwrap();
    match user_database::revoke_api_token(&conn, token_id, auth.user_id) {
        Ok(true) => {
            info!(username = %auth.username, token_id, "api token revoked");
            StatusCode::NO_CONTENT
        }
        Ok(false) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
