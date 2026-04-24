use crate::error::ApiError;
use crate::extractors::ApiBearerOrSession;
use crate::state::AppState;
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use data_sdk::{NewSession, Session as ControlSession};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/sessions", post(create))
}

async fn create(
    State(state): State<Arc<AppState>>,
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(body): Json<NewSession>,
) -> Result<(StatusCode, Json<ControlSession>), ApiError> {
    let cp = state.control_plane.lock().unwrap();
    let session = cp.create_session(&body)?;
    Ok((StatusCode::CREATED, Json(session)))
}
