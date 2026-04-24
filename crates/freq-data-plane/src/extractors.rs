use crate::state::AppState;
use axum::{
    http::StatusCode,
    response::Redirect,
};
use std::sync::Arc;
use tower_sessions::Session;

pub const SESSION_USER_KEY: &str = "username";

pub struct AuthUser {
    pub username: String,
    pub user_id: i64,
}

pub struct ApiBearerOrSession(pub AuthUser);

impl axum::extract::FromRequestParts<Arc<AppState>> for ApiBearerOrSession {
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        if let Some(auth_header) = parts.headers.get("authorization") {
            if let Ok(value) = auth_header.to_str()
                && let Some(token) = value.strip_prefix("Bearer ")
            {
                let conn = state.db.lock().unwrap();
                if let Ok(Some(user)) = user_database::authenticate_token(&conn, token.trim()) {
                    return Ok(ApiBearerOrSession(AuthUser {
                        username: user.username,
                        user_id: user.user_id,
                    }));
                }
            }
            return Err(StatusCode::UNAUTHORIZED);
        }

        let session = Session::from_request_parts(parts, state)
            .await
            .map_err(|_| StatusCode::UNAUTHORIZED)?;

        let username: Option<String> = session.get(SESSION_USER_KEY).await.unwrap_or(None);
        let username = username.ok_or(StatusCode::UNAUTHORIZED)?;

        let conn = state.db.lock().unwrap();
        let user = user_database::get_user_by_username(&conn, &username)
            .map_err(|_| StatusCode::UNAUTHORIZED)?
            .ok_or(StatusCode::UNAUTHORIZED)?;

        Ok(ApiBearerOrSession(AuthUser {
            username: user.username,
            user_id: user.user_id,
        }))
    }
}

impl axum::extract::FromRequestParts<Arc<AppState>> for AuthUser {
    type Rejection = Redirect;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        if let Some(auth_header) = parts.headers.get("authorization") {
            if let Ok(value) = auth_header.to_str()
                && let Some(token) = value.strip_prefix("Bearer ")
            {
                let conn = state.db.lock().unwrap();
                if let Ok(Some(user)) = user_database::authenticate_token(&conn, token.trim()) {
                    return Ok(AuthUser {
                        username: user.username,
                        user_id: user.user_id,
                    });
                }
            }
            return Err(Redirect::to("/login"));
        }

        let session = Session::from_request_parts(parts, state)
            .await
            .map_err(|_| Redirect::to("/login"))?;

        let username: Option<String> = session.get(SESSION_USER_KEY).await.unwrap_or(None);
        let username = username.ok_or_else(|| Redirect::to("/login"))?;

        let conn = state.db.lock().unwrap();
        let user = user_database::get_user_by_username(&conn, &username)
            .map_err(|_| Redirect::to("/login"))?
            .ok_or_else(|| Redirect::to("/login"))?;

        Ok(AuthUser {
            username: user.username,
            user_id: user.user_id,
        })
    }
}
