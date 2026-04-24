use crate::extractors::{ApiBearerOrSession, AuthUser, SESSION_USER_KEY};
use crate::state::AppState;
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Form, Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_sessions::Session;
use tracing::info;

const LOGIN_HTML: &str = include_str!("../../templates/login.html");
const LOGIN_FAILED_HTML: &str = include_str!("../../templates/login_failed.html");
const DASHBOARD_HTML: &str = include_str!("../../templates/dashboard.html");
const CONTROL_PLANE_HTML: &str = include_str!("../../templates/control_plane.html");

#[derive(Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct CreateMessageRequest {
    name: String,
}

#[derive(Serialize)]
struct CreateMessageResponse {
    message: String,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/login", get(login_page).post(login_submit))
        .route("/logout", post(logout))
        .route("/", get(dashboard))
        .route("/control-plane", get(control_plane_page))
        .route("/hello", post(create_message))
}

async fn login_page(session: Session) -> impl IntoResponse {
    if let Ok(Some(_)) = session.get::<String>(SESSION_USER_KEY).await {
        return Redirect::to("/").into_response();
    }
    Html(LOGIN_HTML).into_response()
}

async fn login_submit(
    State(state): State<Arc<AppState>>,
    session: Session,
    Form(form): Form<LoginForm>,
) -> impl IntoResponse {
    let username = form.username.trim().to_string();

    let user = {
        let conn = state.db.lock().unwrap();
        user_database::authenticate(&conn, &username, &form.password)
    };

    match user {
        Ok(Some(u)) => {
            info!(username = %u.username, "user logged in");
            session.insert(SESSION_USER_KEY, &u.username).await.unwrap();
            Redirect::to("/").into_response()
        }
        _ => Html(LOGIN_FAILED_HTML).into_response(),
    }
}

async fn logout(session: Session) -> impl IntoResponse {
    session.flush().await.unwrap();
    Redirect::to("/login")
}

async fn dashboard(
    auth: AuthUser,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    Html(
        DASHBOARD_HTML
            .replace("{{APP_NAME}}", &state.app_name)
            .replace("{{USERNAME}}", &auth.username),
    )
}

async fn control_plane_page(
    auth: AuthUser,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let initial = auth
        .username
        .chars()
        .next()
        .unwrap_or('?')
        .to_uppercase()
        .to_string();
    Html(
        CONTROL_PLANE_HTML
            .replace("{{APP_NAME}}", &state.app_name)
            .replace("{{USERNAME}}", &auth.username)
            .replace("{{U}}", &initial),
    )
}

async fn create_message(
    ApiBearerOrSession(_auth): ApiBearerOrSession,
    Json(payload): Json<CreateMessageRequest>,
) -> Result<Json<CreateMessageResponse>, StatusCode> {
    if payload.name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    info!(name = %payload.name, "creating hello message");
    Ok(Json(CreateMessageResponse {
        message: format!("Hello, {}!", payload.name),
    }))
}
