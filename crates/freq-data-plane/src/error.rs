use axum::{
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde::Serialize;

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            Self::NotFound(m) => (StatusCode::NOT_FOUND, m),
            Self::Conflict(m) => (StatusCode::CONFLICT, m),
            Self::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (status, Json(ErrorBody { error: msg })).into_response()
    }
}

impl From<data_sdk::Error> for ApiError {
    fn from(err: data_sdk::Error) -> Self {
        match &err {
            data_sdk::Error::NotFound { entity, key } => {
                Self::NotFound(format!("{entity} not found: {key}"))
            }
            data_sdk::Error::Db(db_err) => {
                let msg = db_err.to_string();
                if msg.contains("Duplicate key") || msg.contains("UNIQUE constraint") {
                    Self::Conflict(msg)
                } else {
                    Self::Internal(msg)
                }
            }
        }
    }
}
