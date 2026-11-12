use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub enum AppError {
    BadRequest(String),
    Forbidden(String),
    NotFound(String),
    Internal(String),
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::BadRequest(msg)
            | AppError::Forbidden(msg)
            | AppError::NotFound(msg)
            | AppError::Internal(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for AppError {}

impl From<rusqlite::Error> for AppError {
    fn from(err: rusqlite::Error) -> Self {
        AppError::Internal(format!("Database error: {err}"))
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": msg, "message": msg })),
            )
                .into_response(),
            AppError::Forbidden(msg) => (
                StatusCode::FORBIDDEN,
                Json(json!({ "error": msg, "message": msg })),
            )
                .into_response(),
            AppError::NotFound(msg) => (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": msg, "message": msg })),
            )
                .into_response(),
            AppError::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": msg, "message": msg })),
            )
                .into_response(),
        }
    }
}
