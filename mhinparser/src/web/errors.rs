use actix_web::{HttpResponse, ResponseError};

#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("External API error: {0}")]
    ExternalApi(String),

    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    #[error("Invalid pagination parameters: {0}")]
    InvalidPagination(String),

    #[error("Template error: {0}")]
    Template(String),

    #[error("Internal server error: {0}")]
    Internal(String),
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ApiError::InvalidAddress(_) | ApiError::InvalidPagination(_) => {
                HttpResponse::BadRequest().json(serde_json::json!({
                    "error": self.to_string()
                }))
            }
            ApiError::ExternalApi(_) => {
                HttpResponse::ServiceUnavailable().json(serde_json::json!({
                    "error": self.to_string()
                }))
            }
            ApiError::Template(_) => HttpResponse::InternalServerError().json(serde_json::json!({
                "error": self.to_string()
            })),
            _ => HttpResponse::InternalServerError().json(serde_json::json!({
                "error": self.to_string()
            })),
        }
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
