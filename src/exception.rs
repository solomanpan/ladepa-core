//! Two kinds of failure, two representations.
//!
//! * A **business** failure ("the role is a system role", "username taken") is a [`DomainError`]: an ordinary
//!   value carried in `Err`. The API answers it with HTTP 200 and a non-zero envelope `code`.
//! * A **technical** failure (database down, broker unreachable) is an [`InfraError`]; the API answers 500 / 502
//!   and never leaks its details.
//!
//! Services return [`AppResult`], whose error is either of the two, so `?` works across both.
use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Validation,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Validation => "validation",
            Kind::Unauthorized => "unauthorized",
            Kind::Forbidden => "forbidden",
            Kind::NotFound => "not_found",
            Kind::Conflict => "conflict",
        }
    }

    pub fn parse(s: &str) -> Option<Kind> {
        Some(match s {
            "validation" => Kind::Validation,
            "unauthorized" => Kind::Unauthorized,
            "forbidden" => Kind::Forbidden,
            "not_found" => Kind::NotFound,
            "conflict" => Kind::Conflict,
            _ => return None,
        })
    }

    /// The envelope `code` (HTTP-like numbers; the HTTP status itself stays 200).
    pub fn code(self) -> i32 {
        match self {
            Kind::Validation => 400,
            Kind::Unauthorized => 401,
            Kind::Forbidden => 403,
            Kind::NotFound => 404,
            Kind::Conflict => 409,
        }
    }
}

/// A business failure. `template` + `params` are kept next to the rendered `message` so a translation can be
/// looked up (and re-filled) without parsing the message back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainError {
    pub kind: Kind,
    pub code: String,
    pub message: String,
    pub template: String,
    pub params: Vec<(String, String)>,
}

pub const NOT_FOUND_TEMPLATE: &str = "{entity} '{id}' not found";

impl DomainError {
    /// "<Entity> '<id>' not found" with the stable code `<entity>_not_found`.
    pub fn not_found(entity: &str, id: &str) -> DomainError {
        DomainError {
            kind: Kind::NotFound,
            code: format!("{}_not_found", entity.to_lowercase()),
            message: format!("{entity} '{id}' not found"),
            template: NOT_FOUND_TEMPLATE.to_string(),
            params: vec![("entity".into(), entity.into()), ("id".into(), id.into())],
        }
    }
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for DomainError {}

/// A technical failure. `upstream` marks a dependency that is down (answered with 502 instead of 500).
#[derive(Debug, thiserror::Error)]
#[error("{code}: {message}")]
pub struct InfraError {
    pub code: String,
    pub message: String,
    pub upstream: bool,
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl InfraError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        InfraError { code: code.into(), message: message.into(), upstream: false, source: None }
    }

    pub fn upstream(code: &str, message: impl Into<String>) -> Self {
        InfraError { upstream: true, ..Self::new(code, message) }
    }

    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Infra(#[from] InfraError),
}

pub type DomainResult<T> = Result<T, DomainError>;
pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn into_domain(self) -> Result<DomainError, InfraError> {
        match self {
            AppError::Domain(d) => Ok(d),
            AppError::Infra(i) => Err(i),
        }
    }
}
