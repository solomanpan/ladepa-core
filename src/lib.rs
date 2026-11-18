//! # ladepa-core
//!
//! The shared kernel of the ladepa services (the Rust counterpart of `kimoj-core` / the Go `core/` package).
//! It knows nothing about HTTP, SQL, Kafka or Redis: it only defines the *vocabulary* the layers speak.
//!
//! * [`exception`] — `DomainError` (a business failure, a *value*) vs `InfraError` (a technical failure), and
//!   `AppError`, the one error type services return (`AppResult<T>`).
//! * [`factory`]   — the error catalog: one `ErrorSpec` per business error, built once, formatted everywhere.
//! * [`rctx`]      — the request context (request id, operator, locale, deterministic ids) in a task-local.
//! * [`facade`]    — contracts implemented by the infrastructure: `UnitOfWork` (master / replica scopes,
//!   after-commit messages), `Publisher` / `MessageBus`, `Cache`, `PasswordHasher`.
pub mod exception;
pub mod facade;
pub mod memory;
pub mod factory;
pub mod rctx;

pub use exception::{AppError, AppResult, DomainError, DomainResult, InfraError, Kind};
pub use factory::ErrorSpec;
