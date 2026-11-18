//! Contracts implemented by the infrastructure layer of a service.
use std::any::Any;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::exception::{AppResult, InfraError};

// ---------------------------------------------------------------- messages
/// What travels on a bus: the envelope of one domain event. `payload` is the event's own JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub event_id: String,
    pub event_type: String,
    pub org_uuid: String,
    /// aggregate id: the partition key (per-aggregate ordering)
    pub key: String,
    pub occurred_at: DateTime<Utc>,
    pub payload: serde_json::Value,
}

pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;
/// A subscriber. An `Err` (or a panic) makes the bus retry and finally dead-letter the message; business
/// failures must therefore be *recorded* by the handler and answered with `Ok`.
pub type Handler = Arc<dyn Fn(Message) -> BoxFuture<'static, Result<(), HandlerError>> + Send + Sync>;

#[async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(&self, message: &Message) -> Result<(), InfraError>;
}

#[async_trait]
pub trait MessageBus: Publisher {
    /// Handlers are registered before `start`.
    fn subscribe(&self, event_type: &str, handler: Handler);
    async fn start(&self) -> Result<(), InfraError>;
    async fn stop(&self);
}

// ---------------------------------------------------------------- unit of work
/// An open database scope as the repositories see it. The domain only knows this trait; the infrastructure
/// downcasts it (`as_any_mut`) to its concrete session (a SQL transaction, say).
pub trait Session: Send {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// Queue a message; it is published only after the scope committed.
    fn defer(&mut self, message: Message);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// read-write transaction on the primary
    Master,
    /// read-only work on a replica (the primary when none is configured)
    Replica,
}

pub type ScopeFn<'f> = Box<dyn for<'s> FnOnce(&'s mut dyn Session) -> BoxFuture<'s, AppResult<()>> + Send + 'f>;

/// Runs a closure inside a scope: commit on `Ok`, roll back on `Err` (a business failure rolls back too),
/// publish the deferred messages after the commit. Scopes do not nest: compose work inside one scope.
#[async_trait]
pub trait UnitOfWork: Send + Sync {
    async fn run(&self, mode: Mode, scope: ScopeFn<'_>) -> AppResult<()>;
}

async fn run_typed<T, F>(uow: &dyn UnitOfWork, mode: Mode, f: F) -> AppResult<T>
where
    F: for<'s> FnOnce(&'s mut dyn Session) -> BoxFuture<'s, AppResult<T>> + Send + 'static,
    T: Send + 'static,
{
    let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let out = slot.clone();
    uow.run(
        mode,
        Box::new(move |session| {
            Box::pin(async move {
                let value = f(session).await?;
                *out.lock().unwrap() = Some(value);
                Ok(())
            })
        }),
    )
    .await?;
    let value = slot.lock().unwrap().take().expect("scope finished without a value");
    Ok(value)
}

/// `in_master(uow, |s| Box::pin(async move { ... }))`
pub async fn in_master<T, F>(uow: &dyn UnitOfWork, f: F) -> AppResult<T>
where
    F: for<'s> FnOnce(&'s mut dyn Session) -> BoxFuture<'s, AppResult<T>> + Send + 'static,
    T: Send + 'static,
{
    run_typed(uow, Mode::Master, f).await
}

pub async fn in_replica<T, F>(uow: &dyn UnitOfWork, f: F) -> AppResult<T>
where
    F: for<'s> FnOnce(&'s mut dyn Session) -> BoxFuture<'s, AppResult<T>> + Send + 'static,
    T: Send + 'static,
{
    run_typed(uow, Mode::Replica, f).await
}

// ---------------------------------------------------------------- cache, hashing
#[async_trait]
pub trait Cache: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<String>, InfraError>;
    async fn set(&self, key: &str, value: &str, ttl: Duration) -> Result<(), InfraError>;
    async fn delete(&self, key: &str) -> Result<(), InfraError>;
}

#[async_trait]
pub trait PasswordHasher: Send + Sync {
    async fn hash(&self, raw: &str) -> Result<String, InfraError>;
    async fn verify(&self, raw: &str, hash: &str) -> Result<bool, InfraError>;
}
