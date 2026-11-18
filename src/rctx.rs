//! The request context, carried in a task-local so it needs no parameter threading: set by the HTTP middleware
//! (and by the command consumer), read by services, repositories and the error mapper.
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use uuid::Uuid;

/// Namespace of the deterministic ids (uuid v5).
pub const ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6f, 0x1d, 0x3c, 0x52, 0x8a, 0x0b, 0x4c, 0x8e, 0x9a, 0x52, 0x0d, 0x1f, 0x6b, 0x7e, 0x4a, 0x11,
]);

#[derive(Debug)]
pub struct IdSequence {
    request: String,
    next: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    pub request_id: String,
    pub operator: String,
    pub locale: String,
    pub org_uuid: String,
    /// `Some` while a write request is executed: ids derive from the request id (replay-safe).
    pub ids: Option<Arc<IdSequence>>,
}

tokio::task_local! {
    static CURRENT: RequestContext;
}

pub fn current() -> RequestContext {
    CURRENT.try_with(|c| c.clone()).unwrap_or_default()
}

pub async fn scope<F: Future>(ctx: RequestContext, f: F) -> F::Output {
    CURRENT.scope(ctx, f).await
}

/// The context of a write request that is being executed: deterministic ids, the original operator.
pub fn for_command(request_id: &str, operator: &str, org: &str) -> RequestContext {
    RequestContext {
        request_id: request_id.into(),
        operator: operator.into(),
        locale: String::new(),
        org_uuid: org.into(),
        ids: Some(Arc::new(IdSequence { request: request_id.into(), next: AtomicU64::new(0) })),
    }
}

/// uuid (32 hex chars). Random normally; derived from the request id while a write request is executed, so a
/// redelivered message re-creates the same ids and the API can report the new uuid *before* it exists.
pub fn next_id() -> String {
    match CURRENT.try_with(|c| c.ids.clone()).ok().flatten() {
        Some(seq) => command_id(&seq.request, seq.next.fetch_add(1, Ordering::SeqCst)),
        None => Uuid::new_v4().simple().to_string(),
    }
}

/// The id of the n-th entity created by `request_id`.
pub fn command_id(request_id: &str, n: u64) -> String {
    Uuid::new_v5(&ID_NAMESPACE, format!("{request_id}:{n}").as_bytes()).simple().to_string()
}

/// A stable id derived from any name (sys_log ids of command outcomes / audited events).
pub fn derived_id(name: &str) -> String {
    Uuid::new_v5(&ID_NAMESPACE, name.as_bytes()).simple().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ids_are_random_outside_a_command_and_derived_inside() {
        assert_ne!(next_id(), next_id());
        let (a, b) = scope(for_command("req-1", "op", "org"), async { (next_id(), next_id()) }).await;
        assert_eq!(a, command_id("req-1", 0));
        assert_eq!(b, command_id("req-1", 1));
        let again = scope(for_command("req-1", "op", "org"), async { next_id() }).await;
        assert_eq!(a, again);
    }

    #[test]
    fn context_defaults_outside_a_scope() {
        assert_eq!(current().operator, "");
    }
}
