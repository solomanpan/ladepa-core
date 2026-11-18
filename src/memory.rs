//! In-process implementations of the messaging and cache contracts: for development, tests and the
//! single-process deployment (`APP_BROKER=memory`).
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::FutureExt;
use tokio::sync::{mpsc, Notify};

use crate::exception::InfraError;
use crate::facade::{Cache, Handler, Message, MessageBus, Publisher};

type OnError = Arc<dyn Fn(&Message, &str) + Send + Sync>;

/// Delivers messages one at a time, in publish order, on a background task. A failing or panicking handler is
/// reported to `on_error` and does not stop the others (there are no retries in memory).
pub struct MemoryBus {
    handlers: Arc<RwLock<HashMap<String, Vec<Handler>>>>,
    tx: mpsc::UnboundedSender<Message>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<Message>>>,
    pending: Arc<AtomicUsize>,
    idle: Arc<Notify>,
    on_error: Arc<RwLock<OnError>>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Default for MemoryBus {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBus {
    pub fn new() -> MemoryBus {
        let (tx, rx) = mpsc::unbounded_channel();
        MemoryBus {
            handlers: Default::default(),
            tx,
            rx: Mutex::new(Some(rx)),
            pending: Default::default(),
            idle: Default::default(),
            on_error: Arc::new(RwLock::new(Arc::new(|m: &Message, e: &str| {
                tracing::error!(event = %m.event_type, error = e, "memory bus handler failed")
            }))),
            worker: Mutex::new(None),
        }
    }

    pub fn on_error(&self, f: impl Fn(&Message, &str) + Send + Sync + 'static) {
        *self.on_error.write().unwrap() = Arc::new(f);
    }

    /// Waits until every published message has been handled (tests).
    pub async fn drain(&self) {
        while self.pending.load(Ordering::SeqCst) > 0 {
            let notified = self.idle.notified();
            if self.pending.load(Ordering::SeqCst) == 0 {
                break;
            }
            let _ = tokio::time::timeout(Duration::from_millis(50), notified).await;
        }
    }
}

#[async_trait]
impl Publisher for MemoryBus {
    async fn publish(&self, message: &Message) -> Result<(), InfraError> {
        self.pending.fetch_add(1, Ordering::SeqCst);
        self.tx.send(message.clone()).map_err(|e| {
            self.pending.fetch_sub(1, Ordering::SeqCst);
            InfraError::upstream("publish_failed", "memory bus is closed").with_source(e)
        })
    }
}

#[async_trait]
impl MessageBus for MemoryBus {
    fn subscribe(&self, event_type: &str, handler: Handler) {
        self.handlers.write().unwrap().entry(event_type.to_string()).or_default().push(handler);
    }

    async fn start(&self) -> Result<(), InfraError> {
        let Some(mut rx) = self.rx.lock().unwrap().take() else { return Ok(()) };
        let handlers = self.handlers.clone();
        let pending = self.pending.clone();
        let idle = self.idle.clone();
        let on_error = self.on_error.clone();
        let task = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let list = handlers.read().unwrap().get(&message.event_type).cloned().unwrap_or_default();
                for handler in list {
                    match AssertUnwindSafe(handler(message.clone())).catch_unwind().await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => (on_error.read().unwrap().clone())(&message, &e.to_string()),
                        Err(_) => (on_error.read().unwrap().clone())(&message, "handler panicked"),
                    }
                }
                pending.fetch_sub(1, Ordering::SeqCst);
                idle.notify_waiters();
            }
        });
        *self.worker.lock().unwrap() = Some(task);
        Ok(())
    }

    async fn stop(&self) {
        self.drain().await;
        if let Some(task) = self.worker.lock().unwrap().take() {
            task.abort();
        }
    }
}

/// A TTL cache in a map.
#[derive(Default)]
pub struct MemoryCache {
    entries: Mutex<HashMap<String, (String, Instant)>>,
}

impl MemoryCache {
    pub fn new() -> MemoryCache {
        MemoryCache::default()
    }
}

#[async_trait]
impl Cache for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<String>, InfraError> {
        let mut map = self.entries.lock().unwrap();
        match map.get(key) {
            Some((v, exp)) if *exp > Instant::now() => Ok(Some(v.clone())),
            Some(_) => {
                map.remove(key);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn set(&self, key: &str, value: &str, ttl: Duration) -> Result<(), InfraError> {
        self.entries.lock().unwrap().insert(key.to_string(), (value.to_string(), Instant::now() + ttl));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), InfraError> {
        self.entries.lock().unwrap().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn msg(kind: &str, key: &str) -> Message {
        Message {
            event_id: key.into(),
            event_type: kind.into(),
            org_uuid: "o".into(),
            key: key.into(),
            occurred_at: Utc::now(),
            payload: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn delivers_in_order_and_survives_failing_handlers() {
        let bus = MemoryBus::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(AtomicUsize::new(0));
        let s = seen.clone();
        bus.subscribe("A", Arc::new(move |m| {
            let s = s.clone();
            async move {
                s.lock().unwrap().push(m.key);
                Ok(())
            }
            .boxed()
        }));
        bus.subscribe("B", Arc::new(|_| async { panic!("boom") }.boxed()));
        bus.subscribe("C", Arc::new(|_| async { Err("nope".into()) }.boxed()));
        let e = errors.clone();
        bus.on_error(move |_, _| {
            e.fetch_add(1, Ordering::SeqCst);
        });
        bus.start().await.unwrap();
        for m in [msg("A", "1"), msg("B", "x"), msg("C", "y"), msg("A", "2"), msg("A", "3")] {
            bus.publish(&m).await.unwrap();
        }
        bus.drain().await;
        assert_eq!(*seen.lock().unwrap(), vec!["1", "2", "3"]);
        assert_eq!(errors.load(Ordering::SeqCst), 2);
        bus.stop().await;
    }

    #[tokio::test]
    async fn cache_expires() {
        let c = MemoryCache::new();
        c.set("k", "v", Duration::from_millis(30)).await.unwrap();
        assert_eq!(c.get("k").await.unwrap().as_deref(), Some("v"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(c.get("k").await.unwrap(), None);
        c.set("k", "v", Duration::from_secs(5)).await.unwrap();
        c.delete("k").await.unwrap();
        assert_eq!(c.get("k").await.unwrap(), None);
    }
}
