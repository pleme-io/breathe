//! The NATS reactive-reconcile trigger — the event-reactive extension to
//! breathe-controller's watch-driven reconcile loop (task #90, nervous-system
//! integration; see the `reactive-nervous-system` skill).
//!
//! `kube_runtime::controller::Controller::reconcile_all_on` (verified against the
//! exact pinned kube-runtime 0.96.0 source, `src/controller/mod.rs:1472`) is
//! ADDITIVE by construction: it `.push()`es a second stream into the SAME
//! `trigger_selector: stream::SelectAll` the primary watch stream already lives
//! in — "This can be called multiple times, in which case they are additive" per
//! its own doc comment. So wiring a NATS trigger alongside the existing watch is
//! never a replacement, never a race — it is one more emitter into a fan-in the
//! `Controller` already owns.
//!
//! # The two-gate resolver
//!
//! [`resolve_trigger`] is the ENTIRE safety mechanism: two independent gates
//! (`BREATHE_NATS_URL` set, `BREATHE_NATS_RECONCILE_ENABLED=true`), both must be
//! true, and `None` means the caller must NOT call `.reconcile_all_on()` at all —
//! not "call it with a stream that never fires". This is stronger than a runtime
//! no-op: when either gate is unmet, the `Controller` value `main.rs` builds is
//! the LITERAL SAME object `gen_controller!` already produces today — byte-
//! identical-when-off, structurally, not just behaviorally.
//!
//! # Mockable vs needs-live
//!
//! [`resolve_trigger`]'s gate logic is tested against a mock [`NatsTrigger`] below
//! (env-unset / disabled / subscribe-fails paths) — zero sockets. [`LiveNats`]
//! (the real `async_nats::Client::subscribe` call) and
//! `kube_runtime::Controller::reconcile_all_on`'s own bulk-reconcile mechanics are
//! NEEDS-LIVE — the latter is kube-rs's own tested behavior (cited above by exact
//! source line), not re-tested here.

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use std::pin::Pin;

/// `kube_runtime::Controller::reconcile_all_on` requires `Stream + Send + Sync +
/// 'static` — a stronger bound than `futures::stream::BoxStream`'s own `+ Send`
/// (no `Sync`), so that convenience alias can't be used here (a real, compiler-
/// caught finding: `.boxed()` alone does not satisfy `reconcile_all_on`). This
/// alias + [`sync_boxed`] are the fix — `Box::pin(..) as Pin<Box<dyn ...>>` only
/// compiles when the concrete stream genuinely IS `Sync` (a tokio `mpsc::Receiver`-
/// backed stream is), so the bound is proven, not asserted.
pub type SyncBoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'static>>;

fn sync_boxed<S>(s: S) -> SyncBoxStream<S::Item>
where
    S: Stream + Send + Sync + 'static,
{
    Box::pin(s)
}

/// Abstracts "connect + subscribe" so the two-gate resolver is testable with a
/// mock that never touches a socket — the same TYPED-SPEC Environment-trait
/// pattern this session has used repeatedly (escuta's `ChangeSource`/`ChangeSink`,
/// `kind_watch`'s `WatchEventStream`, breathe-lifecycle's `DriftEnvironment`).
#[async_trait]
pub trait NatsTrigger: Send + Sync {
    /// Subscribe to `subject`, yielding `()` on every message (the trigger
    /// stream `reconcile_all_on` wants — the payload itself is never inspected;
    /// a message's mere arrival is the nudge).
    async fn subscribe(&self, subject: &str) -> Result<SyncBoxStream<()>, String>;
}

/// The real backend: an already-connected `async_nats::Client`.
pub struct LiveNats(pub async_nats::Client);

#[async_trait]
impl NatsTrigger for LiveNats {
    async fn subscribe(&self, subject: &str) -> Result<SyncBoxStream<()>, String> {
        self.0
            .subscribe(subject.to_string())
            .await
            .map(|s| sync_boxed(s.map(|_msg| ())))
            .map_err(|e| e.to_string())
    }
}

/// The two-gate resolver. `url` is `None` when `BREATHE_NATS_URL` is unset;
/// `enabled` is `BREATHE_NATS_RECONCILE_ENABLED == "true"`. `None` means the
/// caller must NOT call `.reconcile_all_on()` at all — the byte-identical-when-off
/// guarantee lives in the CALLER never invoking that method, not in this function
/// returning a never-firing stream. A subscribe failure (broker unreachable, bad
/// subject) degrades to `None` with a warning — the primary kube watch is the
/// standing safety net regardless (this trigger only ever makes reconciles
/// happen SOONER, never instead-of).
pub async fn resolve_trigger(
    url: Option<String>,
    enabled: bool,
    trigger: &dyn NatsTrigger,
    subject: &str,
) -> Option<SyncBoxStream<()>> {
    let _url = url?; // gate 1: BREATHE_NATS_URL unset ⇒ None
    if !enabled {
        return None; // gate 2: BREATHE_NATS_RECONCILE_ENABLED != "true" ⇒ None
    }
    match trigger.subscribe(subject).await {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!(error = %e, subject, "NATS subscribe failed — watch-only fallback (primary reconcile unaffected)");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A mock `NatsTrigger` that records how many times `subscribe` was called
    /// and returns a scripted outcome — no socket, no broker, no cluster.
    struct MockTrigger {
        calls: Arc<AtomicUsize>,
        outcome: Result<Vec<()>, String>,
    }

    #[async_trait]
    impl NatsTrigger for MockTrigger {
        async fn subscribe(&self, _subject: &str) -> Result<SyncBoxStream<()>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match &self.outcome {
                Ok(items) => Ok(sync_boxed(futures::stream::iter(items.clone()))),
                Err(e) => Err(e.clone()),
            }
        }
    }

    #[tokio::test]
    async fn url_unset_never_calls_subscribe() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mock = MockTrigger { calls: calls.clone(), outcome: Ok(vec![()]) };

        let trigger = resolve_trigger(None, true, &mock, "escuta.*.memoryband.>").await;

        assert!(trigger.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0, "gate 1 must short-circuit before ever touching the trigger");
    }

    #[tokio::test]
    async fn disabled_never_calls_subscribe_even_with_a_url() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mock = MockTrigger { calls: calls.clone(), outcome: Ok(vec![()]) };

        let trigger = resolve_trigger(Some("nats://x:4222".into()), false, &mock, "escuta.*.memoryband.>").await;

        assert!(trigger.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0, "gate 2 must short-circuit before ever touching the trigger");
    }

    #[tokio::test]
    async fn url_set_and_enabled_subscribes_and_returns_the_stream() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mock = MockTrigger { calls: calls.clone(), outcome: Ok(vec![(), ()]) };

        let trigger = resolve_trigger(Some("nats://x:4222".into()), true, &mock, "escuta.*.memoryband.>").await;

        assert!(trigger.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let items: Vec<()> = trigger.unwrap().collect().await;
        assert_eq!(items.len(), 2);
    }

    #[tokio::test]
    async fn subscribe_failure_degrades_to_none_not_a_panic() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mock = MockTrigger { calls: calls.clone(), outcome: Err("connection refused".into()) };

        let trigger = resolve_trigger(Some("nats://x:4222".into()), true, &mock, "escuta.*.memoryband.>").await;

        assert!(trigger.is_none(), "a broker-unreachable failure must degrade to watch-only, never panic/propagate");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
