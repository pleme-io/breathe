use std::collections::BTreeSet;
use std::sync::Mutex;

use crate::drift::{DriftEnvironment, DriftError, ObservedInstance, OrphanTracker};
use crate::fsm::{InstanceId, NodeId};

/// A minimal no-dependency executor — mocks never await a pending future, so
/// a busy-poll to the first `Ready` is sufficient. Reused verbatim from
/// `breathe-admission::tests`'s convention: keeps this crate tokio-free.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VT)
    }
    fn noop(_: *const ()) {}
    static VT: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VT)) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

struct MockEnv {
    observed: Vec<ObservedInstance>,
    declared: BTreeSet<NodeId>,
    terminated: Mutex<Vec<InstanceId>>,
}

impl MockEnv {
    fn new(observed: Vec<ObservedInstance>, declared: BTreeSet<NodeId>) -> Self {
        Self {
            observed,
            declared,
            terminated: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl DriftEnvironment for MockEnv {
    async fn observe_tagged_instances(&self) -> Result<Vec<ObservedInstance>, DriftError> {
        Ok(self.observed.clone())
    }
    async fn declared_live_node_ids(&self) -> Result<BTreeSet<NodeId>, DriftError> {
        Ok(self.declared.clone())
    }
    async fn terminate_instance(&self, instance_id: &InstanceId) -> Result<(), DriftError> {
        self.terminated.lock().unwrap().push(instance_id.clone());
        Ok(())
    }
}

#[test]
fn grace_ticks_zero_is_rejected() {
    let err = OrphanTracker::new(0).unwrap_err();
    assert!(matches!(err, DriftError::Config(_)));
}

#[test]
fn untagged_instance_is_marked_then_swept_after_the_grace_window() {
    let env = MockEnv::new(
        vec![ObservedInstance {
            instance_id: InstanceId::new("i-1"),
            lifecycle_id: None,
        }],
        BTreeSet::new(),
    );
    let mut tracker = OrphanTracker::new(2).unwrap();

    let r1 = block_on(tracker.tick(&env)).unwrap();
    assert_eq!(r1.newly_marked, vec![InstanceId::new("i-1")]);
    assert_eq!(r1.still_marked, vec![(InstanceId::new("i-1"), 1)]);
    assert!(
        r1.swept.is_empty(),
        "must NOT sweep on the first sighting — that is the single-tick-sweep race"
    );
    assert!(env.terminated.lock().unwrap().is_empty());

    let r2 = block_on(tracker.tick(&env)).unwrap();
    assert_eq!(r2.swept, vec![InstanceId::new("i-1")]);
    assert_eq!(
        *env.terminated.lock().unwrap(),
        vec![InstanceId::new("i-1")]
    );
    assert!(
        tracker.marked().next().is_none(),
        "a swept instance's mark must be cleared"
    );
}

#[test]
fn a_declared_live_instance_is_never_marked() {
    let declared: BTreeSet<NodeId> = [NodeId::new("n-1")].into_iter().collect();
    let env = MockEnv::new(
        vec![ObservedInstance {
            instance_id: InstanceId::new("i-1"),
            lifecycle_id: Some(NodeId::new("n-1")),
        }],
        declared,
    );
    let mut tracker = OrphanTracker::new(1).unwrap();
    let report = block_on(tracker.tick(&env)).unwrap();
    assert!(report.newly_marked.is_empty());
    assert!(report.swept.is_empty());
    assert!(report.declared_but_unobserved.is_empty());
}

#[test]
fn an_orphan_that_resolves_before_the_grace_window_is_cleared_not_swept() {
    let mut tracker = OrphanTracker::new(3).unwrap();

    // Tick 1: i-1 is untagged (or its record hasn't landed yet) — marked.
    let env1 = MockEnv::new(
        vec![ObservedInstance {
            instance_id: InstanceId::new("i-1"),
            lifecycle_id: None,
        }],
        BTreeSet::new(),
    );
    let r1 = block_on(tracker.tick(&env1)).unwrap();
    assert_eq!(r1.newly_marked, vec![InstanceId::new("i-1")]);

    // Tick 2: the FSM record caught up — i-1 is now tagged AND declared live.
    let declared: BTreeSet<NodeId> = [NodeId::new("n-1")].into_iter().collect();
    let env2 = MockEnv::new(
        vec![ObservedInstance {
            instance_id: InstanceId::new("i-1"),
            lifecycle_id: Some(NodeId::new("n-1")),
        }],
        declared,
    );
    let r2 = block_on(tracker.tick(&env2)).unwrap();
    assert_eq!(r2.cleared, vec![InstanceId::new("i-1")]);
    assert!(r2.swept.is_empty());
    assert!(tracker.marked().next().is_none());
}

#[test]
fn declared_but_unobserved_is_surfaced_never_actioned() {
    let declared: BTreeSet<NodeId> = [NodeId::new("n-1")].into_iter().collect();
    let env = MockEnv::new(Vec::new(), declared);
    let mut tracker = OrphanTracker::new(1).unwrap();
    let report = block_on(tracker.tick(&env)).unwrap();
    assert_eq!(report.declared_but_unobserved, vec![NodeId::new("n-1")]);
    // Surfaced only — this reconciler never mutates a declared record.
    assert!(env.terminated.lock().unwrap().is_empty());
}

#[test]
fn tick_report_vectors_are_sorted_regardless_of_observation_order() {
    let env = MockEnv::new(
        vec![
            ObservedInstance {
                instance_id: InstanceId::new("i-2"),
                lifecycle_id: None,
            },
            ObservedInstance {
                instance_id: InstanceId::new("i-1"),
                lifecycle_id: None,
            },
        ],
        BTreeSet::new(),
    );
    let mut tracker = OrphanTracker::new(5).unwrap();
    let report = block_on(tracker.tick(&env)).unwrap();
    assert_eq!(
        report.newly_marked,
        vec![InstanceId::new("i-1"), InstanceId::new("i-2")]
    );
}

#[test]
fn a_single_tick_never_sweeps_when_grace_ticks_is_at_least_two() {
    let env = MockEnv::new(
        vec![ObservedInstance {
            instance_id: InstanceId::new("i-1"),
            lifecycle_id: None,
        }],
        BTreeSet::new(),
    );
    let mut tracker = OrphanTracker::new(2).unwrap();
    let report = block_on(tracker.tick(&env)).unwrap();
    assert!(report.swept.is_empty());
    assert_eq!(report.still_marked, vec![(InstanceId::new("i-1"), 1)]);
}

#[test]
fn terminate_failure_propagates_and_leaves_the_mark_in_place() {
    struct FailingEnv;
    #[async_trait::async_trait]
    impl DriftEnvironment for FailingEnv {
        async fn observe_tagged_instances(&self) -> Result<Vec<ObservedInstance>, DriftError> {
            Ok(vec![ObservedInstance {
                instance_id: InstanceId::new("i-1"),
                lifecycle_id: None,
            }])
        }
        async fn declared_live_node_ids(&self) -> Result<BTreeSet<NodeId>, DriftError> {
            Ok(BTreeSet::new())
        }
        async fn terminate_instance(&self, _instance_id: &InstanceId) -> Result<(), DriftError> {
            Err(DriftError::CloudApi("boom".into()))
        }
    }
    let mut tracker = OrphanTracker::new(1).unwrap();
    let err = block_on(tracker.tick(&FailingEnv)).unwrap_err();
    assert!(matches!(err, DriftError::CloudApi(_)));
    // The mark is NOT cleared on a failed sweep — it stays, so the next tick
    // retries the termination rather than silently forgetting the orphan.
    assert_eq!(tracker.marked().next(), Some((&InstanceId::new("i-1"), 1)));
}
