//! The continuous drift-reconciler — the C2-honest half of "no straggler
//! nodes, ever."
//!
//! [`crate::fsm`]'s `Node<P>` FSM governs only what OUR process constructs
//! and transitions; it says nothing about whether the real AWS instance a
//! `Node<Active>` believes exists is still running. Spot reclamation, a
//! manual console termination, or an out-of-band `terraform destroy`/`tofu
//! destroy` can kill the real instance with zero synchronous notice — the
//! typed value stays `Active` regardless. This module closes THAT gap, by
//! periodically comparing real AWS state against the FSM's declared live
//! set and reclaiming anything neither side can account for.
//!
//! # Tier: only-mitigated — a composite of C1 + C2 + C5, named honestly
//!
//! Per `theory/UNREPRESENTABILITY.md` §II and `theory/ECLUSA.md` §XVIII(b),
//! this module can never exceed `only-mitigated`, on three independent axes:
//!
//! - **C1** — "no orphan ever" is a forever-quantifier over infinite
//!   executions. It is discharged only by a continuously-ticking reconciler
//!   (this module, called on a cadence by the caller), never a compile proof.
//! - **C2** — whether the AWS-side instance is ACTUALLY still running is a
//!   fact about reality outside the type system. [`DriftEnvironment::observe_tagged_instances`]
//!   is that external read; nothing in this crate can make it more than
//!   eventually-consistent with reality at the moment it returns.
//! - **C5** — `terminate_instance` + removing a mark has no atomic cutover.
//!   A crash mid-tick needs a resumable outcome, not faked atomicity — this
//!   module does not persist its marks across a restart (that is the
//!   caller's job, via a real `shigoto::Dag`/durable store); a restart loses
//!   in-flight grace-window progress and a straggler simply gets re-marked
//!   from tick 1, which is SAFE (never under-detects) but not free.
//!
//! # The mechanism: lease-expiry-bounded mark-and-sweep, not bare mark-and-sweep
//!
//! An orphan is MARKED, not swept, on first sighting. It sweeps (is
//! terminated) only after staying orphaned for `grace_ticks` CONSECUTIVE
//! ticks. This is required, not decorative: `DescribeInstances` and the
//! declared-record store are eventually consistent RELATIVE TO EACH OTHER —
//! an instance can legitimately exist in AWS microseconds before its FSM
//! record commits, and a single-tick sweep would terminate a perfectly good
//! in-flight `Node<Provisioning>`. The grace window is the debounce,
//! structurally the same shape as the fleet's `awase::KeyRepeatGate` pattern
//! applied to a slower clock.
//!
//! [`crate::breaker`]'s vocabulary is deliberately NOT reused for the
//! mark/sweep decision here (`DriftPolicy`-shaped: Disabled/Warn/Enabled is
//! `magma_converge::drift::DriftPolicy`'s shape) — this crate does not
//! depend on `magma-converge` (it would pull the full magma-apply/
//! magma-state/magma-plan graph for one 3-variant enum); the tick loop below
//! is vocabulary-ALIGNED with that policy shape but locally defined. A
//! follow-up, gated on breathe adopting magma's `Reconciler` trait
//! fleet-wide, could depend on it for real. Named, not silently worked
//! around.

use std::collections::{BTreeMap, BTreeSet};

use crate::fsm::{InstanceId, NodeId};

/// The one write this reconciler performs, and the one external read (beyond
/// enumeration) it needs — everything else is pure local bookkeeping. A real
/// implementation wraps an AWS SDK client + the FSM record store; tests wrap
/// an in-memory fixture. This is the mockable side-effect seam (the same
/// `Environment`-trait discipline `camelot-bootstrap` and every other
/// pleme-io interpreter uses).
#[async_trait::async_trait]
pub trait DriftEnvironment: Send + Sync {
    /// Enumerate REAL cloud instances tagged as this controller's own
    /// (`project=camelot` + the `camelot.pleme.io/lifecycle-id` tag). A C2
    /// external-world read.
    async fn observe_tagged_instances(&self) -> Result<Vec<ObservedInstance>, DriftError>;

    /// Enumerate the [`NodeId`]s the FSM record store currently believes are
    /// LIVE (any non-terminal `FaseNode`). Also a side-effecting read (the
    /// record store), abstracted the same way so tests never need a real DB.
    async fn declared_live_node_ids(&self) -> Result<BTreeSet<NodeId>, DriftError>;

    /// Terminate a real cloud instance — the sole mutating call this
    /// reconciler makes, and only after an instance survives `grace_ticks`
    /// consecutive ticks as an orphan.
    async fn terminate_instance(&self, instance_id: &InstanceId) -> Result<(), DriftError>;
}

/// A real AWS instance, tagged, as observed this tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedInstance {
    pub instance_id: InstanceId,
    /// Parsed from the `camelot.pleme.io/lifecycle-id` tag. `None` = untagged
    /// — itself an immediate orphan signal (the bluntest case: no FSM record
    /// ever existed for this instance at all, e.g. a hand-launched instance
    /// or a crashed provisioner that died before stamping the tag).
    pub lifecycle_id: Option<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftError {
    Config(String),
    CloudApi(String),
    RecordStore(String),
}

impl std::fmt::Display for DriftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(m) => write!(f, "drift reconciler config error: {m}"),
            Self::CloudApi(m) => write!(f, "cloud API error: {m}"),
            Self::RecordStore(m) => write!(f, "record store error: {m}"),
        }
    }
}

impl std::error::Error for DriftError {}

#[derive(Debug, Clone, Copy, Default)]
struct Mark {
    consecutive_ticks: u32,
}

/// One tick's outcome — every vector sorted (by `InstanceId`/`NodeId`'s
/// `Ord`) so a caller gets a deterministic, diffable, attestable report
/// (never trust a tick's effect blind — `theory/CORRENTEZA.md` §9's own
/// named principle, applied here).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    /// Instances newly identified as orphaned THIS tick (mark count now 1).
    pub newly_marked: Vec<InstanceId>,
    /// Instances still under the grace window: `(instance, ticks_marked)`.
    pub still_marked: Vec<(InstanceId, u32)>,
    /// Instances that crossed `grace_ticks` and were terminated THIS tick.
    pub swept: Vec<InstanceId>,
    /// Instances that were marked but resolved themselves (the FSM record
    /// caught up, or AWS no longer reports them at all) — mark cleared.
    pub cleared: Vec<InstanceId>,
    /// The INVERSE drift signal: a [`NodeId`] the record store believes is
    /// live, but no observed instance carries its lifecycle-id tag at all.
    /// Surfaced, never actioned here — this routes to the caller's own
    /// `DriftPolicy`-shaped decision (a record claiming ground truth has
    /// already reclaimed is a different bug than an orphan; see the module
    /// doc's `magma_converge::drift::DriftPolicy` note).
    pub declared_but_unobserved: Vec<NodeId>,
}

/// The lease-expiry-bounded mark-and-sweep GC. One instance per reconcile
/// loop; call [`OrphanTracker::tick`] on a cadence (a Viggy `(defpromessa)`
/// tick, in the destination — see `theory/CORRENTEZA.md`).
#[derive(Debug)]
pub struct OrphanTracker {
    grace_ticks: u32,
    marks: BTreeMap<InstanceId, Mark>,
}

impl OrphanTracker {
    /// # Errors
    /// [`DriftError::Config`] if `grace_ticks == 0` — a zero-tick grace
    /// window reintroduces the single-tick-sweep race this whole mechanism
    /// exists to avoid (an in-flight `Node<Provisioning>` racing its own
    /// first `DescribeInstances` sighting would be terminated on sight).
    pub fn new(grace_ticks: u32) -> Result<Self, DriftError> {
        if grace_ticks == 0 {
            return Err(DriftError::Config(
                "grace_ticks must be >= 1 (a zero-tick window reintroduces the single-tick-sweep race)".into(),
            ));
        }
        Ok(Self {
            grace_ticks,
            marks: BTreeMap::new(),
        })
    }

    #[must_use]
    pub fn grace_ticks(&self) -> u32 {
        self.grace_ticks
    }

    /// Instances currently under a mark, with their consecutive-tick count —
    /// read-only observability into the debounce state.
    pub fn marked(&self) -> impl Iterator<Item = (&InstanceId, u32)> {
        self.marks.iter().map(|(id, m)| (id, m.consecutive_ticks))
    }

    /// Run one reconcile tick: enumerate reality, enumerate declared state,
    /// diff, advance/clear marks, sweep anything that crossed the grace
    /// window.
    ///
    /// # Errors
    /// Propagates any [`DriftEnvironment`] error — enumeration and
    /// termination calls are both real I/O in production.
    pub async fn tick<E: DriftEnvironment>(&mut self, env: &E) -> Result<TickReport, DriftError> {
        let observed = env.observe_tagged_instances().await?;
        let declared = env.declared_live_node_ids().await?;

        let mut report = TickReport::default();

        // This tick's orphan set: untagged (no lifecycle-id at all) OR
        // tagged with a lifecycle-id the record store no longer considers
        // live. Sorted (BTreeSet) for deterministic iteration.
        let orphan_ids: BTreeSet<InstanceId> = observed
            .iter()
            .filter(|inst| match &inst.lifecycle_id {
                None => true,
                Some(id) => !declared.contains(id),
            })
            .map(|inst| inst.instance_id.clone())
            .collect();

        // Clear marks whose instance resolved (no longer orphaned) or
        // vanished from AWS entirely (already gone, nothing left to sweep).
        let stale: Vec<InstanceId> = self
            .marks
            .keys()
            .filter(|id| !orphan_ids.contains(*id))
            .cloned()
            .collect();
        for id in stale {
            self.marks.remove(&id);
            report.cleared.push(id);
        }

        // Advance marks for this tick's orphans; sweep anything that crossed
        // the grace window.
        for id in &orphan_ids {
            let mark = self.marks.entry(id.clone()).or_default();
            mark.consecutive_ticks += 1;
            if mark.consecutive_ticks == 1 {
                report.newly_marked.push(id.clone());
            }
            if mark.consecutive_ticks >= self.grace_ticks {
                env.terminate_instance(id).await?;
                report.swept.push(id.clone());
            } else {
                report
                    .still_marked
                    .push((id.clone(), mark.consecutive_ticks));
            }
        }
        for id in &report.swept {
            self.marks.remove(id);
        }

        // The inverse-drift signal — surfaced, not actioned (see module doc).
        let observed_lifecycle_ids: BTreeSet<NodeId> = observed
            .into_iter()
            .filter_map(|o| o.lifecycle_id)
            .collect();
        report.declared_but_unobserved = declared
            .difference(&observed_lifecycle_ids)
            .cloned()
            .collect();

        Ok(report)
    }
}

#[cfg(test)]
mod tests;
