//! `breathe-lifecycle` — the "no straggler nodes, ever" typescape for
//! Camelot node provisioning. Node-lifecycle addendum to
//! `theory/CORRENTEZA.md` (which owns tick-by-tick WORKLOAD placement onto
//! EXISTING nodes via taints/tolerations); this crate owns the orthogonal
//! concern CORRENTEZA §5 deliberately declines: the cloud NODE's own
//! birth-to-death lifecycle and the guarantee that no cloud instance this
//! controller ever created outlives its typed record.
//!
//! Three composed layers, each independently tier-graded (never rounded up):
//!
//! 1. [`fsm`] — a phantom-typestate lifecycle FSM. `Requested → Provisioning
//!    → Joining → Active → Draining → TerminationIssued → Reaped`, plus the
//!    short-circuit terminal `Aborted`. **Truly-unrepresentable**: an
//!    illegal transition is an absent method (E0599); `Reaped` is a
//!    structurally distinct type with no instance/k8s-ref fields at all, so
//!    "believed reaped but still tracked" cannot be constructed.
//! 2. [`drift`] — a continuous reconciler comparing real AWS state against
//!    the FSM's declared live set, reclaiming orphans via a lease-expiry-
//!    bounded mark-and-sweep GC. **Only-mitigated**, a named composite of
//!    C1 (forever-quantifier) + C2 (external-world read) + C5
//!    (non-transactional cloud I/O) — this is the honest ceiling for
//!    "no orphan ever," not a gap to chase further.
//! 3. [`breaker`] — a bulkhead (sealed permit pool) + a spend-rate Nygard
//!    circuit breaker, gating every provisioning call at credential/client
//!    acquisition. The bulkhead is **truly-unrepresentable** ("provision
//!    without a permit" has no expressible form); the spend-rate breaker's
//!    trip decision is **only-mitigated by construction** (a C2/C4 ceiling —
//!    a fact about shared external state can never be a compile proof).
//!
//! None of the three duplicates `breathe-admission::Recurso<P>` (the fleet's
//! other phantom-typestate resource FSM) — see [`fsm`]'s module doc for the
//! explicit near-miss-but-not-a-fold accounting, and
//! `theory/CORRENTEZA.md`'s node-lifecycle addendum for how this composes
//! with correnteza's workload-placement scope (disjoint: this crate never
//! decides WHICH workload runs where, only whether a given cloud NODE record
//! is alive, joining, draining, or gone).

pub mod breaker;
pub mod drift;
pub mod fsm;

pub use breaker::{
    Admission, AuthorizedProvision, BreakerError, BreakerState, Bulkhead, Permit, SpendRateBreaker,
};
pub use drift::{DriftEnvironment, DriftError, ObservedInstance, OrphanTracker, TickReport};
pub use fsm::{
    Aborted, Active, AwsTerminationConfirmed, Draining, FaseNode, InstanceId, Joining, K8sNodeRef,
    LifecycleError, Node, NodeId, Phase, Provisioning, Reaped, Requested, TerminationIssued,
};
