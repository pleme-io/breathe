//! The phantom-typestate cloud-node lifecycle FSM.
//!
//! `Requested → Provisioning → Joining → Active → Draining →
//! TerminationIssued → Reaped`, plus the short-circuit terminal `Aborted`
//! (reachable only pre-`Active`). Styled after — and reusing the shared
//! convergence-proof primitive of — `breathe-admission::Recurso<P>`
//! (`breathe/breathe-admission/src/lib.rs`), the fleet's other phantom-
//! typestate resource FSM. **Deliberately a distinct type, not a fold into
//! `Recurso<P>`**: `Recurso<P>` answers "has this candidate passed its
//! admission GATES" (a pluggable `Portao` validation sequence ending in an
//! `Admitido<T>` pool membership); `Node<P>` answers "what is this AWS
//! instance's OWN boot→join→drain→terminate→reap sequence" (an
//! infrastructure-lifecycle question, gated by a real termination witness,
//! feeding the drift reconciler in [`crate::drift`]) — a different shape
//! (7 live phases + `Aborted`, not 9 + 2) and a different terminal contract
//! (`Reaped` erases its tracking fields structurally; `Aposentado` does not).
//! See `theory/CORRENTEZA.md`'s node-lifecycle addendum for the full
//! near-miss-but-not-a-fold accounting.
//!
//! # Tier: truly-unrepresentable (the library path)
//!
//! An illegal transition is an ABSENT METHOD (E0599), not a runtime branch —
//! proven mechanically below via `compile_fail` doctests (the same convention
//! `breathe-admission` uses; `trybuild` is not a fleet-standard dev-dependency
//! anywhere in the org, confirmed before reaching for it, so it is not
//! introduced here). What this crate does NOT prove — named honestly, never
//! rounded up — is whether the AWS-side instance a `Node<Active>` believes is
//! running is STILL running: that is an external-world fact (C2), discharged
//! only by [`crate::drift`]'s reconciler, never by the type system.
//!
//! ```compile_fail
//! use breathe_lifecycle::{Node, NodeId};
//! let r = Node::request(NodeId::new("n-1"));
//! let _ = r.ready(breathe_lifecycle::K8sNodeRef::new("k-1")); // E0599: no `ready` on Node<Requested>
//! ```
//!
//! ```compile_fail
//! use breathe_lifecycle::{Node, NodeId, InstanceId};
//! let active = Node::request(NodeId::new("n-1"))
//!     .provision()
//!     .joined(InstanceId::new("i-1"))
//!     .ready(breathe_lifecycle::K8sNodeRef::new("k-1"));
//! let _ = active.abort("nope"); // E0599: no `abort` on Node<Active> — that's `drain`, a different fact
//! ```
//!
//! ```compile_fail
//! use breathe_lifecycle::{Node, NodeId, InstanceId};
//! let active = Node::request(NodeId::new("n-1"))
//!     .provision()
//!     .joined(InstanceId::new("i-1"))
//!     .ready(breathe_lifecycle::K8sNodeRef::new("k-1"));
//! let _ = active.issue_termination(); // E0599: no `issue_termination` on Node<Active> — Draining cannot be skipped
//! ```
//!
//! ```compile_fail
//! use breathe_lifecycle::Reaped;
//! // Reaped's fields are private and it has no public struct-literal path —
//! // its sole constructor is `Node<TerminationIssued>::confirm_reaped`.
//! let _bad = Reaped { id: todo!(), witness: todo!(), _seal: () }; // E0451/E0063
//! ```
//!
//! ```compile_fail
//! use breathe_lifecycle::{Node, NodeId, InstanceId};
//! let ti = Node::request(NodeId::new("n-1"))
//!     .provision()
//!     .joined(InstanceId::new("i-1"))
//!     .ready(breathe_lifecycle::K8sNodeRef::new("k-1"))
//!     .drain()
//!     .issue_termination();
//! let _ = ti.confirm_reaped(); // E0061: confirm_reaped requires a witness argument — "reaped" cannot be claimed bare
//! ```

use std::marker::PhantomData;

// ============================================================================
// Identity newtypes — shared with crate::drift as the FSM ⇄ reconciler key.
// ============================================================================

macro_rules! id_newtype {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
        pub struct $name(pub String);

        impl $name {
            #[must_use]
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_newtype!(
    /// The FSM record's own stable identity — stamped at [`Node::request`] time
    /// and carried as the `camelot.pleme.io/lifecycle-id` tag on the AWS
    /// instance once one exists, so the drift reconciler ([`crate::drift`]) and
    /// the FSM record share one identifier from birth (never re-derived, never
    /// guessed at reconcile time).
    NodeId
);
id_newtype!(
    /// An AWS `InstanceId` (e.g. `i-0123456789abcdef0`). Present on a `Node<P>`
    /// from the `Provisioning → Joining` transition onward (see [`Node::joined`]).
    InstanceId
);
id_newtype!(
    /// The Kubernetes `Node` object name this instance joined as.
    K8sNodeRef
);

// ============================================================================
// FaseNode — the serializable phase label (mirrors FaseRecurso's split).
// ============================================================================

/// The closed legal-state set of a cloud node's lifecycle. The *serializable*
/// label (a future CRD's `status.phase`); [`Phase`]/[`Node<P>`] below is the
/// compile-time enforcement of the same FSM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FaseNode {
    /// A node identity has been minted; nothing provisioned yet.
    Requested,
    /// A cloud provisioning call is in flight; no instance id known yet.
    Provisioning,
    /// A real AWS instance exists; it has not yet joined the k8s cluster.
    Joining,
    /// Joined and scheduling workloads.
    Active,
    /// Cordoned; draining existing work before termination (`AnnouncedMove`'s
    /// node-lifecycle sibling — see `theory/CORRENTEZA.md` §4).
    Draining,
    /// A `TerminateInstances` call has been issued; awaiting confirmation.
    TerminationIssued,
    /// A good terminal: cleanly refused/abandoned before ever going `Active`.
    Aborted,
    /// A good terminal: termination confirmed by a real witness. NOTE — the
    /// [`Reaped`] VALUE (not this label) is a structurally distinct type with
    /// no instance/k8s-ref fields at all; this label is what a status field
    /// would carry.
    Reaped,
}

impl FaseNode {
    pub const ALL: [FaseNode; 8] = [
        Self::Requested,
        Self::Provisioning,
        Self::Joining,
        Self::Active,
        Self::Draining,
        Self::TerminationIssued,
        Self::Aborted,
        Self::Reaped,
    ];

    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Aborted | Self::Reaped)
    }

    /// Both terminals are GOOD: `Aborted` a clean pre-`Active` abandonment,
    /// `Reaped` a witnessed clean termination. Neither is a failure sink.
    #[must_use]
    pub fn is_good_terminal(self) -> bool {
        self.is_terminal()
    }

    /// The legal forward edges. `Active` has no edge to `Aborted` — a running
    /// node is drained, never abandoned; `abort` and `drain` are different
    /// words for different fasts, encoded as different phases entirely.
    #[must_use]
    pub fn legal_successors(self) -> &'static [FaseNode] {
        use FaseNode::{
            Aborted, Active, Draining, Joining, Provisioning, Reaped, Requested, TerminationIssued,
        };
        match self {
            Requested => &[Provisioning, Aborted],
            Provisioning => &[Joining, Aborted],
            Joining => &[Active, Aborted],
            Active => &[Draining],
            Draining => &[TerminationIssued],
            TerminationIssued => &[Reaped],
            Aborted | Reaped => &[],
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Provisioning => "provisioning",
            Self::Joining => "joining",
            Self::Active => "active",
            Self::Draining => "draining",
            Self::TerminationIssued => "termination-issued",
            Self::Aborted => "aborted",
            Self::Reaped => "reaped",
        }
    }
}

impl std::fmt::Display for FaseNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The node lifecycle is a convergent typed FSM: every reachable phase
/// reaches a GOOD terminal over the legal edges, and both terminals are good.
/// One call (`assert_convergent_fsm::<FaseNode>()`) proves closed-graph +
/// terminal-soundness + no-traps + universal convergence — the same shared
/// harness `breathe-admission::FaseRecurso` already consumes (Operating
/// Principle #1: the GENERIC convergence-proof primitive is reused; the
/// SPECIFIC FSM shape is not).
impl shigoto_fsm::ConvergentFsm for FaseNode {
    fn all() -> &'static [Self] {
        &Self::ALL
    }
    fn successors(&self) -> Vec<Self> {
        self.legal_successors().to_vec()
    }
    fn is_terminal(&self) -> bool {
        (*self).is_terminal()
    }
    fn is_good_terminal(&self) -> bool {
        (*self).is_good_terminal()
    }
}

// ============================================================================
// The phantom typestate — the in-Rust enforcement.
// ============================================================================

mod sealed {
    pub trait Sealed {}
}

/// A typestate marker — a zero-sized phase. Sealed: only this crate's phases
/// implement it, so external code cannot mint a new phase or bypass the FSM.
pub trait Phase: sealed::Sealed {
    const FASE: FaseNode;
}

macro_rules! phase {
    ($(#[$m:meta])* $name:ident => $fase:ident) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy)]
        pub struct $name;
        impl sealed::Sealed for $name {}
        impl Phase for $name {
            const FASE: FaseNode = FaseNode::$fase;
        }
    };
}

phase!(/// Phase marker: identity minted, nothing provisioned.
    Requested => Requested);
phase!(/// Phase marker: a cloud provisioning call is in flight.
    Provisioning => Provisioning);
phase!(/// Phase marker: a real instance exists, not yet joined to k8s.
    Joining => Joining);
phase!(/// Phase marker: joined and scheduling workloads.
    Active => Active);
phase!(/// Phase marker: cordoned and draining.
    Draining => Draining);
phase!(/// Phase marker: termination issued, awaiting confirmation.
    TerminationIssued => TerminationIssued);
phase!(/// Phase marker (terminal): cleanly aborted pre-Active.
    Aborted => Aborted);

/// A cloud node whose lifecycle phase is encoded in the type `P`. **Only the
/// forward method to a LEGAL next phase exists** — an illegal transition is
/// an absent method (E0599), never a runtime branch. Construct with
/// [`Node::request`]; advance with the per-phase methods below.
#[derive(Debug)]
pub struct Node<P: Phase> {
    id: NodeId,
    aws_instance_id: Option<InstanceId>,
    k8s_node_ref: Option<K8sNodeRef>,
    abort_reason: Option<String>,
    _p: PhantomData<P>,
}

impl<P: Phase> Node<P> {
    /// PRIVATE — the only way to change phase is the legal per-phase methods
    /// below, so an illegal jump has no expressible form for an external
    /// caller (there is no `advance::<AnyPhase>` in the public API).
    fn advance<Q: Phase>(self) -> Node<Q> {
        Node {
            id: self.id,
            aws_instance_id: self.aws_instance_id,
            k8s_node_ref: self.k8s_node_ref,
            abort_reason: self.abort_reason,
            _p: PhantomData,
        }
    }

    fn into_aborted(mut self, reason: impl Into<String>) -> Node<Aborted> {
        self.abort_reason = Some(reason.into());
        self.advance()
    }

    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.id
    }
    /// The runtime phase label of this typed value — `P::FASE`.
    #[must_use]
    pub fn fase(&self) -> FaseNode {
        P::FASE
    }
    /// `Some` from `Joining` onward (set once, at [`Node::joined`]); `None` in
    /// `Requested`/`Provisioning` — structurally, since `joined` is the only
    /// place this field is ever populated.
    #[must_use]
    pub fn aws_instance_id(&self) -> Option<&InstanceId> {
        self.aws_instance_id.as_ref()
    }
    #[must_use]
    pub fn k8s_node_ref(&self) -> Option<&K8sNodeRef> {
        self.k8s_node_ref.as_ref()
    }
}

impl Node<Requested> {
    /// The FSM's only entry point. Mints a fresh, unprovisioned record.
    #[must_use]
    pub fn request(id: NodeId) -> Self {
        Node {
            id,
            aws_instance_id: None,
            k8s_node_ref: None,
            abort_reason: None,
            _p: PhantomData,
        }
    }
    /// Requested → Provisioning.
    #[must_use]
    pub fn provision(self) -> Node<Provisioning> {
        self.advance()
    }
    /// Requested → Aborted (refused before any cloud call was made — e.g. the
    /// spend-rate breaker in [`crate::breaker`] tripped `Open`). Structurally
    /// cheap: no instance was ever tracked, so nothing needs reclaiming.
    #[must_use]
    pub fn abort(self, reason: impl Into<String>) -> Node<Aborted> {
        self.into_aborted(reason)
    }
}

impl Node<Provisioning> {
    /// Provisioning → Joining. The ONLY place `aws_instance_id` is ever set —
    /// every phase from here on structurally carries `Some(_)`.
    #[must_use]
    pub fn joined(self, instance: InstanceId) -> Node<Joining> {
        let mut n = self.advance::<Joining>();
        n.aws_instance_id = Some(instance);
        n
    }
    /// Provisioning → Aborted (the cloud call failed before returning an
    /// instance id — e.g. `RunInstances` errored). No instance to reclaim.
    #[must_use]
    pub fn abort(self, reason: impl Into<String>) -> Node<Aborted> {
        self.into_aborted(reason)
    }
}

impl Node<Joining> {
    /// Joining → Active: the instance registered as a real k8s `Node`.
    #[must_use]
    pub fn ready(self, k8s_ref: K8sNodeRef) -> Node<Active> {
        let mut n = self.advance::<Active>();
        n.k8s_node_ref = Some(k8s_ref);
        n
    }
    /// Joining → Aborted (booted but never joined — e.g. kubelet never
    /// registered before a deadline). **Unlike the two abort edges above,
    /// this record DOES carry a live `Some(aws_instance_id)`** — a real AWS
    /// liability exists. [`crate::drift`]'s reconciler treats any
    /// `Aborted`-with-instance record as an immediate (no-grace-window)
    /// reclaim target, distinct from an organic orphan (see its module doc).
    #[must_use]
    pub fn abort(self, reason: impl Into<String>) -> Node<Aborted> {
        self.into_aborted(reason)
    }
}

impl Node<Active> {
    /// Active → Draining. **No `abort()` exists on this phase** — a running
    /// node is drained, never abandoned; the type system makes "abort a live
    /// node" and "drain a live node" different facts with different words,
    /// not two branches of one method.
    #[must_use]
    pub fn drain(self) -> Node<Draining> {
        self.advance()
    }
}

impl Node<Draining> {
    /// Draining → `TerminationIssued`. Sole forward edge — draining cannot be
    /// skipped and cannot dead-end (every `Draining` value has exactly one
    /// legal next phase).
    #[must_use]
    pub fn issue_termination(self) -> Node<TerminationIssued> {
        self.advance()
    }
}

impl Node<TerminationIssued> {
    /// **The SOLE constructor of [`Reaped`]** — `TerminationIssued`'s only
    /// exit, and it REQUIRES an [`AwsTerminationConfirmed`] witness argument:
    /// there is no zero-argument or Result-agnostic way to claim "reaped."
    ///
    /// Tier-honest: requiring the argument is truly-unrepresentable (E0061
    /// on a missing witness). The witness's CONTENT being truthful is not —
    /// `AwsTerminationConfirmed` is a typed carrier for an external AWS
    /// observation (C2), not a cryptographic attestation; nothing in this
    /// crate proves the caller actually queried AWS. What IS mechanically
    /// checked here: if this record already carries a tracked
    /// `aws_instance_id` and the witness names a DIFFERENT instance, that is
    /// a caller bug and is rejected rather than silently accepted.
    ///
    /// # Errors
    /// [`LifecycleError::WitnessInstanceMismatch`] if the witness names a
    /// different instance than this record tracked. In practice this arm is
    /// unreachable via the public API (only [`Node::joined`] ever sets
    /// `aws_instance_id`, and every path to `TerminationIssued` passes
    /// through it) — checked anyway because the field's TYPE (`Option<_>`)
    /// does not itself prove it, only the call graph does.
    pub fn confirm_reaped(
        self,
        witness: AwsTerminationConfirmed,
    ) -> Result<Reaped, LifecycleError> {
        if let Some(tracked) = &self.aws_instance_id
            && tracked != &witness.instance_id
        {
            return Err(LifecycleError::WitnessInstanceMismatch {
                expected: tracked.clone(),
                got: witness.instance_id,
            });
        }
        Ok(Reaped {
            id: self.id,
            witness,
            _seal: (),
        })
    }
}

// ============================================================================
// AwsTerminationConfirmed — the typed witness carrier.
// ============================================================================

/// A typed carrier for an external observation: "AWS confirms instance `X`
/// reached the `terminated` state at unix time `T`." Its constructor is
/// public (this crate ships no AWS SDK client — producing this value IS the
/// C2 read a real caller performs against `DescribeInstances`). The private
/// `_seal` field is structural hygiene (blocks a struct-literal bypassing
/// `new`), not a security boundary — see the module-level tier note.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsTerminationConfirmed {
    instance_id: InstanceId,
    confirmed_at_unix: u64,
    _seal: (),
}

impl AwsTerminationConfirmed {
    #[must_use]
    pub fn new(instance_id: InstanceId, confirmed_at_unix: u64) -> Self {
        Self {
            instance_id,
            confirmed_at_unix,
            _seal: (),
        }
    }
    #[must_use]
    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }
    #[must_use]
    pub fn confirmed_at_unix(&self) -> u64 {
        self.confirmed_at_unix
    }
}

/// Errors [`Node::confirm_reaped`] can return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleError {
    WitnessInstanceMismatch {
        expected: InstanceId,
        got: InstanceId,
    },
}

impl std::fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WitnessInstanceMismatch { expected, got } => {
                write!(
                    f,
                    "termination witness names instance {got} but this record tracked {expected}"
                )
            }
        }
    }
}

impl std::error::Error for LifecycleError {}

// ============================================================================
// Reaped — the sealed, structurally-erased good terminal.
// ============================================================================

/// The FSM's final good terminal. **A DISTINCT struct, not `Node<Reaped>`
/// carrying an inert `Aborted`-shaped payload** — it has NO `aws_instance_id`
/// / `k8s_node_ref` fields at all. "We believe this is reaped but still track
/// its instance" has no representable value: the fields that WOULD carry that
/// belief do not exist on this type. Its sole constructor is
/// [`Node::confirm_reaped`]; the private fields (+ `_seal`) block a
/// struct-literal bypass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reaped {
    id: NodeId,
    witness: AwsTerminationConfirmed,
    _seal: (),
}

impl Reaped {
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.id
    }
    #[must_use]
    pub fn witness(&self) -> &AwsTerminationConfirmed {
        &self.witness
    }
    /// Always `FaseNode::Reaped` — provided for symmetry with `Node::fase`.
    #[must_use]
    pub fn fase(&self) -> FaseNode {
        FaseNode::Reaped
    }
}

#[cfg(test)]
mod tests;
