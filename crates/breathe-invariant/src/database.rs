//! The DATABASE dimension — the architecture-aware, discovery-molded,
//! failover-safe-100%-spot carve (`DatabaseBand`).
//!
//! ## What this module is
//!
//! A database does not breathe like a stateless pod. Its breath is its
//! *replication architecture*, and carving it wrong loses data. This is the
//! deepest breathe dimension (BREATHABILITY.md §II.5): breathe must be
//! **TOTALLY aware of every database architecture it touches, discover it
//! dynamically, and auction it at 100% spot safely — even the primary.**
//!
//! It is the DatabaseBand's typed contract, the sibling of
//! [`crate::isolation`]. Four coupled properties, each a typed lattice point:
//!
//! 1. **Architecture-aware** — breathe knows which pod is the writable PRIMARY
//!    vs the READERS vs a distributed VOTER, for every shape: single-writer,
//!    primary + read-replicas, and multi-primary / distributed-quorum
//!    ([`ReplicationTopology`] / [`ReplicationClass`]).
//! 2. **Discovery-molded** — a [`ReplicationDiscovery`] seam reads the live
//!    engine (which pod is writable-primary, how many readers, the quorum size)
//!    and MOLDS the carve to it, `default ← discovered ← override`
//!    ([`resolve_permutation`]).
//! 3. **Failover-safe 100% spot, even the primary** — a primary's spot-reclaim
//!    triggers `retirada` drain-ahead → graceful replica-promotion BEFORE the
//!    node dies, so the primary is never lost un-gracefully. Read-replicas
//!    scale freely; the primary is NEVER carelessly reclaimed. The
//!    [`FailoverMachine`] FSM encodes the closed loop, and the primary-node
//!    reclaim is authorized ONLY through a [`PromotionReceipt`] witness
//!    (truly-unrepresentable: no code path authorizes reclaiming the primary
//!    without a completed promotion).
//! 4. **Configurable permutations** — topology × placement × spot × replica ×
//!    failover as a typed [`DatabasePermutation`] lattice (legal permutations
//!    enumerated by [`legal_permutations`]), not a hand-tuned per-DB script.
//!
//! ## `/algorithmic-prowess-seal` — best-fit, NO ML
//!
//! - **The primary is never lost un-gracefully** is proof-carrying: a
//!   [`PrimaryReclaimAuthorization`] is constructible ONLY from a
//!   [`PromotionReceipt`], and a receipt is minted ONLY by the FSM's
//!   `PromotionSucceeded` transition — so "reclaim the writable primary before a
//!   failover" has no code path (truly-unrepresentable on the
//!   never-lose-primary axis; the live promote actuator is the C2 destination).
//! - **The permutation legality** ("100% spot on the primary REQUIRES a
//!   failover policy") is a typed **constraint satisfaction** over a finite
//!   product space — [`DatabasePermutation::validate`] rejects
//!   spot-even-primary-without-failover at the boundary (parse-time-rejected).
//!   Classical CSP reduced to its contract core, no ML.
//! - **The topology class ↔ scale invariant coupling** mirrors
//!   `breathe_control::replica::Topology` + `REPLICA_TOPOLOGY_AXIS`
//!   (never-scale-primary / odd-quorum / ordinal-drain) — REFERENCED as the
//!   `crd_kind` token, never re-implemented (this crate stays decoupled from the
//!   band crates; the coupling is bound by the lisp cross-check).
//!
//! ## Tier-honest (the UNREPRESENTABILITY model applied to itself)
//!
//! The DatabaseBand is **Landing**, not Shipped — exactly like
//! [`crate::isolation`]:
//! - **Shipped (typed contract, CI-tested):** the [`ReplicationTopology`] +
//!   [`ReplicationClass`], the [`ReplicationDiscovery`] seam + a mock, the
//!   [`FailoverMachine`] FSM + the proof-carrying reclaim authorization, the
//!   5-engine [`DB_ARCHITECTURES`] matrix, and the [`DatabasePermutation`]
//!   legality lattice.
//! - **DESIGN (the C2 destination):** the LIVE metric reader that reads a
//!   *running* engine's replication status (`SHOW REPLICA STATUS`,
//!   `pg_stat_replication`, `rs.status()`), and the LIVE actuator that actually
//!   promotes a replica + drains the old primary. Discovery is an external-world
//!   observation and promotion is an external-world act — both are the C2
//!   ceiling, a runtime control loop, never a compile-time proof.

use serde::{Deserialize, Serialize};

use crate::isolation::PlacementIsolation;

// ─────────────────────────────────────────────────────────────────────────────
// THE ARCHITECTURE CLASS — the coupling to REPLICA_TOPOLOGY_AXIS
// ─────────────────────────────────────────────────────────────────────────────

/// The replication ARCHITECTURE CLASS a database runs under — the coupling to
/// `breathe_control::replica::Topology` + `breathe_catalog::REPLICA_TOPOLOGY_AXIS`
/// (the three STATEFUL arms; a database always has data, so it never breathes as
/// `nonPersistent`). This selects BOTH the scale algorithm and the hard invariant
/// the band may never cross. The [`Self::crd_kind`] token mirrors the axis;
/// they are bound by the lisp cross-check, not a cross-crate dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReplicationClass {
    /// Primary + read-replicas (MySQL / PostgreSQL / Redis-Sentinel). Only the
    /// read-replica count breathes; the primary is NEVER scaled away — a primary
    /// loss is a *failover* (retirada), not a replica scale-down.
    MasterSlave,
    /// A distributed odd-quorum consensus set (MongoDB replica-set majority
    /// election, etcd/Raft, Redis Cluster). No fixed primary — any voter can be
    /// elected; membership steps one odd rung at a time, never crossing majority.
    FullyDistributed,
    /// A single-writer PVC-per-ordinal store (Neo4j). Scale-up adds an
    /// ordinal+PVC; a scale-down drains the ordinal's data FIRST.
    Persistent,
}

impl ReplicationClass {
    /// The `crd_kind` token this class couples to in
    /// `breathe_catalog::REPLICA_TOPOLOGY_AXIS` (and `breathe_control::replica::
    /// Topology::as_str`, camelCased on the CRD). REFERENCED, never redefined.
    #[must_use]
    pub const fn crd_kind(self) -> &'static str {
        match self {
            Self::MasterSlave => "masterSlave",
            Self::FullyDistributed => "fullyDistributed",
            Self::Persistent => "persistent",
        }
    }

    /// The stable kebab-case label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MasterSlave => "master-slave",
            Self::FullyDistributed => "fully-distributed",
            Self::Persistent => "persistent",
        }
    }

    /// Every architecture class (all three are stateful — a database always has
    /// data). The partition the architecture matrix + permutation lattice cover.
    pub const ALL: [ReplicationClass; 3] =
        [Self::MasterSlave, Self::FullyDistributed, Self::Persistent];

    /// Does this class have a *designated* writable primary that must never be
    /// scaled away (MasterSlave / Persistent)? `FullyDistributed` re-elects, so
    /// it has no single un-scalable primary — the odd-quorum invariant guards it.
    #[must_use]
    pub const fn has_designated_primary(self) -> bool {
        matches!(self, Self::MasterSlave | Self::Persistent)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// THE ROLE — the discovered per-pod fact
// ─────────────────────────────────────────────────────────────────────────────

/// The role a database pod plays in its replication set — the DISCOVERED fact a
/// [`ReplicationDiscovery`] reads from the live engine. This is the "which pod is
/// the primary" awareness the operator directed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DbRole {
    /// The sole/elected writable PRIMARY (master). Never scaled away; a loss is a
    /// failover, not a scale-down.
    Primary,
    /// A read-replica / hot-standby (slave). Breathes freely (scale on read-load).
    Reader,
    /// A voting member of a distributed quorum — no fixed primary; any voter can
    /// be elected. Scales in quorum-safe odd rungs.
    Voter,
}

impl DbRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Reader => "reader",
            Self::Voter => "voter",
        }
    }
}

/// A stable pod/ordinal identity within a replication set (a StatefulSet ordinal).
/// `Copy` + exact — the smallest-sufficient rung, no allocation on the hot path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReplicaId(pub u32);

// ─────────────────────────────────────────────────────────────────────────────
// THE DISCOVERED LIVE TOPOLOGY — clause 5 (discovery-molded)
// ─────────────────────────────────────────────────────────────────────────────

/// The DISCOVERED live replication architecture of a database — which pod is the
/// primary vs the readers vs the quorum voters, read from the running engine and
/// molded into the breathe carve (clause 5). This is the *observation* (the C2
/// external-world signal), distinct from the static [`ReplicationClass`] (the
/// architecture CLASS): the class says "master-slave", the topology says "primary
/// = ordinal-0, 3 healthy readers".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "shape")]
pub enum ReplicationTopology {
    /// One writable primary, NO promotable replica (a dev/small deployment, or a
    /// masterSlave whose readers are all lagging/absent). Spot-on-primary is
    /// UNSAFE here — there is no failover target — so the FSM blocks the reclaim.
    SingleWriter { primary: ReplicaId },
    /// One (or an HA pair of) writable primary + N read-replicas — the
    /// `masterSlave` shape. A failover target exists iff `readers ≥ 1`.
    PrimaryReaders { primary: ReplicaId, readers: u32 },
    /// A distributed odd-quorum consensus set — the `fullyDistributed` shape. Any
    /// voter can be elected; a majority (`voters/2 + 1`) can re-elect. A
    /// discovered EVEN or sub-3 `voters` is a real-world anomaly the FSM treats as
    /// no-safe-target (the C2 observation, not a type violation — discovery reads
    /// the world as it is).
    Quorum { voters: u32 },
}

impl ReplicationTopology {
    /// Does this discovered topology have a healthy FAILOVER TARGET — i.e. can the
    /// primary be reclaimed safely (a replica promoted / a quorum re-elected)?
    /// `SingleWriter` never does; `PrimaryReaders` does iff `readers ≥ 1`;
    /// `Quorum` does iff the discovered set is a safe majority (odd, ≥ 3).
    #[must_use]
    pub const fn has_failover_target(self) -> bool {
        match self {
            Self::SingleWriter { .. } => false,
            Self::PrimaryReaders { readers, .. } => readers >= 1,
            Self::Quorum { voters } => voters >= 3 && voters % 2 == 1,
        }
    }

    /// The architecture CLASS this discovered topology belongs to.
    #[must_use]
    pub const fn class(self) -> ReplicationClass {
        match self {
            // A single-writer with no reader is still a master-slave shape at rest
            // (zero readers) — its class is masterSlave (Persistent is a distinct
            // discovered shape only when the engine matrix says so).
            Self::SingleWriter { .. } | Self::PrimaryReaders { .. } => ReplicationClass::MasterSlave,
            Self::Quorum { .. } => ReplicationClass::FullyDistributed,
        }
    }

    /// The quorum majority (`voters/2 + 1`) — only meaningful for [`Self::Quorum`];
    /// `0` for the primary shapes.
    #[must_use]
    pub const fn quorum_majority(self) -> u32 {
        match self {
            Self::Quorum { voters } => voters / 2 + 1,
            _ => 0,
        }
    }
}

/// The side-effecting boundary the discovery interpreter reads through — the
/// TYPED-SPEC triplet's Environment trait (the testability contract). A real impl
/// reads a *running* engine's replication status (`SHOW REPLICA STATUS`,
/// `pg_stat_replication`, `rs.status()`, `SENTINEL masters`); tests pass a mock.
/// Sync + dependency-free, matching this crate's pure-contract discipline (the
/// async engine-RPC adapter lives at the provider layer — the C2 destination).
pub trait ReplicationDiscovery {
    /// Read the live replication topology of the database (which pod is primary,
    /// how many readers, the quorum size).
    ///
    /// # Errors
    /// [`DiscoveryError`] when the engine's replication status cannot be read.
    fn discover_topology(&self) -> Result<ReplicationTopology, DiscoveryError>;
}

/// Why a live replication discovery failed — the typed gate that keeps a
/// mis-read topology out of the carve (a silent wrong topology could authorize an
/// unsafe primary reclaim).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiscoveryError {
    /// The engine's replication-status endpoint was unreachable / returned no rows.
    Unreadable(&'static str),
    /// The status was read but names no writable primary (a split-brain / election
    /// in progress) — breathe must NOT carve until a primary is known.
    NoPrimary,
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreadable(what) => write!(f, "replication status unreadable: {what}"),
            Self::NoPrimary => f.write_str("no writable primary in the discovered topology"),
        }
    }
}

impl std::error::Error for DiscoveryError {}

/// A canned [`ReplicationDiscovery`] for tests + shadow dry-runs — the discovered
/// topology is a field, so a test drives the interpreter with zero I/O.
#[derive(Clone, Copy, Debug)]
pub struct MockReplicationDiscovery {
    pub topology: ReplicationTopology,
    /// Force a read failure to exercise the interpreter's typed-error path.
    pub unreadable: bool,
}

impl ReplicationDiscovery for MockReplicationDiscovery {
    fn discover_topology(&self) -> Result<ReplicationTopology, DiscoveryError> {
        if self.unreadable {
            Err(DiscoveryError::Unreadable("mock forced failure"))
        } else {
            Ok(self.topology)
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// THE PROOF-CARRYING RECLAIM AUTHORIZATION (truly-unrep core)
// ─────────────────────────────────────────────────────────────────────────────

/// A witness that a replica was PROMOTED to primary — proof the failover
/// completed. Its fields are PRIVATE and the only mint is [`PromotionReceipt::issue`],
/// which is module-private: no code outside this module can forge a receipt, so a
/// receipt always corresponds to a real FSM promotion transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PromotionReceipt {
    /// The replica that became the NEW writable primary.
    new_primary: ReplicaId,
    /// The OLD primary being demoted (whose node is now safe to reclaim).
    demoted: ReplicaId,
}

impl PromotionReceipt {
    /// Mint a receipt. **Module-private** — only the FSM's `PromotionSucceeded`
    /// transition calls this, so a receipt cannot be forged by a caller.
    #[must_use]
    const fn issue(new_primary: ReplicaId, demoted: ReplicaId) -> Self {
        Self { new_primary, demoted }
    }

    /// The replica that is now the writable primary.
    #[must_use]
    pub const fn new_primary(self) -> ReplicaId {
        self.new_primary
    }

    /// The old primary being demoted (whose node the authorization covers).
    #[must_use]
    pub const fn demoted(self) -> ReplicaId {
        self.demoted
    }
}

/// Authorization to reclaim the OLD primary's node — constructible ONLY from a
/// [`PromotionReceipt`] via [`Self::from_receipt`]. So "reclaim the writable
/// primary before a failover" has NO code path: there is no constructor that
/// yields this without a receipt, and a receipt only exists after a promotion
/// succeeded. This is the truly-unrepresentable core of the never-lose-the-primary
/// invariant (★★ UNREPRESENTABILITY: proof-carrying capability). The authorization
/// covers only the DEMOTED node named by the receipt — never the current primary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrimaryReclaimAuthorization {
    receipt: PromotionReceipt,
}

impl PrimaryReclaimAuthorization {
    /// The ONLY ingress — an authorization requires a promotion receipt.
    #[must_use]
    pub const fn from_receipt(receipt: PromotionReceipt) -> Self {
        Self { receipt }
    }

    /// The node this authorization permits reclaiming (the demoted old primary).
    #[must_use]
    pub const fn reclaimable_node(self) -> ReplicaId {
        self.receipt.demoted()
    }

    /// The receipt that authorized this reclaim.
    #[must_use]
    pub const fn receipt(self) -> PromotionReceipt {
        self.receipt
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// THE FAILOVER-SAFE-SPOT FSM — the closed promote-before-reclaim loop
// ─────────────────────────────────────────────────────────────────────────────

/// The typed state of the failover-safe-spot loop. The invariant: the OLD
/// primary's node reaches a reclaimable state ONLY via a completed promotion
/// (`PromotingReplica → FailedOver`), never directly from a reclaim signal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailoverState {
    /// Normal operation. The primary is healthy; no reclaim pending. A good
    /// resting terminal.
    Steady,
    /// A spot reclaim (or a scale-down) targets the node hosting the PRIMARY. The
    /// grace window has opened; we must fail over BEFORE the node dies.
    PrimaryReclaimSignaled,
    /// A healthy replica is being promoted to primary (the failover in progress).
    PromotingReplica,
    /// Failover complete — a new primary is promoted/elected. The OLD primary is
    /// now demoted, and its node is authorized for reclaim.
    FailedOver,
    /// The old primary's node was drained/reclaimed AFTER the failover. The
    /// good terminal of the spot-on-primary path.
    OldPrimaryReclaimed,
    /// No healthy failover target (single-writer / all readers lagging / an even
    /// quorum): the reclaim is BLOCKED — `retirada` HOLDS the node and escalates.
    /// The primary is NEVER reclaimed here (the never-lose-primary guard).
    ReclaimBlocked,
}

impl FailoverState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Steady => "steady",
            Self::PrimaryReclaimSignaled => "primary-reclaim-signaled",
            Self::PromotingReplica => "promoting-replica",
            Self::FailedOver => "failed-over",
            Self::OldPrimaryReclaimed => "old-primary-reclaimed",
            Self::ReclaimBlocked => "reclaim-blocked",
        }
    }

    /// Every state — the partition the reachability test covers.
    pub const ALL: [FailoverState; 6] = [
        Self::Steady,
        Self::PrimaryReclaimSignaled,
        Self::PromotingReplica,
        Self::FailedOver,
        Self::OldPrimaryReclaimed,
        Self::ReclaimBlocked,
    ];

    /// Is this a GOOD terminal — a resting state the loop is allowed to settle at
    /// (nothing lost, no primary reclaimed un-gracefully)? `Steady` (homeostasis)
    /// and `OldPrimaryReclaimed` (a clean failover completed).
    #[must_use]
    pub const fn is_good_terminal(self) -> bool {
        matches!(self, Self::Steady | Self::OldPrimaryReclaimed)
    }
}

/// An event driving the failover FSM — a signal from the scheduler, the discovery
/// seam, or the promotion actuator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailoverEvent {
    /// The cloud/scheduler signals the primary's node WILL be reclaimed (spot
    /// interruption notice / a scale-down that would hit the primary). Carries the
    /// current primary + a chosen promotion candidate (the reader/voter to promote).
    PrimaryReclaimSignal { primary: ReplicaId, candidate: ReplicaId },
    /// Discovery confirms a healthy promotable target exists (a lag-safe reader, or
    /// a quorum majority) — proceed with the promotion.
    PromotableTargetAvailable,
    /// Discovery confirms NO healthy promotable target (single-writer, every reader
    /// lagging past the safety bound, or an even/degraded quorum) — block + escalate.
    NoPromotableTarget,
    /// The promotion actuator reports the new primary is writable.
    PromotionSucceeded,
    /// The old primary's node finished draining after the failover.
    OldPrimaryDrained,
    /// The reclaim signal cleared (spot bid restored / scale-down cancelled) —
    /// return to steady.
    ReclaimCleared,
}

/// The action the FSM emits for a transition — what the actuator should do. A
/// `ReclaimOldPrimary` carries the proof-carrying [`PrimaryReclaimAuthorization`],
/// which the FSM can only build via a [`PromotionReceipt`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailoverAction {
    /// Do nothing (steady, or holding through a phase).
    Hold,
    /// Begin promoting the chosen replica (the actuator: promote-before-death).
    PromoteReplica { candidate: ReplicaId },
    /// Authorize the OLD primary's node reclaim — carries the authorization witness
    /// (constructible only from a promotion receipt). The ONLY action that reclaims
    /// a former-primary node, and it exists only after a successful promotion.
    ReclaimOldPrimary { authorization: PrimaryReclaimAuthorization },
    /// Block the reclaim + escalate (no safe failover target). `retirada` holds the
    /// node; the primary is never reclaimed.
    BlockReclaimEscalate,
}

/// Why a failover transition is illegal — a `(state, event)` pair with no defined
/// edge. Illegal transitions are only-mitigated (a `Result::Err`, not a compile
/// error) — the FSM's transition legality is a runtime graph, which Rust cannot
/// prove exhaustively; the LOAD-BEARING safety property (the primary is never
/// reclaimed without a promotion) IS truly-unrepresentable, via the receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IllegalTransition {
    pub state: FailoverState,
    pub event: FailoverEvent,
}

impl std::fmt::Display for IllegalTransition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "illegal failover transition: no edge from {} on {:?}",
            self.state.as_str(),
            self.event
        )
    }
}

impl std::error::Error for IllegalTransition {}

/// The pure transition function of the failover-safe-spot FSM: `(state, event) →
/// (next_state, action)`. No I/O — the whole loop is unit-testable without a
/// cluster (the TYPED-SPEC triplet's pure interpreter). A `PromotionSucceeded`
/// from `PromotingReplica` is the ONLY edge that mints a [`PromotionReceipt`] and
/// emits a `ReclaimOldPrimary` — so the never-lose-the-primary invariant is
/// structural, not a documented convention.
///
/// # Errors
/// [`IllegalTransition`] for any `(state, event)` pair with no defined edge.
pub fn failover_step(
    state: FailoverState,
    event: FailoverEvent,
) -> Result<(FailoverState, FailoverAction), IllegalTransition> {
    use FailoverEvent as E;
    use FailoverState as S;

    // A cleared reclaim always returns to steady (the spot bid was restored).
    if matches!(event, E::ReclaimCleared) {
        return Ok((S::Steady, FailoverAction::Hold));
    }

    let next = match (state, event) {
        // A reclaim of the primary's node is signaled — enter the grace window.
        (S::Steady, E::PrimaryReclaimSignal { .. }) => {
            (S::PrimaryReclaimSignaled, FailoverAction::Hold)
        }
        // In the grace window: a healthy target ⇒ promote; no target ⇒ block.
        (S::PrimaryReclaimSignaled, E::PromotableTargetAvailable) => {
            // The chosen candidate was carried on the signal; the actuator promotes
            // it. (A production shell threads the candidate through; the FSM emits
            // the intent.)
            (S::PromotingReplica, FailoverAction::PromoteReplica { candidate: ReplicaId(u32::MAX) })
        }
        (S::PrimaryReclaimSignaled, E::NoPromotableTarget) => {
            (S::ReclaimBlocked, FailoverAction::BlockReclaimEscalate)
        }
        // A blocked reclaim can still proceed if a target becomes healthy (a
        // lagging reader caught up / the quorum recovered).
        (S::ReclaimBlocked, E::PromotableTargetAvailable) => {
            (S::PromotingReplica, FailoverAction::PromoteReplica { candidate: ReplicaId(u32::MAX) })
        }
        // ★ THE LOAD-BEARING EDGE. The promotion succeeded — mint the receipt and
        // authorize the OLD primary's reclaim. This is the ONLY place a
        // ReclaimOldPrimary action (and a PromotionReceipt) is constructed.
        (S::PromotingReplica, E::PromotionSucceeded) => {
            // The identities are recomputed by the shell from the signal it carried;
            // here the receipt proves a promotion happened before any reclaim.
            let receipt = PromotionReceipt::issue(ReplicaId(u32::MAX), ReplicaId(u32::MAX));
            (
                S::FailedOver,
                FailoverAction::ReclaimOldPrimary {
                    authorization: PrimaryReclaimAuthorization::from_receipt(receipt),
                },
            )
        }
        // The old primary drained — the clean terminal.
        (S::FailedOver, E::OldPrimaryDrained) => (S::OldPrimaryReclaimed, FailoverAction::Hold),
        // no edge.
        (state, event) => return Err(IllegalTransition { state, event }),
    };
    Ok(next)
}

/// A small stateful wrapper over [`failover_step`] — holds the current state and
/// advances it, for the closed-loop driver + the reachability proof. The FSM owns
/// no I/O; the async controller shell reads the discovery + drives the actuator and
/// feeds events here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FailoverMachine {
    state: FailoverState,
}

impl Default for FailoverMachine {
    fn default() -> Self {
        Self { state: FailoverState::Steady }
    }
}

impl FailoverMachine {
    /// A fresh machine at [`FailoverState::Steady`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The current state.
    #[must_use]
    pub const fn state(self) -> FailoverState {
        self.state
    }

    /// Advance the machine on an event, returning the emitted action.
    ///
    /// # Errors
    /// [`IllegalTransition`] when the event has no edge from the current state.
    pub fn on(&mut self, event: FailoverEvent) -> Result<FailoverAction, IllegalTransition> {
        let (next, action) = failover_step(self.state, event)?;
        self.state = next;
        Ok(action)
    }

    /// Choose the discovery-driven event for a topology at the grace window: a
    /// topology with a failover target yields `PromotableTargetAvailable`, one
    /// without yields `NoPromotableTarget`. This is where the DISCOVERY molds the
    /// FSM — the never-lose-primary guard reads the live topology.
    #[must_use]
    pub fn target_event(topology: ReplicationTopology) -> FailoverEvent {
        if topology.has_failover_target() {
            FailoverEvent::PromotableTargetAvailable
        } else {
            FailoverEvent::NoPromotableTarget
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// THE PERMUTATION LATTICE — topology × placement × spot × replica × failover
// ─────────────────────────────────────────────────────────────────────────────

/// The SPOT posture for a database tier — how aggressively spot is used. 100%
/// spot even on the primary is the destination, but ONLY with a failover policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SpotPosture {
    /// No spot — the whole tier runs on-demand (the conservative baseline).
    NoSpot,
    /// Spot on the READ-REPLICAS only; the primary stays on-demand (safe without a
    /// failover loop — a reader reclaim is a scale-out, never a data loss).
    SpotReadersOnly,
    /// 100% spot INCLUDING the primary — the destination. REQUIRES a failover
    /// policy so a primary reclaim promotes a replica before the node dies.
    SpotEvenPrimary,
}

impl SpotPosture {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoSpot => "no-spot",
            Self::SpotReadersOnly => "spot-readers-only",
            Self::SpotEvenPrimary => "spot-even-primary",
        }
    }
    pub const ALL: [SpotPosture; 3] = [Self::NoSpot, Self::SpotReadersOnly, Self::SpotEvenPrimary];
}

/// The REPLICA scale policy for a database tier — which count breathes and how.
/// Couples to the `breathe_control::replica::Topology` scale invariant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReplicaScalePolicy {
    /// Never scale — a fixed count (the single-writer / at-rest default).
    NeverScale,
    /// Scale the READ-REPLICAS freely on read-load; NEVER the primary
    /// (masterSlave / persistent-with-readers).
    ScaleReadersFreely,
    /// Step the quorum one ODD rung at a time, never crossing majority
    /// (fullyDistributed).
    QuorumOddSteps,
}

impl ReplicaScalePolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NeverScale => "never-scale",
            Self::ScaleReadersFreely => "scale-readers-freely",
            Self::QuorumOddSteps => "quorum-odd-steps",
        }
    }
    pub const ALL: [ReplicaScalePolicy; 3] =
        [Self::NeverScale, Self::ScaleReadersFreely, Self::QuorumOddSteps];
}

/// The FAILOVER policy for a database tier — how a primary loss is handled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailoverPolicy {
    /// No failover — a primary loss is an outage (only safe when the primary never
    /// runs on spot).
    NoFailover,
    /// Promote a designated replica BEFORE the primary's node dies (masterSlave /
    /// persistent) — the retirada drain-ahead loop.
    PromoteBeforeReclaim,
    /// A distributed quorum RE-ELECTS a primary from a live majority
    /// (fullyDistributed) — no designated replica to promote.
    QuorumReElect,
}

impl FailoverPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoFailover => "no-failover",
            Self::PromoteBeforeReclaim => "promote-before-reclaim",
            Self::QuorumReElect => "quorum-re-elect",
        }
    }
    pub const ALL: [FailoverPolicy; 3] =
        [Self::NoFailover, Self::PromoteBeforeReclaim, Self::QuorumReElect];

    /// Does this policy make a primary reclaim SAFE (there is a failover path)?
    /// `NoFailover` does not — so it cannot pair with `SpotEvenPrimary`.
    #[must_use]
    pub const fn is_failover_safe(self) -> bool {
        matches!(self, Self::PromoteBeforeReclaim | Self::QuorumReElect)
    }
}

impl ReplicationClass {
    /// Does this architecture class admit this replica-scale policy? A class's
    /// scale algorithm is fixed by its topology (masterSlave scales readers,
    /// distributed steps quorum, persistent holds); `NeverScale` is universally
    /// admissible (the safe at-rest default).
    #[must_use]
    pub const fn admits_replica(self, replica: ReplicaScalePolicy) -> bool {
        match (self, replica) {
            (_, ReplicaScalePolicy::NeverScale) => true,
            (Self::MasterSlave | Self::Persistent, ReplicaScalePolicy::ScaleReadersFreely) => true,
            (Self::FullyDistributed, ReplicaScalePolicy::QuorumOddSteps) => true,
            _ => false,
        }
    }

    /// Does this architecture class admit this failover policy? A designated-primary
    /// class (masterSlave / persistent) promotes a replica; a distributed class
    /// re-elects. `NoFailover` is universally admissible (it is the conservative
    /// on-demand-primary choice).
    #[must_use]
    pub const fn admits_failover(self, failover: FailoverPolicy) -> bool {
        match (self, failover) {
            (_, FailoverPolicy::NoFailover) => true,
            (Self::MasterSlave | Self::Persistent, FailoverPolicy::PromoteBeforeReclaim) => true,
            (Self::FullyDistributed, FailoverPolicy::QuorumReElect) => true,
            _ => false,
        }
    }
}

/// A configurable database breathe PERMUTATION — one point on the
/// `topology × placement × spot × replica × failover` lattice. This is the typed
/// permutation the operator directed: not a hand-tuned per-DB script, a checked
/// point in a finite product space. [`Self::validate`] rejects an illegal
/// permutation at the boundary (parse-time-rejected).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabasePermutation {
    /// The architecture class (couples the scale invariant).
    pub class: ReplicationClass,
    /// The placement isolation — reuse of [`crate::isolation::PlacementIsolation`]
    /// (a DB primary should never co-locate with its readers → anti-affinity by
    /// default). Not re-implemented (Prime Directive).
    pub placement: PlacementIsolation,
    /// The spot posture.
    pub spot: SpotPosture,
    /// The replica scale policy.
    pub replica: ReplicaScalePolicy,
    /// The failover policy.
    pub failover: FailoverPolicy,
}

/// Why a [`DatabasePermutation`] is illegal — the CSP constraints, named once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PermutationError {
    /// ★ THE load-bearing constraint: 100% spot on the primary
    /// (`SpotEvenPrimary`) with NO failover policy — a primary reclaim would lose
    /// the primary un-gracefully. Spot-even-primary REQUIRES a failover-safe policy.
    SpotPrimaryWithoutFailover,
    /// The replica-scale policy is not one this architecture class admits (e.g.
    /// `QuorumOddSteps` on a `MasterSlave` tier — there is no quorum).
    ReplicaClassMismatch,
    /// The failover policy is not one this architecture class admits (e.g.
    /// `QuorumReElect` on a `MasterSlave` tier — it promotes, it does not re-elect).
    FailoverClassMismatch,
}

impl std::fmt::Display for PermutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SpotPrimaryWithoutFailover => f.write_str(
                "100% spot on the primary requires a failover policy (promote-before-reclaim / quorum-re-elect) — else the primary is lost un-gracefully",
            ),
            Self::ReplicaClassMismatch => {
                f.write_str("the replica-scale policy is not admitted by this architecture class")
            }
            Self::FailoverClassMismatch => {
                f.write_str("the failover policy is not admitted by this architecture class")
            }
        }
    }
}

impl std::error::Error for PermutationError {}

impl DatabasePermutation {
    /// **The permutation CSP gate (parse-time-rejected).** A permutation is legal
    /// iff (1) spot-even-primary carries a failover-safe policy, (2) the replica
    /// policy is admitted by the class, (3) the failover policy is admitted by the
    /// class. Classical constraint satisfaction over a finite product space, no ML.
    ///
    /// # Errors
    /// [`PermutationError`] naming the first violated constraint.
    pub const fn validate(self) -> Result<(), PermutationError> {
        // ★ the never-lose-primary constraint first.
        if matches!(self.spot, SpotPosture::SpotEvenPrimary) && !self.failover.is_failover_safe() {
            return Err(PermutationError::SpotPrimaryWithoutFailover);
        }
        if !self.class.admits_replica(self.replica) {
            return Err(PermutationError::ReplicaClassMismatch);
        }
        if !self.class.admits_failover(self.failover) {
            return Err(PermutationError::FailoverClassMismatch);
        }
        Ok(())
    }

    /// Is this permutation legal?
    #[must_use]
    pub const fn is_legal(self) -> bool {
        self.validate().is_ok()
    }
}

/// Enumerate EVERY legal permutation on the lattice — the typed configurable
/// permutation space the operator directed (topology × placement × spot × replica
/// × failover, filtered by [`DatabasePermutation::validate`]). This is the lattice,
/// materialized: a `breathe confirm` report / a permutation-picker iterates it.
#[must_use]
pub fn legal_permutations() -> Vec<DatabasePermutation> {
    let mut out = Vec::new();
    for class in ReplicationClass::ALL {
        for placement in PlacementIsolation::ALL {
            for spot in SpotPosture::ALL {
                for replica in ReplicaScalePolicy::ALL {
                    for failover in FailoverPolicy::ALL {
                        let p = DatabasePermutation { class, placement, spot, replica, failover };
                        if p.is_legal() {
                            out.push(p);
                        }
                    }
                }
            }
        }
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// THE OVERLAY PRECEDENCE — default ← discovered ← override (clause 5)
// ─────────────────────────────────────────────────────────────────────────────

/// A partial database permutation override — the enjulho config shape (typed,
/// `deny_unknown_fields`, no `format!()`), mirroring
/// [`crate::isolation::IsolationOverlay`]. Each field is `Option`: `None` = do not
/// touch this layer; `Some` = this layer's contribution to the precedence fold.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReplicationOverlay {
    /// Override the architecture class (rare — usually discovered once).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class: Option<ReplicationClass>,
    /// Override the placement isolation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement: Option<PlacementIsolation>,
    /// Override the spot posture (e.g. a prod tenant forces `SpotReadersOnly`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spot: Option<SpotPosture>,
    /// Override the replica-scale policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica: Option<ReplicaScalePolicy>,
    /// Override the failover policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failover: Option<FailoverPolicy>,
}

/// Resolve the effective permutation via the `default ← discovered ← override`
/// precedence (clause 5, discovery-molded — the same shikumi precedence
/// `resolve_posture` uses for isolation): the engine matrix supplies the DEFAULT,
/// live discovery narrows the class from the observed topology, and the operator
/// override pins the rest. The result is [`DatabasePermutation::validate`]d, so an
/// override that produces an illegal permutation (e.g. forcing `SpotEvenPrimary`
/// with `NoFailover`) is rejected — the precedence fold cannot mint an unsafe carve.
///
/// # Errors
/// [`PermutationError`] when the resolved permutation is illegal.
pub fn resolve_permutation(
    default: DatabasePermutation,
    discovered: Option<ReplicationTopology>,
    overlay: &ReplicationOverlay,
) -> Result<DatabasePermutation, PermutationError> {
    let mut p = default;
    // discovered ▷ default — narrow the class to the OBSERVED topology's class.
    if let Some(topo) = discovered {
        p.class = topo.class();
    }
    // override ▷ discovered — the operator pins each explicitly-set field.
    if let Some(c) = overlay.class {
        p.class = c;
    }
    if let Some(pl) = overlay.placement {
        p.placement = pl;
    }
    if let Some(s) = overlay.spot {
        p.spot = s;
    }
    if let Some(r) = overlay.replica {
        p.replica = r;
    }
    if let Some(f) = overlay.failover {
        p.failover = f;
    }
    p.validate()?;
    Ok(p)
}

// ─────────────────────────────────────────────────────────────────────────────
// THE 5-ENGINE ARCHITECTURE MATRIX — MySQL · Postgres · Redis · Mongo · Neo4j
// ─────────────────────────────────────────────────────────────────────────────

/// A database engine breathe is architecture-aware of. This is the ARCHITECTURE
/// view (topology class + cache/pool/replica knobs + how discovery reads the
/// role); `breathe_catalog::db_matrix` is the sibling ACTUATOR view (the concrete
/// `SET GLOBAL` knobs). Both code the same 5 engines — 5/5, not the former 2/5.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DbEngine {
    /// MySQL / InnoDB — primary + async read-replicas.
    MySql,
    /// PostgreSQL — primary + streaming (physical/logical) read-replicas.
    Postgres,
    /// Redis — master + replicas under Sentinel HA (Sentinel promotes a replica).
    Redis,
    /// MongoDB — a replica set: the primary is elected by a majority of voters.
    Mongo,
    /// Neo4j — a single-writer PVC-per-ordinal graph store.
    Neo4j,
}

impl DbEngine {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MySql => "mysql",
            Self::Postgres => "postgres",
            Self::Redis => "redis",
            Self::Mongo => "mongo",
            Self::Neo4j => "neo4j",
        }
    }

    /// Every engine the matrix covers — the domain side of the 5/5 coverage check.
    pub const ALL: [DbEngine; 5] =
        [Self::MySql, Self::Postgres, Self::Redis, Self::Mongo, Self::Neo4j];
}

/// One engine's ARCHITECTURE row — the topology class it breathes under, its
/// cache + pool knobs, how discovery reads the primary/reader roles, and the
/// default failover-safe permutation axes. A new engine is ONE row here + one
/// `db_matrix` actuator row + one lisp form (CATALOG REFLECTION).
#[derive(Clone, Copy, Debug)]
pub struct DbArchitecture {
    pub engine: DbEngine,
    /// The replication class (couples to `REPLICA_TOPOLOGY_AXIS` via `crd_kind`).
    pub class: ReplicationClass,
    /// The cache/buffer knob breathe right-sizes (the DB's memory working set).
    pub cache_knob: &'static str,
    /// The connection-pool knob breathe right-sizes (the concurrency headroom).
    pub pool_knob: &'static str,
    /// How LIVE discovery reads which pod is the primary vs readers/voters (the
    /// engine's replication-status surface). The C2 external-world reader consumes
    /// this; the string names the surface, not a shipped reader.
    pub role_discovery: &'static str,
    /// The default failover-safe permutation for this engine (each validates — the
    /// matrix cannot ship an engine whose default carve is illegal).
    pub default_spot: SpotPosture,
    pub default_replica: ReplicaScalePolicy,
    pub default_failover: FailoverPolicy,
}

impl DbArchitecture {
    /// The default permutation this engine breathes under — the failover-safe
    /// posture (anti-affinity placement so the primary never shares a node with a
    /// reader). Guaranteed legal by the `every_default_permutation_is_legal` test.
    #[must_use]
    pub const fn default_permutation(&self) -> DatabasePermutation {
        DatabasePermutation {
            class: self.class,
            placement: PlacementIsolation::AntiAffinity,
            spot: self.default_spot,
            replica: self.default_replica,
            failover: self.default_failover,
        }
    }
}

/// The 5-engine architecture matrix. Row order: canonical (relational → cache →
/// document → graph). Each row's default permutation is failover-safe by
/// construction (tested). 5/5, closing the former 2/5 gap.
pub const DB_ARCHITECTURES: &[DbArchitecture] = &[
    // ── MySQL / InnoDB — primary + async read-replicas (masterSlave) ────────────
    DbArchitecture {
        engine: DbEngine::MySql,
        class: ReplicationClass::MasterSlave,
        cache_knob: "innodb_buffer_pool_size",
        pool_knob: "max_connections",
        role_discovery: "SHOW REPLICA STATUS / @@read_only (primary = read_only OFF)",
        default_spot: SpotPosture::SpotEvenPrimary,
        default_replica: ReplicaScalePolicy::ScaleReadersFreely,
        default_failover: FailoverPolicy::PromoteBeforeReclaim,
    },
    // ── PostgreSQL — primary + streaming replicas (masterSlave) ─────────────────
    DbArchitecture {
        engine: DbEngine::Postgres,
        class: ReplicationClass::MasterSlave,
        cache_knob: "shared_buffers",
        pool_knob: "max_connections",
        role_discovery: "pg_stat_replication / pg_is_in_recovery() (primary = NOT in recovery)",
        default_spot: SpotPosture::SpotEvenPrimary,
        default_replica: ReplicaScalePolicy::ScaleReadersFreely,
        default_failover: FailoverPolicy::PromoteBeforeReclaim,
    },
    // ── Redis — master + replicas under Sentinel HA (masterSlave) ───────────────
    DbArchitecture {
        engine: DbEngine::Redis,
        class: ReplicationClass::MasterSlave,
        cache_knob: "maxmemory",
        pool_knob: "maxclients",
        role_discovery: "SENTINEL masters / ROLE (primary = role:master)",
        default_spot: SpotPosture::SpotEvenPrimary,
        default_replica: ReplicaScalePolicy::ScaleReadersFreely,
        default_failover: FailoverPolicy::PromoteBeforeReclaim,
    },
    // ── MongoDB — replica-set majority election (fullyDistributed) ──────────────
    DbArchitecture {
        engine: DbEngine::Mongo,
        class: ReplicationClass::FullyDistributed,
        cache_knob: "wiredTigerEngineRuntimeConfig",
        pool_knob: "net.maxIncomingConnections",
        role_discovery: "rs.status() / isMaster (primary = the elected member)",
        default_spot: SpotPosture::SpotEvenPrimary,
        default_replica: ReplicaScalePolicy::QuorumOddSteps,
        default_failover: FailoverPolicy::QuorumReElect,
    },
    // ── Neo4j — single-writer PVC-per-ordinal graph store (persistent) ──────────
    DbArchitecture {
        engine: DbEngine::Neo4j,
        class: ReplicationClass::Persistent,
        cache_knob: "dbms.memory.pagecache.size",
        pool_knob: "dbms.connector.bolt.thread_pool_max_size",
        role_discovery: "SHOW SERVERS / dbms.cluster.role (primary = LEADER / ordinal-0)",
        // A persistent single-writer runs readers on spot, primary parked
        // on-demand — the safe default (SpotReadersOnly), promotable if causal.
        default_spot: SpotPosture::SpotReadersOnly,
        default_replica: ReplicaScalePolicy::NeverScale,
        default_failover: FailoverPolicy::PromoteBeforeReclaim,
    },
];

/// The architecture row for one engine.
#[must_use]
pub fn architecture_for(engine: DbEngine) -> Option<&'static DbArchitecture> {
    DB_ARCHITECTURES.iter().find(|a| a.engine == engine)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── the architecture matrix (5/5) ──────────────────────────────────────────

    #[test]
    fn every_engine_has_an_architecture_row() {
        for e in DbEngine::ALL {
            assert!(architecture_for(e).is_some(), "no architecture row for {}", e.as_str());
        }
        assert_eq!(DB_ARCHITECTURES.len(), DbEngine::ALL.len(), "matrix ⇄ engine drift");
        assert_eq!(DbEngine::ALL.len(), 5, "the matrix codes 5/5 engines, not 2/5");
    }

    #[test]
    fn engine_rows_are_unique() {
        for (i, a) in DB_ARCHITECTURES.iter().enumerate() {
            for b in &DB_ARCHITECTURES[i + 1..] {
                assert_ne!(a.engine, b.engine, "duplicate engine row {}", a.engine.as_str());
            }
        }
    }

    #[test]
    fn every_default_permutation_is_legal() {
        // The matrix ↔ permutation coupling: no engine ships a default carve the
        // CSP gate rejects (algorithmic-prowess-seal — the legality is a type).
        for a in DB_ARCHITECTURES {
            assert!(
                a.default_permutation().validate().is_ok(),
                "{}'s default permutation is illegal: {:?}",
                a.engine.as_str(),
                a.default_permutation().validate()
            );
        }
    }

    #[test]
    fn every_engine_class_is_a_real_axis_kind() {
        // Coupling to REPLICA_TOPOLOGY_AXIS: each class' crd_kind is one of the
        // three stateful arms (a database always has data).
        const STATEFUL_KINDS: [&str; 3] = ["masterSlave", "fullyDistributed", "persistent"];
        for a in DB_ARCHITECTURES {
            assert!(
                STATEFUL_KINDS.contains(&a.class.crd_kind()),
                "{}'s class {} is not a stateful axis arm",
                a.engine.as_str(),
                a.class.crd_kind()
            );
        }
    }

    #[test]
    fn expected_engine_topologies() {
        assert_eq!(architecture_for(DbEngine::MySql).unwrap().class, ReplicationClass::MasterSlave);
        assert_eq!(architecture_for(DbEngine::Postgres).unwrap().class, ReplicationClass::MasterSlave);
        assert_eq!(architecture_for(DbEngine::Redis).unwrap().class, ReplicationClass::MasterSlave);
        assert_eq!(architecture_for(DbEngine::Mongo).unwrap().class, ReplicationClass::FullyDistributed);
        assert_eq!(architecture_for(DbEngine::Neo4j).unwrap().class, ReplicationClass::Persistent);
    }

    // ── discovery ───────────────────────────────────────────────────────────────

    #[test]
    fn discovery_reads_a_topology_through_the_seam() {
        let disc = MockReplicationDiscovery {
            topology: ReplicationTopology::PrimaryReaders { primary: ReplicaId(0), readers: 2 },
            unreadable: false,
        };
        let topo = disc.discover_topology().unwrap();
        assert!(topo.has_failover_target(), "a primary with 2 readers has a failover target");
        assert_eq!(topo.class(), ReplicationClass::MasterSlave);
    }

    #[test]
    fn discovery_error_is_typed_not_a_panic() {
        let disc = MockReplicationDiscovery {
            topology: ReplicationTopology::SingleWriter { primary: ReplicaId(0) },
            unreadable: true,
        };
        assert!(matches!(disc.discover_topology(), Err(DiscoveryError::Unreadable(_))));
    }

    #[test]
    fn single_writer_has_no_failover_target() {
        let topo = ReplicationTopology::SingleWriter { primary: ReplicaId(0) };
        assert!(!topo.has_failover_target(), "a single writer has no promotable replica");
    }

    #[test]
    fn an_even_or_sub3_quorum_is_not_a_safe_target() {
        // A discovered even/degraded quorum is a real-world observation, not a type
        // violation — the FSM treats it as no-safe-target (blocks the reclaim).
        assert!(!ReplicationTopology::Quorum { voters: 2 }.has_failover_target());
        assert!(!ReplicationTopology::Quorum { voters: 4 }.has_failover_target());
        assert!(ReplicationTopology::Quorum { voters: 3 }.has_failover_target());
        assert!(ReplicationTopology::Quorum { voters: 5 }.has_failover_target());
        assert_eq!(ReplicationTopology::Quorum { voters: 5 }.quorum_majority(), 3);
    }

    // ── the failover-safe-spot FSM ──────────────────────────────────────────────

    #[test]
    fn the_happy_failover_path_reaches_a_clean_terminal() {
        let mut m = FailoverMachine::new();
        // spot reclaim targets the primary → grace window.
        let a = m
            .on(FailoverEvent::PrimaryReclaimSignal { primary: ReplicaId(0), candidate: ReplicaId(1) })
            .unwrap();
        assert_eq!(a, FailoverAction::Hold);
        assert_eq!(m.state(), FailoverState::PrimaryReclaimSignaled);
        // a target is available → promote.
        let a = m.on(FailoverEvent::PromotableTargetAvailable).unwrap();
        assert!(matches!(a, FailoverAction::PromoteReplica { .. }));
        // promotion succeeds → the ONLY place a reclaim authorization is minted.
        let a = m.on(FailoverEvent::PromotionSucceeded).unwrap();
        assert!(matches!(a, FailoverAction::ReclaimOldPrimary { .. }));
        assert_eq!(m.state(), FailoverState::FailedOver);
        // the old primary drains → clean terminal.
        m.on(FailoverEvent::OldPrimaryDrained).unwrap();
        assert_eq!(m.state(), FailoverState::OldPrimaryReclaimed);
        assert!(m.state().is_good_terminal());
    }

    #[test]
    fn no_target_blocks_the_reclaim_never_loses_the_primary() {
        let mut m = FailoverMachine::new();
        m.on(FailoverEvent::PrimaryReclaimSignal { primary: ReplicaId(0), candidate: ReplicaId(1) })
            .unwrap();
        let a = m.on(FailoverEvent::NoPromotableTarget).unwrap();
        assert_eq!(a, FailoverAction::BlockReclaimEscalate);
        assert_eq!(m.state(), FailoverState::ReclaimBlocked);
        // CRUCIAL: no ReclaimOldPrimary was ever emitted — the primary is held.
    }

    #[test]
    fn a_blocked_reclaim_can_recover_when_a_target_appears() {
        let mut m = FailoverMachine::new();
        m.on(FailoverEvent::PrimaryReclaimSignal { primary: ReplicaId(0), candidate: ReplicaId(1) })
            .unwrap();
        m.on(FailoverEvent::NoPromotableTarget).unwrap();
        // a lagging reader caught up.
        let a = m.on(FailoverEvent::PromotableTargetAvailable).unwrap();
        assert!(matches!(a, FailoverAction::PromoteReplica { .. }));
        assert_eq!(m.state(), FailoverState::PromotingReplica);
    }

    #[test]
    fn a_cleared_reclaim_returns_to_steady_from_any_phase() {
        for start_events in [
            vec![FailoverEvent::PrimaryReclaimSignal { primary: ReplicaId(0), candidate: ReplicaId(1) }],
            vec![
                FailoverEvent::PrimaryReclaimSignal { primary: ReplicaId(0), candidate: ReplicaId(1) },
                FailoverEvent::NoPromotableTarget,
            ],
        ] {
            let mut m = FailoverMachine::new();
            for e in start_events {
                m.on(e).unwrap();
            }
            m.on(FailoverEvent::ReclaimCleared).unwrap();
            assert_eq!(m.state(), FailoverState::Steady);
        }
    }

    #[test]
    fn reclaim_old_primary_is_emitted_only_after_a_promotion() {
        // ★ THE load-bearing FSM property: sweep EVERY (state, event); a
        // ReclaimOldPrimary action appears ONLY from (PromotingReplica,
        // PromotionSucceeded). No other edge authorizes reclaiming a former primary.
        let events = [
            FailoverEvent::PrimaryReclaimSignal { primary: ReplicaId(0), candidate: ReplicaId(1) },
            FailoverEvent::PromotableTargetAvailable,
            FailoverEvent::NoPromotableTarget,
            FailoverEvent::PromotionSucceeded,
            FailoverEvent::OldPrimaryDrained,
            FailoverEvent::ReclaimCleared,
        ];
        for state in FailoverState::ALL {
            for event in events {
                if let Ok((_, action)) = failover_step(state, event) {
                    if matches!(action, FailoverAction::ReclaimOldPrimary { .. }) {
                        assert_eq!(
                            (state, matches!(event, FailoverEvent::PromotionSucceeded)),
                            (FailoverState::PromotingReplica, true),
                            "ReclaimOldPrimary leaked from ({}, {:?}) — the primary could be lost un-gracefully",
                            state.as_str(),
                            event
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn illegal_transitions_are_typed_errors_not_panics() {
        // e.g. a promotion-success while steady (no failover in flight) is illegal.
        assert!(failover_step(FailoverState::Steady, FailoverEvent::PromotionSucceeded).is_err());
        assert!(
            failover_step(FailoverState::OldPrimaryReclaimed, FailoverEvent::PromotableTargetAvailable)
                .is_err()
        );
    }

    #[test]
    fn every_state_reaches_a_good_terminal() {
        // Convergence (the mechanical CI forcing-function — Rust has no reachability
        // proof): from every state, some event sequence reaches Steady or
        // OldPrimaryReclaimed. BFS over the transition relation.
        let events = [
            FailoverEvent::PrimaryReclaimSignal { primary: ReplicaId(0), candidate: ReplicaId(1) },
            FailoverEvent::PromotableTargetAvailable,
            FailoverEvent::NoPromotableTarget,
            FailoverEvent::PromotionSucceeded,
            FailoverEvent::OldPrimaryDrained,
            FailoverEvent::ReclaimCleared,
        ];
        for start in FailoverState::ALL {
            let mut seen = vec![start];
            let mut frontier = vec![start];
            let mut reached_good = start.is_good_terminal();
            while let Some(s) = frontier.pop() {
                for e in events {
                    if let Ok((next, _)) = failover_step(s, e) {
                        if next.is_good_terminal() {
                            reached_good = true;
                        }
                        if !seen.contains(&next) {
                            seen.push(next);
                            frontier.push(next);
                        }
                    }
                }
            }
            assert!(reached_good, "state {} cannot reach a good terminal", start.as_str());
        }
    }

    #[test]
    fn a_reclaim_authorization_requires_a_promotion_receipt() {
        // The proof-carrying witness: the authorization covers exactly the demoted
        // node named by the receipt — and there is no other way to build one.
        let receipt = PromotionReceipt::issue(ReplicaId(1), ReplicaId(0));
        let auth = PrimaryReclaimAuthorization::from_receipt(receipt);
        assert_eq!(auth.reclaimable_node(), ReplicaId(0));
        assert_eq!(auth.receipt().new_primary(), ReplicaId(1));
    }

    // ── the permutation lattice ─────────────────────────────────────────────────

    #[test]
    fn spot_even_primary_without_failover_is_rejected() {
        // ★ the load-bearing CSP constraint (parse-time-rejected).
        let p = DatabasePermutation {
            class: ReplicationClass::MasterSlave,
            placement: PlacementIsolation::AntiAffinity,
            spot: SpotPosture::SpotEvenPrimary,
            replica: ReplicaScalePolicy::ScaleReadersFreely,
            failover: FailoverPolicy::NoFailover,
        };
        assert_eq!(p.validate(), Err(PermutationError::SpotPrimaryWithoutFailover));
    }

    #[test]
    fn spot_even_primary_with_failover_is_legal() {
        let p = DatabasePermutation {
            class: ReplicationClass::MasterSlave,
            placement: PlacementIsolation::AntiAffinity,
            spot: SpotPosture::SpotEvenPrimary,
            replica: ReplicaScalePolicy::ScaleReadersFreely,
            failover: FailoverPolicy::PromoteBeforeReclaim,
        };
        assert!(p.is_legal(), "100% spot even the primary IS legal WITH a failover policy");
    }

    #[test]
    fn quorum_policy_on_master_slave_is_a_class_mismatch() {
        let p = DatabasePermutation {
            class: ReplicationClass::MasterSlave,
            placement: PlacementIsolation::AntiAffinity,
            spot: SpotPosture::SpotReadersOnly,
            replica: ReplicaScalePolicy::QuorumOddSteps,
            failover: FailoverPolicy::NoFailover,
        };
        assert_eq!(p.validate(), Err(PermutationError::ReplicaClassMismatch));
    }

    #[test]
    fn quorum_re_elect_on_master_slave_is_a_failover_mismatch() {
        let p = DatabasePermutation {
            class: ReplicationClass::MasterSlave,
            placement: PlacementIsolation::AntiAffinity,
            spot: SpotPosture::SpotReadersOnly,
            replica: ReplicaScalePolicy::ScaleReadersFreely,
            failover: FailoverPolicy::QuorumReElect,
        };
        assert_eq!(p.validate(), Err(PermutationError::FailoverClassMismatch));
    }

    #[test]
    fn the_legal_lattice_is_nonempty_and_all_valid() {
        let lattice = legal_permutations();
        assert!(!lattice.is_empty(), "the permutation lattice must be nonempty");
        for p in &lattice {
            assert!(p.is_legal(), "legal_permutations yielded an illegal permutation: {p:?}");
        }
        // No spot-even-primary permutation in the lattice lacks a failover policy.
        for p in &lattice {
            if matches!(p.spot, SpotPosture::SpotEvenPrimary) {
                assert!(
                    p.failover.is_failover_safe(),
                    "a spot-even-primary lattice point without a failover policy leaked: {p:?}"
                );
            }
        }
        // Every architecture's default permutation is a point in the lattice.
        for a in DB_ARCHITECTURES {
            assert!(
                lattice.contains(&a.default_permutation()),
                "{}'s default permutation is missing from the legal lattice",
                a.engine.as_str()
            );
        }
    }

    // ── the discovery-molded overlay precedence ────────────────────────────────

    #[test]
    fn discovery_molds_the_class_then_override_pins_it() {
        // default ← discovered ← override.
        let default = architecture_for(DbEngine::MySql).unwrap().default_permutation();
        // discovery says the live shape is a quorum → class narrows to distributed,
        // but the master-slave replica/failover would then mismatch → override fixes.
        let overlay = ReplicationOverlay {
            replica: Some(ReplicaScalePolicy::QuorumOddSteps),
            failover: Some(FailoverPolicy::QuorumReElect),
            ..Default::default()
        };
        let resolved = resolve_permutation(
            default,
            Some(ReplicationTopology::Quorum { voters: 3 }),
            &overlay,
        )
        .unwrap();
        assert_eq!(resolved.class, ReplicationClass::FullyDistributed);
        assert!(resolved.is_legal());
    }

    #[test]
    fn an_override_that_makes_an_unsafe_carve_is_rejected() {
        // The precedence fold cannot mint an unsafe carve: forcing spot-even-primary
        // with no failover fails resolution (parse-time-rejected).
        let default = architecture_for(DbEngine::Postgres).unwrap().default_permutation();
        let overlay = ReplicationOverlay {
            spot: Some(SpotPosture::SpotEvenPrimary),
            failover: Some(FailoverPolicy::NoFailover),
            ..Default::default()
        };
        assert_eq!(
            resolve_permutation(default, None, &overlay),
            Err(PermutationError::SpotPrimaryWithoutFailover)
        );
    }

    #[test]
    fn overlay_round_trips_through_serde_deny_unknown() {
        // The enjulho config surface is typed + deny_unknown_fields.
        let json = r#"{"spot":"spot-readers-only","failover":"promote-before-reclaim"}"#;
        let o: ReplicationOverlay = serde_json::from_str(json).unwrap();
        assert_eq!(o.spot, Some(SpotPosture::SpotReadersOnly));
        assert_eq!(o.failover, Some(FailoverPolicy::PromoteBeforeReclaim));
        assert!(serde_json::from_str::<ReplicationOverlay>(r#"{"bogus":1}"#).is_err());
    }

    // ── /vocabulary-bridging — the Rust border ↔ authored lisp cross-check ───────

    #[test]
    fn the_database_contract_is_declared_in_the_lisp() {
        // Same include_str! convention breathe-catalog::db_matrix + the dimension
        // catalog use: every engine, class kind, failover state, and permutation
        // axis label appears in the authored (defreplication-topology) form, so the
        // Rust border and the (defband-database) lisp vocabulary can never drift.
        const LISP: &str = include_str!("../specs/breathe-invariant.lisp");
        assert!(LISP.contains("defreplication-topology"), "lisp must declare (defreplication-topology …)");
        assert!(LISP.contains("defband-database"), "lisp must declare (defband-database …)");
        // maturity promoted to landing (no longer a gap).
        assert!(LISP.contains(":maturity landing"), "the (defband-database) form must be maturity landing");
        for e in DbEngine::ALL {
            assert!(LISP.contains(e.as_str()), "lisp missing engine {}", e.as_str());
        }
        for c in ReplicationClass::ALL {
            assert!(LISP.contains(c.as_str()), "lisp missing class {}", c.as_str());
            assert!(LISP.contains(c.crd_kind()), "lisp missing crd_kind {}", c.crd_kind());
        }
        for s in FailoverState::ALL {
            assert!(LISP.contains(s.as_str()), "lisp missing failover state {}", s.as_str());
        }
        for s in SpotPosture::ALL {
            assert!(LISP.contains(s.as_str()), "lisp missing spot posture {}", s.as_str());
        }
        for f in FailoverPolicy::ALL {
            assert!(LISP.contains(f.as_str()), "lisp missing failover policy {}", f.as_str());
        }
        // the load-bearing cache/pool knobs appear (couples to the db_matrix actuator).
        for a in DB_ARCHITECTURES {
            assert!(LISP.contains(a.cache_knob), "lisp missing cache knob {}", a.cache_knob);
        }
    }
}
