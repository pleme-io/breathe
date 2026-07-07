//! The VARIANT interface — one typed point on the breathe lattice per
//! resource dimension.
//!
//! Where [`crate::clause`] is the INVARIANT (the six clauses every dimension
//! discharges), this module is the VARIANT: the typed record a dimension
//! supplies — `{ id, band, setpoint, carve-algorithm, discovery-strategy,
//! maturity, cost-effect, resiliency-effect }` — plus its tier-honest
//! per-clause status. Each [`BreatheDimension`] is a lattice point; the
//! [`crate::catalog`] is the set of them.

use crate::clause::{BreatheClause, UnrepTier};
use crate::setpoint::UtilizationSetpoint;

/// The resource dimensions the breathability doctrine names. A workload's
/// consumption of any of these MUST be carved by the matching `*Band`
/// (clause 1 / clause 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DimensionId {
    /// Pod memory limit — the vertical band (`MemoryBand`).
    Memory,
    /// Pod cpu limit — the vertical band (`CpuBand`).
    Cpu,
    /// PVC capacity — the elastic band (`StorageBand`): provision-minimal +
    /// grow-on-demand.
    Storage,
    /// Workload replica count — the horizontal band (`ReplicaBand`), with the
    /// topology sub-axis (stateless / masterSlave / distributed / persistent).
    Replica,
    /// Per-engine database architecture — the discovery-molded carve
    /// (`DatabaseBand`): discover master/multi-reader/distributed + carve the
    /// engine knobs (buffer pool, page cache, connection headroom).
    Database,
}

impl DimensionId {
    /// Every dimension, in canonical order. The partition the matrix covers.
    pub const ALL: [DimensionId; 5] = [
        DimensionId::Memory,
        DimensionId::Cpu,
        DimensionId::Storage,
        DimensionId::Replica,
        DimensionId::Database,
    ];

    /// The stable kebab-case label (the axis the catalog + lisp key on).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            DimensionId::Memory => "memory",
            DimensionId::Cpu => "cpu",
            DimensionId::Storage => "storage",
            DimensionId::Replica => "replica",
            DimensionId::Database => "database",
        }
    }
}

/// The carve algorithm a dimension uses — the control law that turns
/// observed utilization into a target. Selects the per-resource-optimal law
/// (BREATHABILITY-CARVING §2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CarveAlgorithm {
    /// `target = ⌈used / setpoint⌉` — the multiplicative BandLaw
    /// (bidirectional). Memory / cpu.
    MultiplicativeBand,
    /// Grow-only predictive (linear-fit fill velocity + time-to-full) — never
    /// shrink an irreversible actuator. Storage.
    GrowOnlyPredictive,
    /// Per-topology replica scaling (odd-quorum snap / ordinal-rebalance /
    /// read-replica scale) selected by the topology sub-axis. Replica.
    ReplicaTopologyScale,
    /// Architecture-aware per-engine knob carving under the discovered
    /// replica topology. Database.
    ArchitectureAwareEngine,
}

impl CarveAlgorithm {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            CarveAlgorithm::MultiplicativeBand => "multiplicative-band",
            CarveAlgorithm::GrowOnlyPredictive => "grow-only-predictive",
            CarveAlgorithm::ReplicaTopologyScale => "replica-topology-scale",
            CarveAlgorithm::ArchitectureAwareEngine => "architecture-aware-engine",
        }
    }
}

/// How the carve config is obtained — clause 5 (discovery-molded).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DiscoveryStrategy {
    /// `default ← discovered ← override` (kanchi/shikumi precedence): the
    /// floor/ceiling/band is discovered from live cluster signal, overridable.
    KanchiDiscovered,
    /// The full architecture is discovered (master / multi-reader /
    /// distributed topology) and molded into the carve. Database.
    ArchitectureDiscovered,
}

impl DiscoveryStrategy {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            DiscoveryStrategy::KanchiDiscovered => "kanchi-discovered",
            DiscoveryStrategy::ArchitectureDiscovered => "architecture-discovered",
        }
    }
}

/// Maturity gate for a dimension's carving Band — the tier-honest shipped
/// state. Ordered weakest→strongest so a histogram + a "no regression" check
/// are trivial.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Maturity {
    /// No carving Band exists for this dimension yet — the doctrine claims it
    /// but reality has no first-class Band. A claimed `Gap` MUST carry a
    /// `pending` note (the models-stay-current honesty gate).
    Gap,
    /// A carving Band exists but its full carve behavior is landing (e.g. the
    /// CRD kind ships; the elastic-provision behavior is under construction).
    Landing,
    /// Fully shipped: the Band CRD + carve law + live reconcile all ship.
    Shipped,
}

impl Maturity {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Maturity::Gap => "gap",
            Maturity::Landing => "landing",
            Maturity::Shipped => "shipped",
        }
    }
}

/// One dimension's per-clause conformance: which clause, and the tier that
/// clause CURRENTLY sits at for THIS dimension (never rounded up). `tier ==
/// None` means the dimension does not discharge the clause at all (a Gap).
#[derive(Clone, Copy, Debug)]
pub struct ClauseStatus {
    pub clause: BreatheClause,
    pub tier: Option<UnrepTier>,
}

/// A typed point on the breathe lattice — one resource dimension's carving
/// Band, described as data (CATALOG REFLECTION).
#[derive(Clone, Debug)]
pub struct BreatheDimension {
    /// The dimension (the catalog's unique key).
    pub id: DimensionId,
    /// The `*Band` primitive that carves it (`"MemoryBand"`, `"StorageBand"`,
    /// …) — the clause-1 witness.
    pub band: &'static str,
    /// The `(defband …)` authoring keyword this dimension exposes in the lisp
    /// vocabulary bridge — globally unique across the catalog.
    pub band_keyword: &'static str,
    /// The default utilization SETPOINT the carve holds (clause 2). Sealed —
    /// an out-of-range value here is a const-eval compile error.
    pub setpoint: UtilizationSetpoint,
    /// The carve algorithm (BREATHABILITY-CARVING §2).
    pub carve_algorithm: CarveAlgorithm,
    /// How the carve config is discovered (clause 5).
    pub discovery: DiscoveryStrategy,
    /// The Band's shipped maturity — tier-honest, never rounded up.
    pub maturity: Maturity,
    /// Does the doctrine CLAIM this dimension is breathed? If so and the Band
    /// is a `Gap` with no `pending`, that is the models-stay-current
    /// violation (the 155GB class).
    pub claimed_by_doctrine: bool,
    /// **DUAL-PURPOSE (clause 6) — the cost control this carve IS.** Nonempty
    /// for every dimension (the both-effects-named matrix gate).
    pub cost_effect: &'static str,
    /// **DUAL-PURPOSE (clause 6) — the availability/resiliency this SAME carve
    /// maximizes.** Nonempty for every dimension. Cost and resiliency are
    /// achieved TOGETHER by the one carve, not traded.
    pub resiliency_effect: &'static str,
    /// The BREATHABILITY doctrine section this dimension is anchored in.
    pub doctrine_ref: &'static str,
    /// A `pending-breathe:` note REQUIRED when a claimed dimension is a `Gap`
    /// — the explicit, tracked acknowledgement that the model names a
    /// dimension reality does not yet carve. `None` for shipped/landing.
    pub pending: Option<&'static str>,
    /// Per-clause tier status, tier-honest (one entry per `BreatheClause`).
    pub clauses: &'static [ClauseStatus],
    /// A tier-honest one-line note (e.g. "db knobs ride the AppParam family
    /// via db_matrix; the discovering DatabaseBand is the Gap").
    pub note: &'static str,
}

impl BreatheDimension {
    /// The tier this dimension earns for a clause today (`None` = undischarged).
    #[must_use]
    pub fn tier_for(&self, clause: BreatheClause) -> Option<UnrepTier> {
        self.clauses
            .iter()
            .find(|c| c.clause == clause)
            .and_then(|c| c.tier)
    }

    /// Does this dimension discharge every clause at least at only-mitigated?
    /// (Shipped/Landing dimensions do; a Gap does not.)
    #[must_use]
    pub fn discharges_all_clauses(&self) -> bool {
        BreatheClause::ALL.iter().all(|c| self.tier_for(*c).is_some())
    }

    /// Is this dimension carved (a Band exists at ≥ `Landing`)?
    #[must_use]
    pub fn is_carved(&self) -> bool {
        matches!(self.maturity, Maturity::Landing | Maturity::Shipped)
    }

    /// **THE load-bearing predicate (clause 4).** A dimension is an
    /// UNCARVED-CLAIM — the 155GB class — when the doctrine claims it, no Band
    /// carves it, and it carries no `pending` acknowledgement. This being
    /// `true` for any catalogued dimension fails the matrix build.
    #[must_use]
    pub fn is_uncarved_claim(&self) -> bool {
        self.claimed_by_doctrine && !self.is_carved() && self.pending.is_none()
    }
}
