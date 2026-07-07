//! The breathe dimension CATALOG (CATALOG REFLECTION).
//!
//! Every resource dimension the breathability doctrine names is a typed point
//! on the breathe lattice, declared here as a [`BreatheDimension`]. The
//! catalog is the self-describing object: tooling (a `breathe confirm`
//! report, generated docs, the verification matrix) iterates it mechanically
//! instead of grepping doc-strings.
//!
//! **The forcing rule (CATALOG REFLECTION + CLOSED-LOOP MASS-SYNTHESIS rule
//! 1):** adding a dimension REQUIRES landing its catalog entry here AND its
//! matrix row in `tests/breathe_dimension_matrix.rs` in the same commit. The
//! matrix test `matrix_covers_every_dimension` fails the build on drift. The
//! catalog IS the doc; the matrix IS the proof.
//!
//! **Tier-honest maturity (2026-07):** MemoryBand + CpuBand + ReplicaBand are
//! SHIPPED (live CRD kinds + carve laws in the breathe substrate); StorageBand
//! is LANDING (CRD kind ships; the provision-minimal + grow-on-demand
//! elasticity carve is under construction); DatabaseBand is a GAP (per-engine
//! knobs ride the generic `AppParam` family via `breathe-catalog::db_matrix`,
//! but the architecture-aware discovering DatabaseBand does not exist as a
//! first-class Band) — tracked with an explicit `pending-breathe` note so the
//! models-stay-current gate stays green honestly.

use crate::clause::{BreatheClause, UnrepTier};
use crate::dimension::{
    BreatheDimension, CarveAlgorithm, ClauseStatus, DimensionId, DiscoveryStrategy, Maturity,
};
use crate::setpoint::UtilizationSetpoint;

use BreatheClause::{
    CarveToSetpoint, CarvedByABand, DefaultOnFleetWide, DiscoveryMolded, DualPurpose,
    ModelsStayCurrent,
};
use UnrepTier::{CeilingC1, CeilingC2, OnlyMitigated, ParseTimeRejected};

// ── clause-status shorthands ─────────────────────────────────────────────
const fn cs(clause: BreatheClause, tier: UnrepTier) -> ClauseStatus {
    ClauseStatus { clause, tier: Some(tier) }
}
const fn gap(clause: BreatheClause) -> ClauseStatus {
    ClauseStatus { clause, tier: None }
}

/// The 80% utilization homeostasis default (the common breathe setpoint).
const SP80: UtilizationSetpoint = UtilizationSetpoint::from_bps(8_000);

/// MemoryBand — SHIPPED. `breathe-crd::MemoryBand` + `breathe-control`
/// BandLaw + safety_clamp (never-OOM proven). The vertical pod-memory band.
const MEMORY: BreatheDimension = BreatheDimension {
    id: DimensionId::Memory,
    band: "MemoryBand",
    band_keyword: "defband-memory",
    setpoint: SP80,
    carve_algorithm: CarveAlgorithm::MultiplicativeBand,
    discovery: DiscoveryStrategy::KanchiDiscovered,
    maturity: Maturity::Shipped,
    claimed_by_doctrine: true,
    cost_effect: "right-size the pod memory limit DOWN to the working set — no over-provisioned RAM billed",
    resiliency_effect: "hold ≥1/setpoint headroom before the OOM cliff — the setpoint absorbs a burst before saturation (availability)",
    doctrine_ref: "BREATHABILITY.md §II.5 (vertical band) + BREATHABILITY-CARVING §2 (memory law)",
    pending: None,
    clauses: &[
        cs(CarvedByABand, CeilingC1),
        cs(CarveToSetpoint, ParseTimeRejected),
        cs(DefaultOnFleetWide, OnlyMitigated),
        cs(ModelsStayCurrent, CeilingC1),
        cs(DiscoveryMolded, CeilingC2),
        cs(DualPurpose, CeilingC1),
    ],
    note: "MemoryBand is the most-protected dimension — the safety_clamp gate proves never-OOM against every future law.",
};

/// CpuBand — SHIPPED. `breathe-crd::CpuBand` (millicores) + BandLaw. The
/// vertical pod-cpu band; a soft (CFS-throttle) floor, symmetric carve.
const CPU: BreatheDimension = BreatheDimension {
    id: DimensionId::Cpu,
    band: "CpuBand",
    band_keyword: "defband-cpu",
    setpoint: SP80,
    carve_algorithm: CarveAlgorithm::MultiplicativeBand,
    discovery: DiscoveryStrategy::KanchiDiscovered,
    maturity: Maturity::Shipped,
    claimed_by_doctrine: true,
    cost_effect: "right-size the cpu limit DOWN to the working set — no over-provisioned cores billed",
    resiliency_effect: "keep setpoint headroom before CFS throttle — latency stays low under a burst (availability)",
    doctrine_ref: "BREATHABILITY.md §II.5 (vertical band) + BREATHABILITY-CARVING §2 (cpu throttle-aware law)",
    pending: None,
    clauses: &[
        cs(CarvedByABand, CeilingC1),
        cs(CarveToSetpoint, ParseTimeRejected),
        cs(DefaultOnFleetWide, OnlyMitigated),
        cs(ModelsStayCurrent, CeilingC1),
        cs(DiscoveryMolded, CeilingC2),
        cs(DualPurpose, CeilingC1),
    ],
    note: "CPU is soft + symmetrically reversible — throttle_ratio is the CPU-native pre-degradation signal memory lacks.",
};

/// StorageBand — LANDING. `breathe-crd::StorageBand` CRD kind ships (the
/// utilization band of a pool); the provision-minimal + grow-on-demand
/// ELASTICITY carve (linear-fit fill velocity, GrowOnly, resize-at-80%) is
/// under active construction. This is the dimension whose gap produced the
/// 155GB receipt — now a landing Band, no longer an uncarved claim.
const STORAGE: BreatheDimension = BreatheDimension {
    id: DimensionId::Storage,
    band: "StorageBand",
    band_keyword: "defband-storage",
    setpoint: SP80,
    carve_algorithm: CarveAlgorithm::GrowOnlyPredictive,
    discovery: DiscoveryStrategy::KanchiDiscovered,
    maturity: Maturity::Landing,
    claimed_by_doctrine: true,
    cost_effect: "provision-MINIMAL — no over-provisioned PVC (the 155GB-provisioned / 5GB-used waste is carved away)",
    resiliency_effect: "grow-on-demand BEFORE it fills (predictive, forecast-sized) — a disk-full outage is pre-empted (availability)",
    doctrine_ref: "BREATHABILITY.md §V (storage elasticity at 80%) + BREATHABILITY-CARVING §2 (grow-only storage law)",
    pending: None,
    clauses: &[
        // CRD kind ships; elastic grow-on-demand carve is landing → partial.
        cs(CarvedByABand, OnlyMitigated),
        cs(CarveToSetpoint, OnlyMitigated),
        cs(DefaultOnFleetWide, OnlyMitigated),
        cs(ModelsStayCurrent, CeilingC1),
        cs(DiscoveryMolded, CeilingC2),
        cs(DualPurpose, OnlyMitigated),
    ],
    note: "The storage-claimed-not-carved class (155GB provisioned / 5GB used) is exactly what the models-stay-current gate now CI-catches.",
};

/// ReplicaBand — SHIPPED. `breathe-crd::ReplicaBand` + `breathe-control::replica`
/// (ReplicaBandConfig + Topology axis + validate_for_target). The horizontal
/// band: floor-2 HA + algorithmic scale, topology sub-axis picking the algorithm.
const REPLICA: BreatheDimension = BreatheDimension {
    id: DimensionId::Replica,
    band: "ReplicaBand",
    band_keyword: "defband-replica",
    setpoint: SP80,
    carve_algorithm: CarveAlgorithm::ReplicaTopologyScale,
    discovery: DiscoveryStrategy::KanchiDiscovered,
    maturity: Maturity::Shipped,
    claimed_by_doctrine: true,
    cost_effect: "scale-down-when-idle to the floor — no idle replicas billed (scale-to-zero for stateless)",
    resiliency_effect: "floor-2 HA + topology-correct scale (odd-quorum / read-replica / ordinal-rebalance) — survive a node loss (resiliency)",
    doctrine_ref: "BREATHABILITY.md §II.6 (replica band + topology sub-axis)",
    pending: None,
    clauses: &[
        cs(CarvedByABand, CeilingC1),
        cs(CarveToSetpoint, ParseTimeRejected),
        cs(DefaultOnFleetWide, OnlyMitigated),
        cs(ModelsStayCurrent, CeilingC1),
        // topology (master-slave/distributed/persistent) is discovered/molded.
        cs(DiscoveryMolded, CeilingC2),
        cs(DualPurpose, CeilingC1),
    ],
    note: "The topology sub-axis (stateless free-scale / masterSlave read-replicas / distributed odd-quorum / persistent ordinal) picks the per-topology algorithm; validate_for_target gates target-kind coupling.",
};

/// DatabaseBand — GAP. There is NO first-class architecture-aware DatabaseBand:
/// per-engine knobs (InnoDB buffer pool, Neo4j page cache, max_connections)
/// ride the generic `AppParam` family via `breathe-catalog::db_matrix`, but a
/// Band that DISCOVERS master/multi-reader/distributed topology + molds a
/// failover-safe 100%-spot permutation does not exist. Doctrine-claimed →
/// tracked with an explicit `pending-breathe` note (models-stay-current
/// honesty).
const DATABASE: BreatheDimension = BreatheDimension {
    id: DimensionId::Database,
    band: "DatabaseBand",
    band_keyword: "defband-database",
    setpoint: SP80,
    carve_algorithm: CarveAlgorithm::ArchitectureAwareEngine,
    discovery: DiscoveryStrategy::ArchitectureDiscovered,
    maturity: Maturity::Gap,
    claimed_by_doctrine: true,
    cost_effect: "right-size the per-engine caches + connection headroom (buffer pool / page cache) — no over-sized DB nodes billed",
    resiliency_effect: "discover + hold failover-safe replicas (never scale the primary, never cross a quorum majority) + never-starve the buffer pool (resiliency)",
    doctrine_ref: "BREATHABILITY.md §II.5 (per-engine database matrix)",
    pending: Some(
        "pending-breathe: architecture-aware discovering DatabaseBand is a Gap — db_matrix carves per-engine knobs as AppParam instances, but the master/multi-reader/distributed discovery + failover-safe-spot permutation Band is unbuilt",
    ),
    clauses: &[
        gap(CarvedByABand),
        gap(CarveToSetpoint),
        gap(DefaultOnFleetWide),
        gap(ModelsStayCurrent),
        gap(DiscoveryMolded),
        gap(DualPurpose),
    ],
    note: "Per-engine knobs breathe TODAY via breathe-catalog::db_matrix (AppParam instances, MySQL/Neo4j); the first-class discovering DatabaseBand is the named Gap.",
};

/// IsolationBand — LANDING. The ISOLATION posture that BOUNDS the carve. The
/// typed posture surface (QoS class + requests-floor + limits-ceiling +
/// placement) + the seal-floor carve constraint (`carve_respecting_seal` /
/// `SealedCarve`) + the critical-must-be-sealed forcing-function + the overlay
/// precedence + the constrained optimization all SHIP as typed contract in
/// `breathe-invariant::isolation` (CI-tested). The live in-cluster `IsolationBand`
/// CRD reconcile (interference-sensitivity discovery from live throttle/eviction
/// metrics + auto re-placement) is the C2 destination. This is the dimension
/// whose ABSENCE produced the victoria-logs-422 receipt (a BestEffort pod with
/// no isolation floor) — now a first-class landing Band.
const ISOLATION: BreatheDimension = BreatheDimension {
    id: DimensionId::Isolation,
    band: "IsolationBand",
    band_keyword: "defband-isolation",
    setpoint: SP80,
    carve_algorithm: CarveAlgorithm::ConstrainedIsolationAssignment,
    discovery: DiscoveryStrategy::InterferenceDiscovered,
    maturity: Maturity::Landing,
    claimed_by_doctrine: true,
    cost_effect: "right-size requests/limits toward the working set WITHOUT over-reserving isolation — Batch bin-packs BestEffort, Standard runs Burstable; no capacity wasted sealing a workload that needs no seal",
    resiliency_effect: "SEAL a critical / interference-sensitive workload — guaranteed requests-floor + Guaranteed QoS + anti-affinity so a noisy neighbor can never throttle or evict it (the victoria-logs-422 class is unrepresentable), and the floor BOUNDS the carve so cost never strips the seal",
    doctrine_ref: "BREATHABILITY.md §II.7 (isolation/interference — the seal dimension)",
    pending: None,
    clauses: &[
        // Typed posture surface ships; the live in-cluster IsolationBand carve is landing → partial.
        cs(CarvedByABand, OnlyMitigated),
        cs(CarveToSetpoint, OnlyMitigated),
        cs(DefaultOnFleetWide, OnlyMitigated),
        // The critical-must-be-sealed CI forcing-function IS shipped (the seal gate).
        cs(ModelsStayCurrent, CeilingC1),
        // Interference-sensitivity discovery is the InterferenceDiscovered strategy;
        // the overlay precedence is typed, live metric-reading is the C2 destination.
        cs(DiscoveryMolded, OnlyMitigated),
        cs(DualPurpose, OnlyMitigated),
    ],
    note: "Isolation is BOTH carved (the posture) AND a CONSTRAINT on the other carves (the seal-floor lower-bounds mem/cpu). The per-workload seal is parse-time-rejected (IsolationPosture::try_seal); fleet coverage is the CeilingC1 critical_workload_must_be_sealed matrix gate; the live IsolationBand CRD reconcile is the destination.",
};

/// The full breathe dimension catalog. Order: strongest maturity first, then
/// canonical. Adding a dimension = a `const` + one entry here + one matrix row
/// (same commit; the matrix enforces it).
pub const CATALOG: &[&BreatheDimension] =
    &[&MEMORY, &CPU, &REPLICA, &STORAGE, &ISOLATION, &DATABASE];

/// Look up a dimension by id.
#[must_use]
pub fn dimension(id: DimensionId) -> Option<&'static BreatheDimension> {
    CATALOG.iter().copied().find(|d| d.id == id)
}

/// Maturity histogram — `(gap, landing, shipped)`. Sums to `CATALOG.len()`
/// (partition-complete; the catalog test asserts it). This line IS the
/// honest ledger; promote a dimension's maturity DELIBERATELY when its Band
/// ships.
#[must_use]
pub fn maturity_histogram() -> (usize, usize, usize) {
    let mut h = (0, 0, 0);
    for d in CATALOG {
        match d.maturity {
            Maturity::Gap => h.0 += 1,
            Maturity::Landing => h.1 += 1,
            Maturity::Shipped => h.2 += 1,
        }
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_dimension_id_is_unique() {
        // CATALOG REFLECTION: no two entries share an id.
        let mut ids: Vec<&str> = CATALOG.iter().map(|d| d.id.as_str()).collect();
        ids.sort_unstable();
        let before = ids.len();
        ids.dedup();
        assert_eq!(before, ids.len(), "duplicate dimension id in CATALOG");
    }

    #[test]
    fn every_band_keyword_is_globally_unique() {
        // CATALOG REFLECTION: authoring keywords never collide.
        let mut kws: Vec<&str> = CATALOG.iter().map(|d| d.band_keyword).collect();
        kws.sort_unstable();
        let before = kws.len();
        kws.dedup();
        assert_eq!(before, kws.len(), "duplicate band_keyword in CATALOG");
    }

    #[test]
    fn catalog_covers_every_dimension_id() {
        // The catalog is a complete partition of DimensionId::ALL.
        for id in DimensionId::ALL {
            assert!(dimension(id).is_some(), "dimension {id:?} missing from CATALOG");
        }
        assert_eq!(CATALOG.len(), DimensionId::ALL.len());
    }

    #[test]
    fn every_dimension_declares_every_clause_exactly_once() {
        for d in CATALOG {
            for clause in BreatheClause::ALL {
                let n = d.clauses.iter().filter(|c| c.clause == clause).count();
                assert_eq!(n, 1, "dimension {} must declare clause {clause:?} exactly once (found {n})", d.id.as_str());
            }
            assert_eq!(
                d.clauses.len(),
                BreatheClause::ALL.len(),
                "dimension {} declares extra/duplicate clause rows",
                d.id.as_str()
            );
        }
    }

    #[test]
    fn maturity_histogram_partitions_the_catalog() {
        let (g, l, s) = maturity_histogram();
        assert_eq!(g + l + s, CATALOG.len(), "maturity histogram must sum to catalog size");
        // Tier-honest snapshot of the CURRENT fleet state (2026-07):
        // memory/cpu/replica SHIPPED, storage+isolation LANDING, database GAP.
        // Update deliberately when a dimension advances — this line IS the ledger.
        assert_eq!((g, l, s), (1, 2, 3), "breathe dimension maturity ledger drifted — update deliberately, never round up");
    }

    #[test]
    fn no_dimension_rounds_a_clause_tier_up_past_its_achievable_ceiling() {
        // A dimension's per-clause tier must never be STRONGER than the tier
        // that clause can achieve at its destination — rounding up is the
        // cardinal honesty sin. (Weaker is fine: the burn-down backlog.)
        for d in CATALOG {
            for c in d.clauses {
                if let Some(t) = c.tier {
                    let ach = c.clause.achievable_tier();
                    assert!(
                        t.rank() <= ach.rank(),
                        "dimension {} rounds clause {:?} UP: claims {:?}, achievable {:?}",
                        d.id.as_str(), c.clause, t, ach
                    );
                }
            }
        }
    }

    #[test]
    fn shipped_and_landing_dimensions_discharge_every_clause_gaps_do_not() {
        for d in CATALOG {
            match d.maturity {
                Maturity::Shipped | Maturity::Landing => assert!(
                    d.discharges_all_clauses(),
                    "{} is {} but leaves a clause undischarged",
                    d.id.as_str(), d.maturity.as_str()
                ),
                Maturity::Gap => assert!(
                    !d.discharges_all_clauses(),
                    "{} is a Gap yet discharges every clause — promote its maturity deliberately",
                    d.id.as_str()
                ),
            }
        }
    }

    #[test]
    fn every_claimed_gap_carries_a_pending_note() {
        // The models-stay-current honesty gate (clause 4), at the catalog
        // level: a dimension the doctrine CLAIMS but no Band carves MUST carry
        // an explicit pending acknowledgement. This is what makes the 155GB
        // class (claim + no Band + NO pending) unrepresentable in the catalog.
        for d in CATALOG {
            assert!(
                !d.is_uncarved_claim(),
                "dimension {} is a claimed-but-uncarved dimension with NO pending note — the 155GB class. Ship a Band or add a pending-breathe note.",
                d.id.as_str()
            );
        }
    }

    #[test]
    fn every_dimension_names_both_a_cost_and_a_resiliency_effect() {
        // CLAUSE 6 (dual-purpose), at the catalog level: every Band is
        // simultaneously a cost control AND a resiliency maximizer, so every
        // entry MUST name BOTH effects (nonempty). A Band that named only one
        // would be claiming a tradeoff — forbidden by construction.
        for d in CATALOG {
            assert!(!d.cost_effect.is_empty(), "dimension {} names no cost effect", d.id.as_str());
            assert!(!d.resiliency_effect.is_empty(), "dimension {} names no resiliency effect", d.id.as_str());
            assert!(!d.doctrine_ref.is_empty(), "dimension {} has no doctrine reference", d.id.as_str());
        }
    }

    #[test]
    fn a_gap_dimension_must_carry_a_pending_note() {
        // The converse of the honesty gate: a Gap that is claimed MUST have a
        // pending note; a shipped/landing dimension must NOT (it is carved).
        for d in CATALOG {
            match d.maturity {
                Maturity::Gap if d.claimed_by_doctrine => assert!(
                    d.pending.is_some(),
                    "claimed Gap {} must carry a pending-breathe note",
                    d.id.as_str()
                ),
                Maturity::Shipped | Maturity::Landing => assert!(
                    d.pending.is_none(),
                    "carved dimension {} must not carry a pending note",
                    d.id.as_str()
                ),
                Maturity::Gap => {}
            }
        }
    }
}
