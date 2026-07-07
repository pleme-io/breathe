//! The breathe dimension verification MATRIX — CLOSED-LOOP MASS-SYNTHESIS
//! rule 1.
//!
//! ONE matrix with ONE row per dimension, exercising each against the
//! breathability INVARIANT:
//!
//!   > every resource dimension a workload consumes is carved by a Band, to a
//!   > setpoint, default-on, dual-purpose (cost AND resiliency together).
//!
//! The matrix FAILS THE BUILD when:
//!   - a Shipped/Landing dimension's fixture violates the invariant
//!     (`every_shipped_and_landing_dimension_is_conformant`);
//!   - a new catalogued dimension lands without a matrix row, or a row names a
//!     dimension absent from the catalog (`matrix_covers_every_dimension` —
//!     the CATALOG REFLECTION forcing function);
//!   - a doctrine-claimed dimension has no shipped/landing Band AND no pending
//!     note (`no_dimension_claimed_but_uncarved` — THE load-bearing gate that
//!     makes the 155GB / storage-claimed-not-carved class CI-caught);
//!   - a dimension names only one of {cost, resiliency}
//!     (`every_dimension_is_dual_purpose` — clause 6).
//!
//! Failures aggregate BEFORE the assert — one run reports every broken row.

use breathe_invariant::catalog::CATALOG;
use breathe_invariant::check::check;
use breathe_invariant::dimension::{DimensionId, Maturity};
use breathe_invariant::fixture;

/// One row of the matrix: a dimension + the maturity the catalog claims.
struct MatrixRow {
    dimension: DimensionId,
    expected_maturity: Maturity,
}

/// The matrix — ONE row per dimension in the catalog. Adding a catalog entry
/// without a row here fails `matrix_covers_every_dimension`.
const MATRIX: &[MatrixRow] = &[
    MatrixRow { dimension: DimensionId::Memory, expected_maturity: Maturity::Shipped },
    MatrixRow { dimension: DimensionId::Cpu, expected_maturity: Maturity::Shipped },
    MatrixRow { dimension: DimensionId::Replica, expected_maturity: Maturity::Shipped },
    MatrixRow { dimension: DimensionId::Storage, expected_maturity: Maturity::Landing },
    MatrixRow { dimension: DimensionId::Isolation, expected_maturity: Maturity::Landing },
    // DatabaseBand promoted Gap→Landing: breathe-invariant::database ships the
    // architecture-aware contract (ReplicationTopology + discovery seam +
    // FailoverMachine + 5-engine DB_ARCHITECTURES + DatabasePermutation lattice).
    MatrixRow { dimension: DimensionId::Database, expected_maturity: Maturity::Landing },
];

#[test]
fn matrix_covers_every_dimension() {
    // The forcing function (CATALOG REFLECTION + CLOSED-LOOP rule 1): the
    // matrix rows and the catalog entries are the SAME set.
    let mut catalog_ids: Vec<&str> = CATALOG.iter().map(|d| d.id.as_str()).collect();
    let mut matrix_ids: Vec<&str> = MATRIX.iter().map(|r| r.dimension.as_str()).collect();
    catalog_ids.sort_unstable();
    matrix_ids.sort_unstable();
    assert_eq!(
        catalog_ids, matrix_ids,
        "matrix ⇄ catalog drift: every catalogued dimension needs exactly one matrix row"
    );
    assert!(MATRIX.len() >= 6, "matrix regressed below the six known dimensions");
}

#[test]
fn matrix_maturity_agrees_with_catalog() {
    // Each row's expected_maturity must match the catalog — a second witness
    // of the ledger, so a maturity edit in one place without the other fails.
    let mut failures = Vec::new();
    for row in MATRIX {
        let d = breathe_invariant::catalog::dimension(row.dimension).unwrap();
        if d.maturity != row.expected_maturity {
            failures.push(format!(
                "{}: matrix says {:?}, catalog says {:?}",
                row.dimension.as_str(), row.expected_maturity, d.maturity
            ));
        }
    }
    assert!(failures.is_empty(), "maturity drift:\n  - {}", failures.join("\n  - "));
}

#[test]
fn every_shipped_and_landing_dimension_is_conformant() {
    // THE core matrix assertion. For every Shipped/Landing dimension, its
    // fixture — a carved workload — must be breathability-valid (carved,
    // setpoint present, default-on, dual-purpose). Failures aggregate.
    let mut failures: Vec<String> = Vec::new();
    for row in MATRIX {
        if !matches!(row.expected_maturity, Maturity::Shipped | Maturity::Landing) {
            continue;
        }
        let out = check(&fixture::fixture_for(row.dimension));
        if !out.is_valid() {
            failures.push(format!("{}: {:?}", row.dimension.as_str(), out.violations));
        }
        if out.dual_purpose_carves == 0 {
            failures.push(format!(
                "{}: no dual-purpose carve observed — cost+resiliency must be named together",
                row.dimension.as_str()
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} shipped/landing dimension(s) failed the breathability invariant:\n  - {}",
        failures.len(),
        failures.join("\n  - ")
    );
}

#[test]
fn gap_dimensions_are_honestly_uncarved() {
    // The tier-honest half: a Gap dimension's fixture (uncarved + pending) is
    // NOT conformant-by-carve — it is a tracked gap. If a Gap fixture starts
    // carving, the dimension has advanced and its catalog maturity must be
    // promoted DELIBERATELY. The pending-fixture is valid (tracked), but it
    // records ZERO dual-purpose carves — the honest "not yet carved" signal.
    for row in MATRIX {
        if row.expected_maturity != Maturity::Gap {
            continue;
        }
        let out = check(&fixture::fixture_for(row.dimension));
        assert_eq!(
            out.dual_purpose_carves, 0,
            "gap dimension {} is carving — promote its maturity deliberately",
            row.dimension.as_str()
        );
    }
}

#[test]
fn no_dimension_claimed_but_uncarved() {
    // ★ THE LOAD-BEARING TEST. For every catalogued dimension the doctrine
    // CLAIMS, either a Band carves it (maturity >= Landing) OR it carries an
    // explicit pending-breathe note. A claimed dimension with no Band and no
    // pending note — the 155GB / storage-claimed-not-carved class — FAILS the
    // build HERE, so that class is CI-caught, not discovered live.
    let mut uncarved_claims: Vec<&str> = Vec::new();
    for d in CATALOG {
        if d.is_uncarved_claim() {
            uncarved_claims.push(d.id.as_str());
        }
    }
    assert!(
        uncarved_claims.is_empty(),
        "claimed-but-uncarved dimension(s) with NO pending note (the 155GB class): {uncarved_claims:?} \
         — ship a Band or add a pending-breathe note; a doctrine claim without a carving Band is the storage-155GB regression."
    );
}

#[test]
fn the_invariant_catches_the_155gb_storage_class_adversarially() {
    // Adversarial: reproduce the 155GB receipt — a workload consuming storage
    // but leaving it uncarved with NO pending note. The checker MUST report
    // ClaimedButUncarved — proves the gate has teeth, not a vacuous pass.
    let out = check(&fixture::uncarved_claim_fixture(DimensionId::Storage));
    assert!(!out.is_valid(), "an uncarved storage claim must be a violation");
    assert!(
        out.violations.iter().any(|v| matches!(
            v,
            breathe_invariant::check::BreatheViolation::ClaimedButUncarved {
                dimension: DimensionId::Storage
            }
        )),
        "expected a ClaimedButUncarved(storage) violation, got {:?}",
        out.violations
    );
}

#[test]
fn every_dimension_is_dual_purpose() {
    // Clause 6 (the operator directive): every Band is SIMULTANEOUSLY a cost
    // control AND an availability/resiliency maximizer — so every catalog
    // entry names BOTH effects. A Band naming only one is claiming a tradeoff.
    let mut failures = Vec::new();
    for d in CATALOG {
        if d.cost_effect.is_empty() {
            failures.push(format!("{}: no cost effect", d.id.as_str()));
        }
        if d.resiliency_effect.is_empty() {
            failures.push(format!("{}: no resiliency effect", d.id.as_str()));
        }
    }
    assert!(
        failures.is_empty(),
        "dimension(s) not dual-purpose (must name cost AND resiliency, achieved together):\n  - {}",
        failures.join("\n  - ")
    );
}

#[test]
fn critical_workload_must_be_sealed() {
    // ★ THE ISOLATION-DIMENSION LOAD-BEARING TEST — the analog of
    // `no_dimension_claimed_but_uncarved` for the seal. A critical /
    // interference-sensitive workload CANNOT carry a no-isolation posture
    // (BestEffort / zero requests-floor). The per-workload seal is
    // parse-time-rejected; this is the fleet-coverage forcing-function
    // (CeilingC1): the class is CI-caught, not discovered live (the
    // victoria-logs-422 receipt).
    use breathe_invariant::isolation::{
        all_critical_sealed, IsolationPosture, PlacementIsolation, QosClass, SealError,
        WorkloadClass,
    };

    // (a) A Critical-with-BestEffort posture is rejected at construction — the
    //     no-seal posture for a critical workload is unrepresentable.
    let unsealed = IsolationPosture::try_seal(
        WorkloadClass::Critical,
        QosClass::BestEffort,
        0,
        0,
        PlacementIsolation::AntiAffinity,
        false,
    );
    assert_eq!(
        unsealed,
        Err(SealError::CriticalIsBestEffort),
        "a critical workload with BestEffort QoS must be rejected (the victoria-logs-422 class)"
    );

    // (b) A Critical-with-zero-floor is rejected too — no guaranteed reservation.
    assert_eq!(
        IsolationPosture::try_seal(
            WorkloadClass::Critical,
            QosClass::Guaranteed,
            0,
            512,
            PlacementIsolation::AntiAffinity,
            false,
        ),
        Err(SealError::CriticalHasNoFloor),
        "a critical workload with no requests-floor must be rejected"
    );

    // (c) A fleet of sealed criticals is feasible (the fleet-coverage predicate).
    let sealed_critical = IsolationPosture::for_class(WorkloadClass::Critical, 512, 512).unwrap();
    let batch = IsolationPosture::for_class(WorkloadClass::Batch, 0, 0).unwrap();
    assert!(
        all_critical_sealed(&[sealed_critical, batch]),
        "every constructed critical workload is sealed; batch may be unsealed"
    );
}

#[test]
fn the_carve_never_strips_the_seal_of_a_critical_workload() {
    // The carve-preserves-the-seal constraint, at the matrix layer: even when the
    // cost carve wants to right-size a critical workload's reservation DOWN, the
    // seal floor is the lower bound — isolation is preserved THROUGH the carve.
    use breathe_invariant::isolation::{carve_respecting_seal, IsolationPosture, WorkloadClass};
    let critical = IsolationPosture::for_class(WorkloadClass::Critical, 1024, 1024).unwrap();
    // The workload idles; the cost carve would drop to 64 — the seal holds 1024.
    let carved = carve_respecting_seal(64, &critical);
    assert_eq!(carved.target(), 1024, "cost cannot carve a critical reservation below its seal");
    assert!(carved.seal_bound(), "the seal bound the carve (isolation preserved over cost)");
}
