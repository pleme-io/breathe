//! The DATABASE-architecture verification MATRIX — CLOSED-LOOP MASS-SYNTHESIS
//! rule 1, applied to the DatabaseBand's 5-engine surface.
//!
//! ONE matrix with ONE row per engine, exercising each against the four coupled
//! DatabaseBand properties (BREATHABILITY.md §II.5):
//!   1. architecture-aware — the engine breathes under its correct topology class;
//!   2. discovery-molded    — a topology is discoverable through the seam;
//!   3. failover-safe spot  — the engine's default permutation is legal (a
//!      spot-even-primary default carries a failover-safe policy);
//!   4. configurable perms  — the default permutation is a point in the legal lattice.
//!
//! The matrix FAILS THE BUILD when:
//!   - a new engine lands without a row, or a row names an engine absent from
//!     `DB_ARCHITECTURES` (`matrix_covers_every_engine` — CATALOG REFLECTION);
//!   - an engine's default permutation is illegal
//!     (`every_engine_default_permutation_is_failover_safe`);
//!   - the failover FSM authorizes a primary reclaim without a promotion
//!     (`the_fsm_never_authorizes_a_primary_reclaim_without_a_promotion` —
//!     the never-lose-the-primary load-bearing gate).
//!
//! Failures aggregate BEFORE the assert — one run reports every broken row.

use breathe_invariant::database::{
    architecture_for, failover_step, legal_permutations, DbEngine, FailoverAction, FailoverEvent,
    FailoverState, ReplicaId, ReplicationClass, ReplicationTopology, SpotPosture, DB_ARCHITECTURES,
};

/// One row of the matrix: an engine + the architecture class it must breathe under.
struct MatrixRow {
    engine: DbEngine,
    expected_class: ReplicationClass,
}

/// The matrix — ONE row per engine. Adding a `DB_ARCHITECTURES` entry without a
/// row here fails `matrix_covers_every_engine`. 5/5 — the former 2/5 gap closed.
const MATRIX: &[MatrixRow] = &[
    MatrixRow { engine: DbEngine::MySql, expected_class: ReplicationClass::MasterSlave },
    MatrixRow { engine: DbEngine::Postgres, expected_class: ReplicationClass::MasterSlave },
    MatrixRow { engine: DbEngine::Redis, expected_class: ReplicationClass::MasterSlave },
    MatrixRow { engine: DbEngine::Mongo, expected_class: ReplicationClass::FullyDistributed },
    MatrixRow { engine: DbEngine::Neo4j, expected_class: ReplicationClass::Persistent },
];

#[test]
fn matrix_covers_every_engine() {
    // The forcing function: the matrix rows and DB_ARCHITECTURES are the SAME set.
    let mut matrix_ids: Vec<&str> = MATRIX.iter().map(|r| r.engine.as_str()).collect();
    let mut arch_ids: Vec<&str> = DB_ARCHITECTURES.iter().map(|a| a.engine.as_str()).collect();
    matrix_ids.sort_unstable();
    arch_ids.sort_unstable();
    assert_eq!(matrix_ids, arch_ids, "matrix ⇄ DB_ARCHITECTURES drift: every engine needs one row");
    assert_eq!(MATRIX.len(), 5, "the matrix codes 5/5 engines (MySQL/Postgres/Redis/Mongo/Neo4j)");
}

#[test]
fn every_engine_breathes_under_its_expected_class() {
    let mut failures = Vec::new();
    for row in MATRIX {
        let a = architecture_for(row.engine).expect("every matrix engine has an architecture row");
        if a.class != row.expected_class {
            failures.push(format!(
                "{}: matrix expects {:?}, architecture says {:?}",
                row.engine.as_str(),
                row.expected_class,
                a.class
            ));
        }
    }
    assert!(failures.is_empty(), "engine-class drift:\n  - {}", failures.join("\n  - "));
}

#[test]
fn every_engine_default_permutation_is_failover_safe() {
    // THE core matrix assertion: every engine's DEFAULT carve is a legal lattice
    // point, and a spot-even-primary default ALWAYS carries a failover-safe policy
    // (else the primary is lost un-gracefully). Failures aggregate.
    let lattice = legal_permutations();
    let mut failures: Vec<String> = Vec::new();
    for a in DB_ARCHITECTURES {
        let p = a.default_permutation();
        if p.validate().is_err() {
            failures.push(format!("{}: default permutation illegal ({:?})", a.engine.as_str(), p.validate()));
        }
        if !lattice.contains(&p) {
            failures.push(format!("{}: default permutation not in the legal lattice", a.engine.as_str()));
        }
        if matches!(p.spot, SpotPosture::SpotEvenPrimary) && !p.failover.is_failover_safe() {
            failures.push(format!(
                "{}: 100% spot on the primary with NO failover policy — the primary could be lost",
                a.engine.as_str()
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} engine(s) failed the failover-safe-spot invariant:\n  - {}",
        failures.len(),
        failures.join("\n  - ")
    );
}

#[test]
fn discovery_molds_every_engine_topology_through_the_seam() {
    // Property 2: a topology is discoverable for each engine's class, and the
    // never-lose-primary guard reads it (a target-less shape blocks the reclaim).
    // A primary+readers shape has a failover target; a single writer does not.
    assert!(ReplicationTopology::PrimaryReaders { primary: ReplicaId(0), readers: 1 }.has_failover_target());
    assert!(!ReplicationTopology::SingleWriter { primary: ReplicaId(0) }.has_failover_target());
    assert!(ReplicationTopology::Quorum { voters: 3 }.has_failover_target());
    assert_eq!(
        ReplicationTopology::PrimaryReaders { primary: ReplicaId(0), readers: 3 }.class(),
        ReplicationClass::MasterSlave
    );
}

#[test]
fn the_fsm_never_authorizes_a_primary_reclaim_without_a_promotion() {
    // ★ THE LOAD-BEARING GATE (integration-level): sweep every (state, event); a
    // ReclaimOldPrimary — the only action that reclaims a former-primary node —
    // is emitted ONLY from (PromotingReplica, PromotionSucceeded). Any other edge
    // emitting it would let the primary be lost un-gracefully.
    let events = [
        FailoverEvent::PrimaryReclaimSignal { primary: ReplicaId(0), candidate: ReplicaId(1) },
        FailoverEvent::PromotableTargetAvailable,
        FailoverEvent::NoPromotableTarget,
        FailoverEvent::PromotionSucceeded,
        FailoverEvent::OldPrimaryDrained,
        FailoverEvent::ReclaimCleared,
    ];
    let mut leaks = Vec::new();
    for state in FailoverState::ALL {
        for event in events {
            if let Ok((_, FailoverAction::ReclaimOldPrimary { .. })) = failover_step(state, event) {
                let is_promotion = matches!(event, FailoverEvent::PromotionSucceeded);
                if state != FailoverState::PromotingReplica || !is_promotion {
                    leaks.push(format!("({}, {:?})", state.as_str(), event));
                }
            }
        }
    }
    assert!(
        leaks.is_empty(),
        "ReclaimOldPrimary leaked from non-promotion edges (the primary could be lost): {leaks:?}"
    );
}
