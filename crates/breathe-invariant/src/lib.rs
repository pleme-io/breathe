//! breathe-invariant — the BREATHABILITY variant/invariant CONTRACT.
//!
//! ## What this crate is
//!
//! The typed LOCK of the breathability doctrine — the same shape as
//! `gen-pdc` (per-dependency caching) and `gen-secattest` (security
//! attestation). **Build / security / breathability are three instances of
//! the same variant/invariant discipline:** a named INVARIANT (a small set of
//! clauses, each with its honest UNREPRESENTABILITY tier), a per-instance
//! VARIANT catalog (each a typed lattice point with a tier-honest maturity),
//! and a verification MATRIX forcing-function (a new instance cannot ship
//! without a row).
//!
//! This crate COMPOSES the shipped/landing breathe substrate by REFERENCE —
//! it names the `*Band` primitives (`MemoryBand`, `CpuBand`, `StorageBand`,
//! `ReplicaBand`, `DatabaseBand`) and their doctrine anchors as data, and does
//! NOT depend on the breathe band crates (deliberately decoupled from their
//! churn, exactly as `gen-pdc` stays decoupled from the adapter crates). The
//! breathe substrate REALIZES the carve; this crate is the CONTRACT the
//! doctrine surfaces (`theory/BREATHABILITY.md`, the `/breathability` +
//! `/camelot` skills, the org rule) point to as the canonical lock.
//!
//! ## The invariant
//!
//! > **breathability = every resource dimension a workload consumes is
//! > continuously CARVED to its utilization setpoint, by DEFAULT, fleet-wide
//! > — and the same carve is simultaneously a cost control AND an
//! > availability/resiliency maximizer, tuned continuously over time.**
//!
//! Six clauses ([`clause`]):
//! 1. **carved-by-a-Band** — every consumed dimension is carved by a `*Band`.
//! 2. **carve-to-a-setpoint** — carving drives utilization to a sealed
//!    [`setpoint::UtilizationSetpoint`] (out-of-range = compile/parse error).
//! 3. **default-on fleet-wide** — not opt-in, not per-env.
//! 4. **models-stay-current (load-bearing)** — a doctrine-claimed dimension
//!    MUST be backed by a shipped/landing Band; a claimed-but-uncarved
//!    dimension is a violation (the 155GB receipt, now CI-caught).
//! 5. **discovery-molded** — the carve config is discovered, not hand-tuned.
//! 6. **dual-purpose** — every Band is SIMULTANEOUSLY a cost control AND an
//!    availability/resiliency maximizer; cost and resiliency are achieved
//!    TOGETHER by the one carve, never traded.
//!
//! ## The forcing-function
//!
//! `tests/breathe_dimension_matrix.rs` — one row per dimension asserting
//! {carved-by-a-Band, to-setpoint, default-on, dual-purpose, tier}. THE
//! load-bearing test, `no_dimension_claimed_but_uncarved`, fails the build if
//! a doctrine-named dimension has no shipped Band and no pending note — making
//! the 155GB / storage-claimed-not-carved class CI-CAUGHT, not discovered
//! live. `matrix_covers_every_dimension` fails on catalog⇄matrix drift.
//!
//! Composes GEN-TYPED-SPEC-CONTRACT, CATALOG REFLECTION, CLOSED-LOOP
//! MASS-SYNTHESIS (rule 1), UNREPRESENTABILITY, and the breathe substrate
//! (BREATHABILITY.md + -CARVING + -SCALING).

#![allow(clippy::module_name_repetitions)]

pub mod carve;
pub mod catalog;
pub mod check;
pub mod clause;
pub mod dimension;
pub mod fixture;
pub mod setpoint;

pub use carve::{
    carve_to_setpoint, fill_velocity, realized_utilization, seconds_to_full, CarveLaw,
    MultiplicativeBand,
};
pub use catalog::{dimension, maturity_histogram, CATALOG};
pub use check::{check, BreatheCheckOutcome, BreatheViolation, DimensionUse, WorkloadProfile};
pub use clause::{BreatheClause, UnrepTier};
pub use dimension::{
    BreatheDimension, CarveAlgorithm, ClauseStatus, DimensionId, DiscoveryStrategy, Maturity,
};
pub use setpoint::{SetpointError, UtilizationSetpoint};

/// The six-clause breathability invariant, one line each — the
/// human-readable form of the doctrine header ([`clause::BreatheClause`] is
/// the typed form). The doctrine-parity test binds the prose to the enum so
/// they can never drift apart in count or order.
pub const BREATHE_CLAUSES: [&str; 6] = [
    "1. carved-by-a-band: every resource dimension a workload consumes is carved by a typed *Band",
    "2. carve-to-a-setpoint: carving drives utilization to a sealed setpoint (out-of-range unrepresentable)",
    "3. default-on-fleet-wide: breathability is on by default, not opt-in, not per-env",
    "4. models-stay-current: a doctrine-claimed dimension must be a shipped/landing Band (else a violation)",
    "5. discovery-molded: the carve config is discovered (default<-discovered<-override), not hand-tuned",
    "6. dual-purpose: every Band is a cost control AND an availability/resiliency maximizer, together not traded",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prose_clauses_and_typed_clauses_agree_on_count_and_order() {
        // The doctrine-parity gate: the human-readable clause strings and the
        // typed `BreatheClause` partition stay in lock-step. Add a clause to
        // one and not the other → this fails; the prose can never round
        // up/down past the typed enum.
        assert_eq!(BREATHE_CLAUSES.len(), BreatheClause::ALL.len());
        assert_eq!(BREATHE_CLAUSES.len(), 6);
        let expected_prefixes = ["1. ", "2. ", "3. ", "4. ", "5. ", "6. "];
        for (i, clause) in BreatheClause::ALL.iter().enumerate() {
            let _ = clause.achievable_tier();
            assert!(
                BREATHE_CLAUSES[i].starts_with(expected_prefixes[i]),
                "prose clause {} must be 1-indexed to its typed position",
                i + 1
            );
        }
    }

    #[test]
    fn the_contract_is_declared_in_the_lisp() {
        // The /vocabulary-bridging cross-check (same include_str! convention
        // breathe-catalog::db_matrix uses): every dimension name + clause rule
        // name appears in the authored lisp, so the Rust border and the
        // (defbreathe-invariant …) / (defband …) lisp vocabulary can never
        // drift.
        const LISP: &str = include_str!("../specs/breathe-invariant.lisp");
        assert!(LISP.contains("defbreathe-invariant"), "lisp must declare (defbreathe-invariant …)");
        for d in CATALOG {
            assert!(LISP.contains(d.band_keyword), "lisp missing band keyword {}", d.band_keyword);
            assert!(LISP.contains(d.band), "lisp missing band {}", d.band);
        }
        for c in BreatheClause::ALL {
            assert!(LISP.contains(c.rule_name()), "lisp missing clause {}", c.rule_name());
        }
    }
}
