//! The six BREATHABILITY clauses + their honest UNREPRESENTABILITY tiers.
//!
//! Breathability is ONE law with six clauses. Every resource *dimension* a
//! workload consumes (memory, cpu, storage, replica-count, per-engine
//! database) discharges the same clauses; today each does so with its own
//! `*Band` primitive in the breathe substrate (`MemoryBand`, `CpuBand`,
//! `StorageBand`, `ReplicaBand`, …). This module names the law once so the
//! catalog + matrix can hold every dimension against the SAME contract — the
//! typed peer of `gen-pdc` (per-dependency caching) and `gen-secattest`
//! (security attestation): build / security / breathability are three
//! instances of the same variant/invariant discipline.
//!
//! Canonical doctrine: `theory/BREATHABILITY.md` (+ `-CARVING`, `-SCALING`).

use serde::{Deserialize, Serialize};

/// The six clauses of the breathability invariant. A dimension is a legal
/// point on the breathe lattice iff its carving discharges every clause.
///
/// > **breathability = every resource dimension a workload consumes is
/// > continuously CARVED to its utilization setpoint, by DEFAULT, fleet-wide
/// > — and the same carve is simultaneously a cost control AND an
/// > availability/resiliency maximizer.**
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BreatheClause {
    /// **C1 Carved-by-a-Band.** Every resource dimension a workload consumes
    /// is carved by a typed `*Band` primitive — memory, cpu, storage,
    /// replica, per-engine-database. A consumed-but-uncarved dimension is the
    /// violation. This is a coverage quantifier (∀ consumed dim, ∃ Band) — no
    /// dependent-type proof in Rust; a CI forcing-function is the terminal
    /// answer (the `no_dimension_claimed_but_uncarved` matrix test).
    CarvedByABand,
    /// **C2 Carve-to-a-setpoint.** Carving drives utilization to a SETPOINT
    /// (a utilization target, e.g. 80/20) — continuous, not a static request.
    /// The setpoint VALUE is a sealed [`crate::setpoint::UtilizationSetpoint`]
    /// (out-of-range rejected at parse; a bad catalog setpoint is a const-eval
    /// compile error) — so a carve with no valid setpoint is unrepresentable
    /// past the boundary. HOLDING live utilization at the setpoint is a
    /// control loop over the real world (the C2 external-world ceiling).
    CarveToSetpoint,
    /// **C3 Default-on fleet-wide.** Breathability is not opt-in, not
    /// per-env: every workload is breathe-managed by default (`pleme-lib
    /// global.breathe` + `breathe-admission`). A workload admitted without a
    /// breathe posture is the violation — an admission gate is the terminal
    /// forcing-function (CeilingC1); today only-mitigated (a default, not an
    /// absence).
    DefaultOnFleetWide,
    /// **C4 Models-stay-current (THE load-bearing clause).** A dimension
    /// NAMED in the breathability doctrine MUST be backed by a shipped (or
    /// landing) carving Band — a claimed-but-uncarved dimension is a
    /// violation. This is the CI-caught form of the 155 GB receipt (storage
    /// was doctrine-claimed as "elasticity" but never carved → 155 GB
    /// provisioned / 5 GB used). A cross-artifact coverage property ⇒ a CI
    /// forcing-function (CeilingC1) is the honest terminal.
    ModelsStayCurrent,
    /// **C5 Discovery-molded.** The carve config is DISCOVERED
    /// (kanchi-style: default ← discovered ← override, shikumi precedence),
    /// not hand-tuned — especially the database architecture (master /
    /// multi-reader / distributed topology discovered + molded). Discovery
    /// reads the real cluster (external-world observation ⇒ the C2 ceiling).
    DiscoveryMolded,
    /// **C6 Dual-purpose (the unification).** Every `*Band` is
    /// SIMULTANEOUSLY a cost control AND an availability/resiliency maximizer
    /// — one mechanism, both outcomes, by construction, tuned continuously
    /// over time. Carving-to-setpoint is NOT a cost-vs-resiliency tradeoff;
    /// it IS both at once (StorageBand: provision-minimal + grow-before-full;
    /// ReplicaBand: scale-down-idle + floor-2 HA; Memory/Cpu: right-size-down
    /// + setpoint headroom-before-saturation). A Band that names only one
    /// effect is the violation — the both-effects-named matrix gate is the
    /// terminal forcing-function (CeilingC1); the "same carve maximizes
    /// uptime AND saves money" claim is a structural design property, not a
    /// Rust type.
    DualPurpose,
}

impl BreatheClause {
    /// Every clause, in canonical order. The partition the matrix asserts.
    pub const ALL: [BreatheClause; 6] = [
        BreatheClause::CarvedByABand,
        BreatheClause::CarveToSetpoint,
        BreatheClause::DefaultOnFleetWide,
        BreatheClause::ModelsStayCurrent,
        BreatheClause::DiscoveryMolded,
        BreatheClause::DualPurpose,
    ];

    /// The honest UNREPRESENTABILITY tier the clause CAN reach at its
    /// destination (never a claim any given dimension is already there — a
    /// dimension's per-clause status lives in the catalog, tier-honest).
    #[must_use]
    pub fn achievable_tier(self) -> UnrepTier {
        match self {
            // Coverage quantifier over the doctrine's claimed set — a CI
            // forcing-function is the terminal answer (no dependent types).
            BreatheClause::CarvedByABand
            | BreatheClause::DefaultOnFleetWide
            | BreatheClause::ModelsStayCurrent
            | BreatheClause::DualPurpose => UnrepTier::CeilingC1,
            // The setpoint value is a sealed refined type (const-eval compile
            // error for a bad catalog value; parse-rejected on the wire).
            BreatheClause::CarveToSetpoint => UnrepTier::ParseTimeRejected,
            // Discovery reads the live cluster — external-world observation.
            BreatheClause::DiscoveryMolded => UnrepTier::CeilingC2,
        }
    }

    /// A stable kebab-case rule name — the clause vocabulary a `confirm`
    /// report / cse-lint prints, one name fleet-wide.
    #[must_use]
    pub fn rule_name(self) -> &'static str {
        match self {
            BreatheClause::CarvedByABand => "breathe-carved-by-a-band",
            BreatheClause::CarveToSetpoint => "breathe-carve-to-setpoint",
            BreatheClause::DefaultOnFleetWide => "breathe-default-on-fleet-wide",
            BreatheClause::ModelsStayCurrent => "breathe-models-stay-current",
            BreatheClause::DiscoveryMolded => "breathe-discovery-molded",
            BreatheClause::DualPurpose => "breathe-dual-purpose",
        }
    }
}

/// The tier ladder from `theory/UNREPRESENTABILITY.md` §II, plus the two
/// honest ceilings. Never round up: a `Result::Err` is mitigation, a compile
/// error / absent path is unrepresentability; a ceiling is the correct
/// terminal for a property Rust's type system provably cannot prove.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UnrepTier {
    /// Compile error / no expressible path — the strongest tier.
    TrulyUnrep,
    /// truly-unrep in the Rust library + parse-time-rejected at the wire.
    TrulyUnrepLibraryParseWire,
    /// `Err` at the deserialize boundary + sealed in-Rust construction.
    ParseTimeRejected,
    /// runtime lock/clamp/`if is_valid()` — a `Vec<Violation>` row.
    OnlyMitigated,
    /// Best-possible: no dependent-type coverage/reachability proof in Rust;
    /// a mechanical CI forcing-function is the correct terminal answer.
    CeilingC1,
    /// Best-possible: the property is an external-world observation (live
    /// utilization, discovered topology) — a runtime control loop is the
    /// correct terminal, never a compile-time proof.
    CeilingC2,
}

impl UnrepTier {
    /// A monotone strength rank so a "no round-up" gate can compare a
    /// dimension's claimed tier against its clause's achievable ceiling.
    /// Ceilings sit just above only-mitigated — they are "as good as it gets"
    /// for their clause, and a dimension may claim at most its ceiling.
    #[must_use]
    pub fn rank(self) -> u8 {
        match self {
            UnrepTier::OnlyMitigated => 0,
            UnrepTier::CeilingC2 | UnrepTier::CeilingC1 => 1,
            UnrepTier::ParseTimeRejected => 2,
            UnrepTier::TrulyUnrepLibraryParseWire => 3,
            UnrepTier::TrulyUnrep => 4,
        }
    }
}
