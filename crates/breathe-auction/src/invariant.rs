//! The AUCTION invariant — the seven clauses every spread discharges, each with
//! its honest UNREPRESENTABILITY tier.
//!
//! This is the compute/auction peer of `breathe-invariant::clause` (the six
//! breathability clauses). Where breathability locks the DIMENSION carve, this
//! locks the COMPUTE floor: 100 % spot (never on-demand), cost-optimized
//! arch-native placement, an always-placing degrade ladder, dual-purpose (cost AND
//! resiliency), AZ-safe placement, cost-justified-where-abnormal, and
//! models-stay-current. A spread is a legal point on the auction lattice iff it
//! discharges every clause.
//!
//! Tier honesty is non-negotiable (UNREPRESENTABILITY §II): a `Result::Err` is
//! MITIGATION, a compile error / absent arm is UNREPRESENTABILITY; a ceiling is
//! the correct terminal for a property Rust cannot prove (C1 = no dependent-type
//! coverage proof → CI forcing-function; C2 = external-world observation → runtime
//! control loop). Never round up.

use serde::{Deserialize, Serialize};

/// The seven clauses of the auction invariant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuctionClause {
    /// **A1 Never-on-demand.** Every pool is 100 % spot. CAPACITY IS NOT AN AXIS —
    /// there is no on-demand arm to permute over. In this crate the state is truly
    /// unrepresentable (no field, no enum arm carries on-demand); at the Ruby DSL
    /// boundary it is parse-rejected (`CamelotBuilderNodeGroup::reject_on_demand!`
    /// refuses every on-demand-shaped key + the removed `perf_class`). The removed
    /// `guaranteed-wake`/`dedicated` perf classes are named receipts of the ban.
    NeverOnDemand,
    /// **A2 Arch-native, cost-optimized.** Arch is a FREE cost lever: because the
    /// image is multi-arch (AUTOBUMP emits `arm64`+`amd64`), a workload runs on
    /// whatever arch the auction lands, so the ladder picks the cheapest-deepest
    /// arch. Every resolved arch runs arch-NATIVE families (no cross-emulation,
    /// ever). The multi-arch prerequisite is parse-rejected (a `CostOptimized`
    /// single-arch-image spread is invalid); the live-price resolution is a runtime
    /// observation (C2).
    ArchNativeCostOptimized,
    /// **A3 Evolving-degrade total order.** The auction ladder is a total-order
    /// preference (fastest → floor) whose FLOOR tier is diversified enough that
    /// SOME tier always places — so scarcity NEVER forces on-demand, it degrades
    /// DOWN the ladder. The proof (ranks contiguous, floor ≥ min-diversified, no
    /// on-demand arm) lives in `breathe-catalog::builder`; a coverage quantifier ⇒
    /// a CI forcing-function is the terminal (C1).
    EvolvingDegradeTotalOrder,
    /// **A4 Dual-purpose (the breathability lift).** Every spread is
    /// SIMULTANEOUSLY a cost control AND a resiliency maximizer — one mechanism,
    /// both outcomes: 100 % spot = cheapest AND (across both arch pools) 2× spot
    /// depth = fewer correlated reclaims; scale-to-zero = near-free-idle AND
    /// fresh-node hygiene; multi-AZ = cost-neutral AND survives an AZ loss. A
    /// spread naming only ONE effect is the violation (the both-effects-named gate,
    /// C1).
    DualPurpose,
    /// **A5 Placement-safe.** A single-instance-EBS pod is single-AZ (its volume's
    /// AZ always has a landing node); a stateless / per-replica-EBS workload is
    /// multi-AZ. The declared AZ span is config-caught (`AzTopology`); the subnet's
    /// REAL AZ is a plan-time truth against live AWS — external-world observation
    /// (C2), never a compile proof.
    PlacementSafe,
    /// **A6 Cost-justified-where-abnormal.** Every conflict resolves by COST, and
    /// where the cost answer is COUNTER-INTUITIVE (arm losing at the floor; x86
    /// chosen while we push arm), the spread CARRIES the justification INLINE — the
    /// number + the why + the auto-flip trigger — so a surprising choice is
    /// self-explaining and never re-litigated. The justification-present +
    /// loud-where-arm-loses matrix gate is the terminal (C1).
    CostJustifiedWhereAbnormal,
    /// **A7 Models-stay-current.** Every doctrine-named use-case (SaaS / build-burst
    /// / eyes) MUST resolve to a VALID typed permutation; a molding naming an
    /// unexpressible arch/strategy or an on-demand shape fails the build. The
    /// compute peer of breathability's C4 (the 155 GB class) — a claimed-but-invalid
    /// molding is CI-caught (C1).
    ModelsStayCurrent,
}

impl AuctionClause {
    /// Every clause, canonical order. The partition the matrix asserts.
    pub const ALL: [AuctionClause; 7] = [
        AuctionClause::NeverOnDemand,
        AuctionClause::ArchNativeCostOptimized,
        AuctionClause::EvolvingDegradeTotalOrder,
        AuctionClause::DualPurpose,
        AuctionClause::PlacementSafe,
        AuctionClause::CostJustifiedWhereAbnormal,
        AuctionClause::ModelsStayCurrent,
    ];

    /// The honest tier this clause CAN reach at its destination (never a claim any
    /// given spread is already there).
    #[must_use]
    pub fn achievable_tier(self) -> UnrepTier {
        match self {
            // No on-demand arm in Rust + parse-rejected at the Ruby boundary.
            AuctionClause::NeverOnDemand => UnrepTier::TrulyUnrepLibraryParseWire,
            // The multi-arch prerequisite is parse-rejected; the live-price
            // resolution is external observation → the honest ceiling is C2, but
            // the prerequisite gate is parse-time. Report the stronger-of-the-gate.
            AuctionClause::ArchNativeCostOptimized => UnrepTier::ParseTimeRejected,
            // Coverage / total-order quantifiers + the both-effects + justification
            // + models-current gates: CI forcing-functions (no dependent types).
            AuctionClause::EvolvingDegradeTotalOrder
            | AuctionClause::DualPurpose
            | AuctionClause::CostJustifiedWhereAbnormal
            | AuctionClause::ModelsStayCurrent => UnrepTier::CeilingC1,
            // The subnet's real AZ is only knowable at plan-time against AWS.
            AuctionClause::PlacementSafe => UnrepTier::CeilingC2,
        }
    }

    /// A stable kebab-case rule name (the vocabulary a lint / report prints).
    #[must_use]
    pub fn rule_name(self) -> &'static str {
        match self {
            AuctionClause::NeverOnDemand => "auction-never-on-demand",
            AuctionClause::ArchNativeCostOptimized => "auction-arch-native-cost-optimized",
            AuctionClause::EvolvingDegradeTotalOrder => "auction-evolving-degrade-total-order",
            AuctionClause::DualPurpose => "auction-dual-purpose",
            AuctionClause::PlacementSafe => "auction-placement-safe",
            AuctionClause::CostJustifiedWhereAbnormal => "auction-cost-justified-where-abnormal",
            AuctionClause::ModelsStayCurrent => "auction-models-stay-current",
        }
    }
}

/// The tier ladder from `theory/UNREPRESENTABILITY.md` §II + the two honest
/// ceilings (same shape as `breathe-invariant::UnrepTier`, re-declared so the crate
/// stays standalone).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UnrepTier {
    /// Compile error / no expressible path — the strongest tier.
    TrulyUnrep,
    /// Truly-unrep in the Rust library + parse-time-rejected at the wire.
    TrulyUnrepLibraryParseWire,
    /// `Err` at the deserialize boundary + sealed in-Rust construction.
    ParseTimeRejected,
    /// Runtime lock/clamp/`if is_valid()` — a mitigation row.
    OnlyMitigated,
    /// Best-possible: no dependent-type coverage/total-order proof in Rust; a
    /// mechanical CI forcing-function is the correct terminal.
    CeilingC1,
    /// Best-possible: an external-world observation (live spot price, the subnet's
    /// real AZ) — a runtime evaluation is the correct terminal.
    CeilingC2,
}

impl UnrepTier {
    /// A monotone strength rank (ceilings sit just above only-mitigated — "as good
    /// as it gets" for their clause). A no-round-up gate compares a spread's claimed
    /// tier against its clause's achievable ceiling.
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

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            UnrepTier::TrulyUnrep => "truly-unrep",
            UnrepTier::TrulyUnrepLibraryParseWire => "truly-unrep-library-parse-wire",
            UnrepTier::ParseTimeRejected => "parse-time-rejected",
            UnrepTier::OnlyMitigated => "only-mitigated",
            UnrepTier::CeilingC1 => "ceiling-c1",
            UnrepTier::CeilingC2 => "ceiling-c2",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AuctionClause, UnrepTier};

    #[test]
    fn every_clause_has_a_unique_rule_name() {
        let names: Vec<&str> = AuctionClause::ALL.iter().map(|c| c.rule_name()).collect();
        let mut s = names.clone();
        s.sort_unstable();
        s.dedup();
        assert_eq!(s.len(), names.len(), "clause rule names must be unique");
    }

    #[test]
    fn never_on_demand_is_the_strongest_clause() {
        // A1 is the hard law — it reaches the strongest achievable tier (no arm in
        // Rust + parse-rejected at the wire), stronger than every ceiling clause.
        let a1 = AuctionClause::NeverOnDemand.achievable_tier();
        assert_eq!(a1, UnrepTier::TrulyUnrepLibraryParseWire);
        for c in AuctionClause::ALL {
            if c != AuctionClause::NeverOnDemand && c != AuctionClause::ArchNativeCostOptimized {
                assert!(
                    a1.rank() > c.achievable_tier().rank(),
                    "{} should be weaker than the never-on-demand hard law",
                    c.rule_name()
                );
            }
        }
    }

    #[test]
    fn tier_rank_is_monotone() {
        assert!(UnrepTier::TrulyUnrep.rank() > UnrepTier::TrulyUnrepLibraryParseWire.rank());
        assert!(UnrepTier::TrulyUnrepLibraryParseWire.rank() > UnrepTier::ParseTimeRejected.rank());
        assert!(UnrepTier::ParseTimeRejected.rank() > UnrepTier::CeilingC1.rank());
        assert!(UnrepTier::CeilingC1.rank() > UnrepTier::OnlyMitigated.rank());
    }
}
