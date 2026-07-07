//! `breathe-auction` — the **ARCH × AUCTION × SPOT configuration-spread lock**.
//!
//! The COMPUTE/AUCTION companion to [`breathe-invariant`] (the breathability
//! variant/invariant lock). breathe-invariant locks the *dimension* carve
//! (memory/cpu/storage/replica/database → a setpoint, dual-purpose, default-on);
//! this crate locks the orthogonal *compute floor*: the typed configurable
//! PERMUTATION SPACE every compute pool is one point in —
//!
//! > **arch × spot-strategy × auction-ladder × perf-class × placement × interruption**
//!
//! — every combination configurable, with a MOLDING DEFAULT per use-case
//! (SaaS-steady / build-burst / eyes), under the **never-on-demand** hard law and
//! the **breathability dual-purpose** (cost AND resiliency, one mechanism).
//!
//! ## The layers
//!
//! - [`axis`] — the six typed permutation axes. Each is a closed enum, so an
//!   anti-posture value is unrepresentable (no `lowest-price` strategy, no
//!   `guaranteed-wake` perf class, no on-demand arm). **Arch is a COST-OPTIMIZED
//!   axis** ([`axis::resolve_arch`]): because AUTOBUMP emits multi-arch images, a
//!   workload runs on whatever arch the auction lands, so arch is a free cost
//!   lever the ladder resolves to the cheapest-deepest — self-adjusting when
//!   pricing crosses, never a hardcode.
//! - [`invariant`] — the seven [`invariant::AuctionClause`]s every spread
//!   discharges, each with its honest UNREP tier (never rounded up).
//! - [`spread`] — the [`spread::AuctionSpread`] record + the three
//!   [`spread::MOLDINGS`] + the value-level validity gate ([`spread::AuctionSpread::violations`]).
//!
//! ## What this lock owns vs composes-by-reference
//!
//! It owns the SPACE + the MOLDINGS + the INVARIANT + the verification MATRIX. It
//! composes the concrete realizers BY REFERENCE (each `doctrine_ref`), exactly as
//! breathe-invariant references the band crates without depending on them:
//! `breathe-catalog::builder` (the DegradeTier total-order ladders + BuilderObjective),
//! `pangea-architectures` (Camelot{,Builder}NodeGroup + AzTopology + `reject_on_demand!`),
//! `pangea-spot` (Allocation + Catalog). This keeps the lock decoupled from those
//! surfaces' churn (standalone workspace, no band-crate dep).
//!
//! ## Tier-honest one-liner
//!
//! A configurable spread for arm + auctioning + spot now EXISTS, coherent + typed
//! + defaulted: three moldings, six axes, seven invariant clauses, a cost-optimized
//! arch that resolves builder→arm / floor→x86 / eyes→arm and SAYS SO where arm
//! loses. **Shipped-configurable:** capacity (never-on-demand, truly-unrep),
//! perf-class, evolving-degrade ladder, arch (per-arch node groups + multi-arch
//! images). **Design/gap (named, not rounded up):** the spot-strategy on the EKS
//! managed-NG lane is DROPPED (`StrategyWiring::IgnoredOnManagedNg` — the
//! operator-flagged gap, now CI-visible); the SaaS multi-AZ per-replica placement
//! is the destination over a single-AZ shipped interim; retirada is a skeleton +
//! LiveTODO agent.

pub mod axis;
pub mod invariant;
pub mod spread;

pub use axis::{
    default_placement, resolve_arch, ArchCostSignal, ArchPinReason, ArchSelection, Interruption,
    LadderMode, PerfClass, Placement, ResolvedArch, SpotStrategy, StorageBinding,
    REMOVED_ON_DEMAND_PERF_CLASSES,
};
pub use invariant::{AuctionClause, UnrepTier};
pub use spread::{
    cost_witness, molding, AuctionSpread, CostRationale, CostWitness, Lane, Maturity,
    StrategyWiring, UseCase, BUILD_BURST, COST_WITNESSES, EYES_TINY, MOLDINGS, SAAS_STEADY,
};

/// A compact tier-honest ledger row per permutation axis — what is
/// shipped-configurable vs design/gap, all under never-on-demand +
/// breathability-dual-purpose. Iterated by tooling / a `confirm` report; the
/// matrix asserts it stays honest.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AxisLedgerRow {
    /// The axis name.
    pub axis: &'static str,
    /// Shipped-configurable, or a named design/gap.
    pub maturity: spread::Maturity,
    /// The strongest UNREP tier this axis reaches.
    pub tier: UnrepTier,
    /// One line: what is configurable + where the gap is (tier-honest).
    pub note: &'static str,
}

/// THE AXIS LEDGER — the tier-honest state of every permutation axis. Never
/// rounded up: the spot-strategy-on-managed-NG gap and the retirada LiveTODO are
/// named as gaps/design, not claimed shipped.
pub const AXIS_LEDGER: &[AxisLedgerRow] = &[
    AxisLedgerRow {
        axis: "capacity (never-on-demand)",
        maturity: spread::Maturity::Shipped,
        tier: UnrepTier::TrulyUnrepLibraryParseWire,
        note: "100% spot; NOT an axis — no on-demand arm in Rust, parse-rejected at the Ruby boundary (reject_on_demand!)",
    },
    AxisLedgerRow {
        axis: "arch (cost-optimized)",
        maturity: spread::Maturity::Shipped,
        tier: UnrepTier::ParseTimeRejected,
        note: "cost-optimized default (multi-arch prereq parse-rejected); per-arch node groups + multi-arch AUTOBUMP images shipped; builder→arm / floor→x86 / eyes→arm the current cost answer, self-adjusting",
    },
    AxisLedgerRow {
        axis: "spot-strategy",
        maturity: spread::Maturity::Design,
        tier: UnrepTier::CeilingC1,
        note: "3 strategies configurable + EFFECTIVE on the ASG/EC2-Fleet lane; DROPPED on the EKS-managed-NG lane (IgnoredOnManagedNg) — the operator-flagged gap, now CI-visible",
    },
    AxisLedgerRow {
        axis: "auction-ladder (evolving-degrade)",
        maturity: spread::Maturity::Shipped,
        tier: UnrepTier::CeilingC1,
        note: "DegradeTier total-order ladders (breathe-catalog::builder) proven always-place / never-on-demand; flat-pool mode for tiny eyes",
    },
    AxisLedgerRow {
        axis: "perf-class",
        maturity: spread::Maturity::Shipped,
        tier: UnrepTier::TrulyUnrepLibraryParseWire,
        note: "cost-floor/time-floor (spot-only); guaranteed-wake/dedicated REMOVED (they were on-demand) — no arm exists",
    },
    AxisLedgerRow {
        axis: "placement (AZ)",
        maturity: spread::Maturity::Design,
        tier: UnrepTier::CeilingC2,
        note: "single-instance-EBS ⇒ single-AZ enforced; multi-AZ per-replica is the resilient destination (shipped CamelotNodeGroup single-AZ interim); the subnet's real AZ is plan-time-only (C2)",
    },
    AxisLedgerRow {
        axis: "interruption (retirada)",
        maturity: spread::Maturity::Design,
        tier: UnrepTier::OnlyMitigated,
        note: "Spot::InterruptionHandler skeleton + ASG lifecycle hook shippable/opt-in; the drain agent + NATS reclaim-publish is a NAMED LiveTODO; retry-on-reclaim (builders) is structurally complete",
    },
];

#[cfg(test)]
mod tests {
    use super::{AxisLedgerRow, AXIS_LEDGER};

    #[test]
    fn axis_ledger_names_the_managed_ng_gap_honestly() {
        // The operator's gap must be present + named a design/gap, not shipped.
        let strat: &AxisLedgerRow = AXIS_LEDGER
            .iter()
            .find(|r| r.axis == "spot-strategy")
            .expect("the spot-strategy row must exist");
        assert!(strat.note.contains("DROPPED") && strat.note.contains("managed-NG"), "the gap must be named");
    }

    #[test]
    fn axis_ledger_rows_are_unique() {
        let mut names: Vec<&str> = AXIS_LEDGER.iter().map(|r| r.axis).collect();
        let n = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), n, "axis names must be unique");
    }
}
