//! THE verification MATRIX for the arch × auction × spot configuration spread —
//! the forcing-function that FAILS THE BUILD when a use-case does not resolve to a
//! valid typed permutation, when a never-on-demand / unexpressible-arch /
//! single-effect / un-justified-counter-intuitive permutation slips through, or
//! when the tier-honest ledger drifts.
//!
//! Peer of `breathe-invariant`'s `tests/breathe_dimension_matrix.rs`. Failures
//! aggregate before assert where it aids the operator; every named gate has teeth.

use breathe_auction::axis::{
    ArchSelection, Interruption, LadderMode, PerfClass, Placement, ResolvedArch, SpotStrategy,
    StorageBinding, REMOVED_ON_DEMAND_PERF_CLASSES,
};
use breathe_auction::invariant::AuctionClause;
use breathe_auction::spread::{
    cost_witness, AuctionSpread, Lane, StrategyWiring, UseCase, BUILD_BURST, MOLDINGS, SAAS_STEADY,
};
use breathe_auction::{AXIS_LEDGER, Maturity};

// ── row 1: every use-case resolves to a valid typed permutation ─────────────────

#[test]
fn every_use_case_resolves_to_a_valid_permutation() {
    // Per-molding assert (the allowed typed-assert surface) — reports the first
    // offending molding with its violated rule-names.
    for m in MOLDINGS {
        let v = m.violations();
        assert!(v.is_empty(), "{} molding is an invalid permutation: {:?}", m.use_case.as_str(), v);
    }
}

#[test]
fn matrix_covers_every_use_case() {
    for uc in UseCase::ALL {
        assert!(MOLDINGS.iter().any(|m| m.use_case == uc), "no molding for {}", uc.as_str());
    }
    assert!(MOLDINGS.len() >= 3, "the three named use-cases must each mold a permutation");
}

// ── row 2: never-on-demand is STRUCTURAL (capacity is not an axis) ──────────────

#[test]
fn never_on_demand_is_structural_capacity_is_not_an_axis() {
    // The perf-class axis has EXACTLY the two spot-only arms; the removed on-demand
    // classes are named + genuinely absent from the enum's labels.
    assert_eq!(PerfClass::ALL.len(), 2, "perf-class is spot-only: exactly cost-floor + time-floor");
    let labels: Vec<&str> = PerfClass::ALL.iter().map(|p| p.as_str()).collect();
    for removed in REMOVED_ON_DEMAND_PERF_CLASSES {
        assert!(!labels.contains(&removed), "the on-demand perf class {removed} must have no arm");
    }
    // And the never-on-demand clause is the strongest (truly-unrep library + parse-wire).
    assert_eq!(
        AuctionClause::NeverOnDemand.achievable_tier(),
        breathe_auction::UnrepTier::TrulyUnrepLibraryParseWire
    );
}

// ── row 3: an unexpressible arch / strategy has no arm ──────────────────────────

#[test]
fn anti_posture_strategies_are_unrepresentable() {
    // Only the three our posture permits exist — no lowest-price, no prioritized.
    assert_eq!(SpotStrategy::ALL.len(), 3);
    let labels: Vec<&str> = SpotStrategy::ALL.iter().map(|s| s.as_str()).collect();
    for anti in ["lowest-price", "capacity-optimized-prioritized"] {
        assert!(!labels.contains(&anti), "the anti-posture strategy {anti} must not be an arm");
    }
    // Arch has exactly the cost-optimized default + the two justified pins.
    assert_eq!(ArchSelection::ALL.len(), 3);
    assert_eq!(ArchSelection::default_selection(), ArchSelection::CostOptimized);
}

// ── row 4: every molding is DUAL-PURPOSE (cost AND resiliency) ──────────────────

#[test]
fn every_molding_is_dual_purpose() {
    for m in MOLDINGS {
        assert!(!m.cost_effect.is_empty(), "{}: cost_effect must be named", m.use_case.as_str());
        assert!(!m.resiliency_effect.is_empty(), "{}: resiliency_effect must be named", m.use_case.as_str());
    }
}

// ── row 5: every conflict is cost-justified INLINE, loud where abnormal ─────────

#[test]
fn every_molding_carries_inline_cost_justification() {
    for m in MOLDINGS {
        assert!(!m.cost_rationale.rationale.is_empty(), "{}: rationale must be named", m.use_case.as_str());
        assert!(!m.cost_rationale.auto_flip_when.is_empty(), "{}: auto-flip trigger must be named (self-adjusting)", m.use_case.as_str());
    }
}

#[test]
fn arm_losing_is_always_loud() {
    // EVERY place arm is the wrong cost answer (an amd64 choice) MUST say so plainly
    // — the operator's "be vocal where arm is not winning" made a build gate.
    for m in MOLDINGS {
        if m.cost_rationale.chosen_arch == ResolvedArch::Amd64 {
            let ci = m
                .cost_rationale
                .counterintuitive
                .unwrap_or_else(|| panic!("{}: an x86 cost choice must carry a LOUD counter-intuitive note", m.use_case.as_str()));
            assert!(ci.contains('%'), "{}: the loud note must name the % delta", m.use_case.as_str());
            assert!(
                ci.contains("loses") || ci.contains("LOSES") || ci.contains("pricier") || ci.contains("PRICIER"),
                "{}: the loud note must plainly say arm loses / is pricier",
                m.use_case.as_str()
            );
        }
    }
    // and the SaaS floor is the concrete x86-loud case (the operator's example).
    assert_eq!(SAAS_STEADY.cost_rationale.chosen_arch, ResolvedArch::Amd64);
    assert!(SAAS_STEADY.cost_rationale.counterintuitive.is_some());
}

// ── row 6: the arch is a genuine COST resolution (not a hardcode) ───────────────

#[test]
fn every_molding_arch_flips_with_the_price_signal() {
    use breathe_auction::axis::ArchCostSignal;
    for m in MOLDINGS {
        let w = cost_witness(m.use_case).expect("witness");
        assert_eq!(m.resolved_arch(w.signal), m.cost_rationale.chosen_arch, "{}: arch must equal the cost resolution", m.use_case.as_str());
        let flipped = ArchCostSignal {
            arm64_effective_cost: w.signal.amd64_effective_cost,
            amd64_effective_cost: w.signal.arm64_effective_cost,
        };
        assert_ne!(m.resolved_arch(flipped), m.resolved_arch(w.signal), "{}: flipping price must flip arch", m.use_case.as_str());
    }
}

// ── row 7: the operator-flagged managed-NG strategy gap is CI-visible ───────────

#[test]
fn the_managed_ng_strategy_gap_is_expressible_and_enforced() {
    // A price-capacity-optimized strategy on the EKS-managed-NG lane claimed
    // Effective is INVALID (the strategy is dropped there); marking it ignored is
    // honest + valid. This makes the gap a build gate, not a silent computed field.
    let dishonest = AuctionSpread {
        lane: Lane::EksManagedNodeGroup,
        spot_strategy: SpotStrategy::PriceCapacityOptimized,
        strategy_wiring: StrategyWiring::Effective,
        ..BUILD_BURST
    };
    assert!(!dishonest.is_valid(), "a dropped strategy claimed effective must be rejected");

    let honest = AuctionSpread {
        strategy_wiring: StrategyWiring::IgnoredOnManagedNg,
        ..dishonest
    };
    assert!(honest.is_valid(), "marking the gap ignored-on-managed-ng is honest + valid");
}

// ── row 8: placement-safe (single-instance-EBS ⇒ single-AZ) ─────────────────────

#[test]
fn single_instance_ebs_moldings_are_single_az() {
    for m in MOLDINGS {
        if m.storage_binding == StorageBinding::SingleInstanceEbs {
            assert_eq!(m.placement, Placement::SingleAz, "{}: single-instance-EBS must be single-AZ", m.use_case.as_str());
        }
    }
}

// ── row 9: the tier-honest ledger covers every axis + names the gaps ────────────

#[test]
fn axis_ledger_partitions_and_names_the_gaps() {
    // one row per axis, unique; the two known gaps are Design, not Shipped.
    let mut names: Vec<&str> = AXIS_LEDGER.iter().map(|r| r.axis).collect();
    let n = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), n, "axis ledger names must be unique");
    assert!(n >= 7, "every permutation axis + capacity has a ledger row");

    let strat = AXIS_LEDGER.iter().find(|r| r.axis == "spot-strategy").expect("spot-strategy row");
    assert_eq!(strat.maturity, Maturity::Design, "the managed-NG strategy gap keeps spot-strategy at Design");
    let retirada = AXIS_LEDGER.iter().find(|r| r.axis == "interruption (retirada)").expect("retirada row");
    assert_eq!(retirada.maturity, Maturity::Design, "retirada agent is a LiveTODO, not shipped");
}

// ── row 10: no clause claims above its honest ceiling ───────────────────────────

#[test]
fn no_clause_rounds_up_past_its_ceiling() {
    use breathe_auction::UnrepTier;
    for c in AuctionClause::ALL {
        let t = c.achievable_tier();
        // a ceiling clause never claims a truly-unrep tier it cannot reach.
        if matches!(t, UnrepTier::CeilingC1 | UnrepTier::CeilingC2) {
            assert!(t.rank() < UnrepTier::ParseTimeRejected.rank(), "{}: a ceiling must not out-rank parse-time", c.rule_name());
        }
    }
}

// ── row 11: Lisp ↔ Rust vocabulary bridge (the /vocabulary-bridging cross-check) ─

#[test]
fn the_moldings_are_declared_in_the_lisp() {
    const AUCTION_LISP: &str = include_str!("../specs/auction.lisp");
    assert!(AUCTION_LISP.contains("defauction-spread"), "the lisp must declare the (defauction-spread) form");
    for m in MOLDINGS {
        assert!(AUCTION_LISP.contains(m.use_case.as_str()), "the lisp is missing the {} molding", m.use_case.as_str());
    }
    // the six axes' vocabulary is present.
    for tok in ["cost-optimized", "capacity-optimized", "evolving-degrade", "time-floor", "single-az", "retirada"] {
        assert!(AUCTION_LISP.contains(tok), "the lisp is missing the axis token {tok}");
    }
    // the never-on-demand hard law + the loud floor case are named in the lisp.
    assert!(AUCTION_LISP.contains("never-on-demand"), "the lisp must name the never-on-demand law");
    assert!(AUCTION_LISP.contains("arm-loses") || AUCTION_LISP.contains("arm loses"), "the lisp must name the loud floor case");
}

// ── row 12: the interruption axis is honest about the retirada LiveTODO ─────────

#[test]
fn builder_uses_retry_on_reclaim_no_agent_needed() {
    assert_eq!(BUILD_BURST.interruption, Interruption::RetryOnReclaim);
    assert!(!BUILD_BURST.interruption.uses_retirada(), "the builder needs no drain agent (idempotent + cache-backed)");
    // the ladder mode is evolving-degrade for the builder, flat-pool only for the tiny eyes.
    assert_eq!(BUILD_BURST.ladder, LadderMode::EvolvingDegrade);
}
