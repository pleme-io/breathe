//! The `AuctionSpread` — one typed point in the compute/auction permutation
//! space, plus the MOLDING catalog (a spread per use-case) and the validity gate.
//!
//! A spread composes the six axes ([`crate::axis`]) into one configurable record
//! under the seven-clause invariant ([`crate::invariant`]). The MOLDINGS are the
//! molding DEFAULTS the operator asked for — SaaS-steady / build-burst / eyes —
//! each a valid permutation with its inline, loud-where-abnormal COST rationale
//! ([`CostRationale`]). Every conflict is resolved by COST; where the cost answer
//! is counter-intuitive (arm LOSING at the floor) the rationale SAYS SO with the
//! number + the auto-flip trigger, so the surprising choice is self-explaining and
//! never re-litigated.

use crate::axis::{
    resolve_arch, ArchCostSignal, ArchPinReason, ArchSelection, Interruption, LadderMode,
    PerfClass, Placement, ResolvedArch, SpotStrategy, StorageBinding,
};

/// The use-case a molding arms — the operator's three named shapes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UseCase {
    /// The steady, HA-floored akeyless SaaS + gateway pool (CamelotNodeGroup).
    SaasSteady,
    /// The bursty, floor-0, cache-backed Nix builder pool (CamelotBuilderNodeGroup
    /// + super-cache-ci).
    BuildBurst,
    /// The tiny observability tap (grafana / victoria / vector — the "eyes").
    EyesTiny,
}

impl UseCase {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SaasSteady => "saas-steady",
            Self::BuildBurst => "build-burst",
            Self::EyesTiny => "eyes-tiny",
        }
    }

    pub const ALL: [UseCase; 3] = [Self::SaasSteady, Self::BuildBurst, Self::EyesTiny];
}

/// The AWS lane a pool realizes on — the shipped substrates + the progressively-
/// discovered platform shape. This is what decides whether the spot-strategy
/// axis is EFFECTIVE, DROPPED, or structurally NOT-APPLICABLE
/// ([`StrategyWiring`]) — the typed answer to "discover whether we're on an ASG,
/// an EKS managed node group, or a lone self-managed instance, and load the
/// matching config matrix" (the progressive-platform-discovery axis correnteza's
/// M0 extends; see `theory/CORRENTEZA.md`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Lane {
    /// A standalone mixed-instances ASG / EC2 Fleet (`Spot::MixedInstancesAsg`) —
    /// exposes `spot_allocation_strategy`, so every strategy takes effect.
    MixedInstancesAsg,
    /// An EKS managed node group (`EksDrillNodeGroup`) — takes `capacity_type` but
    /// does NOT expose `SpotAllocationStrategy`. The operator-flagged gap surface.
    EksManagedNodeGroup,
    /// A single, hand-provisioned EC2 instance running self-managed k3s directly
    /// — no ASG, no EC2 Fleet, no EKS node group wrapping it (e.g. Camelot's
    /// bootstrap/control-plane node, brought up outside the
    /// `CamelotNodeGroup`/`CamelotBuilderNodeGroup` EKS-managed-node-group
    /// architectures). Distinct from both pool lanes above: a lone instance has
    /// no *distribution-among-launch-specs* decision to make, so the
    /// spot-strategy axis has no field to bind on this lane at all — see
    /// [`StrategyWiring::NotApplicableSingleInstance`].
    StandaloneEc2Instance,
}

impl Lane {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MixedInstancesAsg => "mixed-instances-asg",
            Self::EksManagedNodeGroup => "eks-managed-node-group",
            Self::StandaloneEc2Instance => "standalone-ec2-instance",
        }
    }

    pub const ALL: [Lane; 3] =
        [Self::MixedInstancesAsg, Self::EksManagedNodeGroup, Self::StandaloneEc2Instance];
}

/// How the spot ALLOCATION strategy is wired on this pool's lane — the tier-honest
/// encoding of the operator-flagged gap: `Pangea::Spot::Allocation` is ASG /
/// EC2-Fleet-wired, NOT managed-node-group-wired — plus the structurally
/// DIFFERENT single-instance case (there is no gap to name there; there is no
/// axis to bind at all).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StrategyWiring {
    /// The strategy takes effect: the ASG lane passes `spot_allocation_strategy`
    /// (`mixed_instances_asg.rb`), AND the managed-NG case where the request is
    /// `capacity-optimized` (which aligns with EKS's own internal default).
    Effective,
    /// **THE GAP.** The strategy is COMPUTED but DROPPED — an EKS managed node
    /// group does not expose `SpotAllocationStrategy`, so a
    /// `price-capacity-optimized` / `diversified` request silently degrades to
    /// EKS's internal fixed strategy. Carried on the spread so the gap is
    /// CI-VISIBLE, never a silent computed-but-ignored field.
    IgnoredOnManagedNg,
    /// **NOT A GAP — structurally inapplicable.** A lone EC2 instance's spot
    /// request (`RunInstances` + `InstanceMarketOptions.SpotOptions`) has no
    /// `AllocationStrategy` field at all — that concept exists only for a POOL
    /// choosing AMONG MULTIPLE launch specs (an ASG/Fleet). There is nothing
    /// computed-then-dropped here, unlike [`Self::IgnoredOnManagedNg`]: a
    /// `StandaloneEc2Instance` lane has no distribution decision to make, ever,
    /// unless the topology itself becomes a pool. Named distinctly so the two
    /// honestly-different shapes (a live platform gap vs. a structural
    /// non-applicability) are never conflated in a report.
    NotApplicableSingleInstance,
}

impl StrategyWiring {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Effective => "effective",
            Self::IgnoredOnManagedNg => "ignored-on-managed-ng",
            Self::NotApplicableSingleInstance => "not-applicable-single-instance",
        }
    }
}

/// Tier-honest shipped state of a molding / axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Maturity {
    /// No shipped realizer — a claimed shape reality does not carve. MUST carry a
    /// `pending` note (models-stay-current).
    Gap,
    /// The realizer is designed / partially shipped (e.g. the placement destination
    /// is per-replica multi-AZ but the shipped pool defaults single-AZ interim).
    Design,
    /// Fully shipped: the node-group + the spot pool + the discovery tags all ship.
    Shipped,
}

impl Maturity {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gap => "gap",
            Self::Design => "design",
            Self::Shipped => "shipped",
        }
    }
}

/// **The inline COST justification that travels with a resolved arch choice.**
/// Every conflict resolves by cost (A6); where the answer is counter-intuitive
/// the rationale is LOUD and names the number + the auto-flip trigger, so a
/// surprising choice is self-explaining and never re-litigated later.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostRationale {
    /// The arch this rationale explains (the cost-optimal resolution for THIS pool).
    pub chosen_arch: ResolvedArch,
    /// The number + the why (e.g. "builder=arm: −37%/build-hr + ~18% faster").
    pub rationale: &'static str,
    /// **LOUD where the choice is COUNTER-INTUITIVE** (arm LOSING; x86 chosen
    /// while we push arm). `None` when the choice is intuitive (arm winning the
    /// builder). When `Some`, it MUST name the % AND that arm loses — the
    /// loud-where-arm-loses gate (`is_valid`). This is where a weird choice SAYS
    /// SO plainly instead of silently defaulting.
    pub counterintuitive: Option<&'static str>,
    /// The cost condition that FLIPS this answer — so it is self-adjusting, and the
    /// flip is documented rather than a silent hardcode.
    pub auto_flip_when: &'static str,
}

/// A representative live-price SIGNAL under which a molding's arch is the cost
/// resolution — carried so a test can PROVE the arch is a genuine
/// [`resolve_arch`] output (not a hardcode) and that flipping the signal flips the
/// arch. Distinct per use-case: big-compute (builder), large-spot (floor),
/// tiny-burstable (eyes) price the two arches differently.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostWitness {
    pub use_case: UseCase,
    pub signal: ArchCostSignal,
}

/// One typed point in `Arch × SpotStrategy × LadderMode × PerfClass × Placement ×
/// Interruption`, under the never-on-demand hard law (capacity is NOT a field) +
/// the breathability dual-purpose (both `cost_effect` AND `resiliency_effect`).
#[derive(Clone, Copy, Debug)]
pub struct AuctionSpread {
    /// The use-case this molding arms.
    pub use_case: UseCase,

    // ── the six permutation axes ──────────────────────────────────────────────
    /// Arch axis — cost-optimized by default (never a static per-pool hardcode).
    pub arch: ArchSelection,
    /// REQUIRED iff `arch.is_pinned()` — a pin must justify itself.
    pub arch_pin_reason: Option<ArchPinReason>,
    /// The AUTOBUMP multi-arch-image prerequisite for `CostOptimized` (the thing
    /// that makes arch selection free). A `CostOptimized` spread with a single-arch
    /// image is invalid.
    pub image_multi_arch: bool,
    /// Spot allocation strategy axis.
    pub spot_strategy: SpotStrategy,
    /// Auction-ladder axis (evolving-degrade / flat-pool).
    pub ladder: LadderMode,
    /// Perf-class axis (cost-floor / time-floor) — spot-only.
    pub perf_class: PerfClass,
    /// The workload's storage binding — DERIVES the placement default.
    pub storage_binding: StorageBinding,
    /// Placement axis (single-az / multi-az) — must be single-az when the binding
    /// is single-instance-EBS.
    pub placement: Placement,
    /// Interruption axis (retirada drain / retry-on-reclaim).
    pub interruption: Interruption,

    // ── dual-purpose (A4) — both nonempty ─────────────────────────────────────
    /// The cost control this whole spread IS.
    pub cost_effect: &'static str,
    /// The availability/resiliency this SAME spread maximizes — together with cost,
    /// not traded.
    pub resiliency_effect: &'static str,

    // ── cost reasoning (A6) ───────────────────────────────────────────────────
    /// The inline, loud-where-abnormal justification for this pool's arch choice.
    pub cost_rationale: CostRationale,

    // ── lane + the strategy-wiring gap ────────────────────────────────────────
    /// The AWS lane this molding realizes on.
    pub lane: Lane,
    /// Whether the spot strategy is effective or dropped on this lane (the gap).
    pub strategy_wiring: StrategyWiring,

    // ── tier-honesty ──────────────────────────────────────────────────────────
    pub maturity: Maturity,
    /// A `pending:` note REQUIRED when `maturity == Gap` (or when a shipped molding
    /// carries a named interim, e.g. single-AZ-today vs multi-AZ-destination).
    pub pending: Option<&'static str>,
    /// The doctrine / shipped-realizer this molding composes BY REFERENCE.
    pub doctrine_ref: &'static str,
    /// A tier-honest one-line note.
    pub note: &'static str,
}

impl AuctionSpread {
    /// **THE validity gate — the seven-clause discharge as a value check.** Returns
    /// the list of violated rule-names (empty = valid). The matrix asserts every
    /// molding is valid and that specific adversarial spreads are rejected.
    #[must_use]
    pub fn violations(&self) -> Vec<&'static str> {
        let mut v = Vec::new();

        // A2 — a pin must justify itself; cost-optimized must NOT carry a pin
        // reason and MUST have a multi-arch image (the free-arch prerequisite).
        if self.arch.is_pinned() && self.arch_pin_reason.is_none() {
            v.push("auction-arch-native-cost-optimized: a pinned arch must name an ArchPinReason");
        }
        if self.arch == ArchSelection::CostOptimized {
            if self.arch_pin_reason.is_some() {
                v.push("auction-arch-native-cost-optimized: cost-optimized must not carry a pin reason");
            }
            if !self.image_multi_arch {
                v.push("auction-arch-native-cost-optimized: cost-optimized requires a multi-arch image (AUTOBUMP arm64+amd64)");
            }
        }

        // A4 — dual-purpose: both effects named.
        if self.cost_effect.is_empty() {
            v.push("auction-dual-purpose: cost_effect must be named");
        }
        if self.resiliency_effect.is_empty() {
            v.push("auction-dual-purpose: resiliency_effect must be named");
        }

        // A5 — placement-safe: a single-instance-EBS pod MUST be single-AZ.
        if self.storage_binding == StorageBinding::SingleInstanceEbs
            && self.placement != Placement::SingleAz
        {
            v.push("auction-placement-safe: single-instance-EBS requires single-az (else the volume is stranded on a reclaim)");
        }

        // A6 — cost-justified-where-abnormal: a counter-intuitive choice must be
        // LOUD (name a % and that arm loses / is pricier); AND an x86 choice while
        // we push arm IS counter-intuitive, so it MUST be flagged.
        if let Some(ci) = self.cost_rationale.counterintuitive {
            let has_pct = ci.contains('%');
            let says_loses = ci.contains("loses")
                || ci.contains("LOSES")
                || ci.contains("pricier")
                || ci.contains("PRICIER");
            if !(has_pct && says_loses) {
                v.push("auction-cost-justified-where-abnormal: a counter-intuitive note must name the % AND that arm loses");
            }
        } else if self.cost_rationale.chosen_arch == ResolvedArch::Amd64 {
            // x86 chosen while the fleet pushes arm — the surprising answer MUST
            // say so plainly (the loud-where-arm-loses gate).
            v.push("auction-cost-justified-where-abnormal: an x86 (amd64) cost choice must carry a LOUD counter-intuitive note (arm loses here)");
        }
        if self.cost_rationale.auto_flip_when.is_empty() {
            v.push("auction-cost-justified-where-abnormal: every cost choice must name its auto-flip trigger (self-adjusting, not a hardcode)");
        }

        // Strategy-wiring honesty (the operator gap): managed-NG + a non-capacity-
        // optimized strategy MUST be marked IgnoredOnManagedNg; the ASG lane is
        // always Effective; a lone standalone instance has no axis to bind at
        // all, so it must always be marked NotApplicableSingleInstance — never
        // Effective (nothing to wire) and never IgnoredOnManagedNg (that names a
        // different, GAP shape — a computed-then-dropped preference — which does
        // not apply here).
        match self.lane {
            Lane::EksManagedNodeGroup => {
                if self.spot_strategy != SpotStrategy::CapacityOptimized
                    && self.strategy_wiring != StrategyWiring::IgnoredOnManagedNg
                {
                    v.push("auction-strategy-wiring: a non-capacity-optimized strategy on the managed-NG lane must be marked ignored-on-managed-ng (the gap)");
                }
            }
            Lane::MixedInstancesAsg => {
                if self.strategy_wiring != StrategyWiring::Effective {
                    v.push("auction-strategy-wiring: the ASG lane wires the strategy — must be effective");
                }
            }
            Lane::StandaloneEc2Instance => {
                if self.strategy_wiring != StrategyWiring::NotApplicableSingleInstance {
                    v.push("auction-strategy-wiring: a standalone single-instance lane has no allocation-strategy axis to bind — strategy_wiring must be not-applicable-single-instance");
                }
            }
        }

        // A7 — models-stay-current: a Gap molding must carry a pending note.
        if self.maturity == Maturity::Gap && self.pending.is_none() {
            v.push("auction-models-stay-current: a Gap molding must carry a pending note");
        }

        v
    }

    /// True iff the spread discharges every clause (no violations).
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.violations().is_empty()
    }

    /// The resolved arch under a given live-price signal — the cost axis in action.
    #[must_use]
    pub fn resolved_arch(&self, cost: ArchCostSignal) -> ResolvedArch {
        resolve_arch(self.arch, cost)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// THE MOLDINGS — a spread per use-case (the molding DEFAULTS)
// ─────────────────────────────────────────────────────────────────────────────

/// **SaaS-steady** — the CamelotNodeGroup posture. Cost-optimized arch resolves to
/// **x86 at the floor** (the LOUD counter-intuitive case: current-gen Graviton
/// large-spot is +19 % pricier than 2019-gen m5a x86 NOW — the shipped
/// CamelotNodeGroup DEFAULTS are already `m5/m5a/m6i/m6a`, x86). Steady:
/// capacity-optimized deepest pool, evolving-degrade, cost-floor, per-replica-EBS
/// stateful tiers → multi-AZ resilient (destination; the shipped pool defaults
/// single-AZ interim), retirada graceful drain.
pub const SAAS_STEADY: AuctionSpread = AuctionSpread {
    use_case: UseCase::SaasSteady,
    arch: ArchSelection::CostOptimized,
    arch_pin_reason: None,
    image_multi_arch: true,
    spot_strategy: SpotStrategy::CapacityOptimized,
    ladder: LadderMode::EvolvingDegrade,
    perf_class: PerfClass::CostFloor,
    storage_binding: StorageBinding::PerReplicaEbs,
    placement: Placement::MultiAz,
    interruption: Interruption::RetiradaGracefulDrain,
    cost_effect: "100% spot on the cheapest deep pool (m5a x86 large-spot) + cost-floor small nodes + scale-down-idle to the HA floor",
    resiliency_effect: "capacity-optimized deepest pool = rare reclaims; multi-AZ per-replica stateful = survives an AZ loss; retirada drains before reclaim; HA floor 2",
    cost_rationale: CostRationale {
        chosen_arch: ResolvedArch::Amd64,
        rationale: "floor=x86: 2019-gen m5a.large spot is currently the cheapest deep general-purpose pool for the steady floor (the shipped CamelotNodeGroup DEFAULTS are m5/m5a/m6i/m6a)",
        counterintuitive: Some(
            "ARM LOSES HERE (floor): current-gen Graviton m7g/m8g large-spot is +19% PRICIER than the cheap 2019-gen m5a x86 large-spot as of 2026-07; x86 chosen for COST despite the arm push; the multi-arch image makes it free to auto-flip to arm the instant Graviton crosses below m5a",
        ),
        auto_flip_when: "Graviton (m7g/m8g) large-spot effective-$ drops below m5a x86 large-spot effective-$",
    },
    lane: Lane::EksManagedNodeGroup,
    strategy_wiring: StrategyWiring::Effective, // capacity-optimized aligns with EKS's internal default
    maturity: Maturity::Design,
    pending: Some(
        "shipped CamelotNodeGroup DEFAULTS single-AZ (conservative, lone-volume-safe); multi-AZ is the destination for the multi-replica per-AZ stateful tiers (mysql primary+replica floor 2, quorum floor 3) — gated on per-AZ volume topology",
    ),
    doctrine_ref: "pangea-architectures::CamelotNodeGroup + breathe-catalog::preset::CAMELOT",
    note: "the steady SaaS floor; x86 is the current cost answer (shipped), multi-AZ per-replica the resilience destination",
};

/// **Build-burst** — the CamelotBuilderNodeGroup + super-cache-ci posture.
/// Cost-optimized arch resolves to **arm at the builder** (the expected win:
/// −37 %/build-hr + ~18 % faster wall-clock, proven by the arm cross-compile eval;
/// the whole fleet is CGO=0 pure-Go so arm is native). Bursty: time-floor biggest
/// latest-gen, evolving-degrade, floor-0 scale-to-zero, stateless (cache-backed) →
/// multi-AZ deep pools, retry-on-reclaim (no drain agent needed).
pub const BUILD_BURST: AuctionSpread = AuctionSpread {
    use_case: UseCase::BuildBurst,
    arch: ArchSelection::CostOptimized,
    arch_pin_reason: None,
    image_multi_arch: true,
    spot_strategy: SpotStrategy::CapacityOptimized, // standard depth; evolves to price-capacity at :deep/:deepest
    ladder: LadderMode::EvolvingDegrade,
    perf_class: PerfClass::TimeFloor,
    storage_binding: StorageBinding::Stateless,
    placement: Placement::MultiAz,
    interruption: Interruption::RetryOnReclaim,
    cost_effect: "100% spot, floor-0 scale-to-zero (near-free at idle — rent the monster only for the build minutes), cost-optimal arm compute",
    resiliency_effect: "evolving-degrade always places (never on-demand); multi-AZ stateless = deep independent pools; retry-on-reclaim survives a mid-build reclaim (idempotent + cache-backed)",
    cost_rationale: CostRationale {
        chosen_arch: ResolvedArch::Arm64,
        rationale: "builder=arm: −37%/build-hr + ~18% faster wall-clock (proven by the arm cross-compile eval); the akeyless fleet builds CGO=0 pure-Go so arm is native — no emulation, no cgo blocker",
        counterintuitive: None, // arm winning the builder is the EXPECTED answer
        auto_flip_when: "x86 large-spot effective-$/build drops below arm effective-$/build (the multi-arch image makes the flip free)",
    },
    lane: Lane::MixedInstancesAsg, // Tier A default (the cold-aarch64 unblock)
    strategy_wiring: StrategyWiring::Effective, // the ASG lane wires spot_allocation_strategy
    maturity: Maturity::Shipped,
    pending: Some(
        "the Tier-B EKS-managed-NG builder lane (in-cluster sui) DROPS the strategy at :deep/:deepest (StrategyWiring::IgnoredOnManagedNg) — EKS managed node groups do not expose SpotAllocationStrategy; the ASG lane (default here) wires it",
    ),
    doctrine_ref: "pangea-architectures::CamelotBuilderNodeGroup + breathe-catalog::builder::SUPER_CACHE_CI_BUILD",
    note: "the bursty builder; arm is the current cost answer (proven), time-floor for best build times",
};

/// **Eyes-tiny** — the observability tap. Cost-optimized arch resolves to **arm at
/// tiny sizes** (t4g burstable spot < t3 x86 at burstable sizes — arm winning
/// small is the norm; grafana/victoria/vector all publish arm64). Tiny: flat-pool
/// (one small size — a preference order buys nothing), cost-floor, single-instance-
/// EBS eyes store → single-AZ (the genuinely-correct lone-volume case; the
/// eyes-2a-vs-floor-2b lesson), retirada graceful drain.
pub const EYES_TINY: AuctionSpread = AuctionSpread {
    use_case: UseCase::EyesTiny,
    arch: ArchSelection::CostOptimized,
    arch_pin_reason: None,
    image_multi_arch: true,
    spot_strategy: SpotStrategy::CapacityOptimized,
    ladder: LadderMode::FlatPool,
    perf_class: PerfClass::CostFloor,
    storage_binding: StorageBinding::SingleInstanceEbs,
    placement: Placement::SingleAz,
    interruption: Interruption::RetiradaGracefulDrain,
    cost_effect: "tiny 100% spot footprint (t4g burstable) — near-free; single small pool, cost-floor",
    resiliency_effect: "single-AZ keeps the lone eyes volume on a landing node (never stranded); retirada drains before reclaim; observation continuity",
    cost_rationale: CostRationale {
        chosen_arch: ResolvedArch::Arm64,
        rationale: "eyes=arm: t4g burstable spot is cheaper than t3 x86 at tiny sizes; the observability stack (grafana/victoria/vector) all publish arm64, so the flip is free",
        counterintuitive: None, // arm winning the tiny burstable pool is expected
        auto_flip_when: "t3 x86 burstable spot drops below t4g arm at the eyes size",
    },
    lane: Lane::EksManagedNodeGroup,
    strategy_wiring: StrategyWiring::Effective,
    maturity: Maturity::Shipped,
    pending: None,
    doctrine_ref: "pangea-architectures::AzTopology (single-AZ stateful) + the tendril observability tap",
    note: "the tiny eyes; arm is the current cost answer (tiny burstable), single-AZ for the lone eyes volume",
};

/// The molding catalog — one spread per use-case (CATALOG REFLECTION).
pub const MOLDINGS: &[AuctionSpread] = &[SAAS_STEADY, BUILD_BURST, EYES_TINY];

/// Representative live-price witnesses proving each molding's arch is a genuine
/// cost resolution (not a hardcode). Effective cost per unit of work, per arch;
/// the resolver lands on the cheaper. Flip the signal → the arch flips (proven by
/// the matrix). Numbers are illustrative of the CURRENT market shape (big-compute
/// arm-favoured, large-spot x86-favoured, tiny-burstable arm-favoured), not live
/// quotes.
pub const COST_WITNESSES: &[CostWitness] = &[
    CostWitness {
        use_case: UseCase::SaasSteady,
        // large-spot: Graviton +19% pricier → x86 wins
        signal: ArchCostSignal { arm64_effective_cost: 1.19, amd64_effective_cost: 1.00 },
    },
    CostWitness {
        use_case: UseCase::BuildBurst,
        // big-compute per-build: arm −37% + faster → arm wins
        signal: ArchCostSignal { arm64_effective_cost: 0.63, amd64_effective_cost: 1.00 },
    },
    CostWitness {
        use_case: UseCase::EyesTiny,
        // tiny burstable: t4g < t3 → arm wins
        signal: ArchCostSignal { arm64_effective_cost: 0.85, amd64_effective_cost: 1.00 },
    },
];

/// Look up a molding by use-case.
#[must_use]
pub fn molding(use_case: UseCase) -> Option<&'static AuctionSpread> {
    MOLDINGS.iter().find(|m| m.use_case == use_case)
}

/// The cost witness for a use-case.
#[must_use]
pub fn cost_witness(use_case: UseCase) -> Option<&'static CostWitness> {
    COST_WITNESSES.iter().find(|w| w.use_case == use_case)
}

#[cfg(test)]
mod tests {
    use super::{
        cost_witness, AuctionSpread, CostRationale, Lane, StrategyWiring, UseCase, BUILD_BURST,
        MOLDINGS, SAAS_STEADY,
    };
    use crate::axis::{
        ArchPinReason, ArchSelection, Placement, ResolvedArch, SpotStrategy, StorageBinding,
    };

    #[test]
    fn every_molding_is_valid() {
        for m in MOLDINGS {
            let v = m.violations();
            assert!(v.is_empty(), "{} molding is invalid: {:?}", m.use_case.as_str(), v);
        }
    }

    #[test]
    fn moldings_are_a_bijection_with_use_cases() {
        assert_eq!(MOLDINGS.len(), UseCase::ALL.len());
        for uc in UseCase::ALL {
            assert_eq!(MOLDINGS.iter().filter(|m| m.use_case == uc).count(), 1, "one molding per use-case");
        }
    }

    #[test]
    fn each_molding_arch_is_a_genuine_cost_resolution() {
        use crate::axis::ArchCostSignal;
        // THE not-a-hardcode proof: the molding's stated arch equals resolve_arch
        // under its representative signal, AND flipping the signal flips the arch.
        for m in MOLDINGS {
            let w = cost_witness(m.use_case).expect("witness");
            let resolved = m.resolved_arch(w.signal);
            assert_eq!(
                resolved, m.cost_rationale.chosen_arch,
                "{}: stated arch must equal the cost resolution",
                m.use_case.as_str()
            );
            // flip the signal → the arch flips (cost-driven, self-adjusting).
            let flipped = ArchCostSignal {
                arm64_effective_cost: w.signal.amd64_effective_cost,
                amd64_effective_cost: w.signal.arm64_effective_cost,
            };
            let after = m.resolved_arch(flipped);
            assert_ne!(
                after, resolved,
                "{}: flipping the price signal must flip the arch (not a hardcode)",
                m.use_case.as_str()
            );
        }
    }

    #[test]
    fn saas_floor_is_x86_and_loudly_says_arm_loses() {
        // The operator's exact example: the floor picks x86 and SAYS SO with the %.
        assert_eq!(SAAS_STEADY.cost_rationale.chosen_arch, ResolvedArch::Amd64);
        let ci = SAAS_STEADY.cost_rationale.counterintuitive.expect("x86 floor must be flagged loud");
        assert!(ci.contains("19%") || ci.contains("+19%"), "must name the +19% Graviton premium");
        assert!(ci.contains("LOSES") || ci.contains("loses"), "must plainly say arm loses");
    }

    #[test]
    fn builder_is_arm_and_is_not_flagged_counterintuitive() {
        // arm winning the builder is the expected answer — no loud flag.
        assert_eq!(BUILD_BURST.cost_rationale.chosen_arch, ResolvedArch::Arm64);
        assert!(BUILD_BURST.cost_rationale.counterintuitive.is_none());
    }

    #[test]
    fn an_x86_choice_without_a_loud_note_is_rejected() {
        // Adversarial: an amd64 cost choice that does NOT carry the loud note is
        // invalid (the loud-where-arm-loses gate has teeth).
        let mut bad = SAAS_STEADY;
        bad.cost_rationale = CostRationale {
            chosen_arch: ResolvedArch::Amd64,
            rationale: "x86 because reasons",
            counterintuitive: None, // <- silently defaults to x86: FORBIDDEN
            auto_flip_when: "someday",
        };
        assert!(!bad.is_valid(), "a silent x86 choice must be rejected");
    }

    #[test]
    fn single_instance_ebs_multi_az_is_rejected() {
        // Adversarial: the stranded-volume class is refused.
        let mut bad = SAAS_STEADY;
        bad.storage_binding = StorageBinding::SingleInstanceEbs;
        bad.placement = Placement::MultiAz;
        assert!(!bad.is_valid(), "single-instance-EBS + multi-AZ must be rejected (stranded volume)");
    }

    #[test]
    fn cost_optimized_without_a_multi_arch_image_is_rejected() {
        let mut bad = BUILD_BURST;
        bad.image_multi_arch = false;
        assert!(!bad.is_valid(), "cost-optimized needs a multi-arch image");
    }

    #[test]
    fn a_pin_needs_a_reason_and_cost_optimized_needs_none() {
        let mut pinned = BUILD_BURST;
        pinned.arch = ArchSelection::PinnedArm64;
        pinned.arch_pin_reason = None;
        assert!(!pinned.is_valid(), "a pin must name a reason");
        pinned.arch_pin_reason = Some(ArchPinReason::CgoFipsSingleArch);
        assert!(pinned.is_valid(), "a pin with a reason is valid");
    }

    #[test]
    fn standalone_instance_lane_has_no_strategy_axis_and_is_never_effective_or_ignored() {
        // A lone hand-launched EC2 instance (Camelot's bootstrap/control-plane
        // node, or any self-managed-k3s single instance) has no ASG/Fleet
        // distribution decision to make — the strategy axis has no field to bind.
        // Claiming Effective (nothing wired it) OR IgnoredOnManagedNg (that names
        // a DIFFERENT shape — a computed-then-dropped preference) must both be
        // rejected; only NotApplicableSingleInstance is honest.
        let claims_effective = AuctionSpread {
            lane: Lane::StandaloneEc2Instance,
            strategy_wiring: StrategyWiring::Effective,
            ..BUILD_BURST
        };
        assert!(!claims_effective.is_valid(), "a standalone instance has no strategy axis to be Effective");

        let claims_ignored = AuctionSpread {
            lane: Lane::StandaloneEc2Instance,
            strategy_wiring: StrategyWiring::IgnoredOnManagedNg,
            ..BUILD_BURST
        };
        assert!(!claims_ignored.is_valid(), "a standalone instance is not a managed-NG — the gap shape does not apply");

        let honest = AuctionSpread {
            lane: Lane::StandaloneEc2Instance,
            strategy_wiring: StrategyWiring::NotApplicableSingleInstance,
            ..BUILD_BURST
        };
        assert!(honest.is_valid(), "marking the lane not-applicable-single-instance is honest + valid");
    }

    #[test]
    fn lane_labels_are_stable_and_unique() {
        fn uniq(v: &[&str]) -> bool {
            let mut s: Vec<&str> = v.to_vec();
            s.sort_unstable();
            s.dedup();
            s.len() == v.len()
        }
        assert!(uniq(&Lane::ALL.map(Lane::as_str)));
    }

    #[test]
    fn the_managed_ng_strategy_gap_is_ci_visible() {
        // The operator-flagged gap: a price-capacity-optimized strategy on the
        // EKS-managed-NG lane MUST be marked ignored (the strategy is dropped);
        // claiming it Effective is invalid.
        let mut gap = AuctionSpread {
            lane: Lane::EksManagedNodeGroup,
            spot_strategy: SpotStrategy::PriceCapacityOptimized,
            strategy_wiring: StrategyWiring::Effective, // <- dishonest
            ..BUILD_BURST
        };
        assert!(!gap.is_valid(), "a dropped strategy claimed effective is invalid");
        gap.strategy_wiring = StrategyWiring::IgnoredOnManagedNg;
        assert!(gap.is_valid(), "marking the gap ignored-on-managed-ng is honest + valid");
    }
}
