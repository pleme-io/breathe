//! The SIX permutation AXES of the compute/auction spread — each a typed enum
//! with a stable kebab-case label, a canonical `ALL`, and a documented default.
//!
//! A spread is one point in `Arch × SpotStrategy × LadderMode × PerfClass ×
//! Placement × Interruption`. The **capacity** axis is deliberately ABSENT: 100%
//! spot is an INVARIANT, not a knob — there is no on-demand arm to permute over
//! (see [`crate::invariant::AuctionClause::NeverOnDemand`]). Removing the axis is
//! how the never-on-demand law becomes structural rather than defaulted-off.
//!
//! ── /algorithmic-prowess-seal (best-fit, NO ML) ──
//! Every axis is a small closed enum (an anti-posture value simply does not exist
//! in the type — e.g. `SpotStrategy` has no `lowest-price` arm, `PerfClass` has no
//! `guaranteed-wake`/`dedicated` arm), so the illegal choice is unrepresentable in
//! the spread. The arch axis is a COST optimization ([`resolve_arch`]) — a pure
//! cheaper-of comparison over a live price signal, the smallest-sufficient rung
//! (no ML): brilliance is in the framing (arch made a free cost lever by the
//! multi-arch image), not in a model.

use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// AXIS 1 — ARCH (a COST-OPTIMIZED lever, not a static per-pool default)
// ─────────────────────────────────────────────────────────────────────────────

/// The ARCH selection axis — **cost-optimized, not a hardcode**.
///
/// Because AUTOBUMP emits MULTI-ARCH images (`dockerImage:arm64` AND `:amd64`), a
/// workload runs on whatever arch the auction lands on, so **arch is a free cost
/// lever**: the evolving-degrade ladder evaluates arm-vs-x86 spot pricing and the
/// pool lands on the cheapest-deepest arch that meets its needs. It RESOLVES today
/// to builder ≈ arm (faster + −37 %/build-hr per the arm eval) and floor ≈ x86
/// (m5a 2019-gen large-spot is currently −19 % vs current-gen Graviton large-spot)
/// — but that is the CURRENT cost answer, **not** a hardcode: it AUTO-ADJUSTS when
/// Graviton pricing crosses (same [`resolve_arch`], new [`ArchCostSignal`], no
/// re-decision). See [`crate::spread::CostRationale`] for the loud, inline
/// justification that travels with every resolved choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArchSelection {
    /// THE DEFAULT. Evaluate arm-vs-x86 effective spot cost and land on the
    /// cheaper. REQUIRES a multi-arch image (AUTOBUMP emits both) so arch
    /// selection is genuinely free + safe. A `CostOptimized` spread whose image is
    /// single-arch is invalid (the multi-arch prerequisite, `is_valid`).
    CostOptimized,
    /// EXCEPTION — pin arm64. Legitimate only for a genuinely single-arch image
    /// (e.g. a cgo/FIPS-BoringCrypto build that ships one arch) — must carry an
    /// [`ArchPinReason`].
    PinnedArm64,
    /// EXCEPTION — pin amd64. Same rule: a single-arch image + a reason.
    PinnedAmd64,
}

impl ArchSelection {
    /// The default arch selection — cost-optimized (never a static per-pool arch).
    #[must_use]
    pub const fn default_selection() -> Self {
        Self::CostOptimized
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CostOptimized => "cost-optimized",
            Self::PinnedArm64 => "pinned-arm64",
            Self::PinnedAmd64 => "pinned-amd64",
        }
    }

    /// Is this a PIN (an exception that must justify itself with a reason)?
    #[must_use]
    pub fn is_pinned(self) -> bool {
        matches!(self, Self::PinnedArm64 | Self::PinnedAmd64)
    }

    pub const ALL: [ArchSelection; 3] =
        [Self::CostOptimized, Self::PinnedArm64, Self::PinnedAmd64];
}

/// The concrete arch a cost evaluation resolves a pool to. An ASG launch template
/// / EKS managed-node-group `ami_type` pins ONE arch, so a POOL is single-arch at
/// the AWS layer; `CostOptimized` picks WHICH one. Spanning BOTH arches is a
/// FLEET composition (two single-arch pools + cost-aware placement — the
/// multi-arch image makes it safe), not a single pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResolvedArch {
    Arm64,
    Amd64,
}

impl ResolvedArch {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Arm64 => "arm64",
            Self::Amd64 => "amd64",
        }
    }

    /// The nix system triple for this arch (the build-lane the multi-arch image
    /// is emitted from — no cross-emulation, ever).
    #[must_use]
    pub fn nix_system(self) -> &'static str {
        match self {
            Self::Arm64 => "aarch64-linux",
            Self::Amd64 => "x86_64-linux",
        }
    }
}

/// The reason a spread PINS an arch (the [`ArchSelection::PinnedArm64`] /
/// `PinnedAmd64` exception must name one). The two named paths are exactly the
/// arm-eval's latent risks + the plain single-arch-image case.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArchPinReason {
    /// A real cgo/FIPS posture (`GOEXPERIMENT=boringcrypto` needs `CGO_ENABLED=1`,
    /// or a cgo pkcs11/HSM path) that ships a single-arch image — the arm-eval's
    /// named latent risk. Today camelot is CGO=0/ldflag-FIPS so this does not bite.
    CgoFipsSingleArch,
    /// The upstream image is genuinely single-arch (a third-party binary published
    /// for one arch only) — no multi-arch image exists to make arch free.
    UpstreamSingleArchImage,
}

impl ArchPinReason {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CgoFipsSingleArch => "cgo-fips-single-arch",
            Self::UpstreamSingleArchImage => "upstream-single-arch-image",
        }
    }
}

/// The COST signal the arch axis optimizes over — the EFFECTIVE cost per unit of
/// useful work, per arch, folding spot price AND per-arch throughput (so a
/// perf-sensitive use-case like the builder can prefer arm even at a higher $/hr
/// because it finishes faster: −37 %/build-hr). Live spot pricing feeds this; here
/// it is a declared input the resolver is pure over.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ArchCostSignal {
    /// Effective cost of a unit of useful work on arm64 (spot $/hr ÷ arm
    /// throughput). Lower = cheaper.
    pub arm64_effective_cost: f64,
    /// Effective cost of the same unit of work on amd64.
    pub amd64_effective_cost: f64,
}

impl ArchCostSignal {
    /// The margin by which the cheaper arch wins, as a signed fraction relative to
    /// the pricier one (`+0.19` = the loser is 19 % pricier). Positive means arm
    /// is cheaper; negative means x86 is cheaper. Used to make a counter-intuitive
    /// choice LOUD with its number.
    #[must_use]
    pub fn arm_advantage(self) -> f64 {
        if self.arm64_effective_cost <= self.amd64_effective_cost {
            // arm cheaper: how much pricier x86 is, relative to arm
            (self.amd64_effective_cost - self.arm64_effective_cost) / self.arm64_effective_cost
        } else {
            // x86 cheaper: negative — how much pricier arm is, relative to x86
            (self.amd64_effective_cost - self.arm64_effective_cost) / self.amd64_effective_cost
        }
    }
}

/// THE ARCH RESOLVER — pure, cost-driven, self-adjusting, NO ML. `CostOptimized`
/// lands on the cheaper effective cost; a tie goes to arm (the aspirational push).
/// A pin forces its arch. Because it is a pure fold over the live signal, the SAME
/// call yields the OTHER arch the moment pricing crosses — the auto-adjust is
/// mechanical, never a re-decision (proven by the `arch_auto_flips_*` matrix test).
#[must_use]
pub fn resolve_arch(selection: ArchSelection, cost: ArchCostSignal) -> ResolvedArch {
    match selection {
        ArchSelection::PinnedArm64 => ResolvedArch::Arm64,
        ArchSelection::PinnedAmd64 => ResolvedArch::Amd64,
        ArchSelection::CostOptimized => {
            if cost.arm64_effective_cost <= cost.amd64_effective_cost {
                ResolvedArch::Arm64
            } else {
                ResolvedArch::Amd64
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AXIS 2 — SPOT STRATEGY (the AWS allocation strategy, our-posture subset)
// ─────────────────────────────────────────────────────────────────────────────

/// The spot ALLOCATION strategy axis — mirrors `Pangea::Spot::Allocation`, but
/// ONLY the three our posture permits. `lowest-price` (high interruption on
/// shallow pools) and `capacity-optimized-prioritized` are NOT arms of this enum:
/// an anti-posture strategy is unrepresentable in the spread.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SpotStrategy {
    /// THE DEFAULT — launch from the DEEPEST pool (fewest reclaims). The steady
    /// SaaS + the standard-depth builder posture.
    CapacityOptimized,
    /// Balance price AND capacity — fill the cheapest deep pool first. The
    /// evolve-deeper rung (`spot_depth: :deep/:deepest`) when a pool shows churn.
    PriceCapacityOptimized,
    /// Spread across every pool — for genuinely SHALLOW pools (huge instances) so
    /// no one pool's reclaim wave takes the fleet.
    Diversified,
}

impl SpotStrategy {
    #[must_use]
    pub const fn default_strategy() -> Self {
        Self::CapacityOptimized
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CapacityOptimized => "capacity-optimized",
            Self::PriceCapacityOptimized => "price-capacity-optimized",
            Self::Diversified => "diversified",
        }
    }

    /// The AWS hyphen form the Terraform provider + EC2 Fleet API expect (the same
    /// value `Pangea::Spot::Allocation.to_aws` emits) — the lock's label IS the
    /// AWS form, so the Rust border and the Ruby wire can never drift.
    #[must_use]
    pub fn to_aws(self) -> &'static str {
        self.as_str()
    }

    pub const ALL: [SpotStrategy; 3] =
        [Self::CapacityOptimized, Self::PriceCapacityOptimized, Self::Diversified];
}

// ─────────────────────────────────────────────────────────────────────────────
// AXIS 3 — AUCTION LADDER (the evolving-degrade preference order)
// ─────────────────────────────────────────────────────────────────────────────

/// The auction-LADDER axis — how the pool degrades under scarcity. The concrete
/// per-arch tier lists + the total-order proof live in
/// `breathe-catalog::builder::{AMD64,ARM64}_DEGRADE_LADDER`; this axis names WHICH
/// ladder shape a spread uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LadderMode {
    /// THE DEFAULT — a total-order preference ladder (fastest latest-gen → prior-gen
    /// + memory-heavy → broad diversified floor). On scarcity the auction degrades
    /// DOWN the ladder (never to on-demand); the floor tier ALWAYS places. This is
    /// the `breathe-catalog::builder` DegradeTier contract.
    EvolvingDegrade,
    /// A single flat diversified pool — no tiered preference. For a TINY pool (the
    /// eyes) where a preference order buys nothing (one small size, the floor IS
    /// the pool). Still 100 % spot, still diversified for depth.
    FlatPool,
}

impl LadderMode {
    #[must_use]
    pub const fn default_mode() -> Self {
        Self::EvolvingDegrade
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EvolvingDegrade => "evolving-degrade",
            Self::FlatPool => "flat-pool",
        }
    }

    pub const ALL: [LadderMode; 2] = [Self::EvolvingDegrade, Self::FlatPool];
}

// ─────────────────────────────────────────────────────────────────────────────
// AXIS 4 — PERF CLASS (the builder_perf objective — SPOT-ONLY)
// ─────────────────────────────────────────────────────────────────────────────

/// The perf-class axis — WHAT the pool optimizes FOR. The Rust mirror of
/// `CamelotBuilderNodeGroup::BUILDER_PERF` and `breathe-catalog::builder
/// ::BuilderObjective`. **Exactly two spot-only arms.** The old `guaranteed-wake`
/// / `dedicated` classes are ABSENT — they introduced on-demand, which the
/// never-on-demand hard law forbids (see [`REMOVED_ON_DEMAND_PERF_CLASSES`] +
/// `reject_on_demand!` at the Ruby boundary). A perf class that would need
/// on-demand is unrepresentable here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PerfClass {
    /// Cheapest small deep node; the scale-to-zero floor keeps it near-free. The
    /// DEFAULT when build LATENCY is not the binding constraint (steady SaaS, eyes).
    CostFloor,
    /// The FASTEST node spot can give — minimum wall-clock (biggest latest-gen
    /// 24xl/48xl, big RAMDISK, max-parallel). The build-burst default.
    TimeFloor,
}

impl PerfClass {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        // matches the Ruby `builder_perf` discovery-tag form + BuilderObjective::as_str
        match self {
            Self::CostFloor => "cost-floor",
            Self::TimeFloor => "time-floor",
        }
    }

    pub const ALL: [PerfClass; 2] = [Self::CostFloor, Self::TimeFloor];
}

/// The perf classes that were REMOVED because they introduced on-demand — named
/// here so the never-on-demand law is documented at the exact point the two
/// surviving spot-only arms are declared. `reject_on_demand!` refuses the old
/// `perf_class` key with a migration hint; this const is the Rust-side receipt
/// that the removal is intentional, not an omission (asserted by the matrix).
pub const REMOVED_ON_DEMAND_PERF_CLASSES: [&str; 2] = ["guaranteed-wake", "dedicated"];

// ─────────────────────────────────────────────────────────────────────────────
// AXIS 5 — PLACEMENT (the AZ topology)
// ─────────────────────────────────────────────────────────────────────────────

/// The placement axis — single-AZ vs multi-AZ. The Rust mirror of
/// `pangea-architectures::AzTopology`. The DEFAULT is DERIVED from the storage
/// binding, not free ([`default_placement`]): a single-instance-EBS pod needs its
/// volume's AZ to always have a landing node (single-AZ); a stateless or
/// per-replica-EBS workload wins deeper independent spot pools + AZ-failure
/// resilience from multi-AZ.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Placement {
    /// Span exactly one AZ. REQUIRED for a single-instance-EBS pod (else a reclaim
    /// in the volume's AZ strands it — the eyes-2a-vs-floor-2b class).
    SingleAz,
    /// Span ≥ 2 AZs. The resilient default for stateless + per-replica-EBS
    /// (distributed) workloads: 2× independent spot pools + survives an AZ loss.
    MultiAz,
}

impl Placement {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SingleAz => "single-az",
            Self::MultiAz => "multi-az",
        }
    }

    pub const ALL: [Placement; 2] = [Self::SingleAz, Self::MultiAz];
}

/// How a workload binds storage — the input that DERIVES the placement default.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageBinding {
    /// No persistent volume (stateless pod, or a pure-ephemeral cache-backed
    /// builder). Multi-AZ safe.
    Stateless,
    /// A single pod bound to ONE EBS volume in ONE AZ (a single-writer store on a
    /// lone PVC). Single-AZ REQUIRED (volume-AZ affinity).
    SingleInstanceEbs,
    /// A StatefulSet with a PVC PER replica, each in its own AZ (a distributed /
    /// per-ordinal store). Multi-AZ is MORE resilient (each AZ carries its own
    /// volume; an AZ loss drops one replica, not the tier).
    PerReplicaEbs,
}

impl StorageBinding {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stateless => "stateless",
            Self::SingleInstanceEbs => "single-instance-ebs",
            Self::PerReplicaEbs => "per-replica-ebs",
        }
    }
}

/// THE PLACEMENT DEFAULT — derived from the storage binding (never a free knob).
/// Single-instance-EBS ⇒ single-AZ (volume-AZ affinity); everything else ⇒
/// multi-AZ (deeper pools + AZ resilience). This reconciles the operator's
/// "multi-AZ for resilience where stateful" with the CamelotNodeGroup
/// single-AZ-EBS contract: multi-AZ IS the resilient stateful default WHEN storage
/// is per-replica; single-AZ is required ONLY for the lone-volume case.
#[must_use]
pub fn default_placement(binding: StorageBinding) -> Placement {
    match binding {
        StorageBinding::SingleInstanceEbs => Placement::SingleAz,
        StorageBinding::Stateless | StorageBinding::PerReplicaEbs => Placement::MultiAz,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AXIS 6 — INTERRUPTION (retirada drain / failover on spot reclaim)
// ─────────────────────────────────────────────────────────────────────────────

/// The interruption axis — how the pool handles a spot 2-minute reclaim warning.
/// The Rust mirror of the `retirada` / `Spot::InterruptionHandler` seam.
/// Tier-honest: the skeleton (EventBridge → SNS/Lambda + ASG lifecycle hook) is
/// shippable/opt-in; the drain agent + NATS reclaim-publish is a NAMED LiveTODO
/// (never rounded up to "retirada shipped").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Interruption {
    /// Graceful drain before reclaim (SNS drain seam). The DEFAULT for a stateful
    /// / in-flight-request-bearing pool (SaaS, eyes).
    RetiradaGracefulDrain,
    /// Full node-drain lambda (cordon + drain the reclaimed node). Heavier; for a
    /// pool that must hand off before the node dies.
    RetiradaNodeDrain,
    /// No drain — the pool is idempotent + cache-backed, so a reclaimed unit just
    /// re-dispatches (retry-on-reclaim). The build-burst default (a killed build
    /// re-runs cheaply off the cache).
    RetryOnReclaim,
}

impl Interruption {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RetiradaGracefulDrain => "retirada-graceful-drain",
            Self::RetiradaNodeDrain => "retirada-node-drain",
            Self::RetryOnReclaim => "retry-on-reclaim",
        }
    }

    /// Does this arm rely on the retirada drain seam (the tier-honest LiveTODO
    /// agent)? `RetryOnReclaim` does not — it is structurally complete (no agent).
    #[must_use]
    pub fn uses_retirada(self) -> bool {
        matches!(self, Self::RetiradaGracefulDrain | Self::RetiradaNodeDrain)
    }

    pub const ALL: [Interruption; 3] =
        [Self::RetiradaGracefulDrain, Self::RetiradaNodeDrain, Self::RetryOnReclaim];
}

#[cfg(test)]
mod tests {
    use super::{
        default_placement, resolve_arch, ArchCostSignal, ArchSelection, Interruption, LadderMode,
        Placement, PerfClass, ResolvedArch, SpotStrategy, StorageBinding,
    };

    #[test]
    fn arch_resolves_by_cost_and_a_pin_forces_it() {
        let arm_cheaper = ArchCostSignal { arm64_effective_cost: 0.63, amd64_effective_cost: 1.00 };
        let x86_cheaper = ArchCostSignal { arm64_effective_cost: 1.19, amd64_effective_cost: 1.00 };
        assert_eq!(resolve_arch(ArchSelection::CostOptimized, arm_cheaper), ResolvedArch::Arm64);
        assert_eq!(resolve_arch(ArchSelection::CostOptimized, x86_cheaper), ResolvedArch::Amd64);
        // a pin ignores the cost signal
        assert_eq!(resolve_arch(ArchSelection::PinnedArm64, x86_cheaper), ResolvedArch::Arm64);
        assert_eq!(resolve_arch(ArchSelection::PinnedAmd64, arm_cheaper), ResolvedArch::Amd64);
    }

    #[test]
    fn arch_auto_flips_when_pricing_crosses_no_re_decision() {
        // THE self-adjust proof: same CostOptimized selection, flip the signal,
        // get the other arch — the arch choice is cost-driven, never a hardcode.
        let sel = ArchSelection::CostOptimized;
        let before = ArchCostSignal { arm64_effective_cost: 1.19, amd64_effective_cost: 1.00 };
        let after = ArchCostSignal { arm64_effective_cost: 0.90, amd64_effective_cost: 1.00 };
        assert_eq!(resolve_arch(sel, before), ResolvedArch::Amd64, "x86 wins while Graviton is pricier");
        assert_eq!(resolve_arch(sel, after), ResolvedArch::Arm64, "arm wins the instant Graviton crosses");
    }

    #[test]
    fn placement_default_is_storage_derived() {
        assert_eq!(default_placement(StorageBinding::SingleInstanceEbs), Placement::SingleAz);
        assert_eq!(default_placement(StorageBinding::Stateless), Placement::MultiAz);
        assert_eq!(default_placement(StorageBinding::PerReplicaEbs), Placement::MultiAz);
    }

    #[test]
    fn axis_labels_are_stable_and_unique() {
        // each axis' labels are distinct (no two arms share a wire token)
        fn uniq(v: &[&str]) -> bool {
            let mut s: Vec<&str> = v.to_vec();
            s.sort_unstable();
            s.dedup();
            s.len() == v.len()
        }
        assert!(uniq(&ArchSelection::ALL.map(ArchSelection::as_str)));
        assert!(uniq(&SpotStrategy::ALL.map(SpotStrategy::as_str)));
        assert!(uniq(&LadderMode::ALL.map(LadderMode::as_str)));
        assert!(uniq(&PerfClass::ALL.map(PerfClass::as_str)));
        assert!(uniq(&Placement::ALL.map(Placement::as_str)));
        assert!(uniq(&Interruption::ALL.map(Interruption::as_str)));
    }

    #[test]
    fn spot_strategy_label_is_the_aws_form() {
        assert_eq!(SpotStrategy::CapacityOptimized.to_aws(), "capacity-optimized");
        assert_eq!(SpotStrategy::PriceCapacityOptimized.to_aws(), "price-capacity-optimized");
        assert_eq!(SpotStrategy::Diversified.to_aws(), "diversified");
    }

    #[test]
    fn perf_class_matches_the_ruby_and_rust_mirrors() {
        assert_eq!(PerfClass::CostFloor.as_str(), "cost-floor");
        assert_eq!(PerfClass::TimeFloor.as_str(), "time-floor");
    }

    #[test]
    fn retry_on_reclaim_needs_no_retirada_agent() {
        assert!(!Interruption::RetryOnReclaim.uses_retirada());
        assert!(Interruption::RetiradaGracefulDrain.uses_retirada());
    }
}
