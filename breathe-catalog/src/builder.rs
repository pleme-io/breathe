//! `builder` — the DEFAULT breathe posture for the SUPER-CACHE-CI BUILD case
//! (theory/SUPER-CACHE-CI.md; theory/BREATHABILITY.md §II.6 flex-window).
//!
//! The [`preset`](crate::preset) module arms the Camelot *SaaS* workloads (the
//! steady, HA-floored, storage-bearing services). A *builder* pool breathes on a
//! DIFFERENT setpoint — **bursty**: floor 0 (pure ephemeral, cache-backed), a big
//! RAMDISK sized to hold the whole build tree in RAM (never-touch-disk), max
//! parallelism (saturate every vCPU), 100%-spot with an EVOLVING-DEGRADE instance
//! ladder that always places on the best-available spot tier and NEVER on-demand.
//!
//! This module is the ONE typed home of that posture so any super-cache-ci build
//! INHERITS it by default (Pillar 12 — declare, don't author per-build). It is the
//! offline-buildable half: the typed ladder + the burst preset + the parallelism
//! contract, all with `zero live cluster`. The live wake/auction/reclaim-drain are
//! LiveTODO (they need the `CamelotBuilderNodeGroup` infra + the ARC queue-scaler);
//! the tier-honest markers below never round those up.
//!
//! ── /algorithmic-prowess-seal (best-fit algorithm per sub-problem, NO ML) ──
//! * evolving-degrade ladder → a total, strictly-monotone PREFERENCE ORDER over a
//!   diversified spot menu, realized by AWS *capacity-optimized* allocation (the
//!   auction's own placement algorithm picks the deepest pool = fewest reclaims).
//!   The classical primitive is a **fallback / graceful-degradation ladder** made
//!   a **total order** so SOME tier always places; the SOTA primitive is
//!   capacity-optimized spot allocation as the realizer.
//! * "always the best-available spot, NEVER on-demand" → a typed absence: there is
//!   NO on-demand arm in the ladder type, and the union-of-tiers is proven
//!   non-empty (the floor tier is always reachable) → the never-place / on-demand
//!   states are unrepresentable in this crate (parse-time-rejected at the Ruby DSL
//!   boundary; see the diff to `CamelotBuilderNodeGroup` this seals into).
//!
//! Reflection tests below FAIL THE BUILD if the ladder is not strictly-preferred,
//! if a tier repeats a family (no extra depth), if the union under-covers the
//! diversified floor, or if the parallelism contract does not saturate the box.

use crate::cost::{FlexWindow, CAMELOT_FLEX_WINDOW};

/// The optimization OBJECTIVE a builder pool is tuned for — orthogonal to the
/// evolving-degrade DEPTH (which widens the pool for capacity). This is the Rust
/// mirror of `CamelotBuilderNodeGroup::BUILDER_PERF`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuilderObjective {
    /// Cheapest small deep node. The scale-to-zero floor keeps it near-free; the
    /// default when build LATENCY is not the binding constraint.
    CostFloor,
    /// The FASTEST node spot can give — minimum wall-clock build time. Selects the
    /// biggest latest-gen pool, a big RAMDISK, and max-parallel nix tuning. The
    /// super-cache-ci-build DEFAULT (the whole point is best-possible build times).
    TimeFloor,
}

impl BuilderObjective {
    /// The kebab-case stable label (matches the Ruby `builder_perf` symbol
    /// `cost_floor` / `time_floor`, hyphen-normalized to the discovery-tag form).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CostFloor => "cost-floor",
            Self::TimeFloor => "time-floor",
        }
    }
}

/// One rung of the EVOLVING-DEGRADE instance ladder — the typed encoding of "prefer
/// the fastest tier; if spot can't place it, fall through to the next-fastest tier
/// that still places". `rank` is the preference order (0 = MOST preferred = fastest);
/// `families` are the instance families this rung draws from; `note` is one line.
///
/// The ladder is a **total order** (every `rank` distinct, contiguous from 0) and
/// the LAST rung is the FLOOR — a broad diversified tier that the auction can
/// always place from. There is NO on-demand rung: the type has no field for it, so
/// an on-demand fallback is unrepresentable HERE (it is parse-rejected at the Ruby
/// DSL boundary in `CamelotBuilderNodeGroup::reject_on_demand!`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DegradeTier {
    /// Preference rank — 0 is the MOST-preferred (fastest) tier; higher = the
    /// graceful-degradation fall-throughs; the max rank is the FLOOR.
    pub rank: u8,
    /// A stable label (fastest / fast / broad-floor …).
    pub label: &'static str,
    /// The instance families this tier draws from (ordered preference-first). More
    /// families in the lower tiers ⇒ deeper spot capacity ⇒ the floor always places.
    pub families: &'static [&'static str],
    /// One line: what this tier optimizes for + when the auction degrades TO it.
    pub note: &'static str,
}

/// The evolving-degrade ladder for the amd64 super-cache-ci builder — a total,
/// strictly-monotone preference order from the fastest latest-gen compute down to a
/// broad diversified floor. Every tier is 100% spot; the auction runs
/// capacity-optimized across the UNION and the preference order biases it toward
/// rank 0 while the deeper tiers guarantee SOME placement.
///
/// The families are the shape a `Pangea::Spot::Catalog` timefloor→floor profile
/// resolves to (latest-gen 48xl/24xl compute at rank 0 → prior-gen + memory-heavy
/// at rank 1 → the broad diversified `cost` menu at the rank-2 FLOOR). The Ruby
/// side owns the concrete instance-type sizes; this is the FAMILY-level contract
/// the diff seals into the perf-class.
pub const AMD64_DEGRADE_LADDER: &[DegradeTier] = &[
    DegradeTier {
        rank: 0,
        label: "fastest-latest-gen",
        families: &["c7i", "c7a", "m7i", "m7a"],
        note: "biggest latest-gen compute (48xl/24xl = 96-192 vCPU) — the time-floor \
               target; capacity-optimized picks the deepest of these first",
    },
    DegradeTier {
        rank: 1,
        label: "fast-prior-gen-plus-memory",
        families: &["c6i", "c6a", "m6i", "m6a", "r7i", "r7a"],
        note: "prior-gen compute + memory-heavy — degrade here when the latest-gen \
               48xl pools show spot churn; still very fast, far deeper capacity",
    },
    DegradeTier {
        rank: 2,
        label: "broad-diversified-floor",
        families: &["m6i", "m6a", "m7i", "m5", "m5a", "c6i", "c6a", "r6i", "r6a"],
        note: "the diversified cost-floor menu (mirrors cost::CAMELOT_INSTANCE_FAMILIES) \
               — the FLOOR that ALWAYS places; slower per-node but a build still RUNS. \
               Never on-demand — scarcity widens the pool, it never leaves spot",
    },
];

/// The arm64 evolving-degrade ladder — the Graviton peer. Same total-order shape,
/// arch-native families (no cross-emulation, ever).
pub const ARM64_DEGRADE_LADDER: &[DegradeTier] = &[
    DegradeTier {
        rank: 0,
        label: "fastest-latest-gen",
        families: &["c7g", "m7g"],
        note: "biggest latest-gen Graviton3 compute (16xl = 64 vCPU) — the time-floor \
               target; capacity-optimized picks the deepest of these first",
    },
    DegradeTier {
        rank: 1,
        label: "fast-prior-gen-plus-memory",
        families: &["c6g", "m6g", "r7g"],
        note: "prior-gen Graviton2 compute + memory-heavy — degrade here on latest-gen \
               spot churn; deeper capacity, still fast",
    },
    DegradeTier {
        rank: 2,
        label: "broad-diversified-floor",
        families: &["c7g", "c6g", "m7g", "m6g", "r7g", "r6g"],
        note: "the broad Graviton floor that ALWAYS places; never on-demand",
    },
];

/// The MAX-PARALLEL contract every super-cache-ci build obeys — two levels:
/// ACROSS-IMAGES (all N services build concurrently, so wall-clock ≈
/// shared-closure-once + slowest-service, not the SUM) and WITHIN-DERIVATION (nix
/// `--max-jobs` + `--cores` auto-tuned to the node's ACTUAL vCPU count, never
/// hardcoded — a 48xl = 192 vCPU must be saturated).
///
/// The values are the DEFAULTS the build Job/pipeline reads; `auto`/`0` are nix's
/// "detect from the box" sentinels, which is exactly the "saturate the ACTUAL
/// vCPU count" requirement — a hardcoded number would UNDER-saturate a bigger box.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParallelismContract {
    /// nix `--max-jobs`: how many derivations build in parallel WITHIN one build.
    /// `"auto"` = one per core = full within-derivation fan (the correct saturating
    /// value on ANY box size). Never a fixed integer (that under-fills a bigger node).
    pub nix_max_jobs: &'static str,
    /// nix `--cores`: cores PER derivation build. `0` = all available (let the
    /// individual build's `-j$(nproc)` see the whole box). The saturating value.
    pub nix_cores: u32,
    /// ACROSS-IMAGES: build all services concurrently (a GHA `strategy.matrix`
    /// fan over the (svc,arch) rows, OR one nix invocation over all image attrs).
    /// `true` ⇒ wall-clock ≈ slowest-service, not the sum. The whole speed thesis.
    pub across_images_concurrent: bool,
    /// The default GHA matrix `max-parallel` cap — 0 means UNCAPPED (fan every row
    /// at once; a scale-to-zero spot pool absorbs the burst). A positive value caps
    /// concurrent build cells (only when a quota genuinely bounds it — C4 ceiling).
    pub matrix_max_parallel: u32,
}

/// The saturating parallelism contract — auto/all-cores, across-images concurrent,
/// uncapped matrix. This is the "absolute-best build times" default; nothing here
/// under-fills a big node or serializes the service fan.
pub const SATURATING_PARALLELISM: ParallelismContract = ParallelismContract {
    nix_max_jobs: "auto",
    nix_cores: 0,
    across_images_concurrent: true,
    matrix_max_parallel: 0,
};

/// The DEFAULT breathe posture for the super-cache-ci BUILD use-case class — the
/// bursty peer of [`crate::preset::CAMELOT`] (which arms the SaaS workloads). One
/// typed row so any super-cache-ci build inherits the whole posture by default:
/// time-floor objective (preferred) + evolving-degrade (never on-demand) +
/// RAMDISK-by-default + max-parallel + scale-to-zero + 100%-spot + retry-on-reclaim.
#[derive(Debug, Clone, Copy)]
pub struct BuilderBreatheClass {
    /// The stable use-case class label (`super-cache-ci-build`).
    pub class: &'static str,
    /// The default optimization objective — `TimeFloor` (best build times).
    pub objective: BuilderObjective,
    /// The at-rest replica/node floor — `0` (pure ephemeral; scale-from-zero).
    pub floor: u32,
    /// Aggressive scale-to-zero: sleep the pool this many seconds after the build
    /// queue drains. Short ⇒ near-free at idle.
    pub scale_to_zero_idle_secs: u32,
    /// The RAMDISK (build-sandbox tmpfs) size in GiB — sized to hold the whole build
    /// tree in RAM (never-touch-disk). A big-instance default; the box holds it
    /// trivially (a 48xl = 384 GB RAM). `0` would disable the never-touch-disk signal.
    pub ramdisk_gib: u32,
    /// The two-level parallelism contract (across-images + within-derivation).
    pub parallelism: ParallelismContract,
    /// The evolving-degrade instance ladder this class uses per-arch. amd64 shown;
    /// the arm64 ladder is the peer ([`ARM64_DEGRADE_LADDER`]).
    pub degrade_ladder_amd64: &'static [DegradeTier],
    /// Retry a build that a spot reclaim killed mid-run. Builders are idempotent +
    /// cache-backed, so a reclaimed build re-dispatches cheaply (the cache warms the
    /// re-run). `true` ⇒ the mid-build reclaim is survived, not fatal.
    pub retry_on_spot_reclaim: bool,
    /// The 100%-spot fraction (`1.0`). Never on-demand — a value below 1.0 would
    /// contradict the hard law (guarded by the tier-honesty test).
    pub spot_fraction: f64,
    /// The shared flex-window cost envelope (the offline-buildable $ budget + the
    /// diversified floor menu the ladder's floor tier mirrors).
    pub flex_window: FlexWindow,
}

/// THE default super-cache-ci build breathe class — the discoverable posture any
/// build inherits. Time-floor-preferred, evolving-degrade, big RAMDISK, max-parallel,
/// scale-to-zero, 100%-spot, retry-on-reclaim.
pub const SUPER_CACHE_CI_BUILD: BuilderBreatheClass = BuilderBreatheClass {
    class: "super-cache-ci-build",
    objective: BuilderObjective::TimeFloor,
    floor: 0,
    scale_to_zero_idle_secs: 120,
    ramdisk_gib: 64,
    parallelism: SATURATING_PARALLELISM,
    degrade_ladder_amd64: AMD64_DEGRADE_LADDER,
    retry_on_spot_reclaim: true,
    spot_fraction: 1.0,
    flex_window: CAMELOT_FLEX_WINDOW,
};

/// The floor (least-preferred) tier of a ladder — the one that ALWAYS places.
/// Total-order guarantee: this is the tier with the maximum rank.
#[must_use]
pub fn floor_tier(ladder: &'static [DegradeTier]) -> Option<&'static DegradeTier> {
    ladder.iter().max_by_key(|t| t.rank)
}

/// The most-preferred (fastest) tier — rank 0.
#[must_use]
pub fn fastest_tier(ladder: &'static [DegradeTier]) -> Option<&'static DegradeTier> {
    ladder.iter().min_by_key(|t| t.rank)
}

/// The UNION of every family across the whole ladder — the full instance-family set
/// the capacity-optimized auction draws from (the diversified pool that realizes
/// "always places"). Deduped, order-preserving (preference-first).
#[must_use]
pub fn ladder_family_union(ladder: &'static [DegradeTier]) -> Vec<&'static str> {
    let mut seen: Vec<&'static str> = Vec::new();
    for t in ladder {
        for &f in t.families {
            if !seen.contains(&f) {
                seen.push(f);
            }
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::{
        floor_tier, fastest_tier, ladder_family_union, BuilderObjective, AMD64_DEGRADE_LADDER,
        ARM64_DEGRADE_LADDER, SATURATING_PARALLELISM, SUPER_CACHE_CI_BUILD,
    };
    use crate::cost::{CAMELOT_INSTANCE_FAMILIES, MIN_DIVERSIFIED_FAMILIES};

    const LADDERS: [&[super::DegradeTier]; 2] = [AMD64_DEGRADE_LADDER, ARM64_DEGRADE_LADDER];

    /// THE total-order invariant: every ladder's ranks are DISTINCT and contiguous
    /// from 0 — so the preference order is total (no ambiguous tie, no gap) and the
    /// auction always has a well-defined "next-fastest tier to degrade to".
    #[test]
    fn ladder_ranks_are_a_total_order() {
        for ladder in LADDERS {
            assert!(!ladder.is_empty(), "a degrade ladder must have at least a floor tier");
            let mut ranks: Vec<u8> = ladder.iter().map(|t| t.rank).collect();
            ranks.sort_unstable();
            for (i, r) in ranks.iter().enumerate() {
                assert_eq!(*r as usize, i, "ranks must be contiguous from 0 (a total order); got {ranks:?}");
            }
        }
    }

    /// No tier repeats a family (a duplicate is not extra spot depth; it is a typo).
    #[test]
    fn no_tier_repeats_a_family() {
        for ladder in LADDERS {
            for t in ladder {
                let mut fams: Vec<&str> = t.families.to_vec();
                fams.sort_unstable();
                fams.dedup();
                assert_eq!(fams.len(), t.families.len(), "tier {} repeats a family", t.label);
            }
        }
    }

    /// THE degrade-is-total invariant: the FLOOR tier (max rank) is genuinely
    /// diversified (≥ MIN_DIVERSIFIED_FAMILIES) — so SOME tier ALWAYS places. A
    /// shallow floor could drain under a reclaim wave and leave NO placeable tier,
    /// which would force an on-demand fallback — the state this seals against.
    #[test]
    fn floor_tier_is_diversified_so_some_tier_always_places() {
        for ladder in LADDERS {
            let floor = floor_tier(ladder).expect("a ladder has a floor tier");
            assert!(
                floor.families.len() >= MIN_DIVERSIFIED_FAMILIES,
                "the floor tier {} has {} families; a total degrade needs >= {} for depth",
                floor.label,
                floor.families.len(),
                MIN_DIVERSIFIED_FAMILIES
            );
        }
    }

    /// The amd64 floor tier MIRRORS the shared diversified cost menu — the ladder's
    /// floor is exactly the `cost::CAMELOT_INSTANCE_FAMILIES` set, so the two
    /// diversified surfaces can never silently disagree (one source of "the floor").
    #[test]
    fn amd64_floor_mirrors_the_shared_cost_menu() {
        let floor = floor_tier(AMD64_DEGRADE_LADDER).expect("floor");
        let mut floor_fams: Vec<&str> = floor.families.to_vec();
        floor_fams.sort_unstable();
        let mut menu: Vec<&str> = CAMELOT_INSTANCE_FAMILIES.to_vec();
        menu.sort_unstable();
        assert_eq!(floor_fams, menu, "the amd64 degrade floor must mirror cost::CAMELOT_INSTANCE_FAMILIES");
    }

    /// The union across the ladder is at least as deep as the floor — degrading UP
    /// (rank 0 → floor) only ever WIDENS the pool, never narrows it. The auction's
    /// capacity-optimized fill over the union is what realizes "always places".
    #[test]
    fn ladder_union_is_deep() {
        for ladder in LADDERS {
            let union = ladder_family_union(ladder);
            let floor = floor_tier(ladder).expect("floor");
            assert!(
                union.len() >= floor.families.len(),
                "the ladder union must be at least as deep as its floor"
            );
            assert!(union.len() >= MIN_DIVERSIFIED_FAMILIES, "the union must be diversified");
        }
    }

    /// There is NO on-demand arm anywhere in the ladder type — the never-on-demand
    /// invariant made structural. A DegradeTier has NO on-demand field; every tier's
    /// note affirms it never leaves spot. (Truly-unrep in this crate: no field to
    /// set; parse-time-rejected at the Ruby boundary in reject_on_demand!.)
    #[test]
    fn the_floor_note_affirms_never_on_demand() {
        for ladder in LADDERS {
            let floor = floor_tier(ladder).expect("floor");
            assert!(
                floor.note.contains("never on-demand") || floor.note.contains("Never on-demand"),
                "the floor tier must affirm it never leaves spot"
            );
        }
    }

    /// THE parallelism contract SATURATES the box: max-jobs is the auto sentinel
    /// (never a fixed integer that under-fills a bigger node), cores is 0 (all),
    /// across-images is concurrent, and the matrix is uncapped by default.
    #[test]
    fn parallelism_saturates_the_box() {
        let p = SATURATING_PARALLELISM;
        assert_eq!(p.nix_max_jobs, "auto", "max-jobs must be the auto sentinel (saturate ANY box)");
        assert_eq!(p.nix_cores, 0, "cores must be 0 (all available cores per derivation)");
        assert!(p.across_images_concurrent, "the service fan must be concurrent (wall-clock = slowest, not sum)");
        assert_eq!(p.matrix_max_parallel, 0, "the matrix is uncapped by default (scale-to-zero absorbs the burst)");
    }

    /// A hardcoded max-jobs INTEGER is the anti-pattern this seals against: the auto
    /// sentinel is NOT parseable as a fixed count, so it can only mean "detect the
    /// box" — a future edit to a fixed number would fail to parse here.
    #[test]
    fn max_jobs_is_not_a_hardcoded_integer() {
        assert!(
            SATURATING_PARALLELISM.nix_max_jobs.parse::<u32>().is_err(),
            "max-jobs must be 'auto' (box-detected), never a hardcoded integer that under-fills a bigger node"
        );
    }

    /// THE super-cache-ci build posture: time-floor-preferred, floor 0, big RAMDISK,
    /// max-parallel, 100%-spot, retry-on-reclaim. Guards the whole named posture
    /// against a future edit rounding it up (flipping off spot, dropping the RAMDISK).
    #[test]
    fn super_cache_ci_build_is_the_best_build_times_posture() {
        let b = SUPER_CACHE_CI_BUILD;
        assert_eq!(b.class, "super-cache-ci-build");
        assert_eq!(b.objective, BuilderObjective::TimeFloor, "time-floor is the best-build-times default");
        assert_eq!(b.floor, 0, "pure ephemeral — scale-from-zero");
        assert!(b.ramdisk_gib >= 32, "the RAMDISK must hold the build tree in RAM (never-touch-disk)");
        assert!(b.retry_on_spot_reclaim, "a mid-build reclaim must be survived, not fatal");
        assert!((b.spot_fraction - 1.0).abs() < f64::EPSILON, "100% spot — never on-demand");
        assert_eq!(b.parallelism, SATURATING_PARALLELISM, "the build inherits the saturating parallelism contract");
        assert!(b.scale_to_zero_idle_secs > 0, "aggressive scale-to-zero keeps it near-free at idle");
    }

    /// The fastest tier is genuinely the latest-gen compute (rank 0), distinct from
    /// the floor — so the preference order actually BUYS speed at rank 0 while the
    /// floor guarantees placement. A one-tier ladder (fastest == floor) would still
    /// pass the total-order test, so this pins the real evolving shape.
    #[test]
    fn the_ladder_actually_evolves_fastest_distinct_from_floor() {
        for ladder in LADDERS {
            let fast = fastest_tier(ladder).expect("fastest");
            let floor = floor_tier(ladder).expect("floor");
            assert_eq!(fast.rank, 0, "the fastest tier is rank 0");
            assert!(ladder.len() >= 2, "an evolving-degrade ladder has >= 2 tiers (fastest + a floor to degrade to)");
            assert_ne!(fast.label, floor.label, "the fastest tier must differ from the floor (the ladder evolves)");
        }
    }

    /// The objective labels match the Ruby `builder_perf` discovery-tag form
    /// (hyphenated) — the Rust border and the Ruby node-group can never drift on the
    /// objective name (cost-floor / time-floor).
    #[test]
    fn objective_labels_match_the_ruby_discovery_tags() {
        assert_eq!(BuilderObjective::CostFloor.as_str(), "cost-floor");
        assert_eq!(BuilderObjective::TimeFloor.as_str(), "time-floor");
    }
}
