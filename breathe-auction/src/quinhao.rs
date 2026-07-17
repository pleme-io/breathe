//! `quinhão` — the hierarchical, vector-valued, dynamically-rebalancing
//! even-fair-share allocator (BREATHABILITY-FABRIC §III.0 — "every part held at
//! the *same* 80/20 band by the *same* law, so they all shift together").
//!
//! **What this is, and what it is NOT.** This is the SOLVED, provable fair-share
//! divider — the dual of [`crate::BandLeiloeiro`]: where the band law answers
//! *"how big should this one limit be?"*, `quinhão` answers *"how do N weighted
//! claimants split a fixed band evenly?"*. It is NOT the deferred multi-cloud
//! [`crate::Otimizador`] (cost/latency/risk Pareto arbitration over alternative
//! shapes — genuinely unsolved). A `quinhão` allocation only ever *divides* a
//! pool the band law already holds at its setpoint; it adds **no new safety
//! surface** (it never grows a pool, never carves a k8s/host limit — the existing
//! band caps the pool; `quinhão` partitions what that band reports as available).
//!
//! **The shape (the operator's "resident flexibility that shifts accordingly").**
//!
//! 1. **Hierarchical claimants.** A [`Quinhao`] (claimant share) carries an
//!    optional `parent`, so the claimants form a forest: an implicit pool root →
//!    groups → users (gaveta: groups = shared-folders/families, users = members).
//!    Allocation is recursive weighted max-min fair share — the pool's
//!    `capacity * setpoint` (the 80% band) is split among top-level claimants by
//!    their weights/demands; each claimant's grant becomes the sub-pool its
//!    children split, recursively. Even by default (`weight = 1`).
//!
//! 2. **Vector-valued demand.** Each claimant's demand is a [`DemandVector`] over
//!    the fabric [`Dim`]s (storage live; cpu / bandwidth / request-rate
//!    typed-but-dormant). The allocator is a vector of INDEPENDENT per-dimension
//!    fair-shares — each dimension is held at the same band by the same law, so
//!    they "shift together" (FABRIC §III.0) while never coupling (a starved
//!    storage axis never drags the cpu axis).
//!
//! 3. **Resident flexibility / dynamic rebalance.** [`allocate_fabric`] is a PURE
//!    function of the current claimant tree + their vectors. So a user/group
//!    joining, leaving, or going active↔idle is a pure RE-DERIVATION — zero
//!    latency, no migration, no stateful cursor. This is the "balanced at any
//!    time" property as a structural fact, decoupled from breathe's slow band
//!    loop (the loop grows the POOL; `quinhão` divides the CURRENT pool the
//!    instant the claimant set changes).
//!
//! The single-level kernel is [`allocate_even`] (weighted max-min water-filling
//! with min/max clamps); the hierarchy is `allocate_even` applied recursively
//! down the tree, per dimension. The properties below are theorems the tests
//! prove (Σ ≤ band, equal-share, monotone add/remove, tree-respecting).

use std::collections::BTreeMap;

// ============================================================================
// Dim — the fabric dimensions the allocator divides over (vector-valued demand).
// ============================================================================

/// A breathe fabric dimension this allocator can divide a pool over. Storage is
/// the live axis for the drive product; the others are typed-but-dormant so a
/// future cpu/bandwidth/request-rate share is a value edit, not a rewrite (the
/// vector is already vector-valued — adding a working axis is data, per the
/// FABRIC "shift together" thesis).
///
/// Kept allocator-local (not `breathe_catalog::DimensionId`): a `Dim` is a
/// SHARE AXIS the allocator *divides*, whereas a `DimensionId` is a knob breathe
/// *carves* on a k8s/host target. The two are distinct concerns (the allocator
/// adds no carve surface); they happen to share names. The ordering here IS the
/// canonical vector order (`ALL_DIMS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Dim {
    /// Disk bytes on the pool (the live drive-product axis). `HardDownSoftUp`
    /// upstream, but the allocator only divides — directionality is the pool
    /// band's concern, not the divider's.
    Storage,
    /// CPU millicores (typed-but-dormant — composes when the drive runs compute).
    Cpu,
    /// Egress/ingress bandwidth bytes/s (typed-but-dormant).
    Bandwidth,
    /// API request-rate, requests/s (typed-but-dormant).
    RequestRate,
}

impl Dim {
    /// The canonical dimension vector order — the fixed arity every
    /// [`DemandVector`] / [`GrantVector`] is keyed on.
    pub const ALL: [Dim; 4] = [Dim::Storage, Dim::Cpu, Dim::Bandwidth, Dim::RequestRate];

    /// Stable wire/identity string (the CRD + status key for this axis).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Dim::Storage => "storage",
            Dim::Cpu => "cpu",
            Dim::Bandwidth => "bandwidth",
            Dim::RequestRate => "request-rate",
        }
    }

    /// Parse a wire string back to a [`Dim`]; `None` for an unknown axis (the
    /// caller reports + skips, never guesses). Inherent (not `FromStr`) because it
    /// is infallible-with-`None`, not `Result`-returning, and never used in a
    /// `str::parse` position.
    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Dim> {
        match s {
            "storage" => Some(Dim::Storage),
            "cpu" => Some(Dim::Cpu),
            "bandwidth" => Some(Dim::Bandwidth),
            "request-rate" => Some(Dim::RequestRate),
            _ => None,
        }
    }
}

// ============================================================================
// Demand — one claimant's bounded, weighted demand on ONE dimension.
// ============================================================================

/// One claimant's demand on a single [`Dim`]: a relative `weight` (the even-split
/// knob — `1` ⇒ a fair equal share), a hard `min` floor (always granted if the
/// pool can cover the Σ of floors), a hard `max` ceiling (never grant more, even
/// if free), and the `demand` it would actually use (the cap a generous share is
/// trimmed to — an idle claimant demanding little frees surplus for its
/// siblings, the "shifts accordingly" property).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Demand {
    /// Relative share weight. `0` ⇒ this claimant is inactive on this axis (it
    /// claims only its `min`, never a weighted share — the "idle" state). `1` is
    /// the even default; a larger weight buys a proportionally larger share.
    pub weight: u32,
    /// The floor always granted (a small reserved quota), in the dim's unit.
    pub min: u64,
    /// The ceiling never exceeded, in the dim's unit. `max == u64::MAX` ⇒ no cap.
    pub max: u64,
    /// What the claimant would actually use — a weighted share above this is
    /// trimmed (the surplus redistributes to hungrier siblings).
    pub demand: u64,
}

impl Demand {
    /// An EVEN, uncapped, weight-1 claimant that would use the whole pool if it
    /// could (`demand = u64::MAX`, `max = u64::MAX`, `min = 0`). The drive
    /// product's default member: a strictly even split falls out when every
    /// claimant is `even(…)` with the same demand.
    #[must_use]
    pub fn even() -> Self {
        Self { weight: 1, min: 0, max: u64::MAX, demand: u64::MAX }
    }

    /// A claimant absent from an axis entirely (weight 0, no floor, no demand) —
    /// the default fill for a [`DemandVector`] dimension a claimant does not
    /// participate in. Granted exactly 0 on that axis.
    #[must_use]
    pub fn absent() -> Self {
        Self { weight: 0, min: 0, max: 0, demand: 0 }
    }

    /// The effective ceiling on a granted share — `min(max, demand)`. A claimant
    /// can never be granted more than it would use, nor more than its hard cap.
    #[must_use]
    fn effective_cap(self) -> u64 {
        self.max.min(self.demand)
    }
}

// ============================================================================
// allocate_even — the single-level kernel (weighted max-min water-filling).
// ============================================================================

/// Divide `band` among `claims` by **weighted max-min fair share** (water-fill):
/// each claimant's weighted share is `band * weight / Σweight`, clamped UP to its
/// `min` and DOWN to `min(max, demand)`; surplus freed by a clamped-low claimant
/// redistributes to the still-hungry ones, iterated to a fixpoint. Deterministic,
/// terminating, pure — adding/removing a claim is a re-derivation.
///
/// Returns a grant per input claim, **index-aligned to `claims`**, with the
/// invariants the tests prove:
/// - `Σ grants ≤ band` (never over-allocates the band).
/// - every claimant gets `≥ min` when the floors fit (`Σ min ≤ band`); if the
///   floors do NOT fit, floors are scaled down proportionally (the band is the
///   hard wall — the never-over-commit peer of the band law's never-OOM).
/// - equal `weight` + equal `(min,max,demand)` ⇒ equal grant (the EVEN split).
/// - a grant never exceeds `min(max, demand)` (respects the clamps).
///
/// The unit is the dimension's unit (bytes for storage); the function is
/// unit-blind, exactly like `breathe_control::decide`.
#[must_use]
pub fn allocate_even(band: u64, claims: &[Demand]) -> Vec<u64> {
    let n = claims.len();
    if n == 0 {
        return Vec::new();
    }

    // ── Floors first: every claimant is owed its `min`. If the floors don't fit
    //    the band, scale them down proportionally (the band is the hard wall) and
    //    return — there is nothing left to weight-share. ──────────────────────
    let sum_min: u128 = claims.iter().map(|c| u128::from(c.min)).sum();
    let band128 = u128::from(band);
    if sum_min >= band128 {
        if sum_min == 0 {
            return vec![0; n]; // band 0 and no floors ⇒ all zero
        }
        // Proportional floor scaling — preserves relative floor shares, Σ ≤ band.
        return claims
            .iter()
            .map(|c| u64::try_from(u128::from(c.min) * band128 / sum_min).unwrap_or(u64::MAX))
            .collect();
    }

    // Start every claimant at its floor; water-fill the remaining band over the
    // claimants still below their effective cap, by weight.
    let mut grant: Vec<u64> = claims.iter().map(|c| c.min).collect();
    // A claimant is "frozen" once it hits its effective cap (or has weight 0): it
    // takes no further surplus. Weight-0 claimants get exactly their floor.
    let mut frozen: Vec<bool> = claims.iter().map(|c| c.weight == 0 || c.min >= c.effective_cap()).collect();
    let mut remaining = band128 - sum_min;

    // Each pass distributes `remaining` over the active (unfrozen) claimants by
    // weight; any claimant that would exceed its cap is clamped + frozen, freeing
    // its surplus for the next pass. At least one claimant freezes per pass (or we
    // exhaust `remaining`), so the loop terminates in ≤ n passes.
    loop {
        let active_weight: u128 = claims
            .iter()
            .zip(&frozen)
            .filter(|(_, f)| !**f)
            .map(|(c, _)| u128::from(c.weight))
            .sum();
        if active_weight == 0 || remaining == 0 {
            break;
        }

        // Tentative weighted distribution of `remaining` across active claimants.
        // Clamp each to its effective cap; collect the surplus freed by clamping.
        let mut freed: u128 = 0;
        let mut any_frozen_this_pass = false;
        let mut distributed: u128 = 0;
        // First compute each active claimant's tentative add (floor of the exact
        // share), tracking the largest remainder for deterministic crumb assignment.
        let mut adds: Vec<u128> = vec![0; n];
        for (i, c) in claims.iter().enumerate() {
            if frozen[i] {
                continue;
            }
            let share = remaining * u128::from(c.weight) / active_weight;
            adds[i] = share;
            distributed += share;
        }
        // The integer-division crumb (remaining - distributed) — hand it to the
        // active claimants in a stable order so the result is deterministic and Σ
        // is exact. Larger-weight claimants first, then lower index (ties).
        let mut crumb = remaining - distributed;
        if crumb > 0 {
            let mut order: Vec<usize> = (0..n).filter(|&i| !frozen[i]).collect();
            order.sort_by(|&a, &b| {
                claims[b].weight.cmp(&claims[a].weight).then(a.cmp(&b))
            });
            for &i in &order {
                if crumb == 0 {
                    break;
                }
                adds[i] += 1;
                crumb -= 1;
            }
        }

        // Apply the adds, clamping to the effective cap + freezing on hit.
        for (i, c) in claims.iter().enumerate() {
            if frozen[i] || adds[i] == 0 {
                continue;
            }
            let cap = c.effective_cap();
            let cur = u128::from(grant[i]);
            let want = cur + adds[i];
            if want >= u128::from(cap) {
                freed += want - u128::from(cap);
                grant[i] = cap;
                frozen[i] = true;
                any_frozen_this_pass = true;
            } else {
                grant[i] = u64::try_from(want).unwrap_or(u64::MAX);
            }
        }

        remaining = freed;
        // If nothing froze AND nothing is freed, the distribution is stable —
        // every active claimant absorbed its share without hitting a cap.
        if !any_frozen_this_pass && freed == 0 {
            break;
        }
    }

    grant
}

// ============================================================================
// DemandVector / GrantVector — the per-dimension vectors.
// ============================================================================

/// A claimant's demand across every fabric [`Dim`] — fixed-arity (one [`Demand`]
/// per [`Dim::ALL`]). Storage is the live axis; the others default to
/// [`Demand::absent`] until the drive product runs that workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DemandVector {
    storage: Demand,
    cpu: Demand,
    bandwidth: Demand,
    request_rate: Demand,
}

impl DemandVector {
    /// A vector that participates only on [`Dim::Storage`] with `storage`, and is
    /// [`Demand::absent`] on every other axis — the drive-product default.
    #[must_use]
    pub fn storage_only(storage: Demand) -> Self {
        Self { storage, cpu: Demand::absent(), bandwidth: Demand::absent(), request_rate: Demand::absent() }
    }

    /// A vector with an explicit [`Demand`] on every axis.
    #[must_use]
    pub fn new(storage: Demand, cpu: Demand, bandwidth: Demand, request_rate: Demand) -> Self {
        Self { storage, cpu, bandwidth, request_rate }
    }

    /// This vector's demand on one axis.
    #[must_use]
    pub fn get(&self, dim: Dim) -> Demand {
        match dim {
            Dim::Storage => self.storage,
            Dim::Cpu => self.cpu,
            Dim::Bandwidth => self.bandwidth,
            Dim::RequestRate => self.request_rate,
        }
    }
}

/// A claimant's granted share across every fabric [`Dim`] — the index-aligned
/// dual of [`DemandVector`]. The grant ledger gaveta reads (per-dimension bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GrantVector {
    storage: u64,
    cpu: u64,
    bandwidth: u64,
    request_rate: u64,
}

impl GrantVector {
    /// The granted share on one axis.
    #[must_use]
    pub fn get(&self, dim: Dim) -> u64 {
        match dim {
            Dim::Storage => self.storage,
            Dim::Cpu => self.cpu,
            Dim::Bandwidth => self.bandwidth,
            Dim::RequestRate => self.request_rate,
        }
    }

    fn set(&mut self, dim: Dim, v: u64) {
        match dim {
            Dim::Storage => self.storage = v,
            Dim::Cpu => self.cpu = v,
            Dim::Bandwidth => self.bandwidth = v,
            Dim::RequestRate => self.request_rate = v,
        }
    }
}

// ============================================================================
// Quinhao — a hierarchical claimant (the forest node).
// ============================================================================

/// One claimant in the breathing fabric: a stable `id`, an optional `parent` id
/// (`None` ⇒ a top-level claimant that splits the pool band directly; `Some` ⇒ a
/// child that splits its parent's grant), and a [`DemandVector`]. For gaveta:
/// groups are parent-less (or pool-rooted) claimants; users are children naming
/// their group as `parent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quinhao {
    /// Stable identity (a gaveta group-id or member-id). Unique within the fabric.
    pub id: String,
    /// The parent claimant's id, if this is a child (a user under a group). `None`
    /// ⇒ a top-level claimant dividing the pool band.
    pub parent: Option<String>,
    /// The demand vector across every fabric dimension.
    pub demand: DemandVector,
}

impl Quinhao {
    /// A top-level (pool-rooted) claimant — `parent = None`.
    #[must_use]
    pub fn root(id: impl Into<String>, demand: DemandVector) -> Self {
        Self { id: id.into(), parent: None, demand }
    }

    /// A child claimant under `parent`.
    #[must_use]
    pub fn child(id: impl Into<String>, parent: impl Into<String>, demand: DemandVector) -> Self {
        Self { id: id.into(), parent: Some(parent.into()), demand }
    }
}

// ============================================================================
// FabricGrants — the recursive allocation result.
// ============================================================================

/// The computed grant ledger: a [`GrantVector`] per claimant id. The status
/// surface gaveta consumes — `grants[member_id].get(Dim::Storage)` is the
/// member's storage quota in bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FabricGrants {
    grants: BTreeMap<String, GrantVector>,
}

impl FabricGrants {
    /// The grant vector for a claimant id (all-zero if unknown).
    #[must_use]
    pub fn get(&self, id: &str) -> GrantVector {
        self.grants.get(id).copied().unwrap_or_default()
    }

    /// A claimant's grant on one dimension (0 if unknown).
    #[must_use]
    pub fn get_dim(&self, id: &str, dim: Dim) -> u64 {
        self.get(id).get(dim)
    }

    /// Iterate every claimant id + its grant vector, in id order.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &GrantVector)> {
        self.grants.iter()
    }

    /// How many claimants carry a grant.
    #[must_use]
    pub fn len(&self) -> usize {
        self.grants.len()
    }

    /// True when no claimant carries a grant.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.grants.is_empty()
    }
}

/// Why a claimant tree is rejected at the allocator boundary — a typed refusal,
/// never a silently-wrong allocation (the ★★ UNREPRESENTABLE-adjacent parse gate).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FabricError {
    /// A claimant id appears more than once (the tree is not a set of nodes).
    DuplicateId { id: String },
    /// A claimant's `parent` names an id that is not in the tree (dangling edge).
    UnknownParent { id: String, parent: String },
    /// The parent edges form a cycle (no root reachable — not a forest).
    Cycle { id: String },
}

impl std::fmt::Display for FabricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateId { id } => write!(f, "duplicate claimant id {id:?}"),
            Self::UnknownParent { id, parent } => write!(f, "claimant {id:?} names unknown parent {parent:?}"),
            Self::Cycle { id } => write!(f, "claimant {id:?} is part of a parent cycle (not a forest)"),
        }
    }
}

impl std::error::Error for FabricError {}

// ============================================================================
// allocate_fabric — the hierarchical, vector-valued recursion.
// ============================================================================

/// The pool's per-dimension capacity (the quantity the band holds at `setpoint`).
/// The allocatable band per dimension is `capacity * setpoint`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolCapacity {
    storage: u64,
    cpu: u64,
    bandwidth: u64,
    request_rate: u64,
}

impl PoolCapacity {
    /// A pool with only a storage capacity (the drive default); other axes 0.
    #[must_use]
    pub fn storage_only(storage: u64) -> Self {
        Self { storage, cpu: 0, bandwidth: 0, request_rate: 0 }
    }

    /// A pool capacity on every axis.
    #[must_use]
    pub fn new(storage: u64, cpu: u64, bandwidth: u64, request_rate: u64) -> Self {
        Self { storage, cpu, bandwidth, request_rate }
    }

    /// This pool's total capacity on one dimension (the band is this × setpoint).
    #[must_use]
    pub fn get(&self, dim: Dim) -> u64 {
        match dim {
            Dim::Storage => self.storage,
            Dim::Cpu => self.cpu,
            Dim::Bandwidth => self.bandwidth,
            Dim::RequestRate => self.request_rate,
        }
    }
}

/// **The fabric allocator.** Split the pool's `capacity * setpoint` (the 80/20
/// band) across the claimant forest by recursive weighted max-min fair share,
/// PER DIMENSION (the vector axes are independent — they "shift together" under
/// one shared band law but never couple). Pure: the whole allocation is a
/// re-derivation from `(capacity, setpoint, claimants)`, so a join/leave/idle is
/// instantaneous + migration-free (the "balanced at any time" property).
///
/// Returns one [`GrantVector`] per claimant id, with the theorems the tests
/// prove:
/// - **band-bounded**: `Σ top-level grants ≤ capacity * setpoint` on every axis.
/// - **tree-respecting**: `Σ children grants ≤ parent grant` (a user never gets
///   more than its group; a group's children split exactly the group's grant).
/// - **even**: equal-weight, equal-demand, equal-bounds siblings get equal grants.
/// - **monotone**: adding a sibling never raises another's grant; removing one
///   never lowers a remaining sibling's grant (the rebalance properties).
///
/// `setpoint` is clamped to `(0, 1]`; out-of-range collapses to `1.0` (divide the
/// whole capacity — the band law itself enforces the real setpoint on the pool).
///
/// # Errors
/// A typed [`FabricError`] when the claimant set is not a well-formed forest
/// (duplicate id / unknown parent / cycle) — rejected, never half-allocated.
pub fn allocate_fabric(
    capacity: PoolCapacity,
    setpoint: f64,
    claimants: &[Quinhao],
) -> Result<FabricGrants, FabricError> {
    // ── Parse-gate the forest: ids unique, parents resolve, no cycles. ──────
    let mut by_id: BTreeMap<&str, &Quinhao> = BTreeMap::new();
    for q in claimants {
        if by_id.insert(q.id.as_str(), q).is_some() {
            return Err(FabricError::DuplicateId { id: q.id.clone() });
        }
    }
    for q in claimants {
        if let Some(p) = &q.parent {
            if !by_id.contains_key(p.as_str()) {
                return Err(FabricError::UnknownParent { id: q.id.clone(), parent: p.clone() });
            }
        }
    }
    // Cycle check: walk parent edges from each node, bounded by tree size.
    for q in claimants {
        let mut cur = q.parent.as_deref();
        let mut steps = 0usize;
        while let Some(p) = cur {
            if p == q.id {
                return Err(FabricError::Cycle { id: q.id.clone() });
            }
            steps += 1;
            if steps > claimants.len() {
                return Err(FabricError::Cycle { id: q.id.clone() });
            }
            cur = by_id.get(p).and_then(|n| n.parent.as_deref());
        }
    }

    // Children index: parent-id → [child ids]; `None` bucket = top-level.
    let mut children: BTreeMap<Option<&str>, Vec<&str>> = BTreeMap::new();
    for q in claimants {
        children.entry(q.parent.as_deref()).or_default().push(q.id.as_str());
    }

    let setpoint = if setpoint > 0.0 && setpoint <= 1.0 { setpoint } else { 1.0 };
    let mut grants = FabricGrants::default();

    // ── Allocate each dimension independently (the vector is per-axis). ──────
    for dim in Dim::ALL {
        #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let band = (capacity.get(dim) as f64 * setpoint) as u64;
        allocate_subtree(dim, band, None, &children, &by_id, &mut grants);
    }

    Ok(grants)
}

/// Recursively split `band` (the sub-pool for this dim) among the children of
/// `parent` by [`allocate_even`], then recurse into each child with ITS grant as
/// the new band. Writes each claimant's per-dim grant into `grants`.
fn allocate_subtree(
    dim: Dim,
    band: u64,
    parent: Option<&str>,
    children: &BTreeMap<Option<&str>, Vec<&str>>,
    by_id: &BTreeMap<&str, &Quinhao>,
    grants: &mut FabricGrants,
) {
    let Some(kids) = children.get(&parent) else { return };
    if kids.is_empty() {
        return;
    }
    let demands: Vec<Demand> = kids.iter().map(|id| by_id[*id].demand.get(dim)).collect();
    let shares = allocate_even(band, &demands);
    for (id, share) in kids.iter().zip(shares) {
        grants.grants.entry((*id).to_string()).or_default().set(dim, share);
        // The child's grant becomes the band its own children split.
        allocate_subtree(dim, share, Some(*id), children, by_id, grants);
    }
}

// ============================================================================
// allocate_drf — the second single-level kernel (Dominant Resource Fairness).
// ============================================================================
//
// theory/BREATHABILITY.md §II.6.8 names this: a "breathable autonomous zone"
// that jointly allocates CPU + Memory + Storage + Network-IP across enrolled
// surfaces needs a policy that couples the axes (equalizes each claimant's
// *dominant* share across ALL resources at once) — the opposite design choice
// from `allocate_even`/`allocate_fabric` above, which deliberately divide each
// [`Dim`] independently. This is Dominant Resource Fairness (DRF; Ghodsi et
// al., NSDI 2011 — the Mesos/YARN multi-resource scheduler algorithm),
// reusing `quinhao`'s existing types end to end: zero new types, one new
// kernel selectable alongside `allocate_even`.
//
// **Scoped v0 — stated honestly, not rounded up.** This is the FLAT
// (single-level, non-hierarchical) kernel — the DRF analogue of
// `allocate_even`, not yet the recursive analogue of `allocate_fabric`.
// `Demand.weight`/`.min`/`.max` are NOT consumed here (DRF's own per-claimant
// clamping is a genuine multi-round extension — a claimant hitting a max
// before its dominant resource saturates gets frozen and the remaining
// claimants re-solve over the freed capacity, exactly like `allocate_even`'s
// freeze loop) — only `.demand` (the claimant's per-resource consumption
// quantity) is read. A `Demand::absent()` axis (`demand == 0`) does not count
// toward that claimant's dominant share; a `Demand::even()` axis
// (`demand == u64::MAX`, "unbounded") is likewise excluded — DRF requires a
// FINITE, differentiated per-resource demand vector to compute a meaningful
// dominant share, so callers must supply real consumption quantities on the
// axes a claimant is meant to contend on, not `even()`'s "give me everything"
// convention (that convention belongs to `allocate_even`'s water-fill model,
// not DRF's proportional-task model). The hierarchical (nested-forest)
// extension is a named follow-up, not attempted here.

/// One claimant's dominant share ratio at `band` — `max_r(demand[r] / band[r])`
/// over the axes it actually contends on (`demand` finite and nonzero). `0.0`
/// for a claimant with no finite demand on any axis (it is excluded from the
/// allocation entirely — see [`allocate_drf`]).
fn dominant_ratio(demand: &DemandVector, band: &[f64; 4]) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    Dim::ALL
        .iter()
        .enumerate()
        .map(|(i, dim)| {
            let d = demand.get(*dim).demand;
            if d == 0 || d == u64::MAX || band[i] <= 0.0 { 0.0 } else { d as f64 / band[i] }
        })
        .fold(0.0_f64, f64::max)
}

/// Divide `capacity`'s `setpoint` band among `claims` by **Dominant Resource
/// Fairness** — the progressive-filling closed form for the single-round
/// (unclamped) case: every claimant's dominant share grows in lockstep at a
/// shared rate `s`, until the FIRST resource saturates across the whole
/// claimant set; that `s*` is the equalized dominant share every claimant
/// receives, and each claimant's per-axis grant is `s* * demand[axis] /
/// dominant_ratio(claimant)` (i.e. its demand vector scaled by its own
/// "number of tasks" at that share).
///
/// Returns one [`GrantVector`] per input claim, **index-aligned to `claims`**
/// (matching [`allocate_even`]'s convention), with the theorems the tests
/// prove:
/// - **band-bounded**: `Σ grants[dim] ≤ band[dim]` on every axis (never
///   over-allocates; the non-binding axes carry slack by construction).
/// - **dominant-share-equalized**: every claimant with nonzero demand ends up
///   at the SAME dominant share (`max_r(grant[r] / band[r])` is equal across
///   claimants) — the property `allocate_even` does not have, and the reason
///   this kernel exists alongside it.
/// - **proportional**: a claimant's grant vector is a scalar multiple of its
///   demand vector (the allocation preserves the claimant's own resource
///   shape, unlike per-axis-independent water-filling).
#[must_use]
pub fn allocate_drf(capacity: PoolCapacity, setpoint: f64, claims: &[DemandVector]) -> Vec<GrantVector> {
    let n = claims.len();
    if n == 0 {
        return Vec::new();
    }

    let setpoint = if setpoint > 0.0 && setpoint <= 1.0 { setpoint } else { 1.0 };
    #[allow(clippy::cast_precision_loss)]
    let band: [f64; 4] = Dim::ALL.map(|d| capacity.get(d) as f64 * setpoint);

    let dom_ratios: Vec<f64> = claims.iter().map(|c| dominant_ratio(c, &band)).collect();

    // s* = the smallest per-axis saturation point, weighted by each
    // contending claimant's OWN dominant ratio (so growth is in units of
    // "dominant share", not raw demand) — the closed form of progressive
    // filling for the unclamped, single-round case (derivation + a
    // hand-verified worked example in `tests.rs`).
    let mut s_star = f64::INFINITY;
    for (i, dim) in Dim::ALL.iter().enumerate() {
        if band[i] <= 0.0 {
            continue;
        }
        #[allow(clippy::cast_precision_loss)]
        let sum_weighted: f64 = claims
            .iter()
            .zip(&dom_ratios)
            .map(|(c, &dr)| {
                if dr <= 0.0 {
                    return 0.0;
                }
                let d = c.get(*dim).demand;
                if d == 0 || d == u64::MAX {
                    0.0
                } else {
                    (d as f64) / dr
                }
            })
            .sum();
        if sum_weighted > 0.0 {
            let s_r = band[i] / sum_weighted;
            if s_r < s_star {
                s_star = s_r;
            }
        }
    }
    if !s_star.is_finite() {
        // No claimant demands anything finite on any axis — nothing to divide.
        return vec![GrantVector::default(); n];
    }

    claims
        .iter()
        .zip(&dom_ratios)
        .map(|(c, &dr)| {
            let mut g = GrantVector::default();
            if dr <= 0.0 {
                return g;
            }
            let k = s_star / dr; // this claimant's "number of tasks" at s*
            for dim in Dim::ALL {
                let d = c.get(dim).demand;
                if d == 0 || d == u64::MAX {
                    continue;
                }
                #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss, clippy::cast_possible_truncation)]
                let alloc = (k * d as f64).floor().max(0.0) as u64;
                g.set(dim, alloc);
            }
            g
        })
        .collect()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod proptests;
