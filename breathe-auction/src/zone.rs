//! `zone` — the breathable autonomous zone (theory/BREATHABILITY.md §II.6.8).
//!
//! **The correction that shaped this module (stated so the history isn't
//! lost).** The theory doc's first draft proposed a brand-new "O6" invariant
//! for cross-surface structural dependency. A direct source read of
//! `breathe-catalog` found that invariant already exists, shipped, and
//! tested: [`breathe_catalog::DimensionSpec::depends_on`] is a typed `&[DimensionId]`
//! edge list per dimension, checked acyclic
//! ([`breathe_catalog::dependency_graph_is_acyclic`]) and dangling-edge-free by the
//! catalog's own reflection tests. Its ONLY consumer today is
//! `breathe-facade`, which renders it into a JSON `dependsOn` status field —
//! informational, never used to order a tick's decisions. This module is the
//! missing consumer: it turns that already-proven-acyclic graph into an
//! executable [`shigoto_dag::Dag`].
//!
//! **The one genuinely new piece.** `depends_on` is scoped to `DimensionId`
//! (k8s/host resource bands — memory, cpu, storage, …); `breathe_catalog::forma`'s
//! own `falls_back_to` graph is scoped to `Forma` (infra shapes — node pools,
//! IOPS, …) and means something different (substitution on failure, not
//! ordering on success). Neither catalog crosses into the other, but the
//! camelot 17-pod incident's root cause is EXACTLY a cross-catalog edge (a
//! `Forma` decision — which node-pool shape to run — gates a `DimensionId`
//! decision — what pod-density a `Cpu`/`Memory` band may assume). [`BreatheZone`]
//! is that typed bridge: a zone names which `Forma`s and `DimensionId`s
//! compose it, plus which dims a `Forma` resolution structurally gates.

use std::collections::HashMap;

use breathe_catalog::{lookup, ALL_DIMENSIONS};
use breathe_provider::{DimensionId, Forma};
use shigoto_dag::Dag;
use shigoto_types::{JobId, JobKindId, JobScope, JobSubject};

use crate::quinhao::{allocate_drf_fabric, allocate_fabric, FabricError, FabricGrants, PoolCapacity, Quinhao};

/// The typed `JobId` for one dimension's per-tick `decide()` call, scoped to
/// `scope`. Stable — two calls with the same `(scope, dim)` are `JobId`-equal.
#[must_use]
pub fn dimension_job(scope: &JobScope, dim: DimensionId) -> JobId {
    JobId {
        scope: scope.clone(),
        kind: JobKindId::new("breathe.dimension-decide"),
        subject: JobSubject::Pinned(dim.as_str().to_string()),
    }
}

/// The typed `JobId` for one `Forma`'s per-tick provisioning decision, scoped
/// to `scope`.
#[must_use]
pub fn forma_job(scope: &JobScope, forma: Forma) -> JobId {
    JobId {
        scope: scope.clone(),
        kind: JobKindId::new("breathe.forma-decide"),
        subject: JobSubject::Pinned(forma.as_str().to_string()),
    }
}

/// Build the tick `Dag` for a set of enrolled dimensions, scoped to one
/// zone — the real, catalog-driven consumer of `depends_on`. Every dim in
/// `dims` becomes a node; an edge `dep → dim` is added ONLY when `dep` is
/// itself enrolled in this zone (an edge to a dimension the zone doesn't
/// carry is dropped, never a dangling reference into another zone's scope —
/// `depends_on`'s own dangling-edge-within-the-catalog invariant is a
/// different, already-proven guarantee this respects rather than assumes).
///
/// Pure and cheap: `depends_on` is `'static` catalog data, so this is a
/// direct re-derivation, not a query against anything mutable.
#[must_use]
pub fn dimension_tick_dag(scope: &JobScope, dims: &[DimensionId]) -> Dag {
    let mut d = Dag::new();
    for &dim in dims {
        let job = dimension_job(scope, dim);
        d.ensure_node(job.clone());
        if let Some(spec) = lookup(dim) {
            for &dep in spec.depends_on {
                if dims.contains(&dep) {
                    d.add_edge(dimension_job(scope, dep), job.clone());
                }
            }
        }
    }
    d
}

/// A breathable autonomous zone — the typed scope theory/BREATHABILITY.md
/// §II.6.8 names: the set of `Forma`s and `DimensionId`s that should
/// converge TOGETHER, tick by tick, rather than as independent surfaces
/// blind to each other's decisions.
#[derive(Debug, Clone)]
pub struct BreatheZone {
    /// The zone's identity (e.g. a node pool, a cluster) — every job this
    /// zone's Dag contains is scoped here.
    pub scope: JobScope,
    /// The `Forma`s (infra shapes — node pools, IOPS, …) enrolled in this zone.
    pub formas: &'static [Forma],
    /// The `DimensionId`s (resource bands) enrolled in this zone.
    pub dims: &'static [DimensionId],
    /// The cross-catalog bridge: every `Forma` in [`Self::formas`] structurally
    /// gates every dim in `gated_dims` — that dim's `decide()` may not be
    /// trusted until every enrolled `Forma`'s decision has resolved. The
    /// camelot incident's edge, stated as data: node-pool shape gates the
    /// pod-density assumption CPU/Memory bands make.
    pub gated_dims: &'static [DimensionId],
    /// WHICH `quinhao` kernel this zone's claimants are divided by — the
    /// zone-level knob theory/BREATHABILITY.md §II.6.8 names: independent
    /// surfaces stay `PerAxisIndependent` (today's default everywhere);
    /// surfaces that jointly contend the SAME claimants (the auction case)
    /// select `DominantResourceFairness`. See [`allocate_for_zone`].
    pub allocation_policy: AllocationPolicy,
}

/// Which `quinhao` kernel a [`BreatheZone`]'s claimants are divided by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationPolicy {
    /// `allocate_even`/`allocate_fabric` — divide each `Dim` independently
    /// (today's default everywhere breathe carves — §II.6.2's O1/O2 law).
    PerAxisIndependent,
    /// `allocate_drf`/`allocate_drf_fabric` — equalize each claimant's
    /// dominant share across every enrolled dim AT ONCE (§II.6.8's leilão
    /// extension). Select this when a zone's claimants contend the SAME
    /// pool jointly across resource TYPES (the multi-resource-fairness
    /// case DRF exists for), not merely when several surfaces happen to be
    /// co-located.
    DominantResourceFairness,
}

/// Why a [`BreatheZone`] is refused at construction — a typed parse-gate,
/// never a silently-wrong Dag (mirroring `quinhao::FabricError`'s discipline).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZoneError {
    /// A dim in `gated_dims` is not also in `dims` — a gate on a dimension
    /// this zone doesn't even enroll.
    GatedDimNotEnrolled { dim: DimensionId },
    /// The within-catalog `depends_on` edges, restricted to THIS ZONE'S OWN
    /// `dims` (not the whole catalog), are NOT acyclic. Cannot happen against
    /// the shipped catalog today (`breathe_catalog`'s own reflection test
    /// already proves the FULL graph acyclic, and a subgraph of an acyclic
    /// graph is acyclic) — checked anyway, because a future catalog edit
    /// could still introduce one, and this zone must never silently build a
    /// cyclic Dag from it. `Forma` nodes can never participate in a cycle
    /// (they only ever have OUTGOING edges into `gated_dims`, never an
    /// incoming one — no mechanism in this module lets a `DimensionId`
    /// depend back on a `Forma`), so checking just the `DimensionId`
    /// subgraph is sufficient, not merely convenient.
    ///
    /// **Tier-honest gap, stated not hidden:** this arm has NO unit test.
    /// `dimension_tick_dag` reads the real, `'static`, hardcoded
    /// `breathe_catalog::CATALOG` — there is no injectable/mockable catalog
    /// seam to construct a fake cycle through, and adding one solely to
    /// test an arm that cannot fire against any real data today would be
    /// over-engineering ahead of need. The defense stays: a genuine future
    /// catalog edit that introduced a cycle would still be caught here
    /// (and by `breathe_catalog`'s own `dependency_graph_is_acyclic` CI
    /// gate, first, fleet-wide) — untested-but-structurally-present, not
    /// unrepresentable.
    DimensionCycle,
}

impl std::fmt::Display for ZoneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GatedDimNotEnrolled { dim } => {
                write!(f, "gated_dims names {dim:?}, which is not in this zone's own dims")
            }
            Self::DimensionCycle => write!(f, "this zone's dims contain a depends_on cycle"),
        }
    }
}

impl std::error::Error for ZoneError {}

impl BreatheZone {
    /// Validate this zone's own internal consistency — a parse-gate, run
    /// before [`zone_tick_dag`] ever builds a Dag from it.
    ///
    /// # Errors
    /// A typed [`ZoneError`] when `gated_dims` names something outside
    /// `dims`, or the zone's own dimension subset carries a `depends_on`
    /// cycle.
    pub fn validate(&self) -> Result<(), ZoneError> {
        for &dim in self.gated_dims {
            if !self.dims.contains(&dim) {
                return Err(ZoneError::GatedDimNotEnrolled { dim });
            }
        }
        // Reuse shigoto's OWN cycle detection (Dag::toposort) over THIS
        // zone's restricted dimension subgraph, rather than hand-rolling a
        // second DFS or (an earlier, corrected draft of this function)
        // checking the whole catalog's unrelated dimensions. `dims` alone
        // is sufficient — see DimensionCycle's own doc comment for why
        // `formas` can never introduce a cycle.
        if dimension_tick_dag(&self.scope, self.dims).toposort().is_err() {
            return Err(ZoneError::DimensionCycle);
        }
        Ok(())
    }
}

/// Build the FULL per-tick `Dag` for a zone: [`dimension_tick_dag`]'s
/// within-catalog `depends_on` edges, UNIONED with the zone's own
/// cross-catalog `Forma → gated_dims` edges. One Dag, two edge sources,
/// composed — the mechanism theory/BREATHABILITY.md §II.6.8 names.
///
/// # Errors
/// Propagates [`BreatheZone::validate`]'s typed refusal — never builds a
/// Dag from an inconsistent zone.
pub fn zone_tick_dag(zone: &BreatheZone) -> Result<Dag, ZoneError> {
    zone.validate()?;
    let mut d = dimension_tick_dag(&zone.scope, zone.dims);
    for &forma in zone.formas {
        let fjob = forma_job(&zone.scope, forma);
        d.ensure_node(fjob.clone());
        for &dim in zone.gated_dims {
            d.add_edge(fjob.clone(), dimension_job(&zone.scope, dim));
        }
    }
    Ok(d)
}

/// Every dimension this substrate knows, restricted to those with at least
/// one `depends_on` edge — a quick catalog-wide summary, useful for an
/// operator asking "which dimensions are structurally coupled today?"
/// without hand-walking the catalog.
#[must_use]
pub fn dimensions_with_structural_dependencies() -> HashMap<DimensionId, &'static [DimensionId]> {
    ALL_DIMENSIONS
        .iter()
        .filter_map(|&id| lookup(id).map(|spec| (id, spec.depends_on)))
        .filter(|(_, deps)| !deps.is_empty())
        .collect()
}

/// **Divide a zone's claimant forest by its OWN declared [`AllocationPolicy`]**
/// — the dispatcher that ties [`BreatheZone`] to `quinhao`'s two fabric
/// kernels, completing the concept theory/BREATHABILITY.md §II.6.8 names:
/// the zone boundary decides not just WHICH surfaces tick together (via
/// [`zone_tick_dag`]) but HOW their shared pool is divided once they do.
///
/// `PerAxisIndependent` and `DominantResourceFairness` are genuinely
/// different kernels (§ the `allocate_drf` doc comment in `quinhao`) — this
/// function makes the choice a single typed field on the zone, not a
/// call-site decision a caller could get wrong or forget.
///
/// # Errors
/// Propagates the underlying kernel's [`FabricError`] (malformed claimant
/// forest) — both kernels share the SAME `parse_forest` validation, so the
/// refusal behaves identically regardless of which policy is selected.
pub fn allocate_for_zone(
    zone: &BreatheZone,
    capacity: PoolCapacity,
    setpoint: f64,
    claimants: &[Quinhao],
) -> Result<FabricGrants, FabricError> {
    match zone.allocation_policy {
        AllocationPolicy::PerAxisIndependent => allocate_fabric(capacity, setpoint, claimants),
        AllocationPolicy::DominantResourceFairness => allocate_drf_fabric(capacity, setpoint, claimants),
    }
}

#[cfg(test)]
mod tests;
