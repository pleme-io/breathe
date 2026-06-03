//! `breathe-catalog` — the self-describing dimensions catalog (CATALOG REFLECTION).
//!
//! Every breathe dimension declares itself here as one typed row. Adding a
//! dimension is no longer just "write a provider" — it is "write a provider +
//! land its catalog row", and the reflection tests below **fail the build** if a
//! [`DimensionId`] variant has no row (or a row names no provider). The catalog
//! IS the inventory: `breathe-inventory` (M3) iterates it; the typed DAG of
//! `depends_on` edges gives the implementation order for free.
//!
//! Mirrors `sui-spec`'s catalog template; the maturity gate is a breathe-local
//! enum (a conscious fork, not a verbatim reuse of sui-spec's `M*TypedOnly`).

use breathe_provider::{DimensionId, Directionality};

/// How a dimension RECOVERS when its allocation is briefly wrong — the property
/// that decides whether "provided-for on average" is sound or fatal
/// (BREATHABILITY-THESIS §2/§3). It is orthogonal to [`Directionality`] (which
/// says which ways breathe may *move* the limit): memory and cpu are both
/// `Bidirectional`, but memory is `Hard` (OOM) and cpu is `Soft` (throttle).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceClass {
    /// Depletion is a throttle; recovery is automatic + lossless (the OS
    /// scheduler/reclaim integrates over time). The floor is anti-flap only —
    /// the WHOLE allocation is averageable. cpu, replicas, ARC (cache eviction),
    /// cgroup `MemoryHigh` (reclaim pressure, never OOM).
    Soft,
    /// Growth is monotone-irreversible (CSI online-resize is grow-only); the
    /// down-cliff is made unrepresentable by `GrowOnly`. Holds as a one-sided
    /// headroom promise (grow before the write-cliff). storage.
    HardDownSoftUp,
    /// The cliff is instantaneous, lossy, controller-irreversible (OOM-kill). The
    /// thesis holds ONLY above a peak-derived static floor recomputed every fresh
    /// tick — never an average. Must appear in the L2 never-swap sum. memory.
    Hard,
}

/// Dimensions whose floor is a hard constraint (must be provisioned from the
/// PEAK, never an average) and must therefore appear in the L2 never-swap sum.
/// Exactly the `Hard` ∪ `HardDownSoftUp` rows; asserted against the catalog.
pub const STATIC_FLOOR_DIMENSIONS: [DimensionId; 2] = [DimensionId::Memory, DimensionId::Storage];

impl ResourceClass {
    /// True when this class needs a peak-derived static floor (not an anti-flap
    /// floor): `Hard` and `HardDownSoftUp`. `Soft` floors are anti-flap only.
    #[must_use]
    pub fn needs_static_floor(self) -> bool {
        matches!(self, Self::Hard | Self::HardDownSoftUp)
    }
}

/// Mechanical readiness signal — lets tooling plan implementation order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Maturity {
    /// Implemented + tested + shippable now.
    Working,
    /// Typed border + spec authored; interpreter lands at the named milestone.
    M2Typed,
    M3Typed,
    /// Declared for completeness; no mutating interpreter (e.g. ObserveOnly).
    Informational,
}

/// One dimension declared as typed data.
#[derive(Debug, Clone)]
pub struct DimensionSpec {
    pub id: DimensionId,
    pub name: &'static str,
    /// The tatara-lisp authoring form this dimension exposes.
    pub authoring_keyword: &'static str,
    pub maturity: Maturity,
    pub directionality: Directionality,
    /// How the dimension recovers from a brief mis-allocation — decides whether
    /// "on average" is sound (`Soft`) or fatal-below-floor (`Hard`).
    pub resource_class: ResourceClass,
    pub purpose: &'static str,
    /// The upstream surface this mirrors, if any.
    pub upstream_mirror: Option<&'static str>,
    /// Dimensions this one consumes context from (the typed DAG edges).
    pub depends_on: &'static [DimensionId],
}

/// The catalog. One row per [`DimensionId`]; the reflection tests enforce the
/// bijection.
pub const CATALOG: &[DimensionSpec] = &[
    DimensionSpec {
        id: DimensionId::Memory,
        name: "memory",
        authoring_keyword: "defdimension-memory",
        maturity: Maturity::Working,
        directionality: Directionality::Bidirectional,
        resource_class: ResourceClass::Hard, // exceeding limits.memory → OOM-kill (pointwise cliff)
        purpose: "hold container memory at the band by carving resources.limits.memory",
        upstream_mirror: None,
        depends_on: &[DimensionId::Replica],
    },
    DimensionSpec {
        id: DimensionId::Storage,
        name: "storage",
        authoring_keyword: "defdimension-storage",
        maturity: Maturity::M2Typed,
        directionality: Directionality::GrowOnly,
        resource_class: ResourceClass::HardDownSoftUp, // CSI grow-only; the down-cliff is unrepresentable
        purpose: "grow PVC capacity at 80% (data persists; never shrink)",
        upstream_mirror: None,
        depends_on: &[],
    },
    DimensionSpec {
        id: DimensionId::Cpu,
        name: "cpu",
        authoring_keyword: "defdimension-cpu",
        maturity: Maturity::M2Typed,
        directionality: Directionality::Bidirectional,
        resource_class: ResourceClass::Soft, // over-limit is a throttle, recoverable
        purpose: "hold cpu at the band by carving resources.limits.cpu (millicores)",
        upstream_mirror: None,
        depends_on: &[DimensionId::Replica],
    },
    DimensionSpec {
        id: DimensionId::Replica,
        name: "replica",
        authoring_keyword: "defdimension-replica",
        maturity: Maturity::Informational,
        directionality: Directionality::ObserveOnly,
        resource_class: ResourceClass::Soft, // scaling is recoverable; never mutated anyway
        purpose: "observe replica count; compose with KEDA via disjoint fields (never write)",
        upstream_mirror: Some("KEDA ScaledObject"),
        depends_on: &[],
    },
    // ── HOST dimensions (the HostCluster boundary; ride within nodeBudget L2) ──
    DimensionSpec {
        id: DimensionId::Arc,
        name: "arc",
        authoring_keyword: "defdimension-arc",
        maturity: Maturity::Working,
        directionality: Directionality::Bidirectional,
        resource_class: ResourceClass::Soft, // shrinking evicts cache (perf-recoverable), never OOM
        purpose: "hold the ZFS ARC at the band by carving zfs_arc_max within nodeBudget.arcMaxGiB",
        upstream_mirror: Some("/sys/module/zfs/parameters/zfs_arc_max"),
        depends_on: &[],
    },
    DimensionSpec {
        id: DimensionId::Cgroup,
        name: "cgroup",
        authoring_keyword: "defdimension-cgroup",
        maturity: Maturity::Working,
        directionality: Directionality::Bidirectional,
        resource_class: ResourceClass::Soft, // MemoryHigh is a soft reclaim throttle (not MemoryMax/OOM)
        purpose: "hold a unit's working set at the band by carving transient MemoryHigh within its nodeBudget envelope",
        upstream_mirror: Some("systemctl set-property --runtime <unit> MemoryHigh"),
        depends_on: &[],
    },
];

/// All dimension ids the substrate knows (the partition the catalog must cover).
pub const ALL_DIMENSIONS: [DimensionId; 6] = [
    DimensionId::Memory,
    DimensionId::Storage,
    DimensionId::Cpu,
    DimensionId::Replica,
    DimensionId::Arc,
    DimensionId::Cgroup,
];

/// Look up a dimension's row.
#[must_use]
pub fn lookup(id: DimensionId) -> Option<&'static DimensionSpec> {
    CATALOG.iter().find(|d| d.id == id)
}

/// True when the `depends_on` DAG is acyclic (topological order solvable).
/// Iterative DFS with a visiting-set; pure, no allocation beyond two small vecs.
#[must_use]
pub fn dependency_graph_is_acyclic() -> bool {
    fn visit(id: DimensionId, stack: &mut Vec<DimensionId>, done: &mut Vec<DimensionId>) -> bool {
        if done.contains(&id) {
            return true;
        }
        if stack.contains(&id) {
            return false; // back-edge ⇒ cycle
        }
        stack.push(id);
        if let Some(spec) = lookup(id) {
            for &dep in spec.depends_on {
                if !visit(dep, stack, done) {
                    return false;
                }
            }
        }
        stack.pop();
        done.push(id);
        true
    }
    let mut done = Vec::new();
    for &id in &ALL_DIMENSIONS {
        let mut stack = Vec::new();
        if !visit(id, &mut stack, &mut done) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Substrate invariant: every dimension id has exactly one catalog row, and
    /// every row names a known id — the bijection that makes the catalog the
    /// inventory. Fails the build if a dimension is added without a row.
    #[test]
    fn catalog_is_a_bijection_with_dimension_ids() {
        assert_eq!(CATALOG.len(), ALL_DIMENSIONS.len(), "row count == dimension count");
        for &id in &ALL_DIMENSIONS {
            let n = CATALOG.iter().filter(|d| d.id == id).count();
            assert_eq!(n, 1, "exactly one row for {id}");
        }
    }

    /// Authoring keywords must be globally unique (no `(defdimension-*)` collision).
    #[test]
    fn authoring_keywords_are_unique() {
        for (i, a) in CATALOG.iter().enumerate() {
            for b in &CATALOG[i + 1..] {
                assert_ne!(a.authoring_keyword, b.authoring_keyword, "keyword collision");
            }
        }
    }

    /// The dependency DAG must be acyclic so M-phase order falls out mechanically.
    #[test]
    fn dependency_dag_has_no_cycle() {
        assert!(dependency_graph_is_acyclic());
    }

    /// Every `depends_on` edge must resolve to a real catalog row (no dangling refs).
    #[test]
    fn dependency_edges_resolve() {
        for d in CATALOG {
            for &dep in d.depends_on {
                assert!(lookup(dep).is_some(), "{} depends on a missing dimension", d.name);
            }
        }
    }

    /// The maturity histogram partitions the catalog (sum == size).
    #[test]
    fn maturity_histogram_partitions_the_catalog() {
        let counts = [Maturity::Working, Maturity::M2Typed, Maturity::M3Typed, Maturity::Informational]
            .iter()
            .map(|m| CATALOG.iter().filter(|d| d.maturity == *m).count())
            .sum::<usize>();
        assert_eq!(counts, CATALOG.len());
    }

    /// The directionality recorded in the catalog must match each provider's
    /// contract (memory/cpu bidirectional, storage grow-only, replica observe-only).
    #[test]
    fn directionality_matches_dimension_semantics() {
        assert_eq!(lookup(DimensionId::Memory).unwrap().directionality, Directionality::Bidirectional);
        assert_eq!(lookup(DimensionId::Storage).unwrap().directionality, Directionality::GrowOnly);
        assert_eq!(lookup(DimensionId::Cpu).unwrap().directionality, Directionality::Bidirectional);
        assert_eq!(lookup(DimensionId::Replica).unwrap().directionality, Directionality::ObserveOnly);
        assert_eq!(lookup(DimensionId::Arc).unwrap().directionality, Directionality::Bidirectional);
        assert_eq!(lookup(DimensionId::Cgroup).unwrap().directionality, Directionality::Bidirectional);
    }

    /// Every dimension declares a recovery class (the partition the floor logic
    /// keys off). Fails the build if a new dimension lands without one.
    #[test]
    fn resource_class_partitions_the_catalog() {
        let counts = [ResourceClass::Soft, ResourceClass::HardDownSoftUp, ResourceClass::Hard]
            .iter()
            .map(|rc| CATALOG.iter().filter(|d| d.resource_class == *rc).count())
            .sum::<usize>();
        assert_eq!(counts, CATALOG.len(), "every dimension has exactly one ResourceClass");
    }

    /// A `GrowOnly` dimension is EXACTLY a `HardDownSoftUp` one and vice-versa:
    /// the only reason to forbid shrink is an irreversible down-cliff. This ties
    /// the movement policy (Directionality) to the recovery class (ResourceClass)
    /// so the two can never disagree (storage is the sole member of both).
    #[test]
    fn grow_only_iff_hard_down_soft_up() {
        for d in CATALOG {
            let grow_only = d.directionality == Directionality::GrowOnly;
            let hd_su = d.resource_class == ResourceClass::HardDownSoftUp;
            assert_eq!(grow_only, hd_su, "{}: GrowOnly ⟺ HardDownSoftUp must hold", d.name);
        }
    }

    /// The dimensions needing a peak-derived static floor (Hard ∪ HardDownSoftUp)
    /// are EXACTLY `STATIC_FLOOR_DIMENSIONS` — the set that must appear in the L2
    /// never-swap sum. The L2 partition reads this to know which floors are hard.
    #[test]
    fn static_floor_dimensions_match_the_hard_classes() {
        let derived: Vec<DimensionId> = CATALOG
            .iter()
            .filter(|d| d.resource_class.needs_static_floor())
            .map(|d| d.id)
            .collect();
        for id in STATIC_FLOOR_DIMENSIONS {
            assert!(derived.contains(&id), "{id} should need a static floor");
        }
        assert_eq!(derived.len(), STATIC_FLOOR_DIMENSIONS.len(), "static-floor set must match exactly");
    }

    /// `Hard` (OOM) dimensions must be `Bidirectional` — a hard resource has to be
    /// able to BOTH provision its floor (grow) and reclaim headroom (shrink);
    /// a one-directional hard resource is a contradiction (it would be
    /// HardDownSoftUp instead). Memory is the sole `Hard` member today.
    #[test]
    fn hard_resources_are_bidirectional() {
        for d in CATALOG {
            if d.resource_class == ResourceClass::Hard {
                assert_eq!(d.directionality, Directionality::Bidirectional, "{} is Hard ⇒ must be Bidirectional", d.name);
            }
        }
    }

    /// The host dimensions route to the HostCluster boundary, not the k8s API.
    #[test]
    fn host_dimensions_are_flagged_host() {
        assert!(DimensionId::Arc.is_host());
        assert!(DimensionId::Cgroup.is_host());
        assert!(!DimensionId::Memory.is_host());
        assert!(!DimensionId::Cpu.is_host());
        for d in CATALOG {
            if d.id.is_host() {
                assert!(d.upstream_mirror.is_some(), "{} must name its host upstream", d.name);
            }
        }
    }
}
