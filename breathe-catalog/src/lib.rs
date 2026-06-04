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

use breathe_provider::{DimensionId, Directionality, DisruptionClass};

/// The provisioning catalog (`Floresta`) — the infra-scale peer of the dimension
/// catalog (resource SHAPES + the `Densa` envelope). See docs/PROVISIONING.md §2.2.
pub mod forma;

/// The complete breathability HANDLE control surface — every cgroup-v2 / k8s /
/// host resource lever, typed with its control semantics (breathed vs steered),
/// plus the `steer_diff` for adjusting a workload's resource map + weights on the
/// fly. See [`handle`].
pub mod handle;

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
    DimensionSpec {
        id: DimensionId::CgroupCpu,
        name: "cgroup-cpu",
        authoring_keyword: "defdimension-cgroup-cpu",
        maturity: Maturity::Working,
        directionality: Directionality::Bidirectional,
        resource_class: ResourceClass::Soft, // CPUQuota throttles (slow), never kills — a soft cap
        purpose: "hold a unit's cpu rate at the band by carving transient CPUQuota within its nodeBudget cpu territory",
        upstream_mirror: Some("systemctl set-property --runtime <unit> CPUQuota"),
        depends_on: &[],
    },
];

/// All dimension ids the substrate knows (the partition the catalog must cover).
pub const ALL_DIMENSIONS: [DimensionId; 7] = [
    DimensionId::Memory,
    DimensionId::Storage,
    DimensionId::Cpu,
    DimensionId::Replica,
    DimensionId::Arc,
    DimensionId::Cgroup,
    DimensionId::CgroupCpu,
];

/// Look up a dimension's row.
#[must_use]
pub fn lookup(id: DimensionId) -> Option<&'static DimensionSpec> {
    CATALOG.iter().find(|d| d.id == id)
}

// ── The ACTION catalog — the explicit, typed enumeration of every knob breathe
//    can carve, classified by RESTART COST. The restart-free set is what breathe
//    drives toward real-time through the standard tick; the restart-requiring set
//    is enumerated, gated by DisruptionPolicy, and used only when the carve is
//    reachable no other way (or the disruption is worth it). ───────────────────

/// Which actuation plane an action lives on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plane {
    /// systemd/sysfs on the node (HostCluster).
    Host,
    /// the live pod via `pods/resize` (KubeCluster).
    Pod,
    /// the PVC via CSI online-expand.
    Pvc,
    /// a controller's desired-state object (pod template / CNPG `Cluster`) — a roll.
    Workload,
    /// the cluster's node set (autoscaler / Karpenter).
    Node,
}

/// One concrete action breathe can take, declared as typed data.
#[derive(Debug, Clone, Copy)]
pub struct ActionSpec {
    /// Stable id.
    pub name: &'static str,
    /// The concrete knob carved.
    pub knob: &'static str,
    pub plane: Plane,
    /// The restart cost — the load-bearing field.
    pub class: DisruptionClass,
    /// Drivable in the standard reconcile tick at high cadence (true ⟺ the live
    /// workload is undisturbed each tick). Every `RestartFree` action is tickable;
    /// `RestartRequiring` actions are NOT (they gate on cooldown + policy).
    pub tickable: bool,
    /// One line. For `RestartRequiring` actions: WHEN it is still worth the roll.
    pub note: &'static str,
}

use DisruptionClass::{RestartConditional, RestartFree, RestartRequiring};

/// Every action, enumerated + classified. Adding a knob is one row; the
/// reflection tests fail the build if a `RestartFree` action is not tickable.
pub const ACTIONS: &[ActionSpec] = &[
    // ── RESTART-FREE — the real-time set (no workload disturbance, ever) ────────
    ActionSpec { name: "arc-max",            knob: "zfs_arc_max",          plane: Plane::Host, class: RestartFree, tickable: true, note: "sysfs write; ARC re-sizes live" },
    ActionSpec { name: "cgroup-memory-high", knob: "MemoryHigh",           plane: Plane::Host, class: RestartFree, tickable: true, note: "systemd transient set-property; soft reclaim throttle" },
    ActionSpec { name: "cgroup-cpu-quota",   knob: "CPUQuota",             plane: Plane::Host, class: RestartFree, tickable: true, note: "EXPLOIT: live cpu bandwidth cap on a host unit" },
    ActionSpec { name: "cgroup-cpu-weight",  knob: "CPUWeight",            plane: Plane::Host, class: RestartFree, tickable: true, note: "EXPLOIT: live cpu share under contention" },
    ActionSpec { name: "cgroup-io-weight",   knob: "IOWeight",             plane: Plane::Host, class: RestartFree, tickable: true, note: "EXPLOIT: live io share (blkio) under contention" },
    ActionSpec { name: "pod-cpu-resize",     knob: "pods/resize cpu",      plane: Plane::Pod,  class: RestartFree, tickable: true, note: "in-place both directions; cpu never restarts" },
    ActionSpec { name: "pod-memory-grow",    knob: "pods/resize memory↑",  plane: Plane::Pod,  class: RestartFree, tickable: true, note: "in-place; a memory GROW never restarts" },
    ActionSpec { name: "pvc-expand",         knob: "CSI ExpandVolume",     plane: Plane::Pvc,  class: RestartFree, tickable: true, note: "online grow-only; no remount" },
    ActionSpec { name: "node-add",           knob: "NodePool/Karpenter",   plane: Plane::Node, class: RestartFree, tickable: true, note: "EXPLOIT (K2): grow the envelope; existing pods undisturbed" },

    // ── RESTART-CONDITIONAL — restart-gated in one direction ───────────────────
    ActionSpec { name: "pod-memory-shrink",  knob: "pods/resize memory↓",  plane: Plane::Pod,  class: RestartConditional, tickable: true, note: "in-place iff resizePolicy memory == NotRequired, else restarts" },

    // ── RESTART-REQUIRING — useful, but disruptive; gated by DisruptionPolicy ──
    ActionSpec { name: "pod-template-carve", knob: "template resources",   plane: Plane::Workload, class: RestartRequiring, tickable: false, note: "USE when k8s <1.33 (no resize) or a QoS-class change needs a roll" },
    ActionSpec { name: "cnpg-cluster-carve", knob: "CNPG spec.resources",  plane: Plane::Workload, class: RestartRequiring, tickable: false, note: "USE: the only way to resize a CNPG instance; the operator rolls it safely" },
    ActionSpec { name: "replica-scale-down", knob: "terminate a pod",      plane: Plane::Workload, class: RestartRequiring, tickable: false, note: "USE: shed load (HPA-class); survivors undisturbed, the shed pod is lost" },
    ActionSpec { name: "reschedule",         knob: "drain + reschedule",   plane: Plane::Node,     class: RestartRequiring, tickable: false, note: "USE: NUMA/CCD re-placement, bin-packing, escape a degraded node, maintenance" },
];

/// True when every dimension's carve plane has at least one restart-free action —
/// the keystone's promise that the live workload can be held without disturbance.
#[must_use]
pub fn restart_free_actions() -> impl Iterator<Item = &'static ActionSpec> {
    ACTIONS.iter().filter(|a| a.class == DisruptionClass::RestartFree)
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

    // ── ACTION catalog reflection ───────────────────────────────────────

    /// THE load-bearing invariant: a restart-free action is ALWAYS tickable, and
    /// a restart-requiring one is NEVER tickable. This is what lets breathe drive
    /// the restart-free set toward real-time through the standard tick while
    /// gating the disruptive set on cooldown + policy. Fails the build on drift.
    #[test]
    fn restart_free_iff_tickable_and_requiring_never_tickable() {
        for a in ACTIONS {
            if a.class == DisruptionClass::RestartFree {
                assert!(a.tickable, "{} is RestartFree but not tickable", a.name);
            }
            if a.class == DisruptionClass::RestartRequiring {
                assert!(!a.tickable, "{} is RestartRequiring but tickable", a.name);
            }
        }
    }

    #[test]
    fn action_names_are_unique() {
        for (i, a) in ACTIONS.iter().enumerate() {
            for b in &ACTIONS[i + 1..] {
                assert_ne!(a.name, b.name, "duplicate action {}", a.name);
            }
        }
    }

    #[test]
    fn restart_cost_partitions_the_action_catalog() {
        let n = [DisruptionClass::RestartFree, DisruptionClass::RestartConditional, DisruptionClass::RestartRequiring]
            .iter()
            .map(|c| ACTIONS.iter().filter(|a| a.class == *c).count())
            .sum::<usize>();
        assert_eq!(n, ACTIONS.len());
    }

    /// The keystone's structural promise: BOTH the host plane and the pod plane
    /// have a restart-free carve — so a live workload on either plane is held
    /// without disturbance. (The substrate is converging on restart-free: it is
    /// the largest class.)
    #[test]
    fn both_host_and_pod_planes_have_a_restart_free_action() {
        let free_planes: Vec<Plane> = restart_free_actions().map(|a| a.plane).collect();
        assert!(free_planes.contains(&Plane::Host), "host plane needs a restart-free action");
        assert!(free_planes.contains(&Plane::Pod), "pod plane needs a restart-free action");
        let free = restart_free_actions().count();
        assert!(free * 2 > ACTIONS.len(), "restart-free should be the majority (converging)");
    }

    /// Every restart-requiring action MUST justify itself (a non-empty `note`
    /// saying when the roll is still worth it) — no silent disruptive action.
    #[test]
    fn restart_requiring_actions_justify_the_roll() {
        for a in ACTIONS {
            if a.class == DisruptionClass::RestartRequiring {
                assert!(a.note.contains("USE"), "{} must say when the roll is worth it", a.name);
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
