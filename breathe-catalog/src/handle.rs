//! The complete breathability **handle** control surface
//! (docs/PROVISIONING.md + BREATHE.md). Every resource lever a workload exposes
//! — cgroup v2, the Kubernetes pod plane, and the host — enumerated as typed
//! data with its **control semantics**, so the platform can *steer* an
//! application's full resource-consumption **map + weights on the fly** and
//! always knows *how* each lever is controlled.
//!
//! **The load-bearing distinction (the whole point):**
//!
//! | semantics | what it is | how it's controlled |
//! |---|---|---|
//! | [`Cap`](ControlSemantics::Cap) | a work-conserving ceiling | **breathed** (the 80/20 band law) *iff it can saturate*; else ceiling-pinned |
//! | [`HardLimit`](ControlSemantics::HardLimit) | a wall that KILLs/REJECTs (OOM) | **breathed** carefully (the never-OOM floor-from-peak) |
//! | [`Reservation`](ControlSemantics::Reservation) | a protected floor/guarantee | **steered** (set to the desired reservation) |
//! | [`Weight`](ControlSemantics::Weight) | a proportional share under contention | **steered** ("weights on the fly") — never breathed (a weight arbitrates, it never saturates) |
//! | [`Count`](ControlSemantics::Count) | a discrete count (replicas, pids) | **breathed** (the replica/forma band) or set |
//!
//! A `Cap`/`HardLimit` is a *homeostasis* lever — the band law holds it at the
//! setpoint. A `Weight`/`Reservation` is a *steering* lever — set it to the
//! desired map; the band law does NOT breathe it (BREATHABILITY-MATH §2.2: a
//! weight is work-conserving arbitration, the L¹/soft class, not a pointwise cap).
//! Completeness of this catalog == complete control: every lever has a row, a
//! semantics, and a place in the control cycle.

/// A typed resource lever — the COMPLETE set across the three control planes.
/// (Esoteric cgroup controllers — uclamp, hugetlb, rdma, misc — extend this enum
/// with their row; the reflection test fails the build if a row is added without
/// a `HandleSpec`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Handle {
    // ── cgroup v2 — cpu ──
    /// `cpu.max` — the CPU bandwidth cap (quota/period). Work-conserving.
    CpuMax,
    /// `cpu.weight` — proportional CPU share (1..=10000, default 100). A weight.
    CpuWeight,
    /// `cpu.uclamp.min` — performance-hint floor (schedutil). A reservation-hint.
    CpuUclampMin,
    // ── cgroup v2 — memory ──
    /// `memory.max` — the hard memory limit; exceeding it OOM-kills. A hard limit.
    MemoryMax,
    /// `memory.high` — the throttle/reclaim ceiling (no kill). A cap (breathable).
    MemoryHigh,
    /// `memory.low` — best-effort reclaim protection. A reservation.
    MemoryLow,
    /// `memory.min` — hard reclaim protection (never reclaimed). A reservation.
    MemoryMin,
    /// `memory.swap.max` — swap hard limit. A hard limit.
    MemorySwapMax,
    // ── cgroup v2 — io ──
    /// `io.weight` — proportional IO share (1..=10000). A weight.
    IoWeight,
    /// `io.max` — IO bandwidth/iops cap per device. A cap.
    IoMax,
    /// `io.latency` — target latency protection (qos). A reservation.
    IoLatency,
    // ── cgroup v2 — pids ──
    /// `pids.max` — task-count hard limit. A hard limit.
    PidsMax,
    // ── Kubernetes — the pod plane (project onto the cgroup levers) ──
    /// `resources.limits.memory` → `memory.max`. A hard limit.
    K8sMemoryLimit,
    /// `resources.requests.memory` → scheduling + `memory.min`. A reservation.
    K8sMemoryRequest,
    /// `resources.limits.cpu` → `cpu.max`. A cap.
    K8sCpuLimit,
    /// `resources.requests.cpu` → `cpu.weight` + scheduling. A weight.
    K8sCpuRequest,
    /// the replica count. A count.
    K8sReplicas,
    // ── host (sysfs/systemd; the HostCluster boundary) ──
    /// `zfs_arc_max` — the live ZFS ARC ceiling. A cap.
    HostArcMax,
}

/// How a [`Handle`] is controlled — the load-bearing classification (see the
/// module table). Decides whether a lever is *breathed* (the band law) or
/// *steered* (set to a desired map).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlSemantics {
    /// A work-conserving ceiling. Breathed iff it can saturate.
    Cap,
    /// A wall that kills/rejects (OOM). Breathed carefully (never-OOM).
    HardLimit,
    /// A protected floor/guarantee. Steered.
    Reservation,
    /// A proportional share under contention. Steered ("weights on the fly").
    Weight,
    /// A discrete count. Breathed (replica/forma band) or set.
    Count,
}

impl ControlSemantics {
    /// Is a handle of this semantics driven by the **band law** (homeostasis)?
    /// Caps + hard-limits + counts are breathed; weights + reservations are steered.
    #[must_use]
    pub fn is_breathed(self) -> bool {
        matches!(self, Self::Cap | Self::HardLimit | Self::Count)
    }
    /// Is a handle of this semantics **steered** (set to a desired map / weights)?
    #[must_use]
    pub fn is_steered(self) -> bool {
        matches!(self, Self::Weight | Self::Reservation)
    }
}

/// Which control plane a [`Handle`] lives on (decides the actuation backend).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HandlePlane {
    /// cgroup v2 unified hierarchy (host cgroupfs / pod cgroup).
    Cgroup,
    /// the Kubernetes API (pod resources / replicas).
    Kubernetes,
    /// host sysfs / systemd (the `HostCluster` boundary).
    Host,
}

/// One handle declared as typed data — the control-surface peer of `DimensionSpec`
/// / `FormaSpec`. The reflection tests enforce the [`Handle`] ⇄ `HandleSpec`
/// bijection: a lever without a row (or a row without a lever) fails the build.
#[derive(Debug, Clone)]
pub struct HandleSpec {
    pub handle: Handle,
    pub name: &'static str,
    /// The kernel/k8s field this lever writes (`cpu.max`, `memory.high`, …).
    pub field: &'static str,
    pub plane: HandlePlane,
    pub semantics: ControlSemantics,
    /// The value unit (`bytes`, `millicores`, `weight(1..=10000)`, `count`).
    pub unit: &'static str,
    pub purpose: &'static str,
}

/// The catalog. One row per [`Handle`]; the reflection tests enforce the bijection.
pub const HANDLE_CATALOG: &[HandleSpec] = &[
    HandleSpec { handle: Handle::CpuMax, name: "cpu-max", field: "cpu.max", plane: HandlePlane::Cgroup, semantics: ControlSemantics::Cap, unit: "millicores", purpose: "cap CPU bandwidth (work-conserving)" },
    HandleSpec { handle: Handle::CpuWeight, name: "cpu-weight", field: "cpu.weight", plane: HandlePlane::Cgroup, semantics: ControlSemantics::Weight, unit: "weight(1..=10000)", purpose: "proportional CPU share under contention" },
    HandleSpec { handle: Handle::CpuUclampMin, name: "cpu-uclamp-min", field: "cpu.uclamp.min", plane: HandlePlane::Cgroup, semantics: ControlSemantics::Reservation, unit: "percent", purpose: "performance-hint floor (schedutil)" },
    HandleSpec { handle: Handle::MemoryMax, name: "memory-max", field: "memory.max", plane: HandlePlane::Cgroup, semantics: ControlSemantics::HardLimit, unit: "bytes", purpose: "hard memory limit (OOM cliff)" },
    HandleSpec { handle: Handle::MemoryHigh, name: "memory-high", field: "memory.high", plane: HandlePlane::Cgroup, semantics: ControlSemantics::Cap, unit: "bytes", purpose: "throttle/reclaim ceiling (no kill) — the breathable memory cap" },
    HandleSpec { handle: Handle::MemoryLow, name: "memory-low", field: "memory.low", plane: HandlePlane::Cgroup, semantics: ControlSemantics::Reservation, unit: "bytes", purpose: "best-effort reclaim protection" },
    HandleSpec { handle: Handle::MemoryMin, name: "memory-min", field: "memory.min", plane: HandlePlane::Cgroup, semantics: ControlSemantics::Reservation, unit: "bytes", purpose: "hard reclaim protection (never reclaimed)" },
    HandleSpec { handle: Handle::MemorySwapMax, name: "memory-swap-max", field: "memory.swap.max", plane: HandlePlane::Cgroup, semantics: ControlSemantics::HardLimit, unit: "bytes", purpose: "swap hard limit" },
    HandleSpec { handle: Handle::IoWeight, name: "io-weight", field: "io.weight", plane: HandlePlane::Cgroup, semantics: ControlSemantics::Weight, unit: "weight(1..=10000)", purpose: "proportional IO share under contention" },
    HandleSpec { handle: Handle::IoMax, name: "io-max", field: "io.max", plane: HandlePlane::Cgroup, semantics: ControlSemantics::Cap, unit: "bps-or-iops", purpose: "IO bandwidth/iops cap per device" },
    HandleSpec { handle: Handle::IoLatency, name: "io-latency", field: "io.latency", plane: HandlePlane::Cgroup, semantics: ControlSemantics::Reservation, unit: "microseconds", purpose: "target-latency QoS protection" },
    HandleSpec { handle: Handle::PidsMax, name: "pids-max", field: "pids.max", plane: HandlePlane::Cgroup, semantics: ControlSemantics::HardLimit, unit: "count", purpose: "task-count hard limit" },
    HandleSpec { handle: Handle::K8sMemoryLimit, name: "k8s-memory-limit", field: "resources.limits.memory", plane: HandlePlane::Kubernetes, semantics: ControlSemantics::HardLimit, unit: "bytes", purpose: "pod memory limit → memory.max" },
    HandleSpec { handle: Handle::K8sMemoryRequest, name: "k8s-memory-request", field: "resources.requests.memory", plane: HandlePlane::Kubernetes, semantics: ControlSemantics::Reservation, unit: "bytes", purpose: "pod memory request → scheduling + memory.min" },
    HandleSpec { handle: Handle::K8sCpuLimit, name: "k8s-cpu-limit", field: "resources.limits.cpu", plane: HandlePlane::Kubernetes, semantics: ControlSemantics::Cap, unit: "millicores", purpose: "pod cpu limit → cpu.max" },
    HandleSpec { handle: Handle::K8sCpuRequest, name: "k8s-cpu-request", field: "resources.requests.cpu", plane: HandlePlane::Kubernetes, semantics: ControlSemantics::Weight, unit: "millicores", purpose: "pod cpu request → cpu.weight + scheduling" },
    HandleSpec { handle: Handle::K8sReplicas, name: "k8s-replicas", field: "spec.replicas", plane: HandlePlane::Kubernetes, semantics: ControlSemantics::Count, unit: "count", purpose: "workload replica count" },
    HandleSpec { handle: Handle::HostArcMax, name: "host-arc-max", field: "zfs_arc_max", plane: HandlePlane::Host, semantics: ControlSemantics::Cap, unit: "bytes", purpose: "live ZFS ARC ceiling" },
];

/// Every handle — the domain side of the catalog bijection.
pub const ALL_HANDLES: [Handle; 18] = [
    Handle::CpuMax, Handle::CpuWeight, Handle::CpuUclampMin,
    Handle::MemoryMax, Handle::MemoryHigh, Handle::MemoryLow, Handle::MemoryMin, Handle::MemorySwapMax,
    Handle::IoWeight, Handle::IoMax, Handle::IoLatency,
    Handle::PidsMax,
    Handle::K8sMemoryLimit, Handle::K8sMemoryRequest, Handle::K8sCpuLimit, Handle::K8sCpuRequest, Handle::K8sReplicas,
    Handle::HostArcMax,
];

impl Handle {
    /// This handle's catalog row (`None` ⇒ a reflection-test failure).
    #[must_use]
    pub fn spec(self) -> Option<&'static HandleSpec> {
        HANDLE_CATALOG.iter().find(|s| s.handle == self)
    }
    #[must_use]
    pub fn semantics(self) -> ControlSemantics {
        self.spec().map_or(ControlSemantics::Cap, |s| s.semantics)
    }
    /// Driven by the band law (a homeostasis lever)?
    #[must_use]
    pub fn is_breathed(self) -> bool {
        self.semantics().is_breathed()
    }
    /// Set to a desired map / weights (a steering lever)?
    #[must_use]
    pub fn is_steered(self) -> bool {
        self.semantics().is_steered()
    }
}

// ============================================================================
// The resource MAP + the steering diff — adjusting weights/the map on the fly.
// ============================================================================

/// A workload's full resource-consumption **map** — every handle it sets, to its
/// value (unit per the handle's [`HandleSpec`]; the band law is unit-blind).
/// This is the typed object the platform steers.
pub type ResourceMap = std::collections::BTreeMap<Handle, u64>;

/// One lever to set to move a workload toward a desired map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandleDelta {
    pub handle: Handle,
    pub from: Option<u64>,
    pub to: u64,
    pub semantics: ControlSemantics,
}

/// The **steering diff**: the levers to actuate to move a workload from its
/// `current` map to the `desired` map. Only changed handles appear (idempotent —
/// an unchanged map yields an empty diff, so steering at rest is a no-op, the
/// same determinism discipline as the controllers). Deterministically ordered
/// (BTreeMap key order). This is "adjust the resource consumption map + weights
/// on the fly", as a typed, reviewable plan.
#[must_use]
pub fn steer_diff(current: &ResourceMap, desired: &ResourceMap) -> Vec<HandleDelta> {
    let mut out = Vec::new();
    for (&handle, &to) in desired {
        let from = current.get(&handle).copied();
        if from != Some(to) {
            out.push(HandleDelta { handle, from, to, semantics: handle.semantics() });
        }
    }
    out
}

/// Split a steering diff into the **breathed** levers (caps/limits the band law
/// owns — a steer here is an override the controller will re-converge) and the
/// **steered** levers (weights/reservations — set directly, the band law leaves
/// them alone). Lets a caller route each delta to the right control path.
#[must_use]
pub fn partition_diff(diff: &[HandleDelta]) -> (Vec<HandleDelta>, Vec<HandleDelta>) {
    diff.iter().partition(|d| d.semantics.is_breathed())
}

#[cfg(test)]
mod tests {
    use super::{
        partition_diff, steer_diff, ControlSemantics, Handle, HandleDelta, ResourceMap, ALL_HANDLES,
        HANDLE_CATALOG,
    };

    #[test]
    fn catalog_has_one_row_per_handle() {
        assert_eq!(HANDLE_CATALOG.len(), ALL_HANDLES.len(), "row count == handle count");
        for h in ALL_HANDLES {
            assert_eq!(HANDLE_CATALOG.iter().filter(|s| s.handle == h).count(), 1, "exactly one row for {h:?}");
        }
    }

    #[test]
    fn every_handle_resolves_its_spec() {
        for h in ALL_HANDLES {
            let s = h.spec().expect("spec");
            assert!(!s.field.is_empty(), "{h:?} has an empty field");
            assert!(!s.unit.is_empty());
        }
    }

    #[test]
    fn fields_are_unique() {
        for (i, a) in HANDLE_CATALOG.iter().enumerate() {
            for b in &HANDLE_CATALOG[i + 1..] {
                assert_ne!(a.field, b.field, "two handles write {} — field collision", a.field);
            }
        }
    }

    #[test]
    fn breathed_and_steered_partition_the_handles() {
        // Every handle is EITHER breathed (cap/limit/count) OR steered
        // (weight/reservation) — never both, never neither. The load-bearing law.
        for h in ALL_HANDLES {
            let sem = h.semantics();
            assert_ne!(sem.is_breathed(), sem.is_steered(), "{h:?}: must be exactly one of breathed/steered");
        }
        // and both sides are non-empty (we actually have weights to steer)
        assert!(ALL_HANDLES.iter().any(|h| h.is_steered()), "no steerable handles — where are the weights?");
        assert!(ALL_HANDLES.iter().any(|h| h.is_breathed()), "no breathed handles");
    }

    #[test]
    fn weights_are_steered_not_breathed() {
        // The operator's point: weights are set on the fly, never breathed.
        for h in [Handle::CpuWeight, Handle::IoWeight, Handle::K8sCpuRequest] {
            assert_eq!(h.semantics(), ControlSemantics::Weight);
            assert!(h.is_steered() && !h.is_breathed(), "{h:?} (a weight) must be steered, not breathed");
        }
    }

    #[test]
    fn memory_max_is_a_hard_limit_high_is_a_breathable_cap() {
        assert_eq!(Handle::MemoryMax.semantics(), ControlSemantics::HardLimit);
        assert_eq!(Handle::MemoryHigh.semantics(), ControlSemantics::Cap);
        assert!(Handle::MemoryHigh.is_breathed());
    }

    #[test]
    fn steer_diff_is_idempotent_and_ordered() {
        let mut current = ResourceMap::new();
        current.insert(Handle::CpuWeight, 100);
        current.insert(Handle::MemoryHigh, 8 * 1024 * 1024 * 1024);
        // identical map → empty diff (no churn — steering at rest is a no-op)
        assert!(steer_diff(&current, &current).is_empty());
        // change the cpu weight + add an io weight
        let mut desired = current.clone();
        desired.insert(Handle::CpuWeight, 400);
        desired.insert(Handle::IoWeight, 200);
        let diff = steer_diff(&current, &desired);
        assert_eq!(diff.len(), 2);
        // deterministic BTreeMap order: CpuWeight (variant 1) before IoWeight (variant 8)
        assert_eq!(diff[0].handle, Handle::CpuWeight);
        assert_eq!(diff[0], HandleDelta { handle: Handle::CpuWeight, from: Some(100), to: 400, semantics: ControlSemantics::Weight });
        assert_eq!(diff[1], HandleDelta { handle: Handle::IoWeight, from: None, to: 200, semantics: ControlSemantics::Weight });
    }

    #[test]
    fn partition_routes_breathed_vs_steered() {
        let mut current = ResourceMap::new();
        current.insert(Handle::MemoryHigh, 1000); // breathed cap
        current.insert(Handle::CpuWeight, 100); // steered weight
        let mut desired = current.clone();
        desired.insert(Handle::MemoryHigh, 2000);
        desired.insert(Handle::CpuWeight, 300);
        let diff = steer_diff(&current, &desired);
        let (breathed, steered) = partition_diff(&diff);
        assert_eq!(breathed.len(), 1);
        assert_eq!(breathed[0].handle, Handle::MemoryHigh);
        assert_eq!(steered.len(), 1);
        assert_eq!(steered[0].handle, Handle::CpuWeight);
    }
}
