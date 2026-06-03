//! `breathe-provider` — the provider/plugin spine: the `Cluster` Environment
//! trait, the `DimensionDescriptor` trait, and the **one generic
//! [`BandProvider`]** that implements [`ResourceProvider`] for every dimension.
//!
//! The compounding shape (theory/BREATHE.md §3): the observe/assign/release
//! *orchestration* is solved exactly once, in `BandProvider`; a new dimension
//! supplies only its genuinely-specific data via a `DimensionDescriptor`
//! (metric query, owned field, directionality, owner layout). A provider never
//! sees `decide`/`BandConfig` — `BandProvider` calls the proven band law's
//! inputs but the deciding lives entirely in `breathe-core`/`breathe-control`.

use async_trait::async_trait;

pub use breathe_control::{Directionality, FieldOwner, Observation};

/// Typed category atom — keys the registry, equals the catalog `:name`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DimensionId {
    Memory,
    Storage,
    Cpu,
    Replica,
    /// HOST: ZFS ARC max (`/sys/module/zfs/parameters/zfs_arc_max`).
    Arc,
    /// HOST: a systemd unit's transient cgroup memory high-water (`MemoryHigh`).
    Cgroup,
}

impl DimensionId {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Storage => "storage",
            Self::Cpu => "cpu",
            Self::Replica => "replica",
            Self::Arc => "arc",
            Self::Cgroup => "cgroup",
        }
    }

    /// True for dimensions whose I/O boundary is the HOST (systemd/sysfs via
    /// `HostCluster`) rather than the Kubernetes API (`KubeCluster`).
    #[must_use]
    pub fn is_host(self) -> bool {
        matches!(self, Self::Arc | Self::Cgroup)
    }
}

impl std::fmt::Display for DimensionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A reconcile target — the owner object whose limit a band controls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub namespace: String,
    pub name: String,
    /// `Deployment` | `StatefulSet` | `Cluster` (CNPG) | `PersistentVolumeClaim`.
    pub kind: String,
    pub api_version: String,
    pub container: Option<String>,
}

/// A writable HOST lever — the address `HostCluster` writes a breathe decision
/// to. Disjoint by construction from what `nodeBudget` (the static L2 partition)
/// owns: breathe writes the *runtime* `zfs_arc_max` parameter and *transient*
/// (`--runtime`) cgroup properties; nodeBudget owns the boot modprobe ceiling,
/// the static unit `MemoryMax`, and the cpuset pin. They never write the same
/// field, so the two layers compose without contention (the L1-within-L2 contract).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostKnob {
    /// `/sys/module/zfs/parameters/zfs_arc_max` — the live ARC ceiling, in bytes.
    ZfsArcMax,
    /// A systemd unit's transient cgroup property, e.g.
    /// (`nix-daemon.service`, `MemoryHigh`) applied via `systemctl set-property
    /// --runtime`. Never the unit file (that is nodeBudget's static `MemoryMax`).
    CgroupProperty { unit: String, property: String },
}

/// Where a HOST dimension reads its `used` scalar from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostMetric {
    /// ZFS ARC current size from `/proc/spl/kstat/zfs/arcstats` (`size` row), bytes.
    ArcSize,
    /// cgroup v2 `memory.current` for a systemd unit's slice, bytes.
    CgroupMemoryCurrent { unit: String },
}

/// Where a managed quantity lives on a target object — interpreted by the
/// `Cluster` impl when reading/patching. The *dimension* + the *owner kind*
/// together pick the layout (memory on a Deployment is `PodTemplate`; memory on
/// a CNPG `Cluster` is `ClusterTopLevel`; storage is always `PvcRequest`). The
/// `Host` arm carries the host lever for the `HostCluster` impl — `KubeCluster`
/// rejects it with a typed error (it can never legitimately receive one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitLayout {
    /// CNPG `Cluster`: `spec.resources.limits.<res>`.
    ClusterTopLevel,
    /// Deployment/StatefulSet: `spec.template.spec.containers[name].resources.limits.<res>`.
    /// Writing it ROLLS the workload (the controller re-creates pods).
    PodTemplate { container: Option<String> },
    /// IN-PLACE resize of the live pods via the `pods/{name}/resize` subresource
    /// (k8s ≥ 1.33) — the homeostasis keystone: carve the running container's
    /// cgroup with NO restart, exactly as `HostCluster` carves a host unit's
    /// cgroup. Reads + writes the LIVE pods (found by the owner's selector), not
    /// the template, so it never rolls; a re-created pod starts at the template
    /// default and the band re-converges it in-place on the next tick. QoS is
    /// preserved (a Guaranteed pod stays Guaranteed). Distinct from `PodTemplate`
    /// precisely because `d(restart)/d(carve) = 0`.
    PodResize { container: Option<String> },
    /// PVC: `spec.resources.requests.storage` (grow-only).
    PvcRequest,
    /// HOST: a systemd/sysfs lever — written by `HostCluster`, not the k8s API.
    Host(HostKnob),
}

/// How a category's `assign` lands (GALHO `ApplySemantics`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplySemantics {
    Transactional,
    ContinuousReconciliation,
    PartialProgress,
}

/// The RESTART COST of an action — the most load-bearing typed property in
/// breathe, because *without-restart* is the whole value: a restart-free action
/// can be driven through the standard tick at any cadence (near-real-time
/// management of the live workload), while a restart-requiring one must be gated.
/// Three honest classes, not two:
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisruptionClass {
    /// NEVER restarts — the live container/host resource is re-sized in place
    /// (host cgroup/sysfs lever, a pod cpu resize either way, a pod memory
    /// resize *up*, CSI online-expand, a survivor of a scale event). Tickable at
    /// any frequency: this is the set breathe drives toward real-time.
    RestartFree,
    /// Restart-free in the safe direction, restart-GATED in the other — a pod
    /// memory *shrink* is in-place only if the container's `resizePolicy` for
    /// memory is `NotRequired`; with `RestartContainer` it restarts. The actuator
    /// must read the policy and either carve in place or honor the gate.
    RestartConditional,
    /// ALWAYS re-creates the workload (pod-template write, CNPG `Cluster`
    /// top-level, image/env change, a drain+reschedule). Disruptive — but often
    /// the ONLY path (CNPG resize, k8s <1.33, NUMA re-placement) and sometimes
    /// worth it. Gated by [`DisruptionPolicy`].
    RestartRequiring,
}

impl DisruptionClass {
    /// True only for [`RestartFree`](Self::RestartFree) — drivable through ticks
    /// at any cadence with zero workload disturbance.
    #[must_use]
    pub fn is_restart_free(self) -> bool {
        matches!(self, Self::RestartFree)
    }
    /// True when the action can (possibly) restart the workload.
    #[must_use]
    pub fn may_restart(self) -> bool {
        !matches!(self, Self::RestartFree)
    }
}

/// The FLAG that makes "without restart" controllable + explicit. Set per band /
/// per node; the actuator refuses any action whose [`DisruptionClass`] the policy
/// does not permit (returning a typed deferral, never a silent roll). The default
/// is the cautious one — never restart a workload unless explicitly allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DisruptionPolicy {
    /// Only `RestartFree` actions — the workload is NEVER disturbed. A carve that
    /// would require (even conditionally) a restart is deferred + surfaced. The
    /// strictest, real-time-safe default.
    #[default]
    RestartFreeOnly,
    /// `RestartFree` + `RestartConditional` — allow an in-place memory shrink even
    /// where the resizePolicy may restart the container, but still never a full
    /// template roll.
    AllowConditional,
    /// Any action, including a full re-create — for workloads where the carve is
    /// only reachable by a roll (CNPG, k8s <1.33) and the disruption is acceptable.
    AllowRestart,
}

impl DisruptionPolicy {
    /// Whether this policy permits an action of the given restart cost.
    #[must_use]
    pub fn permits(self, class: DisruptionClass) -> bool {
        match self {
            Self::RestartFreeOnly => class == DisruptionClass::RestartFree,
            Self::AllowConditional => class != DisruptionClass::RestartRequiring,
            Self::AllowRestart => true,
        }
    }
}

/// Per-restart-class cooldown windows — golden berths cost nothing to occupy (a
/// `RestartFree` carve cools only ~one scrape interval, so the loop tracks the
/// band in near-real-time), while a ceiling crossing is expensive and stays
/// damped. This is what turns the catalog's `restart_free ⟺ tickable` promise
/// into actual loop cadence; a uniform cooldown discards it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassCooldowns {
    pub restart_free: u64,
    pub restart_conditional: u64,
    pub restart_requiring: u64,
}

impl Default for ClassCooldowns {
    fn default() -> Self {
        // restart_free ≈ a scrape interval (real-time); crossings stay long.
        Self { restart_free: 15, restart_conditional: 120, restart_requiring: 600 }
    }
}

impl ClassCooldowns {
    #[must_use]
    pub fn for_class(&self, class: DisruptionClass) -> u64 {
        match class {
            DisruptionClass::RestartFree => self.restart_free,
            DisruptionClass::RestartConditional => self.restart_conditional,
            DisruptionClass::RestartRequiring => self.restart_requiring,
        }
    }
    /// Structural invariant: free ≤ conditional ≤ requiring (golden is cheapest).
    #[must_use]
    pub fn well_ordered(&self) -> bool {
        self.restart_free <= self.restart_conditional && self.restart_conditional <= self.restart_requiring
    }
}

/// A carve's position relative to the GOLDEN region (the no-restart action
/// space). A `RestartFree` carve keeps every intermediate limit a comfortable,
/// always-restable berth — `GoldenPreserving`; anything restart-bearing is a
/// `CeilingCrossing` out of golden (the eclusa §XVIII line, drawn at the layout).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeTier {
    /// The live workload is undisturbed — golden.
    GoldenPreserving,
    /// Crossing out of golden carries this restart cost.
    CeilingCrossing(DisruptionClass),
}

impl EdgeTier {
    #[must_use]
    pub fn is_golden(self) -> bool {
        matches!(self, Self::GoldenPreserving)
    }
}

impl DisruptionClass {
    /// Project a restart cost onto the golden/ceiling line: only `RestartFree`
    /// preserves golden; any restart-bearing class is a crossing.
    #[must_use]
    pub fn edge_tier(self) -> EdgeTier {
        match self {
            Self::RestartFree => EdgeTier::GoldenPreserving,
            other => EdgeTier::CeilingCrossing(other),
        }
    }
}

impl LimitLayout {
    /// The layout's coarse worst-case restart cost — `PodResize` collapses to
    /// `RestartConditional` (a memory shrink may restart). For the PRECISE
    /// per-direction class of a specific carve use [`action_class`](Self::action_class).
    #[must_use]
    pub fn disruption_class(&self) -> DisruptionClass {
        match self {
            Self::PvcRequest | Self::Host(_) => DisruptionClass::RestartFree,
            Self::PodResize { .. } => DisruptionClass::RestartConditional,
            Self::PodTemplate { .. } | Self::ClusterTopLevel => DisruptionClass::RestartRequiring,
        }
    }

    /// The PRECISE restart cost of the SPECIFIC carve `(direction, resource)` —
    /// the fact `disruption_class()` throws away. A `PodResize` carve is
    /// `RestartFree` for cpu (either direction) AND for a memory GROW; only a
    /// memory (or other byte-resource) SHRINK is `RestartConditional` (it may
    /// restart per the container's `resizePolicy`). `PvcRequest`/`Host` are always
    /// `RestartFree`; the template-write layouts are always `RestartRequiring`.
    /// This is what lets growth be eager (golden) while only a reclaiming shrink
    /// can require a crossing.
    #[must_use]
    pub fn action_class(&self, growing: bool, resource: &str) -> DisruptionClass {
        match self {
            Self::PvcRequest | Self::Host(_) => DisruptionClass::RestartFree,
            Self::PodResize { .. } => {
                if resource == "cpu" || growing {
                    DisruptionClass::RestartFree
                } else {
                    DisruptionClass::RestartConditional
                }
            }
            Self::PodTemplate { .. } | Self::ClusterTopLevel => DisruptionClass::RestartRequiring,
        }
    }
}

/// A metric reading + the age of the underlying sample (freshness gate input).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    pub value: u64,
    pub age_secs: u64,
}

/// Where a dimension's `used` reading comes from. `PodMetricsMax` is the
/// always-on metrics-server (`metrics.k8s.io`) — the live working-set/cpu that
/// `kubectl top` shows, present on any cluster with metrics-server (core)
/// regardless of whether a TSDB is running. `Prometheus` is a PromQL endpoint
/// (historical / volume stats). breathe defaults memory+cpu to `PodMetricsMax`
/// so it never depends on a scale-to-zero TSDB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricSource {
    /// Raw PromQL against a Prometheus-compatible endpoint (storage / historical).
    Prometheus(String),
    /// Max container `resource` (memory bytes / cpu millicores) across the
    /// owner's pods, read live from metrics-server.
    PodMetricsMax { resource: String, pod_prefix: String },
    /// HOST: read directly from procfs/sysfs/cgroup via `HostCluster`.
    /// `KubeCluster` rejects this with a typed error.
    Host(HostMetric),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReceipt {
    pub from: u64,
    pub to: u64,
    pub source_hash: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseReceipt {
    pub baseline: Option<u64>,
    pub source_hash: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedReceipt {
    pub source_hash: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderError {
    TargetNotFound,
    MetricsMissing,
    NoCapacityField,
    ApiTransient(String),
    ApiPermanent(String),
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetNotFound => f.write_str("target not found"),
            Self::MetricsMissing => f.write_str("metrics missing"),
            Self::NoCapacityField => f.write_str("no capacity field (no limit set)"),
            Self::ApiTransient(m) => write!(f, "transient API error: {m}"),
            Self::ApiPermanent(m) => write!(f, "permanent API error: {m}"),
        }
    }
}

impl std::error::Error for ProviderError {}

/// The SSA field a provider owns (the guard input + status surface).
#[derive(Debug, Clone)]
pub struct OwnedField {
    pub manager: String,
    pub path: String,
}

/// A typed Server-Side-Apply patch. **True SSA only** — carries the `layout` so
/// the `Cluster` impl builds the right nested patch, and the `resource`
/// (`memory`/`cpu`/`storage`) for the leaf key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsaPatch {
    pub target: Target,
    pub field_manager: String,
    pub layout: LimitLayout,
    pub resource: String,
    pub value: u64,
}

/// The side-effecting boundary. Real impl is `KubeCluster`; tests pass
/// `MockCluster`. Dimension-agnostic: `query` runs raw PromQL, `read_limit`
/// reads a quantity at a layout, `field_owners` extracts ownership of a
/// fieldsV1 path, `apply` performs true SSA.
#[async_trait]
pub trait Cluster: Send + Sync {
    /// Read the dimension's `used` scalar from its [`MetricSource`].
    async fn read_used(&self, source: &MetricSource) -> Result<Sample, ProviderError>;
    async fn read_limit(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
    ) -> Result<u64, ProviderError>;
    async fn field_owners(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
        logical_field: &str,
    ) -> Result<Vec<FieldOwner>, ProviderError>;
    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError>;
}

/// Whether a band's layout has a GOLDEN path to its setpoint — the eclusa
/// reachability question made mechanical + typed. Because the band law is
/// monotone-convergent and every intermediate value is a never-OOM berth
/// (`safety_clamp`), golden reachability reduces to a pure question about the
/// carve actions: does every direction the band may move stay `RestartFree`?
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetpointReachability {
    /// Every carve toward the setpoint is `RestartFree` — golden end to end.
    GoldenToSetpoint,
    /// Reaching the setpoint needs a carve that crosses out of golden (names the
    /// ceiling) — the band can still PARK golden, but only converges to setpoint
    /// once the operator's `DisruptionPolicy` permits the crossing.
    RequiresCrossing { ceiling: DisruptionClass, layout: LimitLayout },
}

/// Does `layout` have a golden path to the setpoint for `resource`, given the
/// directions `dir` lets the band move? `ObserveOnly` never carves ⇒ trivially
/// golden; `GrowOnly` checks only the grow direction; `Bidirectional` needs BOTH
/// grow and shrink to be `RestartFree`. Policy-independent: golden-ness is a
/// property of the action space, not of whether the operator permits a crossing.
#[must_use]
pub fn setpoint_reachability(layout: &LimitLayout, dir: Directionality, resource: &str) -> SetpointReachability {
    let directions: &[bool] = match dir {
        Directionality::Bidirectional => &[true, false],
        Directionality::GrowOnly => &[true],
        Directionality::ObserveOnly => &[],
    };
    for &growing in directions {
        let class = layout.action_class(growing, resource);
        if !class.edge_tier().is_golden() {
            return SetpointReachability::RequiresCrossing { ceiling: class, layout: layout.clone() };
        }
    }
    SetpointReachability::GoldenToSetpoint
}

/// The per-dimension data + small layout logic — everything that is genuinely
/// dimension-specific. The observe/assign/release orchestration lives once in
/// [`BandProvider`], so a new dimension is *only* an impl of this trait + a
/// catalog row. It can carry no band logic (no `decide`/`BandConfig`).
pub trait DimensionDescriptor: Send + Sync + 'static {
    /// Construct the descriptor for a cluster that can (`resize_capable`) or
    /// cannot carve a pod-backed workload in place (`pods/resize`, k8s ≥1.33).
    /// This is the K1 "breathe never rolls" default: a dimension that *can* carve
    /// zero-disruption (memory/cpu via `PodResize`) prefers it whenever the
    /// cluster supports it; dimensions that are already zero-disruption
    /// (storage/host) or always roll ignore the capability. Default = ignore it
    /// (`Self::default()`); memory/cpu override to flip on `in_place`.
    fn with_resize_capability(resize_capable: bool) -> Self
    where
        Self: Sized + Default,
    {
        let _ = resize_capable;
        Self::default()
    }

    fn id(&self) -> DimensionId;
    fn directionality(&self) -> Directionality;
    /// SSA field manager (disjoint across dimensions → memory ⟂ cpu, breathe ⟂ KEDA).
    fn field_manager(&self) -> &'static str;
    /// Stable logical field label (layout-independent) — both the guard's
    /// `owned_field().path` and the stamped `FieldOwner.field` use this.
    fn logical_field(&self) -> &'static str;
    /// The leaf resource key in `limits`/`requests` (`memory`/`cpu`/`storage`).
    fn resource(&self) -> &'static str;
    fn semantics(&self) -> ApplySemantics;
    /// Where this dimension's limit lives on the given target.
    fn layout(&self, target: &Target) -> LimitLayout;
    /// The PromQL whose scalar is the dimension's `used`.
    fn metric_source(&self, target: &Target) -> MetricSource;
}

/// The spine — the dyn interface `breathe-core` reconciles through.
#[async_trait]
pub trait ResourceProvider: Send + Sync + 'static {
    fn id(&self) -> DimensionId;
    fn directionality(&self) -> Directionality;
    fn owned_field(&self) -> OwnedField;
    fn semantics(&self) -> ApplySemantics;
    /// The layout (plane) this dimension carves `target` at — carries the restart
    /// class. The loop reads this to NAME the action it is about to take.
    fn layout_for(&self, target: &Target) -> LimitLayout;
    /// The leaf resource key (`memory`/`cpu`/`storage`) — for the per-direction class.
    fn resource_key(&self) -> &str;
    /// The PRECISE restart class of the carve this provider would make on `target`
    /// in the `growing` direction. The loop consults this against the band's
    /// `DisruptionPolicy` before committing a carve (the golden-edge gate).
    fn action_class(&self, target: &Target, growing: bool) -> DisruptionClass {
        self.layout_for(target).action_class(growing, self.resource_key())
    }
    /// Whether this provider has a golden (restart-free) path to the setpoint.
    fn setpoint_reachability(&self, target: &Target) -> SetpointReachability {
        setpoint_reachability(&self.layout_for(target), self.directionality(), self.resource_key())
    }
    async fn observe(&self, target: &Target) -> Result<Observation, ProviderError>;
    async fn assign(&self, target: &Target, to_value: u64)
        -> Result<AssignReceipt, ProviderError>;
    async fn release(&self, target: &Target) -> Result<ReleaseReceipt, ProviderError>;
}

/// **The one generic provider.** Implements [`ResourceProvider`] for every
/// dimension; the dimension's specifics come from its `DimensionDescriptor`.
/// Adding a dimension never touches this code — that is the whole compounding
/// claim, made by one type.
pub struct BandProvider<C: Cluster + 'static, D: DimensionDescriptor> {
    cluster: C,
    descriptor: D,
}

impl<C: Cluster + 'static, D: DimensionDescriptor> BandProvider<C, D> {
    pub fn new(cluster: C, descriptor: D) -> Self {
        Self { cluster, descriptor }
    }
    /// Borrow the cluster (tests assert applied patches).
    pub fn cluster(&self) -> &C {
        &self.cluster
    }
}

#[async_trait]
impl<C: Cluster + 'static, D: DimensionDescriptor> ResourceProvider for BandProvider<C, D> {
    fn id(&self) -> DimensionId {
        self.descriptor.id()
    }
    fn directionality(&self) -> Directionality {
        self.descriptor.directionality()
    }
    fn owned_field(&self) -> OwnedField {
        OwnedField {
            manager: self.descriptor.field_manager().to_string(),
            path: self.descriptor.logical_field().to_string(),
        }
    }
    fn semantics(&self) -> ApplySemantics {
        self.descriptor.semantics()
    }
    fn layout_for(&self, target: &Target) -> LimitLayout {
        self.descriptor.layout(target)
    }
    fn resource_key(&self) -> &str {
        self.descriptor.resource()
    }

    async fn observe(&self, target: &Target) -> Result<Observation, ProviderError> {
        let used = self.cluster.read_used(&self.descriptor.metric_source(target)).await?;
        let layout = self.descriptor.layout(target);
        let capacity = self.cluster.read_limit(target, &layout, self.descriptor.resource()).await?;
        let owners = self
            .cluster
            .field_owners(target, &layout, self.descriptor.resource(), self.descriptor.logical_field())
            .await?;
        Ok(Observation { used: used.value, capacity, owners, staleness_secs: used.age_secs })
    }

    async fn assign(&self, target: &Target, to_value: u64) -> Result<AssignReceipt, ProviderError> {
        let layout = self.descriptor.layout(target);
        let from = self.cluster.read_limit(target, &layout, self.descriptor.resource()).await?;
        if to_value == from {
            return Ok(AssignReceipt { from, to: to_value, source_hash: [0u8; 16] });
        }
        let patch = SsaPatch {
            target: target.clone(),
            field_manager: self.descriptor.field_manager().to_string(),
            layout,
            resource: self.descriptor.resource().to_string(),
            value: to_value,
        };
        let applied = self.cluster.apply(&patch).await?;
        Ok(AssignReceipt { from, to: to_value, source_hash: applied.source_hash })
    }

    async fn release(&self, _target: &Target) -> Result<ReleaseReceipt, ProviderError> {
        Ok(ReleaseReceipt { baseline: None, source_hash: [0u8; 16] })
    }
}

#[cfg(test)]
mod tests {
    use super::{DisruptionClass, DisruptionPolicy, HostKnob, LimitLayout};

    #[test]
    fn layouts_classify_by_restart_cost() {
        assert_eq!(LimitLayout::PvcRequest.disruption_class(), DisruptionClass::RestartFree);
        assert_eq!(LimitLayout::Host(HostKnob::ZfsArcMax).disruption_class(), DisruptionClass::RestartFree);
        // PodResize is honestly RestartConditional (memory-shrink may restart).
        assert_eq!(LimitLayout::PodResize { container: None }.disruption_class(), DisruptionClass::RestartConditional);
        assert_eq!(LimitLayout::PodTemplate { container: None }.disruption_class(), DisruptionClass::RestartRequiring);
        assert_eq!(LimitLayout::ClusterTopLevel.disruption_class(), DisruptionClass::RestartRequiring);
    }

    #[test]
    fn pod_resize_is_strictly_less_disruptive_than_pod_template() {
        // the keystone: the SAME carve is RestartRequiring via the template but
        // only RestartConditional via resize — never a forced roll.
        let roll = LimitLayout::PodTemplate { container: Some("app".into()) }.disruption_class();
        let live = LimitLayout::PodResize { container: Some("app".into()) }.disruption_class();
        assert_eq!(roll, DisruptionClass::RestartRequiring);
        assert!(roll.may_restart() && live.may_restart());
        assert_ne!(live, DisruptionClass::RestartRequiring); // resize never forces a full roll
    }

    #[test]
    fn edge_tier_is_golden_iff_restart_free() {
        use DisruptionClass::{RestartConditional, RestartFree, RestartRequiring};
        assert!(RestartFree.edge_tier().is_golden());
        assert!(!RestartConditional.edge_tier().is_golden());
        assert!(!RestartRequiring.edge_tier().is_golden());
        assert_eq!(RestartRequiring.edge_tier(), super::EdgeTier::CeilingCrossing(RestartRequiring));
    }

    #[test]
    fn action_class_is_per_direction_and_per_resource() {
        use DisruptionClass::{RestartConditional, RestartFree, RestartRequiring};
        let resize = LimitLayout::PodResize { container: None };
        // memory: grow is golden/RestartFree, shrink may restart → conditional.
        assert_eq!(resize.action_class(true, "memory"), RestartFree);
        assert_eq!(resize.action_class(false, "memory"), RestartConditional);
        // cpu never restarts, either direction.
        assert_eq!(resize.action_class(true, "cpu"), RestartFree);
        assert_eq!(resize.action_class(false, "cpu"), RestartFree);
        // host + pvc always restart-free; template always requires a roll.
        assert_eq!(LimitLayout::PvcRequest.action_class(false, "storage"), RestartFree);
        assert_eq!(LimitLayout::Host(HostKnob::ZfsArcMax).action_class(false, "memory"), RestartFree);
        assert_eq!(LimitLayout::PodTemplate { container: None }.action_class(true, "memory"), RestartRequiring);
    }

    #[test]
    fn setpoint_reachability_names_the_golden_paths() {
        use super::{setpoint_reachability, Directionality, DisruptionClass, SetpointReachability};
        // cpu in-place: golden both directions.
        assert_eq!(
            setpoint_reachability(&LimitLayout::PodResize { container: None }, Directionality::Bidirectional, "cpu"),
            SetpointReachability::GoldenToSetpoint
        );
        // storage online-expand (grow-only): golden.
        assert_eq!(
            setpoint_reachability(&LimitLayout::PvcRequest, Directionality::GrowOnly, "storage"),
            SetpointReachability::GoldenToSetpoint
        );
        // memory in-place, bidirectional: the SHRINK is a conditional crossing.
        assert_eq!(
            setpoint_reachability(&LimitLayout::PodResize { container: None }, Directionality::Bidirectional, "memory"),
            SetpointReachability::RequiresCrossing { ceiling: DisruptionClass::RestartConditional, layout: LimitLayout::PodResize { container: None } }
        );
        // CNPG top-level: any carve is a full crossing.
        assert!(matches!(
            setpoint_reachability(&LimitLayout::ClusterTopLevel, Directionality::Bidirectional, "memory"),
            SetpointReachability::RequiresCrossing { ceiling: DisruptionClass::RestartRequiring, .. }
        ));
    }

    #[test]
    fn disruption_policy_gates_actions_by_class() {
        use DisruptionClass::{RestartConditional, RestartFree, RestartRequiring};
        // RestartFreeOnly (the default): only restart-free actions pass.
        assert_eq!(DisruptionPolicy::default(), DisruptionPolicy::RestartFreeOnly);
        assert!(DisruptionPolicy::RestartFreeOnly.permits(RestartFree));
        assert!(!DisruptionPolicy::RestartFreeOnly.permits(RestartConditional));
        assert!(!DisruptionPolicy::RestartFreeOnly.permits(RestartRequiring));
        // AllowConditional: free + conditional, never a full roll.
        assert!(DisruptionPolicy::AllowConditional.permits(RestartConditional));
        assert!(!DisruptionPolicy::AllowConditional.permits(RestartRequiring));
        // AllowRestart: everything.
        assert!(DisruptionPolicy::AllowRestart.permits(RestartRequiring));
    }
}

/// A programmable in-memory [`Cluster`] for tests — the typed-spec-triplet
/// testability seam. Records every SSA patch; programmable used/limit/owners.
#[cfg(feature = "mock")]
pub mod mock {
    use super::{
        AppliedReceipt, Cluster, FieldOwner, LimitLayout, MetricSource, ProviderError, Sample,
        SsaPatch, Target,
    };
    use async_trait::async_trait;
    use std::sync::Mutex;

    pub struct MockCluster {
        pub used: Sample,
        pub limit: u64,
        pub owners: Vec<FieldOwner>,
        applied: Mutex<Vec<SsaPatch>>,
    }

    impl MockCluster {
        #[must_use]
        pub fn new(used: u64, age_secs: u64, limit: u64, owners: Vec<FieldOwner>) -> Self {
            Self { used: Sample { value: used, age_secs }, limit, owners, applied: Mutex::new(Vec::new()) }
        }
        #[must_use]
        pub fn applied(&self) -> Vec<SsaPatch> {
            self.applied.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Cluster for MockCluster {
        async fn read_used(&self, _source: &MetricSource) -> Result<Sample, ProviderError> {
            Ok(self.used)
        }
        async fn read_limit(
            &self,
            _t: &Target,
            _layout: &LimitLayout,
            _resource: &str,
        ) -> Result<u64, ProviderError> {
            Ok(self.limit)
        }
        async fn field_owners(
            &self,
            _t: &Target,
            _layout: &LimitLayout,
            _resource: &str,
            _logical: &str,
        ) -> Result<Vec<FieldOwner>, ProviderError> {
            Ok(self.owners.clone())
        }
        async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
            self.applied.lock().unwrap().push(patch.clone());
            Ok(AppliedReceipt { source_hash: [0u8; 16] })
        }
    }
}
