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

/// Whether a carve disturbs the running workload — the property the in-place
/// keystone makes UNIVERSAL. `ZeroDisruption` = the live resource is re-sized
/// with no restart (`d(restart)/d(carve) = 0`): the host cgroup/sysfs lever, the
/// `pods/resize` subresource, the CSI online-expand. `Rolling` = the carve goes
/// through desired-state and re-creates pods (the pod template, the CNPG
/// `Cluster` top-level). The substrate is converging on `ZeroDisruption`
/// everywhere — `PodResize` obsoletes `PodTemplate` wherever the cluster supports
/// it — so "breathe never rolls (when it can avoid it)" is now a typed property,
/// not a hope. It is the precondition for the thesis's core claim: *the app
/// simply exists, continuously provided-for, and never notices a restart*. A
/// zero-disruption carve is also always-"golden" (it never leaves a
/// non-converged, pods-pending state) — the eclusa berth property, applied to
/// resource carving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disruption {
    /// The live container/host resource is re-sized in place — no restart.
    ZeroDisruption,
    /// The carve re-creates pods (template / CNPG top-level) — a rolling update.
    Rolling,
}

impl Disruption {
    #[must_use]
    pub fn is_zero(self) -> bool {
        matches!(self, Self::ZeroDisruption)
    }
}

impl LimitLayout {
    /// Whether carving at this layout disturbs the running workload. The in-place
    /// keystone moved memory/cpu (`PodResize`) into the zero-disruption set,
    /// joining storage (`PvcRequest`) and the host levers (`Host`); only the
    /// template-write layouts (`PodTemplate`, `ClusterTopLevel`) still roll.
    #[must_use]
    pub fn disruption(&self) -> Disruption {
        match self {
            Self::PodResize { .. } | Self::PvcRequest | Self::Host(_) => Disruption::ZeroDisruption,
            Self::PodTemplate { .. } | Self::ClusterTopLevel => Disruption::Rolling,
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
    use super::{Disruption, HostKnob, LimitLayout};

    #[test]
    fn in_place_layouts_are_zero_disruption() {
        assert!(LimitLayout::PodResize { container: None }.disruption().is_zero());
        assert!(LimitLayout::PvcRequest.disruption().is_zero());
        assert!(LimitLayout::Host(HostKnob::ZfsArcMax).disruption().is_zero());
    }

    #[test]
    fn template_write_layouts_roll() {
        assert_eq!(LimitLayout::PodTemplate { container: None }.disruption(), Disruption::Rolling);
        assert_eq!(LimitLayout::ClusterTopLevel.disruption(), Disruption::Rolling);
        assert!(!LimitLayout::PodTemplate { container: None }.disruption().is_zero());
    }

    #[test]
    fn pod_resize_obsoletes_pod_template_on_the_disruption_axis() {
        // the keystone: the SAME memory carve is Rolling via the template but
        // ZeroDisruption via resize — so preferring PodResize strictly removes
        // disruption with no other change.
        let roll = LimitLayout::PodTemplate { container: Some("app".into()) };
        let live = LimitLayout::PodResize { container: Some("app".into()) };
        assert_eq!(roll.disruption(), Disruption::Rolling);
        assert!(live.disruption().is_zero());
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
