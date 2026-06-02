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
}

impl DimensionId {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Storage => "storage",
            Self::Cpu => "cpu",
            Self::Replica => "replica",
        }
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

/// Where a managed quantity lives on a target object — interpreted by the
/// `Cluster` impl when reading/patching. The *dimension* + the *owner kind*
/// together pick the layout (memory on a Deployment is `PodTemplate`; memory on
/// a CNPG `Cluster` is `ClusterTopLevel`; storage is always `PvcRequest`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitLayout {
    /// CNPG `Cluster`: `spec.resources.limits.<res>`.
    ClusterTopLevel,
    /// Deployment/StatefulSet: `spec.template.spec.containers[name].resources.limits.<res>`.
    PodTemplate { container: Option<String> },
    /// PVC: `spec.resources.requests.storage` (grow-only).
    PvcRequest,
}

/// How a category's `assign` lands (GALHO `ApplySemantics`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplySemantics {
    Transactional,
    ContinuousReconciliation,
    PartialProgress,
}

/// A metric reading + the age of the underlying sample (freshness gate input).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    pub value: u64,
    pub age_secs: u64,
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
    async fn query(&self, promql: &str) -> Result<Sample, ProviderError>;
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
    fn used_promql(&self, target: &Target) -> String;
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
        let used = self.cluster.query(&self.descriptor.used_promql(target)).await?;
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

/// A programmable in-memory [`Cluster`] for tests — the typed-spec-triplet
/// testability seam. Records every SSA patch; programmable used/limit/owners.
#[cfg(feature = "mock")]
pub mod mock {
    use super::{
        AppliedReceipt, Cluster, FieldOwner, LimitLayout, ProviderError, Sample, SsaPatch, Target,
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
        async fn query(&self, _promql: &str) -> Result<Sample, ProviderError> {
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
