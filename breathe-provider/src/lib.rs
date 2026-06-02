//! `breathe-provider` — the `ResourceProvider` trait (the spine) + the `Cluster`
//! Environment trait.
//!
//! No dependency on the controller, so each `dimension-*` crate is an external
//! impl — mirroring `galho-types::IaCSystem`. The algebra is **non-overloadable**:
//! a provider exposes ONLY category-atomic I/O and never sees
//! [`breathe_control::decide`]/`BandConfig`. It receives a computed `to_value`
//! and translates it into one platform mutation; it cannot re-decide, widen the
//! band, or subvert the safety clamp ([`theory/BREATHE.md`] §3).
//!
//! `Observation` / `FieldOwner` / `Directionality` are re-exported from
//! `breathe-control` — every category projects into the same `(used, capacity)`
//! scalars the proven band law consumes.

use async_trait::async_trait;

pub use breathe_control::{Directionality, FieldOwner, Observation};

/// Typed category atom — keys the registry and equals the catalog `:name`.
/// Adding a variant is a deliberate substrate edit; an unknown dimension fails
/// to *compile*, never at startup.
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

/// A reconcile target — the workload owner whose field a provider manages. For
/// CNPG the kind is `Cluster` and the patched field lives on the `Cluster` CR
/// (which the CNPG operator propagates to its pods) — see BREATHE.md §15.5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub namespace: String,
    pub name: String,
    /// `Deployment` | `StatefulSet` | `Cluster` (CNPG) | …
    pub kind: String,
    pub api_version: String,
    /// Container within the pod template; `None` = the first container.
    pub container: Option<String>,
}

/// The SSA field a provider owns: a field-manager name + the dotted path. The
/// single-writer guard checks ownership of THIS exact path, so disjoint paths
/// across dimensions (memory `resources.limits.memory` vs replica `spec.replicas`)
/// never fight.
#[derive(Debug, Clone)]
pub struct OwnedField {
    pub manager: String,
    pub path: String,
}

/// How a category's `assign` lands — lets the loop interpret disruption and
/// attest honestly (mirrors GALHO `ApplySemantics`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplySemantics {
    /// One API write; the owner performs a bounded, reversible rolling update.
    Transactional,
    /// The write is requested; an external reconciler (CSI) converges async.
    ContinuousReconciliation,
    /// In-place change that may complete partially (in-place pod resize).
    PartialProgress,
}

/// Which scalar a metric read projects into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Used,
    Capacity,
}

/// A metric reading plus the age of the underlying sample. The loop gates every
/// mutation on `age_secs` (BREATHE.md §15.4) — the never-OOM proof holds only on
/// a fresh sample, so a stale read must never drive a carve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    pub value: u64,
    pub age_secs: u64,
}

/// Receipt of one atomic `assign`; `source_hash` (BLAKE3-128) anchors the
/// OutcomeChain entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignReceipt {
    pub from: u64,
    pub to: u64,
    pub source_hash: [u8; 16],
}

/// Receipt of a `release` (de-enrollment); `baseline` is the restored
/// pre-enrollment value when one was recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseReceipt {
    pub baseline: Option<u64>,
    pub source_hash: [u8; 16],
}

/// Receipt of a single SSA apply against the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedReceipt {
    pub source_hash: [u8; 16],
}

/// Typed errors a provider surfaces. Never a silent hang or a panic — the loop
/// maps each to a deterministic outcome (transient → fast requeue, permanent →
/// escalate + AnomalyChain).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderError {
    TargetNotFound,
    MetricsMissing,
    /// No denominator (no limit set) — the band law returns `NoLimit`.
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

/// A typed Server-Side-Apply patch. **True SSA only** (`Patch::Apply` with the
/// provider's field manager + `force`) — never strategic `Merge`. This is the
/// load-bearing single-writer contract (BREATHE.md §15.2): only a real SSA field
/// manager produces the `managedFields` ownership the guard reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsaPatch {
    pub target: Target,
    pub field_manager: String,
    /// Dotted field path being set (e.g. `resources.limits.memory`).
    pub path: String,
    /// Base-unit value (bytes / millicores).
    pub value: u64,
}

/// The side-effecting boundary every provider's I/O goes through. The real impl
/// is `KubeCluster`; tests pass `MockCluster`. This Environment trait is the
/// testability contract — the interpreter half of the typed-spec triplet is
/// mockable, so a provider's observe/assign is unit-tested with no real cluster.
#[async_trait]
pub trait Cluster: Send + Sync {
    async fn metric(&self, target: &Target, kind: MetricKind) -> Result<Sample, ProviderError>;
    async fn current_allocation(
        &self,
        target: &Target,
        dim: DimensionId,
    ) -> Result<u64, ProviderError>;
    /// Field-managers currently owning `field` on the target (from
    /// `metadata.managedFields`), for the field-granular single-writer guard.
    async fn field_owners(
        &self,
        target: &Target,
        field: &str,
    ) -> Result<Vec<FieldOwner>, ProviderError>;
    /// Apply a typed SSA patch. Implementations MUST use `Patch::Apply` (never
    /// `Merge`) so the field manager becomes a real ownership record.
    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError>;
}

/// The spine. A provider does only category-atomic I/O for one resident problem
/// category; it never carries band logic (the loop owns `decide` + the
/// directionality clamp). Object-safe via `async_trait` so the registry can hold
/// `Box<dyn ResourceProvider>`.
#[async_trait]
pub trait ResourceProvider: Send + Sync + 'static {
    /// Stable category atom — keys the registry, equals the catalog `:name`.
    fn id(&self) -> DimensionId;

    /// What this category may do; the loop enforces it via
    /// [`breathe_control::clamp_to_directionality`]. Providers carry no band logic.
    fn directionality(&self) -> Directionality;

    /// The SSA field manager + dotted path this provider owns (the guard input).
    fn owned_field(&self) -> OwnedField;

    /// Apply-semantics this category exposes, so the loop interprets disruption.
    fn semantics(&self) -> ApplySemantics;

    /// OBSERVE — read-only. Project the target into `(used, capacity)` in this
    /// category's base unit + the field owners + sample age. Never mutates.
    async fn observe(&self, target: &Target) -> Result<Observation, ProviderError>;

    /// ASSIGN — the ONE mutation. Carve/return `to_value` (base units), atomically
    /// for this category, via true SSA. Idempotent: `assign(to == current)` is a no-op.
    async fn assign(&self, target: &Target, to_value: u64)
        -> Result<AssignReceipt, ProviderError>;

    /// RELEASE — de-enrollment/finalizer. Return the category to its baseline
    /// (drop the SSA claim; restore the recorded original where one exists).
    /// Atomic; idempotent. `GrowOnly` providers never shrink — release is bookkeeping.
    async fn release(&self, target: &Target) -> Result<ReleaseReceipt, ProviderError>;
}

/// A programmable in-memory [`Cluster`] for tests — the test double that makes
/// every provider (the interpreter half of the typed-spec triplet) mockable with
/// no real cluster. Records every SSA patch so a test can assert exactly what was
/// carved, on which field, by which manager.
#[cfg(feature = "mock")]
pub mod mock {
    use super::{
        AppliedReceipt, Cluster, DimensionId, FieldOwner, MetricKind, ProviderError, Sample,
        SsaPatch, Target,
    };
    use async_trait::async_trait;
    use std::sync::Mutex;

    pub struct MockCluster {
        pub used: Sample,
        pub capacity: u64,
        pub owners: Vec<FieldOwner>,
        applied: Mutex<Vec<SsaPatch>>,
    }

    impl MockCluster {
        #[must_use]
        pub fn new(used: u64, age_secs: u64, capacity: u64, owners: Vec<FieldOwner>) -> Self {
            Self {
                used: Sample { value: used, age_secs },
                capacity,
                owners,
                applied: Mutex::new(Vec::new()),
            }
        }

        /// Every SSA patch this cluster has applied, in order.
        #[must_use]
        pub fn applied(&self) -> Vec<SsaPatch> {
            self.applied.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Cluster for MockCluster {
        async fn metric(&self, _t: &Target, kind: MetricKind) -> Result<Sample, ProviderError> {
            Ok(match kind {
                MetricKind::Used => self.used,
                MetricKind::Capacity => Sample { value: self.capacity, age_secs: 0 },
            })
        }
        async fn current_allocation(
            &self,
            _t: &Target,
            _dim: DimensionId,
        ) -> Result<u64, ProviderError> {
            Ok(self.capacity)
        }
        async fn field_owners(
            &self,
            _t: &Target,
            _field: &str,
        ) -> Result<Vec<FieldOwner>, ProviderError> {
            Ok(self.owners.clone())
        }
        async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
            self.applied.lock().unwrap().push(patch.clone());
            Ok(AppliedReceipt { source_hash: [0u8; 16] })
        }
    }
}
