//! `dimension-memory` — the memory [`ResourceProvider`].
//!
//! Atomic-to-category I/O for the memory dimension, and nothing else: it
//! projects working-set + limit into `(used, capacity)`, and carves the new
//! limit via **true SSA** (BREATHE.md §15.2) so the owning Deployment /
//! StatefulSet / CNPG `Cluster` performs its normal rolling update. It never
//! sees the band law — the loop owns `decide` and the directionality clamp.

use async_trait::async_trait;
use breathe_provider::{
    ApplySemantics, AssignReceipt, Cluster, DimensionId, Directionality, MetricKind, Observation,
    OwnedField, ProviderError, ReleaseReceipt, ResourceProvider, SsaPatch, Target,
};

/// The dotted field path this dimension owns (the single-writer guard input).
pub const MEMORY_FIELD: &str = "resources.limits.memory";
/// The SSA field-manager this dimension applies under — disjoint from every
/// other dimension's manager and from KEDA's, so they never fight.
pub const MEMORY_MANAGER: &str = "breathe/memory";

/// The memory provider, generic over the [`Cluster`] boundary so it is unit-
/// tested against `MockCluster` and runs against `KubeCluster` in production.
pub struct MemoryProvider<C: Cluster + 'static> {
    cluster: C,
}

impl<C: Cluster + 'static> MemoryProvider<C> {
    pub fn new(cluster: C) -> Self {
        Self { cluster }
    }

    /// Borrow the underlying cluster (handy for tests asserting applied patches).
    pub fn cluster(&self) -> &C {
        &self.cluster
    }
}

#[async_trait]
impl<C: Cluster + 'static> ResourceProvider for MemoryProvider<C> {
    fn id(&self) -> DimensionId {
        DimensionId::Memory
    }

    fn directionality(&self) -> Directionality {
        Directionality::Bidirectional
    }

    fn owned_field(&self) -> OwnedField {
        OwnedField { manager: MEMORY_MANAGER.into(), path: MEMORY_FIELD.into() }
    }

    fn semantics(&self) -> ApplySemantics {
        // SSA-patching limits.memory rolls one bounded, reversible ReplicaSet generation.
        ApplySemantics::Transactional
    }

    async fn observe(&self, target: &Target) -> Result<Observation, ProviderError> {
        let used = self.cluster.metric(target, MetricKind::Used).await?;
        let capacity = self
            .cluster
            .current_allocation(target, DimensionId::Memory)
            .await?;
        let owners = self.cluster.field_owners(target, MEMORY_FIELD).await?;
        Ok(Observation {
            used: used.value,
            capacity,
            owners,
            staleness_secs: used.age_secs,
        })
    }

    async fn assign(&self, target: &Target, to_value: u64) -> Result<AssignReceipt, ProviderError> {
        let from = self
            .cluster
            .current_allocation(target, DimensionId::Memory)
            .await?;
        if to_value == from {
            // AlreadyConverged — idempotent no-op, no patch.
            return Ok(AssignReceipt { from, to: to_value, source_hash: [0u8; 16] });
        }
        let patch = SsaPatch {
            target: target.clone(),
            field_manager: MEMORY_MANAGER.into(),
            path: MEMORY_FIELD.into(),
            value: to_value,
        };
        let applied = self.cluster.apply(&patch).await?;
        Ok(AssignReceipt { from, to: to_value, source_hash: applied.source_hash })
    }

    async fn release(&self, _target: &Target) -> Result<ReleaseReceipt, ProviderError> {
        // M0: drop the claim, no recorded baseline yet (baseline restore = M1).
        Ok(ReleaseReceipt { baseline: None, source_hash: [0u8; 16] })
    }
}
