//! `breathe-dimensions` — the concrete [`DimensionDescriptor`]s.
//!
//! Each descriptor is the *small, genuinely dimension-specific* data: the
//! metric source, the owned field, the directionality, the owner layout. The
//! observe/assign/release orchestration is solved once in
//! [`breathe_provider::BandProvider`]. Adding a dimension = a descriptor here +
//! a catalog row in `breathe-catalog`; the controller code never grows.
//!
//! memory + cpu read `used` from the **always-on metrics-server**
//! (`MetricSource::PodMetricsMax`) so breathe never depends on a scale-to-zero
//! TSDB; storage reads volume stats via PromQL.

use breathe_provider::{
    ApplySemantics, DimensionDescriptor, DimensionId, Directionality, LimitLayout, MetricSource,
    Target,
};

/// memory/cpu live on the pod template (Deployment/StatefulSet) or top-level
/// on a CNPG `Cluster`.
fn pod_or_cluster(target: &Target) -> LimitLayout {
    match target.kind.as_str() {
        "Cluster" => LimitLayout::ClusterTopLevel,
        _ => LimitLayout::PodTemplate { container: target.container.clone() },
    }
}

/// **Memory** — bidirectional; carve `limits.memory`; the owner rolls. `used` is
/// the live max container working-set across the owner's pods (metrics-server).
#[derive(Default)]
pub struct MemoryDescriptor;
impl DimensionDescriptor for MemoryDescriptor {
    fn id(&self) -> DimensionId { DimensionId::Memory }
    fn directionality(&self) -> Directionality { Directionality::Bidirectional }
    fn field_manager(&self) -> &'static str { "breathe/memory" }
    fn logical_field(&self) -> &'static str { "resources.limits.memory" }
    fn resource(&self) -> &'static str { "memory" }
    fn semantics(&self) -> ApplySemantics { ApplySemantics::Transactional }
    fn layout(&self, target: &Target) -> LimitLayout { pod_or_cluster(target) }
    fn metric_source(&self, target: &Target) -> MetricSource {
        MetricSource::PodMetricsMax { resource: "memory".into(), pod_prefix: target.name.clone() }
    }
}

/// **CPU** — bidirectional; carve `limits.cpu` (millicores); in-place or roll.
/// `used` is the live max container cpu across the owner's pods (metrics-server).
#[derive(Default)]
pub struct CpuDescriptor;
impl DimensionDescriptor for CpuDescriptor {
    fn id(&self) -> DimensionId { DimensionId::Cpu }
    fn directionality(&self) -> Directionality { Directionality::Bidirectional }
    fn field_manager(&self) -> &'static str { "breathe/cpu" }
    fn logical_field(&self) -> &'static str { "resources.limits.cpu" }
    fn resource(&self) -> &'static str { "cpu" }
    fn semantics(&self) -> ApplySemantics { ApplySemantics::PartialProgress }
    fn layout(&self, target: &Target) -> LimitLayout { pod_or_cluster(target) }
    fn metric_source(&self, target: &Target) -> MetricSource {
        MetricSource::PodMetricsMax { resource: "cpu".into(), pod_prefix: target.name.clone() }
    }
}

/// **Storage** — grow-only (data persists); grow PVC `requests.storage` (CSI
/// online-resize). `used` is volume stats via PromQL (no metrics-server analog).
/// The shrink path is mechanically disabled by the core's directionality clamp.
#[derive(Default)]
pub struct StorageDescriptor;
impl DimensionDescriptor for StorageDescriptor {
    fn id(&self) -> DimensionId { DimensionId::Storage }
    fn directionality(&self) -> Directionality { Directionality::GrowOnly }
    fn field_manager(&self) -> &'static str { "breathe/storage" }
    fn logical_field(&self) -> &'static str { "spec.resources.requests.storage" }
    fn resource(&self) -> &'static str { "storage" }
    fn semantics(&self) -> ApplySemantics { ApplySemantics::ContinuousReconciliation }
    fn layout(&self, _target: &Target) -> LimitLayout { LimitLayout::PvcRequest }
    fn metric_source(&self, target: &Target) -> MetricSource {
        MetricSource::Prometheus(format!(
            r#"max(kubelet_volume_stats_used_bytes{{namespace="{ns}",persistentvolumeclaim="{name}"}})"#,
            ns = target.namespace,
            name = target.name
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cnpg() -> Target {
        Target { namespace: "pangea-system".into(), name: "pangea-database".into(), kind: "Cluster".into(), api_version: "postgresql.cnpg.io/v1".into(), container: None }
    }
    fn deploy() -> Target {
        Target { namespace: "x".into(), name: "app".into(), kind: "Deployment".into(), api_version: "apps/v1".into(), container: Some("app".into()) }
    }

    #[test]
    fn memory_reads_always_on_metrics_server() {
        let d = MemoryDescriptor;
        assert_eq!(d.id(), DimensionId::Memory);
        assert_eq!(d.directionality(), Directionality::Bidirectional);
        assert_eq!(d.layout(&cnpg()), LimitLayout::ClusterTopLevel);
        assert!(matches!(d.layout(&deploy()), LimitLayout::PodTemplate { .. }));
        match d.metric_source(&cnpg()) {
            MetricSource::PodMetricsMax { resource, pod_prefix } => {
                assert_eq!(resource, "memory");
                assert_eq!(pod_prefix, "pangea-database");
            }
            other => panic!("memory must read metrics-server, got {other:?}"),
        }
    }

    #[test]
    fn cpu_reads_metrics_server_bidirectional() {
        let d = CpuDescriptor;
        assert_eq!(d.directionality(), Directionality::Bidirectional);
        assert!(matches!(d.metric_source(&deploy()), MetricSource::PodMetricsMax { .. }));
    }

    #[test]
    fn storage_is_grow_only_pvc_promql() {
        let d = StorageDescriptor;
        assert_eq!(d.directionality(), Directionality::GrowOnly);
        assert_eq!(d.layout(&cnpg()), LimitLayout::PvcRequest);
        match d.metric_source(&cnpg()) {
            MetricSource::Prometheus(q) => assert!(q.contains("kubelet_volume_stats_used_bytes")),
            other => panic!("storage uses PromQL, got {other:?}"),
        }
    }
}
