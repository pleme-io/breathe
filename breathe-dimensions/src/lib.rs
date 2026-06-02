//! `breathe-dimensions` — the concrete [`DimensionDescriptor`]s.
//!
//! Each descriptor is the *small, genuinely dimension-specific* data: the
//! metric query, the owned field, the directionality, the owner layout. The
//! observe/assign/release orchestration is solved once in
//! [`breathe_provider::BandProvider`]. Adding a dimension = a descriptor here +
//! a catalog row in `breathe-catalog`; the controller code never grows.

use breathe_provider::{
    ApplySemantics, DimensionDescriptor, DimensionId, Directionality, LimitLayout, Target,
};

/// memory/cpu live on the pod template (Deployment/StatefulSet) or top-level
/// on a CNPG `Cluster`.
fn pod_or_cluster(target: &Target) -> LimitLayout {
    match target.kind.as_str() {
        "Cluster" => LimitLayout::ClusterTopLevel,
        _ => LimitLayout::PodTemplate { container: target.container.clone() },
    }
}

/// Working-set / cpu-rate PromQL scoped to the owner's pods.
fn pod_scoped(metric_expr: &str, target: &Target) -> String {
    format!(
        r#"max({metric_expr}{{namespace="{ns}",pod=~"{name}.*",container!="",container!="POD"}})"#,
        ns = target.namespace,
        name = target.name
    )
}

/// **Memory** — bidirectional; carve `limits.memory`; the owner rolls.
pub struct MemoryDescriptor;
impl DimensionDescriptor for MemoryDescriptor {
    fn id(&self) -> DimensionId { DimensionId::Memory }
    fn directionality(&self) -> Directionality { Directionality::Bidirectional }
    fn field_manager(&self) -> &'static str { "breathe/memory" }
    fn logical_field(&self) -> &'static str { "resources.limits.memory" }
    fn resource(&self) -> &'static str { "memory" }
    fn semantics(&self) -> ApplySemantics { ApplySemantics::Transactional }
    fn layout(&self, target: &Target) -> LimitLayout { pod_or_cluster(target) }
    fn used_promql(&self, target: &Target) -> String {
        pod_scoped("container_memory_working_set_bytes", target)
    }
}

/// **CPU** — bidirectional; carve `limits.cpu` (millicores); in-place or roll.
pub struct CpuDescriptor;
impl DimensionDescriptor for CpuDescriptor {
    fn id(&self) -> DimensionId { DimensionId::Cpu }
    fn directionality(&self) -> Directionality { Directionality::Bidirectional }
    fn field_manager(&self) -> &'static str { "breathe/cpu" }
    fn logical_field(&self) -> &'static str { "resources.limits.cpu" }
    fn resource(&self) -> &'static str { "cpu" }
    fn semantics(&self) -> ApplySemantics { ApplySemantics::PartialProgress }
    fn layout(&self, target: &Target) -> LimitLayout { pod_or_cluster(target) }
    fn used_promql(&self, target: &Target) -> String {
        // millicores: rate(cpu seconds) * 1000.
        format!(
            r#"max(rate(container_cpu_usage_seconds_total{{namespace="{ns}",pod=~"{name}.*",container!="",container!="POD"}}[5m]))*1000"#,
            ns = target.namespace,
            name = target.name
        )
    }
}

/// **Storage** — grow-only (data persists); grow PVC `requests.storage` (CSI
/// online-resize). The shrink path is mechanically disabled by the core's
/// directionality clamp — zero storage-specific control code.
pub struct StorageDescriptor;
impl DimensionDescriptor for StorageDescriptor {
    fn id(&self) -> DimensionId { DimensionId::Storage }
    fn directionality(&self) -> Directionality { Directionality::GrowOnly }
    fn field_manager(&self) -> &'static str { "breathe/storage" }
    fn logical_field(&self) -> &'static str { "spec.resources.requests.storage" }
    fn resource(&self) -> &'static str { "storage" }
    fn semantics(&self) -> ApplySemantics { ApplySemantics::ContinuousReconciliation }
    fn layout(&self, _target: &Target) -> LimitLayout { LimitLayout::PvcRequest }
    fn used_promql(&self, target: &Target) -> String {
        format!(
            r#"max(kubelet_volume_stats_used_bytes{{namespace="{ns}",persistentvolumeclaim="{name}"}})"#,
            ns = target.namespace,
            name = target.name
        )
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
    fn memory_descriptor_shape() {
        let d = MemoryDescriptor;
        assert_eq!(d.id(), DimensionId::Memory);
        assert_eq!(d.directionality(), Directionality::Bidirectional);
        assert_eq!(d.field_manager(), "breathe/memory");
        // layout follows the owner kind
        assert_eq!(d.layout(&cnpg()), LimitLayout::ClusterTopLevel);
        assert!(matches!(d.layout(&deploy()), LimitLayout::PodTemplate { .. }));
        assert!(d.used_promql(&cnpg()).contains("container_memory_working_set_bytes"));
    }

    #[test]
    fn cpu_descriptor_is_millicores_and_bidirectional() {
        let d = CpuDescriptor;
        assert_eq!(d.directionality(), Directionality::Bidirectional);
        assert_eq!(d.resource(), "cpu");
        let q = d.used_promql(&deploy());
        assert!(q.contains("container_cpu_usage_seconds_total") && q.contains("*1000"));
    }

    #[test]
    fn storage_is_grow_only_and_pvc() {
        let d = StorageDescriptor;
        assert_eq!(d.directionality(), Directionality::GrowOnly);
        // storage alway uses the PVC layout, regardless of the target kind
        assert_eq!(d.layout(&cnpg()), LimitLayout::PvcRequest);
        assert!(d.used_promql(&cnpg()).contains("kubelet_volume_stats_used_bytes"));
    }
}
