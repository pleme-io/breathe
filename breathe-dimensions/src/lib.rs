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
    SuppressedDemand, Target,
};

/// memory/cpu live on the pod template (Deployment/StatefulSet) or top-level on a
/// CNPG `Cluster`. When `in_place` is set (and the owner is pod-backed, not a
/// CNPG Cluster), carve the live pods via the resize subresource — no restart
/// (`LimitLayout::PodResize`) — instead of the template (which rolls).
///
/// A `pod_selector` target (a label-selected pod group — ARC ephemeral runners and
/// other owner-less pod sets) is ALWAYS `PodResize`: there is no owning template to
/// roll, so the only sensible carve is in-place on the live pods. It overrides the
/// `in_place` capability flag (a selector group can only be carved this way).
fn pod_layout(target: &Target, in_place: bool) -> LimitLayout {
    if target.pod_selector.is_some() {
        return LimitLayout::PodResize { container: target.container.clone() };
    }
    match target.kind.as_str() {
        "Cluster" => LimitLayout::ClusterTopLevel,
        _ if in_place => LimitLayout::PodResize { container: target.container.clone() },
        _ => LimitLayout::PodTemplate { container: target.container.clone() },
    }
}

/// **Memory** — bidirectional; carve `limits.memory`. `used` is the live max
/// container working-set across the owner's pods (metrics-server). `in_place`
/// selects zero-restart resize (`pods/resize`, k8s ≥1.33) over a template roll.
#[derive(Default)]
pub struct MemoryDescriptor {
    /// Carve the live pods in-place (no restart) instead of rolling the template.
    pub in_place: bool,
    /// The owning `MemoryBand` CR's own `(namespace, name)` — set via
    /// [`DimensionDescriptor::set_cr_identity`] by the controller right after
    /// construction. Scopes the SSA field-manager string per-CR (task #200) so two
    /// `MemoryBand` CRs uncoordinatedly targeting the SAME object become two
    /// DIFFERENT k8s field managers, turning SSA's own conflict detection into a
    /// real backstop instead of being structurally blind behind one shared static
    /// manager string. `None` (never bound) keeps the byte-identical
    /// dimension-wide `"breathe/memory"` manager.
    cr_identity: Option<(String, String)>,
}
impl DimensionDescriptor for MemoryDescriptor {
    fn with_resize_capability(resize_capable: bool) -> Self {
        Self { in_place: resize_capable, cr_identity: None }
    }
    fn id(&self) -> DimensionId { DimensionId::Memory }
    fn directionality(&self) -> Directionality { Directionality::Bidirectional }
    fn field_manager(&self) -> &'static str { "breathe/memory" }
    fn logical_field(&self) -> &'static str { "resources.limits.memory" }
    fn resource(&self) -> &'static str { "memory" }
    fn semantics(&self) -> ApplySemantics { ApplySemantics::Transactional }
    fn layout(&self, target: &Target) -> LimitLayout { pod_layout(target, self.in_place) }
    fn metric_source(&self, target: &Target) -> MetricSource {
        MetricSource::PodMetricsMax {
            resource: "memory".into(),
            pod_prefix: target.name.clone(),
            selector: target.pod_selector.clone(),
        }
    }
    /// Memory's suppressed demand is ALREADY visible: the working set spikes ABOVE the
    /// soft `memory.high` (folding into the demonstrated peak) while the hard
    /// `memory.max` holds — so no separate throttle read is needed (the default).
    fn suppressed_demand(&self) -> SuppressedDemand {
        SuppressedDemand::WorkingSetExceedsSoftLimit
    }
    fn set_cr_identity(&mut self, namespace: String, name: String) {
        self.cr_identity = Some((namespace, name));
    }
    fn field_manager_scope(&self) -> Option<(&str, &str)> {
        self.cr_identity.as_ref().map(|(ns, name)| (ns.as_str(), name.as_str()))
    }
}

/// **CPU** — bidirectional; carve `limits.cpu` (millicores). `used` is the live
/// max container cpu across the owner's pods (metrics-server). `in_place` selects
/// zero-restart resize over a template roll (cpu-up is always restart-free).
#[derive(Default)]
pub struct CpuDescriptor {
    /// Carve the live pods in-place (no restart) instead of rolling the template.
    pub in_place: bool,
    /// The owning `CpuBand` CR's own `(namespace, name)` — set via
    /// [`DimensionDescriptor::set_cr_identity`] by the controller right after
    /// construction. Scopes the SSA field-manager string per-CR (task #200):
    /// confirmed LIVE that `pangea-operator` and `pangea-operator-cpu`, two
    /// DIFFERENT `CpuBand` CRs, both target the same Deployment via the
    /// identical static `"breathe/cpu"` manager string, so k8s's own SSA
    /// conflict detection was structurally blind to the double-writer. `None`
    /// (never bound) keeps the byte-identical dimension-wide manager.
    cr_identity: Option<(String, String)>,
}
impl DimensionDescriptor for CpuDescriptor {
    fn with_resize_capability(resize_capable: bool) -> Self {
        Self { in_place: resize_capable, cr_identity: None }
    }
    fn id(&self) -> DimensionId { DimensionId::Cpu }
    fn directionality(&self) -> Directionality { Directionality::Bidirectional }
    fn field_manager(&self) -> &'static str { "breathe/cpu" }
    fn logical_field(&self) -> &'static str { "resources.limits.cpu" }
    fn resource(&self) -> &'static str { "cpu" }
    fn semantics(&self) -> ApplySemantics { ApplySemantics::PartialProgress }
    fn layout(&self, target: &Target) -> LimitLayout { pod_layout(target, self.in_place) }
    fn metric_source(&self, target: &Target) -> MetricSource {
        MetricSource::PodMetricsMax {
            resource: "cpu".into(),
            pod_prefix: target.name.clone(),
            selector: target.pod_selector.clone(),
        }
    }
    /// CPU's `used` is HARD-CAPPED by the CFS quota — it can never exceed the limit —
    /// so its suppressed demand is non-blind ONLY via CFS THROTTLING. This declaration
    /// is what makes the CPU-blindness ratchet (the pangea-operator 2026-06 starve)
    /// structurally impossible: a CPU band MUST read its throttle signal.
    fn suppressed_demand(&self) -> SuppressedDemand {
        SuppressedDemand::CfsThrottling
    }
    /// The CFS-throttling signal — `rate(container_cpu_cfs_throttled_periods_total)`
    /// from `cAdvisor` (mirrors the storage dimension's `PromQL` path). A non-zero rate
    /// means the workload was throttled in the window — it wanted MORE CPU than its
    /// (capped) `used` shows. The reconcile layer maps the non-zero scalar onto
    /// `Observation.throttle_signal`, which lifts demand above the cap so the proven
    /// safe-min floor refuses a shrink and the band grows out of the throttle. Scoped
    /// to the same pod group the `used` metric reads (selector or owner-name prefix).
    /// (cAdvisor `container_cpu_cfs_throttled_periods_total` is the cleanest fleet
    /// source; `cpu.stat`'s `nr_throttled` is the host-agent equivalent for the
    /// cgroup-cpu host dimension.)
    fn throttle_source(&self, target: &Target) -> Option<MetricSource> {
        // ceil so any throttling at all reads as >= 1 (a small fractional rate must
        // not floor to 0 and re-hide the suppressed demand).
        let pod_match = match &target.pod_selector {
            // a label-selected pod group: match the container metric by the same labels.
            Some(sel) => sel
                .split(',')
                .filter(|kv| !kv.is_empty())
                .map(|kv| {
                    let mut it = kv.splitn(2, '=');
                    let k = it.next().unwrap_or("").trim();
                    let v = it.next().unwrap_or("").trim();
                    format!(r#"{k}="{v}""#)
                })
                .collect::<Vec<_>>()
                .join(","),
            // owner-resolved: cAdvisor labels carry the pod name; match by prefix.
            None => format!(r#"pod=~"{name}.*""#, name = target.name),
        };
        let sel = if pod_match.is_empty() {
            format!(r#"namespace="{ns}""#, ns = target.namespace)
        } else {
            format!(r#"namespace="{ns}",{pod_match}"#, ns = target.namespace)
        };
        Some(MetricSource::Prometheus(format!(
            r#"ceil(sum(rate(container_cpu_cfs_throttled_periods_total{{{sel},container!=""}}[2m])))"#
        )))
    }
    fn set_cr_identity(&mut self, namespace: String, name: String) {
        self.cr_identity = Some((namespace, name));
    }
    fn field_manager_scope(&self) -> Option<(&str, &str)> {
        self.cr_identity.as_ref().map(|(ns, name)| (ns.as_str(), name.as_str()))
    }
}

/// **Storage** — grow-only (data persists); grow PVC `requests.storage` (CSI
/// online-resize). `used` is volume stats via PromQL (no metrics-server analog).
/// The shrink path is mechanically disabled by the core's directionality clamp.
#[derive(Default)]
pub struct StorageDescriptor {
    /// The owning `StorageBand` CR's own `(namespace, name)` — set via
    /// [`DimensionDescriptor::set_cr_identity`] by the controller right after
    /// construction. Scopes the SSA field-manager string per-CR (task #200's
    /// class): two `StorageBand` CRs targeting the same PVC/CNPG `Cluster` become
    /// two DIFFERENT k8s field managers instead of sharing one static
    /// `"breathe/storage"` manager SSA's conflict detection can't see through.
    /// `None` (never bound) keeps the byte-identical dimension-wide manager.
    cr_identity: Option<(String, String)>,
}
impl DimensionDescriptor for StorageDescriptor {
    fn id(&self) -> DimensionId { DimensionId::Storage }
    fn directionality(&self) -> Directionality { Directionality::GrowOnly }
    fn field_manager(&self) -> &'static str { "breathe/storage" }
    fn logical_field(&self) -> &'static str { "spec.resources.requests.storage" }
    fn resource(&self) -> &'static str { "storage" }
    fn semantics(&self) -> ApplySemantics { ApplySemantics::ContinuousReconciliation }
    fn layout(&self, target: &Target) -> LimitLayout {
        match target.kind.as_str() {
            "Cluster" => LimitLayout::ClusterStorage,
            _ => LimitLayout::PvcRequest,
        }
    }
    fn metric_source(&self, target: &Target) -> MetricSource {
        let pvc_sel = if target.kind == "Cluster" {
            format!(r#"persistentvolumeclaim=~"{name}-[0-9]+""#, name = target.name)
        } else {
            format!(r#"persistentvolumeclaim="{name}""#, name = target.name)
        };
        MetricSource::Prometheus(format!(
            r#"max(kubelet_volume_stats_used_bytes{{namespace="{ns}",{pvc_sel}}})"#,
            ns = target.namespace
        ))
    }
    /// Storage is grow-only — there is no shrink to ratchet, so suppressed demand is a
    /// non-issue by construction (the down-cliff is unrepresentable). No throttle read.
    fn suppressed_demand(&self) -> SuppressedDemand {
        SuppressedDemand::GrowOnly
    }
    fn set_cr_identity(&mut self, namespace: String, name: String) {
        self.cr_identity = Some((namespace, name));
    }
    fn field_manager_scope(&self) -> Option<(&str, &str)> {
        self.cr_identity.as_ref().map(|(ns, name)| (ns.as_str(), name.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cnpg() -> Target {
        Target { namespace: "pangea-system".into(), name: "pangea-database".into(), kind: "Cluster".into(), api_version: "postgresql.cnpg.io/v1".into(), container: None, pod_selector: None }
    }
    fn deploy() -> Target {
        Target { namespace: "x".into(), name: "app".into(), kind: "Deployment".into(), api_version: "apps/v1".into(), container: Some("app".into()), pod_selector: None }
    }
    /// An ARC ephemeral-runner pod group: no resolvable owner, name unstable —
    /// addressed by a label selector. Carved in-place regardless of `in_place`.
    fn runner() -> Target {
        Target {
            namespace: "arc-rio-default".into(),
            name: "rio-build-01".into(),
            kind: "EphemeralRunner".into(),
            api_version: "actions.github.com/v1alpha1".into(),
            container: Some("runner".into()),
            pod_selector: Some("actions.github.com/scale-set-name=rio-build-01".into()),
        }
    }

    #[test]
    fn memory_reads_always_on_metrics_server() {
        let d = MemoryDescriptor::default();
        assert_eq!(d.id(), DimensionId::Memory);
        assert_eq!(d.directionality(), Directionality::Bidirectional);
        assert_eq!(d.layout(&cnpg()), LimitLayout::ClusterTopLevel);
        assert!(matches!(d.layout(&deploy()), LimitLayout::PodTemplate { .. }));
        match d.metric_source(&cnpg()) {
            MetricSource::PodMetricsMax { resource, pod_prefix, selector } => {
                assert_eq!(resource, "memory");
                assert_eq!(pod_prefix, "pangea-database");
                assert_eq!(selector, None, "owner-resolved target carries no selector");
            }
            other => panic!("memory must read metrics-server, got {other:?}"),
        }
    }

    #[test]
    fn ephemeral_runner_carves_in_place_by_selector() {
        // The label-selected pod-group carve: an ARC runner has no owning template
        // to roll, so memory/cpu ALWAYS go PodResize (in-place, zero restart) — even
        // with in_place=false — and the metric reads the SAME label selector.
        let r = runner();
        for d in [
            MemoryDescriptor { in_place: false, ..Default::default() },
            MemoryDescriptor { in_place: true, ..Default::default() },
        ] {
            assert!(matches!(d.layout(&r), LimitLayout::PodResize { container: Some(c) } if c == "runner"));
            match d.metric_source(&r) {
                MetricSource::PodMetricsMax { resource, pod_prefix, selector } => {
                    assert_eq!(resource, "memory");
                    assert_eq!(pod_prefix, "rio-build-01");
                    assert_eq!(selector.as_deref(), Some("actions.github.com/scale-set-name=rio-build-01"));
                }
                other => panic!("runner memory must read metrics-server by selector, got {other:?}"),
            }
        }
        // a selector carve is never RestartRequiring (it cannot force a roll).
        use breathe_provider::DisruptionClass;
        assert_ne!(
            MemoryDescriptor::default().layout(&r).disruption_class(),
            DisruptionClass::RestartRequiring
        );
        assert!(matches!(CpuDescriptor::default().layout(&r), LimitLayout::PodResize { .. }));
    }

    #[test]
    fn cpu_reads_metrics_server_bidirectional() {
        let d = CpuDescriptor::default();
        assert_eq!(d.directionality(), Directionality::Bidirectional);
        assert!(matches!(d.metric_source(&deploy()), MetricSource::PodMetricsMax { .. }));
    }

    /// THE CPU-BLINDNESS DESCRIPTOR CONTRACT: cpu declares `CfsThrottling` and
    /// supplies a CFS-throttling `PromQL` throttle source (the non-blind signal),
    /// whereas memory (over-soft-limit spike) and storage (grow-only) declare their
    /// own variants and supply NO throttle source. This is the descriptor-level half
    /// of the structural fix — a cpu band cannot exist without its throttle read.
    #[test]
    fn cpu_declares_cfs_throttling_with_a_throttle_source() {
        use breathe_provider::SuppressedDemand;
        let d = CpuDescriptor::default();
        assert_eq!(d.suppressed_demand(), SuppressedDemand::CfsThrottling);
        match d.throttle_source(&deploy()) {
            Some(MetricSource::Prometheus(q)) => {
                assert!(q.contains("container_cpu_cfs_throttled_periods_total"), "must read CFS throttling: {q}");
                assert!(q.contains("rate("), "a throttle RATE over a window: {q}");
                assert!(q.contains(r#"namespace="x""#), "scoped to the target namespace: {q}");
            }
            other => panic!("cpu must supply a CFS-throttling PromQL source, got {other:?}"),
        }
        // a label-selected (ARC runner) cpu band scopes the throttle query by the SAME labels.
        match d.throttle_source(&runner()) {
            Some(MetricSource::Prometheus(q)) => {
                assert!(q.contains(r#"actions.github.com/scale-set-name="rio-build-01""#), "selector-scoped throttle: {q}");
            }
            other => panic!("runner cpu throttle source must be label-scoped, got {other:?}"),
        }
        // memory + storage declare their own variants and carry NO throttle read.
        assert_eq!(MemoryDescriptor::default().suppressed_demand(), SuppressedDemand::WorkingSetExceedsSoftLimit);
        assert!(MemoryDescriptor::default().throttle_source(&deploy()).is_none(), "memory has no separate throttle read");
        assert_eq!(StorageDescriptor::default().suppressed_demand(), SuppressedDemand::GrowOnly);
        assert!(StorageDescriptor::default().throttle_source(&deploy()).is_none(), "storage is grow-only — no throttle read");
    }

    #[test]
    fn no_mutating_dimension_forces_a_roll_for_pod_backed_owners() {
        // The keystone's convergence: memory + cpu carve via PodResize
        // (RestartConditional — never a forced roll) and storage online-expands
        // (PvcRequest — RestartFree) — so NO mutating k8s dimension must ROLL a
        // pod-backed workload to be held at the band. (CNPG `Cluster` owners are
        // RestartRequiring — a documented gap, not a regression.)
        use breathe_provider::DisruptionClass;
        let t = deploy();
        for layout in [
            MemoryDescriptor { in_place: true, ..Default::default() }.layout(&t),
            CpuDescriptor { in_place: true, ..Default::default() }.layout(&t),
            StorageDescriptor::default().layout(&t),
        ] {
            assert_ne!(layout.disruption_class(), DisruptionClass::RestartRequiring);
        }
        // storage is the strongest: fully RestartFree.
        assert_eq!(StorageDescriptor::default().layout(&t).disruption_class(), DisruptionClass::RestartFree);
    }

    #[test]
    fn with_resize_capability_makes_zero_disruption_the_default() {
        // K1 "breathe never rolls": memory/cpu prefer in-place when the cluster
        // supports pods/resize, and roll only when it does not.
        assert!(MemoryDescriptor::with_resize_capability(true).in_place);
        assert!(!MemoryDescriptor::with_resize_capability(false).in_place);
        assert!(CpuDescriptor::with_resize_capability(true).in_place);
        // storage ignores it (already zero-disruption) — the default ctor.
        let _ = StorageDescriptor::with_resize_capability(true);
    }

    #[test]
    fn in_place_carves_pods_via_resize_not_template() {
        // in_place memory on a Deployment → PodResize (zero-restart); a CNPG
        // Cluster still goes top-level (CNPG owns its own resize). Default
        // (in_place: false) keeps the template-roll behaviour unchanged.
        let inplace = MemoryDescriptor { in_place: true, ..Default::default() };
        assert!(matches!(inplace.layout(&deploy()), LimitLayout::PodResize { .. }));
        assert_eq!(inplace.layout(&cnpg()), LimitLayout::ClusterTopLevel);
        assert!(matches!(MemoryDescriptor::default().layout(&deploy()), LimitLayout::PodTemplate { .. }));
        // cpu likewise.
        assert!(matches!(CpuDescriptor { in_place: true, ..Default::default() }.layout(&deploy()), LimitLayout::PodResize { .. }));
    }

    #[test]
    fn storage_is_grow_only_kind_aware_promql() {
        let d = StorageDescriptor::default();
        assert_eq!(d.directionality(), Directionality::GrowOnly);
        // A raw PVC target carves its own requests.storage, keyed on its exact name.
        let pvc = Target { namespace: "drive".into(), name: "data-garage-0".into(), kind: "PersistentVolumeClaim".into(), api_version: "v1".into(), container: None, pod_selector: None };
        assert_eq!(d.layout(&pvc), LimitLayout::PvcRequest);
        match d.metric_source(&pvc) {
            MetricSource::Prometheus(q) => {
                assert!(q.contains("kubelet_volume_stats_used_bytes"));
                assert!(q.contains(r#"persistentvolumeclaim="data-garage-0""#), "exact PVC name: {q}");
            }
            other => panic!("storage uses PromQL, got {other:?}"),
        }
        // A CNPG `Cluster` owns the raw PVC; breathe carves spec.storage.size and
        // aggregates the volume stats across the cluster's instance PVCs (<name>-N).
        assert_eq!(d.layout(&cnpg()), LimitLayout::ClusterStorage);
        match d.metric_source(&cnpg()) {
            MetricSource::Prometheus(q) => {
                assert!(q.contains(r#"persistentvolumeclaim=~"pangea-database-[0-9]+""#), "regex over instance PVCs: {q}");
            }
            other => panic!("storage uses PromQL, got {other:?}"),
        }
    }

    /// TASK #200 — the field-manager double-writer fix: a descriptor never bound
    /// to a CR identity keeps the byte-identical dimension-wide manager string
    /// (`field_manager_scope() == None`); once bound, two DIFFERENT CRs of the
    /// SAME dimension produce two DIFFERENT scopes. Confirmed LIVE for
    /// `CpuBand`: `pangea-operator` and `pangea-operator-cpu` both target the
    /// same Deployment through the identical static `"breathe/cpu"` manager, so
    /// k8s's own SSA conflict detection was structurally blind to the
    /// double-writer — this is what makes it a real backstop instead.
    #[test]
    fn cr_identity_scopes_the_field_manager_only_when_bound_and_differs_per_cr() {
        let unbound = CpuDescriptor::default();
        assert_eq!(unbound.field_manager_scope(), None, "never bound ⇒ dimension-wide manager, unchanged");

        let mut a = CpuDescriptor::default();
        a.set_cr_identity("rio".into(), "pangea-operator".into());
        let mut b = CpuDescriptor::default();
        b.set_cr_identity("rio".into(), "pangea-operator-cpu".into());
        assert_eq!(a.field_manager_scope(), Some(("rio", "pangea-operator")));
        assert_eq!(b.field_manager_scope(), Some(("rio", "pangea-operator-cpu")));
        assert_ne!(a.field_manager_scope(), b.field_manager_scope(), "two CRs of the same dimension must scope DIFFERENTLY");

        // the SAME mechanism, wired on every dimension prone to the class (not just cpu).
        let mut m = MemoryDescriptor::default();
        m.set_cr_identity("rio".into(), "my-memory-band".into());
        assert_eq!(m.field_manager_scope(), Some(("rio", "my-memory-band")));

        let mut s = StorageDescriptor::default();
        s.set_cr_identity("rio".into(), "my-storage-band".into());
        assert_eq!(s.field_manager_scope(), Some(("rio", "my-storage-band")));
    }
}
