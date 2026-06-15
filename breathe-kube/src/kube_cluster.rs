//! `KubeCluster` — the real [`Cluster`] implementation over kube-rs.
//!
//! Dimension-agnostic I/O: `query` runs raw PromQL (with sample age),
//! `read_limit` reads a quantity at a [`LimitLayout`], `field_owners` extracts
//! ownership of the layout's fieldsV1 path (resolving the container name from
//! the live object), `apply` performs **true SSA** (`Patch::Apply` + force).
//! The layout interpretation — CNPG `Cluster` top-level, pod-template, PVC — is
//! the only K8s-specific branching, and it lives here, not in the descriptors.

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use breathe_control::{FieldOwner, Quantity, Unit};
use breathe_provider::{
    AppliedReceipt, Cluster, LimitLayout, MetricSource, ProviderError, Sample, SsaPatch, Target,
};
use kube::{
    api::{Api, ApiResource, DynamicObject, ListParams, Patch, PatchParams},
    core::GroupVersionKind,
    Client,
};
use serde_json::{json, Value};

use crate::managed_fields::{
    cnpg_cluster_limit_segments, cnpg_storage_segments, field_owners, pod_template_limit_segments, pvc_request_segments,
};

pub struct KubeCluster {
    client: Client,
    prometheus_url: String,
    http: reqwest::Client,
}

impl KubeCluster {
    #[must_use]
    pub fn new(client: Client, prometheus_url: String) -> Self {
        Self { client, prometheus_url, http: reqwest::Client::new() }
    }

    fn group_version(target: &Target) -> (String, String) {
        if !target.api_version.is_empty() {
            return match target.api_version.split_once('/') {
                Some((g, v)) => (g.to_string(), v.to_string()),
                None => (String::new(), target.api_version.clone()),
            };
        }
        match target.kind.as_str() {
            "Cluster" => ("postgresql.cnpg.io".into(), "v1".into()),
            "PersistentVolumeClaim" => (String::new(), "v1".into()),
            _ => ("apps".into(), "v1".into()),
        }
    }

    fn api_for(&self, target: &Target) -> Api<DynamicObject> {
        let (g, v) = Self::group_version(target);
        let gvk = GroupVersionKind::gvk(&g, &v, &target.kind);
        let ar = ApiResource::from_gvk(&gvk);
        Api::namespaced_with(self.client.clone(), &target.namespace, &ar)
    }

    async fn get_owner(&self, target: &Target) -> Result<DynamicObject, ProviderError> {
        self.api_for(target).get(&target.name).await.map_err(|e| match e {
            kube::Error::Api(ae) if ae.code == 404 => ProviderError::TargetNotFound,
            other => ProviderError::ApiTransient(other.to_string()),
        })
    }

    /// Resolve the managed container name for a pod-template layout (the given
    /// name, or the first container in the live object).
    fn container_name(data: &Value, want: &Option<String>) -> Option<String> {
        want.clone().or_else(|| {
            data.pointer("/spec/template/spec/containers/0/name")
                .and_then(Value::as_str)
                .map(String::from)
        })
    }

    /// JSON pointer to the quantity for a layout+resource within a fetched object.
    fn read_qty(data: &Value, layout: &LimitLayout, resource: &str) -> Option<String> {
        match layout {
            LimitLayout::ClusterTopLevel => data
                .pointer(&format!("/spec/resources/limits/{resource}"))
                .and_then(Value::as_str)
                .map(String::from),
            LimitLayout::PvcRequest => data
                .pointer("/spec/resources/requests/storage")
                .and_then(Value::as_str)
                .map(String::from),
            LimitLayout::ClusterStorage => data
                .pointer("/spec/storage/size")
                .and_then(Value::as_str)
                .map(String::from),
            LimitLayout::PodTemplate { container } => {
                let containers = data.pointer("/spec/template/spec/containers")?.as_array()?;
                let c = match container {
                    Some(name) => containers
                        .iter()
                        .find(|c| c.get("name").and_then(Value::as_str) == Some(name.as_str()))?,
                    None => containers.first()?,
                };
                c.pointer(&format!("/resources/limits/{resource}")).and_then(Value::as_str).map(String::from)
            }
            // PodResize reads from the live pods (handled in read_limit), not the
            // fetched owner object — so there is nothing to read here.
            LimitLayout::PodResize { .. } => None,
            // No k8s object holds a host lever — handled (rejected) in read_limit.
            LimitLayout::Host(_) => None,
        }
    }

    /// A pod's container quantity at `kind` (`limits`/`requests`) for `resource`.
    fn pod_container_qty(pod_data: &Value, container: &Option<String>, kind: &str, resource: &str) -> Option<String> {
        let containers = pod_data.pointer("/spec/containers")?.as_array()?;
        let c = match container {
            Some(name) => containers.iter().find(|c| c.get("name").and_then(Value::as_str) == Some(name.as_str()))?,
            None => containers.first()?,
        };
        c.pointer(&format!("/resources/{kind}/{resource}")).and_then(Value::as_str).map(String::from)
    }

    /// The first container name on a pod (when the band names none).
    fn pod_first_container(pod_data: &Value) -> Option<String> {
        pod_data.pointer("/spec/containers/0/name").and_then(Value::as_str).map(String::from)
    }

    /// True iff the pod's managed container declares `resizePolicy[<resource>] =
    /// NotRequired` — the kubelet then resizes that resource in place WITHOUT
    /// restarting the container. Absent policy ⇒ false (k8s defaults to
    /// `RestartContainer`); a missing container/spec ⇒ false. This is the live fact
    /// that turns a memory shrink from `RestartConditional` into `RestartFree`.
    fn container_resize_not_required(pod_data: &Value, container: &Option<String>, resource: &str) -> bool {
        let Some(containers) = pod_data.pointer("/spec/containers").and_then(Value::as_array) else {
            return false;
        };
        let c = match container {
            Some(name) => containers.iter().find(|c| c.get("name").and_then(Value::as_str) == Some(name.as_str())),
            None => containers.first(),
        };
        let Some(policies) = c.and_then(|c| c.pointer("/resizePolicy")).and_then(Value::as_array) else {
            return false;
        };
        policies.iter().any(|p| {
            p.get("resourceName").and_then(Value::as_str) == Some(resource)
                && p.get("restartPolicy").and_then(Value::as_str) == Some("NotRequired")
        })
    }

    /// Build a label selector (`k=v,k2=v2`) from an owner's `spec.selector.matchLabels`.
    fn owner_pod_selector(owner_data: &Value) -> Option<String> {
        let ml = owner_data.pointer("/spec/selector/matchLabels")?.as_object()?;
        let sel = ml.iter().filter_map(|(k, v)| v.as_str().map(|v| format!("{k}={v}"))).collect::<Vec<_>>().join(",");
        (!sel.is_empty()).then_some(sel)
    }

    /// List the live pods a band manages in `target.namespace`. Two resolution
    /// modes: a `target.pod_selector` (the **label-selected pod group** — ARC
    /// ephemeral runners and other owner-less pod sets) lists pods directly by that
    /// label selector; otherwise the owner is fetched and its
    /// `spec.selector.matchLabels` drives the list (Deployment/StatefulSet/CNPG).
    /// Both are scoped to `target.namespace` and return live `Pod` objects to carve.
    async fn owner_pods(&self, target: &Target) -> Result<Vec<DynamicObject>, ProviderError> {
        let sel = match &target.pod_selector {
            Some(s) => s.clone(),
            None => {
                let owner = self.get_owner(target).await?;
                Self::owner_pod_selector(&owner.data).ok_or(ProviderError::NoCapacityField)?
            }
        };
        let gvk = GroupVersionKind::gvk("", "v1", "Pod");
        let ar = ApiResource::from_gvk(&gvk);
        let api: Api<DynamicObject> = Api::namespaced_with(self.client.clone(), &target.namespace, &ar);
        let pods = api
            .list(&ListParams::default().labels(&sel))
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        Ok(pods.items)
    }

    /// Prometheus instant query → (value, sample age).
    async fn prometheus_used(&self, promql: &str) -> Result<Sample, ProviderError> {
        let url = format!("{}/api/v1/query", self.prometheus_url.trim_end_matches('/'));
        let resp: Value = self
            .http
            .get(&url)
            .query(&[("query", promql)])
            .send()
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?
            .json()
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        let pair = resp
            .pointer("/data/result/0/value")
            .and_then(Value::as_array)
            .ok_or(ProviderError::MetricsMissing)?;
        let ts = pair.first().and_then(Value::as_f64).ok_or(ProviderError::MetricsMissing)?;
        let value: u64 = pair
            .get(1)
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<f64>().ok())
            .map(|f| f as u64)
            .ok_or(ProviderError::MetricsMissing)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(ts);
        Ok(Sample { value, age_secs: (now - ts).max(0.0) as u64 })
    }

    /// The ALWAYS-ON metric source: read live container usage from metrics-server
    /// (`metrics.k8s.io` PodMetrics) — what `kubectl top` shows. Returns the MAX
    /// `resource` (memory bytes / cpu millicores) across the band's pod group, so
    /// the band holds the hottest instance at the setpoint. Independent of any TSDB.
    /// The group is `selector`-matched (the label-selected carve — PodMetrics mirror
    /// their pod's labels, so the same selector that resolves the carve resolves the
    /// metric) when set, else the pods whose name starts with `pod_prefix`.
    async fn pod_metrics_max(
        &self,
        resource: &str,
        pod_prefix: &str,
        selector: Option<&str>,
    ) -> Result<Sample, ProviderError> {
        let gvk = GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics");
        let ar = ApiResource::from_gvk_with_plural(&gvk, "pods");
        // metrics-server is cluster-scoped reads; a selector filters server-side
        // (PodMetrics carry the pod labels), a prefix filters client-side by name.
        let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &ar);
        let lp = match selector {
            Some(s) => ListParams::default().labels(s),
            None => ListParams::default(),
        };
        let list = api
            .list(&lp)
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        let mut max: u64 = 0;
        let mut found = false;
        for pm in &list.items {
            // selector path: the server already filtered; prefix path: match by name.
            if selector.is_none() {
                let name = pm.metadata.name.as_deref().unwrap_or("");
                if !name.starts_with(pod_prefix) {
                    continue;
                }
            }
            let Some(containers) = pm.data.pointer("/containers").and_then(Value::as_array) else {
                continue;
            };
            for c in containers {
                if let Some(raw) = c.pointer(&format!("/usage/{resource}")).and_then(Value::as_str) {
                    let v = Unit::for_resource(resource).parse(raw);
                    if let Some(v) = v {
                        found = true;
                        max = max.max(v);
                    }
                }
            }
        }
        if !found {
            return Err(ProviderError::MetricsMissing);
        }
        // metrics-server samples are recent (scrape window ~15-30s); treat as fresh.
        Ok(Sample { value: max, age_secs: 0 })
    }
}

/// The QoS-preserving `resources` block for an in-place pod resize. A Guaranteed
/// pod (requests == limits) keeps requests == limits so it STAYS Guaranteed
/// (both grow and shrink); a Burstable/BestEffort pod sets the limit and clamps
/// its request DOWN to the new limit only if the old request would now exceed it
/// (k8s rejects request > limit) — otherwise the request is left untouched.
/// Pure + unit-tested; the actuator's only QoS-relevant decision lives here.
fn resize_resources_block(qos: &str, resource: &str, value: u64, current_request: Option<&str>) -> Value {
    let unit = Unit::for_resource(resource);
    let qty = Quantity { value, unit }.to_string();
    if qos == "Guaranteed" {
        return json!({ "limits": { resource: qty.clone() }, "requests": { resource: qty } });
    }
    match current_request.and_then(|r| unit.parse(r)) {
        Some(req) if req > value => json!({ "limits": { resource: qty.clone() }, "requests": { resource: qty } }),
        _ => json!({ "limits": { resource: qty } }),
    }
}

#[async_trait]
impl Cluster for KubeCluster {
    async fn read_used(&self, source: &MetricSource) -> Result<Sample, ProviderError> {
        match source {
            MetricSource::Prometheus(promql) => self.prometheus_used(promql).await,
            MetricSource::PodMetricsMax { resource, pod_prefix, selector } => {
                self.pod_metrics_max(resource, pod_prefix, selector.as_deref()).await
            }
            // A host metric can never reach the k8s boundary — the controller
            // routes host dimensions to `HostCluster`. Typed, never silent.
            MetricSource::Host(_) => Err(ProviderError::ApiPermanent(
                "host metric source on KubeCluster (route host dimensions to HostCluster)".into(),
            )),
        }
    }

    async fn read_limit(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
    ) -> Result<u64, ProviderError> {
        // PodResize reads the LIVE pods' current limit (the MAX across the owner's
        // pods) — that is the value the in-place band manages, not the template.
        if let LimitLayout::PodResize { container } = layout {
            let mut max = 0u64;
            for pod in self.owner_pods(target).await? {
                if let Some(q) = Self::pod_container_qty(&pod.data, container, "limits", resource) {
                    if let Some(v) = Unit::for_resource(resource).parse(&q) {
                        max = max.max(v);
                    }
                }
            }
            return Ok(max); // 0 ⇒ decide() seeds to the floor (the ceded-field path)
        }
        let obj = self.get_owner(target).await?;
        match Self::read_qty(&obj.data, layout, resource) {
            // Unset limit → 0; decide() seeds it to the floor (the ceded-field path).
            None => Ok(0),
            // Parse in the resource's base unit (cpu → millicores, else bytes) so
            // a cpu limit "1" reads as 1000, not 1.
            Some(qty) => Unit::for_resource(resource).parse(&qty).ok_or(ProviderError::NoCapacityField),
        }
    }

    async fn field_owners(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
        logical_field: &str,
    ) -> Result<Vec<FieldOwner>, ProviderError> {
        let obj = self.get_owner(target).await?;
        let mf = serde_json::to_value(&obj.metadata.managed_fields)
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        let segments = match layout {
            LimitLayout::ClusterTopLevel => cnpg_cluster_limit_segments(resource),
            LimitLayout::PvcRequest => pvc_request_segments(),
            LimitLayout::ClusterStorage => cnpg_storage_segments(),
            LimitLayout::PodTemplate { container } => {
                match Self::container_name(&obj.data, container) {
                    Some(c) => pod_template_limit_segments(&c, resource),
                    None => return Ok(Vec::new()),
                }
            }
            // In-place resize writes the pods' `resize` subresource, which breathe
            // owns; cross-writer detection on the subresource (a co-resizing VPA)
            // is a documented v1 follow-on — for now breathe is the sole resizer.
            LimitLayout::PodResize { .. } => return Ok(Vec::new()),
            LimitLayout::Host(_) => {
                return Err(ProviderError::ApiPermanent(
                    "host layout on KubeCluster (route host dimensions to HostCluster)".into(),
                ))
            }
        };
        Ok(field_owners(&mf, &segments, logical_field))
    }

    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
        let target = &patch.target;

        // IN-PLACE RESIZE: carve the live pods via the `pods/{name}/resize`
        // subresource (k8s ≥1.33) — no restart, exactly like HostCluster's live
        // cgroup write. The template is untouched (a re-created pod re-converges
        // in-place next tick); QoS is preserved per pod.
        if let LimitLayout::PodResize { container } = &patch.layout {
            let pods = self.owner_pods(target).await?;
            if pods.is_empty() {
                return Err(ProviderError::TargetNotFound);
            }
            let gvk = GroupVersionKind::gvk("", "v1", "Pod");
            let ar = ApiResource::from_gvk(&gvk);
            let pod_api: Api<DynamicObject> = Api::namespaced_with(self.client.clone(), &target.namespace, &ar);
            let pp = PatchParams { field_manager: Some(patch.field_manager.clone()), ..Default::default() };
            for pod in &pods {
                let Some(pod_name) = pod.metadata.name.clone() else { continue };
                let cname = match container {
                    Some(c) => c.clone(),
                    None => Self::pod_first_container(&pod.data).ok_or(ProviderError::NoCapacityField)?,
                };
                let qos = pod.data.pointer("/status/qosClass").and_then(Value::as_str).unwrap_or("Burstable");
                let current_req = Self::pod_container_qty(&pod.data, &Some(cname.clone()), "requests", &patch.resource);
                let resources = resize_resources_block(qos, &patch.resource, patch.value, current_req.as_deref());
                let body = json!({ "spec": { "containers": [ { "name": cname, "resources": resources } ] } });
                pod_api
                    .patch_subresource("resize", &pod_name, &pp, &Patch::Strategic(&body))
                    .await
                    .map_err(|e| ProviderError::ApiPermanent(e.to_string()))?;
            }
            return Ok(AppliedReceipt { source_hash: [0u8; 16] });
        }

        let (g, v) = Self::group_version(target);
        let api_version = if g.is_empty() { v.clone() } else { format!("{g}/{v}") };
        // Render in the resource's base unit: bytes as a bare integer, cpu with
        // the `m` suffix (a bare "250" would be read by k8s as 250 *cores*).
        let qty = Quantity { value: patch.value, unit: Unit::for_resource(&patch.resource) }.to_string();
        let res = &patch.resource;
        let spec = match &patch.layout {
            LimitLayout::ClusterTopLevel => json!({ "resources": { "limits": { res: qty } } }),
            LimitLayout::PvcRequest => json!({ "resources": { "requests": { "storage": qty } } }),
            LimitLayout::ClusterStorage => json!({ "storage": { "size": qty } }),
            LimitLayout::PodTemplate { container } => {
                let cname = match container {
                    Some(c) => c.clone(),
                    None => {
                        let obj = self.get_owner(target).await?;
                        Self::container_name(&obj.data, &None).ok_or(ProviderError::NoCapacityField)?
                    }
                };
                json!({ "template": { "spec": { "containers": [
                    { "name": cname, "resources": { "limits": { res: qty } } }
                ] } } })
            }
            // PodResize is fully handled by the in-place path at the top of
            // apply; this arm is structurally unreachable (typed error, no panic).
            LimitLayout::PodResize { .. } => {
                return Err(ProviderError::ApiPermanent(
                    "internal: PodResize must be handled by the in-place path".into(),
                ))
            }
            LimitLayout::Host(_) => {
                return Err(ProviderError::ApiPermanent(
                    "host layout on KubeCluster (route host dimensions to HostCluster)".into(),
                ))
            }
        };
        let body = json!({
            "apiVersion": api_version,
            "kind": target.kind,
            "metadata": { "name": target.name, "namespace": target.namespace },
            "spec": spec,
        });
        self.api_for(target)
            .patch(&target.name, &PatchParams::apply(&patch.field_manager).force(), &Patch::Apply(&body))
            .await
            .map_err(|e| ProviderError::ApiPermanent(e.to_string()))?;
        Ok(AppliedReceipt { source_hash: [0u8; 16] })
    }

    /// Phase 2 (resizePolicy-aware shrink): is an in-place shrink of `resource`
    /// restart-free? Only a `PodResize` carve can be — every other layout is already
    /// `RestartFree` (host/pvc) or `RestartRequiring` (template/CNPG), so we answer
    /// the conservative false and never read a pod there. For `PodResize` it is
    /// restart-free iff EVERY resized pod's managed container declares
    /// `resizePolicy[<resource>] = NotRequired`; a single `RestartContainer` (or
    /// absent ⇒ the k8s default) means the shrink may restart, so the gate keeps it
    /// `RestartConditional`. No live pods ⇒ false (nothing to resize in place).
    async fn read_resize_restart_free(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
    ) -> Result<bool, ProviderError> {
        let LimitLayout::PodResize { container } = layout else {
            return Ok(false);
        };
        let pods = self.owner_pods(target).await?;
        if pods.is_empty() {
            return Ok(false);
        }
        Ok(pods.iter().all(|p| Self::container_resize_not_required(&p.data, container, resource)))
    }
}

#[cfg(test)]
mod tests {
    use super::resize_resources_block;
    use serde_json::json;

    const GI: u64 = 1 << 30;
    const MI: u64 = 1 << 20;

    #[test]
    fn guaranteed_pod_keeps_requests_equal_limits_on_grow_and_shrink() {
        // grow: both requests and limits move to the new value → stays Guaranteed.
        assert_eq!(
            resize_resources_block("Guaranteed", "memory", 2 * GI, Some("1Gi")),
            json!({ "limits": { "memory": "2147483648" }, "requests": { "memory": "2147483648" } })
        );
        // shrink: likewise (req == lim preserved).
        assert_eq!(
            resize_resources_block("Guaranteed", "memory", 512 * MI, Some("1Gi")),
            json!({ "limits": { "memory": "536870912" }, "requests": { "memory": "536870912" } })
        );
    }

    #[test]
    fn burstable_pod_sets_only_limit_when_request_still_fits() {
        // request (512Mi) ≤ new limit (2Gi) → leave the request untouched.
        assert_eq!(
            resize_resources_block("Burstable", "memory", 2 * GI, Some("512Mi")),
            json!({ "limits": { "memory": "2147483648" } })
        );
    }

    #[test]
    fn burstable_pod_clamps_request_down_when_it_would_exceed_the_new_limit() {
        // shrinking the limit below the existing request (512Mi) → clamp the
        // request down to the new limit (k8s rejects request > limit).
        assert_eq!(
            resize_resources_block("Burstable", "memory", 256 * MI, Some("512Mi")),
            json!({ "limits": { "memory": "268435456" }, "requests": { "memory": "268435456" } })
        );
    }

    #[test]
    fn besteffort_pod_with_no_request_sets_only_the_limit() {
        assert_eq!(
            resize_resources_block("BestEffort", "memory", GI, None),
            json!({ "limits": { "memory": "1073741824" } })
        );
    }

    #[test]
    fn cpu_resize_carries_the_millicores_suffix() {
        // cpu must render with the `m` suffix — a bare "500" is 500 CORES.
        assert_eq!(
            resize_resources_block("Guaranteed", "cpu", 500, Some("250m")),
            json!({ "limits": { "cpu": "500m" }, "requests": { "cpu": "500m" } })
        );
    }

    #[test]
    fn resize_not_required_reads_the_container_policy() {
        use super::KubeCluster;
        let not_required = json!({ "spec": { "containers": [
            { "name": "app", "resizePolicy": [
                { "resourceName": "cpu", "restartPolicy": "NotRequired" },
                { "resourceName": "memory", "restartPolicy": "NotRequired" }
            ] }
        ] } });
        let restart_container = json!({ "spec": { "containers": [
            { "name": "app", "resizePolicy": [
                { "resourceName": "memory", "restartPolicy": "RestartContainer" }
            ] }
        ] } });
        let no_policy = json!({ "spec": { "containers": [ { "name": "app" } ] } });

        let c = Some("app".to_string());
        // NotRequired ⇒ a memory shrink is restart-free (golden).
        assert!(KubeCluster::container_resize_not_required(&not_required, &c, "memory"));
        // RestartContainer (explicit) ⇒ not restart-free.
        assert!(!KubeCluster::container_resize_not_required(&restart_container, &c, "memory"));
        // Absent policy ⇒ false (k8s default is RestartContainer for memory).
        assert!(!KubeCluster::container_resize_not_required(&no_policy, &c, "memory"));
        // A named container that doesn't exist ⇒ false (never assume).
        assert!(!KubeCluster::container_resize_not_required(&not_required, &Some("missing".into()), "memory"));
        // None ⇒ first container; resolves the same policy.
        assert!(KubeCluster::container_resize_not_required(&not_required, &None, "memory"));
    }
}
