//! `KubeCluster` — the real [`Cluster`] implementation over kube-rs.
//!
//! Dimension-agnostic I/O: `query` runs raw PromQL (with sample age),
//! `read_limit` reads a quantity at a [`LimitLayout`], `field_owners` extracts
//! ownership of the layout's fieldsV1 path (resolving the container name from
//! the live object), `apply` performs **true SSA** (`Patch::Apply`, NO force —
//! yields on a 409 field-conflict rather than clobbering a competitor, BU3′).
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
            // k8s-CR-path layouts (Step-6/8/12): read the scalar at the JSON-pointer
            // `field_path` on the fetched CR (Istio DestinationRule, ResourceQuota,
            // HPA, CNPG/VM/VLogs CR).
            LimitLayout::CrField { field_path, .. }
            | LimitLayout::DestinationRuleField { field_path, .. }
            | LimitLayout::NamespaceEnvelope { field_path, .. }
            | LimitLayout::ControllerSetpoint { field_path, .. } => {
                data.pointer(field_path).map(json_scalar_to_string)
            }
            // external-protocol / network layouts are never read on a k8s object here
            // (their actuators own the read) — typed None, never a silent wrong value.
            LimitLayout::ConfigFile { .. } | LimitLayout::ApiCall { .. } | LimitLayout::PodNetworkBandwidth { .. } => None,
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

    /// **Part 1 (SOFT k8s carve):** resolve the cgroup-path coordinates of EVERY
    /// live pod a band manages — the apiserver side of routing a `MemoryBand`'s
    /// efficiency carve to the pod's `memory.high` (SOFT) cgroup file instead of the
    /// k8s `limits.memory` (`memory.max`, HARD). Lists the band's owner pods (the
    /// SAME `owner_pods` the in-place resize uses) and resolves each via the pure
    /// [`pod_coords_from_value`](crate::pod_cgroup::pod_coords_from_value).
    ///
    /// Returns one `(PodCgroupCoords, container)` per pod whose managed container is
    /// running (has a `containerID`); a pod that hasn't started its container yet is
    /// SKIPPED (a benign "not ready", not an error — it has no cgroup to carve). An
    /// empty result ⇒ no pod is carveable this tick (the caller holds, exactly like a
    /// dormant group). The live list is the one impure edge — the coordinate
    /// extraction itself is the pure, fully-tested `pod_coords_from_value`.
    ///
    /// `tier-honest`: this method runs against a LIVE apiserver (`pending-deploy`);
    /// only the per-pod extraction is library-pure (`parse-time-rejected`).
    pub async fn resolve_pod_cgroup_coords(
        &self,
        target: &Target,
    ) -> Result<Vec<crate::pod_cgroup::PodCgroupCoords>, ProviderError> {
        let mut coords = Vec::new();
        for pod in self.owner_pods(target).await? {
            // a pod whose managed container isn't running yet has no cgroup to carve —
            // skip it (typed parse-rejection → skip), never produce a wrong path.
            if let Ok(c) = crate::pod_cgroup::pod_coords_from_value(&pod.data, &target.container) {
                coords.push(c);
            }
        }
        Ok(coords)
    }

    /// **Part 1 (SOFT k8s carve):** resolve `(coords, node_name)` for every live pod
    /// a band manages — the apiserver inputs the controller needs to build a
    /// `PodMemoryHigh` dispatch per pod (the coords address the cgroup file; the node
    /// names the host-agent that owns it). Skips a pod that isn't scheduled yet (no
    /// node) or whose managed container isn't running (no cgroup); both are benign
    /// "not ready" states, not errors. `pending-deploy` (live apiserver list); the
    /// per-pod extraction is the pure, tested `pod_coords_from_value`/`node_name_from_pod`.
    pub async fn resolve_pod_soft_carve_targets(
        &self,
        target: &Target,
    ) -> Result<Vec<(crate::pod_cgroup::PodCgroupCoords, String)>, ProviderError> {
        let mut out = Vec::new();
        for pod in self.owner_pods(target).await? {
            let (Ok(c), Some(node)) = (
                crate::pod_cgroup::pod_coords_from_value(&pod.data, &target.container),
                crate::pod_cgroup::node_name_from_pod(&pod.data),
            ) else {
                continue;
            };
            out.push((c, node));
        }
        Ok(out)
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
        // A label-selected group with ZERO matching pods is DORMANT (the ephemeral
        // target is scaled to zero — no runner between builds), not an error. The
        // server already filtered by label, so an empty list IS an empty group.
        // (The prefix path keeps `MetricsMissing` — an owner with no pods is abnormal.)
        if selector.is_some() && list.items.is_empty() {
            return Err(ProviderError::NoTargetPods);
        }
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
/// A JSON scalar rendered to a string — `"10Gi"` stays a string, `100` becomes
/// `"100"`. Reads a generic CR field's current value regardless of its JSON type.
fn json_scalar_to_string(v: &Value) -> String {
    v.as_str().map(String::from).unwrap_or_else(|| v.to_string())
}

/// Build the SSA `spec` content for a `/spec/...` JSON-pointer `field_path` set to
/// `value`. `/spec/trafficPolicy/connectionPool/tcp/maxConnections` →
/// `{"trafficPolicy":{"connectionPool":{"tcp":{"maxConnections": value}}}}` (the
/// content UNDER /spec, since `apply` wraps it in the object body's `spec`).
/// Object paths only — an array-index segment (HPA `metrics/0/…`) is not supported.
fn nested_json_under_spec(field_path: &str, value: Value) -> Value {
    let trimmed = field_path.trim_start_matches('/');
    let rel = trimmed.strip_prefix("spec/").unwrap_or(trimmed);
    let mut node = value;
    for seg in rel.split('/').filter(|s| !s.is_empty()).collect::<Vec<_>>().into_iter().rev() {
        node = json!({ seg: node });
    }
    node
}

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
        // In-place resize writes the pods' `resize` subresource, which breathe
        // owns; cross-writer detection on the subresource (a co-resizing VPA) is a
        // documented v1 follow-on — for now breathe is the sole resizer. This MUST
        // short-circuit BEFORE get_owner: a label-selected pod group (ARC runners)
        // has no gettable owner object, so fetching one would 404/403.
        if matches!(layout, LimitLayout::PodResize { .. }) {
            return Ok(Vec::new());
        }
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
            // Already handled above (kept for exhaustiveness; unreachable).
            LimitLayout::PodResize { .. } => return Ok(Vec::new()),
            LimitLayout::Host(_) => {
                return Err(ProviderError::ApiPermanent(
                    "host layout on KubeCluster (route host dimensions to HostCluster)".into(),
                ))
            }
            // generic CR-path + external layouts: no managed-field competitor tracked
            // here yet — breathe is the writer (a per-field managedFields competitor
            // check for arbitrary CR paths is a follow-up). Proceed (empty owner set).
            LimitLayout::CrField { .. }
            | LimitLayout::DestinationRuleField { .. }
            | LimitLayout::NamespaceEnvelope { .. }
            | LimitLayout::ControllerSetpoint { .. }
            | LimitLayout::ConfigFile { .. }
            | LimitLayout::ApiCall { .. }
            | LimitLayout::PodNetworkBandwidth { .. } => return Ok(Vec::new()),
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
            // k8s-CR-path layouts (Step-6/8/12): SSA-write the value at the
            // `/spec/...` JSON-pointer field_path (a bare number — maxConnections,
            // retention seconds, quota count, HPA percent). Object paths only;
            // array-index paths (HPA metrics[]) are a typed follow-up.
            LimitLayout::CrField { field_path, .. }
            | LimitLayout::DestinationRuleField { field_path, .. }
            | LimitLayout::NamespaceEnvelope { field_path, .. }
            | LimitLayout::ControllerSetpoint { field_path, .. } => {
                nested_json_under_spec(field_path, json!(patch.value))
            }
            // external-protocol / network layouts have dedicated actuators.
            LimitLayout::ConfigFile { .. } | LimitLayout::ApiCall { .. } | LimitLayout::PodNetworkBandwidth { .. } => {
                return Err(ProviderError::ApiPermanent(
                    "config-file/api-call/network layout requires a dedicated actuator (ConfigReload/ApiCall/Host-tc), not KubeCluster".into(),
                ))
            }
        };
        let body = json!({
            "apiVersion": api_version,
            "kind": target.kind,
            "metadata": { "name": target.name, "namespace": target.namespace },
            "spec": spec,
        });
        // BU3′ — NO `.force()`. A forced SSA apply reclaims a field another
        // manager owns, silently clobbering a competitor between the single-writer
        // guard's read and this write — the exact race that makes cooperative-yield
        // `only-mitigated`. Without force, a conflicting field yields a 409, which
        // we map to a TRANSIENT error: breathe never clobbers, requeues, and the
        // pre-write guard then observes the competitor's managedFields and yields
        // cleanly (TickPlan::Conflict). Blast-radius-bounded — not unrepresentable
        // (a force-applying PEER can still win the field), per the §I tier-honest
        // ledger. (Host-tier carves take a different path entirely — sysfs/systemd
        // have no managedFields; their safety is the L2 ceiling wall + the clamp.)
        self.api_for(target)
            .patch(&target.name, &PatchParams::apply(&patch.field_manager), &Patch::Apply(&body))
            .await
            .map_err(|e| match e {
                kube::Error::Api(ae) if ae.code == 409 => ProviderError::ApiTransient(format!(
                    "SSA field conflict (a competitor owns the field) — yielding, will re-observe: {ae}"
                )),
                other => ProviderError::ApiPermanent(other.to_string()),
            })?;
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

    /// Part 3: read the target's LIVE declared `resources.requests.<resource>` — the
    /// inviolable shrink floor (a limit below the request is invalid in k8s + unsafe).
    /// Reads the MAX request across the band's pod group (so the floor covers the
    /// hottest instance), in the resource's base unit. For pod-backed layouts
    /// (`PodResize`/`PodTemplate`) the request lives on the live pods; for a CNPG
    /// `Cluster` it lives at `spec.resources.requests.<resource>`. Best-effort `0`
    /// when there is no readable request (the band's own `requestFloor` still binds).
    async fn read_request_floor(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
    ) -> Result<u64, ProviderError> {
        let unit = Unit::for_resource(resource);
        match layout {
            // pod-backed: the request lives on the live pods (max across the group).
            LimitLayout::PodResize { container } | LimitLayout::PodTemplate { container } => {
                let mut max = 0u64;
                for pod in self.owner_pods(target).await? {
                    if let Some(q) = Self::pod_container_qty(&pod.data, container, "requests", resource) {
                        if let Some(v) = unit.parse(&q) {
                            max = max.max(v);
                        }
                    }
                }
                Ok(max)
            }
            // CNPG Cluster top-level: spec.resources.requests.<resource>.
            LimitLayout::ClusterTopLevel => {
                let obj = self.get_owner(target).await?;
                let q = obj
                    .data
                    .pointer(&format!("/spec/resources/requests/{resource}"))
                    .and_then(Value::as_str)
                    .and_then(|s| unit.parse(s));
                Ok(q.unwrap_or(0))
            }
            // storage / host / generic-CR layouts carry no per-pod memory/cpu request.
            _ => Ok(0),
        }
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
    fn generic_cr_path_builds_the_nested_ssa_spec() {
        // an Istio DestinationRule connection-pool field (Step-6) → nested spec.
        assert_eq!(
            super::nested_json_under_spec("/spec/trafficPolicy/connectionPool/tcp/maxConnections", json!(100)),
            json!({ "trafficPolicy": { "connectionPool": { "tcp": { "maxConnections": 100 } } } })
        );
        // a ResourceQuota field (Step-8).
        assert_eq!(super::nested_json_under_spec("/spec/hard/limits.cpu", json!(8000)), json!({ "hard": { "limits.cpu": 8000 } }));
        // reads back string-or-number uniformly.
        assert_eq!(super::json_scalar_to_string(&json!(100)), "100");
        assert_eq!(super::json_scalar_to_string(&json!("10Gi")), "10Gi");
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
