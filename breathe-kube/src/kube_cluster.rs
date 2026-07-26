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
use breathe_control::{FieldOwner, Quantity, StorageCapability, Unit};
use breathe_provider::LiveWitness;
use breathe_provider::request::{ClassPreserved, RequestActuator};
use breathe_provider::{
    AppliedReceipt, Cluster, LimitLayout, MetricSource, ProviderError, Sample, SsaPatch, Target,
};
use k8s_openapi::api::storage::v1::StorageClass;
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
            //
            // PodRequestResize is the same structural fact for the same reason:
            // both address the LIVE pod via the resize subresource, so neither
            // has anything to read out of the fetched owner. This arm is a
            // statement about WHERE the value lives, not a stub for the unwired
            // request actuation (that boundary is in `apply`, below).
            LimitLayout::PodResize { .. } | LimitLayout::PodRequestResize { .. } => None,
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
            // HORIZONTAL: the workload's current replica count (`.spec.replicas`),
            // rendered as a bare integer string the Count unit parses.
            LimitLayout::Replica { .. } => data.pointer("/spec/replicas").map(json_scalar_to_string),
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

    /// **THE ONE in-place `pods/{name}/resize` I/O path**, shared by the LIMIT
    /// carve (`Cluster::apply`'s `PodResize` arm) and the REQUEST carve
    /// (`RequestActuator::resize_in_place`).
    ///
    /// The two dimensions differ ONLY in the `resources` block they write — a
    /// limit carve writes `limits` (and clamps `requests` down when it must),
    /// a request carve writes `requests` alone — so the block builder is the
    /// injected parameter and everything else (pod listing, container
    /// resolution, field-manager scoping, the subresource patch, error mapping)
    /// is written once. Forking a second resize loop for requests would have
    /// duplicated exactly the parts most expensive to get wrong twice.
    ///
    /// `mk_resources` receives the live pod's JSON and the resolved container
    /// name, and returns the `resources` block for that pod.
    async fn patch_pods_resize<F>(
        &self,
        target: &Target,
        container: Option<&str>,
        field_manager: &str,
        mk_resources: F,
    ) -> Result<AppliedReceipt, ProviderError>
    where
        F: Fn(&Value, &str) -> Value,
    {
        let pods = self.owner_pods(target).await?;
        if pods.is_empty() {
            return Err(ProviderError::TargetNotFound);
        }
        let gvk = GroupVersionKind::gvk("", "v1", "Pod");
        let ar = ApiResource::from_gvk(&gvk);
        let pod_api: Api<DynamicObject> = Api::namespaced_with(self.client.clone(), &target.namespace, &ar);
        let pp = PatchParams { field_manager: Some(field_manager.to_owned()), ..Default::default() };
        for pod in &pods {
            let Some(pod_name) = pod.metadata.name.clone() else { continue };
            let cname = match container {
                Some(c) => c.to_owned(),
                None => Self::pod_first_container(&pod.data).ok_or(ProviderError::NoCapacityField)?,
            };
            let resources = mk_resources(&pod.data, &cname);
            let body = json!({ "spec": { "containers": [ { "name": cname, "resources": resources } ] } });
            pod_api
                .patch_subresource("resize", &pod_name, &pp, &Patch::Strategic(&body))
                .await
                .map_err(|e| ProviderError::ApiPermanent(e.to_string()))?;
        }
        Ok(AppliedReceipt { source_hash: [0u8; 16] })
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

    /// Prometheus instant query → the RAW `f64` scalar + the underlying sample's
    /// age (seconds). The fractional read the HORIZONTAL band needs — a per-replica
    /// utilization ratio (`0.9`) or a fractional work rate would be destroyed by the
    /// `u64` truncation [`prometheus_used`](Self::prometheus_used) applies for the
    /// vertical (byte/millicore) dimensions. Every failure is a typed
    /// [`ProviderError`] (never a panic).
    ///
    /// # Errors
    /// [`ProviderError::ApiTransient`] on the HTTP/JSON call, [`ProviderError::MetricsMissing`]
    /// when the instant query returns no `data.result[0].value` pair.
    pub async fn query_scalar(&self, promql: &str) -> Result<(f64, u64), ProviderError> {
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
        let value: f64 = pair
            .get(1)
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or(ProviderError::MetricsMissing)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(ts);
        Ok((value, (now - ts).max(0.0) as u64))
    }

    /// Prometheus instant query → (value, sample age). The vertical (byte/millicore)
    /// read: reuses [`query_scalar`](Self::query_scalar) and truncates the scalar to
    /// `u64` (a saturating `f64 as u64`, so a negative reading floors at 0) — the
    /// dimension-agnostic `used` contract of the [`Cluster`] trait.
    async fn prometheus_used(&self, promql: &str) -> Result<Sample, ProviderError> {
        let (value, age_secs) = self.query_scalar(promql).await?;
        Ok(Sample { value: value as u64, age_secs })
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
        let list = api.list(&lp).await.map_err(|e| Self::classify_pod_metrics_error(e.to_string()))?;
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

    /// Recognize the "metrics.k8s.io API group is not registered at all"
    /// signature and turn it into an actionable message instead of an opaque
    /// one. When metrics-server has never been installed, the apiserver's
    /// generic 404 handler answers with a raw, non-JSON `"404 page not
    /// found\n"` body (no metrics-server aggregated API to route to) — kube-rs
    /// can't parse that as a structured `Status`, so it surfaces as
    /// `ErrorResponse { reason: "Failed to parse error data", .. }`. That reads
    /// identically to a genuinely transient hiccup even though the underlying
    /// gap (no metrics-server deployed) will not resolve on its own retry —
    /// distinguishing it here means `kubectl describe cpuband`/`memoryband`
    /// names the real cause instead of a generic "transient API error".
    /// Stays `ApiTransient` (not a new enum variant): the band should still
    /// retry-and-recover for free the moment metrics-server is installed,
    /// with zero controller restart.
    fn classify_pod_metrics_error(raw: String) -> ProviderError {
        if raw.contains("404 page not found") {
            ProviderError::ApiTransient(format!(
                "metrics-server not installed (metrics.k8s.io/v1beta1 PodMetrics API is not registered in this cluster — install metrics-server): {raw}"
            ))
        } else {
            ProviderError::ApiTransient(raw)
        }
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

/// The `resources` block for an in-place REQUEST carve — the RESERVATION
/// sibling of [`resize_resources_block`].
///
/// **It writes `requests` and NOTHING else, and that omission is the safety
/// property.** A limit carve legitimately touches both sides of the pair
/// (`resize_resources_block` mirrors the value into `requests` for a Guaranteed
/// pod, and clamps `requests` down when a shrink would leave `request > limit`).
/// A request carve must never touch `limits`, because the `QoS` class is a
/// function of the requests-vs-limits *relation*: moving the other side is
/// precisely how a within-class carve turns into an undeclared class transition,
/// which `ValidatePodResize` rejects outright.
///
/// The class-preservation decision is NOT made here. It was made — over the
/// whole pod, every container, every resource — by
/// `breathe_provider::request::ClassPreserved::check`, and this function is
/// reached only with that witness in hand. Re-deriving it from one pod's
/// `status.qosClass` string, the way the limit path does, would be a second
/// source of truth for the one fact this dimension turns on.
fn request_resources_block(resource: &str, value: u64) -> Value {
    let qty = Quantity { value, unit: Unit::for_resource(resource) }.to_string();
    json!({ "requests": { resource: qty } })
}

/// Provisioners known to report NODE-WIDE (not per-volume) usage stats via
/// `kubelet_volume_stats_used_bytes` — `local-path`'s hostPath-backed PVs are
/// the canonical case (the metric reports the whole node filesystem, not the
/// 10Gi volume, which is exactly the lie `TickPlan::Unrepresentable` catches
/// when it manifests as `used > capacity`). A StorageClass on this denylist
/// is caught EARLIER, at capability-discovery time, before a bad sample has
/// to land. Absence from this list is NOT proof of correctness — it is the
/// honest default (a class we have no reason to distrust).
const NO_PER_VOLUME_METRICS_PROVISIONERS: &[&str] = &["rancher.io/local-path", "kubernetes.io/host-path"];

/// The JSON-pointer to the StorageClass NAME on a storage layout's fetched
/// owner object. `PvcRequest`'s owner IS the PVC (`spec.storageClassName`,
/// always set post-admission-defaulting); `ClusterStorage`'s owner is the
/// CNPG `Cluster` CR (`spec.storage.storageClass`, the field CNPG's own
/// instance PVCs are provisioned with). `None` for every other layout (no
/// PVC/StorageClass concept) or when the field genuinely isn't set yet (a
/// CNPG cluster whose `spec.storage.storageClass` was omitted — the operator
/// falls back to the namespace default, which this pointer can't see).
fn storage_class_name_for(data: &Value, layout: &LimitLayout) -> Option<String> {
    match layout {
        LimitLayout::PvcRequest => data.pointer("/spec/storageClassName").and_then(Value::as_str).map(String::from),
        LimitLayout::ClusterStorage => {
            data.pointer("/spec/storage/storageClass").and_then(Value::as_str).map(String::from)
        }
        _ => None,
    }
}

/// Project a live [`StorageClass`] onto the typed capability contract — see
/// [`StorageCapability`]'s doc for what each property means and why either
/// being false is fatal to convergence. Pure + unit-tested.
fn storage_capability_from(sc: &StorageClass) -> StorageCapability {
    let provisioner = sc.provisioner.clone();
    StorageCapability {
        volume_expansion: sc.allow_volume_expansion.unwrap_or(false),
        per_volume_metrics: !NO_PER_VOLUME_METRICS_PROVISIONERS.contains(&provisioner.as_str()),
        provisioner,
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
        // Both live-pod resize layouts short-circuit: a label-selected pod group
        // has no gettable owner, and the managed-field competitor that WOULD
        // matter for requests (a Deployment template owned by Flux) lives on the
        // template, not on the pod these layouts write.
        if matches!(layout, LimitLayout::PodResize { .. } | LimitLayout::PodRequestResize { .. }) {
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
            LimitLayout::PodResize { .. } | LimitLayout::PodRequestResize { .. } => return Ok(Vec::new()),
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
            | LimitLayout::PodNetworkBandwidth { .. }
            // HORIZONTAL: `.spec.replicas` — breathe is the writer; a co-writer
            // (KEDA/HPA) is detected via the 409 on apply (no `.force()`), the same
            // cooperative-yield guard every generic-path layout uses.
            | LimitLayout::Replica { .. } => return Ok(Vec::new()),
        };
        Ok(field_owners(&mf, &segments, logical_field))
    }

    // `_witness`: the authorization is enforced at the CALL BOUNDARY (see
    // `Cluster::apply`'s doc) — a caller with a shadow verdict has no witness to
    // pass, so this function is unreachable from one. A witness cannot change what
    // bytes go out, so the SSA patch itself has nothing to do with the value.
    async fn apply(&self, _witness: &LiveWitness, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
        let target = &patch.target;

        // IN-PLACE RESIZE: carve the live pods via the `pods/{name}/resize`
        // subresource (k8s ≥1.33) — no restart, exactly like HostCluster's live
        // cgroup write. The template is untouched (a re-created pod re-converges
        // in-place next tick); QoS is preserved per pod.
        if let LimitLayout::PodResize { container } = &patch.layout {
            // ONE resize I/O path, shared with the REQUEST actuator below. Only
            // the `resources` block differs between a limit carve and a request
            // carve, so only the block builder is per-dimension.
            return self
                .patch_pods_resize(target, container.as_deref(), &patch.field_manager, |pod_data, cname| {
                    let qos = pod_data.pointer("/status/qosClass").and_then(Value::as_str).unwrap_or("Burstable");
                    let current_req =
                        Self::pod_container_qty(pod_data, &Some(cname.to_owned()), "requests", &patch.resource);
                    resize_resources_block(qos, &patch.resource, patch.value, current_req.as_deref())
                })
                .await;
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
            // ── THE PERMANENT BOUNDARY between the generic door and DOOR 1 ──
            //
            // A request write is actuated by `RequestActuator::resize_in_place`
            // (implemented for `KubeCluster` below), never here. This arm stays a
            // typed refusal FOREVER, not until it is "wired": the generic
            // `Cluster::apply` path carries only a `LiveWitness`, and a request
            // write additionally demands a `ClassPreserved` — the proof that the
            // carve does not move the pod's QoS class. This path cannot produce
            // one, so it must not write.
            //
            // That is the whole point of the two-door split arriving here as a
            // dead end rather than as a convenience: a caller who reaches for the
            // familiar door gets a loud, typed error instead of a write that
            // skipped the one check `ValidatePodResize` will reject anyway.
            //
            // A typed error, never a `todo!()`/`unimplemented!()` (which would
            // abort the controller) and never a silent `Ok` (which would report a
            // carve that did not happen — the one outcome worse than an error).
            LimitLayout::PodRequestResize { .. } => {
                return Err(ProviderError::ApiPermanent(
                    "a request write must go through RequestActuator::resize_in_place with a \
                     ClassPreserved witness, never the generic SSA path (which cannot produce one)"
                        .into(),
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
            // HORIZONTAL: SSA-write the bare replica count to `.spec.replicas`.
            // Same no-`.force()` cooperative-yield discipline: a competing scaler
            // that owns the field yields a 409 (mapped to a transient retry), never
            // a clobber. Rendered as a bare integer (never a Quantity string).
            LimitLayout::Replica { .. } => json!({ "replicas": patch.value }),
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

    /// The restart half of the no-starve signal: are the target's pods recently
    /// (re)started or crash-looping? A pod with ANY container in `CrashLoopBackOff`,
    /// or whose current container is `waiting` after a non-zero `restartCount`,
    /// counts. A crash-loop means the current low usage is a symptom (the workload
    /// keeps dying before it can do real work), not safe slack — so a shrink is held.
    /// Best-effort `false` for non-pod-backed layouts / unreadable status (never
    /// blocks a carve spuriously). Read-only.
    async fn read_restarting(
        &self,
        target: &Target,
        layout: &LimitLayout,
        _resource: &str,
    ) -> Result<bool, ProviderError> {
        // only pod-backed layouts have a per-pod restart concept.
        if !matches!(layout, LimitLayout::PodResize { .. } | LimitLayout::PodTemplate { .. }) {
            return Ok(false);
        }
        let pods = self.owner_pods(target).await?;
        for pod in &pods {
            let Some(statuses) = pod.data.pointer("/status/containerStatuses").and_then(Value::as_array) else {
                continue;
            };
            for cs in statuses {
                // an explicit crash-loop is the strongest signal.
                if cs
                    .pointer("/state/waiting/reason")
                    .and_then(Value::as_str)
                    .is_some_and(|r| r == "CrashLoopBackOff")
                {
                    return Ok(true);
                }
                // a container currently waiting AFTER a restart (it died and is backing
                // off / re-pulling) is also still un-stable — treat as restarting.
                let restart_count = cs.pointer("/restartCount").and_then(Value::as_u64).unwrap_or(0);
                let waiting = cs.pointer("/state/waiting").is_some();
                if restart_count > 0 && waiting {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// The CAPABILITY-DISCOVERY read (the fail-fast fix): resolve the
    /// StorageClass backing `target`'s PVC (`PvcRequest`) or CNPG-managed PVCs
    /// (`ClusterStorage`) and project it onto [`StorageCapability`]. `Ok(None)`
    /// for every other layout (no PVC concept) and for the two "can't
    /// determine, don't gate" cases: the layout's owner has no explicit
    /// StorageClass name yet, or the named StorageClass has vanished. Both are
    /// UNKNOWN, never a false negative — see [`Cluster::read_storage_capability`]'s
    /// doc for why the weakest answer is the safe default.
    async fn read_storage_capability(
        &self,
        target: &Target,
        layout: &LimitLayout,
    ) -> Result<Option<StorageCapability>, ProviderError> {
        if !matches!(layout, LimitLayout::PvcRequest | LimitLayout::ClusterStorage) {
            return Ok(None);
        }
        let obj = self.get_owner(target).await?;
        let Some(name) = storage_class_name_for(&obj.data, layout) else {
            return Ok(None);
        };
        let api: Api<StorageClass> = Api::all(self.client.clone());
        match api.get_opt(&name).await {
            Ok(Some(sc)) => Ok(Some(storage_capability_from(&sc))),
            Ok(None) => Ok(None), // the named class doesn't exist (yet) — unknown, don't gate
            Err(e) => Err(ProviderError::ApiTransient(e.to_string())),
        }
    }
}

/// Does a witness/patch pair actually describe the same change? Pure, so the
/// agreement rule is testable without a cluster.
///
/// **Tier: only-mitigated, and deliberately kept anyway.** The strong guarantee
/// is upstream — a plan's `InPlaceCarve` builds the witness and the patch from
/// ONE narrowing chain, so they agree by construction and there is no reachable
/// disagreement through the intended pipeline. This is the defense-in-depth
/// layer for a HAND-assembled pair, where a caller could otherwise present a
/// witness proving container `a`'s carve is class-safe while patching container
/// `b`. It cannot be a type: `SsaPatch` is the fleet-wide payload for ten
/// dimensions and must not grow a request-specific field.
fn witness_matches_patch(preserved: &ClassPreserved, patch: &SsaPatch) -> Result<(), ProviderError> {
    if preserved.to() != patch.value {
        return Err(ProviderError::ApiPermanent(
            "request actuation refused: the ClassPreserved witness authorizes a different value \
             than the patch carries"
                .into(),
        ));
    }
    if preserved.resource().as_str() != patch.resource {
        return Err(ProviderError::ApiPermanent(
            "request actuation refused: the ClassPreserved witness authorizes a different resource \
             than the patch carries"
                .into(),
        ));
    }
    let LimitLayout::PodRequestResize { container } = &patch.layout else {
        return Err(ProviderError::ApiPermanent(
            "request actuation refused: the patch does not carry the PodRequestResize layout \
             (a request write must never travel a limit layout)"
                .into(),
        ));
    };
    // A layout with no container pinned resolves per-pod at write time and
    // defers to the witness's own container, so only a PINNED mismatch is a
    // refusal.
    if container.as_deref().is_some_and(|c| c != preserved.container()) {
        return Err(ProviderError::ApiPermanent(
            "request actuation refused: the ClassPreserved witness authorizes a different \
             container than the patch targets"
                .into(),
        ));
    }
    Ok(())
}

/// **DOOR 1, realized.** The in-place, within-class request carve.
///
/// Reuses [`KubeCluster::patch_pods_resize`] verbatim — the SAME pod listing,
/// container resolution, field-manager scoping and subresource patch the limit
/// carve uses — and supplies the one thing that differs: a `resources` block
/// containing `requests` and nothing else.
///
/// # What this impl does NOT do, on purpose
///
/// It never writes `limits`, never reads `status.qosClass` to make a decision,
/// and has no branch that could produce a class transition. It cannot: the trait
/// has exactly one method, that method takes an [`SsaPatch`] (one scalar), and
/// `ClassTransitionProposal` is a different type with no conversion to one. The
/// class question was already answered by the [`ClassPreserved`] argument, whose
/// only constructor recomputes the pod-level class over every container.
#[async_trait]
impl RequestActuator for KubeCluster {
    async fn resize_in_place(
        &self,
        _live: &LiveWitness,
        preserved: &ClassPreserved,
        patch: &SsaPatch,
    ) -> Result<AppliedReceipt, ProviderError> {
        witness_matches_patch(preserved, patch)?;
        self.patch_pods_resize(
            &patch.target,
            Some(preserved.container()),
            &patch.field_manager,
            |_pod_data, _cname| request_resources_block(&patch.resource, patch.value),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{request_resources_block, resize_resources_block, witness_matches_patch};
    use breathe_provider::request::{ClassPreserved, ContainerResources, PodResources, RequestResource};
    use breathe_provider::{LimitLayout, SsaPatch, Target};
    use serde_json::json;

    const GI: u64 = 1 << 30;
    const MI: u64 = 1 << 20;

    // ═══════════ the REQUEST block — requests only, never limits ════════════

    /// **The omission IS the safety property.** A request carve writes
    /// `requests` and nothing else, because `QoS` is a function of the
    /// requests-vs-limits RELATION: touching the other side is exactly how a
    /// within-class carve becomes an undeclared class transition.
    #[test]
    fn the_request_block_writes_requests_and_never_limits() {
        let block = request_resources_block("memory", 2 * GI);
        assert_eq!(block, json!({ "requests": { "memory": "2147483648" } }));
        assert!(block.get("limits").is_none(), "a request carve must NEVER write a limit");
    }

    /// cpu renders with the `m` suffix — a bare `250` would be read by k8s as
    /// 250 *cores*, a 1000× over-reservation that would never schedule.
    #[test]
    fn the_request_block_renders_cpu_in_millicores() {
        assert_eq!(request_resources_block("cpu", 250), json!({ "requests": { "cpu": "250m" } }));
    }

    /// The two block builders are genuinely different shapes, and the LIMIT one
    /// touches `requests` in both of its arms. Pinned so a future "unify these"
    /// refactor has to come here and confront why they must stay apart.
    #[test]
    fn the_limit_block_and_the_request_block_are_not_interchangeable() {
        let limit_side = resize_resources_block("Guaranteed", "memory", 2 * GI, Some("1Gi"));
        let request_side = request_resources_block("memory", 2 * GI);
        assert!(limit_side.get("limits").is_some(), "the limit builder writes limits");
        assert!(request_side.get("limits").is_none(), "the request builder does not");
        assert_ne!(limit_side, request_side);
    }

    // ═══════════ the witness↔patch agreement check (defense in depth) ════════

    fn pod() -> PodResources {
        PodResources::new(vec![
            ContainerResources {
                name: "db".into(),
                cpu_request: Some(100),
                cpu_limit: Some(500),
                memory_request: Some(128 * MI),
                memory_limit: Some(GI),
            },
            ContainerResources {
                name: "sidecar".into(),
                cpu_request: Some(10),
                cpu_limit: Some(50),
                memory_request: Some(32 * MI),
                memory_limit: Some(64 * MI),
            },
        ])
    }

    fn patch_for(container: &str, resource: &str, value: u64) -> SsaPatch {
        SsaPatch {
            target: Target {
                namespace: "camelot-build".into(),
                name: "sui-cache-pg".into(),
                kind: "StatefulSet".into(),
                api_version: "apps/v1".into(),
                container: Some(container.into()),
                pod_selector: None,
            },
            field_manager: "breathe-request".into(),
            layout: LimitLayout::PodRequestResize { container: Some(container.into()) },
            resource: resource.into(),
            value,
        }
    }

    #[test]
    fn an_agreeing_witness_and_patch_pass() {
        let w = ClassPreserved::check(&pod(), "db", RequestResource::Memory, 512 * MI).unwrap();
        assert!(witness_matches_patch(&w, &patch_for("db", "memory", 512 * MI)).is_ok());
    }

    /// The hole this closes: a witness proving container `db`'s carve is
    /// class-safe, presented alongside a patch that targets `sidecar`.
    #[test]
    fn a_witness_for_a_different_container_is_refused() {
        let w = ClassPreserved::check(&pod(), "db", RequestResource::Memory, 512 * MI).unwrap();
        assert!(witness_matches_patch(&w, &patch_for("sidecar", "memory", 512 * MI)).is_err());
    }

    #[test]
    fn a_witness_for_a_different_value_or_resource_is_refused() {
        let w = ClassPreserved::check(&pod(), "db", RequestResource::Memory, 512 * MI).unwrap();
        assert!(
            witness_matches_patch(&w, &patch_for("db", "memory", 900 * MI)).is_err(),
            "a witness proving 512Mi is class-safe does not authorize writing 900Mi"
        );
        assert!(
            witness_matches_patch(&w, &patch_for("db", "cpu", 512 * MI)).is_err(),
            "a memory witness does not authorize a cpu write"
        );
    }

    /// A request write must never travel a LIMIT layout — that is the generic
    /// SSA door, which carries no class proof at all.
    #[test]
    fn a_request_write_on_a_limit_layout_is_refused() {
        let w = ClassPreserved::check(&pod(), "db", RequestResource::Memory, 512 * MI).unwrap();
        let mut p = patch_for("db", "memory", 512 * MI);
        p.layout = LimitLayout::PodResize { container: Some("db".into()) };
        assert!(witness_matches_patch(&w, &p).is_err());
    }

    /// A layout with no container pinned resolves per-pod at write time, so the
    /// witness's own container is what the actuator uses. Accepted, not refused.
    #[test]
    fn an_unpinned_container_layout_defers_to_the_witness() {
        let w = ClassPreserved::check(&pod(), "db", RequestResource::Memory, 512 * MI).unwrap();
        let mut p = patch_for("db", "memory", 512 * MI);
        p.layout = LimitLayout::PodRequestResize { container: None };
        assert!(witness_matches_patch(&w, &p).is_ok());
    }

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

    // ── STORAGE CAPABILITY DISCOVERY (the fail-fast fix) ─────────────────────

    #[test]
    fn storage_class_name_for_reads_the_pvc_field_for_pvc_request() {
        use super::{storage_class_name_for, LimitLayout};
        let pvc = json!({ "spec": { "storageClassName": "local-path", "resources": { "requests": { "storage": "10Gi" } } } });
        assert_eq!(storage_class_name_for(&pvc, &LimitLayout::PvcRequest), Some("local-path".to_string()));
    }

    #[test]
    fn storage_class_name_for_reads_the_cnpg_cluster_field_for_cluster_storage() {
        use super::{storage_class_name_for, LimitLayout};
        let cluster = json!({ "spec": { "storage": { "storageClass": "ebs-gp3", "size": "10Gi" } } });
        assert_eq!(storage_class_name_for(&cluster, &LimitLayout::ClusterStorage), Some("ebs-gp3".to_string()));
    }

    #[test]
    fn storage_class_name_for_is_none_when_unset_or_not_applicable() {
        use super::{storage_class_name_for, LimitLayout};
        // omitted storageClassName (shouldn't happen post-admission, but must not panic).
        assert_eq!(storage_class_name_for(&json!({ "spec": {} }), &LimitLayout::PvcRequest), None);
        // a CNPG cluster whose storage.storageClass was never set (namespace default).
        assert_eq!(
            storage_class_name_for(&json!({ "spec": { "storage": { "size": "10Gi" } } }), &LimitLayout::ClusterStorage),
            None
        );
        // no PVC/StorageClass concept for this layout at all.
        assert_eq!(
            storage_class_name_for(&json!({ "spec": {} }), &LimitLayout::PodTemplate { container: None }),
            None
        );
    }

    #[test]
    fn storage_capability_from_flags_the_local_path_denylist_as_unsupported() {
        use super::storage_capability_from;
        use k8s_openapi::api::storage::v1::StorageClass;
        // the real Camelot shape: local-path, no expansion, no per-volume metrics.
        let sc = StorageClass {
            provisioner: "rancher.io/local-path".into(),
            allow_volume_expansion: None,
            ..Default::default()
        };
        let cap = storage_capability_from(&sc);
        assert!(!cap.volume_expansion);
        assert!(!cap.per_volume_metrics);
        assert_eq!(cap.provisioner, "rancher.io/local-path");
        assert!(!cap.is_supported());
    }

    #[test]
    fn storage_capability_from_reports_a_real_elastic_class_as_supported() {
        use super::storage_capability_from;
        use k8s_openapi::api::storage::v1::StorageClass;
        let sc = StorageClass {
            provisioner: "ebs.csi.aws.com".into(),
            allow_volume_expansion: Some(true),
            ..Default::default()
        };
        assert!(storage_capability_from(&sc).is_supported());
    }

    #[test]
    fn storage_capability_from_treats_no_expansion_as_unsupported_even_off_the_denylist() {
        // a non-denylisted provisioner that simply never turned on
        // allowVolumeExpansion is STILL unsupported — the two properties are
        // independent, and either being false is fatal (see StorageCapability's doc).
        use super::storage_capability_from;
        use k8s_openapi::api::storage::v1::StorageClass;
        let sc = StorageClass { provisioner: "ebs.csi.aws.com".into(), allow_volume_expansion: Some(false), ..Default::default() };
        let cap = storage_capability_from(&sc);
        assert!(cap.per_volume_metrics); // not on the denylist
        assert!(!cap.volume_expansion); // but expansion is off
        assert!(!cap.is_supported());
    }

    #[test]
    fn pod_metrics_404_signature_is_classified_as_metrics_server_missing() {
        // The exact raw string kube-rs produces when metrics.k8s.io/v1beta1
        // isn't registered at all (metrics-server never installed) — a plain
        // 404 body the apiserver's generic handler returns, not a structured
        // Status the client can parse (live-cluster-observed on Camelot EKS).
        let raw = "ApiError: \"404 page not found\\n\": Failed to parse error data \
                    (ErrorResponse { status: \"404 Not Found\", message: \"\\\"404 page not found\\\\n\\\"\", \
                    reason: \"Failed to parse error data\", code: 404 })"
            .to_string();
        match super::KubeCluster::classify_pod_metrics_error(raw.clone()) {
            breathe_provider::ProviderError::ApiTransient(msg) => {
                assert!(msg.contains("metrics-server not installed"), "message was: {msg}");
                assert!(msg.contains("metrics.k8s.io"), "message was: {msg}");
                assert!(msg.contains(&raw), "original error text must stay in the message for debugging");
            }
            other => panic!("expected ApiTransient, got {other:?}"),
        }
    }

    #[test]
    fn other_pod_metrics_errors_pass_through_unmodified() {
        // Anything that isn't the "API group doesn't exist" 404 signature
        // (e.g. a real connection error, a genuine transient 5xx) must not be
        // relabeled — only the specific missing-metrics-server case is enriched.
        let raw = "error trying to connect: tcp connect error".to_string();
        assert_eq!(
            super::KubeCluster::classify_pod_metrics_error(raw.clone()),
            breathe_provider::ProviderError::ApiTransient(raw)
        );
    }
}
