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
    cnpg_cluster_limit_segments, field_owners, pod_template_limit_segments, pvc_request_segments,
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
        }
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
    /// `resource` (memory bytes / cpu millicores) across the owner's pods (those
    /// whose name starts with `pod_prefix`), so the band holds the hottest
    /// instance at the setpoint. Independent of any TSDB.
    async fn pod_metrics_max(&self, resource: &str, pod_prefix: &str) -> Result<Sample, ProviderError> {
        let gvk = GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics");
        let ar = ApiResource::from_gvk_with_plural(&gvk, "pods");
        // metrics-server is cluster-scoped reads; namespace is inferred from the
        // prefix's pods — query all namespaces' PodMetrics and filter by name.
        let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &ar);
        let list = api
            .list(&ListParams::default())
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        let mut max: u64 = 0;
        let mut found = false;
        for pm in &list.items {
            let name = pm.metadata.name.as_deref().unwrap_or("");
            if !name.starts_with(pod_prefix) {
                continue;
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

#[async_trait]
impl Cluster for KubeCluster {
    async fn read_used(&self, source: &MetricSource) -> Result<Sample, ProviderError> {
        match source {
            MetricSource::Prometheus(promql) => self.prometheus_used(promql).await,
            MetricSource::PodMetricsMax { resource, pod_prefix } => {
                self.pod_metrics_max(resource, pod_prefix).await
            }
        }
    }

    async fn read_limit(
        &self,
        target: &Target,
        layout: &LimitLayout,
        resource: &str,
    ) -> Result<u64, ProviderError> {
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
            LimitLayout::PodTemplate { container } => {
                match Self::container_name(&obj.data, container) {
                    Some(c) => pod_template_limit_segments(&c, resource),
                    None => return Ok(Vec::new()),
                }
            }
        };
        Ok(field_owners(&mf, &segments, logical_field))
    }

    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
        let target = &patch.target;
        let (g, v) = Self::group_version(target);
        let api_version = if g.is_empty() { v.clone() } else { format!("{g}/{v}") };
        // Render in the resource's base unit: bytes as a bare integer, cpu with
        // the `m` suffix (a bare "250" would be read by k8s as 250 *cores*).
        let qty = Quantity { value: patch.value, unit: Unit::for_resource(&patch.resource) }.to_string();
        let res = &patch.resource;
        let spec = match &patch.layout {
            LimitLayout::ClusterTopLevel => json!({ "resources": { "limits": { res: qty } } }),
            LimitLayout::PvcRequest => json!({ "resources": { "requests": { "storage": qty } } }),
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
}
