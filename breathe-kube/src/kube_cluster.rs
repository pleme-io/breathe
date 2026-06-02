//! `KubeCluster` — the real [`Cluster`] implementation over kube-rs.
//!
//! The four category-atomic I/O legs the providers call:
//!   - `metric`   — Prometheus/VictoriaMetrics instant query (value + sample age).
//!   - `current_allocation` — read the owner's declared limit from its spec.
//!   - `field_owners` — the live object's `managedFields` → [`FieldOwner`] (the
//!     field-granular single-writer guard's data source).
//!   - `apply`    — **true SSA** (`Patch::Apply` + force) with a per-dimension
//!     field manager, so ownership is a real `managedFields` record.
//!
//! Two owner layouts are handled: CNPG `Cluster` (top-level `spec.resources` —
//! the pangea-database M0 anchor) and pod-template owners (Deployment /
//! StatefulSet `spec.template.spec.containers[name].resources`).

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use breathe_control::FieldOwner;
use breathe_provider::{
    AppliedReceipt, Cluster, DimensionId, MetricKind, ProviderError, Sample, SsaPatch, Target,
};
use kube::{
    api::{Api, ApiResource, DynamicObject, Patch, PatchParams},
    core::GroupVersionKind,
    Client,
};
use serde_json::{json, Value};

use crate::managed_fields::{cnpg_cluster_limit_segments, field_owners, pod_template_limit_segments};

/// Where a memory limit lives on a given owner kind.
enum Layout {
    /// CNPG `Cluster`: `spec.resources.limits.memory`.
    ClusterTopLevel,
    /// Deployment/StatefulSet: `spec.template.spec.containers[name].resources.limits.memory`.
    PodTemplate { container: Option<String> },
}

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

    fn layout(target: &Target) -> Layout {
        match target.kind.as_str() {
            "Cluster" => Layout::ClusterTopLevel,
            _ => Layout::PodTemplate { container: target.container.clone() },
        }
    }

    /// Resolve the owner's `(group, version)` from `api_version`, inferring a
    /// sensible default from the kind when unset.
    fn group_version(target: &Target) -> (String, String) {
        if !target.api_version.is_empty() {
            return match target.api_version.split_once('/') {
                Some((g, v)) => (g.to_string(), v.to_string()),
                None => (String::new(), target.api_version.clone()), // core group
            };
        }
        match target.kind.as_str() {
            "Cluster" => ("postgresql.cnpg.io".into(), "v1".into()),
            _ => ("apps".into(), "v1".into()),
        }
    }

    fn api_for(&self, target: &Target) -> Api<DynamicObject> {
        let (g, v) = Self::group_version(target);
        let gvk = GroupVersionKind::gvk(&g, &v, &target.kind);
        let ar = ApiResource::from_gvk(&gvk); // naive plural: kind.lower()+"s" (deployments/statefulsets/clusters)
        Api::namespaced_with(self.client.clone(), &target.namespace, &ar)
    }

    async fn get_owner(&self, target: &Target) -> Result<DynamicObject, ProviderError> {
        self.api_for(target).get(&target.name).await.map_err(|e| match e {
            kube::Error::Api(ae) if ae.code == 404 => ProviderError::TargetNotFound,
            other => ProviderError::ApiTransient(other.to_string()),
        })
    }

    /// Read the declared memory-limit quantity string from a fetched owner.
    fn read_limit_qty(data: &Value, layout: &Layout) -> Option<String> {
        match layout {
            Layout::ClusterTopLevel => data
                .pointer("/spec/resources/limits/memory")
                .and_then(Value::as_str)
                .map(String::from),
            Layout::PodTemplate { container } => {
                let containers = data.pointer("/spec/template/spec/containers")?.as_array()?;
                let c = match container {
                    Some(name) => containers
                        .iter()
                        .find(|c| c.get("name").and_then(Value::as_str) == Some(name.as_str()))?,
                    None => containers.first()?,
                };
                c.pointer("/resources/limits/memory").and_then(Value::as_str).map(String::from)
            }
        }
    }
}

fn parse_qty(q: &str) -> Option<u64> {
    parse_size::Config::new().with_binary().parse_size(q).ok()
}

#[async_trait]
impl Cluster for KubeCluster {
    async fn metric(&self, target: &Target, kind: MetricKind) -> Result<Sample, ProviderError> {
        // Used = container working set across the owner's pods; Capacity = spec'd limit.
        let promql = match kind {
            MetricKind::Used => format!(
                r#"max(container_memory_working_set_bytes{{namespace="{ns}",pod=~"{name}.*",container!="",container!="POD"}})"#,
                ns = target.namespace, name = target.name
            ),
            MetricKind::Capacity => format!(
                r#"max(container_spec_memory_limit_bytes{{namespace="{ns}",pod=~"{name}.*",container!="",container!="POD"}})"#,
                ns = target.namespace, name = target.name
            ),
        };
        let url = format!("{}/api/v1/query", self.prometheus_url.trim_end_matches('/'));
        let resp: Value = self
            .http
            .get(&url)
            .query(&[("query", promql.as_str())])
            .send()
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?
            .json()
            .await
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        // result[0].value = [<unix_ts: f64>, "<value>"]
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
        let age_secs = (now - ts).max(0.0) as u64;
        Ok(Sample { value, age_secs })
    }

    async fn current_allocation(
        &self,
        target: &Target,
        _dim: DimensionId,
    ) -> Result<u64, ProviderError> {
        let obj = self.get_owner(target).await?;
        let layout = Self::layout(target);
        let qty = Self::read_limit_qty(&obj.data, &layout).ok_or(ProviderError::NoCapacityField)?;
        parse_qty(&qty).ok_or(ProviderError::NoCapacityField)
    }

    async fn field_owners(
        &self,
        target: &Target,
        _field: &str,
    ) -> Result<Vec<FieldOwner>, ProviderError> {
        let obj = self.get_owner(target).await?;
        let mf = serde_json::to_value(&obj.metadata.managed_fields)
            .map_err(|e| ProviderError::ApiTransient(e.to_string()))?;
        let (segments, logical) = match Self::layout(target) {
            Layout::ClusterTopLevel => {
                (cnpg_cluster_limit_segments("memory"), "spec.resources.limits.memory")
            }
            Layout::PodTemplate { container } => {
                // resolve the container name (first container if unset) for the keyed entry
                let name = container.or_else(|| {
                    obj.data
                        .pointer("/spec/template/spec/containers/0/name")
                        .and_then(Value::as_str)
                        .map(String::from)
                });
                match name {
                    Some(c) => (pod_template_limit_segments(&c, "memory"), "resources.limits.memory"),
                    None => return Ok(Vec::new()),
                }
            }
        };
        Ok(field_owners(&mf, &segments, logical))
    }

    async fn apply(&self, patch: &SsaPatch) -> Result<AppliedReceipt, ProviderError> {
        let target = &patch.target;
        let (g, v) = Self::group_version(target);
        let api_version = if g.is_empty() { v.clone() } else { format!("{g}/{v}") };
        let qty = patch.value.to_string(); // bytes as a bare k8s quantity
        let spec = match Self::layout(target) {
            Layout::ClusterTopLevel => json!({ "resources": { "limits": { "memory": qty } } }),
            Layout::PodTemplate { container } => {
                // SSA merges the containers list by name; resolve the name if unset.
                let cname = match container {
                    Some(c) => c,
                    None => {
                        let obj = self.get_owner(target).await?;
                        obj.data
                            .pointer("/spec/template/spec/containers/0/name")
                            .and_then(Value::as_str)
                            .map(String::from)
                            .ok_or(ProviderError::NoCapacityField)?
                    }
                };
                json!({ "template": { "spec": { "containers": [
                    { "name": cname, "resources": { "limits": { "memory": qty } } }
                ] } } })
            }
        };
        let body = json!({
            "apiVersion": api_version,
            "kind": target.kind,
            "metadata": { "name": target.name, "namespace": target.namespace },
            "spec": spec,
        });
        let pp = PatchParams::apply(&patch.field_manager).force();
        self.api_for(target)
            .patch(&target.name, &pp, &Patch::Apply(&body))
            .await
            .map_err(|e| ProviderError::ApiPermanent(e.to_string()))?;
        // M1: source_hash placeholder (BLAKE3 attestation wires in with OutcomeChain).
        Ok(AppliedReceipt { source_hash: [0u8; 16] })
    }
}
