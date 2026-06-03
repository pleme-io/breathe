//! `breathe-facade` — the typed facade over breathe's k8s CRD state.
//!
//! breathe's state lives in CRDs (`BreatheNodePool` + the five `*Band` kinds);
//! this is a typed projection + mutation morphism over `kube::Api<T>`, NOT a
//! second store. A `patch` here is the same mutation a `kubectl patch` or Helm
//! value change performs, so it never contends with the controller (which
//! co-writes `status`). The [`BreatheStore`] trait is the testability seam every
//! surface shares: MCP, REST, gRPC, GraphQL all drive this one core — solved once.

use async_trait::async_trait;
use breathe_crd::{ArcBand, BreatheNodePool, CgroupBand, CpuBand, MemoryBand, StorageBand};
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    core::NamespaceResourceScope,
    Client,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Which band dimension a call addresses. The five kinds share one CRD shape
/// (`band_kind!`); this enum picks the typed `Api<T>`. Derives `schemars` (1.x,
/// matching rmcp) so it drops straight into MCP tool input schemas.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, schemars::JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BandKind {
    Memory,
    Cpu,
    Storage,
    Arc,
    Cgroup,
}

impl BandKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Cpu => "cpu",
            Self::Storage => "storage",
            Self::Arc => "arc",
            Self::Cgroup => "cgroup",
        }
    }
    /// Parse from a path/string token (REST `/bands/{kind}`).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "memory" => Some(Self::Memory),
            "cpu" => Some(Self::Cpu),
            "storage" => Some(Self::Storage),
            "arc" => Some(Self::Arc),
            "cgroup" => Some(Self::Cgroup),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("kube: {0}")]
    Kube(String),
    #[error("serialize: {0}")]
    Serde(String),
    #[error("bad request: {0}")]
    BadRequest(String),
}

/// The facade operations every surface calls. Real impl is [`KubeStore`]; tests
/// pass a mock. Returns are JSON `Value` so callers are uniform across the five
/// band kinds.
#[async_trait]
pub trait BreatheStore: Send + Sync {
    async fn list_bands(&self, kind: BandKind, namespace: Option<String>) -> Result<Value, StoreError>;
    async fn get_band(&self, kind: BandKind, namespace: String, name: String) -> Result<Value, StoreError>;
    /// Merge-patch `spec` (the API/operator co-owns spec; the controller owns status).
    async fn patch_band_spec(&self, kind: BandKind, namespace: String, name: String, spec: Value) -> Result<Value, StoreError>;
    async fn list_pools(&self) -> Result<Value, StoreError>;
    async fn get_pool(&self, name: String) -> Result<Value, StoreError>;
    async fn patch_pool_spec(&self, name: String, spec: Value) -> Result<Value, StoreError>;
    /// The self-describing dimension catalog (zero-I/O, from `breathe-catalog`).
    fn catalog(&self) -> Value;
}

fn mk_api<K>(client: &Client, ns: Option<&str>) -> Api<K>
where
    K: kube::Resource<DynamicType = (), Scope = NamespaceResourceScope>,
{
    match ns {
        Some(n) => Api::namespaced(client.clone(), n),
        None => Api::all(client.clone()),
    }
}

/// Run `$body` with `$api: Api<ConcreteBand>` bound to the kind.
macro_rules! on_band {
    ($client:expr, $kind:expr, $ns:expr, |$api:ident| $body:expr) => {
        match $kind {
            BandKind::Memory => { let $api: Api<MemoryBand> = mk_api($client, $ns); $body }
            BandKind::Cpu => { let $api: Api<CpuBand> = mk_api($client, $ns); $body }
            BandKind::Storage => { let $api: Api<StorageBand> = mk_api($client, $ns); $body }
            BandKind::Arc => { let $api: Api<ArcBand> = mk_api($client, $ns); $body }
            BandKind::Cgroup => { let $api: Api<CgroupBand> = mk_api($client, $ns); $body }
        }
    };
}

/// The real `BreatheStore` over kube-rs.
pub struct KubeStore {
    client: Client,
}

impl KubeStore {
    pub async fn from_env() -> anyhow_lite::Result<Self> {
        Ok(Self { client: Client::try_default().await.map_err(|e| anyhow_lite::Error(e.to_string()))? })
    }
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// A tiny error wrapper so this crate needn't pull `anyhow` just for `from_env`.
pub mod anyhow_lite {
    #[derive(Debug)]
    pub struct Error(pub String);
    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }
    impl std::error::Error for Error {}
    pub type Result<T> = std::result::Result<T, Error>;
}

fn ke(e: kube::Error) -> StoreError {
    StoreError::Kube(e.to_string())
}
fn se(e: serde_json::Error) -> StoreError {
    StoreError::Serde(e.to_string())
}

/// The catalog as JSON (DimensionSpec isn't Serialize — built here).
#[must_use]
pub fn catalog_json() -> Value {
    let rows: Vec<Value> = breathe_catalog::CATALOG
        .iter()
        .map(|d| {
            json!({
                "id": d.id.as_str(),
                "name": d.name,
                "authoringKeyword": d.authoring_keyword,
                "maturity": format!("{:?}", d.maturity),
                "directionality": format!("{:?}", d.directionality),
                "purpose": d.purpose,
                "upstreamMirror": d.upstream_mirror,
                "isHost": d.id.is_host(),
                "dependsOn": d.depends_on.iter().map(|x| x.as_str()).collect::<Vec<_>>(),
            })
        })
        .collect();
    json!({ "dimensions": rows })
}

#[async_trait]
impl BreatheStore for KubeStore {
    async fn list_bands(&self, kind: BandKind, namespace: Option<String>) -> Result<Value, StoreError> {
        let ns = namespace.as_deref();
        on_band!(&self.client, kind, ns, |api| {
            let l = api.list(&ListParams::default()).await.map_err(ke)?;
            serde_json::to_value(l.items).map_err(se)
        })
    }
    async fn get_band(&self, kind: BandKind, namespace: String, name: String) -> Result<Value, StoreError> {
        on_band!(&self.client, kind, Some(namespace.as_str()), |api| {
            let o = api.get(&name).await.map_err(ke)?;
            serde_json::to_value(o).map_err(se)
        })
    }
    async fn patch_band_spec(&self, kind: BandKind, namespace: String, name: String, spec: Value) -> Result<Value, StoreError> {
        let body = json!({ "spec": spec });
        on_band!(&self.client, kind, Some(namespace.as_str()), |api| {
            let o = api.patch(&name, &PatchParams::default(), &Patch::Merge(&body)).await.map_err(ke)?;
            serde_json::to_value(o).map_err(se)
        })
    }
    async fn list_pools(&self) -> Result<Value, StoreError> {
        let api: Api<BreatheNodePool> = Api::all(self.client.clone());
        let l = api.list(&ListParams::default()).await.map_err(ke)?;
        serde_json::to_value(l.items).map_err(se)
    }
    async fn get_pool(&self, name: String) -> Result<Value, StoreError> {
        let api: Api<BreatheNodePool> = Api::all(self.client.clone());
        let o = api.get(&name).await.map_err(ke)?;
        serde_json::to_value(o).map_err(se)
    }
    async fn patch_pool_spec(&self, name: String, spec: Value) -> Result<Value, StoreError> {
        let api: Api<BreatheNodePool> = Api::all(self.client.clone());
        let body = json!({ "spec": spec });
        let o = api.patch(&name, &PatchParams::default(), &Patch::Merge(&body)).await.map_err(ke)?;
        serde_json::to_value(o).map_err(se)
    }
    fn catalog(&self) -> Value {
        catalog_json()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn band_kind_round_trips_str() {
        for k in [BandKind::Memory, BandKind::Cpu, BandKind::Storage, BandKind::Arc, BandKind::Cgroup] {
            assert_eq!(BandKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(BandKind::parse("nope"), None);
    }

    #[test]
    fn catalog_json_has_all_dimensions_with_host_flags() {
        let c = catalog_json();
        let dims = c["dimensions"].as_array().unwrap();
        assert_eq!(dims.len(), 7);
        assert!(dims.iter().any(|d| d["id"] == "arc" && d["isHost"] == true));
        assert!(dims.iter().any(|d| d["id"] == "cgroup-cpu" && d["isHost"] == true));
        assert!(dims.iter().any(|d| d["id"] == "memory" && d["isHost"] == false));
    }
}
