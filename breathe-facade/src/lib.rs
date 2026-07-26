//! `breathe-facade` — the typed facade over breathe's k8s CRD state.
//!
//! breathe's state lives in CRDs (`BreatheNodePool` + the **ten** `*Band` kinds);
//! this is a typed projection + mutation morphism over `kube::Api<T>`, NOT a
//! second store. A `patch` here is the same mutation a `kubectl patch` or Helm
//! value change performs, so it never contends with the controller (which
//! co-writes `status`). The [`BreatheStore`] trait is the testability seam every
//! surface shares: MCP, REST, gRPC, GraphQL all drive this one core — solved once.
//!
//! **The kind vocabulary is [`breathe_provider::DimensionId`], not a local enum.**
//! This crate used to own a closed five-arm `BandKind` while ten `Band` kinds
//! shipped, so `CgroupCpuBand`/`HostParamBand`/`KubeParamBand`/`AppBand`/
//! `ReplicaBand` were reachable by `kubectl` and by nothing else — no MCP tool,
//! no REST route, no GraphQL field, no gRPC method could name them. Dispatching
//! on the canonical enum makes an eleventh kind an `E0004` in [`on_band`] rather
//! than a silent hole.

use async_trait::async_trait;
use breathe_crd::{
    AppBand, ArcBand, BreatheNodePool, BreathePosture, CgroupBand, CgroupCpuBand, CpuBand, HostParamBand,
    KubeParamBand, MemoryBand, ReplicaBand, RequestBand, StorageBand,
};
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    core::NamespaceResourceScope,
    Client,
};
use serde_json::{json, Value};

pub use breathe_provider::DimensionId;

/// The former five-arm facade enum, now the canonical ten-arm
/// [`DimensionId`].
///
/// Kept as a name because it is public API of a published crate and downstream
/// consumers (vendaval, the MCP re-export) spell it this way; it is not a
/// distinct type any more, so it cannot drift back out of sync.
#[deprecated(note = "renamed to DimensionId — the canonical ten-arm dimension atom in breathe-provider")]
pub type BandKind = DimensionId;

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
/// pass a mock. Returns are JSON `Value` so callers are uniform across all ten
/// band kinds.
#[async_trait]
pub trait BreatheStore: Send + Sync {
    async fn list_bands(&self, kind: DimensionId, namespace: Option<String>) -> Result<Value, StoreError>;
    async fn get_band(&self, kind: DimensionId, namespace: String, name: String) -> Result<Value, StoreError>;
    /// Merge-patch `spec` (the API/operator co-owns spec; the controller owns status).
    async fn patch_band_spec(&self, kind: DimensionId, namespace: String, name: String, spec: Value) -> Result<Value, StoreError>;
    /// Merge-patch a band's `metadata.annotations`.
    ///
    /// Exists for one reason: `breathe.pleme.io/confirmed` (see
    /// [`breathe_provider::CONFIRMED_ANNOTATION`]) is the operator fast-path that
    /// promotes a calibrating band to writing *now* instead of waiting out its
    /// confirm window — the strongest live-authorization primitive breathe has,
    /// and until this method it was settable from Rust source and `kubectl` only,
    /// invisible to all four operator surfaces. Annotations are metadata, not
    /// spec, so [`Self::patch_band_spec`] structurally cannot reach it.
    async fn annotate_band(
        &self,
        kind: DimensionId,
        namespace: String,
        name: String,
        annotations: Value,
    ) -> Result<Value, StoreError>;
    async fn list_pools(&self) -> Result<Value, StoreError>;
    async fn get_pool(&self, name: String) -> Result<Value, StoreError>;
    async fn patch_pool_spec(&self, name: String, spec: Value) -> Result<Value, StoreError>;
    /// List every `BreathePosture` (cluster-scoped — mirrors `list_pools`).
    async fn list_postures(&self) -> Result<Value, StoreError>;
    /// Get one `BreathePosture` by name.
    async fn get_posture(&self, name: String) -> Result<Value, StoreError>;
    /// Merge-patch a `BreathePosture`'s spec (mirrors `patch_pool_spec`). A
    /// posture patch fans out to every band referencing it on their NEXT
    /// reconcile tick — it never itself widens a capacity bound or a
    /// promotion state (see `breathe-crd`'s `posture.rs` module doc: the
    /// spec structurally carries no such field).
    async fn patch_posture_spec(&self, name: String, spec: Value) -> Result<Value, StoreError>;
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
///
/// **This match is the coherence gate.** It is exhaustive over [`DimensionId`],
/// so a new dimension cannot ship without being given a concrete CRD type here —
/// the compiler refuses it (`E0004`). That is the mechanism that replaced a
/// hand-written five-arm list which had silently fallen five kinds behind.
macro_rules! on_band {
    ($client:expr, $kind:expr, $ns:expr, |$api:ident| $body:expr) => {
        match $kind {
            DimensionId::Memory => { let $api: Api<MemoryBand> = mk_api($client, $ns); $body }
            DimensionId::Cpu => { let $api: Api<CpuBand> = mk_api($client, $ns); $body }
            DimensionId::Storage => { let $api: Api<StorageBand> = mk_api($client, $ns); $body }
            DimensionId::Replica => { let $api: Api<ReplicaBand> = mk_api($client, $ns); $body }
            DimensionId::Arc => { let $api: Api<ArcBand> = mk_api($client, $ns); $body }
            DimensionId::Cgroup => { let $api: Api<CgroupBand> = mk_api($client, $ns); $body }
            DimensionId::CgroupCpu => { let $api: Api<CgroupCpuBand> = mk_api($client, $ns); $body }
            DimensionId::HostParam => { let $api: Api<HostParamBand> = mk_api($client, $ns); $body }
            DimensionId::KubeParam => { let $api: Api<KubeParamBand> = mk_api($client, $ns); $body }
            DimensionId::AppParam => { let $api: Api<AppBand> = mk_api($client, $ns); $body }
            DimensionId::Request => { let $api: Api<RequestBand> = mk_api($client, $ns); $body }
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
    /// The underlying `kube::Client` — for callers that need to drive a
    /// second `Api<T>` (a different CRD, a `DynamicObject`) against the SAME
    /// connection this facade already holds, rather than opening a second
    /// kubeconfig-derived client. breathe-facade owns connection setup once;
    /// this is the seam other in-process consumers (e.g. vendaval's
    /// `CamelotStormEnv`) reuse it through.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
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
    async fn list_bands(&self, kind: DimensionId, namespace: Option<String>) -> Result<Value, StoreError> {
        let ns = namespace.as_deref();
        on_band!(&self.client, kind, ns, |api| {
            let l = api.list(&ListParams::default()).await.map_err(ke)?;
            serde_json::to_value(l.items).map_err(se)
        })
    }
    async fn get_band(&self, kind: DimensionId, namespace: String, name: String) -> Result<Value, StoreError> {
        on_band!(&self.client, kind, Some(namespace.as_str()), |api| {
            let o = api.get(&name).await.map_err(ke)?;
            serde_json::to_value(o).map_err(se)
        })
    }
    async fn patch_band_spec(&self, kind: DimensionId, namespace: String, name: String, spec: Value) -> Result<Value, StoreError> {
        let body = json!({ "spec": spec });
        on_band!(&self.client, kind, Some(namespace.as_str()), |api| {
            let o = api.patch(&name, &PatchParams::default(), &Patch::Merge(&body)).await.map_err(ke)?;
            serde_json::to_value(o).map_err(se)
        })
    }
    async fn annotate_band(
        &self,
        kind: DimensionId,
        namespace: String,
        name: String,
        annotations: Value,
    ) -> Result<Value, StoreError> {
        let body = json!({ "metadata": { "annotations": annotations } });
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
    async fn list_postures(&self) -> Result<Value, StoreError> {
        let api: Api<BreathePosture> = Api::all(self.client.clone());
        let l = api.list(&ListParams::default()).await.map_err(ke)?;
        serde_json::to_value(l.items).map_err(se)
    }
    async fn get_posture(&self, name: String) -> Result<Value, StoreError> {
        let api: Api<BreathePosture> = Api::all(self.client.clone());
        let o = api.get(&name).await.map_err(ke)?;
        serde_json::to_value(o).map_err(se)
    }
    async fn patch_posture_spec(&self, name: String, spec: Value) -> Result<Value, StoreError> {
        let api: Api<BreathePosture> = Api::all(self.client.clone());
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
        for k in DimensionId::ALL {
            assert_eq!(DimensionId::parse(k.as_str()), Some(k));
        }
        assert_eq!(DimensionId::parse("nope"), None);
    }

    #[test]
    fn catalog_json_has_all_dimensions_with_host_flags() {
        let c = catalog_json();
        let dims = c["dimensions"].as_array().unwrap();
        // Track the catalog dynamically so this can't drift when a dimension lands
        // (it previously hard-coded 8 and went stale when `kube-param` was added).
        assert_eq!(dims.len(), breathe_catalog::ALL_DIMENSIONS.len());
        assert!(dims.iter().any(|d| d["id"] == "arc" && d["isHost"] == true));
        assert!(dims.iter().any(|d| d["id"] == "cgroup-cpu" && d["isHost"] == true));
        assert!(dims.iter().any(|d| d["id"] == "memory" && d["isHost"] == false));
    }
}
