//! `breathe-mcp` — the MCP surface a model uses to drive a running breathe instance.
//!
//! breathe's state lives in k8s CRDs (`BreatheNodePool` + the five `*Band` kinds);
//! this is a typed FACADE over `kube::Api<T>`, not a second store — a tool's
//! `patch` is the same mutation a `kubectl patch` or Helm value change performs
//! against the same CR, so it never contends with the controller (which co-writes
//! `status`). The facade is behind a [`BreatheStore`] trait so the rmcp tools are
//! exercised against a mock without a live cluster (the testability seam).
//!
//! The tool set lets a model hold the full lifecycle: list/get/author/patch bands,
//! flip per-band `dryRun` and the node-level `writeEnabled` master switch (the two
//! safety gates), and read the self-describing catalog — shadow-first by default.

use std::sync::Arc;

use async_trait::async_trait;
use breathe_crd::{ArcBand, BreatheNodePool, CgroupBand, CpuBand, MemoryBand, StorageBand};
use kube::{
    api::{Api, ListParams, Patch, PatchParams},
    core::NamespaceResourceScope,
    Client,
};
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ServerHandler,
};
use serde::Deserialize;
use serde_json::{json, Value};

/// Which band dimension a tool addresses. The five kinds share one CRD shape
/// (`band_kind!`); this enum picks the typed `Api<T>`.
#[derive(Debug, Clone, Copy, Deserialize, schemars::JsonSchema)]
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
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("kube: {0}")]
    Kube(String),
    #[error("serialize: {0}")]
    Serde(String),
}

/// The facade operations the MCP tools call. Real impl is [`KubeStore`]; tests
/// pass a mock. Returns are JSON `Value` so the tool layer is uniform across the
/// five band kinds.
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

/// Build a namespaced-or-all `Api` for a band kind.
fn mk_api<K>(client: &Client, ns: Option<&str>) -> Api<K>
where
    K: kube::Resource<DynamicType = (), Scope = NamespaceResourceScope>,
{
    match ns {
        Some(n) => Api::namespaced(client.clone(), n),
        None => Api::all(client.clone()),
    }
}

/// Run `$body` with `$api: Api<ConcreteBand>` bound to the kind. The body is an
/// async expression (`.await` inside the surrounding async fn).
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
    /// In-cluster or kubeconfig default client.
    pub async fn from_env() -> anyhow::Result<Self> {
        Ok(Self { client: Client::try_default().await? })
    }
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

fn ke(e: kube::Error) -> StoreError {
    StoreError::Kube(e.to_string())
}
fn se(e: serde_json::Error) -> StoreError {
    StoreError::Serde(e.to_string())
}

/// The catalog as JSON — built here (DimensionSpec isn't Serialize) so no
/// breathe-catalog change is needed.
fn catalog_json() -> Value {
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
            let o = api
                .patch(&name, &PatchParams::default(), &Patch::Merge(&body))
                .await
                .map_err(ke)?;
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

// ─────────────────────────── tool inputs ───────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListBandsInput {
    #[schemars(description = "Band dimension: memory | cpu | storage | arc | cgroup")]
    pub kind: BandKind,
    #[serde(default)]
    #[schemars(description = "Namespace to scope to. Omitted = all namespaces.")]
    pub namespace: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BandRef {
    #[schemars(description = "Band dimension: memory | cpu | storage | arc | cgroup")]
    pub kind: BandKind,
    #[schemars(description = "Namespace of the band CR")]
    pub namespace: String,
    #[schemars(description = "Name of the band CR")]
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PatchBandInput {
    #[schemars(description = "Band dimension: memory | cpu | storage | arc | cgroup")]
    pub kind: BandKind,
    pub namespace: String,
    pub name: String,
    #[schemars(description = "Spec fields to merge-patch, e.g. {\"setpoint\":0.8,\"ceiling\":\"6Gi\"}")]
    pub spec: Value,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetDryRunInput {
    #[schemars(description = "Band dimension: memory | cpu | storage | arc | cgroup")]
    pub kind: BandKind,
    pub namespace: String,
    pub name: String,
    #[schemars(description = "true = SHADOW (decide + report, never mutate); false = LIVE carve")]
    pub dry_run: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PoolRef {
    #[schemars(description = "BreatheNodePool name (cluster-scoped)")]
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetWriteEnabledInput {
    #[schemars(description = "BreatheNodePool name")]
    pub name: String,
    #[schemars(description = "Node master write switch. false = whole node in SHADOW.")]
    pub write_enabled: bool,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct Empty {}

// ─────────────────────────── the server ────────────────────────────

#[derive(Clone)]
pub struct BreatheMcp {
    store: Arc<dyn BreatheStore>,
    tool_router: ToolRouter<Self>,
}

fn ok(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|e| format!("{{\"error\":\"serialize: {e}\"}}"))
}
fn err(e: &StoreError) -> String {
    json!({ "error": e.to_string() }).to_string()
}
fn result(r: Result<Value, StoreError>) -> String {
    match r {
        Ok(v) => ok(&v),
        Err(e) => err(&e),
    }
}

#[tool_router]
impl BreatheMcp {
    #[must_use]
    pub fn new(store: Arc<dyn BreatheStore>) -> Self {
        Self { store, tool_router: Self::tool_router() }
    }

    #[tool(description = "List breathe bands of a dimension (memory/cpu/storage/arc/cgroup), optionally namespace-scoped. Returns the CRs incl. spec + status.")]
    pub async fn breathe_band_list(&self, Parameters(p): Parameters<ListBandsInput>) -> String {
        result(self.store.list_bands(p.kind, p.namespace).await)
    }

    #[tool(description = "Get one breathe band CR by kind/namespace/name (full spec + status).")]
    pub async fn breathe_band_get(&self, Parameters(p): Parameters<BandRef>) -> String {
        result(self.store.get_band(p.kind, p.namespace, p.name).await)
    }

    #[tool(description = "Merge-patch a band's spec (setpoint, growAbove, shrinkBelow, growFactor, shrinkFactor, floor, ceiling, cooldownSeconds, maxStalenessSeconds). Tune the homeostasis band.")]
    pub async fn breathe_band_patch(&self, Parameters(p): Parameters<PatchBandInput>) -> String {
        result(self.store.patch_band_spec(p.kind, p.namespace, p.name, p.spec).await)
    }

    #[tool(description = "Flip a band's dryRun: true = SHADOW (decide + report, never carve), false = LIVE. One of the two safety gates.")]
    pub async fn breathe_band_set_dry_run(&self, Parameters(p): Parameters<SetDryRunInput>) -> String {
        result(self.store.patch_band_spec(p.kind, p.namespace, p.name, json!({ "dryRun": p.dry_run })).await)
    }

    #[tool(description = "List BreatheNodePools (cluster-scoped enrollment: node, L2 ceilings, master write switch).")]
    pub async fn breathe_nodepool_list(&self, Parameters(_): Parameters<Empty>) -> String {
        result(self.store.list_pools().await)
    }

    #[tool(description = "Get one BreatheNodePool by name.")]
    pub async fn breathe_nodepool_get(&self, Parameters(p): Parameters<PoolRef>) -> String {
        result(self.store.get_pool(p.name).await)
    }

    #[tool(description = "Flip a node's writeEnabled master switch: false = whole node in SHADOW (no host writes regardless of per-band dryRun), true = LIVE. The node-level safety gate.")]
    pub async fn breathe_nodepool_set_write_enabled(&self, Parameters(p): Parameters<SetWriteEnabledInput>) -> String {
        result(self.store.patch_pool_spec(p.name, json!({ "writeEnabled": p.write_enabled })).await)
    }

    #[tool(description = "The self-describing breathe dimension catalog (the 6 dimensions, maturity, directionality, host-vs-k8s, dependencies).")]
    pub async fn breathe_catalog_list(&self, Parameters(_): Parameters<Empty>) -> String {
        ok(&self.store.catalog())
    }
}

#[tool_handler]
impl ServerHandler for BreatheMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            instructions: Some(
                "Drive a running breathe resource-homeostasis instance. breathe holds workloads at a \
                 utilization band by carving resource limits. List/get/patch bands (memory/cpu/storage in \
                 k8s; arc/cgroup on the host) and BreatheNodePools. Shadow-first: a band's dryRun and a \
                 node's writeEnabled are the two safety gates — observe ShadowWouldApply decisions before \
                 flipping either to LIVE. Host writes are always bounded by the nodepool's L2 ceiling."
                    .into(),
            ),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// In-memory mock — records patches, returns canned data. The testability seam.
    #[derive(Default)]
    struct MockStore {
        patches: Mutex<Vec<(String, Value)>>,
    }
    #[async_trait]
    impl BreatheStore for MockStore {
        async fn list_bands(&self, kind: BandKind, _ns: Option<String>) -> Result<Value, StoreError> {
            Ok(json!([{ "kind": kind.as_str(), "metadata": { "name": "x" } }]))
        }
        async fn get_band(&self, kind: BandKind, ns: String, name: String) -> Result<Value, StoreError> {
            Ok(json!({ "kind": kind.as_str(), "namespace": ns, "name": name }))
        }
        async fn patch_band_spec(&self, _k: BandKind, _ns: String, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        async fn list_pools(&self) -> Result<Value, StoreError> {
            Ok(json!([{ "metadata": { "name": "rio" } }]))
        }
        async fn get_pool(&self, name: String) -> Result<Value, StoreError> {
            Ok(json!({ "name": name }))
        }
        async fn patch_pool_spec(&self, name: String, spec: Value) -> Result<Value, StoreError> {
            self.patches.lock().unwrap().push((name, spec.clone()));
            Ok(json!({ "spec": spec }))
        }
        fn catalog(&self) -> Value {
            catalog_json()
        }
    }

    #[tokio::test]
    async fn set_dry_run_patches_the_dry_run_spec_field() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        let out = mcp
            .breathe_band_set_dry_run(Parameters(SetDryRunInput {
                kind: BandKind::Arc,
                namespace: "pangea-system".into(),
                name: "rio-arc".into(),
                dry_run: false,
            }))
            .await;
        assert!(out.contains("dryRun"));
        let patches = mock.patches.lock().unwrap();
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].0, "rio-arc");
        assert_eq!(patches[0].1, json!({ "dryRun": false }));
    }

    #[tokio::test]
    async fn set_write_enabled_patches_the_pool_master_switch() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        let out = mcp
            .breathe_nodepool_set_write_enabled(Parameters(SetWriteEnabledInput {
                name: "rio".into(),
                write_enabled: true,
            }))
            .await;
        assert!(out.contains("writeEnabled"));
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "writeEnabled": true }));
    }

    #[tokio::test]
    async fn catalog_lists_the_six_dimensions_with_host_flags() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let out = mcp.breathe_catalog_list(Parameters(Empty {})).await;
        assert!(out.contains("\"arc\""));
        assert!(out.contains("\"cgroup\""));
        assert!(out.contains("\"isHost\""));
    }

    #[test]
    fn get_info_advertises_tools_and_instructions() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let info = mcp.get_info();
        assert!(info.capabilities.tools.is_some());
        assert!(info.instructions.unwrap().contains("Shadow-first"));
    }
}
