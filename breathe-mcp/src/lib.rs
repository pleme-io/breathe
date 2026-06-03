//! `breathe-mcp` — the MCP surface a model uses to drive a running breathe instance.
//!
//! The state + the mutation morphism live in [`breathe_facade`] (the CRDs ARE the
//! state — a typed facade, not a second store, shared by every surface). This
//! crate is only the rmcp tool wrapping: one `#[tool]` per facade operation, over
//! stdio. A tool's `patch` is the same mutation a `kubectl patch` performs, so it
//! never contends with the controller (which co-writes `status`).
//!
//! The tool set holds the full shadow-first lifecycle: list/get/author/patch
//! bands, flip per-band `dryRun` and the node `writeEnabled` master switch (the
//! two safety gates), read the self-describing catalog.

use std::sync::Arc;

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router, ServerHandler,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub use breathe_facade::{BandKind, BreatheStore, KubeStore, StoreError};

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
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockStore {
        patches: Mutex<Vec<(String, Value)>>,
    }
    #[async_trait]
    impl BreatheStore for MockStore {
        async fn list_bands(&self, kind: BandKind, _ns: Option<String>) -> Result<Value, StoreError> {
            Ok(json!([{ "kind": kind.as_str() }]))
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
            breathe_facade::catalog_json()
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
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "dryRun": false }));
    }

    #[tokio::test]
    async fn set_write_enabled_patches_the_pool_master_switch() {
        let mock = Arc::new(MockStore::default());
        let mcp = BreatheMcp::new(mock.clone());
        let out = mcp
            .breathe_nodepool_set_write_enabled(Parameters(SetWriteEnabledInput { name: "rio".into(), write_enabled: true }))
            .await;
        assert!(out.contains("writeEnabled"));
        assert_eq!(mock.patches.lock().unwrap()[0].1, json!({ "writeEnabled": true }));
    }

    #[test]
    fn get_info_advertises_tools_and_instructions() {
        let mcp = BreatheMcp::new(Arc::new(MockStore::default()));
        let info = mcp.get_info();
        assert!(info.capabilities.tools.is_some());
        assert!(info.instructions.unwrap().contains("Shadow-first"));
    }
}
